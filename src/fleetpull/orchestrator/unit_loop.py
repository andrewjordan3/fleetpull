# src/fleetpull/orchestrator/unit_loop.py
"""The claim-and-drive loop: concurrent, prefix-committing work-unit execution.

``drive_claimable_units`` drives a ``UnitCrew`` -- one endpoint's work-unit
queue bundled with the runner's per-unit callbacks -- with ``workers``
threads: each claims the lowest claimable unit, drives it, marks it done with
its folded observation, and commits the watermark prefix, until the queue is
drained (DESIGN sections 4/5). Claims are FIFO by ``unit_id`` (ascending
window order, since units enqueue chronologically) but completions may land
in any order -- the watermark's truth invariant no longer rests on completion
order. It rests on the PREFIX-ADVANCE rule (DESIGN section 5, 2026-07-20):
``commit_prefix`` advances the cursor only across the contiguous done-prefix
of the plan, through the cursor store's atomic forward-only write, so every
persisted watermark is true at every instant whatever the interleaving. The
serial-ascending constraint this loop once carried existed solely to protect
the per-unit advance; the prefix rule retired both together.

Failure stops the claiming, not the flight: every failing unit is logged and
marked ``failed`` (claimable again on a later invocation; nothing it staged
was committed), and the stop signal ends further claiming -- in-flight units
run to completion and commit (each is an independent transaction; aborting a
mid-write sibling buys nothing). The stop signal lands before the failed-mark
so a sibling's next stop check usually precedes reclaimability, but the
window is narrowed, not closed: a sibling already past its stop check when
the mark lands can claim the just-failed unit once more within the same
invocation -- at most one extra claim per already-running sibling, each
logged and each incrementing ``attempt_count``. After every worker joins, the
first failure re-raises.

Cross-unit parallel writes stay inside the single-writer invariant on two
legs, both load-bearing: units own disjoint whole-day date partitions by
construction (midnight-aligned, contiguous tiling), and the concurrently
claimable set is always one contiguous plan -- the residual is enqueued only
after the claim loop over leftovers drains, so no two in-flight units can
come from overlapping plans. Both legs hold only for ``DATE_PARTITIONED``
watermark cells -- the only watermark storage shipped; a future
(``SINGLE``, ``WatermarkMode``) cell shares one file across units and must
serialize its units or reject ``workers > 1`` when it is built (DESIGN
section 5's recorded build obligation).
"""

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Protocol

from fleetpull.incremental import DateWindow
from fleetpull.orchestrator.outcome import Executed
from fleetpull.orchestrator.recording import record_failure_safely
from fleetpull.state import ClaimedWorkUnit, WorkUnitSpec
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = ['UnitCrew', 'UnitQueue', 'drive_claimable_units']

logger = logging.getLogger(__name__)


class UnitQueue(Protocol):
    """The work-unit surface the loop and the runner need (``WorkUnitStore``'s shape)."""

    def enqueue(self, units: list[WorkUnitSpec]) -> int:
        """Insert planned units idempotently; return the newly inserted count."""
        ...

    def reset_claimed_to_pending(self, provider: Provider, endpoint: str) -> int:
        """Revert orphaned ``claimed`` units to ``pending`` (startup recovery)."""
        ...

    def claim_next(self, provider: Provider, endpoint: str) -> ClaimedWorkUnit | None:
        """Atomically claim the lowest claimable unit, or return ``None``."""
        ...

    def mark_done(self, unit_id: int, *, observed_max: datetime | None) -> None:
        """Mark a claimed unit completed, recording its folded observation."""
        ...

    def mark_failed(self, unit_id: int, *, error_detail: str) -> None:
        """Mark a claimed unit failed (claimable again on a later invocation)."""
        ...

    def done_prefix_observation(
        self, provider: Provider, endpoint: str
    ) -> datetime | None:
        """The maximum observation across the contiguous done-prefix."""
        ...


@dataclass(frozen=True, slots=True)
class UnitCrew:
    """One endpoint's unit-drive collaborators: the queue and the runner's callbacks.

    The identity a ``drive_claimable_units`` invocation runs over -- who
    serves the units and what to do per unit. Pure identity, no execution
    state: the same crew serves both the leftover pass and the residual pass
    of one watermark run (each invocation builds its stop signal and result
    lists fresh in the private drive).

    Attributes:
        queue: The work-unit claim queue.
        provider: The provider whose units to drive.
        endpoint: The endpoint whose units to drive.
        drive_unit: Runs one unit over its window -- the runner's per-unit
            fetch/write/record sequence -- returning its outcome.
        commit_prefix: Advances the watermark across the contiguous
            done-prefix (the runner's prefix commit); invoked after every
            completion, safe under concurrent invocation by construction.
    """

    queue: UnitQueue
    provider: Provider
    endpoint: str
    drive_unit: Callable[[DateWindow], Executed]
    commit_prefix: Callable[[], None]


class _CrewDrive:
    """The shared state one ``drive_claimable_units`` invocation's workers race on.

    A worker loop is pure claim/drive/commit; everything cross-thread lives
    here: the stop signal that ends claiming on the first failure, and the
    lock-guarded outcome and failure lists (append order is completion
    order). The class exists so the worker function reads as the loop it is,
    with the synchronization named rather than threaded through arguments.
    """

    def __init__(self, crew: UnitCrew) -> None:
        self._crew = crew
        self._stop_claiming = threading.Event()
        self._results_lock = threading.Lock()
        self.outcomes: list[Executed] = []
        self.failures: list[Exception] = []

    def work(self) -> None:
        """Claim and drive units until the queue drains or a failure stops claiming.

        Side Effects:
            Claims, completes, or fails units; drives fetches and writes;
            commits the watermark prefix after each completion; narrates one
            INFO per completed unit. Any exit -- drain, failure, or an
            escaping store error -- stops further claiming crew-wide.
        """
        try:
            while not self._stop_claiming.is_set():
                claimed = self._crew.queue.claim_next(
                    self._crew.provider, self._crew.endpoint
                )
                if claimed is None:
                    return
                self._drive_claimed(claimed)
        finally:
            # A drained worker means nothing is left to claim; a failed or
            # crashed one must keep siblings from claiming more. Either way,
            # in-flight siblings finish their own units.
            self._stop_claiming.set()

    def _drive_claimed(self, claimed: ClaimedWorkUnit) -> None:
        """Drive one claimed unit through completion or failure recording."""
        window = DateWindow(start=claimed.spec.chunk_start, end=claimed.spec.chunk_end)
        try:
            unit_outcome = self._crew.drive_unit(window)
        except Exception as unit_failure:
            # Stop BEFORE the failed-mark lands. This narrows -- but does
            # NOT close -- the same-invocation retry window: a marked-failed
            # unit is claimable again, and a sibling already past its stop
            # check when the mark lands can still claim it -- at most one
            # extra claim per already-running sibling, each logged below and
            # each incrementing attempt_count.
            self._stop_claiming.set()
            # Every failure is surfaced here as it lands, not only the first
            # one that re-raises after the join -- a sibling's failure while
            # another worker's exception wins must never vanish silently.
            logger.exception(
                'unit failed: provider=%s endpoint=%s unit_id=%d',
                self._crew.provider.value,
                self._crew.endpoint,
                claimed.unit_id,
            )
            record_failure_safely(
                partial(
                    self._crew.queue.mark_failed,
                    claimed.unit_id,
                    error_detail=str(unit_failure),
                ),
                f'work unit {claimed.unit_id}',
            )
            with self._results_lock:
                self.failures.append(unit_failure)
            return
        self._crew.queue.mark_done(
            claimed.unit_id, observed_max=unit_outcome.latest_observed
        )
        self._crew.commit_prefix()
        with self._results_lock:
            self.outcomes.append(unit_outcome)
        # The total planned count is not knowable here (the loop claims
        # until the queue drains), so the unit narrates by its id.
        logger.info(
            'unit complete: provider=%s endpoint=%s unit_id=%d '
            'window_start=%s window_end=%s records_fetched=%d',
            self._crew.provider.value,
            self._crew.endpoint,
            claimed.unit_id,
            to_iso8601(window.start),
            to_iso8601(window.end),
            unit_outcome.records_fetched,
        )


def drive_claimable_units(crew: UnitCrew, *, workers: int) -> list[Executed]:
    """Claim and drive every claimable unit, ``workers`` at a time.

    Each worker repeatedly claims the lowest claimable unit (FIFO by
    ``unit_id``), drives it through the crew's ``drive_unit``, marks it done
    with its folded observation, and invokes the crew's ``commit_prefix``;
    the invocation ends when nothing is claimable. The first failure marks
    its unit ``failed`` (claimable again next invocation; nothing it staged
    was committed) and stops further claiming; in-flight units complete and
    commit, and the failure re-raises after every worker joins. The stop
    lands before the failed-mark, which narrows but does not close the
    same-invocation retry window: a sibling already past its stop check can
    claim the just-failed unit once more, so a persistently failing unit can
    fail up to once per already-running sibling before the invocation ends
    -- each failure logged, each incrementing ``attempt_count``.

    Args:
        crew: The queue and per-unit callbacks to drive.
        workers: The number of units in flight at once; at least 1. One
            worker runs inline on the calling thread (no pool), preserving
            the serial path as the degenerate case.

    Returns:
        Each driven unit's ``Executed`` outcome, in completion order (empty
        when nothing was claimable).

    Raises:
        ValueError: ``workers`` is below 1.
        Exception: The first failing unit's exception, re-raised after every
            worker joins. Every failure -- first or later -- is logged with
            its unit id as it lands and its unit marked ``failed``; only the
            first re-raises.

    Side Effects:
        Claims, completes, or fails units in the queue; commits the
        watermark prefix per completion; narrates one INFO per completed
        unit and one exception log per failed unit; whatever ``drive_unit``
        performs (network fetches, parquet writes, ledger commits).
    """
    if workers < 1:
        raise ValueError(f'workers must be at least 1, got {workers}')
    drive = _CrewDrive(crew)
    if workers == 1:
        drive.work()
    else:
        thread_prefix = f'fleetpull-units-{crew.provider.value}-{crew.endpoint}'
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix=thread_prefix
        ) as pool:
            for worker_future in [pool.submit(drive.work) for _ in range(workers)]:
                worker_future.result()
    if drive.failures:
        raise drive.failures[0]
    return drive.outcomes
