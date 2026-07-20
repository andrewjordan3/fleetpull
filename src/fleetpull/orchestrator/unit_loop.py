# src/fleetpull/orchestrator/unit_loop.py
"""The claim-and-drive loop: concurrent, prefix-committing work-unit execution.

``drive_claimable_units`` is the choreography between the work-unit queue and
the runner's per-unit drive (DESIGN sections 4/5): ``workers`` threads each
claim the lowest claimable unit, drive it, mark it done with its folded
observation, and commit the watermark prefix, until the queue is drained.
Claims are FIFO by ``unit_id`` (ascending window order, since units enqueue
chronologically) but completions may land in any order -- the watermark's
truth invariant no longer rests on completion order. It rests on the
PREFIX-ADVANCE rule (2026-07-20): ``commit_prefix`` advances the cursor only
across the contiguous done-prefix of the plan, through the cursor store's
atomic forward-only write, so every persisted watermark is true at every
instant whatever the interleaving. The serial-ascending constraint this loop
once carried existed solely to protect the per-unit advance; the prefix rule
retired both together.

Failure stops the claiming, not the flight: the first failing unit is marked
``failed`` (claimable again on a later invocation; nothing it staged was
committed) and the stop signal keeps every worker from claiming further --
in-flight units run to completion and commit (each is an independent
transaction; aborting a mid-write sibling buys nothing). After every worker
joins, the first failure re-raises.

Cross-unit parallel writes are legal under the single-writer invariant
verbatim: partitioned endpoints may parallelize across partitions, and units
own disjoint whole-day partitions by construction (midnight-aligned,
contiguous tiling).
"""

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Final, Protocol

from fleetpull.incremental import DateWindow
from fleetpull.orchestrator.outcome import Executed
from fleetpull.state import ClaimedWorkUnit, WorkUnitSpec
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = ['UnitQueue', 'drive_claimable_units']

logger = logging.getLogger(__name__)

# A unit stays claimable on every invocation until it succeeds: fail-fast
# already surfaces each failure to the operator per run, so a finite attempt
# cap could only convert a persistent failure into a silently skipped unit --
# a coverage hole behind an advancing watermark, breaking the truth
# invariant. The store's claim API requires a cap, so the loop passes one
# that never binds.
_UNIT_ATTEMPT_CAP: Final[int] = 2**31 - 1


class UnitQueue(Protocol):
    """The work-unit surface the loop and the runner need (``WorkUnitStore``'s shape)."""

    def enqueue(self, units: list[WorkUnitSpec]) -> int:
        """Insert planned units idempotently; return the newly inserted count."""
        ...

    def reset_claimed_to_pending(self, provider: Provider, endpoint: str) -> int:
        """Revert orphaned ``claimed`` units to ``pending`` (startup recovery)."""
        ...

    def claim_next(
        self, provider: Provider, endpoint: str, *, max_attempts: int
    ) -> ClaimedWorkUnit | None:
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


class _UnitCrew:
    """The shared state one ``drive_claimable_units`` invocation's workers race on.

    A worker loop is pure claim/drive/commit; everything cross-thread lives
    here: the stop signal that ends claiming on the first failure, and the
    lock-guarded outcome and failure lists (append order is completion
    order). The class exists so the worker function reads as the loop it is,
    with the synchronization named rather than threaded through arguments.
    """

    def __init__(
        self,
        queue: UnitQueue,
        provider: Provider,
        endpoint: str,
        drive_unit: Callable[[DateWindow], Executed],
        commit_prefix: Callable[[], None],
    ) -> None:
        self._queue = queue
        self._provider = provider
        self._endpoint = endpoint
        self._drive_unit = drive_unit
        self._commit_prefix = commit_prefix
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
                claimed = self._queue.claim_next(
                    self._provider, self._endpoint, max_attempts=_UNIT_ATTEMPT_CAP
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
            unit_outcome = self._drive_unit(window)
        except Exception as unit_failure:
            # Stop BEFORE the failed-mark lands: a marked-failed unit is
            # claimable again, and a sibling passing the stop check after
            # the mark would retry-loop it inside this same invocation.
            self._stop_claiming.set()
            _mark_failed_safely(self._queue, claimed, unit_failure)
            with self._results_lock:
                self.failures.append(unit_failure)
            return
        self._queue.mark_done(
            claimed.unit_id, observed_max=unit_outcome.latest_observed
        )
        self._commit_prefix()
        with self._results_lock:
            self.outcomes.append(unit_outcome)
        # The total planned count is not knowable here (the loop claims
        # until the queue drains), so the unit narrates by its id.
        logger.info(
            'unit complete: provider=%s endpoint=%s unit_id=%d '
            'window_start=%s window_end=%s records_fetched=%d',
            self._provider.value,
            self._endpoint,
            claimed.unit_id,
            to_iso8601(window.start),
            to_iso8601(window.end),
            unit_outcome.records_fetched,
        )


def drive_claimable_units(
    queue: UnitQueue,
    provider: Provider,
    endpoint: str,
    drive_unit: Callable[[DateWindow], Executed],
    *,
    workers: int,
    commit_prefix: Callable[[], None],
) -> list[Executed]:
    """Claim and drive every claimable unit, ``workers`` at a time.

    Each worker repeatedly claims the lowest claimable unit (FIFO by
    ``unit_id``), drives it, marks it done with its folded observation, and
    invokes ``commit_prefix``; the invocation ends when nothing is claimable.
    The first failure marks its unit ``failed`` (claimable again next
    invocation; nothing it staged was committed) and stops further claiming;
    in-flight units complete and commit, and the failure re-raises after
    every worker joins.

    Args:
        queue: The work-unit claim queue.
        provider: The provider whose units to drive.
        endpoint: The endpoint whose units to drive.
        drive_unit: Runs one unit over its window -- the runner's per-unit
            fetch/write/record sequence -- returning its outcome.
        workers: The number of units in flight at once; at least 1. One
            worker runs inline on the calling thread (no pool), preserving
            the serial path as the degenerate case.
        commit_prefix: Advances the watermark across the contiguous
            done-prefix (the runner's prefix commit); invoked after every
            completion, safe under concurrent invocation by construction.

    Returns:
        Each driven unit's ``Executed`` outcome, in completion order (empty
        when nothing was claimable).

    Raises:
        ValueError: ``workers`` is below 1.
        Exception: The first failing unit's exception, re-raised after every
            worker joins (later failures are logged on their workers and
            their units marked ``failed``).

    Side Effects:
        Claims, completes, or fails units in the queue; commits the
        watermark prefix per completion; narrates one INFO per completed
        unit; whatever ``drive_unit`` performs (network fetches, parquet
        writes, ledger commits).
    """
    if workers < 1:
        raise ValueError(f'workers must be at least 1, got {workers}')
    crew = _UnitCrew(queue, provider, endpoint, drive_unit, commit_prefix)
    if workers == 1:
        crew.work()
    else:
        thread_prefix = f'fleetpull-units-{provider.value}-{endpoint}'
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix=thread_prefix
        ) as pool:
            for worker_future in [pool.submit(crew.work) for _ in range(workers)]:
                worker_future.result()
    if crew.failures:
        raise crew.failures[0]
    return crew.outcomes


def _mark_failed_safely(
    queue: UnitQueue, claimed: ClaimedWorkUnit, unit_failure: Exception
) -> None:
    """Record the unit failed without masking the failure that ended it.

    ``mark_failed`` touches SQLite, which can itself fail; that secondary
    failure must not replace the exception that actually ended the unit (the
    ``_fail_run_safely`` stance). A unit left ``claimed`` by a failed mark is
    still recovered: the next invocation's startup reset reverts it.

    Args:
        queue: The work-unit claim queue.
        claimed: The unit that failed.
        unit_failure: The exception that ended it, recorded as the detail.

    Side Effects:
        Marks the unit failed; on a recording failure, logs and swallows it.
    """
    try:
        queue.mark_failed(claimed.unit_id, error_detail=str(unit_failure))
    except Exception:
        logger.exception(
            'failed to record work unit %s as failed after an earlier error',
            claimed.unit_id,
        )
