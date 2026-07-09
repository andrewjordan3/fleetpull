# src/fleetpull/orchestrator/unit_loop.py
"""The claim-and-drive loop: serial, ascending, fail-fast work-unit execution.

``drive_claimable_units`` is the choreography between the work-unit queue and
the runner's per-unit drive (DESIGN sections 4/5): claim the lowest claimable
unit, drive it, mark it done, repeat until the queue is drained. Units drive
serially in ascending window order -- claims are FIFO by ``unit_id`` and units
are always enqueued chronologically, so completed units form a contiguous
prefix of the plan. That prefix is why the per-unit watermark advance is
sound: every persisted watermark is true at every instant (everything at or
before it has been fetched and committed), which is the truth invariant the
serial-ascending constraint exists to protect. Unit order is not a free
choice.

Failure is fail-fast: the first failing unit is marked ``failed`` -- back to a
claimable state, nothing committed by it -- and its exception re-raises,
aborting the endpoint's remaining units (never claimed, still ``pending``).
Completed units stay committed; the next invocation re-claims what remains.
"""

import logging
from collections.abc import Callable
from typing import Final, Protocol

from fleetpull.incremental import DateWindow
from fleetpull.orchestrator.outcome import Executed
from fleetpull.state import ClaimedWorkUnit, WorkUnitSpec
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

    def mark_done(self, unit_id: int) -> None:
        """Mark a claimed unit completed."""
        ...

    def mark_failed(self, unit_id: int, *, error_detail: str) -> None:
        """Mark a claimed unit failed (claimable again on a later invocation)."""
        ...


def drive_claimable_units(
    queue: UnitQueue,
    provider: Provider,
    endpoint: str,
    drive_unit: Callable[[DateWindow], Executed],
) -> list[Executed]:
    """Claim and drive every claimable unit, ascending, until the queue drains.

    Repeatedly claims the lowest claimable unit (FIFO by ``unit_id`` --
    ascending window order, since units enqueue chronologically), drives it,
    and marks it done; stops when nothing is claimable. Fail-fast on the
    first failure: the unit is marked ``failed`` (claimable again next
    invocation; nothing it staged was committed) and the original exception
    re-raises, so units beyond it are never claimed.

    Args:
        queue: The work-unit claim queue.
        provider: The provider whose units to drive.
        endpoint: The endpoint whose units to drive.
        drive_unit: Runs one unit over its window -- the runner's per-unit
            fetch/write/advance/record sequence -- returning its outcome.

    Returns:
        Each driven unit's ``Executed`` outcome, in the order driven (empty
        when nothing was claimable).

    Raises:
        Exception: The first failing unit's exception, re-raised unchanged
            after the unit is marked ``failed``.

    Side Effects:
        Claims, completes, or fails units in the queue; whatever
        ``drive_unit`` performs (network fetches, parquet writes, cursor and
        ledger commits).
    """
    outcomes: list[Executed] = []
    while (
        claimed := queue.claim_next(provider, endpoint, max_attempts=_UNIT_ATTEMPT_CAP)
    ) is not None:
        window = DateWindow(start=claimed.spec.chunk_start, end=claimed.spec.chunk_end)
        try:
            outcomes.append(drive_unit(window))
        except Exception as unit_failure:
            _mark_failed_safely(queue, claimed, unit_failure)
            raise
        queue.mark_done(claimed.unit_id)
    return outcomes


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
