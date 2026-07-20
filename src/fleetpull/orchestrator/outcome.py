# src/fleetpull/orchestrator/outcome.py
"""The run executor's result carrier: what one endpoint run produced.

A frozen tagged union the run executor returns instead of ``None``, so the caller
(the orchestration entry, ``entry.py``) dispatches on the outcome rather than
inferring it. ``Executed`` carries the fetched-row count and the write report;
``CaughtUp`` is the no-op marker for a run whose resume window resolved to
nothing -- no fetch, no writer, no ledger row. ``CaughtUp`` is reachable only on
the watermark arm, whose window resolution can find nothing to drive; a snapshot
always executes.
"""

from dataclasses import dataclass
from datetime import datetime

from fleetpull.storage import WriteResult

__all__: list[str] = ['CaughtUp', 'Executed', 'RunOutcome']


@dataclass(frozen=True, slots=True)
class Executed:
    """A run that fetched and wrote.

    Attributes:
        records_fetched: The count of records fetched across the run -- the
            ledger's row count, distinct from ``write.rows_written`` (which dedup
            and partitioning can make a different number).
        write: The storage layer's write report for the run.
        latest_observed: The folded in-window maximum event time, or ``None``
            when the run observed no in-window event (an empty unit) or has no
            event-time axis (a snapshot). The prefix-advance watermark rule's
            per-unit datum (DESIGN section 5): the unit loop records it at
            ``mark_done`` and the watermark advances only across the
            contiguous done-prefix -- never here, never per-unit in isolation.
    """

    records_fetched: int
    write: WriteResult
    latest_observed: datetime | None = None


@dataclass(frozen=True, slots=True)
class CaughtUp:
    """A run that did nothing because its resume window resolved to empty.

    No fetch, no writer, no ledger row; carries no fields (the run executor logs the
    detail). Reachable only on the watermark arm, whose window resolution can
    resolve to nothing.
    """


type RunOutcome = Executed | CaughtUp
