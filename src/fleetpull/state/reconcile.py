# src/fleetpull/state/reconcile.py
"""The pure roster-reconciliation half: the delta, the reconcile, the staleness.

The listing-to-delta logic behind a roster refresh, split from the store it
feeds (``state/rosters.py``) so the pure set arithmetic and the SQLite
read/write orchestration live one concern per file. Refresh is
reconcile-then-apply: list the feeder, ``reconcile`` the listing against the
current roster into a three-set delta (reset, increment, evict), and
``RosterStore.apply`` it in one transaction. The absence counter is hysteresis
-- a key absent from one listing is not dropped on the first miss, only after
``eviction_threshold`` consecutive misses -- and append-only is the degenerate
case (``eviction_threshold`` ``None`` evicts nothing). With permanent,
absent-means-empty keys (vehicle ids) the counter is an efficiency mechanism,
not a correctness one: it stops the fan-out paying for empty fetches over
long-retired vehicles, so the threshold is generous. ``is_roster_stale`` is
the refresh-due decision the coordinator takes against the feeder's
ledger-recorded last success.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

__all__: list[str] = [
    'RosterDelta',
    'is_roster_stale',
    'reconcile',
]


@dataclass(frozen=True, slots=True)
class RosterDelta:
    """A reconciliation result: the three roster mutations a refresh applies.

    The disjoint sets one refresh produces -- the output of ``reconcile`` and the input
    to ``RosterStore.apply``. A member present at count zero and still listed appears in
    none of the three: no write is needed.

    Attributes:
        to_zero: Members to upsert at absence-count zero -- new keys (insert) and
            reappeared keys that had a nonzero count (reset).
        to_increment: Roster members absent from the listing whose incremented count
            stays at or below the eviction threshold -- a tolerated miss.
        to_evict: Roster members absent from the listing whose incremented count would
            exceed the threshold -- dropped from the roster.
    """

    to_zero: frozenset[str]
    to_increment: frozenset[str]
    to_evict: frozenset[str]


def reconcile(
    current: Mapping[str, int],
    listed: Iterable[str],
    eviction_threshold: int | None,
) -> RosterDelta:
    """Reconcile a fresh listing against the current roster into a delta.

    Pure set logic over the current roster (``{member: absence_count}``) and the
    freshly-listed keys. Listed keys reset to zero (new ones insert at zero); keys
    absent from the listing increment, and tip to eviction once the incremented count
    would exceed ``eviction_threshold``. ``None`` threshold is append-only -- nothing
    ever evicts. The threshold lives here so ``RosterStore.apply`` stays a dumb writer.

    Args:
        current: The roster as ``{member: absence_count}`` (from ``read_counts``).
        listed: The keys the feeder just produced (``extract_roster_members``);
            duplicates collapse.
        eviction_threshold: Consecutive-miss count past which a member is evicted, or
            ``None`` for append-only (never evict).

    Returns:
        The ``RosterDelta`` of disjoint reset / increment / evict sets.

    Side Effects:
        None -- pure function.
    """
    listed_set = set(listed)
    current_set = set(current)
    new = listed_set - current_set
    reappeared = {member for member in listed_set & current_set if current[member]}
    absent = current_set - listed_set
    to_increment: set[str]
    to_evict: set[str]
    if eviction_threshold is None:
        to_increment = absent
        to_evict = set()
    else:
        to_increment = {
            member for member in absent if current[member] + 1 <= eviction_threshold
        }
        to_evict = {
            member for member in absent if current[member] + 1 > eviction_threshold
        }
    return RosterDelta(
        to_zero=frozenset(new | reappeared),
        to_increment=frozenset(to_increment),
        to_evict=frozenset(to_evict),
    )


def is_roster_stale(
    last_success: datetime | None, now: datetime, max_age: timedelta
) -> bool:
    """Whether the roster is older than its staleness bound and a refresh is due.

    Pure decision from the feeder's last successful run time
    (``RunLedger.last_success_at``) against ``now`` and the allowed ``max_age``.
    ``None`` -- the feeder has never succeeded, so no roster has been built -- is stale.
    The caller treats this as best-effort: a stale verdict triggers a refresh attempt,
    but a failed refresh falls back to the existing roster rather than blocking the
    fan-out (the cold-start-empty case is the orchestrator's loud failure, not this
    function's).

    Args:
        last_success: When the feeder last completed successfully, or ``None``.
        now: The current instant (the caller's ``Clock``).
        max_age: The maximum tolerated roster age before a refresh is due.

    Returns:
        ``True`` when a refresh is due (no prior success, or older than ``max_age``).

    Side Effects:
        None -- pure function.
    """
    return last_success is None or now - last_success > max_age
