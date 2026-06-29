# src/fleetpull/state/rosters.py
"""The fan-out roster: the persisted set of fan-out keys per feeder source.

A ``rosters`` row is one fan-out key (``member``) sourced from one feeder identity
``(provider, source_endpoint, source_column)`` -- the per-vehicle id set
``vehicle_locations`` fans out over, listed from the ``vehicles`` endpoint and kept
here so the fan-out reads the roster, never the feeder's output parquet (the user's
product, not fleetpull's control plane). The keys are opaque strings; fleetpull never
maps a key back to a VIN -- the VIN rides the fan-out response and the mapping is
irrelevant to the call.

This module ships ``RosterDelta`` (a reconciliation result), the pure ``reconcile`` and
``is_roster_stale``, and ``RosterStore`` (the table's read/write orchestrator). The pure
functions sit beside their store, the same shape as the resolution helpers beside the
cursors: the import discipline permits a pure function in ``state/``, and keeping the
threshold and staleness logic next to the store it serves beats scattering it.
``RosterStore`` takes the feeder identity as ``(provider, source_endpoint,
source_column)`` primitives; the typed declaration (``RosterDefinition`` in the
``roster/`` leaf) carries those fields, and the orchestrator unpacks them.

Refresh is reconcile-then-apply: list the feeder, ``reconcile`` the listing against the
current roster into a three-set delta (reset, increment, evict), and ``apply`` it in one
transaction. The absence counter is hysteresis -- a key absent from one listing is not
dropped on the first miss, only after ``eviction_threshold`` consecutive misses -- and
append-only is the degenerate case (``eviction_threshold`` ``None`` evicts nothing).
With permanent, absent-means-empty keys (vehicle ids) the counter is an efficiency
mechanism, not a correctness one: it stops the fan-out paying for empty fetches over
long-retired vehicles, so the threshold is generous.
"""

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from fleetpull.state.database import SqliteScalar, StateDatabase
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'RosterDelta',
    'RosterStore',
    'is_roster_stale',
    'reconcile',
]

logger = logging.getLogger(__name__)


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
        to_increment = {m for m in absent if current[m] + 1 <= eviction_threshold}
        to_evict = {m for m in absent if current[m] + 1 > eviction_threshold}
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


_READ_COUNTS_SQL: Final[str] = """
SELECT member, absence_count FROM rosters
WHERE provider = ? AND source_endpoint = ? AND source_column = ?
"""

_READ_MEMBERS_SQL: Final[str] = """
SELECT member FROM rosters
WHERE provider = ? AND source_endpoint = ? AND source_column = ?
ORDER BY member
"""

_UPSERT_ZERO_SQL: Final[str] = """
INSERT INTO rosters
    (provider, source_endpoint, source_column, member, absence_count)
VALUES (?, ?, ?, ?, 0)
ON CONFLICT (provider, source_endpoint, source_column, member)
DO UPDATE SET absence_count = 0
"""

_INCREMENT_SQL: Final[str] = """
UPDATE rosters SET absence_count = absence_count + 1
WHERE provider = ? AND source_endpoint = ? AND source_column = ? AND member = ?
"""

_DELETE_SQL: Final[str] = """
DELETE FROM rosters
WHERE provider = ? AND source_endpoint = ? AND source_column = ? AND member = ?
"""


class RosterStore:
    """Read/write orchestrator for the ``rosters`` table.

    Translates between the fan-out's needs and ``rosters`` rows, scoped per feeder
    ``(provider, source_endpoint, source_column)``. Holds a ``StateDatabase`` and
    nothing else; runs after ``migrate_to_head`` (the ``rosters`` table must exist,
    schema v2). The feeder identity is three primitives, unpacked by the orchestrator
    from the roster declaration (``RosterDefinition``, the ``roster/`` leaf).
    """

    def __init__(self, database: StateDatabase) -> None:
        """Bind the store to a state database.

        Args:
            database: The state database whose ``rosters`` table this reads and writes;
                must already be migrated (schema v2).
        """
        self._database = database

    def read_counts(
        self, provider: Provider, source_endpoint: str, source_column: str
    ) -> dict[str, int]:
        """Read the roster as ``{member: absence_count}`` for ``reconcile``.

        Args:
            provider: The feeder's provider.
            source_endpoint: The feeder endpoint.
            source_column: The feeder frame column the keys come from.

        Returns:
            Every roster member mapped to its absence count; empty when the roster holds
            no rows for this feeder.

        Raises:
            RuntimeError: A row's ``member`` or ``absence_count`` came back the wrong
                type, violating the STRICT schema contract.

        Side Effects:
            Opens a connection and reads.
        """
        with self._database.connect() as connection:
            rows = connection.execute(
                _READ_COUNTS_SQL, (provider.value, source_endpoint, source_column)
            ).fetchall()
        counts: dict[str, int] = {}
        for row in rows:
            member: SqliteScalar = row[0]
            count: SqliteScalar = row[1]
            if not isinstance(member, str):
                raise RuntimeError(f'rosters.member was not text: {member!r}')
            if not isinstance(count, int):
                raise RuntimeError(
                    f'rosters.absence_count was not an integer: {count!r}'
                )
            counts[member] = count
        return counts

    def read_members(
        self, provider: Provider, source_endpoint: str, source_column: str
    ) -> list[str]:
        """Read the roster's members for the fan-out, ascending.

        Args:
            provider: The feeder's provider.
            source_endpoint: The feeder endpoint.
            source_column: The feeder frame column the keys come from.

        Returns:
            The roster members as strings, ordered ascending for a deterministic
            fan-out; empty when the roster holds no rows for this feeder.

        Raises:
            RuntimeError: A row's ``member`` came back non-text, violating the STRICT
                schema contract.

        Side Effects:
            Opens a connection and reads.
        """
        with self._database.connect() as connection:
            rows = connection.execute(
                _READ_MEMBERS_SQL, (provider.value, source_endpoint, source_column)
            ).fetchall()
        members: list[str] = []
        for row in rows:
            member: SqliteScalar = row[0]
            if not isinstance(member, str):
                raise RuntimeError(f'rosters.member was not text: {member!r}')
            members.append(member)
        return members

    def apply(
        self,
        provider: Provider,
        source_endpoint: str,
        source_column: str,
        delta: RosterDelta,
    ) -> None:
        """Apply a reconciliation delta in one transaction.

        Upserts ``to_zero`` at count zero, increments ``to_increment``, deletes
        ``to_evict`` -- all in one transaction, so a refresh is atomic. An empty
        set is a no-op batch; an all-empty delta touches nothing.

        Args:
            provider: The feeder's provider.
            source_endpoint: The feeder endpoint.
            source_column: The feeder frame column the keys come from.
            delta: The reset / increment / evict sets from ``reconcile``.

        Side Effects:
            Opens a connection, writes up to three statement batches, and commits.
        """
        scope = (provider.value, source_endpoint, source_column)
        with self._database.connect() as connection:
            connection.executemany(
                _UPSERT_ZERO_SQL, [(*scope, member) for member in delta.to_zero]
            )
            connection.executemany(
                _INCREMENT_SQL, [(*scope, member) for member in delta.to_increment]
            )
            connection.executemany(
                _DELETE_SQL, [(*scope, member) for member in delta.to_evict]
            )
            connection.commit()
