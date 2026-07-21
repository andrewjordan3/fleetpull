# src/fleetpull/state/rosters.py
"""The fan-out roster store: the persisted set of fan-out keys, keyed by ``RosterKey``.

A ``rosters`` row is one member (``member``) of one roster, identified by a
``RosterKey`` ``(provider, name)`` -- the per-vehicle id set ``vehicle_locations``
fans out over, listed from the ``vehicles`` feeder and kept here so the fan-out reads
the roster, never the feeder's output parquet (the user's product, not fleetpull's
control plane). The roster's source endpoint and column are not stored here; they live
in its ``RosterDefinition``. Members are opaque strings; fleetpull never maps one back
to a VIN -- the VIN rides the fan-out response and the mapping is irrelevant to the
call.

This module ships ``RosterStore``, the table's read/write orchestrator; the pure
reconciliation half it applies -- ``RosterDelta``, ``reconcile``,
``is_roster_stale`` -- lives beside it in ``state/reconcile.py``. Refresh is
reconcile-then-apply: list the feeder, ``reconcile`` the listing against the
current roster into a three-set delta (reset, increment, evict), and ``apply``
it in one transaction. ``RosterStore`` keys by ``RosterKey`` (the ``roster/``
leaf), which ``state`` may import since the leaf sits below it.
"""

from typing import Final

from fleetpull.roster import RosterKey
from fleetpull.state.database import (
    StateDatabase,
    expect_int,
    expect_text,
)
from fleetpull.state.reconcile import RosterDelta

__all__: list[str] = ['RosterStore']


_READ_COUNTS_SQL: Final[str] = """
SELECT member, absence_count FROM rosters
WHERE provider = ? AND name = ?
"""

_READ_MEMBERS_SQL: Final[str] = """
SELECT member FROM rosters
WHERE provider = ? AND name = ?
ORDER BY member
"""

_UPSERT_ZERO_SQL: Final[str] = """
INSERT INTO rosters (provider, name, member, absence_count)
VALUES (?, ?, ?, 0)
ON CONFLICT (provider, name, member)
DO UPDATE SET absence_count = 0
"""

_INCREMENT_SQL: Final[str] = """
UPDATE rosters SET absence_count = absence_count + 1
WHERE provider = ? AND name = ? AND member = ?
"""

_DELETE_SQL: Final[str] = """
DELETE FROM rosters
WHERE provider = ? AND name = ? AND member = ?
"""


class RosterStore:
    """Read/write orchestrator for the ``rosters`` table.

    Translates between the fan-out's needs and ``rosters`` rows, scoped per roster by
    ``RosterKey``. Holds a ``StateDatabase`` and nothing else; runs after
    ``migrate_to_head`` (the ``rosters`` table must exist). The store keys by
    ``RosterKey`` from the ``roster/`` leaf -- ``state`` may import the leaf, which
    sits below both ``state`` and ``endpoints``; the source endpoint and column live
    in the ``RosterDefinition``.
    """

    def __init__(self, database: StateDatabase) -> None:
        """Bind the store to a state database.

        Args:
            database: The state database whose ``rosters`` table this reads and writes;
                must already be migrated (the ``rosters`` table must exist).
        """
        self._database = database

    def read_counts(self, key: RosterKey) -> dict[str, int]:
        """Read the roster as ``{member: absence_count}`` for ``reconcile``.

        Args:
            key: The roster to read.

        Returns:
            Every roster member mapped to its absence count; empty when the roster
            holds no rows.

        Raises:
            RuntimeError: A row's ``member`` or ``absence_count`` came back the wrong
                type, violating the STRICT schema contract.

        Side Effects:
            Opens a connection and reads.
        """
        with self._database.connect() as connection:
            rows = connection.execute(
                _READ_COUNTS_SQL, (key.provider.value, key.name)
            ).fetchall()
        counts: dict[str, int] = {}
        for row in rows:
            counts[expect_text(row[0], 'rosters.member')] = expect_int(
                row[1], 'rosters.absence_count'
            )
        return counts

    def read_members(self, key: RosterKey) -> list[str]:
        """Read the roster's members for the fan-out, ascending.

        Args:
            key: The roster to read.

        Returns:
            The roster members as strings, ordered ascending for a deterministic
            fan-out; empty when the roster holds no rows.

        Raises:
            RuntimeError: A row's ``member`` came back non-text, violating the STRICT
                schema contract.

        Side Effects:
            Opens a connection and reads.
        """
        with self._database.connect() as connection:
            rows = connection.execute(
                _READ_MEMBERS_SQL, (key.provider.value, key.name)
            ).fetchall()
        return [expect_text(row[0], 'rosters.member') for row in rows]

    def apply(self, key: RosterKey, delta: RosterDelta) -> None:
        """Apply a reconciliation delta in one transaction.

        Upserts ``to_zero`` at count zero, increments ``to_increment``, deletes
        ``to_evict`` -- all in one transaction, so a refresh is atomic. An empty
        set is a no-op batch; an all-empty delta touches nothing.

        Args:
            key: The roster to apply the delta to.
            delta: The reset / increment / evict sets from ``reconcile``.

        Side Effects:
            Opens a connection, writes up to three statement batches, and commits.
        """
        scope = (key.provider.value, key.name)
        with self._database.transaction() as connection:
            connection.executemany(
                _UPSERT_ZERO_SQL, [(*scope, member) for member in delta.to_zero]
            )
            connection.executemany(
                _INCREMENT_SQL, [(*scope, member) for member in delta.to_increment]
            )
            connection.executemany(
                _DELETE_SQL, [(*scope, member) for member in delta.to_evict]
            )
