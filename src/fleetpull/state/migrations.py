# src/fleetpull/state/migrations.py
"""Schema migration runner for the operational state database.

Brings a state database's schema up to the current head version. The schema is
versioned with SQLite's ``user_version`` header field: a fresh database carries
``user_version = 0``, and each migration step raises it by one as it applies its
DDL. :func:`migrate_to_head` reads where a database is and applies every pending
step in order, so a database created by an earlier fleetpull (with fewer tables)
upgrades in place to the current schema — the path a developer's own state file
takes as new tables land across prompts.

Migrations run once at startup, single-threaded, AFTER
:meth:`StateDatabase.initialize` (which establishes WAL, the ``application_id``,
and integrity but deliberately leaves ``user_version`` alone). Each step is
atomic: its DDL and the ``user_version`` bump commit together or not at all, so a
crash mid-migration leaves the database at its prior version with the step
un-applied, and the next run retries cleanly. A database whose version is *newer*
than this code's head is refused — the code is older than the file and cannot
know the schema.

This module owns schema evolution only; reading and writing the rows of any table
(the ``cursors``, ``runs``, ``work_units``, and ``rosters`` tables created here)
belongs to the store layers built on top. Today the head is version 2: v1 is the
``cursors``, ``runs``, and ``work_units`` tables; v2 adds the ``rosters`` table.
"""

import logging
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Final

from fleetpull.exceptions import ConfigurationError
from fleetpull.state.database import SqliteScalar, StateDatabase, fetch_scalar

__all__: list[str] = ['migrate_to_head']

logger = logging.getLogger(__name__)

# The cursors table (schema v1): one row per (provider, endpoint), holding the
# tagged-union resume cursor (DESIGN §4/§5). ``kind`` discriminates the union
# member and is CHECK-constrained to the two valid values, so an unknown
# discriminator is refused at the boundary; ``value`` is the member's serialized
# form (the store layer owns that serialization); ``updated_at`` is the ISO
# instant the row was last written. STRICT enforces the declared column types,
# and the (provider, endpoint) primary key makes a cursor write a single-row
# upsert.
_CURSORS_TABLE_DDL: Final[str] = """
    CREATE TABLE cursors (
        provider    TEXT NOT NULL,
        endpoint    TEXT NOT NULL,
        kind        TEXT NOT NULL CHECK (
            kind IN ('date_watermark', 'feed_token')
        ),
        value       TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        PRIMARY KEY (provider, endpoint)
    ) STRICT
"""

# The runs table (joins schema v1): one row per fetch of one (provider, endpoint)
# in one of three sync modes — a snapshot (no range), a watermark window, or a feed
# version range — the operational record the run ledger reads and writes
# (DESIGN §5). A ``mode`` column records which, so the row is self-describing.
# ``run_id`` is the rowid alias, auto-assigned by the INSERT. The range columns,
# ``row_count``, ``ended_at``, and ``error_detail`` are nullable because a two-phase
# run fills them across its lifecycle: the mode's range shape at start, ``row_count``
# (and a feed run's ``to_version``) at completion, ``error_detail`` only on failure.
# Three table CHECKs are the DB-layer backstop behind the RunLedger API guards — a
# mode-keyed range shape (snapshot carries no range; watermark carries a window;
# feed carries ``from_version``; ``to_version`` is admissible only on a feed run, so
# a snapshot or watermark row carrying one is impossible), a non-negative
# ``row_count``, and a window ordered ``window_start < window_end``. The ``status``
# and ``mode`` columns carry their own value CHECKs. The window bounds compare
# lexically because ``to_iso8601`` emits a fixed-width, zero-padded, Z-suffixed
# form, making the TEXT comparison the chronological one — the same property
# ``coverage_frontier``'s ``max()`` relies on; do not loosen the codec format
# without revisiting both. STRICT enforces the declared column types.
_RUNS_TABLE_DDL: Final[str] = """
    CREATE TABLE runs (
        run_id        INTEGER PRIMARY KEY,
        provider      TEXT NOT NULL,
        endpoint      TEXT NOT NULL,
        status        TEXT NOT NULL CHECK (
            status IN ('running', 'succeeded', 'failed')
        ),
        mode          TEXT NOT NULL CHECK (
            mode IN ('snapshot', 'watermark', 'feed')
        ),
        window_start  TEXT,
        window_end    TEXT,
        from_version  TEXT,
        to_version    TEXT,
        row_count     INTEGER,
        started_at    TEXT NOT NULL,
        ended_at      TEXT,
        error_detail  TEXT,
        CHECK (
            (mode = 'snapshot'
                 AND window_start IS NULL AND window_end IS NULL
                 AND from_version IS NULL AND to_version IS NULL)
            OR (mode = 'watermark'
                 AND window_start IS NOT NULL AND window_end IS NOT NULL
                 AND from_version IS NULL AND to_version IS NULL)
            OR (mode = 'feed'
                 AND from_version IS NOT NULL
                 AND window_start IS NULL AND window_end IS NULL)
        ),
        CHECK (row_count IS NULL OR row_count >= 0),
        CHECK (window_start IS NULL OR window_end IS NULL
                 OR window_start < window_end)
    ) STRICT
"""

# The work_units table (joins schema v1): the backfill claim queue (DESIGN §5). One
# row per unit — a date chunk (``chunk_start``/``chunk_end``) of one
# (provider, endpoint), optionally with an opaque ``partition_key`` (a vehicle id,
# a driver id, ...; NULL for an unpartitioned endpoint, which the store never
# interprets). ``unit_id`` is the rowid alias. ``status`` defaults to ``pending``
# and ``attempt_count`` to 0, so enqueue inserts only the identity + chunk columns;
# ``claimed_at``/``finished_at``/``last_error`` fill across the claim lifecycle. The
# CHECKs are the DB-layer backstop behind the WorkUnitStore guards: a closed status
# set, a non-negative ``attempt_count``, and a chunk ordered
# ``chunk_start < chunk_end`` (lexical = chronological under ``to_iso8601``'s
# fixed-width Z-form). STRICT enforces the declared types.
_WORK_UNITS_TABLE_DDL: Final[str] = """
    CREATE TABLE work_units (
        unit_id       INTEGER PRIMARY KEY,
        provider      TEXT NOT NULL,
        endpoint      TEXT NOT NULL,
        partition_key TEXT,
        chunk_start   TEXT NOT NULL,
        chunk_end     TEXT NOT NULL,
        status        TEXT NOT NULL DEFAULT 'pending' CHECK (
            status IN ('pending', 'claimed', 'done', 'failed')
        ),
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
        claimed_at    TEXT,
        finished_at   TEXT,
        last_error    TEXT,
        CHECK (chunk_start < chunk_end)
    ) STRICT
"""

# Three indexes back the queue (all verified in-container). The two PARTIAL UNIQUE
# indexes give idempotent enqueue (``INSERT OR IGNORE`` on the natural key): SQLite
# treats NULL as distinct in a unique index, so one index covers the
# ``partition_key IS NOT NULL`` arm and a second covers the NULL arm — deduping
# unpartitioned units that a single ``UNIQUE(...)`` would miss. The natural key is
# the full window (``chunk_start`` AND ``chunk_end``), so a same-start/different-end
# unit is distinct — overlap/plan-consistency is the caller's concern, not the
# store's. The third index is the PARTIAL claim index: ``(provider, endpoint,
# unit_id)`` filtered to claimable statuses lets ``claim_next``'s ``ORDER BY
# unit_id`` run sort-free (completed units leave the index), ~2x faster than a sort
# over the full table at 20k units.
_WORK_UNITS_INDEX_DDLS: Final[tuple[str, ...]] = (
    """
    CREATE UNIQUE INDEX ux_work_units_partitioned
        ON work_units (provider, endpoint, partition_key, chunk_start, chunk_end)
        WHERE partition_key IS NOT NULL
    """,
    """
    CREATE UNIQUE INDEX ux_work_units_unpartitioned
        ON work_units (provider, endpoint, chunk_start, chunk_end)
        WHERE partition_key IS NULL
    """,
    """
    CREATE INDEX ix_work_units_claimable
        ON work_units (provider, endpoint, unit_id)
        WHERE status IN ('pending', 'failed')
    """,
)

# The rosters table (joins schema v2): the persisted fan-out roster members.
# One row per fan-out member (``member``) of one roster, identified by a
# ``RosterKey`` ``(provider, name)`` -- the per-vehicle id set ``vehicle_locations``
# fans out over, listed from the ``vehicles`` feeder and kept here so the fan-out
# reads the roster, never the feeder's output parquet (DESIGN §3/§5). The roster's
# source endpoint and column are not stored here; they live in its
# ``RosterDefinition``. ``absence_count`` is the consecutive-miss hysteresis counter
# the reconcile logic drives; the composite primary key makes a member a single-row
# upsert. STRICT enforces the declared types; the non-negative CHECK is the DB-layer
# backstop on the counter.
_ROSTERS_TABLE_DDL: Final[str] = """
    CREATE TABLE rosters (
        provider      TEXT NOT NULL,
        name          TEXT NOT NULL,
        member        TEXT NOT NULL,
        absence_count INTEGER NOT NULL DEFAULT 0 CHECK (absence_count >= 0),
        PRIMARY KEY (provider, name, member)
    ) STRICT
"""


@dataclass(frozen=True, slots=True)
class _Migration:
    """
    One ordered schema step: the version it produces and the change that gets there.

    Attributes:
        version: The ``user_version`` the database carries once this step has
            applied. Steps are authored in ascending, contiguous order from 1.
        apply: Applies the step's schema change to an open connection, inside the
            caller's transaction. Takes the connection and returns nothing.
    """

    version: int
    apply: Callable[[sqlite3.Connection], None]


def _create_cursors_table(connection: sqlite3.Connection) -> None:
    """
    Create the ``cursors`` table.

    Args:
        connection: An open connection, inside the migration's transaction.

    Side Effects:
        Executes ``CREATE TABLE`` on ``connection``.
    """
    connection.execute(_CURSORS_TABLE_DDL)


def _create_runs_table(connection: sqlite3.Connection) -> None:
    """
    Create the ``runs`` table.

    Args:
        connection: An open connection, inside the migration's transaction.

    Side Effects:
        Executes ``CREATE TABLE`` on ``connection``.
    """
    connection.execute(_RUNS_TABLE_DDL)


def _create_work_units_table(connection: sqlite3.Connection) -> None:
    """
    Create the ``work_units`` table and its three indexes.

    Args:
        connection: An open connection, inside the migration's transaction.

    Side Effects:
        Executes one ``CREATE TABLE`` and three ``CREATE INDEX`` statements on
        ``connection``.
    """
    connection.execute(_WORK_UNITS_TABLE_DDL)
    for index_ddl in _WORK_UNITS_INDEX_DDLS:
        connection.execute(index_ddl)


def _create_rosters_table(connection: sqlite3.Connection) -> None:
    """
    Create the ``rosters`` table (schema v2).

    Args:
        connection: An open connection, inside the migration's transaction.

    Side Effects:
        Executes ``CREATE TABLE`` on ``connection``.
    """
    connection.execute(_ROSTERS_TABLE_DDL)


def _create_initial_schema(connection: sqlite3.Connection) -> None:
    """
    Apply schema v1: create the initial tables — ``cursors``, ``runs``, ``work_units``.

    All three tables form the v1 head. No state database has applied an earlier
    schema (none exists anywhere), so each joins v1 here rather than arriving as a
    separate version bump.

    Args:
        connection: An open connection, inside the migration's transaction.

    Side Effects:
        Executes each table's ``CREATE TABLE`` (and the work-units indexes) on
        ``connection``.
    """
    _create_cursors_table(connection)
    _create_runs_table(connection)
    _create_work_units_table(connection)


# Ordered by ascending version; the last entry's version is the head the schema
# is migrated up to. A future schema change appends a new step.
_MIGRATIONS: Final[tuple[_Migration, ...]] = (
    _Migration(version=1, apply=_create_initial_schema),
    _Migration(version=2, apply=_create_rosters_table),
)


def migrate_to_head(state_database: StateDatabase) -> None:
    """
    Bring the state database's schema up to the current head version.

    Reads the database's ``user_version`` and applies every migration step above
    it, in order, each in its own transaction. Idempotent: a database already at
    head has no pending steps and is left untouched. Run once at startup,
    single-threaded, after :meth:`StateDatabase.initialize`.

    Args:
        state_database: The initialized state database to migrate;
            :meth:`StateDatabase.connect` supplies the connection.

    Raises:
        ConfigurationError: The database's ``user_version`` is newer than this
            code's head — the code is older than the database and cannot know its
            schema.
        sqlite3.DatabaseError: A migration's DDL failed; the offending step's
            transaction is rolled back, leaving the database at its prior version.

    Side Effects:
        Opens a connection, applies pending DDL, and advances ``user_version``.
    """
    head_version: int = _MIGRATIONS[-1].version
    with state_database.connect() as connection:
        # Take manual transaction control: under the default isolation an
        # explicit BEGIN raises once an implicit transaction is open, so each
        # step could not own a clean BEGIN/COMMIT. Autocommit mode lets the
        # step's DDL and its user_version bump commit atomically.
        connection.isolation_level = None
        current_version: int = _read_user_version(connection)
        if current_version > head_version:
            raise ConfigurationError(
                'state database schema is newer than this version of fleetpull',
                detail=(
                    f'database at {state_database.database_path} is at schema '
                    f'version {current_version}, newer than this build '
                    f'understands (head {head_version}); upgrade fleetpull to '
                    f'operate on it'
                ),
            )
        for migration in _MIGRATIONS:
            if migration.version <= current_version:
                continue
            _apply_migration(connection, migration)
            logger.info('Applied state schema migration: version=%d', migration.version)


def _apply_migration(connection: sqlite3.Connection, migration: _Migration) -> None:
    """
    Apply one migration step atomically: its DDL and the version bump together.

    Args:
        connection: An open connection in manual-transaction (autocommit) mode.
        migration: The step to apply.

    Side Effects:
        Runs the step's change and sets ``user_version`` within one transaction.
    """
    with _transaction(connection):
        migration.apply(connection)
        connection.execute(f'PRAGMA user_version = {migration.version}')


@contextmanager
def _transaction(connection: sqlite3.Connection) -> Iterator[None]:
    """
    Run the enclosed statements in one transaction; commit on success, else roll back.

    Requires the connection in manual-transaction (autocommit) mode. The commit
    is recorded before the ``finally``, so a failure anywhere in the block rolls
    back rather than leaving a half-applied step.

    Args:
        connection: An open connection in autocommit mode.

    Side Effects:
        Issues ``BEGIN`` and exactly one of ``COMMIT`` / ``ROLLBACK``.
    """
    connection.execute('BEGIN')
    committed: bool = False
    try:
        yield
        connection.execute('COMMIT')
        committed = True
    finally:
        if not committed:
            connection.execute('ROLLBACK')


def _read_user_version(connection: sqlite3.Connection) -> int:
    """
    Read the database's ``user_version`` header field as an ``int``.

    Args:
        connection: An open connection.

    Returns:
        The current ``user_version`` (0 on a fresh database).

    Raises:
        RuntimeError: The PRAGMA returned a non-integer — a SQLite contract
            violation, surfaced loudly.

    Side Effects:
        Reads ``PRAGMA user_version``.
    """
    version: SqliteScalar = fetch_scalar(connection, 'PRAGMA user_version')
    if not isinstance(version, int):
        raise RuntimeError(f'expected an integer user_version, got {version!r}')
    return version
