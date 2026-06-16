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
(the ``cursors`` table created here, the run ledger, work units) belongs to the
store layers built on top. Today the head is version 1: the ``cursors`` table.
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
    Apply schema v1: create the ``cursors`` table.

    Args:
        connection: An open connection, inside the migration's transaction.

    Side Effects:
        Executes ``CREATE TABLE`` on ``connection``.
    """
    connection.execute(_CURSORS_TABLE_DDL)


# Ordered by ascending version; the last entry's version is the head the schema
# is migrated up to. New tables (the run ledger, work units) append new steps.
_MIGRATIONS: Final[tuple[_Migration, ...]] = (
    _Migration(version=1, apply=_create_cursors_table),
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
