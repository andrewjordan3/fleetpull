"""Schema installer for the pre-release operational state database.

Fresh databases install the complete current schema at head version 3. Earlier
pre-release development schemas (versions 1 and 2) are refused and must be
recreated; fleetpull has not shipped a stable state-database format yet.
"""

import logging
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Final

from fleetpull.exceptions import ConfigurationError
from fleetpull.state.database import SqliteScalar, StateDatabase, fetch_scalar

__all__: list[str] = ['HEAD_VERSION', 'migrate_to_head']

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _Migration:
    version: int
    apply: Callable[[sqlite3.Connection], None]


HEAD_VERSION: Final[int] = 3
_OBSOLETE_DEVELOPMENT_VERSIONS: Final[set[int]] = {1, 2}

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

_RUNS_TABLE_DDL: Final[str] = """
    CREATE TABLE runs (
        run_id          INTEGER PRIMARY KEY,
        provider        TEXT NOT NULL,
        endpoint        TEXT NOT NULL,
        status          TEXT NOT NULL CHECK (
            status IN ('running', 'succeeded', 'failed')
        ),
        mode            TEXT NOT NULL CHECK (
            mode IN ('snapshot', 'watermark', 'feed')
        ),
        window_start    TEXT,
        window_end      TEXT,
        bootstrap_start TEXT,
        from_version    TEXT,
        to_version      TEXT,
        row_count       INTEGER,
        started_at      TEXT NOT NULL,
        ended_at        TEXT,
        error_detail    TEXT,
        CHECK (
            (mode = 'snapshot'
                 AND window_start IS NULL AND window_end IS NULL
                 AND bootstrap_start IS NULL
                 AND from_version IS NULL AND to_version IS NULL)
            OR (mode = 'watermark'
                 AND window_start IS NOT NULL AND window_end IS NOT NULL
                 AND bootstrap_start IS NULL
                 AND from_version IS NULL AND to_version IS NULL)
            OR (mode = 'feed'
                 AND window_start IS NULL AND window_end IS NULL
                 AND ((bootstrap_start IS NOT NULL) != (from_version IS NOT NULL)))
        ),
        CHECK (mode != 'feed' OR status != 'succeeded' OR to_version IS NOT NULL),
        CHECK (row_count IS NULL OR row_count >= 0),
        CHECK (window_start IS NULL OR window_end IS NULL
                 OR window_start < window_end)
    ) STRICT
"""

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

_ROSTERS_TABLE_DDL: Final[str] = """
    CREATE TABLE rosters (
        provider      TEXT NOT NULL,
        name          TEXT NOT NULL,
        member        TEXT NOT NULL,
        absence_count INTEGER NOT NULL DEFAULT 0 CHECK (absence_count >= 0),
        PRIMARY KEY (provider, name, member)
    ) STRICT
"""


def _install_schema(connection: sqlite3.Connection) -> None:
    connection.execute(_CURSORS_TABLE_DDL)
    connection.execute(_RUNS_TABLE_DDL)
    connection.execute(_WORK_UNITS_TABLE_DDL)
    for index_ddl in _WORK_UNITS_INDEX_DDLS:
        connection.execute(index_ddl)
    connection.execute(_ROSTERS_TABLE_DDL)


_MIGRATIONS: tuple[_Migration, ...] = (
    _Migration(version=HEAD_VERSION, apply=_install_schema),
)


def migrate_to_head(state_database: StateDatabase) -> None:
    """Install or verify the current pre-release schema."""
    with state_database.connect() as connection:
        connection.isolation_level = None
        current_version = _read_user_version(connection)
        if current_version == HEAD_VERSION:
            return
        if current_version in _OBSOLETE_DEVELOPMENT_VERSIONS:
            raise ConfigurationError(
                'state database uses an obsolete pre-release development schema',
                detail=(
                    f'database at {state_database.database_path} is at schema '
                    f'version {current_version}; delete and recreate it for '
                    f'fleetpull schema version {HEAD_VERSION}'
                ),
            )
        if current_version > HEAD_VERSION:
            raise ConfigurationError(
                'state database schema is newer than this version of fleetpull',
                detail=(
                    f'database at {state_database.database_path} is at schema '
                    f'version {current_version}, newer than this build '
                    f'understands (head {HEAD_VERSION}); upgrade fleetpull to '
                    f'operate on it'
                ),
            )
        if current_version != 0:
            raise ConfigurationError(
                'state database schema version is unsupported',
                detail=f'got schema version {current_version}',
            )
        migration = _MIGRATIONS[-1]
        with _transaction(connection):
            migration.apply(connection)
            connection.execute(f'PRAGMA user_version = {migration.version}')
        logger.info('Installed state schema: version=%d', migration.version)


@contextmanager
def _transaction(connection: sqlite3.Connection) -> Iterator[None]:
    connection.execute('BEGIN')
    committed = False
    try:
        yield
        connection.execute('COMMIT')
        committed = True
    finally:
        if not committed:
            connection.execute('ROLLBACK')


def _read_user_version(connection: sqlite3.Connection) -> int:
    version: SqliteScalar = fetch_scalar(connection, 'PRAGMA user_version')
    if not isinstance(version, int):
        raise RuntimeError(f'expected an integer user_version, got {version!r}')
    return version
