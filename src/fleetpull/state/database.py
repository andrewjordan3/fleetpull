# src/fleetpull/state/database.py
"""SQLite connection lifecycle and integrity verification for the operational state store.

The connection substrate beneath the `state/` package (DESIGN §5). One SQLite
database lives at the resolved path passed to :class:`StateDatabase` at
construction; this module does not derive that path (runtime config resolves it —
see the prompt context).

Layout is a functional core under a thin shell. The module-level functions
(:func:`fetch_scalar`, :func:`apply_connection_pragmas`,
:func:`stamp_or_verify_application_id`, :func:`enable_wal`,
:func:`verify_quick_check`) are stateless database primitives: each takes an
explicit ``connection`` and does one thing. :class:`StateDatabase` is the shell
that owns the path, creates the file, sequences the verification primitives at
startup, and hands out per-connection-configured connections. The primitives are
public (no leading underscore) because the schema/cursor/ledger layers built on
this substrate import and reuse them; keeping them out of the class keeps that
class from growing into a God object.

The module owns no tables and no transactions. Schema DDL, the schema-version
gate (``user_version``), transaction isolation, and the watermark/ledger/
work-unit representations all belong to the layers above and arrive in later
prompts. The module imports nothing about parquet (DESIGN §5/§11: state knows
nothing about parquet); its only internal dependency is the exception hierarchy.

Failure stances:
    - A path holding a different application's SQLite file (foreign
      ``application_id``), a corrupt database, or a filesystem that cannot
      support WAL (a non-local filesystem) all raise ``ConfigurationError`` — the
      operator must fix the local state store or its location, then rerun. A
      removed-because-corrupt store rebuilds on the next run via refetch
      (delete-by-window merge makes that idempotent, §5).
    - A path holding a file that is not a SQLite database at all surfaces the
      stdlib ``sqlite3.DatabaseError`` unchanged.
    - Calling ``connect`` before ``initialize`` is a wiring bug and raises stdlib
      ``RuntimeError`` — a caller bug, kept out of the operational hierarchy (§8).
"""

import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Final

from fleetpull.exceptions import ConfigurationError

__all__: list[str] = [
    'StateDatabase',
    'apply_connection_pragmas',
    'enable_wal',
    'fetch_scalar',
    'stamp_or_verify_application_id',
    'verify_quick_check',
]

logger = logging.getLogger(__name__)

# A single column value as SQLite returns it through the DBAPI: the storage
# classes map to exactly these Python types. Used to type the raw scalar read
# below precisely, rather than falling back to ``object``.
type _SqliteScalar = int | float | str | bytes | None

# Stamped into the SQLite header's application_id field at creation and verified
# on every initialization: a fixed nonzero magic marking a file as fleetpull's
# own state store, so a foreign SQLite file sharing the path is refused rather
# than written to. Value is the ASCII bytes b'flpl' (0x666C706C), an arbitrary
# but stable 32-bit identifier.
_APPLICATION_ID: Final[int] = 0x666C706C

# Short per-connection lock-wait before SQLite raises SQLITE_BUSY (DESIGN §5:
# short busy_timeout). A few seconds absorbs brief single-writer contention
# without masking a genuine deadlock.
_DEFAULT_BUSY_TIMEOUT_MS: Final[int] = 5000


def fetch_scalar(connection: sqlite3.Connection, statement: str) -> _SqliteScalar:
    """
    Execute a single-row, single-column statement and return its raw value.

    The shared read primitive beneath the verification helpers: it centralizes
    the fetch and the empty-result guard so callers narrow a known type rather
    than re-handling the cursor.

    Args:
        connection: The connection to execute against.
        statement: A statement expected to yield one row of one column.

    Returns:
        The single column value as SQLite returns it.

    Raises:
        RuntimeError: The statement returned no row — a contract violation for
            the pragmas read here, surfaced loudly rather than guessed around.

    Side Effects:
        Executes ``statement`` on ``connection``.
    """
    row: tuple[_SqliteScalar, ...] | None = connection.execute(statement).fetchone()
    if row is None:
        raise RuntimeError(f'expected one row from {statement!r}, got none')
    return row[0]


def apply_connection_pragmas(
    connection: sqlite3.Connection, busy_timeout_ms: int
) -> None:
    """
    Apply the connection-scoped pragmas every state connection needs.

    ``busy_timeout`` and ``foreign_keys`` are per-connection settings (they do
    not persist in the file header), so they are set on every connection rather
    than once at initialization.

    Args:
        connection: The connection to configure.
        busy_timeout_ms: Lock-wait in milliseconds before SQLite raises
            ``SQLITE_BUSY``.

    Side Effects:
        Issues ``PRAGMA`` statements on ``connection``.
    """
    connection.execute(f'PRAGMA busy_timeout = {busy_timeout_ms}')
    connection.execute('PRAGMA foreign_keys = ON')


def stamp_or_verify_application_id(
    connection: sqlite3.Connection, database_path: Path
) -> None:
    """
    Stamp a fresh database's ``application_id`` or verify an existing one.

    A brand-new SQLite file carries ``application_id = 0``; that case is stamped
    with the fleetpull magic. A file already carrying the magic is accepted. Any
    other nonzero value means the path holds a different application's SQLite
    database, refused rather than written to. Callers read this before any other
    write, so a foreign file is never mutated.

    Args:
        connection: An open connection to the database.
        database_path: The database's path, used only to identify the file in a
            raised ``ConfigurationError``.

    Raises:
        ConfigurationError: The database carries a foreign ``application_id``.

    Side Effects:
        Sets ``application_id`` in the file header on a fresh database.
    """
    current_id: _SqliteScalar = fetch_scalar(connection, 'PRAGMA application_id')
    if not isinstance(current_id, int):
        raise RuntimeError(f'expected an integer application_id, got {current_id!r}')
    if current_id == 0:
        connection.execute(f'PRAGMA application_id = {_APPLICATION_ID}')
        return
    if current_id != _APPLICATION_ID:
        raise ConfigurationError(
            'state database belongs to another application',
            detail=(
                f'{database_path} carries application_id {current_id:#010x}, '
                f'not the fleetpull application_id {_APPLICATION_ID:#010x}; '
                f'point the state database path at a fleetpull-owned location'
            ),
        )


def enable_wal(connection: sqlite3.Connection, database_path: Path) -> None:
    """
    Switch the database to WAL journaling and confirm it took.

    WAL is a database-level mode persisted in the file header, so callers run
    this once at initialization. The active mode is read back: SQLite silently
    falls back to the prior journal mode on a filesystem that cannot support WAL
    (a network filesystem), so a result other than ``'wal'`` is the loud signal
    that the database is not on local disk, which DESIGN §5 requires.

    Args:
        connection: An open connection to the database.
        database_path: The database's path, used only to identify the file in a
            raised ``ConfigurationError``.

    Raises:
        ConfigurationError: WAL did not take — the database is not on a local
            filesystem.

    Side Effects:
        May convert the database's journal mode (persists in the header).
    """
    active_mode: _SqliteScalar = fetch_scalar(connection, 'PRAGMA journal_mode = WAL')
    if not isinstance(active_mode, str):
        raise RuntimeError(f'expected a text journal_mode, got {active_mode!r}')
    if active_mode.lower() != 'wal':
        raise ConfigurationError(
            'state database is not on a WAL-capable filesystem',
            detail=(
                f'PRAGMA journal_mode=WAL returned {active_mode!r} for '
                f'{database_path}; SQLite operational state requires local disk '
                f'(DESIGN §5)'
            ),
        )


def verify_quick_check(connection: sqlite3.Connection, database_path: Path) -> None:
    """
    Run SQLite's integrity check and refuse a corrupt database.

    ``PRAGMA quick_check`` returns the single value ``'ok'`` on a healthy
    database; anything else is corruption. ``quick_check`` is chosen over
    ``integrity_check`` because it skips the expensive cross-index consistency
    pass while still detecting structural damage — adequate for a startup gate.

    Args:
        connection: An open connection to the database.
        database_path: The database's path, used only to identify the file in a
            raised ``ConfigurationError``.

    Raises:
        ConfigurationError: The integrity check did not return ``'ok'``.

    Side Effects:
        None beyond reading the database.
    """
    result: _SqliteScalar = fetch_scalar(connection, 'PRAGMA quick_check')
    if not isinstance(result, str):
        raise RuntimeError(f'expected a text quick_check result, got {result!r}')
    if result.lower() != 'ok':
        raise ConfigurationError(
            'state database failed its integrity check',
            detail=(
                f'PRAGMA quick_check on {database_path} returned {result!r}; the '
                f'operational state store is corrupt and must be restored or '
                f'removed (a removed store rebuilds on the next run via refetch)'
            ),
        )


class StateDatabase:
    """
    Owns one operational state database's creation and verification lifecycle.

    The database lives at the resolved path passed in at construction (DESIGN
    §5). This class is the thin lifecycle shell over the module's database
    primitives: :meth:`initialize` creates the file, stamps and verifies it, and
    converts it to WAL; :meth:`connect` hands out per-connection-configured
    connections. It holds only the path and the busy-timeout — the mechanics are
    the module-level functions above, and the schema, transactions, and path
    resolution belong to other layers.

    Threading: SQLite connections are not shared across threads, so each worker
    thread opens its own connection via :meth:`connect`. Verification and WAL
    conversion are one-time startup work (:meth:`initialize`), run once
    single-threaded before any worker connects.

    Args:
        database_path: Full, already-resolved path to the SQLite database file.
            Its parent directory is created on :meth:`initialize` if absent; the
            path itself is not derived from a dataset root here.
        busy_timeout_ms: Per-connection lock-wait in milliseconds before SQLite
            raises ``SQLITE_BUSY``. Defaults to a few seconds.
    """

    def __init__(
        self,
        database_path: Path,
        *,
        busy_timeout_ms: int = _DEFAULT_BUSY_TIMEOUT_MS,
    ) -> None:
        self._database_path: Path = database_path
        self._busy_timeout_ms: int = busy_timeout_ms

    @property
    def database_path(self) -> Path:
        """The resolved path to the SQLite database file."""
        return self._database_path

    def initialize(self) -> None:
        """
        Create-or-verify the state database; run once at startup, single-threaded.

        Creates the database's parent directory if absent, opens (creating the
        file on first run) a connection, and verifies the database in order:
        :func:`stamp_or_verify_application_id` (a foreign ``application_id`` is
        refused before any other write, so a non-fleetpull file is never
        mutated), then :func:`enable_wal`, then :func:`verify_quick_check`.

        Idempotent: a second call against an already-initialized database
        re-confirms the ``application_id``, WAL, and integrity, then returns.

        Side Effects:
            Creates the parent directory and the database file on first run;
            stamps ``application_id`` on a fresh database; converts the database
            to WAL mode (persists in the file header).

        Raises:
            ConfigurationError: The path holds a non-fleetpull SQLite file
                (foreign ``application_id``), the database is corrupt, or the
                filesystem does not support WAL (a non-local filesystem — DESIGN
                §5 requires local disk).
            sqlite3.DatabaseError: The path holds a file that is not a SQLite
                database at all.
        """
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        connection: sqlite3.Connection = sqlite3.connect(self._database_path)
        try:
            stamp_or_verify_application_id(connection, self._database_path)
            enable_wal(connection, self._database_path)
            verify_quick_check(connection, self._database_path)
        finally:
            connection.close()
        logger.info('State database ready: path=%s', self._database_path)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """
        Open a per-connection-configured connection to the existing database.

        Opens the database (which must already exist — call :meth:`initialize`
        once at startup first), applies the per-connection pragmas via
        :func:`apply_connection_pragmas`, yields the connection, and closes it on
        exit. Each thread that touches SQLite calls this for its own connection.

        Transaction policy (isolation mode, explicit ``BEGIN``/``COMMIT``) is
        deliberately not set here; it is owned by the write layers built on this
        substrate.

        Yields:
            A ready ``sqlite3.Connection`` with the per-connection pragmas
            applied.

        Raises:
            RuntimeError: The database file does not exist — :meth:`initialize`
                has not run. Surfaced loudly as a wiring bug rather than silently
                creating an unstamped database.

        Side Effects:
            Opens and closes a SQLite connection.
        """
        if not self._database_path.exists():
            raise RuntimeError(
                f'state database {self._database_path} does not exist; '
                f'call initialize() before connect()'
            )
        connection: sqlite3.Connection = sqlite3.connect(self._database_path)
        try:
            apply_connection_pragmas(connection, self._busy_timeout_ms)
            yield connection
        finally:
            connection.close()
