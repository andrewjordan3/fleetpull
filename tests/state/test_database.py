# tests/state/test_database.py
"""Tests for fleetpull.state.database."""

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.state.database import (
    _APPLICATION_ID,
    _DEFAULT_BUSY_TIMEOUT_MS,
    SqliteScalar,
    StateDatabase,
    _apply_connection_pragmas,
    _enable_wal,
    _stamp_or_verify_application_id,
    _verify_quick_check,
    fetch_scalar,
)

# The path argument to the verify primitives reaches only the ConfigurationError
# detail; the function-level tests assert on the message, so any path serves.
_SAMPLE_PATH = Path('state.sqlite3')


@pytest.fixture
def memory_connection() -> Iterator[sqlite3.Connection]:
    """An open in-memory SQLite connection, closed on teardown."""
    connection = sqlite3.connect(':memory:')
    try:
        yield connection
    finally:
        connection.close()


def _read_pragma(database_path: Path, pragma: str) -> SqliteScalar:
    """Open the database file, read a single-value PRAGMA, and return it."""
    connection = sqlite3.connect(database_path)
    try:
        row: tuple[SqliteScalar, ...] | None = connection.execute(
            f'PRAGMA {pragma}'
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return row[0]


def _open_and_close(state_database: StateDatabase) -> None:
    """Enter and immediately exit a connection context (drives connect's guard)."""
    with state_database.connect():
        pass


class TestFetchScalar:
    def test_returns_the_single_value(
        self, memory_connection: sqlite3.Connection
    ) -> None:
        assert fetch_scalar(memory_connection, 'SELECT 42') == 42

    def test_raises_when_no_row(self, memory_connection: sqlite3.Connection) -> None:
        memory_connection.execute('CREATE TABLE empty_probe (value INTEGER)')
        with pytest.raises(RuntimeError, match='one row'):
            fetch_scalar(memory_connection, 'SELECT value FROM empty_probe')


class TestApplyConnectionPragmas:
    def test_sets_the_busy_timeout(self, memory_connection: sqlite3.Connection) -> None:
        busy_timeout_ms = 1234
        _apply_connection_pragmas(memory_connection, busy_timeout_ms)
        actual = fetch_scalar(memory_connection, 'PRAGMA busy_timeout')
        assert actual == busy_timeout_ms

    def test_enables_foreign_keys(self, memory_connection: sqlite3.Connection) -> None:
        _apply_connection_pragmas(memory_connection, _DEFAULT_BUSY_TIMEOUT_MS)
        assert fetch_scalar(memory_connection, 'PRAGMA foreign_keys') == 1


class TestStampOrVerifyApplicationId:
    def test_stamps_a_fresh_database(
        self, memory_connection: sqlite3.Connection
    ) -> None:
        _stamp_or_verify_application_id(memory_connection, _SAMPLE_PATH)
        stamped = fetch_scalar(memory_connection, 'PRAGMA application_id')
        assert stamped == _APPLICATION_ID

    def test_accepts_an_already_stamped_database(
        self, memory_connection: sqlite3.Connection
    ) -> None:
        _stamp_or_verify_application_id(memory_connection, _SAMPLE_PATH)
        _stamp_or_verify_application_id(memory_connection, _SAMPLE_PATH)
        stamped = fetch_scalar(memory_connection, 'PRAGMA application_id')
        assert stamped == _APPLICATION_ID

    def test_refuses_a_foreign_application_id(
        self, memory_connection: sqlite3.Connection
    ) -> None:
        memory_connection.execute('PRAGMA application_id = 12345')
        with pytest.raises(ConfigurationError, match='another application'):
            _stamp_or_verify_application_id(memory_connection, _SAMPLE_PATH)


class TestEnableWal:
    def test_converts_to_wal_on_local_disk(self, database_path: Path) -> None:
        connection = sqlite3.connect(database_path)
        try:
            _enable_wal(connection, database_path)
            mode = fetch_scalar(connection, 'PRAGMA journal_mode')
        finally:
            connection.close()
        assert mode == 'wal'

    def test_refuses_when_wal_does_not_take(
        self, memory_connection: sqlite3.Connection
    ) -> None:
        with (
            patch('fleetpull.state.database.fetch_scalar', return_value='delete'),
            pytest.raises(ConfigurationError, match='filesystem'),
        ):
            _enable_wal(memory_connection, _SAMPLE_PATH)


class TestVerifyQuickCheck:
    def test_passes_on_a_healthy_database(
        self, memory_connection: sqlite3.Connection
    ) -> None:
        _verify_quick_check(memory_connection, _SAMPLE_PATH)

    def test_refuses_a_corrupt_database(
        self, memory_connection: sqlite3.Connection
    ) -> None:
        with (
            patch('fleetpull.state.database.fetch_scalar', return_value='malformed'),
            pytest.raises(ConfigurationError, match='integrity'),
        ):
            _verify_quick_check(memory_connection, _SAMPLE_PATH)


class TestDatabasePath:
    def test_database_path_echoes_the_constructed_path(
        self, database_path: Path
    ) -> None:
        assert StateDatabase(database_path).database_path == database_path


class TestInitialize:
    def test_creates_the_parent_directory_when_absent(self, tmp_path: Path) -> None:
        database_path = tmp_path / '.fleetpull' / 'state.sqlite3'
        StateDatabase(database_path).initialize()
        assert database_path.parent.is_dir()
        assert database_path.is_file()

    def test_creates_the_database_file(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        assert database.database_path.is_file()

    def test_database_is_in_wal_mode(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        assert _read_pragma(database.database_path, 'journal_mode') == 'wal'

    def test_stamps_the_fleetpull_application_id(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        stamped = _read_pragma(database.database_path, 'application_id')
        assert stamped == _APPLICATION_ID

    def test_is_idempotent(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        database.initialize()
        stamped = _read_pragma(database.database_path, 'application_id')
        assert stamped == _APPLICATION_ID

    def test_refuses_a_foreign_application_id(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        foreign_application_id = 12345
        preexisting = sqlite3.connect(database.database_path)
        try:
            preexisting.execute(f'PRAGMA application_id = {foreign_application_id}')
            preexisting.execute('CREATE TABLE marker (id INTEGER)')
            preexisting.commit()
        finally:
            preexisting.close()
        with pytest.raises(ConfigurationError, match='another application'):
            database.initialize()

    def test_surfaces_a_non_sqlite_file(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.database_path.write_bytes(b'this is not a sqlite database')
        with pytest.raises(sqlite3.DatabaseError):
            database.initialize()


class TestConnect:
    def test_yields_a_working_connection(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        with database.connect() as connection:
            value = fetch_scalar(connection, 'SELECT 1')
        assert value == 1

    def test_applies_the_default_busy_timeout(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        with database.connect() as connection:
            busy_timeout = fetch_scalar(connection, 'PRAGMA busy_timeout')
        assert busy_timeout == _DEFAULT_BUSY_TIMEOUT_MS

    def test_applies_a_custom_busy_timeout(self, database_path: Path) -> None:
        custom_busy_timeout_ms = 4321
        database = StateDatabase(database_path, busy_timeout_ms=custom_busy_timeout_ms)
        database.initialize()
        with database.connect() as connection:
            busy_timeout = fetch_scalar(connection, 'PRAGMA busy_timeout')
        assert busy_timeout == custom_busy_timeout_ms

    def test_raises_when_not_initialized(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        with pytest.raises(RuntimeError, match='initialize'):
            _open_and_close(database)

    def test_closes_the_connection_on_exit(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        with database.connect() as connection:
            captured_connection = connection
        with pytest.raises(sqlite3.ProgrammingError):
            captured_connection.execute('SELECT 1')


class TestRoundTrip:
    def test_write_and_read_through_a_connection(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        with database.connect() as connection:
            connection.execute('CREATE TABLE probe (value INTEGER)')
            connection.execute('INSERT INTO probe (value) VALUES (7)')
            connection.commit()
            stored = fetch_scalar(connection, 'SELECT value FROM probe')
        assert stored == 7

    def test_a_second_instance_verifies_the_existing_database(
        self, database_path: Path
    ) -> None:
        StateDatabase(database_path).initialize()
        reopened = StateDatabase(database_path)
        reopened.initialize()
        with reopened.connect() as connection:
            value = fetch_scalar(connection, 'SELECT 1')
        assert value == 1
        stamped = _read_pragma(reopened.database_path, 'application_id')
        assert stamped == _APPLICATION_ID


class TestTransaction:
    def test_commits_on_clean_exit(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        with database.transaction() as connection:
            connection.execute('CREATE TABLE probe (value INTEGER)')
            connection.execute('INSERT INTO probe (value) VALUES (7)')
        with database.connect() as connection:
            assert fetch_scalar(connection, 'SELECT value FROM probe') == 7

    def test_a_raise_discards_the_uncommitted_work(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        with database.transaction() as connection:
            connection.execute('CREATE TABLE probe (value INTEGER)')

        def insert_then_fail() -> None:
            with database.transaction() as connection:
                connection.execute('INSERT INTO probe (value) VALUES (7)')
                raise RuntimeError('mid-transaction failure')

        with pytest.raises(RuntimeError, match='mid-transaction'):
            insert_then_fail()
        with database.connect() as connection:
            assert fetch_scalar(connection, 'SELECT count(*) FROM probe') == 0
