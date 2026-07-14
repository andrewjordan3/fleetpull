"""Tests for fleetpull.state.migrations."""

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.state.database import StateDatabase, fetch_scalar
from fleetpull.state.migrations import (
    _MIGRATIONS,
    _Migration,
    _transaction,
    migrate_to_head,
)


def _database_path(directory: Path) -> Path:
    return directory / 'state.sqlite3'


def _set_user_version(database_path: Path, version: int) -> None:
    """Stamp ``user_version`` on the database via an out-of-band connection."""
    connection = sqlite3.connect(database_path)
    connection.isolation_level = None
    try:
        connection.execute(f'PRAGMA user_version = {version}')
    finally:
        connection.close()


def _expect_insert_rejected(
    tmp_path: Path,
    table: str,
    columns: str,
    values: tuple[str | int | float | bytes | None, ...],
) -> None:
    """Migrate a fresh database, then assert a raw INSERT trips a CHECK."""
    database = StateDatabase(_database_path(tmp_path))
    database.initialize()
    migrate_to_head(database)
    placeholders = ', '.join('?' * len(values))
    with (
        database.connect() as connection,
        pytest.raises(sqlite3.IntegrityError, match='CHECK'),
    ):
        connection.execute(
            f'INSERT INTO {table} {columns} VALUES ({placeholders})', values
        )


@pytest.fixture
def autocommit_connection() -> Iterator[sqlite3.Connection]:
    """A bare in-memory connection in manual-transaction mode, as _transaction requires."""
    connection = sqlite3.connect(':memory:')
    connection.isolation_level = None
    try:
        yield connection
    finally:
        connection.close()


class TestMigrateToHead:
    def test_brings_fresh_database_to_head(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()

        migrate_to_head(database)

        with database.connect() as connection:
            version = fetch_scalar(connection, 'PRAGMA user_version')
        assert version == _MIGRATIONS[-1].version

    def test_creates_the_cursors_table(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()

        migrate_to_head(database)

        with database.connect() as connection:
            columns = {
                row[1]
                for row in connection.execute('PRAGMA table_info(cursors)').fetchall()
            }
        assert columns == {'provider', 'endpoint', 'kind', 'value', 'updated_at'}

    def test_cursors_table_rejects_an_unknown_kind(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()
        migrate_to_head(database)

        with (
            database.connect() as connection,
            pytest.raises(sqlite3.IntegrityError, match='CHECK'),
        ):
            connection.execute(
                'INSERT INTO cursors VALUES (?, ?, ?, ?, ?)',
                ('motive', 'vehicles', 'not_a_kind', 'v', '2026-06-16T00:00:00Z'),
            )

    def test_creates_the_runs_table(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()

        migrate_to_head(database)

        with database.connect() as connection:
            columns = {
                row[1]
                for row in connection.execute('PRAGMA table_info(runs)').fetchall()
            }
        assert columns == {
            'run_id',
            'provider',
            'endpoint',
            'status',
            'mode',
            'window_start',
            'window_end',
            'bootstrap_start',
            'from_version',
            'to_version',
            'row_count',
            'started_at',
            'ended_at',
            'error_detail',
        }

    def test_runs_table_rejects_an_unknown_status(self, tmp_path: Path) -> None:
        _expect_insert_rejected(
            tmp_path,
            'runs',
            '(provider, endpoint, status, mode, window_start, window_end, started_at)',
            (
                'samsara',
                'trips',
                'bogus',
                'watermark',
                '2026-06-01T00:00:00Z',
                '2026-06-02T00:00:00Z',
                '2026-06-16T00:00:00Z',
            ),
        )

    def test_runs_table_rejects_both_arms(self, tmp_path: Path) -> None:
        _expect_insert_rejected(
            tmp_path,
            'runs',
            '(provider, endpoint, status, mode, window_start, window_end, '
            'from_version, started_at)',
            (
                'samsara',
                'trips',
                'running',
                'watermark',
                '2026-06-01T00:00:00Z',
                '2026-06-02T00:00:00Z',
                'v0',
                '2026-06-16T00:00:00Z',
            ),
        )

    def test_runs_table_rejects_to_version_on_a_watermark_run(
        self, tmp_path: Path
    ) -> None:
        _expect_insert_rejected(
            tmp_path,
            'runs',
            '(provider, endpoint, status, mode, window_start, window_end, '
            'to_version, started_at)',
            (
                'samsara',
                'trips',
                'succeeded',
                'watermark',
                '2026-06-01T00:00:00Z',
                '2026-06-02T00:00:00Z',
                'v9',
                '2026-06-16T00:00:00Z',
            ),
        )

    def test_runs_table_rejects_a_negative_row_count(self, tmp_path: Path) -> None:
        _expect_insert_rejected(
            tmp_path,
            'runs',
            '(provider, endpoint, status, mode, window_start, window_end, '
            'row_count, started_at)',
            (
                'samsara',
                'trips',
                'succeeded',
                'watermark',
                '2026-06-01T00:00:00Z',
                '2026-06-02T00:00:00Z',
                -1,
                '2026-06-16T00:00:00Z',
            ),
        )

    def test_runs_table_rejects_an_inverted_window(self, tmp_path: Path) -> None:
        _expect_insert_rejected(
            tmp_path,
            'runs',
            '(provider, endpoint, status, mode, window_start, window_end, started_at)',
            (
                'samsara',
                'trips',
                'succeeded',
                'watermark',
                '2026-06-02T00:00:00Z',
                '2026-06-01T00:00:00Z',
                '2026-06-16T00:00:00Z',
            ),
        )

    def test_creates_the_work_units_table(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()

        migrate_to_head(database)

        with database.connect() as connection:
            columns = {
                row[1]
                for row in connection.execute(
                    'PRAGMA table_info(work_units)'
                ).fetchall()
            }
        assert columns == {
            'unit_id',
            'provider',
            'endpoint',
            'partition_key',
            'chunk_start',
            'chunk_end',
            'status',
            'attempt_count',
            'claimed_at',
            'finished_at',
            'last_error',
        }

    def test_work_units_rejects_an_unknown_status(self, tmp_path: Path) -> None:
        _expect_insert_rejected(
            tmp_path,
            'work_units',
            '(provider, endpoint, chunk_start, chunk_end, status)',
            (
                'samsara',
                'trips',
                '2026-06-01T00:00:00Z',
                '2026-06-02T00:00:00Z',
                'bogus',
            ),
        )

    def test_work_units_rejects_a_negative_attempt_count(self, tmp_path: Path) -> None:
        _expect_insert_rejected(
            tmp_path,
            'work_units',
            '(provider, endpoint, chunk_start, chunk_end, attempt_count)',
            (
                'samsara',
                'trips',
                '2026-06-01T00:00:00Z',
                '2026-06-02T00:00:00Z',
                -1,
            ),
        )

    def test_work_units_rejects_an_inverted_chunk(self, tmp_path: Path) -> None:
        _expect_insert_rejected(
            tmp_path,
            'work_units',
            '(provider, endpoint, chunk_start, chunk_end)',
            (
                'samsara',
                'trips',
                '2026-06-02T00:00:00Z',
                '2026-06-01T00:00:00Z',
            ),
        )

    def test_work_units_dedups_a_null_partition_key(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()
        migrate_to_head(database)
        insert = (
            'INSERT INTO work_units (provider, endpoint, chunk_start, chunk_end) '
            'VALUES (?, ?, ?, ?)'
        )
        values = ('samsara', 'trips', '2026-06-01T00:00:00Z', '2026-06-02T00:00:00Z')
        with database.connect() as connection:
            connection.execute(insert, values)
            with pytest.raises(sqlite3.IntegrityError, match='UNIQUE'):
                connection.execute(insert, values)

    def test_work_units_dedups_a_non_null_partition_key(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()
        migrate_to_head(database)
        insert = (
            'INSERT INTO work_units '
            '(provider, endpoint, partition_key, chunk_start, chunk_end) '
            'VALUES (?, ?, ?, ?, ?)'
        )
        values = (
            'samsara',
            'trips',
            'V1',
            '2026-06-01T00:00:00Z',
            '2026-06-02T00:00:00Z',
        )
        with database.connect() as connection:
            connection.execute(insert, values)
            with pytest.raises(sqlite3.IntegrityError, match='UNIQUE'):
                connection.execute(insert, values)

    def test_work_units_allows_same_start_different_end(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()
        migrate_to_head(database)
        insert = (
            'INSERT INTO work_units '
            '(provider, endpoint, partition_key, chunk_start, chunk_end) '
            'VALUES (?, ?, ?, ?, ?)'
        )
        with database.connect() as connection:
            connection.execute(
                insert,
                (
                    'samsara',
                    'trips',
                    'V1',
                    '2026-06-01T00:00:00Z',
                    '2026-06-02T00:00:00Z',
                ),
            )
            connection.execute(
                insert,
                (
                    'samsara',
                    'trips',
                    'V1',
                    '2026-06-01T00:00:00Z',
                    '2026-06-03T00:00:00Z',
                ),
            )
            connection.commit()
            same_start_count = connection.execute(
                'SELECT count(*) FROM work_units WHERE chunk_start = ?',
                ('2026-06-01T00:00:00Z',),
            ).fetchone()
        assert same_start_count == (2,)

    def test_creates_the_rosters_table(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()

        migrate_to_head(database)

        with database.connect() as connection:
            columns = {
                row[1]
                for row in connection.execute('PRAGMA table_info(rosters)').fetchall()
            }
        assert columns == {
            'provider',
            'name',
            'member',
            'absence_count',
        }

    def test_rosters_rejects_a_negative_absence_count(self, tmp_path: Path) -> None:
        _expect_insert_rejected(
            tmp_path,
            'rosters',
            '(provider, name, member, absence_count)',
            ('motive', 'vehicle_ids', 'V1', -1),
        )

    def test_is_idempotent(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()

        migrate_to_head(database)
        migrate_to_head(database)  # second run: no pending steps, no error

        with database.connect() as connection:
            version = fetch_scalar(connection, 'PRAGMA user_version')
        assert version == _MIGRATIONS[-1].version

    def test_refuses_a_future_schema_version(self, tmp_path: Path) -> None:
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()
        _set_user_version(database.database_path, _MIGRATIONS[-1].version + 1)

        with pytest.raises(ConfigurationError, match='newer'):
            migrate_to_head(database)

    def test_failing_migration_rolls_back_atomically(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        def boom(connection: sqlite3.Connection) -> None:
            connection.execute('CREATE TABLE half_applied (x INTEGER)')
            raise RuntimeError('deliberate migration failure')

        monkeypatch.setattr(
            'fleetpull.state.migrations._MIGRATIONS',
            (_Migration(version=1, apply=boom),),
        )
        database = StateDatabase(_database_path(tmp_path))
        database.initialize()

        with pytest.raises(RuntimeError, match='deliberate migration failure'):
            migrate_to_head(database)

        with database.connect() as connection:
            version = fetch_scalar(connection, 'PRAGMA user_version')
            leftover = connection.execute(
                'SELECT name FROM sqlite_master '
                "WHERE type='table' AND name='half_applied'"
            ).fetchall()
        assert version == 0
        assert leftover == []


def _raise_inside_transaction(connection: sqlite3.Connection) -> None:
    """Run a failing statement inside ``_transaction`` to drive its rollback path."""
    with _transaction(connection):
        connection.execute('CREATE TABLE probe (x INTEGER)')
        raise RuntimeError('boom')


class TestTransaction:
    def test_commits_on_success(
        self, autocommit_connection: sqlite3.Connection
    ) -> None:
        with _transaction(autocommit_connection):
            autocommit_connection.execute('CREATE TABLE probe (x INTEGER)')

        present = autocommit_connection.execute(
            "SELECT name FROM sqlite_master WHERE name='probe'"
        ).fetchall()
        assert present == [('probe',)]

    def test_rolls_back_on_error(
        self, autocommit_connection: sqlite3.Connection
    ) -> None:
        with pytest.raises(RuntimeError, match='boom'):
            _raise_inside_transaction(autocommit_connection)

        absent = autocommit_connection.execute(
            "SELECT name FROM sqlite_master WHERE name='probe'"
        ).fetchall()
        assert absent == []
