"""Tests for fleetpull.state.run_ledger."""

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import DateWindow
from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head
from fleetpull.state.run_ledger import RunLedger, RunMode, RunStatus
from fleetpull.timing.clock import FrozenClock
from fleetpull.timing.codec import to_iso8601
from fleetpull.vocabulary import Provider
from tests.state.conftest import FROZEN_INSTANT

WINDOW_START: datetime = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
WINDOW_END: datetime = datetime(2026, 6, 2, 0, 0, 0, tzinfo=UTC)

# The full runs column set, in declaration order; ``_read_run`` zips it against a
# raw row so assertions read columns by name.
_RUN_COLUMNS: tuple[str, ...] = (
    'run_id',
    'provider',
    'endpoint',
    'status',
    'mode',
    'window_start',
    'window_end',
    'from_version',
    'to_version',
    'row_count',
    'started_at',
    'ended_at',
    'error_detail',
)


def _read_run(
    database_path: Path, run_id: int
) -> dict[str, str | int | float | bytes | None]:
    """Read one run row by id via a bare connection, keyed by column name."""
    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute(
            f'SELECT {", ".join(_RUN_COLUMNS)} FROM runs WHERE run_id = ?',
            (run_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return dict(zip(_RUN_COLUMNS, row, strict=True))


def _insert_raw_succeeded_window_run(
    database_path: Path,
    provider: str,
    endpoint: str,
    window_start: str,
    window_end: str,
) -> None:
    """Insert a succeeded watermark run directly, bypassing the ledger."""
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            'INSERT INTO runs '
            '(provider, endpoint, status, mode, window_start, window_end, '
            'started_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            (
                provider,
                endpoint,
                'succeeded',
                'watermark',
                window_start,
                window_end,
                '2026-06-16T00:00:00Z',
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _insert_raw_run(
    database_path: Path,
    *,
    mode: str,
    window_start: str | None = None,
    window_end: str | None = None,
    from_version: str | None = None,
) -> None:
    """Insert a runs row directly, bypassing the ledger — for CHECK tests."""
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            'INSERT INTO runs '
            '(provider, endpoint, status, mode, window_start, window_end, '
            'from_version, started_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (
                'motive',
                'vehicles',
                'running',
                mode,
                window_start,
                window_end,
                from_version,
                '2026-06-16T00:00:00Z',
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _insert_raw_succeeded_snapshot_run(
    database_path: Path,
    provider: str,
    endpoint: str,
    ended_at: str,
) -> None:
    """Insert a succeeded snapshot run directly with a chosen ``ended_at``.

    A snapshot row carries no range, so the mode-keyed CHECK holds with the range
    columns left NULL; ``ended_at`` is plain TEXT the STRICT schema accepts, its value
    rejected only on read.
    """
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            'INSERT INTO runs '
            '(provider, endpoint, status, mode, started_at, ended_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (
                provider,
                endpoint,
                'succeeded',
                'snapshot',
                '2026-06-16T00:00:00Z',
                ended_at,
            ),
        )
        connection.commit()
    finally:
        connection.close()


@pytest.fixture
def run_ledger(database_path: Path, frozen_clock: FrozenClock) -> RunLedger:
    """A RunLedger over a freshly initialized, migrated state database."""
    database = StateDatabase(database_path)
    database.initialize()
    migrate_to_head(database)
    return RunLedger(database, frozen_clock)


class TestStartWindowRun:
    def test_inserts_a_running_watermark_row(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        run_id = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        assert isinstance(run_id, int)
        run = _read_run(database_path, run_id)
        assert run['status'] == RunStatus.RUNNING
        assert run['mode'] == RunMode.WATERMARK
        assert run['started_at'] == to_iso8601(FROZEN_INSTANT)
        assert run['window_start'] == to_iso8601(WINDOW_START)
        assert run['window_end'] == to_iso8601(WINDOW_END)
        assert run['from_version'] is None
        assert run['to_version'] is None
        assert run['row_count'] is None
        assert run['ended_at'] is None

    @pytest.mark.parametrize(
        ('window_start', 'window_end'),
        [
            (WINDOW_END, WINDOW_START),  # inverted: start after end
            (WINDOW_START, WINDOW_START),  # empty: start equals end
        ],
    )
    def test_a_non_increasing_window_is_unrepresentable(
        self, window_start: datetime, window_end: datetime
    ) -> None:
        # start_window_run takes a DateWindow, whose construction enforces
        # start < end -- the ledger's former ordering re-check moved to the
        # type, and the table's CHECK remains the structural backstop.
        with pytest.raises(ValueError, match='start < end'):
            DateWindow(start=window_start, end=window_end)


class TestStartFeedRun:
    def test_inserts_a_running_feed_row(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        run_id = run_ledger.start_feed_run(
            Provider.GEOTAB, 'log_records', from_version='v0'
        )
        run = _read_run(database_path, run_id)
        assert run['status'] == RunStatus.RUNNING
        assert run['mode'] == RunMode.FEED
        assert run['from_version'] == 'v0'
        assert run['window_start'] is None
        assert run['window_end'] is None
        assert run['to_version'] is None


class TestStartSnapshotRun:
    def test_inserts_a_running_snapshot_row_with_no_range(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        run_id = run_ledger.start_snapshot_run(Provider.MOTIVE, 'vehicles')
        run = _read_run(database_path, run_id)
        assert run['status'] == RunStatus.RUNNING
        assert run['mode'] == RunMode.SNAPSHOT
        assert run['window_start'] is None
        assert run['window_end'] is None
        assert run['from_version'] is None
        assert run['to_version'] is None


class TestCompleteRun:
    def test_watermark_completion_succeeds_and_stamps_the_advanced_clock(
        self, run_ledger: RunLedger, frozen_clock: FrozenClock, database_path: Path
    ) -> None:
        run_id = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        frozen_clock.advance(timedelta(hours=2))
        run_ledger.complete_run(run_id, row_count=42)
        run = _read_run(database_path, run_id)
        assert run['status'] == RunStatus.SUCCEEDED
        assert run['mode'] == RunMode.WATERMARK
        assert run['row_count'] == 42
        assert run['to_version'] is None
        assert run['ended_at'] == to_iso8601(FROZEN_INSTANT + timedelta(hours=2))

    def test_watermark_completion_rejects_a_to_version(
        self, run_ledger: RunLedger
    ) -> None:
        run_id = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        with pytest.raises(ValueError, match='to_version is only valid for feed runs'):
            run_ledger.complete_run(run_id, row_count=1, to_version='nope')

    def test_feed_completion_records_the_to_version(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        run_id = run_ledger.start_feed_run(Provider.GEOTAB, 'trips', from_version='v0')
        run_ledger.complete_run(run_id, row_count=5, to_version='v9')
        run = _read_run(database_path, run_id)
        assert run['status'] == RunStatus.SUCCEEDED
        assert run['mode'] == RunMode.FEED
        assert run['row_count'] == 5
        assert run['from_version'] == 'v0'
        assert run['to_version'] == 'v9'
        assert run['window_start'] is None

    def test_feed_completion_requires_a_to_version(self, run_ledger: RunLedger) -> None:
        run_id = run_ledger.start_feed_run(Provider.GEOTAB, 'trips', from_version='v0')
        with pytest.raises(ValueError, match='feed runs must record to_version'):
            run_ledger.complete_run(run_id, row_count=5)

    def test_snapshot_completion_succeeds_with_row_count(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        run_id = run_ledger.start_snapshot_run(Provider.MOTIVE, 'vehicles')
        run_ledger.complete_run(run_id, row_count=1300)
        run = _read_run(database_path, run_id)
        assert run['status'] == RunStatus.SUCCEEDED
        assert run['row_count'] == 1300
        assert run['to_version'] is None

    def test_snapshot_completion_rejects_a_to_version(
        self, run_ledger: RunLedger
    ) -> None:
        run_id = run_ledger.start_snapshot_run(Provider.MOTIVE, 'vehicles')
        with pytest.raises(ValueError, match='to_version is only valid for feed runs'):
            run_ledger.complete_run(run_id, row_count=1300, to_version='nope')

    def test_rejects_a_negative_row_count(self, run_ledger: RunLedger) -> None:
        run_id = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        with pytest.raises(ValueError, match='non-negative'):
            run_ledger.complete_run(run_id, row_count=-1)

    def test_rejects_an_unknown_run_id(self, run_ledger: RunLedger) -> None:
        with pytest.raises(ValueError, match='no run with run_id'):
            run_ledger.complete_run(999, row_count=0)


class TestFailRun:
    def test_marks_failed_with_detail_and_advanced_clock(
        self, run_ledger: RunLedger, frozen_clock: FrozenClock, database_path: Path
    ) -> None:
        run_id = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        frozen_clock.advance(timedelta(minutes=5))
        run_ledger.fail_run(run_id, error_detail='boom: read timeout')
        run = _read_run(database_path, run_id)
        assert run['status'] == RunStatus.FAILED
        assert run['error_detail'] == 'boom: read timeout'
        assert run['ended_at'] == to_iso8601(FROZEN_INSTANT + timedelta(minutes=5))

    def test_rejects_an_unknown_run_id(self, run_ledger: RunLedger) -> None:
        with pytest.raises(ValueError, match='no run with run_id'):
            run_ledger.fail_run(999, error_detail='whatever')


class TestCoverageFrontier:
    def test_returns_max_window_end_over_succeeded_watermark_runs(
        self, run_ledger: RunLedger
    ) -> None:
        earlier = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(
                start=datetime(2026, 6, 1, tzinfo=UTC),
                end=datetime(2026, 6, 2, tzinfo=UTC),
            ),
        )
        run_ledger.complete_run(earlier, row_count=1)
        later = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(
                start=datetime(2026, 6, 4, tzinfo=UTC),
                end=datetime(2026, 6, 5, tzinfo=UTC),
            ),
        )
        run_ledger.complete_run(later, row_count=2)

        frontier = run_ledger.coverage_frontier(Provider.SAMSARA, 'trips')
        assert frontier == datetime(2026, 6, 5, tzinfo=UTC)

    def test_ignores_running_failed_and_feed_runs(self, run_ledger: RunLedger) -> None:
        succeeded = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        run_ledger.complete_run(succeeded, row_count=1)
        # A running watermark run with a later window_end — not succeeded, ignored.
        run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(
                start=datetime(2026, 6, 9, tzinfo=UTC),
                end=datetime(2026, 6, 10, tzinfo=UTC),
            ),
        )
        # A failed watermark run with the latest window_end — ignored.
        failed = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(
                start=datetime(2026, 6, 19, tzinfo=UTC),
                end=datetime(2026, 6, 20, tzinfo=UTC),
            ),
        )
        run_ledger.fail_run(failed, error_detail='nope')
        # A succeeded feed run carries no window_end — ignored.
        feed = run_ledger.start_feed_run(Provider.SAMSARA, 'trips', from_version='v0')
        run_ledger.complete_run(feed, row_count=3, to_version='v1')

        frontier = run_ledger.coverage_frontier(Provider.SAMSARA, 'trips')
        assert frontier == WINDOW_END

    def test_returns_none_when_no_succeeded_watermark_run_exists(
        self, run_ledger: RunLedger
    ) -> None:
        feed = run_ledger.start_feed_run(Provider.GEOTAB, 'trips', from_version='v0')
        run_ledger.complete_run(feed, row_count=1, to_version='v1')
        assert run_ledger.coverage_frontier(Provider.GEOTAB, 'trips') is None

    def test_corrupt_window_end_raises(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        # window_start='2000-...' is lexically < 'not-a-datetime' ('2' < 'n'), so
        # the window-order CHECK passes; the value is unparseable on read.
        _insert_raw_succeeded_window_run(
            database_path,
            Provider.SAMSARA.value,
            'trips',
            '2000-01-01T00:00:00Z',
            'not-a-datetime',
        )
        with pytest.raises(ConfigurationError, match='unparseable run window_end'):
            run_ledger.coverage_frontier(Provider.SAMSARA, 'trips')


class TestLastSuccessAt:
    def test_returns_latest_ended_at_over_succeeded_runs(
        self, run_ledger: RunLedger, frozen_clock: FrozenClock
    ) -> None:
        earlier = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        frozen_clock.advance(timedelta(hours=1))
        run_ledger.complete_run(earlier, row_count=1)
        frozen_clock.advance(timedelta(hours=1))
        later = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        frozen_clock.advance(timedelta(hours=1))
        run_ledger.complete_run(later, row_count=2)

        assert run_ledger.last_success_at(Provider.SAMSARA, 'trips') == (
            FROZEN_INSTANT + timedelta(hours=3)
        )

    def test_ignores_running_and_failed_runs(
        self, run_ledger: RunLedger, frozen_clock: FrozenClock
    ) -> None:
        succeeded = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        frozen_clock.advance(timedelta(hours=1))
        run_ledger.complete_run(succeeded, row_count=1)
        success_time = FROZEN_INSTANT + timedelta(hours=1)
        # A later failed run — ignored despite its later ended_at.
        frozen_clock.advance(timedelta(hours=1))
        failed = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        frozen_clock.advance(timedelta(hours=1))
        run_ledger.fail_run(failed, error_detail='nope')
        # A later running run carries no ended_at — ignored.
        run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )

        assert run_ledger.last_success_at(Provider.SAMSARA, 'trips') == success_time

    def test_counts_a_succeeded_snapshot_run_unlike_coverage_frontier(
        self, run_ledger: RunLedger, frozen_clock: FrozenClock
    ) -> None:
        run_id = run_ledger.start_snapshot_run(Provider.MOTIVE, 'vehicles')
        frozen_clock.advance(timedelta(hours=2))
        run_ledger.complete_run(run_id, row_count=1300)

        assert run_ledger.last_success_at(Provider.MOTIVE, 'vehicles') == (
            FROZEN_INSTANT + timedelta(hours=2)
        )
        # The snapshot has no window, so it never reaches the coverage frontier.
        assert run_ledger.coverage_frontier(Provider.MOTIVE, 'vehicles') is None

    def test_returns_none_when_no_succeeded_run_exists(
        self, run_ledger: RunLedger
    ) -> None:
        failed = run_ledger.start_snapshot_run(Provider.GEOTAB, 'devices')
        run_ledger.fail_run(failed, error_detail='nope')
        assert run_ledger.last_success_at(Provider.GEOTAB, 'devices') is None

    def test_corrupt_ended_at_raises(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        _insert_raw_succeeded_snapshot_run(
            database_path,
            Provider.SAMSARA.value,
            'trips',
            'not-a-datetime',
        )
        with pytest.raises(ConfigurationError, match='unparseable run ended_at'):
            run_ledger.last_success_at(Provider.SAMSARA, 'trips')


class TestModeKeyedCheck:
    def test_snapshot_row_with_a_window_is_rejected(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _insert_raw_run(
                database_path,
                mode='snapshot',
                window_start='2026-06-01T00:00:00Z',
                window_end='2026-06-02T00:00:00Z',
            )

    def test_watermark_row_without_a_window_is_rejected(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _insert_raw_run(database_path, mode='watermark')

    def test_feed_row_without_a_version_is_rejected(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _insert_raw_run(database_path, mode='feed')

    def test_unknown_mode_value_is_rejected(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _insert_raw_run(database_path, mode='teleport')


class TestDurability:
    def test_a_separate_ledger_reads_back_a_started_run(
        self, run_ledger: RunLedger, database_path: Path
    ) -> None:
        run_id = run_ledger.start_window_run(
            Provider.SAMSARA,
            'trips',
            window=DateWindow(start=WINDOW_START, end=WINDOW_END),
        )
        reopened = RunLedger(
            StateDatabase(database_path),
            FrozenClock(start_time_utc=FROZEN_INSTANT),
        )
        # Completing through a fresh ledger proves the started row committed (its
        # mode SELECT finds the run); the frontier then proves the completion did.
        reopened.complete_run(run_id, row_count=7)
        assert reopened.coverage_frontier(Provider.SAMSARA, 'trips') == WINDOW_END
