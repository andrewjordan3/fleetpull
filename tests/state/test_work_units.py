"""Tests for fleetpull.state.work_units."""

import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head
from fleetpull.state.work_units import (
    WorkUnitProgress,
    WorkUnitSpec,
    WorkUnitStatus,
    WorkUnitStore,
)
from fleetpull.timing.clock import FrozenClock, SystemClock
from fleetpull.timing.codec import to_iso8601
from fleetpull.vocabulary import Provider
from tests.state.conftest import FROZEN_INSTANT

CHUNK_START: datetime = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
CHUNK_END: datetime = datetime(2026, 6, 2, 0, 0, 0, tzinfo=UTC)

_WORK_UNIT_COLUMNS: tuple[str, ...] = (
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
)


def _day(day: int) -> datetime:
    """A distinct whole-day UTC instant for building non-overlapping chunks."""
    return datetime(2026, 6, day, tzinfo=UTC)


def _spec(
    *,
    endpoint: str = 'trips',
    partition_key: str | None = None,
    chunk_start: datetime = CHUNK_START,
    chunk_end: datetime = CHUNK_END,
    provider: Provider = Provider.SAMSARA,
) -> WorkUnitSpec:
    """Build a WorkUnitSpec with sensible defaults for the common case."""
    return WorkUnitSpec(
        provider=provider,
        endpoint=endpoint,
        partition_key=partition_key,
        chunk_start=chunk_start,
        chunk_end=chunk_end,
    )


def _read_unit(
    database_path: Path, unit_id: int
) -> dict[str, str | int | float | bytes | None]:
    """Read one work_units row by id via a bare connection, keyed by column name."""
    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute(
            f'SELECT {", ".join(_WORK_UNIT_COLUMNS)} FROM work_units WHERE unit_id = ?',
            (unit_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return dict(zip(_WORK_UNIT_COLUMNS, row, strict=True))


def _count_units(database_path: Path) -> int:
    """Count all work_units rows via a bare connection."""
    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute('SELECT count(*) FROM work_units').fetchone()
    finally:
        connection.close()
    assert row is not None
    count = row[0]
    assert isinstance(count, int)
    return count


def _insert_raw_unit(
    database_path: Path,
    provider: str,
    endpoint: str,
    chunk_start: str,
    chunk_end: str,
) -> None:
    """Insert a pending unit directly, bypassing the store (for corruption)."""
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            'INSERT INTO work_units (provider, endpoint, chunk_start, chunk_end) '
            'VALUES (?, ?, ?, ?)',
            (provider, endpoint, chunk_start, chunk_end),
        )
        connection.commit()
    finally:
        connection.close()


@pytest.fixture
def work_unit_store(database_path: Path, frozen_clock: FrozenClock) -> WorkUnitStore:
    """A WorkUnitStore over a freshly initialized, migrated state database."""
    database = StateDatabase(database_path)
    database.initialize()
    migrate_to_head(database)
    return WorkUnitStore(database, frozen_clock)


class TestEnqueue:
    def test_inserts_and_returns_the_count(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        specs = [
            _spec(chunk_start=_day(1), chunk_end=_day(2)),
            _spec(chunk_start=_day(2), chunk_end=_day(3)),
        ]
        assert work_unit_store.enqueue(specs) == 2
        assert _count_units(database_path) == 2

    def test_is_idempotent_for_a_null_partition_key(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        specs = [_spec(partition_key=None)]
        assert work_unit_store.enqueue(specs) == 1
        assert work_unit_store.enqueue(specs) == 0
        assert _count_units(database_path) == 1

    def test_is_idempotent_for_a_non_null_partition_key(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        specs = [_spec(partition_key='V1')]
        assert work_unit_store.enqueue(specs) == 1
        assert work_unit_store.enqueue(specs) == 0
        assert _count_units(database_path) == 1

    def test_same_start_different_end_is_distinct(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        assert work_unit_store.enqueue([_spec(chunk_end=CHUNK_END)]) == 1
        assert work_unit_store.enqueue([_spec(chunk_end=_day(3))]) == 1
        assert _count_units(database_path) == 2

    @pytest.mark.parametrize(
        ('chunk_start', 'chunk_end'),
        [
            (CHUNK_END, CHUNK_START),  # inverted: start after end
            (CHUNK_START, CHUNK_START),  # empty: start equals end
        ],
    )
    def test_rejects_a_non_increasing_chunk(
        self,
        work_unit_store: WorkUnitStore,
        chunk_start: datetime,
        chunk_end: datetime,
    ) -> None:
        with pytest.raises(ValueError, match='chunk_start must be strictly before'):
            work_unit_store.enqueue(
                [_spec(chunk_start=chunk_start, chunk_end=chunk_end)]
            )


class TestClaimNext:
    def test_rejects_max_attempts_below_one(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        with pytest.raises(ValueError, match='max_attempts must be at least 1'):
            work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=0)

    def test_returns_the_unit_and_flips_the_row(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec(partition_key='V1')])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert claimed is not None
        assert claimed.attempt_count == 1
        assert claimed.spec == _spec(partition_key='V1')
        row = _read_unit(database_path, claimed.unit_id)
        assert row['status'] == WorkUnitStatus.CLAIMED
        assert row['claimed_at'] == to_iso8601(FROZEN_INSTANT)
        assert row['attempt_count'] == 1

    def test_returns_none_when_nothing_claimable(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert claimed is None

    def test_serves_units_in_unit_id_order(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        work_unit_store.enqueue(
            [
                _spec(chunk_start=_day(1), chunk_end=_day(2)),
                _spec(chunk_start=_day(2), chunk_end=_day(3)),
                _spec(chunk_start=_day(3), chunk_end=_day(4)),
            ]
        )
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert first is not None
        assert second is not None
        assert first.unit_id < second.unit_id

    def test_skips_units_at_or_over_max_attempts(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=1)
        assert first is not None
        assert first.attempt_count == 1
        work_unit_store.mark_failed(first.unit_id, error_detail='nope')
        # attempt_count (1) is no longer < max_attempts (1), so it is skipped.
        skipped = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=1)
        assert skipped is None

    def test_reserves_failed_units_under_the_cap(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert first is not None
        work_unit_store.mark_failed(first.unit_id, error_detail='retry me')
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert second is not None
        assert second.unit_id == first.unit_id
        assert second.attempt_count == 2

    def test_reclaim_clears_the_prior_outcome(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert first is not None
        work_unit_store.mark_failed(first.unit_id, error_detail='boom')
        failed_row = _read_unit(database_path, first.unit_id)
        assert failed_row['last_error'] == 'boom'
        assert failed_row['finished_at'] is not None

        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert second is not None
        assert second.unit_id == first.unit_id
        reclaimed_row = _read_unit(database_path, first.unit_id)
        assert reclaimed_row['last_error'] is None
        assert reclaimed_row['finished_at'] is None


class TestMarkDone:
    def test_flips_claimed_to_done(
        self,
        work_unit_store: WorkUnitStore,
        frozen_clock: FrozenClock,
        database_path: Path,
    ) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert claimed is not None
        frozen_clock.advance(timedelta(minutes=10))
        work_unit_store.mark_done(claimed.unit_id)
        row = _read_unit(database_path, claimed.unit_id)
        assert row['status'] == WorkUnitStatus.DONE
        assert row['finished_at'] == to_iso8601(FROZEN_INSTANT + timedelta(minutes=10))

    def test_rejects_an_unknown_unit(self, work_unit_store: WorkUnitStore) -> None:
        with pytest.raises(ValueError, match='no claimed work unit'):
            work_unit_store.mark_done(999)

    def test_rejects_a_non_claimed_unit(self, work_unit_store: WorkUnitStore) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert claimed is not None
        work_unit_store.mark_done(claimed.unit_id)
        with pytest.raises(ValueError, match='no claimed work unit'):
            work_unit_store.mark_done(claimed.unit_id)  # already done, not claimed


class TestMarkFailed:
    def test_flips_claimed_to_failed_with_error(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert claimed is not None
        work_unit_store.mark_failed(claimed.unit_id, error_detail='kaboom')
        row = _read_unit(database_path, claimed.unit_id)
        assert row['status'] == WorkUnitStatus.FAILED
        assert row['last_error'] == 'kaboom'

    def test_rejects_an_unknown_unit(self, work_unit_store: WorkUnitStore) -> None:
        with pytest.raises(ValueError, match='no claimed work unit'):
            work_unit_store.mark_failed(999, error_detail='x')

    def test_rejects_a_non_claimed_unit(self, work_unit_store: WorkUnitStore) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert claimed is not None
        work_unit_store.mark_done(claimed.unit_id)
        with pytest.raises(ValueError, match='no claimed work unit'):
            work_unit_store.mark_failed(claimed.unit_id, error_detail='x')


class TestResetClaimedToPending:
    def test_reverts_claimed_preserving_attempt_count(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert claimed is not None
        assert claimed.attempt_count == 1
        assert work_unit_store.reset_claimed_to_pending(Provider.SAMSARA, 'trips') == 1
        row = _read_unit(database_path, claimed.unit_id)
        assert row['status'] == WorkUnitStatus.PENDING
        assert row['attempt_count'] == 1

    def test_leaves_other_statuses_untouched(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        work_unit_store.enqueue(
            [
                _spec(chunk_start=_day(1), chunk_end=_day(2)),
                _spec(chunk_start=_day(2), chunk_end=_day(3)),
                _spec(chunk_start=_day(3), chunk_end=_day(4)),
                _spec(chunk_start=_day(4), chunk_end=_day(5)),
            ]
        )
        done_unit = work_unit_store.claim_next(
            Provider.SAMSARA, 'trips', max_attempts=1
        )
        assert done_unit is not None
        work_unit_store.mark_done(done_unit.unit_id)
        failed_unit = work_unit_store.claim_next(
            Provider.SAMSARA, 'trips', max_attempts=1
        )
        assert failed_unit is not None
        work_unit_store.mark_failed(failed_unit.unit_id, error_detail='x')
        claimed_unit = work_unit_store.claim_next(
            Provider.SAMSARA, 'trips', max_attempts=1
        )
        assert claimed_unit is not None

        assert work_unit_store.reset_claimed_to_pending(Provider.SAMSARA, 'trips') == 1

        progress = work_unit_store.progress(Provider.SAMSARA, 'trips')
        assert progress.done == 1
        assert progress.failed == 1
        assert progress.claimed == 0
        assert progress.pending == 2


class TestProgress:
    def test_counts_per_status_including_zeros(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        assert work_unit_store.progress(Provider.SAMSARA, 'trips') == WorkUnitProgress(
            pending=0, claimed=0, done=0, failed=0
        )
        work_unit_store.enqueue(
            [
                _spec(chunk_start=_day(1), chunk_end=_day(2)),
                _spec(chunk_start=_day(2), chunk_end=_day(3)),
            ]
        )
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert claimed is not None
        assert work_unit_store.progress(Provider.SAMSARA, 'trips') == WorkUnitProgress(
            pending=1, claimed=1, done=0, failed=0
        )


class TestLifecycle:
    def test_enqueue_claim_fail_reclaim_done(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert first is not None
        assert first.attempt_count == 1
        work_unit_store.mark_failed(first.unit_id, error_detail='transient')
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert second is not None
        assert second.unit_id == first.unit_id
        assert second.attempt_count == 2
        work_unit_store.mark_done(second.unit_id)
        row = _read_unit(database_path, second.unit_id)
        assert row['status'] == WorkUnitStatus.DONE
        assert row['last_error'] is None  # cleared at the re-claim, never restored


class TestCrashRecovery:
    def test_reset_then_reclaim_increments_attempt(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert first is not None
        assert first.attempt_count == 1
        assert work_unit_store.reset_claimed_to_pending(Provider.SAMSARA, 'trips') == 1
        row = _read_unit(database_path, first.unit_id)
        assert row['status'] == WorkUnitStatus.PENDING
        assert row['attempt_count'] == 1  # preserved across the reset
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)
        assert second is not None
        assert second.unit_id == first.unit_id
        assert second.attempt_count == 2  # the crashed attempt counted


class TestCorruption:
    def test_unparseable_chunk_raises(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        # chunk_start='2026-...' is lexically < 'zzz-bad' ('2' < 'z'), so the order
        # CHECK passes; the value is unparseable when the claim reconstructs it.
        _insert_raw_unit(
            database_path,
            Provider.SAMSARA.value,
            'trips',
            '2026-01-01T00:00:00Z',
            'zzz-bad',
        )
        with pytest.raises(ConfigurationError, match='unparseable work-unit chunk'):
            work_unit_store.claim_next(Provider.SAMSARA, 'trips', max_attempts=3)


class TestConcurrency:
    def test_concurrent_claims_never_double_claim(self, database_path: Path) -> None:
        database = StateDatabase(database_path)
        database.initialize()
        migrate_to_head(database)
        store = WorkUnitStore(database, SystemClock())
        unit_total = 300
        base = datetime(2026, 1, 1, tzinfo=UTC)
        specs = [
            _spec(
                chunk_start=base + timedelta(days=index),
                chunk_end=base + timedelta(days=index + 1),
            )
            for index in range(unit_total)
        ]
        assert store.enqueue(specs) == unit_total

        claimed_ids: list[int] = []
        guard = threading.Lock()

        def claim_until_empty() -> None:
            while True:
                unit = store.claim_next(Provider.SAMSARA, 'trips', max_attempts=1)
                if unit is None:
                    return
                with guard:
                    claimed_ids.append(unit.unit_id)

        workers = [threading.Thread(target=claim_until_empty) for _ in range(4)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        assert len(claimed_ids) == unit_total
        assert len(set(claimed_ids)) == unit_total
