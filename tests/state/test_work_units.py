"""Tests for fleetpull.state.work_units."""

import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import DateWindow
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
    'observed_max',
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


def _corrupt_unit_observation(database_path: Path, observed_max: str | bytes) -> None:
    """Force every unit row done with a corrupt observation, bypassing the store.

    ``bytes`` would land as a BLOB; on this STRICT table SQLite refuses
    it at write time (integers are converted by TEXT affinity on
    non-STRICT tables, but STRICT rejects both) -- callers passing bytes
    are asserting that refusal.
    """
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "UPDATE work_units SET status = 'done', observed_max = ?",
            (observed_max,),
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
    def test_returns_the_unit_and_flips_the_row(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec(partition_key='V1')])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
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
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
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
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert first is not None
        assert second is not None
        assert first.unit_id < second.unit_id

    def test_a_failed_unit_stays_claimable_on_every_pass(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        # There is no attempt cap (DESIGN section 5): a persistently failing
        # unit is re-served on every pass -- a poison unit fails the endpoint
        # loudly rather than being silently skipped behind an advancing
        # watermark -- while attempt_count keeps counting for the record.
        # The loop runs far past any plausible bounded-retry cap (a
        # reintroduced 'attempt_count < 3' survived a 3-attempt version of
        # this test), so a cap of ANY classic size turns a claim into None
        # here and dies loudly.
        work_unit_store.enqueue([_spec()])
        for expected_attempt in range(1, 13):
            claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
            assert claimed is not None
            assert claimed.attempt_count == expected_attempt
            work_unit_store.mark_failed(claimed.unit_id, error_detail='nope')

    def test_reserves_failed_units_on_a_later_pass(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert first is not None
        work_unit_store.mark_failed(first.unit_id, error_detail='retry me')
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert second is not None
        assert second.unit_id == first.unit_id
        assert second.attempt_count == 2

    def test_reclaim_clears_the_prior_outcome(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert first is not None
        work_unit_store.mark_failed(first.unit_id, error_detail='boom')
        failed_row = _read_unit(database_path, first.unit_id)
        assert failed_row['last_error'] == 'boom'
        assert failed_row['finished_at'] is not None

        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
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
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert claimed is not None
        frozen_clock.advance(timedelta(minutes=10))
        work_unit_store.mark_done(claimed.unit_id, observed_max=None)
        row = _read_unit(database_path, claimed.unit_id)
        assert row['status'] == WorkUnitStatus.DONE
        assert row['finished_at'] == to_iso8601(FROZEN_INSTANT + timedelta(minutes=10))

    def test_persists_the_observation(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        # The prefix-advance rule's datum: mark_done records the unit's
        # folded in-window maximum in to_iso8601 form.
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert claimed is not None
        observed = datetime(2026, 6, 1, 18, 45, 0, tzinfo=UTC)
        work_unit_store.mark_done(claimed.unit_id, observed_max=observed)
        row = _read_unit(database_path, claimed.unit_id)
        assert row['observed_max'] == to_iso8601(observed)

    def test_persists_null_for_an_empty_unit(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert claimed is not None
        work_unit_store.mark_done(claimed.unit_id, observed_max=None)
        row = _read_unit(database_path, claimed.unit_id)
        assert row['observed_max'] is None

    def test_rejects_an_unknown_unit(self, work_unit_store: WorkUnitStore) -> None:
        with pytest.raises(ValueError, match='no claimed work unit'):
            work_unit_store.mark_done(999, observed_max=None)

    def test_rejects_a_non_claimed_unit(self, work_unit_store: WorkUnitStore) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert claimed is not None
        work_unit_store.mark_done(claimed.unit_id, observed_max=None)
        with pytest.raises(ValueError, match='no claimed work unit'):
            # already done, not claimed
            work_unit_store.mark_done(claimed.unit_id, observed_max=None)


def _observation(day: int) -> datetime:
    """A whole-second in-window observation for the day's unit (codec round-trip safe)."""
    return datetime(2026, 6, day, 12, 0, 0, tzinfo=UTC)


class TestDonePrefixObservation:
    """The prefix-advance rule's read (DESIGN section 5, 2026-07-20)."""

    @staticmethod
    def _claim_daily_units(store: WorkUnitStore, count: int) -> list[int]:
        """Enqueue ``count`` consecutive daily units and claim every one."""
        store.enqueue(
            [
                _spec(chunk_start=_day(day), chunk_end=_day(day + 1))
                for day in range(1, count + 1)
            ]
        )
        unit_ids: list[int] = []
        for _ in range(count):
            claimed = store.claim_next(Provider.SAMSARA, 'trips')
            assert claimed is not None
            unit_ids.append(claimed.unit_id)
        return unit_ids

    def test_empty_table_reads_none(self, work_unit_store: WorkUnitStore) -> None:
        assert (
            work_unit_store.done_prefix_observation(Provider.SAMSARA, 'trips') is None
        )

    def test_all_done_reads_the_global_maximum(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        unit_ids = self._claim_daily_units(work_unit_store, 3)
        work_unit_store.mark_done(unit_ids[0], observed_max=_observation(1))
        work_unit_store.mark_done(unit_ids[1], observed_max=_observation(3))
        work_unit_store.mark_done(unit_ids[2], observed_max=_observation(2))
        assert work_unit_store.done_prefix_observation(
            Provider.SAMSARA, 'trips'
        ) == _observation(3)

    def test_a_pending_gap_gates_the_prefix(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        # Units 1 and 3 done, unit 2 pending: the prefix stops at the gap,
        # so unit 3's later observation is excluded.
        unit_ids = self._claim_daily_units(work_unit_store, 3)
        work_unit_store.mark_done(unit_ids[0], observed_max=_observation(1))
        work_unit_store.mark_done(unit_ids[2], observed_max=_observation(3))
        assert work_unit_store.reset_claimed_to_pending(Provider.SAMSARA, 'trips') == 1
        assert work_unit_store.done_prefix_observation(
            Provider.SAMSARA, 'trips'
        ) == _observation(1)

    def test_a_failed_gap_gates_the_prefix(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        unit_ids = self._claim_daily_units(work_unit_store, 3)
        work_unit_store.mark_done(unit_ids[0], observed_max=_observation(1))
        work_unit_store.mark_failed(unit_ids[1], error_detail='planted')
        work_unit_store.mark_done(unit_ids[2], observed_max=_observation(3))
        assert work_unit_store.done_prefix_observation(
            Provider.SAMSARA, 'trips'
        ) == _observation(1)

    def test_null_observations_inside_the_prefix_are_skipped(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        # Empty units record NULL; MAX ignores them without gating.
        unit_ids = self._claim_daily_units(work_unit_store, 3)
        work_unit_store.mark_done(unit_ids[0], observed_max=None)
        work_unit_store.mark_done(unit_ids[1], observed_max=_observation(2))
        work_unit_store.mark_done(unit_ids[2], observed_max=None)
        assert work_unit_store.done_prefix_observation(
            Provider.SAMSARA, 'trips'
        ) == _observation(2)

    def test_an_all_null_prefix_reads_none(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        unit_ids = self._claim_daily_units(work_unit_store, 2)
        work_unit_store.mark_done(unit_ids[0], observed_max=None)
        work_unit_store.mark_done(unit_ids[1], observed_max=None)
        assert (
            work_unit_store.done_prefix_observation(Provider.SAMSARA, 'trips') is None
        )

    def test_gap_blindness_a_never_enqueued_hole_does_not_gate(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        # THE DOCUMENTED GAP-BLINDNESS, PINNED DELIBERATELY (the DESIGN
        # section 5 tripwire): the prefix query sees only rows, so a hole no
        # row represents -- here the never-enqueued day between the two done
        # units -- does NOT gate the prefix, and the far observation IS
        # returned. This is safe in production only because the four section
        # 5 invariants (one enqueue site running after the claim loop
        # drains, the never-binding attempt cap, row deletion confined to
        # the planning site's release-then-enqueue pairing, hole-free
        # planner tiling) make such a hole unreachable. If this test ever
        # has to change -- a "fix" that gates on holes, or a new caller
        # relying on gap-blindness -- that change must consciously re-derive
        # the prefix rule's safety, not land as a drive-by.
        work_unit_store.enqueue(
            [
                _spec(chunk_start=_day(1), chunk_end=_day(2)),
                # Day 2 -> 3 is a hole: never enqueued, no row anywhere.
                _spec(chunk_start=_day(3), chunk_end=_day(4)),
            ]
        )
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert first is not None
        assert second is not None
        work_unit_store.mark_done(first.unit_id, observed_max=_observation(1))
        work_unit_store.mark_done(second.unit_id, observed_max=_observation(3))
        assert work_unit_store.done_prefix_observation(
            Provider.SAMSARA, 'trips'
        ) == _observation(3)


class TestMarkFailed:
    def test_flips_claimed_to_failed_with_error(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
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
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert claimed is not None
        work_unit_store.mark_done(claimed.unit_id, observed_max=None)
        with pytest.raises(ValueError, match='no claimed work unit'):
            work_unit_store.mark_failed(claimed.unit_id, error_detail='x')


class TestResetClaimedToPending:
    def test_reverts_claimed_preserving_attempt_count(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
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
        done_unit = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert done_unit is not None
        work_unit_store.mark_done(done_unit.unit_id, observed_max=None)
        # The next claim stays claimed, so the third claim serves the unit
        # after it -- a capless claim would otherwise re-serve a failed unit
        # immediately (FIFO by unit_id over pending and failed alike).
        claimed_unit = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert claimed_unit is not None
        failed_unit = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert failed_unit is not None
        work_unit_store.mark_failed(failed_unit.unit_id, error_detail='x')

        assert work_unit_store.reset_claimed_to_pending(Provider.SAMSARA, 'trips') == 1

        progress = work_unit_store.progress(Provider.SAMSARA, 'trips')
        assert progress.done == 1
        assert progress.failed == 1
        assert progress.claimed == 0
        assert progress.pending == 2


class TestReleaseDoneUnits:
    """The planner's release-then-enqueue pairing (DESIGN section 5,
    corrected 2026-07-21): done rows inside a re-planned window release
    back to plannable, so the idempotent enqueue cannot collapse the
    lookback margin onto committed history."""

    def test_releases_only_done_rows_fully_inside_the_window(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        work_unit_store.enqueue(
            [
                _spec(chunk_start=_day(1), chunk_end=_day(2)),
                _spec(chunk_start=_day(2), chunk_end=_day(3)),
                _spec(chunk_start=_day(3), chunk_end=_day(4)),
            ]
        )
        done_unit = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert done_unit is not None
        work_unit_store.mark_done(done_unit.unit_id, observed_max=None)
        failed_unit = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert failed_unit is not None
        work_unit_store.mark_failed(failed_unit.unit_id, error_detail='x')

        released = work_unit_store.release_done_units(
            Provider.SAMSARA, 'trips', window=DateWindow(start=_day(1), end=_day(4))
        )

        assert released == 1
        progress = work_unit_store.progress(Provider.SAMSARA, 'trips')
        assert progress.done == 0
        assert progress.failed == 1
        assert progress.pending == 1

    def test_a_straddling_done_row_is_kept(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        # Only rows FULLY inside the window release; a straddler stays
        # done, harmlessly overlapped by the re-plan's fresh tiles.
        work_unit_store.enqueue([_spec(chunk_start=_day(1), chunk_end=_day(3))])
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert claimed is not None
        work_unit_store.mark_done(claimed.unit_id, observed_max=None)

        released = work_unit_store.release_done_units(
            Provider.SAMSARA, 'trips', window=DateWindow(start=_day(2), end=_day(4))
        )

        assert released == 0
        assert work_unit_store.progress(Provider.SAMSARA, 'trips').done == 1

    def test_release_scopes_to_the_provider_and_endpoint(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        work_unit_store.enqueue(
            [
                _spec(chunk_start=_day(1), chunk_end=_day(2)),
                _spec(endpoint='drivers', chunk_start=_day(1), chunk_end=_day(2)),
            ]
        )
        for endpoint in ('trips', 'drivers'):
            claimed = work_unit_store.claim_next(Provider.SAMSARA, endpoint)
            assert claimed is not None
            work_unit_store.mark_done(claimed.unit_id, observed_max=None)

        released = work_unit_store.release_done_units(
            Provider.SAMSARA, 'trips', window=DateWindow(start=_day(1), end=_day(2))
        )

        assert released == 1
        assert work_unit_store.progress(Provider.SAMSARA, 'drivers').done == 1

    def test_a_released_unit_reenqueues_as_fresh_claimable_work(
        self, work_unit_store: WorkUnitStore
    ) -> None:
        # THE COLLAPSE TRIPWIRE: without the release, this identical-key
        # re-enqueue inserts nothing (INSERT OR IGNORE onto the kept done
        # row) and the re-covered day is never claimable again -- the
        # silently-skipped lookback margin this pairing exists to prevent.
        spec = _spec(chunk_start=_day(1), chunk_end=_day(2))
        work_unit_store.enqueue([spec])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert first is not None
        work_unit_store.mark_done(first.unit_id, observed_max=None)

        work_unit_store.release_done_units(
            Provider.SAMSARA, 'trips', window=DateWindow(start=_day(1), end=_day(2))
        )
        assert work_unit_store.enqueue([spec]) == 1

        fresh = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert fresh is not None
        assert fresh.attempt_count == 1
        assert fresh.spec == spec


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
        claimed = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert claimed is not None
        assert work_unit_store.progress(Provider.SAMSARA, 'trips') == WorkUnitProgress(
            pending=1, claimed=1, done=0, failed=0
        )


class TestLifecycle:
    def test_enqueue_claim_fail_reclaim_done(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert first is not None
        assert first.attempt_count == 1
        work_unit_store.mark_failed(first.unit_id, error_detail='transient')
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert second is not None
        assert second.unit_id == first.unit_id
        assert second.attempt_count == 2
        work_unit_store.mark_done(second.unit_id, observed_max=None)
        row = _read_unit(database_path, second.unit_id)
        assert row['status'] == WorkUnitStatus.DONE
        assert row['last_error'] is None  # cleared at the re-claim, never restored


class TestCrashRecovery:
    def test_reset_then_reclaim_increments_attempt(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        work_unit_store.enqueue([_spec()])
        first = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert first is not None
        assert first.attempt_count == 1
        assert work_unit_store.reset_claimed_to_pending(Provider.SAMSARA, 'trips') == 1
        row = _read_unit(database_path, first.unit_id)
        assert row['status'] == WorkUnitStatus.PENDING
        assert row['attempt_count'] == 1  # preserved across the reset
        second = work_unit_store.claim_next(Provider.SAMSARA, 'trips')
        assert second is not None
        assert second.unit_id == first.unit_id
        assert second.attempt_count == 2  # the crashed attempt counted


class TestCorruption:
    def test_unparseable_observed_max_raises_configuration_error(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        # A done row whose observed_max is not ISO-8601 is state-store
        # corruption: the prefix read must raise, never freeze the
        # watermark silently.
        _insert_raw_unit(
            database_path,
            Provider.SAMSARA.value,
            'trips',
            '2026-01-01T00:00:00Z',
            '2026-01-08T00:00:00Z',
        )
        _corrupt_unit_observation(database_path, 'not-a-timestamp')
        with pytest.raises(ConfigurationError):
            work_unit_store.done_prefix_observation(Provider.SAMSARA, 'trips')

    def test_strict_schema_rejects_non_text_observation_at_write(
        self, work_unit_store: WorkUnitStore, database_path: Path
    ) -> None:
        # The non-text RuntimeError arm in done_prefix_observation is
        # structurally unreachable: work_units is a STRICT table, so
        # SQLite itself refuses a non-text observed_max at write time --
        # pinned here so a schema change that drops STRICT (reopening
        # the arm) is a conscious decision. The in-code narrowing stays
        # as the typing-required else-branch.
        _insert_raw_unit(
            database_path,
            Provider.SAMSARA.value,
            'trips',
            '2026-01-01T00:00:00Z',
            '2026-01-08T00:00:00Z',
        )
        with pytest.raises(sqlite3.IntegrityError):
            _corrupt_unit_observation(database_path, b'12345')


class TestClaimCorruption:
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
            work_unit_store.claim_next(Provider.SAMSARA, 'trips')


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
                unit = store.claim_next(Provider.SAMSARA, 'trips')
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
