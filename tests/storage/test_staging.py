"""Tests for fleetpull.storage.staging."""

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from fleetpull.storage.files import partition_part_file, partition_staging_dir
from fleetpull.storage.staging import (
    clear_partition_staging,
    compact_partition,
    stage_shard,
)


def _frame(rows: list[tuple[datetime, int]]) -> pl.DataFrame:
    """Build a (located_at, id) frame from (datetime, id) rows."""
    return pl.DataFrame(
        {
            'located_at': [moment for moment, _ in rows],
            'id': [identifier for _, identifier in rows],
        }
    )


class TestStageShard:
    def test_stages_one_shard_per_date_and_returns_the_dates(
        self, tmp_path: Path
    ) -> None:
        frame = _frame(
            [
                (datetime(2026, 6, 1, 8, tzinfo=UTC), 1),
                (datetime(2026, 6, 2, 8, tzinfo=UTC), 2),
            ]
        )
        touched = stage_shard(tmp_path, frame, 'located_at')
        assert touched == {date(2026, 6, 1), date(2026, 6, 2)}
        for partition_date in (date(2026, 6, 1), date(2026, 6, 2)):
            shards = list(
                partition_staging_dir(tmp_path, partition_date).glob('*.shard')
            )
            assert len(shards) == 1

    def test_empty_frame_stages_nothing(self, tmp_path: Path) -> None:
        empty = pl.DataFrame(
            schema={'located_at': pl.Datetime('us', 'UTC'), 'id': pl.Int64}
        )
        assert stage_shard(tmp_path, empty, 'located_at') == set()
        assert list(tmp_path.iterdir()) == []


class TestCompactPartition:
    def test_replace_writes_part(self, tmp_path: Path) -> None:
        partition_date = date(2026, 6, 1)
        stage_shard(
            tmp_path,
            _frame(
                [
                    (datetime(2026, 6, 1, 8, tzinfo=UTC), 1),
                    (datetime(2026, 6, 1, 9, tzinfo=UTC), 2),
                ]
            ),
            'located_at',
        )
        result = compact_partition(tmp_path, partition_date, existing=None)
        part = pl.read_parquet(partition_part_file(tmp_path, partition_date))
        assert part.get_column('id').to_list() == [1, 2]
        assert result.rows_written == 2
        assert result.duplicates_dropped == 0

    def test_leaves_staging_for_the_caller(self, tmp_path: Path) -> None:
        # compact_partition folds only; the writer clears staging afterward.
        partition_date = date(2026, 6, 1)
        stage_shard(
            tmp_path, _frame([(datetime(2026, 6, 1, 8, tzinfo=UTC), 1)]), 'located_at'
        )
        compact_partition(tmp_path, partition_date, existing=None)
        assert partition_part_file(tmp_path, partition_date).exists()
        assert partition_staging_dir(tmp_path, partition_date).exists()

    def test_multiple_shards_fold_into_one_partition(self, tmp_path: Path) -> None:
        partition_date = date(2026, 6, 1)
        stage_shard(
            tmp_path, _frame([(datetime(2026, 6, 1, 8, tzinfo=UTC), 1)]), 'located_at'
        )
        stage_shard(
            tmp_path, _frame([(datetime(2026, 6, 1, 9, tzinfo=UTC), 2)]), 'located_at'
        )
        compact_partition(tmp_path, partition_date, existing=None)
        part = pl.read_parquet(partition_part_file(tmp_path, partition_date))
        assert sorted(part.get_column('id').to_list()) == [1, 2]

    def test_dedup_at_compaction(self, tmp_path: Path) -> None:
        partition_date = date(2026, 6, 1)
        stage_shard(
            tmp_path,
            _frame(
                [
                    (datetime(2026, 6, 1, 8, tzinfo=UTC), 1),
                    (datetime(2026, 6, 1, 8, tzinfo=UTC), 1),  # exact duplicate
                    (datetime(2026, 6, 1, 9, tzinfo=UTC), 2),
                ]
            ),
            'located_at',
        )
        result = compact_partition(tmp_path, partition_date, existing=None)
        part = pl.read_parquet(partition_part_file(tmp_path, partition_date))
        assert part.height == 2
        assert result.rows_written == 2
        assert result.duplicates_dropped == 1

    def test_folds_in_existing(self, tmp_path: Path) -> None:
        partition_date = date(2026, 6, 1)
        stage_shard(
            tmp_path, _frame([(datetime(2026, 6, 1, 8, tzinfo=UTC), 1)]), 'located_at'
        )
        existing = _frame([(datetime(2026, 6, 1, 7, tzinfo=UTC), 9)])
        compact_partition(tmp_path, partition_date, existing=existing)
        part = pl.read_parquet(partition_part_file(tmp_path, partition_date))
        assert sorted(part.get_column('id').to_list()) == [1, 9]


def test_clears_staging_and_keeps_a_partition_with_data(tmp_path: Path) -> None:
    date_dir = tmp_path / 'date=2026-06-01'
    staging = date_dir / 'staging'
    staging.mkdir(parents=True)
    (staging / 'shard-x.shard').write_bytes(b'x')
    (date_dir / 'part.parquet').write_bytes(b'data')
    clear_partition_staging(tmp_path, [date(2026, 6, 1)])
    assert not staging.exists()
    assert (date_dir / 'part.parquet').exists()


def test_clears_staging_and_removes_a_now_empty_partition_dir(
    tmp_path: Path,
) -> None:
    date_dir = tmp_path / 'date=2026-06-01'
    staging = date_dir / 'staging'
    staging.mkdir(parents=True)
    (staging / 'shard-x.shard').write_bytes(b'x')
    clear_partition_staging(tmp_path, [date(2026, 6, 1)])
    assert not date_dir.exists()


def test_clear_partition_staging_is_lenient_when_absent(tmp_path: Path) -> None:
    # No staging directory under the date: clearing is a no-op, not an error.
    clear_partition_staging(tmp_path, [date(2026, 6, 1)])
    assert not (tmp_path / 'date=2026-06-01').exists()
