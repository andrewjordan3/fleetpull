"""Tests for fleetpull.storage.partitioning."""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from fleetpull.incremental import DateWindow
from fleetpull.storage.partitioning import (
    delete_partition,
    existing_partition_dates,
    prune_window_partitions,
    window_dates,
)


def _create_partition(endpoint_dir: Path, partition_date: date) -> Path:
    """Create a ``date=`` partition directory with a ``part.parquet`` inside."""
    directory = endpoint_dir / f'date={partition_date.isoformat()}'
    directory.mkdir(parents=True)
    (directory / 'part.parquet').write_bytes(b'x')
    return directory


class TestWindowDates:
    def test_whole_day_window_covers_only_its_start_date(self) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        assert window_dates(window) == [date(2026, 6, 1)]

    def test_midnight_end_excludes_its_date(self) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, 12, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )
        assert window_dates(window) == [date(2026, 6, 1)]

    def test_mid_day_end_covers_its_date(self) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, 12, tzinfo=UTC),
            end=datetime(2026, 6, 2, 14, tzinfo=UTC),
        )
        assert window_dates(window) == [date(2026, 6, 1), date(2026, 6, 2)]

    def test_multi_day_span_is_contiguous_ascending(self) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 4, 6, tzinfo=UTC),
        )
        assert window_dates(window) == [
            date(2026, 6, 1),
            date(2026, 6, 2),
            date(2026, 6, 3),
            date(2026, 6, 4),
        ]

    def test_sub_day_window_covers_one_date(self) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, 8, tzinfo=UTC),
            end=datetime(2026, 6, 1, 9, tzinfo=UTC),
        )
        assert window_dates(window) == [date(2026, 6, 1)]


class TestExistingPartitionDates:
    def test_returns_only_candidates_present_on_disk(self, tmp_path: Path) -> None:
        _create_partition(tmp_path, date(2026, 6, 1))
        _create_partition(tmp_path, date(2026, 6, 3))
        candidates = {
            date(2026, 6, 1),
            date(2026, 6, 2),
            date(2026, 6, 3),
            date(2026, 6, 4),
        }
        assert existing_partition_dates(tmp_path, candidates) == {
            date(2026, 6, 1),
            date(2026, 6, 3),
        }

    def test_empty_when_endpoint_dir_absent(self, tmp_path: Path) -> None:
        missing = tmp_path / 'not_created'
        assert existing_partition_dates(missing, {date(2026, 6, 1)}) == set()

    def test_empty_when_no_partition_dirs(self, tmp_path: Path) -> None:
        assert existing_partition_dates(tmp_path, {date(2026, 6, 1)}) == set()

    def test_probes_candidates_not_the_directory_listing(self, tmp_path: Path) -> None:
        _create_partition(tmp_path, date(2026, 6, 5))
        result = existing_partition_dates(
            tmp_path, {date(2026, 6, 1), date(2026, 6, 2)}
        )
        assert date(2026, 6, 5) not in result
        assert result == set()


class TestDeletePartition:
    def test_removes_the_whole_partition_directory(self, tmp_path: Path) -> None:
        directory = _create_partition(tmp_path, date(2026, 6, 1))
        (directory / '.part.parquet.abc123.tmp').write_bytes(b'stale')
        delete_partition(tmp_path, date(2026, 6, 1))
        assert not directory.exists()

    def test_raises_when_directory_absent(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            delete_partition(tmp_path, date(2026, 6, 1))


class TestPruneWindowPartitions:
    def test_deletes_only_covered_unwritten_partitions(self, tmp_path: Path) -> None:
        # Window covers June 1-3 (midnight end excludes June 4).
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 4, tzinfo=UTC),
        )
        outside_before = _create_partition(tmp_path, date(2026, 5, 30))
        covered_stale = _create_partition(tmp_path, date(2026, 6, 2))
        outside_after = _create_partition(tmp_path, date(2026, 6, 10))

        deleted = prune_window_partitions(
            tmp_path, window, written_dates={date(2026, 6, 1)}
        )

        assert deleted == [date(2026, 6, 2)]
        assert not covered_stale.exists()
        # The leash: partitions outside the window are never touched.
        assert outside_before.exists()
        assert outside_after.exists()

    def test_deletes_nothing_when_all_covered_dates_written(
        self, tmp_path: Path
    ) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 4, tzinfo=UTC),
        )
        first = _create_partition(tmp_path, date(2026, 6, 1))
        second = _create_partition(tmp_path, date(2026, 6, 2))

        deleted = prune_window_partitions(
            tmp_path,
            window,
            written_dates={date(2026, 6, 1), date(2026, 6, 2)},
        )

        assert deleted == []
        assert first.exists()
        assert second.exists()

    def test_deletes_nothing_when_no_covered_partition_exists(
        self, tmp_path: Path
    ) -> None:
        window = DateWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 4, tzinfo=UTC),
        )
        outside = _create_partition(tmp_path, date(2026, 5, 30))

        deleted = prune_window_partitions(
            tmp_path, window, written_dates={date(2026, 6, 1)}
        )

        assert deleted == []
        assert outside.exists()
