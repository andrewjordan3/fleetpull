# tests/storage/test_splitting.py
"""Tests for fleetpull.storage.splitting."""

from datetime import UTC, date, datetime

import polars as pl

from fleetpull.storage.splitting import split_by_date


def _frame(rows: list[tuple[datetime, int]]) -> pl.DataFrame:
    """Build a two-column (located_at, id) frame from (datetime, id) rows."""
    return pl.DataFrame(
        {
            'located_at': [moment for moment, _ in rows],
            'id': [identifier for _, identifier in rows],
        }
    )


class TestSplitByDate:
    def test_single_date_returns_one_partition(self) -> None:
        rows = [
            (datetime(2026, 6, 1, 8, 0, tzinfo=UTC), 1),
            (datetime(2026, 6, 1, 9, 0, tzinfo=UTC), 2),
        ]
        partitions = split_by_date(_frame(rows), 'located_at')
        assert len(partitions) == 1
        partition_date, sub_frame = partitions[0]
        assert partition_date == date(2026, 6, 1)
        assert sub_frame.height == 2

    def test_multiple_dates_group_correctly(self) -> None:
        rows = [
            (datetime(2026, 6, 1, 8, 0, tzinfo=UTC), 1),
            (datetime(2026, 6, 2, 8, 0, tzinfo=UTC), 2),
            (datetime(2026, 6, 1, 9, 0, tzinfo=UTC), 3),
        ]
        by_date = dict(split_by_date(_frame(rows), 'located_at'))
        assert set(by_date) == {date(2026, 6, 1), date(2026, 6, 2)}
        assert by_date[date(2026, 6, 1)].get_column('id').to_list() == [1, 3]
        assert by_date[date(2026, 6, 2)].get_column('id').to_list() == [2]

    def test_utc_midnight_boundary_splits_adjacent_minutes(self) -> None:
        rows = [
            (datetime(2026, 6, 1, 23, 59, tzinfo=UTC), 1),
            (datetime(2026, 6, 2, 0, 1, tzinfo=UTC), 2),
        ]
        by_date = dict(split_by_date(_frame(rows), 'located_at'))
        assert by_date[date(2026, 6, 1)].get_column('id').to_list() == [1]
        assert by_date[date(2026, 6, 2)].get_column('id').to_list() == [2]

    def test_derived_column_dropped_and_schema_preserved(self) -> None:
        frame = _frame([(datetime(2026, 6, 1, 8, 0, tzinfo=UTC), 1)])
        _, sub_frame = split_by_date(frame, 'located_at')[0]
        assert sub_frame.schema == frame.schema

    def test_keys_are_pure_date_not_datetime(self) -> None:
        frame = _frame([(datetime(2026, 6, 1, 8, 0, tzinfo=UTC), 1)])
        partition_date, _ = split_by_date(frame, 'located_at')[0]
        assert isinstance(partition_date, date)
        assert not isinstance(partition_date, datetime)

    def test_empty_frame_returns_empty_list(self) -> None:
        empty = pl.DataFrame(
            schema={'located_at': pl.Datetime('us', 'UTC'), 'id': pl.Int64}
        )
        assert split_by_date(empty, 'located_at') == []

    def test_input_frame_is_not_mutated(self) -> None:
        frame = _frame(
            [
                (datetime(2026, 6, 1, 8, 0, tzinfo=UTC), 1),
                (datetime(2026, 6, 2, 8, 0, tzinfo=UTC), 2),
            ]
        )
        schema_before = frame.schema
        height_before = frame.height
        split_by_date(frame, 'located_at')
        assert frame.schema == schema_before
        assert frame.height == height_before
