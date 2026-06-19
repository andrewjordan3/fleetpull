# tests/records/test_event_time.py
"""Tests for fleetpull.records.event_time."""

from datetime import UTC, date, datetime

import polars as pl
import pytest

from fleetpull.records.event_time import latest_event_time


def _frame(moments: list[datetime]) -> pl.DataFrame:
    return pl.DataFrame({'located_at': moments})


class TestLatestEventTime:
    def test_returns_the_maximum_timestamp(self) -> None:
        moments = [
            datetime(2026, 6, 1, 8, 0, tzinfo=UTC),
            datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
            datetime(2026, 6, 1, 9, 0, tzinfo=UTC),
        ]
        assert latest_event_time(_frame(moments), 'located_at') == datetime(
            2026, 6, 1, 12, 0, tzinfo=UTC
        )

    def test_single_row_returns_its_timestamp(self) -> None:
        moment = datetime(2026, 6, 1, 8, 0, tzinfo=UTC)
        assert latest_event_time(_frame([moment]), 'located_at') == moment

    def test_empty_frame_returns_none(self) -> None:
        empty = pl.DataFrame(schema={'located_at': pl.Datetime('us', 'UTC')})
        assert latest_event_time(empty, 'located_at') is None

    def test_non_datetime_column_raises(self) -> None:
        frame = pl.DataFrame({'located_at': [date(2026, 6, 1)]})
        with pytest.raises(TypeError, match='datetime'):
            latest_event_time(frame, 'located_at')
