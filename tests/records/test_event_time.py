# tests/records/test_event_time.py
"""Tests for fleetpull.records.event_time."""

from datetime import UTC, date, datetime

import polars as pl
import pytest

from fleetpull.records.event_time import latest_event_time
from fleetpull.timing import to_iso8601


def _frame(moments: list[datetime]) -> pl.DataFrame:
    return pl.DataFrame({'located_at': moments})


class TestLatestEventTime:
    def test_returns_the_maximum_timestamp(self) -> None:
        moments = [
            datetime(2026, 6, 1, 8, 0, tzinfo=UTC),
            datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
            datetime(2026, 6, 1, 9, 0, tzinfo=UTC),
        ]
        observed = latest_event_time(_frame(moments), 'located_at')
        assert observed is not None
        assert observed == datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
        # Identity, not just ==: datetime __eq__ compares instants and is
        # blind to a foreign zero-offset tzinfo -- the exact bug class the
        # ensure_utc ingress guards against.
        assert observed.tzinfo is UTC

    def test_single_row_returns_its_timestamp(self) -> None:
        moment = datetime(2026, 6, 1, 8, 0, tzinfo=UTC)
        observed = latest_event_time(_frame([moment]), 'located_at')
        assert observed is not None
        assert observed == moment
        assert observed.tzinfo is UTC

    def test_empty_frame_returns_none(self) -> None:
        empty = pl.DataFrame(schema={'located_at': pl.Datetime('us', 'UTC')})
        assert latest_event_time(empty, 'located_at') is None

    def test_non_datetime_column_raises(self) -> None:
        frame = pl.DataFrame({'located_at': [date(2026, 6, 1)]})
        with pytest.raises(TypeError, match='datetime'):
            latest_event_time(frame, 'located_at')

    def test_naive_datetime_column_raises(self) -> None:
        naive_frame = pl.DataFrame(
            {'located_at': [datetime(2026, 6, 1, 8, 0)]}  # noqa: DTZ001
        )
        with pytest.raises(ValueError, match='timezone-aware'):
            latest_event_time(naive_frame, 'located_at')

    def test_watermark_serialization_chain_survives_polars_egress(self) -> None:
        # The chain that crashed live: Polars materializes the column maximum
        # tagged zoneinfo.ZoneInfo('UTC'), and the watermark serialization's
        # strict identity guard (to_iso8601 via require_utc) rejected it. The
        # ensure_utc ingress in latest_event_time makes the round trip hold.
        moments = [
            datetime(2026, 6, 1, 8, 0, tzinfo=UTC),
            datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        ]
        observed = latest_event_time(_frame(moments), 'located_at')
        assert observed is not None
        assert observed.tzinfo is UTC
        assert to_iso8601(observed) == '2026-06-01T12:00:00Z'
