# tests/incremental/test_resume.py
"""Tests for fleetpull.incremental.resume."""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.incremental.cursor import DateWatermark
from fleetpull.incremental.resume import compute_resume
from fleetpull.incremental.window import DateWindow

NOW = datetime(2026, 6, 16, 12, 0, 0, tzinfo=UTC)


class TestComputeResume:
    def test_watermark_yields_a_lookback_window(self) -> None:
        watermark_moment = datetime(2026, 6, 10, 0, 0, 0, tzinfo=UTC)
        lookback = timedelta(days=2)
        window = compute_resume(
            DateWatermark(watermark=watermark_moment), lookback, NOW
        )
        assert window is not None
        assert window.start == datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC)
        assert window.end == NOW

    def test_zero_lookback_yields_watermark_to_now(self) -> None:
        watermark_moment = datetime(2026, 6, 10, 0, 0, 0, tzinfo=UTC)
        window = compute_resume(
            DateWatermark(watermark=watermark_moment), timedelta(0), NOW
        )
        assert window == DateWindow(start=watermark_moment, end=NOW)

    def test_none_watermark_returns_none(self) -> None:
        assert compute_resume(None, timedelta(days=1), NOW) is None

    @pytest.mark.parametrize(
        ('watermark_moment', 'lookback'),
        [
            (NOW + timedelta(days=1), timedelta(0)),  # start > now: inverted
            (NOW, timedelta(0)),  # start == now: empty
        ],
    )
    def test_future_watermark_inverts_and_raises(
        self, watermark_moment: datetime, lookback: timedelta
    ) -> None:
        with pytest.raises(ValueError, match='start < end'):
            compute_resume(DateWatermark(watermark=watermark_moment), lookback, NOW)
