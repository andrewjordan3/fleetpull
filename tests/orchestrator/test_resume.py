"""Tests for fleetpull.orchestrator.resume."""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import DateWatermark, FeedToken
from fleetpull.orchestrator.resume import resolve_watermark_start
from fleetpull.vocabulary import Provider

_NOW = datetime(2026, 6, 16, tzinfo=UTC)
_LOOKBACK = timedelta(days=1)


class TestResolveWatermarkStart:
    def test_none_cursor_returns_none(self) -> None:
        start = resolve_watermark_start(
            None, _LOOKBACK, _NOW, Provider.MOTIVE, 'locations'
        )
        assert start is None

    def test_past_watermark_returns_watermark_minus_lookback(self) -> None:
        stored = DateWatermark(watermark=datetime(2026, 6, 10, 8, tzinfo=UTC))
        start = resolve_watermark_start(
            stored, _LOOKBACK, _NOW, Provider.MOTIVE, 'locations'
        )
        assert start == datetime(2026, 6, 9, 8, tzinfo=UTC)

    def test_watermark_exactly_at_now_is_allowed(self) -> None:
        # Guard A is strict ``>``; a watermark at the clock instant is not future.
        stored = DateWatermark(watermark=_NOW)
        start = resolve_watermark_start(
            stored, _LOOKBACK, _NOW, Provider.MOTIVE, 'locations'
        )
        assert start == _NOW - _LOOKBACK

    def test_future_watermark_raises(self) -> None:
        stored = DateWatermark(watermark=datetime(2026, 6, 20, tzinfo=UTC))
        with pytest.raises(ConfigurationError, match='future'):
            resolve_watermark_start(
                stored, _LOOKBACK, _NOW, Provider.MOTIVE, 'locations'
            )

    def test_feed_cursor_raises(self) -> None:
        with pytest.raises(ConfigurationError, match='feed cursor'):
            resolve_watermark_start(
                FeedToken(from_version='v1'),
                _LOOKBACK,
                _NOW,
                Provider.MOTIVE,
                'locations',
            )
