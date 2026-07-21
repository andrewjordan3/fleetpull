"""Tests for fleetpull.orchestrator.resume."""

from datetime import UTC, datetime, timedelta

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import DateWatermark, FeedSeed, FeedToken
from fleetpull.orchestrator.resume import resolve_feed_resume, resolve_watermark_start
from fleetpull.vocabulary import Provider

_NOW = datetime(2026, 6, 16, tzinfo=UTC)
_LOOKBACK = timedelta(days=1)
_DEFAULT_START = datetime(2024, 1, 1, tzinfo=UTC)


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


class TestResolveFeedResume:
    def test_none_cursor_seeds_at_the_default_start(self) -> None:
        # The seed exists ONLY on the no-cursor branch -- the structural
        # half of the seed-once invariant (DESIGN section 14, I4).
        resume = resolve_feed_resume(
            None, _DEFAULT_START, Provider.GEOTAB, 'log_records'
        )
        assert resume == FeedSeed(start=_DEFAULT_START)

    def test_stored_token_resumes_directly(self) -> None:
        stored = FeedToken(from_version='0000000000000042')
        resume = resolve_feed_resume(
            stored, _DEFAULT_START, Provider.GEOTAB, 'log_records'
        )
        assert resume is stored

    def test_watermark_cursor_raises(self) -> None:
        # The cross-mode rejection in the feed direction -- the mirror of
        # resolve_watermark_start's feed-cursor rejection.
        with pytest.raises(ConfigurationError, match='watermark cursor'):
            resolve_feed_resume(
                DateWatermark(watermark=_NOW),
                _DEFAULT_START,
                Provider.GEOTAB,
                'log_records',
            )
