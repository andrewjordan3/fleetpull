"""Tests for fleetpull.endpoints.shared.resume."""

from datetime import UTC, datetime

import pytest

from fleetpull.endpoints.shared import require_date_window, require_feed_resume
from fleetpull.incremental import DateWindow, FeedSeed, FeedToken


def _window() -> DateWindow:
    return DateWindow(
        start=datetime(2026, 7, 6, tzinfo=UTC),
        end=datetime(2026, 7, 13, tzinfo=UTC),
    )


class TestRequireDateWindow:
    def test_returns_the_window_unchanged(self) -> None:
        window = _window()
        assert require_date_window(window, 'SomeBuilder') is window

    def test_rejects_none_naming_the_requirer(self) -> None:
        with pytest.raises(
            TypeError,
            match=r'SomeBuilder requires a DateWindow resume, got NoneType\.',
        ):
            require_date_window(None, 'SomeBuilder')

    def test_rejects_a_feed_token_naming_its_type(self) -> None:
        with pytest.raises(TypeError, match='got FeedToken'):
            require_date_window(
                FeedToken(from_version='0000000000000000'), 'SomeBuilder'
            )


class TestRequireFeedResume:
    def test_returns_a_token_unchanged(self) -> None:
        token = FeedToken(from_version='0000000000000000')
        assert require_feed_resume(token, 'FeedBuilder') is token

    def test_returns_a_seed_unchanged(self) -> None:
        seed = FeedSeed(start=datetime(2024, 1, 1, tzinfo=UTC))
        assert require_feed_resume(seed, 'FeedBuilder') is seed

    def test_rejects_none_naming_the_requirer(self) -> None:
        # A feed endpoint always resumes from something: seed or token.
        with pytest.raises(
            TypeError,
            match=r'FeedBuilder requires a FeedSeed or FeedToken resume, '
            r'got NoneType\.',
        ):
            require_feed_resume(None, 'FeedBuilder')

    def test_rejects_a_date_window_naming_its_type(self) -> None:
        with pytest.raises(TypeError, match='got DateWindow'):
            require_feed_resume(_window(), 'FeedBuilder')
