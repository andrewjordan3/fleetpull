"""Tests for fleetpull.endpoints.shared.resume."""

from datetime import UTC, datetime

import pytest

from fleetpull.endpoints.shared import require_date_window
from fleetpull.incremental import DateWindow, FeedToken


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
