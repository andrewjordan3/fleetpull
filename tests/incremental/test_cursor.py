# tests/incremental/test_cursor.py
"""Tests for fleetpull.incremental.cursor."""

import dataclasses
from datetime import UTC, datetime

import pytest

from fleetpull.incremental.cursor import DateWatermark, FeedToken


class TestDateWatermark:
    def test_holds_the_watermark(self) -> None:
        moment = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
        assert DateWatermark(watermark=moment).watermark == moment

    def test_is_frozen(self) -> None:
        cursor = DateWatermark(watermark=datetime(2026, 6, 1, tzinfo=UTC))
        with pytest.raises(dataclasses.FrozenInstanceError):
            cursor.watermark = datetime(2026, 6, 2, tzinfo=UTC)  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        cursor = DateWatermark(watermark=datetime(2026, 6, 1, tzinfo=UTC))
        assert not hasattr(cursor, '__dict__')


class TestFeedToken:
    def test_holds_the_version(self) -> None:
        assert (
            FeedToken(from_version='0000000000000007').from_version
            == '0000000000000007'
        )

    def test_is_frozen(self) -> None:
        cursor = FeedToken(from_version='0000000000000007')
        with pytest.raises(dataclasses.FrozenInstanceError):
            cursor.from_version = '0000000000000008'  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(FeedToken(from_version='0000000000000007'), '__dict__')
