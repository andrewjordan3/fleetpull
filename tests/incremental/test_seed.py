# tests/incremental/test_seed.py
"""Tests for fleetpull.incremental.seed."""

import dataclasses
from datetime import UTC, datetime

import pytest

from fleetpull.incremental.seed import FeedSeed


class TestFeedSeed:
    def test_holds_the_start(self) -> None:
        moment = datetime(2024, 1, 1, tzinfo=UTC)
        assert FeedSeed(start=moment).start == moment

    def test_is_frozen(self) -> None:
        seed = FeedSeed(start=datetime(2024, 1, 1, tzinfo=UTC))
        with pytest.raises(dataclasses.FrozenInstanceError):
            seed.start = datetime(2024, 1, 2, tzinfo=UTC)  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(FeedSeed(start=datetime(2024, 1, 1, tzinfo=UTC)), '__dict__')
