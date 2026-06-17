# tests/incremental/test_window.py
"""Tests for fleetpull.incremental.window."""

import dataclasses
from datetime import UTC, datetime

import pytest

from fleetpull.incremental.window import DateWindow

START = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
END = datetime(2026, 6, 2, 0, 0, 0, tzinfo=UTC)


class TestDateWindow:
    def test_holds_start_and_end(self) -> None:
        window = DateWindow(start=START, end=END)
        assert window.start == START
        assert window.end == END

    def test_is_frozen(self) -> None:
        window = DateWindow(start=START, end=END)
        with pytest.raises(dataclasses.FrozenInstanceError):
            window.start = END  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(DateWindow(start=START, end=END), '__dict__')

    def test_rejects_an_inverted_window(self) -> None:
        with pytest.raises(ValueError, match='start < end'):
            DateWindow(start=END, end=START)

    def test_rejects_an_empty_window(self) -> None:
        with pytest.raises(ValueError, match='start < end'):
            DateWindow(start=START, end=START)
