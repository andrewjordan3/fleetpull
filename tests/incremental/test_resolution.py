# tests/incremental/test_resolution.py
"""Tests for fleetpull.incremental.resolution."""

from datetime import UTC, datetime, timedelta

from fleetpull.incremental.resolution import (
    resolve_resume_start,
    resolve_trailing_edge,
    window_or_none,
)
from fleetpull.incremental.window import DateWindow


class TestResolveTrailingEdge:
    def test_floors_a_mid_day_now_to_today_midnight(self) -> None:
        now = datetime(2026, 6, 16, 14, 30, 45, tzinfo=UTC)
        edge = resolve_trailing_edge(now, timedelta(0))
        assert edge == datetime(2026, 6, 16, 0, 0, 0, tzinfo=UTC)

    def test_holds_the_edge_back_by_the_cutoff(self) -> None:
        now = datetime(2026, 6, 16, 14, 30, 45, tzinfo=UTC)
        edge = resolve_trailing_edge(now, timedelta(days=2))
        assert edge == datetime(2026, 6, 14, 0, 0, 0, tzinfo=UTC)

    def test_a_midnight_now_floors_to_itself(self) -> None:
        now = datetime(2026, 6, 16, 0, 0, 0, tzinfo=UTC)
        edge = resolve_trailing_edge(now, timedelta(0))
        assert edge == now

    def test_the_edge_carries_utc_tzinfo(self) -> None:
        edge = resolve_trailing_edge(
            datetime(2026, 6, 16, 14, 0, 0, tzinfo=UTC), timedelta(days=1)
        )
        assert edge.tzinfo is UTC


class TestResolveResumeStart:
    DEFAULT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    FRONTIER = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
    WATERMARK_START = datetime(2026, 6, 10, 0, 0, 0, tzinfo=UTC)

    def test_watermark_start_wins_when_present(self) -> None:
        start = resolve_resume_start(self.WATERMARK_START, self.FRONTIER, self.DEFAULT)
        assert start == self.WATERMARK_START

    def test_frontier_wins_when_no_watermark(self) -> None:
        start = resolve_resume_start(None, self.FRONTIER, self.DEFAULT)
        assert start == self.FRONTIER

    def test_default_when_no_watermark_or_frontier(self) -> None:
        start = resolve_resume_start(None, None, self.DEFAULT)
        assert start == self.DEFAULT

    def test_floors_the_watermark_arm_to_its_utc_midnight(self) -> None:
        # The live sliver defect: a watermark of 2026-06-30T23:59:59 less a
        # 1-day lookback yielded this unfloored arm, covering its boundary
        # date by one second -- the day-granular fetch returned the whole day
        # and the window filter kept only the sliver. Floored, lookback means
        # "re-cover N whole days before the watermark's day".
        unfloored_arm = datetime(2026, 6, 29, 23, 59, 59, tzinfo=UTC)
        start = resolve_resume_start(unfloored_arm, None, self.DEFAULT)
        assert start == datetime(2026, 6, 29, 0, 0, 0, tzinfo=UTC)
        assert start.tzinfo is UTC

    def test_floors_a_mid_day_frontier_to_its_utc_midnight(self) -> None:
        mid_day_frontier = datetime(2026, 3, 1, 12, 30, 45, tzinfo=UTC)
        start = resolve_resume_start(None, mid_day_frontier, self.DEFAULT)
        assert start == datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
        assert start.tzinfo is UTC

    def test_a_midnight_arm_floors_to_itself(self) -> None:
        start = resolve_resume_start(self.WATERMARK_START, None, self.DEFAULT)
        assert start == self.WATERMARK_START
        assert start.tzinfo is UTC


class TestWindowOrNone:
    EARLIER = datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC)
    LATER = datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC)

    def test_returns_a_window_when_start_before_end(self) -> None:
        window = window_or_none(self.EARLIER, self.LATER)
        assert window == DateWindow(start=self.EARLIER, end=self.LATER)

    def test_returns_none_when_start_equals_end(self) -> None:
        assert window_or_none(self.EARLIER, self.EARLIER) is None

    def test_returns_none_when_start_after_end(self) -> None:
        assert window_or_none(self.LATER, self.EARLIER) is None
