"""Tests for fleetpull.timing.clock."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from fleetpull.timing.clock import Clock, FrozenClock, SystemClock

FROZEN_START_TIME: datetime = datetime(2026, 1, 23, 12, 0, tzinfo=UTC)
FIXED_OFFSET_TIMEZONE: timezone = timezone(timedelta(hours=-5))


@pytest.fixture
def frozen_clock() -> FrozenClock:
    return FrozenClock(start_time_utc=FROZEN_START_TIME, start_monotonic_seconds=100.0)


@pytest.fixture
def system_clock() -> SystemClock:
    return SystemClock()


class TestSystemClock:
    def test_now_utc_is_timezone_aware_utc(self, system_clock: SystemClock) -> None:
        assert system_clock.now_utc().tzinfo is UTC

    def test_today_utc_matches_now_utc_date(self, system_clock: SystemClock) -> None:
        assert system_clock.today_utc() == system_clock.now_utc().date()

    def test_monotonic_seconds_is_non_decreasing(
        self, system_clock: SystemClock
    ) -> None:
        first_reading: float = system_clock.monotonic_seconds()
        second_reading: float = system_clock.monotonic_seconds()
        assert second_reading >= first_reading


class TestFrozenClockConstruction:
    def test_rejects_naive_start_time(self) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            FrozenClock(start_time_utc=datetime(2026, 1, 23, 12, 0))  # noqa: DTZ001

    def test_rejects_non_utc_start_time(self) -> None:
        non_utc_start: datetime = datetime(
            2026, 1, 23, 12, 0, tzinfo=FIXED_OFFSET_TIMEZONE
        )
        with pytest.raises(ValueError, match=r'must use datetime\.UTC'):
            FrozenClock(start_time_utc=non_utc_start)

    def test_rejects_negative_start_monotonic_seconds(self) -> None:
        with pytest.raises(ValueError, match='non-negative'):
            FrozenClock(start_time_utc=FROZEN_START_TIME, start_monotonic_seconds=-1.0)


class TestFrozenClockBehavior:
    def test_time_does_not_move_without_mutation(
        self, frozen_clock: FrozenClock
    ) -> None:
        assert frozen_clock.now_utc() == frozen_clock.now_utc()
        assert frozen_clock.monotonic_seconds() == frozen_clock.monotonic_seconds()

    def test_advance_moves_wall_and_monotonic_together(
        self, frozen_clock: FrozenClock
    ) -> None:
        advance_delta: timedelta = timedelta(minutes=30)
        frozen_clock.advance(advance_delta)
        assert frozen_clock.now_utc() == FROZEN_START_TIME + advance_delta
        assert frozen_clock.monotonic_seconds() == 100.0 + advance_delta.total_seconds()

    def test_advance_rejects_negative_delta(self, frozen_clock: FrozenClock) -> None:
        with pytest.raises(ValueError, match='non-negative'):
            frozen_clock.advance(timedelta(seconds=-1))

    def test_set_time_moves_wall_time_only(self, frozen_clock: FrozenClock) -> None:
        new_time: datetime = datetime(2026, 2, 1, 8, 0, tzinfo=UTC)
        frozen_clock.set_time(new_time)
        assert frozen_clock.now_utc() == new_time
        assert frozen_clock.monotonic_seconds() == 100.0

    def test_set_time_rejects_naive_datetime(self, frozen_clock: FrozenClock) -> None:
        with pytest.raises(ValueError, match='timezone-aware'):
            frozen_clock.set_time(datetime(2026, 2, 1, 8, 0))  # noqa: DTZ001

    def test_set_time_rejects_non_utc_datetime(self, frozen_clock: FrozenClock) -> None:
        non_utc_time: datetime = datetime(
            2026, 2, 1, 8, 0, tzinfo=FIXED_OFFSET_TIMEZONE
        )
        with pytest.raises(ValueError, match=r'must use datetime\.UTC'):
            frozen_clock.set_time(non_utc_time)


class TestClockProtocol:
    def test_system_clock_satisfies_protocol(self, system_clock: SystemClock) -> None:
        assert isinstance(system_clock, Clock)

    def test_frozen_clock_satisfies_protocol(self, frozen_clock: FrozenClock) -> None:
        assert isinstance(frozen_clock, Clock)
