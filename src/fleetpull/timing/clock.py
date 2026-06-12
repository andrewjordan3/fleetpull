# src/fleetpull/timing/clock.py
"""Time abstraction for fleetpull.

Provides an injectable clock interface to avoid scattered
``datetime.now()`` calls and enable deterministic time in tests.
Anything in fleetpull that needs the current time — rate-limiter token
refill, watermark computation, fetch-window resolution, run-ledger
timestamps — takes a clock rather than calling the standard library
directly. ``Clock`` is the Protocol defining the time-provider
interface; ``SystemClock`` is the production implementation backed by
system time; ``FrozenClock`` is the test implementation fixed at a
specific moment.
"""

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Protocol, runtime_checkable

__all__: list[str] = [
    'Clock',
    'FrozenClock',
    'SystemClock',
]


@runtime_checkable
class Clock(Protocol):
    """
    Interface for time providers in fleetpull.

    Centralizes time access (no scattered ``datetime.now()`` calls),
    enables deterministic time in tests, and enforces timezone-aware
    UTC timestamps internally. All implementations must return
    timezone-aware UTC datetimes.
    """

    def now_utc(self) -> datetime:
        """
        Return the current time as a timezone-aware UTC datetime.

        Returns:
            Timezone-aware datetime in UTC.
        """
        ...

    def today_utc(self) -> date:
        """
        Return today's date in UTC.

        Returns:
            Current UTC date.
        """
        ...

    def monotonic_seconds(self) -> float:
        """
        Return a monotonic timestamp for duration measurement.

        Monotonic clocks are unaffected by NTP adjustments or DST changes,
        making them suitable for measuring elapsed time.

        Returns:
            Monotonic time in seconds.
        """
        ...


@dataclass(frozen=True, slots=True)
class SystemClock:
    """
    Production clock backed by system wall-clock and monotonic timer.

    Returns timezone-aware UTC datetimes. This is the default clock
    for production use.
    """

    def now_utc(self) -> datetime:
        """Return current UTC time."""
        return datetime.now(tz=UTC)

    def today_utc(self) -> date:
        """Return current UTC date."""
        return self.now_utc().date()

    def monotonic_seconds(self) -> float:
        """
        Return monotonic time from ``time.perf_counter()``.

        The invariant is monotonic, never wall clock.
        ``time.perf_counter()`` is the deliberate choice over
        ``time.monotonic()``: on Windows under CPython 3.12,
        ``monotonic()`` is GetTickCount64 with roughly 15.6 ms
        resolution, while ``perf_counter()`` is
        QueryPerformanceCounter — and the test convention of real
        threads with tiny waits needs the finer clock on exactly that
        platform.
        """
        return time.perf_counter()


class FrozenClock:
    """
    Deterministic clock for tests and reproducible runs.

    Starts at a fixed UTC datetime and only advances when explicitly
    mutated via ``advance()`` or ``set_time()``. Not thread-safe: keep
    usage test-scoped or wrap externally.

    Example:
        >>> clock = FrozenClock(start_time_utc=datetime(2026, 1, 23, 12, tzinfo=UTC))
        >>> clock.now_utc()
        datetime.datetime(2026, 1, 23, 12, 0, tzinfo=datetime.timezone.utc)
        >>> clock.advance(timedelta(hours=1))
        >>> clock.now_utc().hour
        13
    """

    __slots__ = ('_current_monotonic_seconds', '_current_time_utc')

    def __init__(
        self,
        *,
        start_time_utc: datetime,
        start_monotonic_seconds: float = 0.0,
    ) -> None:
        """
        Initialize a frozen clock.

        Args:
            start_time_utc: Initial time (must be timezone-aware UTC).
            start_monotonic_seconds: Initial monotonic value (non-negative).

        Raises:
            ValueError: If start_time_utc is naive or not UTC.
            ValueError: If start_monotonic_seconds is negative.
        """
        if start_time_utc.tzinfo is None:
            raise ValueError('start_time_utc must be timezone-aware (UTC)')
        if start_time_utc.tzinfo is not UTC:
            raise ValueError('start_time_utc must use datetime.UTC')
        if start_monotonic_seconds < 0.0:
            raise ValueError('start_monotonic_seconds must be non-negative')

        self._current_time_utc: datetime = start_time_utc
        self._current_monotonic_seconds: float = start_monotonic_seconds

    def now_utc(self) -> datetime:
        """Return the frozen UTC time."""
        return self._current_time_utc

    def today_utc(self) -> date:
        """Return the frozen UTC date."""
        return self._current_time_utc.date()

    def monotonic_seconds(self) -> float:
        """Return the frozen monotonic value."""
        return self._current_monotonic_seconds

    def advance(self, delta: timedelta) -> None:
        """
        Advance the clock by a duration.

        Advances wall-clock and monotonic time together, keeping them
        correlated the way real time behaves.

        Args:
            delta: Time to advance (must be non-negative).

        Raises:
            ValueError: If delta is negative.
        """
        if delta.total_seconds() < 0:
            raise ValueError('delta must be non-negative')

        self._current_time_utc += delta
        self._current_monotonic_seconds += delta.total_seconds()

    def set_time(self, new_time_utc: datetime) -> None:
        """
        Set the clock to a specific UTC time.

        Does not adjust the monotonic counter — use ``advance()`` for
        correlated wall/monotonic changes.

        Args:
            new_time_utc: New time (must be timezone-aware UTC).

        Raises:
            ValueError: If new_time_utc is naive or not UTC.
        """
        if new_time_utc.tzinfo is None:
            raise ValueError('new_time_utc must be timezone-aware (UTC)')
        if new_time_utc.tzinfo is not UTC:
            raise ValueError('new_time_utc must use datetime.UTC')

        self._current_time_utc = new_time_utc
