"""Injectable time abstraction: Clock Protocol and its implementations."""

from fleetpull.timing.clock import Clock, FrozenClock, SystemClock

__all__: list[str] = ['Clock', 'FrozenClock', 'SystemClock']
