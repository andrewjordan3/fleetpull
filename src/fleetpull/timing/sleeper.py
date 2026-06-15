# src/fleetpull/timing/sleeper.py
"""Injectable sleep seam for backoff waiting.

A one-method seam kept separate from ``Clock``: ``Clock`` reads time
(``now_utc`` / ``today_utc`` / ``monotonic_seconds``) and never consumes it.
Code that must pause — the transport client's TRANSIENT backoff — takes a
``Sleeper`` so tests substitute a recording double and never wait real time,
exactly as ``Clock`` is injected to make time deterministic.
"""

import time
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

__all__: list[str] = [
    'Sleeper',
    'SystemSleeper',
]


@runtime_checkable
class Sleeper(Protocol):
    """
    Interface for sleep providers.

    One capability — pause the calling thread for a duration — so the seam is
    a single method (the ``Clock`` / ``RandomFractionGenerator`` precedent).
    Tests inject a recording double; production injects ``SystemSleeper``.
    """

    def sleep(self, seconds: float) -> None:
        """
        Block the calling thread for ``seconds``.

        Args:
            seconds: Non-negative duration to sleep. Behavior on a negative
                value is the implementation's (``SystemSleeper`` defers to
                ``time.sleep``, which raises ``ValueError``).
        """
        ...


@dataclass(frozen=True, slots=True)
class SystemSleeper:
    """
    Production sleeper backed by ``time.sleep``.

    Frozen and stateless, so one shared instance is safe to inject anywhere
    (the ``SystemClock`` precedent).
    """

    def sleep(self, seconds: float) -> None:
        """
        Sleep via ``time.sleep``.

        Args:
            seconds: Non-negative duration to sleep.

        Side Effects:
            Blocks the calling thread for ``seconds``.

        Raises:
            ValueError: If ``seconds`` is negative (raised by ``time.sleep``).
        """
        time.sleep(seconds)
