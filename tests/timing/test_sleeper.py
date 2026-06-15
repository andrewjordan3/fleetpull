"""Tests for fleetpull.timing.sleeper."""

import pytest

from fleetpull.timing.sleeper import Sleeper, SystemSleeper


class TestSystemSleeper:
    def test_satisfies_sleeper_protocol(self) -> None:
        assert isinstance(SystemSleeper(), Sleeper)

    def test_has_slots_no_dict(self) -> None:
        assert not hasattr(SystemSleeper(), '__dict__')

    def test_zero_duration_returns(self) -> None:
        # Exercises the call path without waiting real time.
        SystemSleeper().sleep(0.0)

    def test_negative_duration_raises(self) -> None:
        # time.sleep raises before sleeping, so no real time elapses.
        with pytest.raises(ValueError, match='non-negative'):
            SystemSleeper().sleep(-1.0)
