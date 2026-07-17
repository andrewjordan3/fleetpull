# tests/state/conftest.py
"""Shared fixtures and constants for the state-store tests.

Every store test runs against a real migrated SQLite file under ``tmp_path``
and a frozen clock; the shared database path, the frozen instant, and the
advanceable clock live here so the six modules don't each carry a copy.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from fleetpull.timing.clock import FrozenClock

# Deliberately distinct from any instant the tests store as data, so a stamped
# column provably came from the clock rather than a payload value.
FROZEN_INSTANT: datetime = datetime(2026, 6, 16, 9, 30, 0, tzinfo=UTC)


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    """The state database file path shared by the fixtures and raw-row helpers."""
    return tmp_path / 'state.sqlite3'


@pytest.fixture
def frozen_clock() -> FrozenClock:
    """A clock fixed at FROZEN_INSTANT, shared with the store fixtures."""
    return FrozenClock(start_time_utc=FROZEN_INSTANT)
