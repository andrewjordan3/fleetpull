"""Tests for fleetpull.config.sync."""

from datetime import date

import pytest
from pydantic import ValidationError

from fleetpull.config import SyncConfig


class TestSyncConfig:
    def test_accepts_a_start_date(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1))
        assert config.default_start_date == date(2024, 1, 1)

    def test_parses_an_iso_date_string(self) -> None:
        config = SyncConfig(default_start_date='2024-01-01')
        assert config.default_start_date == date(2024, 1, 1)

    def test_requires_a_start_date(self) -> None:
        with pytest.raises(ValidationError):
            SyncConfig()  # type: ignore[call-arg]

    def test_rejects_an_impossible_date(self) -> None:
        with pytest.raises(ValidationError):
            SyncConfig(default_start_date='2024-02-30')

    def test_is_frozen(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1))
        with pytest.raises(ValidationError):
            config.default_start_date = date(2025, 1, 1)  # type: ignore[misc]

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            SyncConfig(  # type: ignore[call-arg]
                default_start_date=date(2024, 1, 1), unknown='x'
            )
