"""Tests for fleetpull.config.sync."""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from fleetpull.config import SyncConfig

_ROOT = Path('/data')


class TestSyncConfig:
    def test_accepts_a_start_date(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1), dataset_root=_ROOT)
        assert config.default_start_date == date(2024, 1, 1)

    def test_parses_an_iso_date_string(self) -> None:
        config = SyncConfig(default_start_date='2024-01-01', dataset_root=_ROOT)
        assert config.default_start_date == date(2024, 1, 1)

    def test_requires_a_start_date(self) -> None:
        with pytest.raises(ValidationError):
            SyncConfig(dataset_root=_ROOT)  # type: ignore[call-arg]

    def test_requires_a_dataset_root(self) -> None:
        with pytest.raises(ValidationError):
            SyncConfig(default_start_date=date(2024, 1, 1))  # type: ignore[call-arg]

    def test_string_dataset_root_coerces_to_path(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1), dataset_root='/data')
        assert config.dataset_root == Path('/data')

    def test_rejects_an_impossible_date(self) -> None:
        with pytest.raises(ValidationError):
            SyncConfig(default_start_date='2024-02-30', dataset_root=_ROOT)

    def test_is_frozen(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1), dataset_root=_ROOT)
        with pytest.raises(ValidationError):
            config.default_start_date = date(2025, 1, 1)  # type: ignore[misc]

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            SyncConfig(  # type: ignore[call-arg]
                default_start_date=date(2024, 1, 1), dataset_root=_ROOT, unknown='x'
            )

    def test_default_start_datetime_lifts_to_utc_midnight(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1), dataset_root=_ROOT)
        assert config.default_start_datetime == datetime(2024, 1, 1, tzinfo=UTC)
        assert config.default_start_datetime.tzinfo is UTC
