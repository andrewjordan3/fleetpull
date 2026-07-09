"""Tests for fleetpull.config.sections (SyncConfig, StorageConfig, StateConfig)."""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from fleetpull.config import StateConfig, StorageConfig, SyncConfig
from fleetpull.paths import resolve_path


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

    def test_carries_no_dataset_root(self) -> None:
        # dataset_root's one home is StorageConfig; a sync key is unknown.
        with pytest.raises(ValidationError, match='dataset_root'):
            SyncConfig(  # type: ignore[call-arg]
                default_start_date=date(2024, 1, 1), dataset_root='/data'
            )

    def test_window_knobs_default_to_none(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1))
        assert config.lookback_days is None
        assert config.cutoff_days is None

    def test_backfill_chunk_days_defaults_to_a_week(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1))
        assert config.backfill_chunk_days == 7

    def test_accepts_a_backfill_chunk_override(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1), backfill_chunk_days=1)
        assert config.backfill_chunk_days == 1

    def test_rejects_a_zero_backfill_chunk(self) -> None:
        with pytest.raises(ValidationError, match='backfill_chunk_days'):
            SyncConfig(default_start_date=date(2024, 1, 1), backfill_chunk_days=0)

    def test_accepts_window_knobs(self) -> None:
        config = SyncConfig(
            default_start_date=date(2024, 1, 1), lookback_days=3, cutoff_days=1
        )
        assert config.lookback_days == 3
        assert config.cutoff_days == 1

    @pytest.mark.parametrize('knob', ['lookback_days', 'cutoff_days'])
    def test_rejects_negative_window_knobs(self, knob: str) -> None:
        with pytest.raises(ValidationError):
            SyncConfig(default_start_date=date(2024, 1, 1), **{knob: -1})

    def test_default_start_datetime_lifts_to_utc_midnight(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1))
        assert config.default_start_datetime == datetime(2024, 1, 1, tzinfo=UTC)
        assert config.default_start_datetime.tzinfo is UTC

    def test_is_frozen(self) -> None:
        config = SyncConfig(default_start_date=date(2024, 1, 1))
        with pytest.raises(ValidationError):
            config.default_start_date = date(2025, 1, 1)  # type: ignore[misc]


class TestStorageConfig:
    def test_requires_a_dataset_root(self) -> None:
        with pytest.raises(ValidationError):
            StorageConfig()  # type: ignore[call-arg]

    def test_normalizes_through_resolve_path(self) -> None:
        config = StorageConfig(dataset_root='~/data/../data/fleet')
        assert config.dataset_root == resolve_path('~/data/../data/fleet')
        assert config.dataset_root.is_absolute()

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            StorageConfig(dataset_root='/data', unknown='x')  # type: ignore[call-arg]

    def test_is_frozen(self) -> None:
        config = StorageConfig(dataset_root='/data')
        with pytest.raises(ValidationError):
            config.dataset_root = Path('/other')  # type: ignore[misc]


class TestStateConfig:
    def test_database_path_defaults_to_none(self) -> None:
        assert StateConfig().database_path is None

    def test_normalizes_through_resolve_path(self) -> None:
        config = StateConfig(database_path='~/state/../state/db.sqlite3')
        assert config.database_path == resolve_path('~/state/../state/db.sqlite3')

    def test_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            StateConfig(unknown='x')  # type: ignore[call-arg]

    def test_is_frozen(self) -> None:
        config = StateConfig()
        with pytest.raises(ValidationError):
            config.database_path = Path('/x')  # type: ignore[misc]
