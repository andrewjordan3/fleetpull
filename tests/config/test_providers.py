"""Tests for fleetpull.config.providers and fleetpull.config.root."""

from datetime import date

import pytest
from pydantic import ValidationError

from fleetpull.config import (
    FleetpullConfig,
    MotiveConfig,
    ProvidersConfig,
    StorageConfig,
    SyncConfig,
)


class TestProvidersConfig:
    def test_motive_defaults_to_absent(self) -> None:
        assert ProvidersConfig().motive is None

    def test_carries_a_motive_section(self) -> None:
        providers = ProvidersConfig(motive=MotiveConfig())
        assert providers.motive is not None

    def test_rejects_unknown_providers(self) -> None:
        with pytest.raises(ValidationError):
            ProvidersConfig(samsara={})  # type: ignore[call-arg]


class TestFleetpullConfig:
    def test_optional_sections_default_wholesale(self) -> None:
        config = FleetpullConfig(
            sync=SyncConfig(default_start_date=date(2026, 6, 1), dataset_root='/d'),
            storage=StorageConfig(dataset_root='/d'),
            providers=ProvidersConfig(),
        )
        assert config.state.database_path is None
        assert config.logging.file_path is None
        assert config.http.use_truststore is False
        assert config.retry.transient_max_failures == 3

    def test_requires_sync_storage_and_providers(self) -> None:
        with pytest.raises(ValidationError):
            FleetpullConfig()  # type: ignore[call-arg]

    def test_rejects_unknown_sections(self) -> None:
        with pytest.raises(ValidationError):
            FleetpullConfig(  # type: ignore[call-arg]
                sync=SyncConfig(default_start_date=date(2026, 6, 1), dataset_root='/d'),
                storage=StorageConfig(dataset_root='/d'),
                providers=ProvidersConfig(),
                sink={},
            )
