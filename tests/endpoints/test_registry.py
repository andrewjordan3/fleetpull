"""Tests for fleetpull.endpoints.registry."""

import pytest

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive.vehicles import build_endpoint
from fleetpull.endpoints.registry import EndpointRegistry, build_endpoint_registry
from fleetpull.exceptions import ConfigurationError
from fleetpull.vocabulary import Provider


class TestEndpointRegistry:
    def test_get_returns_the_registered_definition(self) -> None:
        definition = build_endpoint(MotiveConfig())
        registry = EndpointRegistry([definition])
        assert registry.get(Provider.MOTIVE, 'vehicles') is definition

    def test_get_unknown_raises_configuration_error(self) -> None:
        registry = EndpointRegistry([build_endpoint(MotiveConfig())])
        with pytest.raises(ConfigurationError):
            registry.get(Provider.MOTIVE, 'nonexistent')

    def test_duplicate_key_raises_configuration_error(self) -> None:
        definition = build_endpoint(MotiveConfig())
        with pytest.raises(ConfigurationError):
            EndpointRegistry([definition, definition])


class TestBuildEndpointRegistry:
    def test_discovers_the_motive_endpoints(self) -> None:
        registry = build_endpoint_registry([MotiveConfig()])
        assert registry.get(Provider.MOTIVE, 'vehicles').name == 'vehicles'
        assert (
            registry.get(Provider.MOTIVE, 'vehicle_locations').name
            == 'vehicle_locations'
        )

    def test_missing_config_raises_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError):
            build_endpoint_registry([])
