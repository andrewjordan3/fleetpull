"""Tests for fleetpull.endpoints.registry."""

from datetime import timedelta
from types import ModuleType

import pytest

from fleetpull.config import GeotabConfig, MotiveConfig
from fleetpull.endpoints.motive.vehicles import build_endpoint
from fleetpull.endpoints.registry import (
    EndpointRegistry,
    _module_roster_definitions,
    build_endpoint_registry,
    build_roster_registry,
)
from fleetpull.exceptions import ConfigurationError
from fleetpull.roster import RosterDefinition, RosterKey
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
        registry = build_endpoint_registry([MotiveConfig(), GeotabConfig()])
        assert registry.get(Provider.MOTIVE, 'vehicles').name == 'vehicles'
        assert (
            registry.get(Provider.MOTIVE, 'vehicle_locations').name
            == 'vehicle_locations'
        )

    def test_missing_config_raises_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError):
            build_endpoint_registry([])


class TestBuildRosterRegistry:
    def test_discovers_the_vehicle_ids_roster(self) -> None:
        registry = build_roster_registry()
        definition = registry.get(RosterKey(Provider.MOTIVE, 'vehicle_ids'))
        assert definition.source_endpoint == 'vehicles'
        assert definition.source_column == 'vehicle_id'

    def test_reverse_lookup_resolves_through_the_discovered_catalog(self) -> None:
        registry = build_roster_registry()
        sourced = registry.sourced_by(Provider.MOTIVE, 'vehicles')
        assert [definition.key.name for definition in sourced] == ['vehicle_ids']

    def test_unknown_key_fails_loudly_naming_it(self) -> None:
        registry = build_roster_registry()
        with pytest.raises(ConfigurationError, match='phantom'):
            registry.get(RosterKey(Provider.MOTIVE, 'phantom'))

    def test_collection_skips_underscore_private_constants(self) -> None:
        module = ModuleType('synthetic_leaf')
        public = RosterDefinition(
            key=RosterKey(Provider.MOTIVE, 'public_roster'),
            source_endpoint='vehicles',
            source_column='vehicle_id',
            max_age=timedelta(days=1),
            eviction_threshold=None,
        )
        private = RosterDefinition(
            key=RosterKey(Provider.MOTIVE, 'private_roster'),
            source_endpoint='vehicles',
            source_column='vehicle_id',
            max_age=timedelta(days=1),
            eviction_threshold=None,
        )
        module.PUBLIC_ROSTER = public  # type: ignore[attr-defined]
        module._PRIVATE_ROSTER = private  # type: ignore[attr-defined]
        assert _module_roster_definitions(module) == [public]
