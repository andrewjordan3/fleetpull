"""Tests for fleetpull.roster.registry."""

from datetime import timedelta

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.roster.definition import RosterDefinition
from fleetpull.roster.key import RosterKey
from fleetpull.roster.registry import RosterRegistry
from fleetpull.vocabulary import Provider


def _definition(name: str, *, column: str = 'vehicle_id') -> RosterDefinition:
    return RosterDefinition(
        key=RosterKey(Provider.MOTIVE, name),
        source_endpoint='vehicles',
        source_column=column,
        max_age=timedelta(days=1),
        eviction_threshold=5,
    )


class TestRosterRegistry:
    def test_resolves_a_registered_key(self) -> None:
        definition = _definition('vehicle_ids')
        registry = RosterRegistry([definition])
        assert registry.get(RosterKey(Provider.MOTIVE, 'vehicle_ids')) is definition

    def test_unknown_key_raises(self) -> None:
        registry = RosterRegistry([_definition('vehicle_ids')])
        with pytest.raises(ConfigurationError, match='unknown roster'):
            registry.get(RosterKey(Provider.MOTIVE, 'driver_ids'))

    def test_duplicate_key_raises_at_construction(self) -> None:
        with pytest.raises(ConfigurationError, match='duplicate roster'):
            RosterRegistry(
                [_definition('vehicle_ids'), _definition('vehicle_ids', column='vin')]
            )

    def test_empty_registry_resolves_nothing(self) -> None:
        registry = RosterRegistry([])
        with pytest.raises(ConfigurationError, match='unknown roster'):
            registry.get(RosterKey(Provider.MOTIVE, 'vehicle_ids'))
