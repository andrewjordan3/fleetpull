"""Tests for fleetpull.roster.definition."""

import dataclasses
from datetime import timedelta

import pytest

from fleetpull.roster.definition import RosterDefinition
from fleetpull.roster.key import RosterKey
from fleetpull.vocabulary import Provider


def _definition() -> RosterDefinition:
    return RosterDefinition(
        key=RosterKey(Provider.MOTIVE, 'vehicle_ids'),
        source_endpoint='vehicles',
        source_column='vehicle_id',
        max_age=timedelta(days=1),
        eviction_threshold=5,
    )


class TestRosterDefinition:
    def test_holds_its_fields(self) -> None:
        definition = _definition()
        assert definition.key == RosterKey(Provider.MOTIVE, 'vehicle_ids')
        assert definition.source_endpoint == 'vehicles'
        assert definition.source_column == 'vehicle_id'
        assert definition.max_age == timedelta(days=1)
        assert definition.eviction_threshold == 5

    def test_accepts_a_none_eviction_threshold(self) -> None:
        definition = RosterDefinition(
            key=RosterKey(Provider.GEOTAB, 'device_ids'),
            source_endpoint='devices',
            source_column='device_id',
            max_age=timedelta(hours=6),
            eviction_threshold=None,
        )
        assert definition.eviction_threshold is None

    def test_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            _definition().source_column = 'other'  # type: ignore[misc]
