"""Tests for fleetpull.models.samsara.address.

Every fixture is the committed 2026-07-20 capture set
(``tests/samsara_addresses_capture.py``): four fully synthetic records
shaped by the full-population walk (one page, 25 records), covering
every modeled arm -- the one circle geofence, three polygon geofences
(the polygon block unmodeled by design), a settings carrier, a record
missing ``addressTypes``, and one ``tags`` carrier. The fixture
properties the capture module promises are asserted here beside the
model they serve.
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import (
    Address,
    AddressGeofence,
    AddressGeofenceCircle,
    AddressGeofenceSettings,
)
from fleetpull.vocabulary import JsonObject
from tests.samsara_addresses_capture import ADDRESS_RECORDS

_REQUIRED_KEYS = frozenset(
    {
        'id',
        'name',
        'createdAtTime',
        'formattedAddress',
        'latitude',
        'longitude',
        'geofence',
    }
)


def _geofence_block(record: JsonObject) -> JsonObject:
    geofence = record['geofence']
    assert isinstance(geofence, dict)
    return geofence


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_the_variant_split(self) -> None:
        assert len(ADDRESS_RECORDS) == 4
        circles = [r for r in ADDRESS_RECORDS if 'circle' in _geofence_block(r)]
        polygons = [r for r in ADDRESS_RECORDS if 'polygon' in _geofence_block(r)]
        assert len(circles) == 1
        assert len(polygons) == 3

    def test_circle_and_polygon_never_co_occur(self) -> None:
        # 1 vs 24 in census, never both; the fixtures mirror the
        # exclusivity without the model enforcing it.
        for record in ADDRESS_RECORDS:
            geofence = _geofence_block(record)
            assert not ({'circle', 'polygon'} <= set(geofence))

    def test_every_record_carries_the_required_keys(self) -> None:
        # The seven 25/25 census keys ride every fixture record -- the
        # whole-population walk is what makes them required fields.
        for record in ADDRESS_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_the_carrier_counts(self) -> None:
        assert sum('tags' in r for r in ADDRESS_RECORDS) == 1
        assert sum('addressTypes' not in r for r in ADDRESS_RECORDS) == 1
        assert sum('settings' in _geofence_block(r) for r in ADDRESS_RECORDS) == 2


class TestAddressValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        # The §8 decision-2 posture with teeth: the whole-population walk
        # made these seven keys required, and only a loud rejection here
        # keeps a future optional-demotion from passing every gate.
        record = {
            key: value
            for key, value in ADDRESS_RECORDS[0].items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            Address.model_validate(record)

    def test_every_record_validates_with_aware_datetimes(self) -> None:
        validated = [Address.model_validate(record) for record in ADDRESS_RECORDS]
        assert len(validated) == 4
        for address in validated:
            assert address.created_at_time.tzinfo is not None
            assert isinstance(address.geofence, AddressGeofence)

    def test_the_maximal_polygon_record_pins_the_wire_values(self) -> None:
        address = Address.model_validate(ADDRESS_RECORDS[0])
        assert address.id == 'addr-001'
        assert address.name == 'Depot North'
        assert address.formatted_address == '100 Example St, Example City, TX 75001'
        assert address.latitude == 33.0001
        assert address.longitude == -97.0001
        assert address.address_types == ['yard']
        assert address.created_at_time == datetime(
            2022, 3, 15, 14, 2, 33, 123000, tzinfo=UTC
        )

    def test_the_circle_geofence_lands(self) -> None:
        address = Address.model_validate(ADDRESS_RECORDS[2])
        circle = address.geofence.circle
        assert isinstance(circle, AddressGeofenceCircle)
        assert circle.latitude == 33.2501
        assert circle.longitude == -96.2501
        assert circle.radius_meters == 150

    def test_the_settings_block_lands(self) -> None:
        validated = [Address.model_validate(record) for record in ADDRESS_RECORDS]
        settings = [
            a.geofence.settings for a in validated if a.geofence.settings is not None
        ]
        assert len(settings) == 2
        assert all(isinstance(s, AddressGeofenceSettings) for s in settings)
        assert {s.show_addresses for s in settings} == {True, False}

    def test_a_polygon_only_geofence_lands_both_modeled_fields_none(self) -> None:
        address = Address.model_validate(ADDRESS_RECORDS[1])
        assert address.geofence.circle is None
        assert address.geofence.settings is None

    def test_missing_address_types_is_none(self) -> None:
        address = Address.model_validate(ADDRESS_RECORDS[1])
        assert address.address_types is None

    def test_the_two_element_address_types_list(self) -> None:
        address = Address.model_validate(ADDRESS_RECORDS[2])
        assert address.address_types == ['yard', 'industrial']


class TestExcludedFields:
    def test_tags_are_not_modeled(self) -> None:
        # The list-of-structs exclusion (Device/User precedent, same as
        # the vehicles/drivers models): tags ride the maximal fixture
        # and must land nowhere.
        tags_carrier = ADDRESS_RECORDS[0]
        assert 'tags' in tags_carrier
        address = Address.model_validate(tags_carrier)
        assert not hasattr(address, 'tags')
        assert 'tags' not in Address.model_fields

    def test_polygon_is_not_modeled_and_the_center_point_survives(self) -> None:
        # The exclusion precedent one level down: polygon's only content
        # is a vertices list-of-objects, so the block is excluded
        # wholesale and extra='ignore' drops it -- while the top-level
        # latitude/longitude keep the polygon address's location.
        polygon_carrier = ADDRESS_RECORDS[1]
        assert 'polygon' in _geofence_block(polygon_carrier)
        address = Address.model_validate(polygon_carrier)
        assert not hasattr(address.geofence, 'polygon')
        assert 'polygon' not in AddressGeofence.model_fields
        assert address.latitude == 32.5001
        assert address.longitude == -96.5001
