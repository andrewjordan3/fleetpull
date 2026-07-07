"""Tests for fleetpull.models.motive.vehicle_locations."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.motive.vehicle_locations import (
    VehicleLocation,
    VehicleLocationType,
)
from fleetpull.vocabulary import JsonObject

# Obviously-synthetic identifiers — never real device/driver IDs in commits.
_LOCATION_ID: str = '11111111-1111-4111-8111-111111111111'
_DEVICE_ID: int = 1000001
_DRIVER_ID: int = 1000002
_ODOMETER: float = 123456.78901234567


def _canonical_record() -> JsonObject:
    """A scrubbed real ``/v3/vehicle_locations`` inner record (every value fake).

    Returns:
        A wire-shaped mapping using Motive's response keys, populated with
        synthetic data, suitable for ``VehicleLocation.model_validate``.
    """
    return {
        'located_at': '2026-06-01T12:00:00Z',
        'lat': 41.85,
        'lon': -87.65,
        'id': _LOCATION_ID,
        'type': 'breadcrumb',
        'description': 'Anytown, ST',
        'speed': None,
        'bearing': 12.99,
        'battery_voltage': None,
        'odometer': _ODOMETER,
        'true_odometer': _ODOMETER,
        'engine_hours': 9876.54321,
        'true_engine_hours': 9876.54321,
        'fuel': 54321.0123456789,
        'fuel_primary_remaining_percentage': None,
        'fuel_secondary_remaining_percentage': None,
        'driver': None,
        'veh_range': None,
        'hvb_state_of_charge': None,
        'hvb_charge_status': None,
        'hvb_charge_source': None,
        'hvb_lifetime_energy_output': None,
        'eld_device': {
            'id': _DEVICE_ID,
            'identifier': 'ELDSERIAL000001',
            'model': 'lbb-3.6ca',
        },
    }


def _driver_dict() -> JsonObject:
    """A synthetic DriverSummary wire dict for the populated-driver case."""
    return {
        'id': _DRIVER_ID,
        'first_name': 'Test',
        'last_name': 'Driver',
        'username': 'tdriver',
        'email': 'tdriver@example.com',
        'driver_company_id': 'TEST-001',
        'status': 'active',
        'role': 'driver',
    }


class TestVehicleLocation:
    def test_canonical_record_validates_to_typed_values(self) -> None:
        location = VehicleLocation.model_validate(_canonical_record())
        assert location.location_id == _LOCATION_ID
        assert location.latitude == 41.85
        assert location.longitude == -87.65
        assert location.location_type is VehicleLocationType.BREADCRUMB
        assert location.located_at == datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
        assert location.description == 'Anytown, ST'

    def test_eld_device_parses_to_embedded_model(self) -> None:
        location = VehicleLocation.model_validate(_canonical_record())
        assert location.eld_device is not None
        assert location.eld_device.device_id == _DEVICE_ID
        assert location.eld_device.identifier == 'ELDSERIAL000001'

    def test_null_fields_become_none(self) -> None:
        location = VehicleLocation.model_validate(_canonical_record())
        assert location.speed is None
        assert location.battery_voltage is None
        assert location.driver is None
        assert location.veh_range is None
        assert location.hvb_state_of_charge is None
        assert location.hvb_charge_status is None
        assert location.hvb_charge_source is None
        assert location.hvb_lifetime_energy_output is None
        assert location.fuel_primary_remaining_percentage is None

    def test_float_precision_preserved(self) -> None:
        location = VehicleLocation.model_validate(_canonical_record())
        assert location.odometer == _ODOMETER

    def test_populated_driver_parses(self) -> None:
        record = _canonical_record()
        record['driver'] = _driver_dict()
        location = VehicleLocation.model_validate(record)
        assert location.driver is not None
        assert location.driver.driver_id == _DRIVER_ID
        assert location.driver.first_name == 'Test'
        assert location.driver.email == 'tdriver@example.com'

    def test_minimal_payload_applies_defaults(self) -> None:
        location = VehicleLocation.model_validate(
            {
                'id': _LOCATION_ID,
                'located_at': '2026-06-01T12:00:00Z',
                'lat': 41.85,
                'lon': -87.65,
                'type': 'breadcrumb',
            }
        )
        assert location.description is None
        assert location.speed is None
        assert location.odometer is None
        assert location.fuel is None
        assert location.driver is None
        assert location.eld_device is None

    def test_unknown_field_is_ignored(self) -> None:
        record = _canonical_record()
        record['motive_added_this_field_yesterday'] = 'surprise'
        location = VehicleLocation.model_validate(record)
        assert not hasattr(location, 'motive_added_this_field_yesterday')

    def test_construct_by_field_name(self) -> None:
        # populate_by_name lets the model build from field names as well as the
        # wire aliases (id / lat / lon / type).
        location = VehicleLocation(
            location_id=_LOCATION_ID,
            located_at=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
            latitude=41.85,
            longitude=-87.65,
            location_type=VehicleLocationType.BREADCRUMB,
        )
        assert location.location_id == _LOCATION_ID
        assert location.latitude == 41.85

    def test_location_id_is_a_string(self) -> None:
        location = VehicleLocation.model_validate(_canonical_record())
        assert isinstance(location.location_id, str)
        assert location.location_id == _LOCATION_ID

    def test_is_frozen(self) -> None:
        location = VehicleLocation.model_validate(_canonical_record())
        with pytest.raises(ValidationError):
            location.description = 'mutated'  # type: ignore[misc]


class TestVehicleLocationType:
    def test_documented_value_parses_to_member(self) -> None:
        record = _canonical_record()
        record['type'] = 'ignition_on'
        location = VehicleLocation.model_validate(record)
        assert location.location_type is VehicleLocationType.IGNITION_ON

    def test_undocumented_value_rejected(self) -> None:
        record = _canonical_record()
        record['type'] = 'teleported'
        with pytest.raises(ValidationError):
            VehicleLocation.model_validate(record)

    def test_member_values(self) -> None:
        assert VehicleLocationType.BREADCRUMB.value == 'breadcrumb'
        assert VehicleLocationType.IGNITION_ON.value == 'ignition_on'
