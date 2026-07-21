"""Tests for fleetpull.models.motive.vehicle."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.motive.vehicle import (
    AvailabilityDetails,
    AvailabilityStatus,
    Vehicle,
    VehicleStatus,
)
from fleetpull.vocabulary import JsonObject, JsonValue

# Obviously-synthetic identifiers — never real VINs or fleet IDs in commits.
_VEHICLE_ID: int = 8000001
_COMPANY_ID: int = 7000001
_VIN: str = 'TESTVIN0000000001'
_DRIVER_ID: int = 9000001
_DEVICE_ID: int = 8800001
# list[JsonValue], not list[int]: the payload dict is a JsonObject, and the
# invariant list arm of JsonValue would otherwise reject a list[int] value.
_GROUP_IDS: list[JsonValue] = [101, 102]
_SENTINEL: int = -1


def _full_vehicle_payload() -> JsonObject:
    """Build a representative fully-populated Motive vehicle payload.

    Returns:
        A wire-shaped mapping using Motive's response keys (``id`` for the
        identifier alias), populated with synthetic data, suitable for
        ``Vehicle.model_validate``.
    """
    return {
        'id': _VEHICLE_ID,
        'company_id': _COMPANY_ID,
        'number': 'TEST-001',
        'status': 'active',
        'ifta': True,
        'vin': _VIN,
        'make': 'TestMake',
        'model': 'TestModel',
        'year': '2020',
        'license_plate_state': 'IL',
        'license_plate_number': 'TEST123',
        'license_plate_country_code': 'US',
        'metric_units': False,
        'fuel_type': 'diesel',
        'prevent_auto_odometer_entry': False,
        'notes': 'synthetic test vehicle',
        'incab_alert_live_stream_enable': 1,
        'driver_facing_camera': 0,
        'incab_audio_recording': -1,
        'group_ids': _GROUP_IDS,
        'created_at': '2025-01-01T00:00:00Z',
        'updated_at': '2026-01-01T00:00:00Z',
        'permanent_driver': None,
        'availability_details': {
            'availability_status': 'in_service',
            'updated_at': '2026-01-01T00:00:00Z',
        },
        'eld_device': {
            'id': _DEVICE_ID,
            'identifier': 'TESTELD0001',
            'model': 'lbb-3.6ca',
        },
        'current_driver': {
            'id': _DRIVER_ID,
            'first_name': 'Sam',
            'last_name': 'Synthetic',
            'status': 'active',
            'role': 'driver',
        },
        'carb_ctc_test_enabled': None,
        'carb_ctc_emission_status': None,
        'registration_expiry_date': None,
    }


class TestVehicle:
    def test_full_payload_validates_to_typed_values(self) -> None:
        vehicle: Vehicle = Vehicle.model_validate(_full_vehicle_payload())
        assert vehicle.vehicle_id == _VEHICLE_ID
        assert vehicle.company_id == _COMPANY_ID
        assert vehicle.number == 'TEST-001'
        assert vehicle.status is VehicleStatus.ACTIVE
        assert vehicle.ifta is True
        assert vehicle.vin == _VIN
        assert vehicle.year == '2020'
        assert vehicle.license_plate_country_code == 'US'
        assert vehicle.group_ids == _GROUP_IDS

    def test_id_alias_populates_vehicle_id(self) -> None:
        vehicle: Vehicle = Vehicle.model_validate(_full_vehicle_payload())
        assert vehicle.vehicle_id == _VEHICLE_ID

    def test_created_and_updated_parse_to_aware_datetimes(self) -> None:
        vehicle: Vehicle = Vehicle.model_validate(_full_vehicle_payload())
        assert vehicle.created_at == datetime(2025, 1, 1, tzinfo=UTC)
        assert vehicle.updated_at == datetime(2026, 1, 1, tzinfo=UTC)

    def test_embedded_models_parse(self) -> None:
        vehicle: Vehicle = Vehicle.model_validate(_full_vehicle_payload())
        assert vehicle.eld_device is not None
        assert vehicle.eld_device.device_id == _DEVICE_ID
        assert vehicle.current_driver is not None
        assert vehicle.current_driver.user_id == _DRIVER_ID
        assert vehicle.availability_details is not None
        assert (
            vehicle.availability_details.availability_status
            is AvailabilityStatus.IN_SERVICE
        )

    def test_deactivated_status_validates(self) -> None:
        # Live responses include 'deactivated'; confirm the enum accepts it.
        payload: JsonObject = _full_vehicle_payload()
        payload['status'] = 'deactivated'
        vehicle: Vehicle = Vehicle.model_validate(payload)
        assert vehicle.status is VehicleStatus.DEACTIVATED

    def test_minimal_payload_applies_defaults(self) -> None:
        vehicle: Vehicle = Vehicle.model_validate(
            {
                'id': _VEHICLE_ID,
                'company_id': _COMPANY_ID,
                'number': 'TEST-002',
                'status': 'inactive',
                'ifta': False,
                'created_at': '2025-01-01T00:00:00Z',
                'updated_at': '2025-01-01T00:00:00Z',
            }
        )
        assert vehicle.vin is None
        assert vehicle.make is None
        assert vehicle.license_plate_country_code is None
        assert vehicle.fuel_type is None
        assert vehicle.metric_units is False
        assert vehicle.incab_alert_live_stream_enable == _SENTINEL
        assert vehicle.driver_facing_camera == _SENTINEL
        assert vehicle.incab_audio_recording == _SENTINEL
        assert vehicle.group_ids == []
        assert vehicle.permanent_driver is None
        assert vehicle.eld_device is None

    def test_fuel_type_is_free_form_string(self) -> None:
        # fuel_type is mirrored as a plain str (no enum, no normalizer), so
        # any casing or value Motive sends passes through unchanged.
        vehicle: Vehicle = Vehicle.model_validate(
            {
                'id': _VEHICLE_ID,
                'company_id': _COMPANY_ID,
                'number': 'TEST-003',
                'status': 'active',
                'ifta': False,
                'fuel_type': 'Diesel',
                'created_at': '2025-01-01T00:00:00Z',
                'updated_at': '2025-01-01T00:00:00Z',
            }
        )
        assert vehicle.fuel_type == 'Diesel'

    def test_unknown_field_is_ignored(self) -> None:
        vehicle: Vehicle = Vehicle.model_validate(
            {
                'id': _VEHICLE_ID,
                'company_id': _COMPANY_ID,
                'number': 'TEST-005',
                'status': 'active',
                'ifta': False,
                'created_at': '2025-01-01T00:00:00Z',
                'updated_at': '2025-01-01T00:00:00Z',
                'motive_added_this_field_yesterday': 'surprise',
            }
        )
        assert not hasattr(vehicle, 'motive_added_this_field_yesterday')

    def test_stringly_typed_integer_coerces(self) -> None:
        # The base is non-strict, so Motive's occasional stringly-typed
        # numerics coerce rather than reject.
        vehicle: Vehicle = Vehicle.model_validate(
            {
                'id': str(_VEHICLE_ID),
                'company_id': str(_COMPANY_ID),
                'number': 'TEST-006',
                'status': 'active',
                'ifta': False,
                'created_at': '2025-01-01T00:00:00Z',
                'updated_at': '2025-01-01T00:00:00Z',
            }
        )
        assert vehicle.vehicle_id == _VEHICLE_ID
        assert vehicle.company_id == _COMPANY_ID

    def test_is_frozen(self) -> None:
        vehicle: Vehicle = Vehicle.model_validate(_full_vehicle_payload())
        with pytest.raises(ValidationError):
            vehicle.number = 'mutated'  # type: ignore[misc]

    def test_unknown_status_value_rejected(self) -> None:
        # status is a closed enum mirror, so a value outside Motive's
        # documented vocabulary fails validation rather than passing.
        with pytest.raises(ValidationError):
            Vehicle.model_validate(
                {
                    'id': _VEHICLE_ID,
                    'company_id': _COMPANY_ID,
                    'number': 'TEST-007',
                    'status': 'teleported',
                    'ifta': False,
                    'created_at': '2025-01-01T00:00:00Z',
                    'updated_at': '2025-01-01T00:00:00Z',
                }
            )


class TestAvailabilityDetails:
    def test_validates_status_and_timestamp(self) -> None:
        details: AvailabilityDetails = AvailabilityDetails.model_validate(
            {
                'availability_status': 'out_of_service',
                'updated_at': '2026-01-01T00:00:00Z',
            }
        )
        assert details.availability_status is AvailabilityStatus.OUT_OF_SERVICE
        assert details.updated_at == datetime(2026, 1, 1, tzinfo=UTC)


class TestVehicleStatus:
    def test_member_values(self) -> None:
        assert VehicleStatus.ACTIVE.value == 'active'
        assert VehicleStatus.INACTIVE.value == 'inactive'
        assert VehicleStatus.DEACTIVATED.value == 'deactivated'


class TestAvailabilityStatus:
    def test_member_values(self) -> None:
        assert AvailabilityStatus.IN_SERVICE.value == 'in_service'
        assert AvailabilityStatus.OUT_OF_SERVICE.value == 'out_of_service'
