"""Tests for fleetpull.models.motive.shared."""

from fleetpull.models.motive.shared import (
    DriverSummary,
    EldDeviceInfo,
    VehicleSummary,
)
from fleetpull.vocabulary import JsonValue

# Obviously-synthetic identifiers — never real VINs or fleet IDs in commits.
_DRIVER_ID: int = 9000001
_DEVICE_ID: int = 8800001
_VEHICLE_ID: int = 9900001


class TestEldDeviceInfo:
    def test_validates_and_aliases_id(self) -> None:
        device: EldDeviceInfo = EldDeviceInfo.model_validate(
            {'id': _DEVICE_ID, 'identifier': 'TESTELD0001', 'model': 'lbb-3.6ca'}
        )
        assert device.device_id == _DEVICE_ID
        assert device.identifier == 'TESTELD0001'
        assert device.model == 'lbb-3.6ca'


class TestDriverSummary:
    def test_validates_with_free_form_status_and_role(self) -> None:
        driver: DriverSummary = DriverSummary.model_validate(
            {
                'id': _DRIVER_ID,
                'first_name': 'Sam',
                'last_name': 'Synthetic',
                'status': 'whatever-motive-sends',
                'role': 'whatever-role',
            }
        )
        assert driver.driver_id == _DRIVER_ID
        assert driver.status == 'whatever-motive-sends'
        assert driver.role == 'whatever-role'

    def test_optional_fields_default_none(self) -> None:
        driver: DriverSummary = DriverSummary.model_validate(
            {'id': _DRIVER_ID, 'first_name': 'Sam', 'last_name': 'Synthetic'}
        )
        assert driver.username is None
        assert driver.email is None
        assert driver.status is None


def _vehicle_block(**overrides: str) -> dict[str, JsonValue]:
    block: dict[str, JsonValue] = {
        'id': _VEHICLE_ID,
        'number': '000001',
        'year': '2022',
        'make': 'Kenworth',
        'model': 'Box',
        'vin': '4SYNTHV1N00000001',
        'metric_units': False,
    }
    block.update(overrides)
    return block


class TestVehicleSummary:
    def test_validates_and_aliases_id(self) -> None:
        vehicle: VehicleSummary = VehicleSummary.model_validate(_vehicle_block())
        assert vehicle.vehicle_id == _VEHICLE_ID
        assert vehicle.vin == '4SYNTHV1N00000001'

    def test_quoted_year_coerces_to_int(self) -> None:
        vehicle = VehicleSummary.model_validate(_vehicle_block(year='2022'))
        assert vehicle.year == 2022

    def test_year_zero_sentinel_mirrors_uninterpreted(self) -> None:
        # The captured not-configured sentinel: "0" mirrors as 0, never
        # as null -- the value is the provider's, not ours to read.
        vehicle = VehicleSummary.model_validate(_vehicle_block(year='0'))
        assert vehicle.year == 0

    def test_empty_year_lifts_to_none(self) -> None:
        # Constructed variant of a captured record: the 2026-07-16 live
        # run observed a fleet vehicle failing int_parsing on year --
        # the empty-string wire error, on the field the capture only
        # showed populated.
        vehicle = VehicleSummary.model_validate(_vehicle_block(year=''))
        assert vehicle.year is None

    def test_missing_year_lands_null(self) -> None:
        block = _vehicle_block()
        del block['year']
        vehicle = VehicleSummary.model_validate(block)
        assert vehicle.year is None

    def test_empty_make_and_model_mirror_verbatim(self) -> None:
        # Models preserve "" faithfully from the wire; empty strings
        # become null at the DataFrame boundary, never here.
        vehicle = VehicleSummary.model_validate(_vehicle_block(make='', model=''))
        assert vehicle.make == ''
        assert vehicle.model == ''

    def test_real_make_and_model_survive(self) -> None:
        vehicle = VehicleSummary.model_validate(_vehicle_block())
        assert vehicle.make == 'Kenworth'
        assert vehicle.model == 'Box'
