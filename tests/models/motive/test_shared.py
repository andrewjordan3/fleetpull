"""Tests for fleetpull.models.motive.shared."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive.shared import (
    EldDeviceInfo,
    MotiveWindowStamp,
    UserSummary,
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


class TestUserSummary:
    def test_validates_with_free_form_status_and_role(self) -> None:
        driver: UserSummary = UserSummary.model_validate(
            {
                'id': _DRIVER_ID,
                'first_name': 'Sam',
                'last_name': 'Synthetic',
                'status': 'whatever-motive-sends',
                'role': 'whatever-role',
            }
        )
        assert driver.user_id == _DRIVER_ID
        assert driver.status == 'whatever-motive-sends'
        assert driver.role == 'whatever-role'

    def test_optional_fields_default_none(self) -> None:
        driver: UserSummary = UserSummary.model_validate(
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
        'vin': '4SYNTHV1N00000017',
        'metric_units': False,
    }
    block.update(overrides)
    return block


class TestVehicleSummary:
    def test_validates_and_aliases_id(self) -> None:
        vehicle: VehicleSummary = VehicleSummary.model_validate(_vehicle_block())
        assert vehicle.vehicle_id == _VEHICLE_ID
        assert vehicle.vin == '4SYNTHV1N00000017'

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

    def test_null_vin_lands_none(self) -> None:
        # The union-lax widening for the vehicle-utilization surface
        # (captured 2026-07-21): vin carries values on the event
        # surfaces but is null on some utilization rows, so the shared
        # shape accepts the null.
        block: dict[str, JsonValue] = {**_vehicle_block(), 'vin': None}
        vehicle = VehicleSummary.model_validate(block)
        assert vehicle.vin is None


class _StampProbe(ResponseModel):
    """A minimal carrier for exercising ``MotiveWindowStamp`` directly."""

    stamp: MotiveWindowStamp


class TestMotiveWindowStamp:
    def test_a_date_label_lifts_to_its_utc_midnight_instant(self) -> None:
        # The label's calendar day is preserved exactly; UTC midnight is
        # its canonical instant representation for partition routing --
        # never a timezone conversion of the data (the company-local
        # caveat rides the consuming models' docstrings).
        probe = _StampProbe.model_validate({'stamp': '2026-01-05'})
        assert probe.stamp == datetime(2026, 1, 5, tzinfo=UTC)
        assert probe.stamp.date().isoformat() == '2026-01-05'

    def test_a_datetime_passes_through_on_revalidation(self) -> None:
        recovered = datetime(2026, 1, 5, tzinfo=UTC)
        probe = _StampProbe.model_validate({'stamp': recovered})
        assert probe.stamp == recovered

    @pytest.mark.parametrize(
        'value',
        [
            # An RFC3339 instant is NOT a date label: the builder only
            # ever renders labels, so anything else is wiring drift that
            # must fail loudly, not pass mangled.
            '2026-01-05T00:00:00Z',
            'not-a-date',
            '',
            None,
            20260105,
        ],
    )
    def test_a_non_label_value_rejects(self, value: JsonValue) -> None:
        with pytest.raises(ValidationError):
            _StampProbe.model_validate({'stamp': value})


class TestWindowStampStrictness:
    """The lift's two rejection arms, pinned against validator laxity."""

    @pytest.mark.parametrize('label', ['20260105', '2026-W02-1'])
    def test_non_dashed_label_forms_reject(self, label: str) -> None:
        # date.fromisoformat alone would accept both of these; the
        # fullmatch pattern keeps every non-dashed form failing as the
        # wiring drift it would be.
        with pytest.raises(ValidationError):
            _StampProbe.model_validate({'stamp': label})

    def test_a_naive_datetime_stamp_rejects(self) -> None:
        # An unzoned event time is never assumed -- the passthrough arm
        # rejects at validation, not at persist time.
        naive = datetime(2026, 1, 5)  # noqa: DTZ001 -- naive on purpose
        with pytest.raises(ValidationError):
            _StampProbe.model_validate({'stamp': naive})
