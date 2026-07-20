"""Tests for fleetpull.models.samsara.driver.

Every fixture is the committed 2026-07-20 capture set
(``tests/samsara_drivers_capture.py``): three of the 832 swept records
-- the maximal active variant (every observed key, excluded blocks
included), the minimal variant (the always-present key set only), and
a deactivated record. The scrub-preserved fixture properties (the
always-present key set, the empty-string home-terminal faces, the
bare-integer ``dotNumber``, the closed activation vocabulary) are
asserted here beside the model they serve.
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import (
    Driver,
    DriverActivationStatus,
    DriverCarrierSettings,
    DriverHosSetting,
    DriverStaticAssignedVehicleRef,
    DriverTagRef,
)
from tests.samsara_drivers_capture import (
    DRIVER_RECORDS,
    DRIVERS_STATUS_ERROR_RESPONSE,
)

_ALWAYS_PRESENT_KEYS = frozenset(
    {
        'id',
        'name',
        'username',
        'driverActivationStatus',
        'timezone',
        'createdAtTime',
        'updatedAtTime',
        'hasVehicleUnpinningEnabled',
        'carrierSettings',
        'hosSetting',
    }
)


class TestFixtureProperties:
    """The scrub-preserved properties the capture module promises."""

    def test_the_variant_split(self) -> None:
        maximal, minimal, deactivated = DRIVER_RECORDS
        assert set(minimal) == _ALWAYS_PRESENT_KEYS
        assert set(maximal) > _ALWAYS_PRESENT_KEYS
        assert deactivated['driverActivationStatus'] == 'deactivated'
        assert [r['driverActivationStatus'] for r in DRIVER_RECORDS[:2]] == [
            'active',
            'active',
        ]

    def test_the_maximal_record_carries_every_observed_key(self) -> None:
        # The union-of-observed census, excluded blocks included --
        # externalIds is deliberately ABSENT (never observed in 832).
        maximal = DRIVER_RECORDS[0]
        partial_keys = {
            'staticAssignedVehicle',
            'peerGroupTag',
            'vehicleGroupTag',
            'licenseNumber',
            'licenseState',
            'phone',
            'locale',
            'notes',
            'profileImageUrl',
            'eldExempt',
            'eldExemptReason',
            'eldAdverseWeatherExemptionEnabled',
            'eldBigDayExemptionEnabled',
            'eldPcEnabled',
            'eldYmEnabled',
            'waitingTimeDutyStatusEnabled',
            'tags',
            'eldSettings',
        }
        assert set(maximal) == _ALWAYS_PRESENT_KEYS | partial_keys
        assert all('externalIds' not in record for record in DRIVER_RECORDS)

    def test_dot_number_is_a_bare_integer_on_the_wire(self) -> None:
        for record in DRIVER_RECORDS:
            carrier = record['carrierSettings']
            assert isinstance(carrier, dict)
            dot_number = carrier['dotNumber']
            assert isinstance(dot_number, int)
            assert not isinstance(dot_number, bool)

    def test_the_empty_string_home_terminal_faces(self) -> None:
        # The minimal record carries both empty-string faces (204/460
        # and 268/460 active) -- absent keys and empty strings are
        # different shapes, both captured.
        minimal_carrier = DRIVER_RECORDS[1]['carrierSettings']
        assert isinstance(minimal_carrier, dict)
        assert minimal_carrier['homeTerminalName'] == ''
        assert minimal_carrier['homeTerminalAddress'] == ''

    def test_ids_ascend_in_capture_order(self) -> None:
        identifiers = [
            identifier
            for record in DRIVER_RECORDS
            if isinstance(identifier := record['id'], str)
        ]
        assert len(identifiers) == 3
        assert identifiers == sorted(identifiers)


class TestDriverValidation:
    def test_every_record_validates_with_aware_datetimes(self) -> None:
        validated = [Driver.model_validate(record) for record in DRIVER_RECORDS]
        assert len(validated) == 3
        for driver in validated:
            assert driver.created_at_time is not None
            assert driver.created_at_time.tzinfo is not None
            assert driver.updated_at_time is not None
            assert driver.updated_at_time.tzinfo is not None

    def test_the_minimal_shape_lands_every_partial_field_null(self) -> None:
        driver = Driver.model_validate(DRIVER_RECORDS[1])
        assert driver.id == '7100002'
        assert driver.static_assigned_vehicle is None
        assert driver.peer_group_tag is None
        assert driver.vehicle_group_tag is None
        assert driver.license_number is None
        assert driver.license_state is None
        assert driver.phone is None
        assert driver.locale is None
        assert driver.notes is None
        assert driver.profile_image_url is None
        assert driver.eld_exempt is None
        assert driver.eld_exempt_reason is None
        assert driver.eld_adverse_weather_exemption_enabled is None
        assert driver.eld_big_day_exemption_enabled is None
        assert driver.eld_pc_enabled is None
        assert driver.eld_ym_enabled is None
        assert driver.waiting_time_duty_status_enabled is None

    def test_the_maximal_record_pins_the_wire_values(self) -> None:
        driver = Driver.model_validate(DRIVER_RECORDS[0])
        assert driver.id == '7100001'
        assert driver.username == 'example.driver001'
        assert driver.timezone == 'America/Chicago'
        assert driver.created_at_time == datetime(
            2021, 6, 14, 18, 22, 5, 114000, tzinfo=UTC
        )
        assert driver.license_number == 'D0000001'
        assert driver.eld_pc_enabled is True
        assert driver.waiting_time_duty_status_enabled is False
        vehicle = driver.static_assigned_vehicle
        assert isinstance(vehicle, DriverStaticAssignedVehicleRef)
        assert vehicle.id == '218000000000001'
        assert vehicle.name == 'U-901 (Example Truck)'

    def test_dot_number_lands_as_int(self) -> None:
        driver = Driver.model_validate(DRIVER_RECORDS[0])
        carrier = driver.carrier_settings
        assert isinstance(carrier, DriverCarrierSettings)
        assert carrier.dot_number == 100001
        assert isinstance(carrier.dot_number, int)
        assert carrier.carrier_name == 'Example Carrier LLC'

    def test_empty_home_terminal_strings_mirror_verbatim(self) -> None:
        # "" from the wire stays "" on the model -- the DataFrame
        # boundary is where empty strings become null.
        carrier = Driver.model_validate(DRIVER_RECORDS[1]).carrier_settings
        assert isinstance(carrier, DriverCarrierSettings)
        assert carrier.home_terminal_name == ''
        assert carrier.home_terminal_address == ''

    def test_the_hos_setting_block(self) -> None:
        for record in DRIVER_RECORDS:
            hos = Driver.model_validate(record).hos_setting
            assert isinstance(hos, DriverHosSetting)
            assert hos.heavy_haul_exemption_toggle_enabled is False

    def test_both_tag_references_share_the_one_ref_shape(self) -> None:
        driver = Driver.model_validate(DRIVER_RECORDS[0])
        peer_tag = driver.peer_group_tag
        vehicle_tag = driver.vehicle_group_tag
        assert isinstance(peer_tag, DriverTagRef)
        assert isinstance(vehicle_tag, DriverTagRef)
        assert peer_tag.parent_tag_id == '4400000'
        assert vehicle_tag.parent_tag_id == '4500000'


class TestActivationStatusEnum:
    def test_the_enum_is_exactly_the_two_proven_values(self) -> None:
        # Closure is API-enforced (every other probed value returned
        # HTTP 400 naming these two) -- the mirror must not widen it.
        assert {member.value for member in DriverActivationStatus} == {
            'active',
            'deactivated',
        }

    def test_both_statuses_land_as_enum_members(self) -> None:
        statuses = [
            Driver.model_validate(record).driver_activation_status
            for record in DRIVER_RECORDS
        ]
        assert statuses == [
            DriverActivationStatus.ACTIVE,
            DriverActivationStatus.ACTIVE,
            DriverActivationStatus.DEACTIVATED,
        ]

    def test_an_unlisted_status_fails_loudly(self) -> None:
        drifted = {**DRIVER_RECORDS[1], 'driverActivationStatus': 'suspended'}
        with pytest.raises(ValidationError):
            Driver.model_validate(drifted)

    def test_the_captured_400_names_exactly_the_enum_vocabulary(self) -> None:
        # The provider's own closure proof: the 400 body every malformed
        # status value returns names precisely the two mirrored members.
        message = DRIVERS_STATUS_ERROR_RESPONSE['message']
        assert isinstance(message, str)
        for member in DriverActivationStatus:
            assert f"'{member.value}'" in message
        assert 'requestId' in DRIVERS_STATUS_ERROR_RESPONSE


class TestExcludedFields:
    def test_the_list_of_object_blocks_are_not_modeled(self) -> None:
        # The list-of-structs exclusion (Device/User precedent): tags
        # and eldSettings ride the maximal capture and must land
        # nowhere.
        maximal = DRIVER_RECORDS[0]
        assert 'tags' in maximal
        assert 'eldSettings' in maximal
        driver = Driver.model_validate(maximal)
        assert not hasattr(driver, 'tags')
        assert 'tags' not in Driver.model_fields
        assert 'eld_settings' not in Driver.model_fields

    def test_external_ids_is_unobserved_and_unmodeled(self) -> None:
        # Never observed in 832 swept records -- not modeled, unlike
        # the vehicles mirror where it was captured.
        assert 'external_ids' not in Driver.model_fields
