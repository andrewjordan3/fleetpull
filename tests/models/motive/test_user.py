"""Tests for fleetpull.models.motive.user.

Every fixture is the committed 2026-07-21 capture set
(``tests/motive_users_capture.py``): four fully synthetic records
shaped by the whole-population walk (2,665 records, perfectly
role-partitioned — 2,359 driver / 274 fleet_user / 32 admin), covering
one record per role plus a second driver exercising the driver block's
null arms. The fixture properties the capture module promises — the
role partition, the null arms, the six never-populated keys — are
asserted here beside the model they serve.
"""

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.motive import User
from tests.motive_users_capture import (
    USER_ADMIN_RECORD,
    USER_DRIVER_MAXIMAL_RECORD,
    USER_DRIVER_NULL_ARM_RECORD,
    USER_FLEET_USER_RECORD,
    USER_RECORDS,
)

# The shared block: present on every one of the 2,665 census records
# regardless of role, so required on the model (nullable per census).
_SHARED_REQUIRED_KEYS = frozenset(
    {
        'id',
        'first_name',
        'last_name',
        'email',
        'phone',
        'phone_country_code',
        'company_reference_id',
        'role',
        'status',
        'group_ids',
        'metric_units',
        'time_zone',
        'created_at',
        'updated_at',
        'mobile_current_sign_in_at',
        'mobile_last_active_at',
        'mobile_last_sign_in_at',
        'web_current_sign_in_at',
        'web_last_active_at',
        'web_last_sign_in_at',
    }
)

# The driver-only block: present on every driver record, absent -- not
# null -- on every admin/fleet_user record.
_DRIVER_ONLY_KEYS = frozenset(
    {
        'username',
        'driver_company_id',
        'drivers_license_number',
        'drivers_license_state',
        'joined_at',
        'duty_status',
        'eld_mode',
        'cycle',
        'cycle2',
        'violation_alerts',
        'carrier_name',
        'carrier_street',
        'carrier_city',
        'carrier_state',
        'carrier_zip',
        'terminal_street',
        'terminal_city',
        'terminal_state',
        'terminal_zip',
        'exception_24_hour_restart',
        'exception_8_hour_break',
        'exception_adverse_driving',
        'exception_ca_farm_school_bus',
        'exception_short_haul',
        'exception_wait_time',
        'exception_24_hour_restart2',
        'exception_8_hour_break2',
        'exception_adverse_driving2',
        'exception_ca_farm_school_bus2',
        'exception_short_haul2',
        'exception_wait_time2',
        'export_combined',
        'export_odometers',
        'export_recap',
        'manual_driving_enabled',
        'minute_logs',
        'personal_conveyance_enabled',
        'yard_moves_enabled',
    }
)

_NEVER_POPULATED_KEYS = frozenset(
    {
        'associated_dispatcher_id',
        'expires_at',
        'external_ids',
        'phone2',
        'phone_country_code2',
        'phone_ext',
    }
)


class TestFixtureProperties:
    """The role partition and null arms the capture module promises."""

    def test_one_record_per_role_plus_the_null_arm_driver(self) -> None:
        assert len(USER_RECORDS) == 4
        assert [record['role'] for record in USER_RECORDS] == [
            'driver',
            'admin',
            'fleet_user',
            'driver',
        ]

    def test_the_role_partition_is_exact(self) -> None:
        # The census truth: driver records carry the WHOLE driver block,
        # non-driver records carry NONE of it -- zero partial presence.
        for record in USER_RECORDS:
            assert set(record) >= _SHARED_REQUIRED_KEYS
            driver_keys_present = _DRIVER_ONLY_KEYS & set(record)
            if record['role'] == 'driver':
                assert driver_keys_present == _DRIVER_ONLY_KEYS
            else:
                assert driver_keys_present == set()

    def test_the_never_populated_keys_ride_every_record_as_null(self) -> None:
        for record in USER_RECORDS:
            for key in _NEVER_POPULATED_KEYS:
                assert record[key] is None

    def test_both_statuses_and_both_group_ids_shapes(self) -> None:
        statuses = {record['status'] for record in USER_RECORDS}
        assert statuses == {'active', 'deactivated'}
        group_id_lists = [record['group_ids'] for record in USER_RECORDS]
        assert [] in group_id_lists
        assert any(group_ids for group_ids in group_id_lists)

    def test_joined_at_rides_both_census_arms(self) -> None:
        # 34 of 2,359 census drivers carried a YYYY-MM-DD join date; the
        # fixtures exercise both the populated and null arms.
        values = [
            record['joined_at'] for record in USER_RECORDS if record['role'] == 'driver'
        ]
        assert None in values
        assert '2023-04-12' in values


class TestUserValidation:
    @pytest.mark.parametrize('required_key', sorted(_SHARED_REQUIRED_KEYS))
    def test_each_shared_key_rejects_absence(self, required_key: str) -> None:
        # The whole-population walk made the shared block required, and
        # only a loud rejection here keeps a future optional-demotion
        # from passing every gate (the addresses precedent).
        record = {
            key: value
            for key, value in USER_ADMIN_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            User.model_validate(record)

    def test_every_record_validates(self) -> None:
        validated = [User.model_validate(record) for record in USER_RECORDS]
        assert [user.user_id for user in validated] == [
            800001,
            800002,
            800003,
            800004,
        ]

    def test_timestamps_are_timezone_aware_utc(self) -> None:
        user = User.model_validate(USER_DRIVER_MAXIMAL_RECORD)
        assert user.created_at == datetime(2024, 2, 1, 15, 4, 5, tzinfo=UTC)
        assert user.updated_at.tzinfo is not None
        assert user.mobile_current_sign_in_at == datetime(
            2026, 7, 20, 11, 5, tzinfo=UTC
        )

    def test_the_nullable_shared_arms(self) -> None:
        admin = User.model_validate(USER_ADMIN_RECORD)
        assert admin.phone is None
        assert admin.phone_country_code is None
        assert admin.company_reference_id is None
        assert admin.mobile_current_sign_in_at is None
        assert admin.web_current_sign_in_at is not None
        fleet_user = User.model_validate(USER_FLEET_USER_RECORD)
        assert fleet_user.email is None
        assert fleet_user.time_zone is None

    def test_group_ids_lands_typed(self) -> None:
        driver = User.model_validate(USER_DRIVER_MAXIMAL_RECORD)
        assert driver.group_ids == [61001, 61002]
        admin = User.model_validate(USER_ADMIN_RECORD)
        assert admin.group_ids == []


class TestRoleShape:
    """One dataset, role-dependent shape: the DESIGN section 8 decision."""

    def test_the_maximal_driver_lands_the_whole_driver_block(self) -> None:
        driver = User.model_validate(USER_DRIVER_MAXIMAL_RECORD)
        assert driver.role == 'driver'
        assert driver.username == 'synthetic.driver001'
        assert driver.driver_company_id == '10001-SYN'
        assert driver.drivers_license_number == 'D0000001'
        assert driver.drivers_license_state == 'TX'
        assert driver.duty_status == 'off_duty'
        assert driver.eld_mode == 'logs'
        assert driver.cycle == '70_8'
        assert driver.violation_alerts == '1_hour'
        assert driver.carrier_name == 'Synthetic Carrier LLC'
        assert driver.carrier_zip == '10001'
        assert driver.terminal_street == '501 Synthetic Terminal Ave'
        assert driver.exception_adverse_driving is True
        assert driver.export_combined is True
        assert driver.minute_logs is True
        assert driver.personal_conveyance_enabled is True
        assert driver.yard_moves_enabled is True

    def test_a_non_driver_record_validates_without_the_driver_block(self) -> None:
        admin = User.model_validate(USER_ADMIN_RECORD)
        assert admin.role == 'admin'
        assert admin.status == 'deactivated'

    def test_driver_only_fields_are_none_on_a_non_driver(self) -> None:
        # Absence-shaped optionality: the block is absent on the wire,
        # so every driver-only field defaults to None.
        admin = User.model_validate(USER_ADMIN_RECORD)
        for field_name in sorted(_DRIVER_ONLY_KEYS):
            assert getattr(admin, field_name) is None

    def test_the_null_arm_driver_nulls_the_nullable_driver_keys(self) -> None:
        driver = User.model_validate(USER_DRIVER_NULL_ARM_RECORD)
        assert driver.username is None
        assert driver.driver_company_id is None
        assert driver.drivers_license_number is None
        assert driver.drivers_license_state is None
        assert driver.cycle is None
        assert driver.terminal_street is None
        assert driver.terminal_zip is None
        # The non-nullable-in-role keys stay populated even here.
        assert driver.duty_status == 'off_duty'
        assert driver.carrier_name == 'Synthetic Carrier LLC'
        assert driver.yard_moves_enabled is False

    def test_joined_at_recovers_the_date_only_wire_value(self) -> None:
        # 34 of 2,359 census drivers carried a YYYY-MM-DD join date; the
        # maximal fixture exercises the populated arm, the null-arm
        # driver the other.
        driver = User.model_validate(USER_DRIVER_MAXIMAL_RECORD)
        assert driver.joined_at == date(2023, 4, 12)

    def test_the_hos_cycle_pair_lands_both_arms(self) -> None:
        driver = User.model_validate(USER_DRIVER_MAXIMAL_RECORD)
        assert driver.cycle2 == '70_8_2020'

    def test_an_unobserved_role_and_status_token_validates(self) -> None:
        # The census-open vocabulary posture with teeth: role/status are
        # plain str mirrors, so tokens the census never showed must
        # validate rather than reject -- vocabulary growth is absorbed,
        # never a crash (the assignmentType driverApp lesson).
        record = {
            **USER_DRIVER_MAXIMAL_RECORD,
            'role': 'unobserved_future_role',
            'status': 'unobserved_future_status',
        }
        user = User.model_validate(record)
        assert user.role == 'unobserved_future_role'
        assert user.status == 'unobserved_future_status'


class TestExcludedFields:
    def test_never_populated_keys_are_not_modeled(self) -> None:
        # The six keys were null/empty on all 2,665 census records --
        # the value types are unobservable, so the model excludes them
        # and extra='ignore' drops the wire nulls.
        user = User.model_validate(USER_DRIVER_MAXIMAL_RECORD)
        for excluded in sorted(_NEVER_POPULATED_KEYS):
            assert excluded in USER_DRIVER_MAXIMAL_RECORD
            assert excluded not in User.model_fields
            assert not hasattr(user, excluded)
