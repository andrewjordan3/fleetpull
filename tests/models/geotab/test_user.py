"""Tests for fleetpull.models.geotab.user.

Every fixture is the committed 2026-07-16 capture set
(``tests/geotab_users_capture.py``): seven of the 157 swept records --
the four driver variants and the three non-driver variants. The
scrub-preserved fixture properties (the absence-shaped driver-only
block, the single ``accessGroupFilter`` carrier, the authority/company
equality classes) are asserted here beside the model they serve. The
sweep-only optionalities (``lastAccessDate`` 156/157,
``maxPCDistancePerDay`` 126/157) ride constructed variants of captured
records, since every committed record carries both keys.
"""

from datetime import UTC, datetime

from fleetpull.models.geotab import User, UserAccessGroupFilterRef
from tests.geotab_users_capture import USER_RECORDS

_DRIVER_ONLY_KEYS = frozenset(
    {'licenseNumber', 'licenseProvince', 'viewDriversOwnDataOnly'}
)


class TestFixtureProperties:
    """The scrub-preserved properties the capture module promises."""

    def test_the_population_split(self) -> None:
        assert len(USER_RECORDS) == 7
        assert sum(record['isDriver'] is True for record in USER_RECORDS) == 4
        assert sum(record['isDriver'] is False for record in USER_RECORDS) == 3

    def test_the_driver_only_block_is_absent_not_null(self) -> None:
        # GeoTab omits keys rather than sending nulls (2026-07-16 sweep:
        # the block sits at exactly the 129-driver count, no null
        # observed anywhere in 157 records).
        for record in USER_RECORDS:
            present = _DRIVER_ONLY_KEYS & set(record)
            if record['isDriver']:
                assert present == _DRIVER_ONLY_KEYS
            else:
                assert not present

    def test_exactly_one_access_group_filter_carrier(self) -> None:
        # 1/157 in the sweep; the carrier is committed.
        carriers = [r for r in USER_RECORDS if 'accessGroupFilter' in r]
        assert len(carriers) == 1

    def test_the_authority_company_equality_classes(self) -> None:
        for record in USER_RECORDS:
            assert record['authorityName'] == record['companyName']
            assert record['authorityAddress'] == record['companyAddress']

    def test_ids_ascend_in_capture_order(self) -> None:
        identifiers = [
            identifier
            for record in USER_RECORDS
            if isinstance(identifier := record['id'], str)
        ]
        assert len(identifiers) == 7
        assert identifiers == sorted(identifiers)


class TestUserValidation:
    def test_every_record_validates_with_aware_datetimes(self) -> None:
        validated = [User.model_validate(record) for record in USER_RECORDS]
        assert len(validated) == 7
        for user in validated:
            assert user.active_from.tzinfo is not None
            assert user.active_to.tzinfo is not None

    def test_first_record_pins_the_wire_values(self) -> None:
        user = User.model_validate(USER_RECORDS[0])
        assert user.id == 'b5CE1'
        assert user.is_driver is True
        assert user.license_number == 'L0000001'
        assert user.license_province == 'OH'
        assert user.view_drivers_own_data_only is True
        assert user.active_from == datetime(2020, 11, 20, 17, 5, 22, 587000, tzinfo=UTC)
        assert user.hos_rule_set == 'America8DayBig'
        assert user.carrier_number == '1000001'

    def test_the_still_active_sentinel_mirrors_verbatim(self) -> None:
        # 2050-01-01 is GeoTab's still-active sentinel, stored as-is,
        # never interpreted (the Device precedent).
        for record in USER_RECORDS:
            user = User.model_validate(record)
            assert user.active_to == datetime(2050, 1, 1, tzinfo=UTC)

    def test_the_acronym_aliases_land_the_captured_values(self) -> None:
        # The five keys to_camel cannot produce carry explicit aliases;
        # each is pinned against a captured value so a silently broken
        # alias fails here, not in production (the Device acronym-trap
        # precedent). maxPCDistancePerDay is the one optional-with-
        # default field, where a broken alias would land as None.
        user = User.model_validate(USER_RECORDS[0])
        assert user.accepted_eula == 20
        assert user.wifi_eula == 0
        assert user.is_eula_accepted is True
        assert user.is_exempt_hos_enabled is False
        assert user.max_pc_distance_per_day == 0

    def test_non_driver_lands_the_driver_block_null(self) -> None:
        non_drivers = [
            User.model_validate(record)
            for record in USER_RECORDS
            if record['isDriver'] is False
        ]
        assert len(non_drivers) == 3
        for user in non_drivers:
            assert user.license_number is None
            assert user.license_province is None
            assert user.view_drivers_own_data_only is None

    def test_the_access_group_filter_reference(self) -> None:
        validated = [User.model_validate(record) for record in USER_RECORDS]
        carriers = [u for u in validated if u.access_group_filter is not None]
        assert len(carriers) == 1
        reference = carriers[0].access_group_filter
        assert isinstance(reference, UserAccessGroupFilterRef)
        assert reference.id == 'aSYN0000000000000000004'

    def test_the_hos_rule_set_none_literal_mirrors_verbatim(self) -> None:
        # Non-driving accounts carry the literal string "None" -- the
        # provider's vocabulary, never lifted to a null.
        rule_sets = {User.model_validate(r).hos_rule_set for r in USER_RECORDS}
        assert rule_sets == {'America8Day', 'America8DayBig', 'None'}

    def test_both_login_shapes_are_mirrored(self) -> None:
        logins = {User.model_validate(record).name for record in USER_RECORDS}
        assert 'synthuser001' in logins
        assert any(login.endswith('@example.com') for login in logins)


class TestEmptyStringsMirrorVerbatim:
    """Models preserve ``""`` faithfully from the wire (DESIGN section 9).

    Empty strings become null once, at the DataFrame boundary
    (``records.normalize_empty_strings``) -- never on the model.
    """

    def test_empty_contact_fields_mirror_verbatim(self) -> None:
        user = User.model_validate(USER_RECORDS[0])
        assert user.phone_number == ''
        assert user.employee_no == ''
        assert user.comment == ''
        assert user.feature_preview == ''
        assert user.default_map_engine == ''

    def test_populated_values_mirror_verbatim(self) -> None:
        populated = [
            User.model_validate(record)
            for record in USER_RECORDS
            if record['phoneNumber'] != ''
        ]
        assert populated
        for user in populated:
            assert user.phone_number.startswith('+1 ')
        designations = {User.model_validate(r).designation for r in USER_RECORDS}
        assert 'Corp' in designations

    def test_carrier_number_mirrors_both_shapes(self) -> None:
        for record in USER_RECORDS:
            user = User.model_validate(record)
            if record['isDriver']:
                assert user.carrier_number == '1000001'
            else:
                assert user.carrier_number == ''


class TestSweepObservedOptionality:
    """Constructed variants of captured records: the sweep-only shapes.

    Every committed record carries these keys; the 2026-07-16 sweep
    proved their absence variants exist in the population
    (``lastAccessDate`` 156/157, ``maxPCDistancePerDay`` 126/157 --
    the latter NOT aligned with the 129-driver split).
    """

    def test_missing_last_access_date_lands_null(self) -> None:
        record = dict(USER_RECORDS[0])
        del record['lastAccessDate']
        assert User.model_validate(record).last_access_date is None

    def test_missing_max_pc_distance_lands_null(self) -> None:
        record = dict(USER_RECORDS[0])
        del record['maxPCDistancePerDay']
        assert User.model_validate(record).max_pc_distance_per_day is None


class TestExcludedFields:
    def test_the_list_and_iam_blocks_are_not_modeled(self) -> None:
        # extra='ignore' makes exclusion exactly "don't model it": the
        # UI/grouping lists and the IAM plumbing ride every capture and
        # must land nowhere.
        user = User.model_validate(USER_RECORDS[0])
        for excluded in ('map_views', 'security_groups', 'driver_groups', 'keys'):
            assert not hasattr(user, excluded)
        assert not hasattr(user, 'i_am_metadata')
