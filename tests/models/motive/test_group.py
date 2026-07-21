"""Tests for fleetpull.models.motive.group.

Every fixture is the committed 2026-07-21 capture set
(``tests/motive_groups_capture.py``): five fully synthetic records
shaped by the whole-population walk (152 records, every key on all
152), covering the root group, the tree children, both owner-ref
``email`` arms, and the two never-populated owner sub-keys. The fixture
properties the capture module promises are asserted here beside the
model they serve.
"""

import pytest
from pydantic import ValidationError

from fleetpull.models.motive import Group, UserSummary
from fleetpull.vocabulary import JsonObject
from tests.motive_groups_capture import GROUP_RECORDS

# The whole-population walk observed every record key on all 152
# records, so every modeled key is required.
_REQUIRED_KEYS = frozenset({'id', 'company_id', 'name', 'parent_id', 'user'})

# UserSummary's structural core; the remaining owner keys are nullable
# on the shared shape (union-lax across its three carrying surfaces).
_OWNER_REQUIRED_KEYS = frozenset({'id', 'first_name', 'last_name'})


def _owner_block(record: JsonObject) -> JsonObject:
    owner = record['user']
    assert isinstance(owner, dict)
    return owner


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_exactly_one_root_group(self) -> None:
        assert len(GROUP_RECORDS) == 5
        roots = [record for record in GROUP_RECORDS if record['parent_id'] is None]
        assert len(roots) == 1

    def test_every_child_points_at_an_existing_fixture_group(self) -> None:
        group_ids = {record['id'] for record in GROUP_RECORDS}
        parents = {
            record['parent_id']
            for record in GROUP_RECORDS
            if record['parent_id'] is not None
        }
        assert parents <= group_ids

    def test_every_record_carries_the_required_keys(self) -> None:
        for record in GROUP_RECORDS:
            assert set(record) >= _REQUIRED_KEYS
            assert set(_owner_block(record)) >= _OWNER_REQUIRED_KEYS

    def test_both_owner_email_arms_ride_the_fixtures(self) -> None:
        emails = [_owner_block(record)['email'] for record in GROUP_RECORDS]
        assert None in emails
        assert any(isinstance(email, str) for email in emails)

    def test_the_never_populated_sub_keys_are_null_on_every_owner(self) -> None:
        # The census truth behind the exclusion: null on ALL 152.
        for record in GROUP_RECORDS:
            owner = _owner_block(record)
            assert owner['username'] is None
            assert owner['driver_company_id'] is None


class TestGroupValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        # The whole-population walk made every key required, and only a
        # loud rejection here keeps a future optional-demotion from
        # passing every gate (the addresses precedent).
        record = {
            key: value for key, value in GROUP_RECORDS[0].items() if key != required_key
        }
        with pytest.raises(ValidationError):
            Group.model_validate(record)

    @pytest.mark.parametrize('required_key', sorted(_OWNER_REQUIRED_KEYS))
    def test_each_owner_required_key_rejects_absence(self, required_key: str) -> None:
        record = dict(GROUP_RECORDS[0])
        record['user'] = {
            key: value
            for key, value in _owner_block(GROUP_RECORDS[0]).items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            Group.model_validate(record)

    def test_every_record_validates(self) -> None:
        validated = [Group.model_validate(record) for record in GROUP_RECORDS]
        assert len(validated) == 5
        assert all(isinstance(group.user, UserSummary) for group in validated)

    def test_the_root_group_pins_the_wire_values(self) -> None:
        group = Group.model_validate(GROUP_RECORDS[0])
        assert group.group_id == 90001
        assert group.company_id == 4200
        assert group.name == 'Synthetic Fleet'
        assert group.parent_id is None
        assert group.user.user_id == 700001
        assert group.user.first_name == 'Synthetic'
        assert group.user.last_name == 'Admin001'
        assert group.user.email == 'synthetic.admin001@example.com'
        assert group.user.role == 'admin'
        assert group.user.status == 'active'

    def test_a_child_group_carries_its_parent_id(self) -> None:
        group = Group.model_validate(GROUP_RECORDS[1])
        assert group.parent_id == 90001

    def test_the_null_email_owner_arm(self) -> None:
        group = Group.model_validate(GROUP_RECORDS[2])
        assert group.user.email is None
        assert group.user.role == 'fleet_user'

    def test_the_deactivated_owner_arm(self) -> None:
        group = Group.model_validate(GROUP_RECORDS[3])
        assert group.user.status == 'deactivated'


class TestSharedShapeOnThisSurface:
    def test_null_here_sub_keys_land_as_none_on_the_shared_shape(self) -> None:
        # username and driver_company_id were null on all 152 census
        # records of THIS surface; the shared UserSummary models them
        # (they carry values on the driving-period/idle-event driver
        # references), so here they simply read as None.
        owner = Group.model_validate(GROUP_RECORDS[0]).user
        assert owner.username is None
        assert owner.driver_company_id is None

    def test_an_unobserved_role_token_validates(self) -> None:
        # The census-open vocabulary posture with teeth: role/status are
        # plain str mirrors, so a token the census never showed must
        # validate rather than reject (vocabulary growth is absorbed,
        # never a crash).
        record = dict(GROUP_RECORDS[0])
        record['user'] = {
            **_owner_block(GROUP_RECORDS[0]),
            'role': 'unobserved_future_role',
            'status': 'unobserved_future_status',
        }
        owner = Group.model_validate(record).user
        assert owner.role == 'unobserved_future_role'
        assert owner.status == 'unobserved_future_status'
