"""Tests for fleetpull.models.geotab.driver_change.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_driver_changes_capture.py``), shaped by the wave two
census (six keys, census-total on 1,114/1,114). Requiredness is the
wave-two conservative posture: only the structural identity (``id`` /
``dateTime`` / ``version`` / ``driver``) rejects absence.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import DriverChange, DriverChangeDriverRef
from fleetpull.records import models_to_dataframe
from tests.geotab_driver_changes_capture import (
    DRIVER_CHANGE_FULL_RECORD,
    DRIVER_CHANGE_RECORDS,
    DRIVER_CHANGE_SENTINEL_RECORD,
)

# The wave-two structural identity: id, the event time, the version,
# and the primary entity ref. Everything else is optional (the
# conservative posture).
_REQUIRED_KEYS = frozenset({'dateTime', 'driver', 'id', 'version'})


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(DRIVER_CHANGE_RECORDS) == 3
        for record in DRIVER_CHANGE_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_both_driver_arms_ride_the_fixtures(self) -> None:
        wire_shapes = {
            type(record['driver']).__name__ for record in DRIVER_CHANGE_RECORDS
        }
        assert wire_shapes == {'str', 'dict'}

    def test_every_record_carries_a_version(self) -> None:
        for record in DRIVER_CHANGE_RECORDS:
            assert isinstance(record['version'], str)


class TestDriverChangeValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in DRIVER_CHANGE_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            DriverChange.model_validate(record)

    def test_every_record_validates(self) -> None:
        changes = [
            DriverChange.model_validate(record) for record in DRIVER_CHANGE_RECORDS
        ]
        assert [change.id for change in changes] == [
            'b23b201',
            'b23b202',
            'b23b203',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = DriverChange.model_validate(DRIVER_CHANGE_FULL_RECORD)
        for field_name in DriverChange.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert isinstance(full.driver, DriverChangeDriverRef)
        assert full.driver.id == 'b4C11'
        assert full.driver.is_driver is True
        assert full.version == '00000000000023b1'

    def test_sentinel_record_lands_the_string_driver_arm(self) -> None:
        # The proven mixed ref's string arm: the shared coercion lifts
        # the bare sentinel, so is_driver nulls exactly here.
        sentinel = DriverChange.model_validate(DRIVER_CHANGE_SENTINEL_RECORD)
        assert sentinel.driver.id == 'UnknownDriverId'
        assert sentinel.driver.is_driver is None

    def test_object_only_device_ref_still_lifts_a_bare_string(self) -> None:
        # The defensive lift on the census-object-only device ref (the
        # StatusData census-scope lesson).
        lifted = DriverChange.model_validate(
            {**DRIVER_CHANGE_FULL_RECORD, 'device': 'UnobservedSentinelId'}
        )
        assert lifted.device is not None
        assert lifted.device.id == 'UnobservedSentinelId'

    def test_unobserved_type_token_validates(self) -> None:
        # The census-open vocabulary posture with teeth: type is a plain
        # str mirror, so a token the census never showed must validate.
        change = DriverChange.model_validate(
            {**DRIVER_CHANGE_FULL_RECORD, 'type': 'UnobservedFutureType'}
        )
        assert change.type == 'UnobservedFutureType'


class TestDriverChangeFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [DriverChange.model_validate(record) for record in DRIVER_CHANGE_RECORDS],
            DriverChange,
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['device__id'] == pl.String
        assert frame.schema['driver__id'] == pl.String
        assert frame.schema['driver__is_driver'] == pl.Boolean
        # The sentinel flattening: the string lands verbatim, is_driver
        # nulls exactly on sentinel rows.
        for driver_id, is_driver in zip(
            frame['driver__id'].to_list(),
            frame['driver__is_driver'].to_list(),
            strict=True,
        ):
            assert (driver_id == 'UnknownDriverId') == (is_driver is None)

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [DriverChange.model_validate(record) for record in DRIVER_CHANGE_RECORDS],
            DriverChange,
        )
        empty = models_to_dataframe([], DriverChange)
        assert empty.height == 0
        assert empty.schema == populated.schema
