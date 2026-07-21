"""Tests for fleetpull.models.geotab.duty_status_log.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_duty_status_logs_capture.py``), shaped by the wave two
census. Requiredness is the wave-two conservative posture: only the
structural identity (``id`` / ``dateTime`` / ``version`` / ``driver``)
rejects absence. The strict ``annotations`` id-lift's teeth are pinned
here: a shape change must fail loudly, never silently drop sibling
keys.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import DutyStatusLog
from fleetpull.records import models_to_dataframe
from tests.geotab_duty_status_logs_capture import (
    DUTY_STATUS_LOG_FULL_RECORD,
    DUTY_STATUS_LOG_RECORDS,
    DUTY_STATUS_LOG_SPARSE_RECORD,
)

# The wave-two structural identity: id, the event time, the version,
# and the primary entity ref. Everything else is optional (the
# conservative posture).
_REQUIRED_KEYS = frozenset({'dateTime', 'driver', 'id', 'version'})


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(DUTY_STATUS_LOG_RECORDS) == 3
        for record in DUTY_STATUS_LOG_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    @pytest.mark.parametrize('reference_key', ['device', 'driver'])
    def test_both_ref_arms_ride_the_fixtures(self, reference_key: str) -> None:
        wire_shapes = {
            type(record[reference_key]).__name__ for record in DUTY_STATUS_LOG_RECORDS
        }
        assert wire_shapes == {'str', 'dict'}

    def test_annotations_ride_only_the_full_record_as_id_objects(self) -> None:
        # 126/2,000 presence: wire elements are exactly {id} objects on
        # the one carrier, absent elsewhere.
        annotations = DUTY_STATUS_LOG_FULL_RECORD['annotations']
        assert isinstance(annotations, list)
        for element in annotations:
            assert isinstance(element, dict)
            assert set(element) == {'id'}
        assert 'annotations' not in DUTY_STATUS_LOG_SPARSE_RECORD
        assert 'annotations' not in DUTY_STATUS_LOG_RECORDS[2]

    def test_both_numeric_arms_ride_the_fixtures(self) -> None:
        full_arms = {
            type(DUTY_STATUS_LOG_FULL_RECORD[key]).__name__
            for key in ('engineHours', 'odometer', 'distanceSinceValidCoordinates')
        }
        terminal_arms = {
            type(DUTY_STATUS_LOG_RECORDS[2][key]).__name__
            for key in ('engineHours', 'odometer', 'distanceSinceValidCoordinates')
        }
        assert full_arms == {'float'}
        assert terminal_arms == {'int'}

    def test_all_three_location_arms_ride_the_fixtures(self) -> None:
        # The wrapper's coordinate arm (record 1), absence (record 2),
        # and the address arm (record 3, the live-proof-found arm).
        coordinate_arm = DUTY_STATUS_LOG_FULL_RECORD['location']
        address_arm = DUTY_STATUS_LOG_RECORDS[2]['location']
        assert isinstance(coordinate_arm, dict)
        assert isinstance(address_arm, dict)
        assert set(coordinate_arm) == {'location'}
        assert 'location' not in DUTY_STATUS_LOG_SPARSE_RECORD
        assert set(address_arm) == {'address'}


class TestDutyStatusLogValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in DUTY_STATUS_LOG_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            DutyStatusLog.model_validate(record)

    def test_every_record_validates(self) -> None:
        logs = [
            DutyStatusLog.model_validate(record) for record in DUTY_STATUS_LOG_RECORDS
        ]
        assert [log.id for log in logs] == ['b22a201', 'b22a202', 'b22a203']

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = DutyStatusLog.model_validate(DUTY_STATUS_LOG_FULL_RECORD)
        for field_name in DutyStatusLog.model_fields:
            assert getattr(full, field_name) is not None, field_name
        # The strict annotations lift: exactly-{id} elements reduce to
        # their ids.
        assert full.annotations == ['bAA31', 'bAA32']
        # The shared wrapper's coordinate arm: x longitude, y latitude,
        # the address arm absent.
        assert full.location is not None
        assert full.location.location is not None
        assert full.location.location.x == -140.25
        assert full.location.location.y == 35.5
        assert full.location.address is None

    def test_sparse_record_nulls_the_partial_presence_block(self) -> None:
        sparse = DutyStatusLog.model_validate(DUTY_STATUS_LOG_SPARSE_RECORD)
        assert sparse.annotations is None
        assert sparse.distance_since_valid_coordinates is None
        assert sparse.engine_hours is None
        assert sparse.event_code is None
        assert sparse.event_type is None
        assert sparse.location is None
        assert sparse.odometer is None
        assert sparse.sequence is None
        assert sparse.verify_date_time is None

    def test_both_ref_arms_land_as_the_ref_id(self) -> None:
        # The proven mixed device/driver refs: the sparse record's bare
        # strings lift to {'id': ...}; the full record's objects pass.
        sparse = DutyStatusLog.model_validate(DUTY_STATUS_LOG_SPARSE_RECORD)
        assert sparse.device is not None
        assert sparse.device.id == 'NoDeviceId'
        assert sparse.driver.id == 'UnknownDriverId'
        full = DutyStatusLog.model_validate(DUTY_STATUS_LOG_FULL_RECORD)
        assert full.device is not None
        assert full.device.id == 'b8A1'
        assert full.driver.id == 'b4C11'

    def test_the_location_address_arm_lands_beside_null_coordinates(self) -> None:
        # The arm the live-proof walk found beyond the 200-sample census:
        # record 3 carries the wrapper's address arm, so its coordinate
        # block is None and the formatted address lands.
        terminal = DutyStatusLog.model_validate(DUTY_STATUS_LOG_RECORDS[2])
        assert terminal.location is not None
        assert terminal.location.location is None
        assert terminal.location.address is not None
        assert (
            terminal.location.address.formatted_address
            == '100 Example Rd, Testton, TS, USA'
        )

    def test_the_int_numeric_arms_land_as_float(self) -> None:
        terminal = DutyStatusLog.model_validate(DUTY_STATUS_LOG_RECORDS[2])
        assert terminal.engine_hours == 5340.0
        assert isinstance(terminal.engine_hours, float)
        assert terminal.odometer == 482400.0
        assert isinstance(terminal.odometer, float)
        assert terminal.distance_since_valid_coordinates == 2.0
        assert isinstance(terminal.distance_since_valid_coordinates, float)

    def test_bare_string_annotation_elements_pass_the_lift(self) -> None:
        # The lift's string arm: a bare id element passes verbatim.
        log = DutyStatusLog.model_validate(
            {**DUTY_STATUS_LOG_FULL_RECORD, 'annotations': ['bAA31', {'id': 'bAA32'}]}
        )
        assert log.annotations == ['bAA31', 'bAA32']

    @pytest.mark.parametrize(
        'bad_element',
        [
            {'id': 'bAA31', 'comment': 'sibling key'},
            {'comment': 'no id at all'},
            {'id': 7},
            7,
            ['bAA31'],
        ],
    )
    # typing-justified: the parametrized breakage covers arbitrary wire shapes
    def test_annotation_shape_changes_fail_loudly(self, bad_element: object) -> None:
        # The strict lift's teeth: an element beyond bare-str or exactly
        # {'id': <str>} must raise, never silently drop sibling keys.
        with pytest.raises(ValidationError):
            DutyStatusLog.model_validate(
                {**DUTY_STATUS_LOG_FULL_RECORD, 'annotations': [bad_element]}
            )

    def test_unobserved_vocabulary_tokens_validate(self) -> None:
        # The census-open vocabulary posture with teeth: the status-ish
        # strs are plain mirrors, so tokens the census never showed must
        # validate rather than reject.
        record = {
            **DUTY_STATUS_LOG_FULL_RECORD,
            'deferralStatus': 'UnobservedFutureDeferral',
            'malfunction': 'UnobservedFutureMalfunction',
            'origin': 'UnobservedFutureOrigin',
            'state': 'UnobservedFutureState',
            'status': 'UnobservedFutureStatus',
        }
        log = DutyStatusLog.model_validate(record)
        assert log.status == 'UnobservedFutureStatus'
        assert log.origin == 'UnobservedFutureOrigin'


class TestDutyStatusLogFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [
                DutyStatusLog.model_validate(record)
                for record in DUTY_STATUS_LOG_RECORDS
            ],
            DutyStatusLog,
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['annotations'] == pl.List(pl.String)
        assert frame.schema['engine_hours'] == pl.Float64
        assert frame.schema['location__location__x'] == pl.Float64
        assert frame.schema['location__address__formatted_address'] == pl.String
        assert frame.schema['device__id'] == pl.String
        assert frame.schema['driver__id'] == pl.String
        assert frame['engine_hours'].to_list() == [5321.5, None, 5340.0]
        assert frame['annotations'].to_list() == [['bAA31', 'bAA32'], None, None]
        # The two location arms are column-exclusive per record: the
        # coordinate record nulls the address column and vice versa.
        assert frame['location__location__x'].to_list() == [-140.25, None, None]
        assert frame['location__address__formatted_address'].to_list() == [
            None,
            None,
            '100 Example Rd, Testton, TS, USA',
        ]

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [
                DutyStatusLog.model_validate(record)
                for record in DUTY_STATUS_LOG_RECORDS
            ],
            DutyStatusLog,
        )
        empty = models_to_dataframe([], DutyStatusLog)
        assert empty.height == 0
        assert empty.schema == populated.schema
