"""Tests for fleetpull.models.geotab.dvir_log.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_dvir_logs_capture.py``), shaped by the wave two census.
Requiredness is the wave-two conservative posture: only the structural
identity (``id`` / ``dateTime`` / ``version`` / ``driver``) rejects
absence. The ``defectList.children`` exclusion is pinned here: a
non-empty children list must be absorbed, never crash.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import DvirLog, DvirLogDefectList
from fleetpull.records import models_to_dataframe
from tests.geotab_dvir_logs_capture import (
    DVIR_LOG_FULL_RECORD,
    DVIR_LOG_RECORDS,
    DVIR_LOG_SPARSE_RECORD,
)

# The wave-two structural identity: id, the event time, the version,
# and the primary entity ref (driver, 500/500 — device is only 205/500
# and could not be it). Everything else is optional.
_REQUIRED_KEYS = frozenset({'dateTime', 'driver', 'id', 'version'})


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(DVIR_LOG_RECORDS) == 3
        for record in DVIR_LOG_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_the_device_trio_travels_together(self) -> None:
        # The census wire fact (205/500 each): device, engineHours, and
        # odometer present on the full and terminal records, all absent
        # on the sparse record.
        for key in ('device', 'engineHours', 'odometer'):
            assert key in DVIR_LOG_FULL_RECORD
            assert key in DVIR_LOG_RECORDS[2]
            assert key not in DVIR_LOG_SPARSE_RECORD

    def test_children_is_empty_on_every_record(self) -> None:
        # The documented exclusion's census truth: children was an empty
        # list on all 200 sampled defectList nodes (the nested-block
        # sample depth), so the fixtures mirror it.
        for record in DVIR_LOG_RECORDS:
            defect_list = record['defectList']
            assert isinstance(defect_list, dict)
            assert defect_list['children'] == []

    def test_engine_hours_is_a_bare_int_on_its_carriers(self) -> None:
        # The census's int-only observation on this surface; the model's
        # float lift is the cross-surface decision (DutyStatusLog proved
        # the same quantity mixed).
        assert isinstance(DVIR_LOG_FULL_RECORD['engineHours'], int)
        assert isinstance(DVIR_LOG_RECORDS[2]['engineHours'], int)


class TestDvirLogValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in DVIR_LOG_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            DvirLog.model_validate(record)

    def test_every_record_validates(self) -> None:
        inspections = [DvirLog.model_validate(record) for record in DVIR_LOG_RECORDS]
        assert [inspection.id for inspection in inspections] == [
            'b24c201',
            'b24c202',
            'b24c203',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = DvirLog.model_validate(DVIR_LOG_FULL_RECORD)
        for field_name in DvirLog.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert full.defect_list is not None
        assert full.defect_list.id == 'bDL41'
        assert full.defect_list.name == 'Truck Defects'
        assert full.trailer is not None
        assert full.trailer.id == 'b9C41'
        assert full.location is not None
        assert full.location.location is not None
        assert full.location.location.x == -140.25

    def test_sparse_record_nulls_the_absent_block(self) -> None:
        sparse = DvirLog.model_validate(DVIR_LOG_SPARSE_RECORD)
        assert sparse.device is None
        assert sparse.engine_hours is None
        assert sparse.odometer is None
        assert sparse.trailer is None
        assert sparse.location is None

    def test_the_int_engine_hours_arm_lands_as_float(self) -> None:
        full = DvirLog.model_validate(DVIR_LOG_FULL_RECORD)
        assert full.engine_hours == 5320.0
        assert isinstance(full.engine_hours, float)

    def test_duration_is_mirrored_verbatim(self) -> None:
        # The opaque duration string: never parsed, never reshaped.
        full = DvirLog.model_validate(DVIR_LOG_FULL_RECORD)
        assert full.duration == '00:12:30'

    def test_populated_children_are_absorbed_not_crashed(self) -> None:
        # The documented-exclusion pin: children is not a model field
        # (empty on all 200 sampled defectList nodes — its element shape
        # is unobservable at this tenant), and extra='ignore' absorbs a
        # tenant that DOES populate it. The revisit condition lives on
        # the model docstring.
        assert 'children' not in DvirLogDefectList.model_fields
        defect_list = DVIR_LOG_FULL_RECORD['defectList']
        assert isinstance(defect_list, dict)
        populated = {
            **defect_list,
            'children': [{'id': 'bDL99', 'name': 'Brakes', 'severity': 'Critical'}],
        }
        inspection = DvirLog.model_validate(
            {**DVIR_LOG_FULL_RECORD, 'defectList': populated}
        )
        assert inspection.defect_list is not None
        assert inspection.defect_list.id == 'bDL41'

    @pytest.mark.parametrize('reference_key', ['device', 'driver', 'trailer'])
    def test_object_only_refs_still_lift_a_bare_string(
        self, reference_key: str
    ) -> None:
        # The defensive lift on the census-object-only refs (the
        # StatusData census-scope lesson).
        lifted = DvirLog.model_validate(
            {**DVIR_LOG_FULL_RECORD, reference_key: 'UnobservedSentinelId'}
        )
        reference = getattr(lifted, reference_key)
        assert reference is not None
        assert reference.id == 'UnobservedSentinelId'

    def test_unobserved_log_type_token_validates(self) -> None:
        # The census-open vocabulary posture with teeth: logType is a
        # plain str mirror, so a token the census never showed must
        # validate.
        inspection = DvirLog.model_validate(
            {**DVIR_LOG_FULL_RECORD, 'logType': 'UnobservedFutureType'}
        )
        assert inspection.log_type == 'UnobservedFutureType'


class TestDvirLogFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [DvirLog.model_validate(record) for record in DVIR_LOG_RECORDS], DvirLog
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['engine_hours'] == pl.Float64
        assert frame.schema['odometer'] == pl.Float64
        assert frame.schema['defect_list__id'] == pl.String
        assert frame.schema['location__location__y'] == pl.Float64
        # The exclusion holds in the derived schema: no children column.
        assert 'defect_list__children' not in frame.columns
        assert frame['engine_hours'].to_list() == [5320.0, None, 5341.0]
        assert frame['trailer__id'].to_list() == ['b9C41', None, 'b9C55']

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [DvirLog.model_validate(record) for record in DVIR_LOG_RECORDS], DvirLog
        )
        empty = models_to_dataframe([], DvirLog)
        assert empty.height == 0
        assert empty.schema == populated.schema
