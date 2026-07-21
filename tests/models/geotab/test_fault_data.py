"""Tests for fleetpull.models.geotab.fault_data.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_fault_data_capture.py``), shaped by the wave two census
(13 census-total keys, the 2/2,000 rare quartet, NO per-record
``version``). Requiredness is the wave-two conservative posture: only
the structural identity (``id`` / ``dateTime`` / ``device``) rejects
absence; everything else is optional even where census-total.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import FaultData
from fleetpull.records import models_to_dataframe
from tests.geotab_fault_data_capture import (
    FAULT_DATA_FULL_RECORD,
    FAULT_DATA_RECORDS,
    FAULT_DATA_SPARSE_RECORD,
)

# The wave-two structural identity: id, the event time, and the primary
# entity ref. Everything else is optional (the conservative posture).
_REQUIRED_KEYS = frozenset({'dateTime', 'device', 'id'})

_RARE_QUARTET = ('diagnosticSeverity', 'riskOfBreakdown', 'severity', 'sourceAddress')


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(FAULT_DATA_RECORDS) == 3
        for record in FAULT_DATA_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_both_failure_mode_arms_ride_the_fixtures(self) -> None:
        wire_shapes = {
            type(record['failureMode']).__name__ for record in FAULT_DATA_RECORDS
        }
        assert wire_shapes == {'str', 'dict'}

    def test_the_rare_quartet_rides_only_the_full_record(self) -> None:
        # 2/2,000 presence in the census: present on the full record,
        # absent elsewhere — the optional absent arm.
        for key in _RARE_QUARTET:
            assert key in FAULT_DATA_FULL_RECORD
            assert key not in FAULT_DATA_SPARSE_RECORD
            assert key not in FAULT_DATA_RECORDS[2]

    def test_no_record_carries_a_version(self) -> None:
        # The LogRecord asymmetry: this active feed does NOT version its
        # records, and the model mirrors the absence.
        for record in FAULT_DATA_RECORDS:
            assert 'version' not in record
        assert 'version' not in FaultData.model_fields


class TestFaultDataValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in FAULT_DATA_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            FaultData.model_validate(record)

    def test_every_record_validates(self) -> None:
        faults = [FaultData.model_validate(record) for record in FAULT_DATA_RECORDS]
        assert [fault.id for fault in faults] == ['b21f201', 'b21f202', 'b21f203']

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = FaultData.model_validate(FAULT_DATA_FULL_RECORD)
        for field_name in FaultData.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert full.diagnostic is not None
        assert full.diagnostic.id == 'DiagnosticEngineOilPressureId'
        assert full.fault_states is not None
        assert full.fault_states.effective_status == 'Active'
        assert full.risk_of_breakdown == 0.85
        assert full.source_address == 0

    def test_sparse_record_nulls_the_rare_quartet(self) -> None:
        sparse = FaultData.model_validate(FAULT_DATA_SPARSE_RECORD)
        assert sparse.diagnostic_severity is None
        assert sparse.risk_of_breakdown is None
        assert sparse.severity is None
        assert sparse.source_address is None

    def test_failure_mode_rides_both_wire_arms(self) -> None:
        # The proven mixed ref: the object arm and the bare known-id
        # string arm both land as failure_mode__id.
        object_arm = FaultData.model_validate(FAULT_DATA_FULL_RECORD)
        assert object_arm.failure_mode is not None
        assert object_arm.failure_mode.id == 'bFA31'
        string_arm = FaultData.model_validate(FAULT_DATA_SPARSE_RECORD)
        assert string_arm.failure_mode is not None
        assert string_arm.failure_mode.id == 'NoFailureModeId'

    @pytest.mark.parametrize('reference_key', ['controller', 'device', 'diagnostic'])
    def test_object_only_refs_still_lift_a_bare_string(
        self, reference_key: str
    ) -> None:
        # The defensive lift on the census-object-only refs (the
        # StatusData census-scope lesson): an unobserved sentinel arm
        # must land as the ref's id, never crash.
        lifted = FaultData.model_validate(
            {**FAULT_DATA_FULL_RECORD, reference_key: 'UnobservedSentinelId'}
        )
        reference = getattr(lifted, reference_key)
        assert reference is not None
        assert reference.id == 'UnobservedSentinelId'

    def test_unobserved_vocabulary_tokens_validate(self) -> None:
        # The census-open vocabulary posture with teeth: faultState and
        # effectiveStatus are plain str mirrors, so tokens the census
        # never showed must validate rather than reject.
        record = {
            **FAULT_DATA_FULL_RECORD,
            'faultState': 'UnobservedFutureState',
            'faultStates': {'effectiveStatus': 'UnobservedFutureStatus'},
        }
        fault = FaultData.model_validate(record)
        assert fault.fault_state == 'UnobservedFutureState'
        assert fault.fault_states is not None
        assert fault.fault_states.effective_status == 'UnobservedFutureStatus'


class TestFaultDataFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [FaultData.model_validate(record) for record in FAULT_DATA_RECORDS],
            FaultData,
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['count'] == pl.Int64
        assert frame.schema['risk_of_breakdown'] == pl.Float64
        assert frame.schema['device__id'] == pl.String
        assert frame.schema['failure_mode__id'] == pl.String
        assert frame.schema['fault_states__effective_status'] == pl.String
        assert frame['risk_of_breakdown'].to_list() == [0.85, None, None]
        assert frame['failure_mode__id'].to_list() == [
            'bFA31',
            'NoFailureModeId',
            'bFA35',
        ]

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [FaultData.model_validate(record) for record in FAULT_DATA_RECORDS],
            FaultData,
        )
        empty = models_to_dataframe([], FaultData)
        assert empty.height == 0
        assert empty.schema == populated.schema
