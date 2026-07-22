"""Tests for fleetpull.models.geotab.annotation_log.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_annotation_logs_capture.py``), shaped by the wave three
SCALE census (six keys, census-total on 8,857 records). Requiredness is
the wave-two conservative posture: only the structural identity (``id``
/ ``dateTime`` / ``version`` / ``dutyStatusLog``) rejects absence.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import AnnotationLog, AnnotationLogDutyStatusLogRef
from fleetpull.records import models_to_dataframe
from tests.geotab_annotation_logs_capture import (
    ANNOTATION_LOG_FULL_RECORD,
    ANNOTATION_LOG_RECORDS,
    ANNOTATION_LOG_SPARSE_RECORD,
)

# The wave-two structural identity: id, the event time, the version, and
# the primary entity ref (dutyStatusLog, the annotated log — the
# annotation's subject). Everything else is optional.
_REQUIRED_KEYS = frozenset({'dateTime', 'dutyStatusLog', 'id', 'version'})


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(ANNOTATION_LOG_RECORDS) == 3
        for record in ANNOTATION_LOG_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_the_optional_driver_is_absent_on_the_sparse_record(self) -> None:
        assert 'driver' in ANNOTATION_LOG_FULL_RECORD
        assert 'driver' not in ANNOTATION_LOG_SPARSE_RECORD

    def test_every_record_carries_a_version(self) -> None:
        for record in ANNOTATION_LOG_RECORDS:
            assert isinstance(record['version'], str)


class TestAnnotationLogValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in ANNOTATION_LOG_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            AnnotationLog.model_validate(record)

    def test_every_record_validates(self) -> None:
        annotations = [
            AnnotationLog.model_validate(record) for record in ANNOTATION_LOG_RECORDS
        ]
        assert [annotation.id for annotation in annotations] == [
            'bAL201',
            'bAL202',
            'bAL203',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = AnnotationLog.model_validate(ANNOTATION_LOG_FULL_RECORD)
        for field_name in AnnotationLog.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert isinstance(full.duty_status_log, AnnotationLogDutyStatusLogRef)
        assert full.duty_status_log.id == 'bDS401'
        assert full.driver is not None
        assert full.driver.id == 'bDR701'
        assert full.version == '0000000000002a01'

    def test_sparse_record_nulls_the_optional_driver(self) -> None:
        sparse = AnnotationLog.model_validate(ANNOTATION_LOG_SPARSE_RECORD)
        assert sparse.driver is None
        assert sparse.duty_status_log.id == 'bDS402'

    @pytest.mark.parametrize('reference_key', ['driver', 'dutyStatusLog'])
    def test_object_only_refs_still_lift_a_bare_string(
        self, reference_key: str
    ) -> None:
        # The defensive lift on the census-object-only refs (the
        # StatusData census-scope lesson).
        lifted = AnnotationLog.model_validate(
            {**ANNOTATION_LOG_FULL_RECORD, reference_key: 'UnobservedSentinelId'}
        )
        # dutyStatusLog aliases to duty_status_log; driver keeps its name.
        attribute = 'duty_status_log' if reference_key == 'dutyStatusLog' else 'driver'
        reference = getattr(lifted, attribute)
        assert reference is not None
        assert reference.id == 'UnobservedSentinelId'

    def test_unobserved_comment_validates(self) -> None:
        # comment is a census-open free-text mirror.
        annotation = AnnotationLog.model_validate(
            {**ANNOTATION_LOG_FULL_RECORD, 'comment': 'An unobserved comment.'}
        )
        assert annotation.comment == 'An unobserved comment.'


class TestAnnotationLogFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [AnnotationLog.model_validate(record) for record in ANNOTATION_LOG_RECORDS],
            AnnotationLog,
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['duty_status_log__id'] == pl.String
        assert frame.schema['driver__id'] == pl.String
        # The back-reference joins to duty_status_logs.annotations.
        assert frame['duty_status_log__id'].to_list() == ['bDS401', 'bDS402', 'bDS403']
        assert frame['driver__id'].to_list() == ['bDR701', None, 'bDR725']

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [AnnotationLog.model_validate(record) for record in ANNOTATION_LOG_RECORDS],
            AnnotationLog,
        )
        empty = models_to_dataframe([], AnnotationLog)
        assert empty.height == 0
        assert empty.schema == populated.schema
