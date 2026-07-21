"""Tests for fleetpull.models.geotab.audit.

Every fixture is the committed synthetic 2026-07-21 fixture set
(``tests/geotab_audits_capture.py``), shaped by the wave three SCALE
census (six keys, census-total on 20,000 records). Audit is the simplest
vertical — NO reference fields. Requiredness is the wave-two
conservative posture: only the structural identity (``id`` / ``dateTime``
/ ``version``) rejects absence.
"""

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.models.geotab import Audit
from fleetpull.records import models_to_dataframe
from tests.geotab_audits_capture import (
    AUDIT_FULL_RECORD,
    AUDIT_RECORDS,
    AUDIT_SPARSE_RECORD,
)

# The wave-two structural identity for the ref-less vertical: id, the
# event time, and the version. Everything else is optional.
_REQUIRED_KEYS = frozenset({'dateTime', 'id', 'version'})


class TestFixtureProperties:
    """The variant coverage the fixture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(AUDIT_RECORDS) == 3
        for record in AUDIT_RECORDS:
            assert set(record) >= _REQUIRED_KEYS

    def test_the_optional_comment_is_absent_on_the_sparse_record(self) -> None:
        assert 'comment' in AUDIT_FULL_RECORD
        assert 'comment' not in AUDIT_SPARSE_RECORD

    def test_every_record_carries_a_version(self) -> None:
        for record in AUDIT_RECORDS:
            assert isinstance(record['version'], str)


class TestAuditValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in AUDIT_FULL_RECORD.items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            Audit.model_validate(record)

    def test_every_record_validates(self) -> None:
        audits = [Audit.model_validate(record) for record in AUDIT_RECORDS]
        assert [audit.id for audit in audits] == ['bAU201', 'bAU202', 'bAU203']

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Trip pattern).
        full = Audit.model_validate(AUDIT_FULL_RECORD)
        for field_name in Audit.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert full.user_name == 'user.synthetic001'
        assert full.name == 'Synthetic Rule Alpha'
        assert full.version == '0000000000002c01'

    def test_sparse_record_nulls_the_optional_comment(self) -> None:
        sparse = Audit.model_validate(AUDIT_SPARSE_RECORD)
        assert sparse.comment is None
        assert sparse.user_name == 'user.synthetic002'

    def test_unobserved_vocabulary_strings_validate(self) -> None:
        # comment/name/userName are census-open str mirrors, so
        # unobserved values must validate.
        audit = Audit.model_validate(
            {
                **AUDIT_FULL_RECORD,
                'name': 'An Unobserved Object',
                'userName': 'user.unobserved999',
            }
        )
        assert audit.name == 'An Unobserved Object'
        assert audit.user_name == 'user.unobserved999'


class TestAuditFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [Audit.model_validate(record) for record in AUDIT_RECORDS], Audit
        )
        assert frame.height == 3
        assert frame.schema['date_time'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['comment'] == pl.String
        assert frame.schema['user_name'] == pl.String
        assert frame['comment'].to_list() == [
            'Synthetic audit comment one.',
            None,
            'Synthetic audit comment three.',
        ]

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [Audit.model_validate(record) for record in AUDIT_RECORDS], Audit
        )
        empty = models_to_dataframe([], Audit)
        assert empty.height == 0
        assert empty.schema == populated.schema
