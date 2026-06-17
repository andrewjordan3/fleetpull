# tests/model_contract/test_response.py
"""Tests for fleetpull.model_contract."""

import pytest
from pydantic import Field, ValidationError

from fleetpull.model_contract import ResponseModel


class _SampleRecord(ResponseModel):
    """A minimal response model with one aliased field, for exercising the base."""

    vehicle_id: str = Field(alias='vehicleId')
    odometer: int


class TestResponseModel:
    def test_valid_payload_validates_to_typed_values(self) -> None:
        record = _SampleRecord.model_validate({'vehicleId': 'V1', 'odometer': 42})
        assert record.vehicle_id == 'V1'
        assert record.odometer == 42

    def test_unknown_extra_field_is_ignored(self) -> None:
        record = _SampleRecord.model_validate(
            {'vehicleId': 'V1', 'odometer': 42, 'surprise': 'dropped'}
        )
        assert not hasattr(record, 'surprise')

    def test_surrounding_whitespace_is_stripped(self) -> None:
        record = _SampleRecord.model_validate({'vehicleId': '  V1  ', 'odometer': 42})
        assert record.vehicle_id == 'V1'

    def test_construction_by_field_name_succeeds(self) -> None:
        record = _SampleRecord.model_validate({'vehicle_id': 'V1', 'odometer': 42})
        assert record.vehicle_id == 'V1'

    def test_loose_numeric_coerces(self) -> None:
        record = _SampleRecord.model_validate({'vehicleId': 'V1', 'odometer': '5'})
        assert record.odometer == 5

    def test_mutation_after_construction_raises(self) -> None:
        record = _SampleRecord.model_validate({'vehicleId': 'V1', 'odometer': 42})
        with pytest.raises(ValidationError):
            record.odometer = 10  # type: ignore[misc]  # the frozen guard under test
