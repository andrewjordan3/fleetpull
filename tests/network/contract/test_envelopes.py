"""Tests for fleetpull.network.contract.envelopes."""

import pytest
from pydantic import BaseModel, ConfigDict

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract.envelopes import (
    require_record_list,
    unwrap_record_objects,
    validated_envelope_slice,
)


class ProbeSlice(BaseModel):
    """Minimal envelope-slice stand-in for exercising the validator."""

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    cursor: str


class TestValidatedEnvelopeSlice:
    def test_happy_path_returns_the_validated_slice(self) -> None:
        validated_slice = validated_envelope_slice(
            ProbeSlice, {'cursor': 'cursor-0001', 'records': []}
        )
        assert validated_slice.cursor == 'cursor-0001'

    def test_failure_translates_to_provider_response_error(self) -> None:
        with pytest.raises(ProviderResponseError) as exception_info:
            validated_envelope_slice(ProbeSlice, {'cursor': 12345})
        # The detail carries Pydantic's field-level complaint.
        assert 'malformed response envelope' in str(exception_info.value)
        assert 'cursor' in str(exception_info.value)

    def test_non_dict_envelope_fails_through_the_same_path(self) -> None:
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            validated_envelope_slice(ProbeSlice, 'not an envelope')


class TestRequireRecordList:
    def test_returns_the_record_list(self) -> None:
        records = require_record_list({'data': [{'id': 1}, {'id': 2}]}, 'data')
        assert records == [{'id': 1}, {'id': 2}]

    def test_non_object_envelope_raises(self) -> None:
        with pytest.raises(ProviderResponseError, match='envelope is not a JSON'):
            require_record_list('not an object', 'data')

    def test_missing_key_raises(self) -> None:
        with pytest.raises(ProviderResponseError, match='missing the record key'):
            require_record_list({'other': []}, 'data')

    def test_value_not_a_list_raises(self) -> None:
        with pytest.raises(ProviderResponseError, match='is not a list'):
            require_record_list({'data': {'id': 1}}, 'data')

    def test_non_object_element_raises(self) -> None:
        with pytest.raises(ProviderResponseError, match='is not a JSON object'):
            require_record_list({'data': [{'id': 1}, 42]}, 'data')


class TestUnwrapRecordObjects:
    def test_unwraps_each_inner_object(self) -> None:
        records = unwrap_record_objects(
            [{'vehicle': {'id': 1}}, {'vehicle': {'id': 2}}], 'vehicle'
        )
        assert records == [{'id': 1}, {'id': 2}]

    def test_missing_item_key_raises(self) -> None:
        with pytest.raises(ProviderResponseError, match='missing the item key'):
            unwrap_record_objects([{'other': {'id': 1}}], 'vehicle')

    def test_non_object_inner_raises(self) -> None:
        with pytest.raises(ProviderResponseError, match='is not a JSON object'):
            unwrap_record_objects([{'vehicle': 42}], 'vehicle')
