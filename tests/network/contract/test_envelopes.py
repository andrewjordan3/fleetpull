"""Tests for fleetpull.network.contract.envelopes."""

import pytest
from pydantic import BaseModel, ConfigDict

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract.envelopes import validated_envelope_slice


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
