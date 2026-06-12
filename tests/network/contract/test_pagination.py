"""Tests for fleetpull.network.contract.pagination."""

import dataclasses

import pytest
from pydantic import BaseModel, ConfigDict

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract.pagination import (
    PageAdvance,
    validate_pagination_envelope,
)


class ProbeSlice(BaseModel):
    """Minimal envelope-slice stand-in for exercising the validator."""

    model_config = ConfigDict(frozen=True, extra='ignore')

    cursor: str


class TestPageAdvance:
    def test_is_frozen(self) -> None:
        verdict = PageAdvance(next_spec=None, durable_progress=None)
        with pytest.raises(dataclasses.FrozenInstanceError):
            verdict.durable_progress = 'other'  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        verdict = PageAdvance(next_spec=None, durable_progress=None)
        assert not hasattr(verdict, '__dict__')

    def test_complete_verdicts_may_still_carry_progress(self) -> None:
        # The GeoTab terminal shape, pinned as a vocabulary-level fact:
        # the terminal page's durable progress is the resume point.
        verdict = PageAdvance(next_spec=None, durable_progress='0000000000000001')
        assert verdict.next_spec is None
        assert verdict.durable_progress == '0000000000000001'


class TestValidatePaginationEnvelope:
    def test_happy_path_returns_the_validated_slice(self) -> None:
        validated_slice = validate_pagination_envelope(
            ProbeSlice, {'cursor': 'cursor-0001', 'records': []}
        )
        assert validated_slice.cursor == 'cursor-0001'

    def test_failure_translates_to_provider_response_error(self) -> None:
        with pytest.raises(ProviderResponseError) as exception_info:
            validate_pagination_envelope(ProbeSlice, {'cursor': 12345})
        # The detail carries Pydantic's field-level complaint.
        assert 'malformed pagination metadata' in str(exception_info.value)
        assert 'cursor' in str(exception_info.value)

    def test_non_dict_envelope_fails_through_the_same_path(self) -> None:
        with pytest.raises(ProviderResponseError, match='malformed pagination'):
            validate_pagination_envelope(ProbeSlice, 'not an envelope')
