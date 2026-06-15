"""Tests for fleetpull.network.contract.outcome."""

import dataclasses

import pytest

from fleetpull.network.contract.outcome import ClassifiedResponse
from fleetpull.vocabulary import ResponseCategory


class TestClassifiedResponse:
    def test_is_frozen(self) -> None:
        outcome = ClassifiedResponse(category=ResponseCategory.SUCCESS)
        with pytest.raises(dataclasses.FrozenInstanceError):
            outcome.category = ResponseCategory.FATAL  # type: ignore[misc]

    def test_has_slots_no_dict(self) -> None:
        outcome = ClassifiedResponse(category=ResponseCategory.SUCCESS)
        assert not hasattr(outcome, '__dict__')

    def test_optional_fields_default_to_none(self) -> None:
        outcome = ClassifiedResponse(category=ResponseCategory.SUCCESS)
        assert outcome.retry_after_seconds is None
        assert outcome.detail is None
        assert outcome.parsed_body is None

    def test_repr_excludes_parsed_body(self) -> None:
        # parsed_body can hold a multi-megabyte structure; a log line
        # formatting the outcome must never embed it.
        outcome = ClassifiedResponse(
            category=ResponseCategory.SUCCESS,
            parsed_body={'data': ['sentinel-payload']},
        )
        assert 'sentinel-payload' not in repr(outcome)
