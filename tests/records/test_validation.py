"""Tests for fleetpull.records.validation."""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.model_contract import ResponseModel
from fleetpull.records.validation import validate_records


class _Sample(ResponseModel):
    sample_id: int
    name: str


def test_validates_good_records() -> None:
    models = validate_records(
        [{'sample_id': 1, 'name': 'a'}, {'sample_id': 2, 'name': 'b'}], _Sample
    )
    assert [model.sample_id for model in models] == [1, 2]


def test_lax_coercion_lands_stringly_numbers() -> None:
    models = validate_records([{'sample_id': '7', 'name': 'a'}], _Sample)
    assert models[0].sample_id == 7


def test_fails_fast_and_names_the_record() -> None:
    with pytest.raises(ProviderResponseError, match='record 1'):
        validate_records(
            [{'sample_id': 1, 'name': 'a'}, {'sample_id': 'NaN', 'name': 'b'}],
            _Sample,
        )


def test_error_excludes_raw_values() -> None:
    with pytest.raises(ProviderResponseError) as caught:
        validate_records([{'sample_id': 'bad', 'name': 'a'}], _Sample)
    assert 'bad' not in str(caught.value)
