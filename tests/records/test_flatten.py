"""Tests for fleetpull.records.flatten."""

from enum import StrEnum

from fleetpull.model_contract import ResponseModel
from fleetpull.records.fields import iter_flat_fields
from fleetpull.records.flatten import flatten_record


class _Color(StrEnum):
    RED = 'red'


class _Leaf(ResponseModel):
    leaf_id: int
    label: str | None = None


class _Sample(ResponseModel):
    sample_id: int
    color: _Color
    tags: list[str]
    leaf: _Leaf | None = None


def _fields(model_class: type[ResponseModel]) -> tuple:
    return tuple(iter_flat_fields(model_class))


def test_flattens_scalars_and_list() -> None:
    record = _Sample(sample_id=1, color=_Color.RED, tags=['a', 'b'])
    row = flatten_record(record=record, flat_fields=_fields(_Sample))
    assert row['sample_id'] == 1
    assert row['tags'] == ['a', 'b']


def test_enum_value_reduced_to_string() -> None:
    record = _Sample(sample_id=1, color=_Color.RED, tags=[])
    row = flatten_record(record=record, flat_fields=_fields(_Sample))
    assert row['color'] == 'red'
    assert isinstance(row['color'], str)


def test_present_nested_block_flattens() -> None:
    record = _Sample(
        sample_id=1, color=_Color.RED, tags=[], leaf=_Leaf(leaf_id=9, label='x')
    )
    row = flatten_record(record=record, flat_fields=_fields(_Sample))
    assert row['leaf__leaf_id'] == 9
    assert row['leaf__label'] == 'x'


def test_absent_nested_block_yields_nulls() -> None:
    record = _Sample(sample_id=1, color=_Color.RED, tags=[], leaf=None)
    row = flatten_record(record=record, flat_fields=_fields(_Sample))
    assert row['leaf__leaf_id'] is None
    assert row['leaf__label'] is None
