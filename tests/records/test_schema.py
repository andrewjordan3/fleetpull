"""Tests for fleetpull.records.schema."""

from datetime import datetime
from enum import StrEnum
from typing import Any

import polars as pl
import pytest

from fleetpull.model_contract import ResponseModel
from fleetpull.records.schema import derive_schema


class _Color(StrEnum):
    RED = 'red'


class _Block(ResponseModel):
    flag: bool


class _Sample(ResponseModel):
    sample_id: int
    ratio: float
    name: str | None = None
    color: _Color
    tags: list[int]
    when: datetime
    block: _Block | None = None


class _Bad(ResponseModel):
    blob: Any  # typing-justified: the unmappable annotation this fixture exists for


def test_maps_each_scalar_kind() -> None:
    schema = derive_schema(_Sample)
    assert schema['sample_id'] == pl.Int64()
    assert schema['ratio'] == pl.Float64()
    assert schema['name'] == pl.String()
    assert schema['when'] == pl.Datetime(time_unit='us', time_zone='UTC')


def test_enum_maps_to_string() -> None:
    assert derive_schema(_Sample)['color'] == pl.String()


def test_list_of_scalar_maps_to_list() -> None:
    assert derive_schema(_Sample)['tags'] == pl.List(pl.Int64())


def test_nested_model_flattens_into_schema() -> None:
    assert 'block__flag' in derive_schema(_Sample)


def test_unmappable_field_raises() -> None:
    with pytest.raises(TypeError):
        derive_schema(_Bad)
