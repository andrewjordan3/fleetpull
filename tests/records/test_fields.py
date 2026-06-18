"""Tests for fleetpull.records.fields."""

from datetime import datetime
from enum import StrEnum
from typing import Any

import pytest

from fleetpull.model_contract import ResponseModel
from fleetpull.records.fields import (
    FieldKind,
    classify_annotation,
    iter_flat_fields,
)


class _Color(StrEnum):
    RED = 'red'
    BLUE = 'blue'


class _Leaf(ResponseModel):
    leaf_id: int
    label: str | None = None


class _Block(ResponseModel):
    flag: bool
    leaf: _Leaf | None = None


class _Sample(ResponseModel):
    sample_id: int
    color: _Color
    tags: list[str]
    when: datetime
    block: _Block | None = None


def _classify(annotation: Any) -> tuple[FieldKind, Any]:
    return classify_annotation(
        annotation=annotation, owning_model_name='M', field_name='f'
    )


class TestClassifyAnnotation:
    def test_scalar(self) -> None:
        assert _classify(int) == (FieldKind.SCALAR, int)

    def test_optional_scalar_unwraps(self) -> None:
        assert _classify(str | None) == (FieldKind.SCALAR, str)

    def test_enum(self) -> None:
        assert _classify(_Color) == (FieldKind.ENUM, _Color)

    def test_list_of_scalar(self) -> None:
        assert _classify(list[str]) == (FieldKind.LIST_OF_SCALAR, str)

    def test_nested_model(self) -> None:
        assert _classify(_Leaf) == (FieldKind.NESTED_MODEL, _Leaf)

    def test_multi_arm_union_raises(self) -> None:
        with pytest.raises(TypeError, match='single non-None'):
            _classify(int | str)

    def test_list_of_model_raises(self) -> None:
        with pytest.raises(TypeError, match='list'):
            _classify(list[_Leaf])

    def test_unsupported_raises(self) -> None:
        with pytest.raises(TypeError, match='unsupported'):
            _classify(Any)


class TestIterFlatFields:
    def test_top_level_fields_keep_bare_names(self) -> None:
        columns = [field.column for field in iter_flat_fields(_Leaf)]
        assert columns == ['leaf_id', 'label']

    def test_nested_columns_double_underscore_joined(self) -> None:
        columns = {field.column for field in iter_flat_fields(_Sample)}
        assert 'block__flag' in columns
        assert 'block__leaf__leaf_id' in columns
        assert 'block__leaf__label' in columns

    def test_paths_track_attribute_access(self) -> None:
        by_column = {f.column: f for f in iter_flat_fields(_Sample)}
        assert by_column['block__leaf__leaf_id'].path == (
            'block',
            'leaf',
            'leaf_id',
        )

    def test_declaration_order_preserved(self) -> None:
        columns = [field.column for field in iter_flat_fields(_Sample)]
        assert columns[0] == 'sample_id'
        assert columns.index('color') < columns.index('tags')
