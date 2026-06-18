"""Tests for fleetpull.records.dataframe."""

import polars as pl

from fleetpull.records.dataframe import build_dataframe, normalize_empty_strings


def _schema() -> dict[str, pl.DataType]:
    return {'a': pl.Int64(), 'b': pl.String()}


def test_builds_with_schema_in_order() -> None:
    rows = [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}]
    frame = build_dataframe(rows=rows, schema=_schema())
    assert frame.columns == ['a', 'b']
    assert frame.schema['a'] == pl.Int64()
    assert frame.height == 2


def test_empty_rows_yield_typed_empty_frame() -> None:
    frame = build_dataframe(rows=[], schema=_schema())
    assert frame.columns == ['a', 'b']
    assert frame.schema['b'] == pl.String()
    assert frame.height == 0


def test_normalizes_empty_strings_to_null() -> None:
    frame = build_dataframe(
        rows=[{'a': 1, 'b': ''}, {'a': 2, 'b': 'y'}], schema=_schema()
    )
    normalized = normalize_empty_strings(frame)
    assert normalized['b'].to_list() == [None, 'y']


def test_leaves_non_string_columns_untouched() -> None:
    frame = build_dataframe(rows=[{'a': 0, 'b': 'k'}], schema=_schema())
    normalized = normalize_empty_strings(frame)
    assert normalized['a'].to_list() == [0]
