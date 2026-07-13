"""Tests for fleetpull.records.convert."""

from datetime import timedelta
from enum import StrEnum

import polars as pl

from fleetpull.model_contract import ResponseModel
from fleetpull.models.motive import Vehicle
from fleetpull.records.convert import models_to_dataframe


class _Color(StrEnum):
    RED = 'red'


class _Leaf(ResponseModel):
    leaf_id: int
    label: str | None = None


class _Sample(ResponseModel):
    sample_id: int
    color: _Color
    note: str | None = None
    leaf: _Leaf | None = None


def test_end_to_end_shapes_rows_and_schema() -> None:
    records = [
        _Sample(sample_id=1, color=_Color.RED, note='', leaf=_Leaf(leaf_id=9)),
        _Sample(sample_id=2, color=_Color.RED, note='hi', leaf=None),
    ]
    frame = models_to_dataframe(records, _Sample)
    assert frame.columns == [
        'sample_id',
        'color',
        'note',
        'leaf__leaf_id',
        'leaf__label',
    ]
    assert frame['note'].to_list() == [None, 'hi']
    assert frame['leaf__leaf_id'].to_list() == [9, None]


def test_empty_records_yield_full_schema() -> None:
    frame = models_to_dataframe([], _Sample)
    assert frame.height == 0
    assert 'leaf__leaf_id' in frame.columns


def test_vehicle_schema_derives_without_override() -> None:
    # The real Vehicle must auto-derive end to end. If this raises, a
    # Vehicle field is unmappable -- STOP and report it; that is the
    # signal the deferred schema-override path is now needed, not a cue to
    # widen the type or drop the field.
    frame = models_to_dataframe([], Vehicle)
    assert frame.height == 0
    assert len(frame.columns) > 0


class _Timed(ResponseModel):
    duration: timedelta | None = None


def test_timedelta_flattens_with_value_and_null() -> None:
    frame = models_to_dataframe(
        [_Timed(duration=timedelta(minutes=5, seconds=1)), _Timed(duration=None)],
        _Timed,
    )
    assert frame.schema['duration'] == pl.Duration(time_unit='us')
    assert frame['duration'].to_list() == [timedelta(minutes=5, seconds=1), None]


def test_empty_input_carries_the_duration_dtype() -> None:
    empty = models_to_dataframe([], _Timed)
    assert empty.height == 0
    assert empty.schema['duration'] == pl.Duration(time_unit='us')
