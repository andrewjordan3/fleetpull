"""Tests for fleetpull.records.fan_out_keys."""

import polars as pl
import pytest

from fleetpull.records.fan_out_keys import extract_fan_out_keys


class TestExtractFanOutKeys:
    def test_distinct_values_stringified_in_first_seen_order(self) -> None:
        frame = pl.DataFrame({'vehicle_id': [543180, 11, 543180, 22]})
        assert extract_fan_out_keys(frame, 'vehicle_id') == ['543180', '11', '22']

    def test_drops_nulls(self) -> None:
        frame = pl.DataFrame(
            {'vehicle_id': [1, None, 2]}, schema={'vehicle_id': pl.Int64}
        )
        assert extract_fan_out_keys(frame, 'vehicle_id') == ['1', '2']

    def test_empty_frame_yields_empty_list(self) -> None:
        frame = pl.DataFrame(schema={'vehicle_id': pl.Int64})
        assert extract_fan_out_keys(frame, 'vehicle_id') == []

    def test_already_string_column_passes_through(self) -> None:
        frame = pl.DataFrame({'vehicle_id': ['a', 'b', 'a']})
        assert extract_fan_out_keys(frame, 'vehicle_id') == ['a', 'b']

    def test_missing_column_raises(self) -> None:
        frame = pl.DataFrame({'vehicle_id': [1]})
        with pytest.raises(pl.exceptions.ColumnNotFoundError):
            extract_fan_out_keys(frame, 'absent')
