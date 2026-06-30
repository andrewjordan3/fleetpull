"""Tests for fleetpull.records.roster_members."""

import polars as pl
import pytest

from fleetpull.records.roster_members import extract_roster_members


class TestExtractRosterMembers:
    def test_distinct_values_stringified(self) -> None:
        frame = pl.DataFrame({'vehicle_id': [543180, 11, 543180, 22]})
        assert extract_roster_members(frame, 'vehicle_id') == {'543180', '11', '22'}

    def test_returns_a_set(self) -> None:
        frame = pl.DataFrame({'vehicle_id': [1, 2]})
        assert isinstance(extract_roster_members(frame, 'vehicle_id'), set)

    def test_empty_frame_yields_empty_set(self) -> None:
        frame = pl.DataFrame(schema={'vehicle_id': pl.Int64})
        assert extract_roster_members(frame, 'vehicle_id') == set()

    def test_already_string_column_passes_through(self) -> None:
        frame = pl.DataFrame({'vehicle_id': ['a', 'b', 'a']})
        assert extract_roster_members(frame, 'vehicle_id') == {'a', 'b'}

    def test_null_member_raises(self) -> None:
        frame = pl.DataFrame(
            {'vehicle_id': [1, None, 2]}, schema={'vehicle_id': pl.Int64}
        )
        with pytest.raises(ValueError, match='null'):
            extract_roster_members(frame, 'vehicle_id')

    def test_missing_column_raises(self) -> None:
        frame = pl.DataFrame({'vehicle_id': [1]})
        with pytest.raises(ValueError, match='not in the frame'):
            extract_roster_members(frame, 'absent')
