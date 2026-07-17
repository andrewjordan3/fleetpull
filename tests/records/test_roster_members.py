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

    def test_null_and_empty_members_are_filtered_loudly(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The garbage-member edge: null and empty-string ids are unfetchable by
        # construction, so they are excluded with a warning rather than
        # raised or passed through to become unbuildable URLs.
        frame = pl.DataFrame(
            {'vehicle_id': ['543180', '', None, '11', '']},
            schema={'vehicle_id': pl.String},
        )
        with caplog.at_level('WARNING'):
            members = extract_roster_members(frame, 'vehicle_id')
        assert members == {'543180', '11'}
        [record] = caplog.records
        assert 'vehicle_id' in record.getMessage()
        assert '1 null' in record.getMessage()
        assert '2 empty-string' in record.getMessage()

    def test_clean_listing_emits_no_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        frame = pl.DataFrame({'vehicle_id': [1, 2]})
        with caplog.at_level('WARNING'):
            members = extract_roster_members(frame, 'vehicle_id')
        assert members == {'1', '2'}
        assert caplog.records == []

    def test_missing_column_raises(self) -> None:
        frame = pl.DataFrame({'vehicle_id': [1]})
        with pytest.raises(ValueError, match='not in the frame'):
            extract_roster_members(frame, 'absent')
