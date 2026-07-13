"""Tests for fleetpull.models.geotab.shared.

The TimeSpan cases are the captured 2026-07-13 wire strings; the
malformed cases are constructed negatives.
"""

import re
from datetime import timedelta

import polars as pl
import pytest

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import (
    GeotabTimeSpan,
    bare_id_to_reference,
    parse_timespan,
)
from fleetpull.records import models_to_dataframe
from fleetpull.records.schema import derive_schema
from fleetpull.vocabulary import JsonValue


class TestParseTimespan:
    @pytest.mark.parametrize(
        ('wire', 'expected'),
        [
            ('00:05:01', timedelta(minutes=5, seconds=1)),
            (
                '00:03:42.3630000',
                timedelta(minutes=3, seconds=42, milliseconds=363),
            ),
            ('4.16:41:16', timedelta(days=4, hours=16, minutes=41, seconds=16)),
            ('3.19:36:59', timedelta(days=3, hours=19, minutes=36, seconds=59)),
            ('21:04:17', timedelta(hours=21, minutes=4, seconds=17)),
            ('00:00:00', timedelta(0)),
        ],
    )
    def test_captured_strings_parse_exactly(
        self, wire: str, expected: timedelta
    ) -> None:
        assert parse_timespan(wire) == expected

    def test_seven_tick_digits_truncate_to_microseconds(self) -> None:
        # 100 ns ticks: ten ticks to the microsecond, so a lone seventh
        # digit is sub-microsecond and truncates away.
        assert parse_timespan('00:00:00.0000001') == timedelta(microseconds=0)
        assert parse_timespan('00:00:00.0000019') == timedelta(microseconds=1)
        assert parse_timespan('00:00:00.0000100') == timedelta(microseconds=10)

    def test_short_fractions_are_decimal_seconds(self) -> None:
        assert parse_timespan('00:00:01.5') == timedelta(seconds=1, milliseconds=500)

    @pytest.mark.parametrize(
        'wire',
        [
            '',
            '-00:05:01',  # negative span
            '00:5:01',  # one-digit field
            '24:00:00',  # hours out of range
            '00:60:00',  # minutes out of range
            '00:00:60',  # seconds out of range
            '00:00:00.12345678',  # eight fractional digits
            '1.2.03:04:05',  # double day prefix
            '05:01',  # missing field
            'garbage',
        ],
    )
    def test_malformed_strings_raise_naming_the_string(self, wire: str) -> None:
        # The raised message always carries the offending string's repr.
        with pytest.raises(ValueError, match=re.escape(repr(wire))):
            parse_timespan(wire)


class _Timed(ResponseModel):
    elapsed: GeotabTimeSpan = None


class TestGeotabTimeSpanAlias:
    def test_field_walk_sees_a_bare_timedelta_leaf(self) -> None:
        # The load-bearing arrangement: Annotated[timedelta | None, ...]
        # lifts its metadata into FieldInfo, leaving the walk a plain
        # nullable timedelta to derive the Duration column from.
        assert _Timed.model_fields['elapsed'].annotation == (timedelta | None)
        assert derive_schema(_Timed)['elapsed'] == pl.Duration(time_unit='us')

    def test_wire_string_validates_and_frames(self) -> None:
        frame = models_to_dataframe(
            [_Timed.model_validate({'elapsed': '00:05:01'})], _Timed
        )
        assert frame['elapsed'].to_list() == [timedelta(minutes=5, seconds=1)]

    def test_none_passes_through(self) -> None:
        assert _Timed.model_validate({'elapsed': None}).elapsed is None
        assert _Timed.model_validate({}).elapsed is None

    def test_already_parsed_timedelta_passes_through(self) -> None:
        # The idempotent-revalidation path: Pydantic may hand back a
        # parsed value; validating the model's own dump must succeed.
        first = _Timed.model_validate({'elapsed': '4.16:41:16'})
        again = _Timed.model_validate(dict(first))
        assert again.elapsed == timedelta(days=4, hours=16, minutes=41, seconds=16)

    def test_non_string_non_timedelta_raises(self) -> None:
        with pytest.raises(ValueError, match='int'):
            _Timed.model_validate({'elapsed': 301})


class TestBareIdToReference:
    def test_bare_string_becomes_the_reference_id_verbatim(self) -> None:
        assert bare_id_to_reference('UnknownDriverId') == {'id': 'UnknownDriverId'}
        # Structural, sentinel-agnostic: any bare string lifts.
        assert bare_id_to_reference('b156') == {'id': 'b156'}

    def test_object_form_passes_through_untouched(self) -> None:
        reference: JsonValue = {'id': 'b156', 'isDriver': True}
        assert bare_id_to_reference(reference) is reference

    def test_none_passes_through(self) -> None:
        assert bare_id_to_reference(None) is None
