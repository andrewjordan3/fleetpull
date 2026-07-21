"""Tests for fleetpull.models.geotab.shared.

The TimeSpan cases are the captured 2026-07-13 wire strings; the
malformed cases are constructed negatives.
"""

import re
from datetime import timedelta

import polars as pl
import pytest
from pydantic import ValidationError

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import (
    GeotabAddressedLocation,
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
        assert bare_id_to_reference('b4B82') == {'id': 'b4B82'}

    def test_object_form_passes_through_untouched(self) -> None:
        reference: JsonValue = {'id': 'b4B82', 'isDriver': True}
        assert bare_id_to_reference(reference) is reference

    def test_none_passes_through(self) -> None:
        assert bare_id_to_reference(None) is None


class TestGeotabAddressedLocation:
    """The nested-location wrapper's two arms (DESIGN §8: the coordinate
    arm the 200-sample census showed and the address arm the 24,860-block
    live proof found)."""

    def test_the_coordinate_arm_validates_with_a_null_address(self) -> None:
        wrapper = GeotabAddressedLocation.model_validate(
            {'location': {'x': -140.25, 'y': 35.5}}
        )
        assert wrapper.location is not None
        assert wrapper.location.x == -140.25
        assert wrapper.location.y == 35.5
        assert wrapper.address is None

    def test_the_coordinate_int_arms_lift_to_float(self) -> None:
        wrapper = GeotabAddressedLocation.model_validate(
            {'location': {'x': -140, 'y': 35}}
        )
        assert wrapper.location is not None
        assert isinstance(wrapper.location.x, float)
        assert isinstance(wrapper.location.y, float)

    def test_the_address_arm_validates_with_a_null_coordinate(self) -> None:
        wrapper = GeotabAddressedLocation.model_validate(
            {'address': {'formattedAddress': '100 Example Rd, Testton, TS, USA'}}
        )
        assert wrapper.location is None
        assert wrapper.address is not None
        assert wrapper.address.formatted_address == '100 Example Rd, Testton, TS, USA'

    def test_neither_arm_is_representable(self) -> None:
        # A wrapper with neither arm is unobserved but not a shape error;
        # both fields are optional.
        wrapper = GeotabAddressedLocation.model_validate({})
        assert wrapper.location is None
        assert wrapper.address is None

    def test_a_present_coordinate_block_missing_a_coordinate_fails(self) -> None:
        # Required-within-the-block: a present coordinate block without
        # its coordinates is a shape change, not a null coordinate.
        with pytest.raises(ValidationError, match='y'):
            GeotabAddressedLocation.model_validate({'location': {'x': -140.25}})

    def test_a_present_address_block_missing_its_key_fails(self) -> None:
        with pytest.raises(
            ValidationError, match=r'formattedAddress|formatted_address'
        ):
            GeotabAddressedLocation.model_validate({'address': {}})

    def test_unmodeled_address_keys_are_absorbed(self) -> None:
        # extra='ignore' absorbs the GeoTab address keys no walk has
        # observed yet (the documented-until-probed posture).
        wrapper = GeotabAddressedLocation.model_validate(
            {
                'address': {
                    'formattedAddress': '100 Example Rd, Testton, TS, USA',
                    'city': 'Testton',
                    'state': 'TS',
                }
            }
        )
        assert wrapper.address is not None
        assert wrapper.address.formatted_address == '100 Example Rd, Testton, TS, USA'
