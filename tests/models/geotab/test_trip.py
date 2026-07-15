"""Tests for fleetpull.models.geotab.trip.

Every fixture is the committed 2026-07-13 capture set
(``tests/geotab_trips_capture.py``): the windowed seek page pair, the
day-prefixed-TimeSpan record, and the zero-distance degenerate record.
The scrub-preserved fixture properties (id/version ordering, the window
bound, the driver variants, the day-record arithmetic) are asserted
here beside the model they serve.
"""

from datetime import UTC, datetime, timedelta

import polars as pl

from fleetpull.models.geotab import Trip, TripDriverRef
from fleetpull.records import models_to_dataframe
from fleetpull.vocabulary import JsonObject
from tests.geotab_trips_capture import (
    TRIP_DAY_FORMAT_RECORD,
    TRIP_FULL_RECORD,
    TRIP_RECORDS,
    TRIP_SEEK_PAGE_1_RESPONSE,
    TRIP_SEEK_PAGE_2_REQUEST,
    TRIP_ZERO_DISTANCE_RECORD,
)

_ALL_RECORDS: list[JsonObject] = [
    *TRIP_RECORDS,
    TRIP_DAY_FORMAT_RECORD,
    TRIP_ZERO_DISTANCE_RECORD,
]

_WINDOW_START = datetime(2026, 7, 6, tzinfo=UTC)
_WINDOW_END = datetime(2026, 7, 13, tzinfo=UTC)


class TestFixtureProperties:
    """The scrub-preserved properties the capture module promises."""

    def test_ids_strictly_ascend_within_and_across_the_page_pair(self) -> None:
        numeric_ids = [int(str(record['id'])[1:], 16) for record in TRIP_RECORDS]
        assert numeric_ids == sorted(numeric_ids)
        assert len(set(numeric_ids)) == len(numeric_ids)

    def test_page_2_offset_is_page_1_last_id(self) -> None:
        page_1_records = TRIP_SEEK_PAGE_1_RESPONSE['result']
        assert isinstance(page_1_records, list)
        last = page_1_records[-1]
        assert isinstance(last, dict)
        page_2_params = TRIP_SEEK_PAGE_2_REQUEST['params']
        assert isinstance(page_2_params, dict)
        page_2_sort = page_2_params['sort']
        assert isinstance(page_2_sort, dict)
        assert page_2_sort['offset'] == last['id'] == 'b12AC4214'

    def test_versions_ascend_in_id_order(self) -> None:
        versions = [record['version'] for record in TRIP_RECORDS]
        assert versions == sorted(versions)  # type: ignore[type-var]

    def test_every_paging_stop_is_inside_the_window(self) -> None:
        # TripSearch matches by STOP time (prediction-confirmed
        # 2026-07-15): stops inside the window are the retrieval
        # guarantee; starts merely happened to fall inside this capture's
        # window and carry no guarantee.
        stops = [Trip.model_validate(record).stop for record in TRIP_RECORDS]
        assert all(
            stop is not None and _WINDOW_START <= stop < _WINDOW_END for stop in stops
        )

    def test_both_driver_variants_appear_in_the_pair(self) -> None:
        wire_shapes = {type(record['driver']).__name__ for record in TRIP_RECORDS}
        assert wire_shapes == {'str', 'dict'}

    def test_b106_is_the_device_on_both_sides_of_the_boundary(self) -> None:
        devices = [Trip.model_validate(record).device for record in TRIP_RECORDS]
        assert devices[2] is not None
        assert devices[3] is not None
        assert devices[2].id == devices[3].id == 'b106'


class TestTripValidation:
    def test_every_committed_record_validates(self) -> None:
        trips = [Trip.model_validate(record) for record in _ALL_RECORDS]
        assert [trip.id for trip in trips] == [
            'b12AC4053',
            'b12AC4055',
            'b12AC4214',
            'b12AC423F',
            'b12AC430C',
            'b12AC4374',
            'b12AC4D3D',
            'b12AC4FF9',
        ]

    def test_full_record_populates_every_field(self) -> None:
        # The alias-trap closure, mechanically (the Device pattern):
        # TRIP_FULL_RECORD carries every modeled field, so a typo'd alias
        # on ANY field would land it as None under extra='ignore' and
        # fail here -- "the shape validates" alone cannot catch it.
        full = Trip.model_validate(TRIP_FULL_RECORD)
        for field_name in Trip.model_fields:
            assert getattr(full, field_name) is not None, field_name
        assert isinstance(full.driver, TripDriverRef)
        assert full.driver.id == 'b156'
        assert full.driver.is_driver is True

    def test_unattributed_record_lands_the_sentinel_as_driver_id(self) -> None:
        unattributed = Trip.model_validate(TRIP_RECORDS[0])
        assert unattributed.driver is not None
        assert unattributed.driver.id == 'UnknownDriverId'
        assert unattributed.driver.is_driver is None

    def test_zero_distance_degenerate_shape(self) -> None:
        zero = Trip.model_validate(TRIP_ZERO_DISTANCE_RECORD)
        assert zero.average_speed is None  # the key is absent, not null
        assert zero.start == zero.stop
        assert zero.driving_duration == timedelta(0)
        assert zero.distance == 0.0

    def test_day_format_record_arithmetic_holds(self) -> None:
        day = Trip.model_validate(TRIP_DAY_FORMAT_RECORD)
        expected = timedelta(days=4, hours=16, minutes=41, seconds=16)
        assert day.stop_duration == expected
        # The captured datetimes reproduce the captured duration exactly.
        assert day.next_trip_start is not None
        assert day.stop is not None
        assert day.next_trip_start - day.stop == expected
        # The work/after-hours split partitions the stop window.
        assert day.work_stop_duration is not None
        assert day.after_hours_stop_duration is not None
        assert day.work_stop_duration + day.after_hours_stop_duration == expected

    def test_interval_semantics_hold_across_every_record(self) -> None:
        # 12-of-12 in the probe session; the committed 8 re-prove it:
        # driving_duration = stop - start on every record.
        for record in _ALL_RECORDS:
            trip = Trip.model_validate(record)
            assert trip.start is not None
            assert trip.stop is not None
            assert trip.stop - trip.start == trip.driving_duration, trip.id


class TestTripFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [Trip.model_validate(record) for record in _ALL_RECORDS], Trip
        )
        assert frame.height == len(_ALL_RECORDS)
        assert frame.schema['driving_duration'] == pl.Duration(time_unit='us')
        assert frame.schema['start'] == pl.Datetime(time_unit='us', time_zone='UTC')
        assert frame.schema['driver__id'] == pl.String
        assert frame.schema['driver__is_driver'] == pl.Boolean
        assert frame.schema['stop_point__x'] == pl.Float64
        # The sentinel flattening: the string lands verbatim, is_driver
        # nulls exactly on sentinel rows.
        driver_ids = frame['driver__id'].to_list()
        assert 'UnknownDriverId' in driver_ids
        for driver_id, is_driver in zip(
            driver_ids, frame['driver__is_driver'].to_list(), strict=True
        ):
            assert (driver_id == 'UnknownDriverId') == (is_driver is None)
        # The absent-key shape: average_speed null on the zero-distance row.
        assert frame['average_speed'].null_count() == 1
        assert frame.filter(pl.col('id') == 'b12AC4FF9')['average_speed'][0] is None

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [Trip.model_validate(record) for record in _ALL_RECORDS], Trip
        )
        empty = models_to_dataframe([], Trip)
        assert empty.height == 0
        assert empty.schema == populated.schema
