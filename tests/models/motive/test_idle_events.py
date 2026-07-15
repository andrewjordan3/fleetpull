"""Tests for fleetpull.models.motive.idle_events.

Every fixture is the committed 2026-07-15 capture set
(``tests/motive_idle_events_capture.py``): the full page and the
``rg_match: false`` single-record page. The scrub-preserved fixture
properties (id and end-time ordering, the equality-class pair, the
location formats) are asserted here beside the model they serve.
"""

from datetime import UTC, datetime

from fleetpull.models.motive import IdleEvent
from fleetpull.models.motive.shared import EldDeviceInfo
from tests.motive_idle_events_capture import (
    IDLE_EVENT_RECORDS,
    IDLE_EVENTS_PAGE_1_RESPONSE,
    IDLE_EVENTS_SINGLE_PAGE_RESPONSE,
)

_PAGE_1_RECORD_COUNT = 5


class TestFixtureProperties:
    """The scrub-preserved properties the capture module promises."""

    def test_page_ids_strictly_ascend(self) -> None:
        page_ids = [
            identifier
            for record in IDLE_EVENT_RECORDS[:_PAGE_1_RECORD_COUNT]
            if isinstance(identifier := record['id'], int)
        ]
        assert len(page_ids) == _PAGE_1_RECORD_COUNT
        assert page_ids == sorted(page_ids)

    def test_page_end_times_are_non_decreasing(self) -> None:
        # The endpoint sorts ascending by end time -- the opposite of
        # its driving_periods sibling.
        end_times = [
            end_time
            for record in IDLE_EVENT_RECORDS[:_PAGE_1_RECORD_COUNT]
            if isinstance(end_time := record['end_time'], str)
        ]
        assert len(end_times) == _PAGE_1_RECORD_COUNT
        assert end_times == sorted(end_times)

    def test_the_equality_class_pair_shares_every_reference(self) -> None:
        # Records one and two are one driver idling one vehicle twice:
        # the scrub maps one raw identity to one image everywhere.
        first, second = IDLE_EVENT_RECORDS[0], IDLE_EVENT_RECORDS[1]
        assert first['driver'] == second['driver']
        assert first['vehicle'] == second['vehicle']
        assert first['eld_device'] == second['eld_device']

    def test_pagination_echoes_verbatim(self) -> None:
        assert IDLE_EVENTS_PAGE_1_RESPONSE['pagination'] == {
            'per_page': 5,
            'page_no': 1,
            'total': 12869,
        }
        assert IDLE_EVENTS_SINGLE_PAGE_RESPONSE['pagination'] == {
            'per_page': 1,
            'page_no': 1,
            'total': 154866,
        }


class TestIdleEventValidation:
    def test_every_record_validates_with_both_timestamps(self) -> None:
        validated = [IdleEvent.model_validate(record) for record in IDLE_EVENT_RECORDS]
        assert len(validated) == 6
        for event in validated:
            assert event.start_time.tzinfo is not None
            assert event.end_time.tzinfo is not None

    def test_first_record_pins_the_wire_values(self) -> None:
        event = IdleEvent.model_validate(IDLE_EVENT_RECORDS[0])
        assert event.start_time == datetime(2026, 7, 13, 4, 28, 49, tzinfo=UTC)
        assert event.end_time == datetime(2026, 7, 13, 5, 58, 56, tzinfo=UTC)
        assert event.veh_fuel_start == 365757.53125
        assert event.end_type == 'engine_stop'
        assert isinstance(event.eld_device, EldDeviceInfo)
        assert event.eld_device.identifier == 'AABL36SYN00003'

    def test_both_driver_wire_variants_are_modeled(self) -> None:
        validated = [IdleEvent.model_validate(record) for record in IDLE_EVENT_RECORDS]
        unattributed = [event for event in validated if event.driver is None]
        attributed = [event for event in validated if event.driver is not None]
        assert unattributed
        assert attributed

    def test_both_end_type_values_appear(self) -> None:
        end_types = {
            IdleEvent.model_validate(record).end_type for record in IDLE_EVENT_RECORDS
        }
        assert end_types == {'engine_stop', 'vehicle_moving'}

    def test_the_rg_match_false_location_format(self) -> None:
        # When the reverse geocoder misses, location is the
        # distance-direction format, mirrored verbatim.
        event = IdleEvent.model_validate(IDLE_EVENT_RECORDS[5])
        assert event.rg_match is False
        assert event.location.startswith('2.6 mi NW of Synthetic City ')
