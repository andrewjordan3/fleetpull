"""Tests for fleetpull.models.motive.driving_period.

Every fixture is the committed 2026-07-15 capture set
(``tests/motive_driving_periods_capture.py``): the offset-pagination
page pair, the in-progress record, the terminal empty page, and the
range-cap error envelope. The scrub-preserved fixture properties (id
ordering, the duration arithmetic, the pagination echoes, the
coercion-rule exhibits) are asserted here beside the model they serve.
"""

from datetime import UTC, datetime

from fleetpull.models.motive import DrivingPeriod
from fleetpull.vocabulary import JsonValue
from tests.motive_driving_periods_capture import (
    DRIVING_PERIOD_IN_PROGRESS_RECORD,
    DRIVING_PERIOD_RECORDS,
    DRIVING_PERIODS_EMPTY_PAGE_RESPONSE,
    DRIVING_PERIODS_PAGE_1_RESPONSE,
    DRIVING_PERIODS_PAGE_2_RESPONSE,
    DRIVING_PERIODS_RANGE_ERROR,
)


def _parse_wire_timestamp(value: JsonValue) -> datetime:
    assert isinstance(value, str)
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


class TestFixtureProperties:
    """The scrub-preserved properties the capture module promises."""

    def test_ids_strictly_descend_within_and_across_the_page_pair(self) -> None:
        record_ids = [
            identifier
            for record in DRIVING_PERIOD_RECORDS
            if isinstance(identifier := record['id'], int)
        ]
        assert len(record_ids) == len(DRIVING_PERIOD_RECORDS)
        assert record_ids == sorted(record_ids, reverse=True)

    def test_duration_equals_end_minus_start_on_every_complete_record(
        self,
    ) -> None:
        for record in DRIVING_PERIOD_RECORDS:
            span = _parse_wire_timestamp(record['end_time']) - _parse_wire_timestamp(
                record['start_time']
            )
            assert span.total_seconds() == record['duration']

    def test_pagination_echoes_verbatim(self) -> None:
        assert DRIVING_PERIODS_PAGE_1_RESPONSE['pagination'] == {
            'per_page': 5,
            'page_no': 1,
            'total': 10366,
        }
        assert DRIVING_PERIODS_PAGE_2_RESPONSE['pagination'] == {
            'per_page': 5,
            'page_no': 2,
            'total': 10366,
        }

    def test_empty_page_sits_past_the_decoders_computed_boundary(self) -> None:
        echo = DRIVING_PERIODS_EMPTY_PAGE_RESPONSE['pagination']
        assert isinstance(echo, dict)
        assert DRIVING_PERIODS_EMPTY_PAGE_RESPONSE['driving_periods'] == []
        page_no, per_page, total = echo['page_no'], echo['per_page'], echo['total']
        assert isinstance(page_no, int)
        assert isinstance(per_page, int)
        assert isinstance(total, int)
        assert page_no * per_page >= total

    def test_range_error_is_the_flat_error_message_envelope(self) -> None:
        assert DRIVING_PERIODS_RANGE_ERROR == {
            'error_message': 'Date range cannot be greater than 30 days'
        }


class TestDrivingPeriodValidation:
    def test_every_complete_record_validates(self) -> None:
        validated = [
            DrivingPeriod.model_validate(record) for record in DRIVING_PERIOD_RECORDS
        ]
        assert len(validated) == 10
        assert all(period.status == 'complete' for period in validated)

    def test_timestamps_are_timezone_aware_utc(self) -> None:
        period = DrivingPeriod.model_validate(DRIVING_PERIOD_RECORDS[0])
        assert period.start_time.tzinfo is not None
        assert period.start_time.utcoffset() is not None
        assert period.start_time == datetime(2026, 7, 14, 23, 59, 55, tzinfo=UTC)

    def test_both_driver_wire_variants_are_modeled(self) -> None:
        validated = [
            DrivingPeriod.model_validate(record) for record in DRIVING_PERIOD_RECORDS
        ]
        unattributed = [period for period in validated if period.driver is None]
        attributed = [period for period in validated if period.driver is not None]
        assert unattributed
        assert attributed
        assert attributed[0].driver is not None
        assert attributed[0].driver.first_name == 'Synthetic'

    def test_distance_stays_a_verbatim_formatted_string(self) -> None:
        period = DrivingPeriod.model_validate(DRIVING_PERIOD_RECORDS[1])
        assert period.distance == '42.2 mi'

    def test_the_coercion_exhibit_vehicle(self) -> None:
        # The captured year-"0" / empty-make/model vehicle: the quoted
        # sentinel mirrors as 0; the empty strings mirror verbatim (the
        # DataFrame boundary nulls them, never the model).
        period = DrivingPeriod.model_validate(DRIVING_PERIOD_RECORDS[3])
        assert period.vehicle.year == 0
        assert period.vehicle.make == ''
        assert period.vehicle.model == ''

    def test_the_annotated_record_carries_its_note(self) -> None:
        period = DrivingPeriod.model_validate(DRIVING_PERIOD_RECORDS[4])
        assert period.annotation_status == 1
        assert period.notes == 'synthetic note 001'

    def test_null_start_kilometers_validates(self) -> None:
        # Production observed (a late-2025 backfill window) a completed span
        # whose start-side odometer was absent -- start_kilometers null on an
        # otherwise-complete record. The 2026-07-15 capture never exhibited it,
        # so the case is synthesized from a captured complete record; the field
        # is nullable because the provider can omit either odometer end.
        record = {**DRIVING_PERIOD_RECORDS[0], 'start_kilometers': None}
        period = DrivingPeriod.model_validate(record)
        assert period.start_kilometers is None
        assert period.status == 'complete'

    def test_the_in_progress_end_side_shape(self) -> None:
        period = DrivingPeriod.model_validate(DRIVING_PERIOD_IN_PROGRESS_RECORD)
        assert period.status == 'in_progress'
        assert period.end_time is None
        assert period.end_kilometers is None
        assert period.distance is None
        assert period.destination_lat is None
        assert period.destination_lon is None
        # The wire carries an empty string, mirrored verbatim; the
        # DataFrame boundary is where it becomes null.
        assert period.destination == ''
        # The routing anchor is present even mid-span.
        assert period.start_time == datetime(2026, 7, 15, 19, 9, 51, tzinfo=UTC)

    def test_in_progress_duration_is_the_fractional_running_counter(self) -> None:
        period = DrivingPeriod.model_validate(DRIVING_PERIOD_IN_PROGRESS_RECORD)
        assert period.duration == 112.896090996


class TestExcludedFields:
    def test_null_only_wire_fields_are_never_modeled(self) -> None:
        # source and the four *_hvb_* fields arrive null-only (a diesel
        # fleet's capture) -- no honest dtype exists, so they are
        # deliberately absent from the model until a capture types them.
        assert 'source' in DRIVING_PERIOD_RECORDS[0]
        assert 'start_hvb_state_of_charge' in DRIVING_PERIOD_RECORDS[0]
        for excluded in (
            'source',
            'start_hvb_state_of_charge',
            'end_hvb_state_of_charge',
            'start_hvb_lifetime_energy_output',
            'end_hvb_lifetime_energy_output',
        ):
            assert excluded not in DrivingPeriod.model_fields
