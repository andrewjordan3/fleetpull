"""Tests for fleetpull.models.geotab.exception_event.

Every fixture is the committed capture set
(``tests/geotab_exception_events_capture.py``): the three idling-rule
records and the two sort-discrimination error envelopes. The
scrub-preserved properties (the duration arithmetic including the
fractional-second span, the version ordering, the sentinel vocabulary)
are asserted here beside the model they serve.
"""

from datetime import UTC, datetime

import polars as pl

from fleetpull.models.geotab import (
    ExceptionEvent,
    ExceptionEventDriverRef,
    ExceptionEventRuleRef,
)
from fleetpull.records import models_to_dataframe
from tests.geotab_exception_events_capture import (
    EXCEPTION_EVENT_RECORDS,
    EXCEPTION_EVENTS_ARGUMENT_ERROR,
    EXCEPTION_EVENTS_EMPTY_RESPONSE,
    EXCEPTION_EVENTS_GENERIC_ERROR,
)


class TestFixtureProperties:
    """The scrub-preserved properties the capture module promises."""

    def test_duration_reproduces_the_interval_on_every_record(self) -> None:
        for record in EXCEPTION_EVENT_RECORDS:
            event = ExceptionEvent.model_validate(record)
            assert event.active_from is not None
            assert event.active_to is not None
            assert event.active_to - event.active_from == event.duration, event.id

    def test_the_fractional_second_span_survives_exactly(self) -> None:
        # Record three: a fractional activeFrom (.750Z) reproducing the
        # seven-digit fractional TimeSpan (00:13:05.2500000) -- the
        # precision exhibit the scrub keeps verbatim.
        event = ExceptionEvent.model_validate(EXCEPTION_EVENT_RECORDS[2])
        assert event.active_from == datetime(2026, 7, 6, 19, 19, 12, 750000, tzinfo=UTC)
        assert event.duration is not None
        assert event.duration.total_seconds() == 785.25

    def test_versions_ascend_in_capture_order(self) -> None:
        versions = [
            version
            for record in EXCEPTION_EVENT_RECORDS
            if isinstance(version := record['version'], str)
        ]
        assert len(versions) == len(EXCEPTION_EVENT_RECORDS)
        assert versions == sorted(versions)

    def test_the_error_envelopes_carry_their_types(self) -> None:
        generic = EXCEPTION_EVENTS_GENERIC_ERROR['error']
        assert isinstance(generic, dict)
        generic_data = generic['data']
        assert isinstance(generic_data, dict)
        assert generic_data['type'] == 'GenericException'
        argument = EXCEPTION_EVENTS_ARGUMENT_ERROR['error']
        assert isinstance(argument, dict)
        argument_data = argument['data']
        assert isinstance(argument_data, dict)
        assert argument_data['type'] == 'ArgumentException'

    def test_the_silent_empty_shape(self) -> None:
        assert EXCEPTION_EVENTS_EMPTY_RESPONSE == {
            'result': [],
            'jsonrpc': '2.0',
        }


class TestExceptionEventValidation:
    def test_every_record_validates_and_populates_every_field(self) -> None:
        # The alias-trap closure (the Trip pattern): every captured record
        # carries every modeled field, so a typo'd alias on ANY field
        # would land it as None under extra='ignore' and fail here.
        for record in EXCEPTION_EVENT_RECORDS:
            event = ExceptionEvent.model_validate(record)
            for field_name in ExceptionEvent.model_fields:
                assert getattr(event, field_name) is not None, field_name

    def test_the_bare_driver_sentinel_lands_as_the_reference_id(self) -> None:
        event = ExceptionEvent.model_validate(EXCEPTION_EVENT_RECORDS[0])
        assert isinstance(event.driver, ExceptionEventDriverRef)
        assert event.driver.id == 'UnknownDriverId'
        assert event.driver.is_driver is None

    def test_the_bare_diagnostic_sentinel_lands_as_the_reference_id(self) -> None:
        event = ExceptionEvent.model_validate(EXCEPTION_EVENT_RECORDS[0])
        assert event.diagnostic is not None
        assert event.diagnostic.id == 'NoDiagnosticId'

    def test_the_rule_reference_carries_state_reason_and_id(self) -> None:
        event = ExceptionEvent.model_validate(EXCEPTION_EVENT_RECORDS[0])
        assert isinstance(event.rule, ExceptionEventRuleRef)
        assert event.rule.id == 'RuleIdlingId'
        assert event.rule.state == 'ExceptionRuleStateActiveId'
        assert event.rule.reason == 'ExceptionRuleReasonNoneId'

    def test_timestamps_are_timezone_aware_utc(self) -> None:
        event = ExceptionEvent.model_validate(EXCEPTION_EVENT_RECORDS[0])
        assert event.active_from is not None
        assert event.active_from.utcoffset() is not None
        assert event.active_from == datetime(2026, 7, 6, 13, 24, 2, tzinfo=UTC)


class TestExceptionEventFrame:
    def test_all_records_build_one_typed_frame(self) -> None:
        frame = models_to_dataframe(
            [
                ExceptionEvent.model_validate(record)
                for record in EXCEPTION_EVENT_RECORDS
            ],
            ExceptionEvent,
        )
        assert frame.height == len(EXCEPTION_EVENT_RECORDS)
        assert frame.schema['duration'] == pl.Duration(time_unit='us')
        assert frame.schema['active_from'] == pl.Datetime(
            time_unit='us', time_zone='UTC'
        )
        assert frame.schema['driver__id'] == pl.String
        assert frame.schema['diagnostic__id'] == pl.String
        assert frame.schema['rule__id'] == pl.String
        assert frame['driver__id'].to_list() == ['UnknownDriverId'] * 3
        assert frame['rule__id'].to_list() == ['RuleIdlingId'] * 3

    def test_empty_input_carries_the_full_schema(self) -> None:
        populated = models_to_dataframe(
            [
                ExceptionEvent.model_validate(record)
                for record in EXCEPTION_EVENT_RECORDS
            ],
            ExceptionEvent,
        )
        empty = models_to_dataframe([], ExceptionEvent)
        assert empty.height == 0
        assert empty.schema == populated.schema
