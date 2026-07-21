"""Tests for fleetpull.models.samsara.idling_event.

Every fixture is the committed 2026-07-20 capture set
(``tests/samsara_idling_events_capture.py``): three fully synthetic
records shaped by the 2,200-event census -- the maximal variant
(operator, address with ``addressTypes: ["yard"]``, air temperature),
the minimal variant (the always-present key set only), and the
mixed-float variant (``fuelConsumedMilliliters`` as a wire float; the
others carry the int shape). The census-preserved wire shapes (the
no-end-key interval, the bare-int ``asset.id``/``operator.id`` beside
the STRING ``address.id``, the string-money blocks, the int|float
fuel mixing) are asserted here beside the model that mirrors them; the
RFC3339 recovery to tz-aware UTC datetimes is pinned to exact values.
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import (
    AssetRef,
    FuelCost,
    IdlingAddress,
    IdlingEvent,
    OperatorRef,
)
from tests.samsara_idling_events_capture import (
    IDLING_EVENT_RECORDS,
    IDLING_EVENTS_LIMIT_ERROR_RESPONSE,
    IDLING_EVENTS_PAGE_RESPONSE,
    IDLING_EVENTS_RANGE_CAP_ERROR_RESPONSE,
    IDLING_EVENTS_TERMINAL_RESPONSE,
)

_ALWAYS_PRESENT_KEYS = frozenset(
    {
        'eventUuid',
        'startTime',
        'durationMilliseconds',
        'asset',
        'latitude',
        'longitude',
        'ptoState',
        'fuelConsumedMilliliters',
        'fuelCost',
        'gaseousFuelConsumedGrams',
        'gaseousFuelCost',
    }
)


class TestFixtureProperties:
    """The census-preserved properties the capture module promises."""

    def test_the_modern_envelope_shape(self) -> None:
        # data + pagination {endCursor, hasNextPage} -- the
        # vehicles/drivers envelope on a windowed surface.
        for envelope in (
            IDLING_EVENTS_PAGE_RESPONSE,
            IDLING_EVENTS_TERMINAL_RESPONSE,
        ):
            assert set(envelope) == {'data', 'pagination'}
        pagination = IDLING_EVENTS_TERMINAL_RESPONSE['pagination']
        assert pagination == {'endCursor': '', 'hasNextPage': False}

    def test_the_variant_split(self) -> None:
        maximal, minimal, mixed_float = IDLING_EVENT_RECORDS
        assert set(minimal) == _ALWAYS_PRESENT_KEYS
        assert set(maximal) == _ALWAYS_PRESENT_KEYS | {
            'operator',
            'address',
            'airTemperatureMillicelsius',
        }
        assert set(mixed_float) == _ALWAYS_PRESENT_KEYS | {
            'operator',
            'airTemperatureMillicelsius',
        }

    def test_no_end_key_exists(self) -> None:
        # The interval is start plus durationMilliseconds -- no record
        # carries any end-shaped key.
        for record in IDLING_EVENT_RECORDS:
            assert 'endTime' not in record
            assert 'endMs' not in record

    def test_the_fuel_field_mixes_int_and_float(self) -> None:
        maximal, minimal, mixed_float = IDLING_EVENT_RECORDS
        for int_shaped in (maximal, minimal):
            value = int_shaped['fuelConsumedMilliliters']
            assert isinstance(value, int)
            assert not isinstance(value, bool)
        assert isinstance(mixed_float['fuelConsumedMilliliters'], float)

    def test_the_id_type_split(self) -> None:
        # address.id is a STRING while asset.id/operator.id are BARE
        # INTs -- one record, two id postures, mirrored exactly.
        maximal = IDLING_EVENT_RECORDS[0]
        asset_block = maximal['asset']
        operator_block = maximal['operator']
        address_block = maximal['address']
        assert isinstance(asset_block, dict)
        assert isinstance(operator_block, dict)
        assert isinstance(address_block, dict)
        assert isinstance(asset_block['id'], int)
        assert isinstance(operator_block['id'], int)
        assert isinstance(address_block['id'], str)

    def test_the_400_bodies_are_loud_json(self) -> None:
        # JSON {"message", "requestId"} -- NOT the text/plain rpc-error
        # posture of the legacy v1 trips surface.
        limit_message = IDLING_EVENTS_LIMIT_ERROR_RESPONSE['message']
        range_message = IDLING_EVENTS_RANGE_CAP_ERROR_RESPONSE['message']
        assert limit_message == (
            'limit must be lesser or equal than 200 but got value 512'
        )
        assert range_message == 'Total duration must be less than 3 months.'


class TestIdlingEventValidation:
    def test_rfc3339_recovers_exact_tz_aware_utc_datetimes(self) -> None:
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[0])
        # Equality against the exact UTC instant pins both the parse
        # and the zero offset; awareness is asserted separately.
        assert event.start_time == datetime(2026, 1, 1, 12, 34, 56, tzinfo=UTC)
        assert event.start_time.tzinfo is not None

    def test_the_duration_stays_a_verbatim_int(self) -> None:
        # A unit-suffixed mirror, never a timedelta recovery.
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[0])
        assert event.duration_milliseconds == 930000
        assert isinstance(event.duration_milliseconds, int)

    def test_the_asset_reference_carries_the_bare_int_id(self) -> None:
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[0])
        assert isinstance(event.asset, AssetRef)
        assert event.asset.id == 90000001
        assert isinstance(event.asset.id, int)

    def test_the_coordinates(self) -> None:
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[0])
        assert event.latitude == pytest.approx(40.2001)
        assert event.longitude == pytest.approx(-100.2001)

    def test_pto_state_is_a_plain_string(self) -> None:
        # Only 'inactive' was observed in 2,200 records, but the value
        # set is not evidence-closed -- a plain str, not an enum, so an
        # unobserved value lands as data, not a crash.
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[0])
        assert event.pto_state == 'inactive'
        assert type(event.pto_state) is str
        drifted = {**IDLING_EVENT_RECORDS[1], 'ptoState': 'active'}
        assert IdlingEvent.model_validate(drifted).pto_state == 'active'

    def test_int_shaped_fuel_lifts_to_float(self) -> None:
        # The wire mixes int and float; the float field type absorbs
        # both shapes.
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[0])
        assert event.fuel_consumed_milliliters == pytest.approx(2500.0)
        assert isinstance(event.fuel_consumed_milliliters, float)

    def test_float_shaped_fuel_survives_exactly(self) -> None:
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[2])
        assert event.fuel_consumed_milliliters == pytest.approx(123.5)

    def test_the_money_blocks_mirror_as_strings(self) -> None:
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[0])
        assert isinstance(event.fuel_cost, FuelCost)
        assert event.fuel_cost.amount == '1.87'
        assert event.fuel_cost.currency == 'usd'
        assert isinstance(event.gaseous_fuel_cost, FuelCost)
        assert event.gaseous_fuel_cost.amount == '0.00'
        assert event.gaseous_fuel_consumed_grams == 0

    def test_the_maximal_record_carries_the_partial_blocks(self) -> None:
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[0])
        assert isinstance(event.operator, OperatorRef)
        assert event.operator.id == 91000001
        assert isinstance(event.operator.id, int)
        assert event.air_temperature_millicelsius == 23500
        address = event.address
        assert isinstance(address, IdlingAddress)
        # The address id is a STRING, unlike the bare-int asset and
        # operator ids beside it.
        assert address.id == '88000001'
        assert isinstance(address.id, str)
        assert address.address_types == ['yard']

    def test_the_minimal_record_lands_the_partial_blocks_null(self) -> None:
        event = IdlingEvent.model_validate(IDLING_EVENT_RECORDS[1])
        assert event.operator is None
        assert event.air_temperature_millicelsius is None
        assert event.address is None

    def test_an_address_block_without_address_types(self) -> None:
        # addressTypes was absent on ~31 of the 552 captured address
        # blocks -- optional within the block.
        record = {**IDLING_EVENT_RECORDS[0], 'address': {'id': '88000002'}}
        event = IdlingEvent.model_validate(record)
        assert isinstance(event.address, IdlingAddress)
        assert event.address.id == '88000002'
        assert event.address.address_types is None

    def test_a_missing_duration_fails_loudly(self) -> None:
        # durationMilliseconds was present on every observed event (the
        # interval has no end key); an event without one is an
        # unobserved shape that must fail validation, not land null.
        truncated = dict(IDLING_EVENT_RECORDS[1])
        del truncated['durationMilliseconds']
        with pytest.raises(ValidationError):
            IdlingEvent.model_validate(truncated)
