"""Tests for fleetpull.models.samsara.trip.

Every fixture is the committed 2026-07-20 capture set
(``tests/samsara_trips_capture.py``): two fully synthetic records
shaped by the 725-trip census -- the maximal variant (both matched
address blocks) and the minimal variant (no address blocks, the
``driverId: 0`` UNASSIGNED sentinel, the empties-only list shape). The
census-preserved wire shapes (epoch-millisecond ints, the bare-int
unit family, the ``{address, id, name}`` block, the empty lists) are
asserted here beside the model that mirrors them; the epoch-ms
recovery to tz-aware UTC datetimes is pinned to exact values.
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import Trip, TripAddress, TripCoordinates
from tests.samsara_trips_capture import (
    STAMPED_TRIP_RECORDS,
    SYNTHETIC_VEHICLE_ID,
    TRIP_RECORDS,
    TRIPS_MISSING_VEHICLE_ID_400_BODY,
    TRIPS_RANGE_CAP_400_BODY,
    TRIPS_RESPONSE,
)

_ALWAYS_PRESENT_KEYS = frozenset(
    {
        'startMs',
        'endMs',
        'driverId',
        'distanceMeters',
        'fuelConsumedMl',
        'tollMeters',
        'startOdometer',
        'endOdometer',
        'startLocation',
        'endLocation',
        'startCoordinates',
        'endCoordinates',
        'assetIds',
        'codriverIds',
    }
)


class TestFixtureProperties:
    """The census-preserved properties the capture module promises."""

    def test_the_envelope_is_the_bare_trips_list(self) -> None:
        # No pagination object of any kind -- one response per
        # (vehicle, window).
        assert set(TRIPS_RESPONSE) == {'trips'}

    def test_the_variant_split(self) -> None:
        maximal, minimal = TRIP_RECORDS
        assert set(minimal) == _ALWAYS_PRESENT_KEYS
        assert set(maximal) == _ALWAYS_PRESENT_KEYS | {
            'startAddress',
            'endAddress',
        }

    def test_epoch_fields_are_bare_millisecond_ints(self) -> None:
        for record in TRIP_RECORDS:
            for key in ('startMs', 'endMs'):
                value = record[key]
                assert isinstance(value, int)
                assert not isinstance(value, bool)
                # Millisecond scale, not seconds: 13 digits for a
                # 2026-era instant.
                assert value > 10**12

    def test_the_unassigned_driver_sentinel(self) -> None:
        assert TRIP_RECORDS[0]['driverId'] == 7100001
        assert TRIP_RECORDS[1]['driverId'] == 0

    def test_the_list_field_shapes(self) -> None:
        maximal, minimal = TRIP_RECORDS
        # Both list[int] shapes the wire carries: the maximal variant a
        # populated assetIds (the shape a full-scale live pull revealed,
        # absent from the 725-trip census), the minimal an empty one.
        assert maximal['assetIds'] == [3300001, 3300002]
        assert minimal['assetIds'] == []
        # codriverIds stayed empty across census and the larger pull.
        assert maximal['codriverIds'] == []
        assert minimal['codriverIds'] == []

    def test_the_400_bodies_are_plain_rpc_error_strings(self) -> None:
        for body in (TRIPS_MISSING_VEHICLE_ID_400_BODY, TRIPS_RANGE_CAP_400_BODY):
            assert body.startswith('rpc error: code = InvalidArgument desc = ')
        assert 'vehicleId' in TRIPS_MISSING_VEHICLE_ID_400_BODY
        assert '90 days' in TRIPS_RANGE_CAP_400_BODY


class TestTripValidation:
    def test_the_stamped_vehicle_id_is_mirrored_as_string(self) -> None:
        # The one synthesized field: SamsaraTripsPageDecoder stamps the
        # fan-out vehicleId off the sent spec, mirrored as a string to
        # match Vehicle.id for a direct join to the vehicles listing.
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[0])
        assert 'vehicle_id' in Trip.model_fields
        assert trip.vehicle_id == SYNTHETIC_VEHICLE_ID
        assert isinstance(trip.vehicle_id, str)

    def test_an_unstamped_record_fails_loudly(self) -> None:
        # vehicle_id is REQUIRED: the wire never echoes it, so a record
        # reaching the model unstamped is a decoder bug that must fail
        # validation, never land vehicle-less.
        with pytest.raises(ValidationError):
            Trip.model_validate(TRIP_RECORDS[0])

    def test_epoch_ms_recovers_exact_tz_aware_utc_datetimes(self) -> None:
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[0])
        assert trip.start_time == datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)
        assert trip.end_time == datetime(2026, 1, 1, 1, 0, 45, 456000, tzinfo=UTC)
        # Canonical UTC identity, not merely aware (the canon doctrine).
        assert trip.start_time.tzinfo is UTC
        assert trip.end_time.tzinfo is UTC

    def test_millisecond_free_epochs_recover_whole_instants(self) -> None:
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[1])
        assert trip.start_time == datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
        assert trip.end_time == datetime(2026, 1, 2, 1, 0, tzinfo=UTC)

    def test_the_unit_int_fields_mirror_verbatim(self) -> None:
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[0])
        assert trip.distance_meters == 52000
        assert trip.fuel_consumed_ml == 21000
        assert trip.toll_meters == 1200
        assert trip.start_odometer == 240001000
        assert trip.end_odometer == 240053000
        for value in (
            trip.distance_meters,
            trip.fuel_consumed_ml,
            trip.toll_meters,
            trip.start_odometer,
            trip.end_odometer,
        ):
            assert isinstance(value, int)

    def test_the_zero_driver_sentinel_is_untouched(self) -> None:
        # 0 means unassigned (110/725) -- mirrored verbatim, never
        # nulled or interpreted.
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[1])
        assert trip.driver_id == 0
        assert isinstance(trip.driver_id, int)

    def test_the_list_fields_mirror_verbatim(self) -> None:
        maximal = Trip.model_validate(STAMPED_TRIP_RECORDS[0])
        minimal = Trip.model_validate(STAMPED_TRIP_RECORDS[1])
        # The list[int] typing round-trips both the populated shape (the
        # at-scale finding) and the empty one, verbatim.
        assert maximal.asset_ids == [3300001, 3300002]
        assert minimal.asset_ids == []
        assert maximal.codriver_ids == []
        assert minimal.codriver_ids == []

    def test_the_coordinate_blocks(self) -> None:
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[0])
        start = trip.start_coordinates
        end = trip.end_coordinates
        assert isinstance(start, TripCoordinates)
        assert isinstance(end, TripCoordinates)
        assert start.latitude == pytest.approx(40.2001)
        assert start.longitude == pytest.approx(-100.2001)
        assert end.latitude == pytest.approx(40.2051)

    def test_the_maximal_record_carries_both_address_blocks(self) -> None:
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[0])
        start_address = trip.start_address
        end_address = trip.end_address
        assert isinstance(start_address, TripAddress)
        assert isinstance(end_address, TripAddress)
        assert start_address.address == '100 Example St, Example City, CA'
        # The address id is a BARE int, unlike the string ids of the
        # vehicles/drivers surfaces.
        assert start_address.id == 8800001
        assert isinstance(start_address.id, int)
        assert end_address.name == 'Example Terminal'

    def test_the_minimal_record_lands_both_address_blocks_null(self) -> None:
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[1])
        assert trip.start_address is None
        assert trip.end_address is None

    def test_the_geocoded_location_strings(self) -> None:
        trip = Trip.model_validate(STAMPED_TRIP_RECORDS[1])
        assert trip.start_location == '300 Example Blvd, Example City, CA'
        assert trip.end_location == '400 Example Way, Example City, CA'

    def test_a_quoted_epoch_fails_loudly(self) -> None:
        # The census observed BARE ints only; a quoted number is wire
        # drift the strict recovery must reject, not coerce.
        drifted = {**STAMPED_TRIP_RECORDS[1], 'startMs': '1767312000000'}
        with pytest.raises(ValidationError):
            Trip.model_validate(drifted)

    def test_a_missing_end_fails_loudly(self) -> None:
        # endMs was present on every observed trip (in-progress trips
        # materialize on completion); a trip without one is an
        # unobserved shape that must fail validation, not land null.
        truncated = dict(STAMPED_TRIP_RECORDS[1])
        del truncated['endMs']
        with pytest.raises(ValidationError):
            Trip.model_validate(truncated)
