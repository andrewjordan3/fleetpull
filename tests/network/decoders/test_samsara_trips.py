"""Tests for fleetpull.network.decoders.samsara_trips.

Fixtures are the committed synthetic trips capture
(``tests/samsara_trips_capture.py``): the unpaginated ``{"trips": [...]}``
envelope whose per-trip records carry NO vehicle field of any kind. The
decoder stamps every record with the fan-out ``vehicleId`` read off the
SENT spec (the report family's sent-spec stamp applied to a fan-out
member rather than a window), so the stored row can be attributed to a
vehicle.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import PageDecoder
from fleetpull.network.contract.request import HttpMethod, RequestSpec
from fleetpull.network.decoders import SamsaraTripsPageDecoder
from fleetpull.vocabulary import JsonValue
from tests.samsara_trips_capture import (
    SYNTHETIC_VEHICLE_ID,
    TRIP_RECORDS,
    TRIPS_RESPONSE,
)

_VEHICLE_ID_PARAM = 'vehicleId'
_RECORDS_KEY = 'trips'


def build_trips_spec(vehicle_id: str = SYNTHETIC_VEHICLE_ID) -> RequestSpec:
    # The fan-out request the decoder reads its stamp back off of: the
    # member merged verbatim as vehicleId beside the epoch window.
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/v1/fleet/trips',
        params={
            _VEHICLE_ID_PARAM: vehicle_id,
            'startMs': '1767225600000',
            'endMs': '1767830400000',
        },
    )


def build_decoder() -> SamsaraTripsPageDecoder:
    return SamsaraTripsPageDecoder(
        records_key=_RECORDS_KEY, member_key=_VEHICLE_ID_PARAM
    )


class TestSamsaraTripsPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = build_decoder()
        assert isinstance(decoder, SamsaraTripsPageDecoder)

    def test_first_request_is_identity(self) -> None:
        # The surface is unpaginated: page one is the base spec unchanged.
        spec = build_trips_spec()
        assert build_decoder().first_request(spec) is spec

    def test_extracts_every_record_and_completes(self) -> None:
        decoded = build_decoder().decode_page(build_trips_spec(), TRIPS_RESPONSE)
        assert len(decoded.records) == len(TRIP_RECORDS)
        # No continuation of any kind -- the terminal single page.
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress is None

    def test_stamps_every_trip_with_the_sent_vehicle(self) -> None:
        # The synthesized vehicle identity, copied VERBATIM from the SENT
        # spec's own vehicleId param -- the report family's sent-spec
        # stamp sourcing a fan-out member rather than a window.
        decoded = build_decoder().decode_page(build_trips_spec(), TRIPS_RESPONSE)
        for record in decoded.records:
            assert record['vehicleId'] == SYNTHETIC_VEHICLE_ID

    def test_the_stamp_rides_the_verbatim_trip(self) -> None:
        # The wire record is mirrored untouched; the stamp is the only
        # added key, and the source record is never mutated.
        decoded = build_decoder().decode_page(build_trips_spec(), TRIPS_RESPONSE)
        assert decoded.records[0] == {
            **TRIP_RECORDS[0],
            'vehicleId': SYNTHETIC_VEHICLE_ID,
        }
        assert 'vehicleId' not in TRIP_RECORDS[0]

    def test_the_stamp_wins_a_wire_key_collision(self) -> None:
        # The wire never echoes vehicleId (census); if it ever does, the
        # stamp -- the vehicle actually asked of the provider -- must
        # win, the report family's stamp-wins merge order.
        envelope: dict[str, JsonValue] = {
            'trips': [{'startMs': 1, 'vehicleId': 'wire-collision'}]
        }
        decoded = build_decoder().decode_page(build_trips_spec(), envelope)
        assert decoded.records[0]['vehicleId'] == SYNTHETIC_VEHICLE_ID

    @pytest.mark.parametrize(
        'params',
        [
            None,
            {},
            # A window with no member is a fan-out wiring bug, not a
            # request: vehicleId is what makes trips a per-vehicle pull,
            # and silently unstamped rows would have no vehicle identity.
            {'startMs': '1767225600000', 'endMs': '1767830400000'},
        ],
    )
    def test_a_sent_spec_without_the_member_raises(
        self, params: dict[str, str] | None
    ) -> None:
        spec = RequestSpec(
            method=HttpMethod.GET,
            url='https://api.example.com/v1/fleet/trips',
            params=params,
        )
        with pytest.raises(ProviderResponseError, match='vehicleId'):
            build_decoder().decode_page(spec, TRIPS_RESPONSE)

    def test_missing_record_key_raises(self) -> None:
        with pytest.raises(ProviderResponseError, match='missing the record key'):
            build_decoder().decode_page(build_trips_spec(), {'other': []})
