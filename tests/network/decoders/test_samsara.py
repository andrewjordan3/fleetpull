"""Tests for fleetpull.network.decoders.samsara.

Fixtures are synthetic, constructed in the verified envelope shape
with the API's real camelCase keys — the alias handling is part of
what is under test.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import PageDecoder
from fleetpull.network.contract.request import HttpMethod, RequestSpec
from fleetpull.network.decoders import (
    SamsaraCursorPageDecoder,
    SamsaraVehicleSeriesPageDecoder,
)
from fleetpull.vocabulary import JsonObject, JsonValue


def build_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/fleet/vehicles/stats',
        params={'types': 'gps'},
    )


class TestSamsaraCursorPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = SamsaraCursorPageDecoder(
            records_key='data', results_limit=512
        )
        assert isinstance(decoder, SamsaraCursorPageDecoder)

    def test_first_request_carries_limit_and_no_after(self) -> None:
        spec = build_spec()
        prepared = SamsaraCursorPageDecoder(
            records_key='data', results_limit=512
        ).first_request(spec)
        assert prepared.params is not None
        assert prepared.params['limit'] == '512'
        assert 'after' not in prepared.params
        # Pre-existing params survive the merge.
        assert prepared.params['types'] == 'gps'

    def test_cursor_advance_merges_after(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True, 'endCursor': 'cursor-0001'},
            'data': [{'id': 'a'}],
        }
        decoded = SamsaraCursorPageDecoder(
            records_key='data', results_limit=512
        ).decode_page(build_spec(), envelope)
        assert decoded.advance.next_spec is not None
        assert decoded.advance.next_spec.params == {
            'types': 'gps',
            'after': 'cursor-0001',
        }
        assert decoded.advance.durable_progress is None
        assert decoded.records == [{'id': 'a'}]

    def test_has_next_page_false_completes(self) -> None:
        # endCursor absent on the terminal page: the default-None field
        # is exercised alongside termination.
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': False},
            'data': [{'id': 'a'}],
        }
        decoded = SamsaraCursorPageDecoder(
            records_key='data', results_limit=512
        ).decode_page(build_spec(), envelope)
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress is None
        assert decoded.records == [{'id': 'a'}]

    def test_continuation_without_cursor_raises_truncation_guard(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True},
            'data': [{'id': 'a'}],
        }
        with pytest.raises(ProviderResponseError, match='endCursor'):
            SamsaraCursorPageDecoder(records_key='data', results_limit=512).decode_page(
                build_spec(), envelope
            )

    def test_continuation_with_empty_cursor_raises_truncation_guard(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True, 'endCursor': ''},
            'data': [{'id': 'a'}],
        }
        with pytest.raises(ProviderResponseError, match='endCursor'):
            SamsaraCursorPageDecoder(records_key='data', results_limit=512).decode_page(
                build_spec(), envelope
            )

    @pytest.mark.parametrize(
        'envelope',
        [
            {'data': []},  # pagination block missing entirely
            {'pagination': {'endCursor': 'cursor-0001'}, 'data': []},  # no flag
            # Type drift on the flag we act on. The slice models are
            # strict=True, so a bool-ish string and an int both reject
            # rather than coerce — the failure mode this layer exists to
            # make loud.
            {'pagination': {'hasNextPage': 'true'}, 'data': []},
            {'pagination': {'hasNextPage': 1}, 'data': []},
        ],
    )
    def test_malformed_metadata_raises(self, envelope: JsonValue) -> None:
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            SamsaraCursorPageDecoder(records_key='data', results_limit=512).decode_page(
                build_spec(), envelope
            )


def build_series_spec() -> RequestSpec:
    # The series tests' own spec: its types value names the same stat
    # type the series decoder unnests, matching the wire contract the
    # production builder enforces (types value == series key).
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/fleet/vehicles/stats/history',
        params={'types': 'engineStates'},
    )


def build_series_decoder() -> SamsaraVehicleSeriesPageDecoder:
    return SamsaraVehicleSeriesPageDecoder(
        records_key='data', results_limit=512, series_key='engineStates'
    )


def series_envelope() -> dict[str, JsonValue]:
    """A synthetic vehicle-stats continuation page in the captured shape.

    Two carrier vehicles (multi- and single-reading, the second without
    ``externalIds``) plus one empty-series and one series-less vehicle.
    """
    return {
        'data': [
            {
                'id': '281474981110001',
                'name': 'Truck 901',
                'externalIds': {
                    'samsara.serial': 'GSYNTH00009A',
                    'samsara.vin': 'SYNTH000000000091',
                },
                'engineStates': [
                    {'time': '2026-01-01T12:00:03.062Z', 'value': 'On'},
                    {'time': '2026-01-01T12:20:15.500Z', 'value': 'Idle'},
                ],
            },
            {
                'id': '281474981110002',
                'name': 'Truck 902',
                'engineStates': [
                    {'time': '2026-01-01T12:05:00.000Z', 'value': 'Off'},
                ],
            },
            {
                'id': '281474981110003',
                'name': 'Truck 903',
                'engineStates': [],
            },
            {
                'id': '281474981110004',
                'name': 'Truck 904',
            },
        ],
        'pagination': {'hasNextPage': True, 'endCursor': 'cursor-0002'},
    }


class TestSamsaraVehicleSeriesPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = build_series_decoder()
        assert isinstance(decoder, SamsaraVehicleSeriesPageDecoder)

    def test_first_request_delegates_to_the_inner_cursor_decoder(self) -> None:
        # Verbatim delegation: the prepared spec is exactly what the
        # inner cursor decoder produces (limit merged, no after,
        # pre-existing params kept).
        spec = build_series_spec()
        prepared = build_series_decoder().first_request(spec)
        inner = SamsaraCursorPageDecoder(
            records_key='data', results_limit=512
        ).first_request(spec)
        assert prepared == inner
        assert prepared.params is not None
        assert prepared.params['limit'] == '512'
        assert 'after' not in prepared.params

    def test_unnests_one_record_per_reading(self) -> None:
        # Four vehicles, 2 + 1 + 0 + 0 readings: exactly three flat
        # records, in vehicle-then-series order; the empty-series and
        # series-less vehicles contribute zero records, not errors.
        decoded = build_series_decoder().decode_page(
            build_series_spec(), series_envelope()
        )
        assert len(decoded.records) == 3
        assert [record['vehicleId'] for record in decoded.records] == [
            '281474981110001',
            '281474981110001',
            '281474981110002',
        ]

    def test_synthesized_identity_merges_onto_the_verbatim_reading(self) -> None:
        decoded = build_series_decoder().decode_page(
            build_series_spec(), series_envelope()
        )
        first = decoded.records[0]
        assert first == {
            'vehicleId': '281474981110001',
            'vehicleName': 'Truck 901',
            'vehicleSerial': 'GSYNTH00009A',
            'vehicleVin': 'SYNTH000000000091',
            'time': '2026-01-01T12:00:03.062Z',
            'value': 'On',
        }

    def test_absent_external_ids_omits_serial_and_vin(self) -> None:
        # The omit-absent-keys posture: no key, not a null.
        decoded = build_series_decoder().decode_page(
            build_series_spec(), series_envelope()
        )
        no_external_ids = decoded.records[2]
        assert no_external_ids['vehicleId'] == '281474981110002'
        assert 'vehicleSerial' not in no_external_ids
        assert 'vehicleVin' not in no_external_ids

    def test_a_partial_external_ids_block_synthesizes_only_its_keys(self) -> None:
        envelope: dict[str, JsonValue] = {
            'data': [
                {
                    'id': '281474981110005',
                    'name': 'Truck 905',
                    'externalIds': {'samsara.serial': 'GSYNTH00009E'},
                    'engineStates': [
                        {'time': '2026-01-01T12:00:03.062Z', 'value': 'On'}
                    ],
                }
            ],
            'pagination': {'hasNextPage': False},
        }
        decoded = build_series_decoder().decode_page(build_series_spec(), envelope)
        (record,) = decoded.records
        assert record['vehicleSerial'] == 'GSYNTH00009E'
        assert 'vehicleVin' not in record

    def test_reading_keys_win_the_merge(self) -> None:
        # Collision is impossible by census (the synthesized names were
        # chosen collision-free against every observed series key); if
        # the wire ever grows one anyway, the verbatim reading value
        # must survive -- the documented merge order, pinned.
        envelope: dict[str, JsonValue] = {
            'data': [
                {
                    'id': '281474981110006',
                    'name': 'Truck 906',
                    'engineStates': [
                        {
                            'time': '2026-01-01T12:00:03.062Z',
                            'value': 'On',
                            'vehicleId': 'wire-collision',
                        }
                    ],
                }
            ],
            'pagination': {'hasNextPage': False},
        }
        decoded = build_series_decoder().decode_page(build_series_spec(), envelope)
        assert decoded.records[0]['vehicleId'] == 'wire-collision'

    def test_the_cursor_passes_through_on_a_two_page_walk(self) -> None:
        # The pagination advance is the inner cursor decoder's,
        # untouched: the continuation merges `after` onto the SENT spec
        # (window and types params persisting) and the terminal page
        # completes.
        decoder = build_series_decoder()
        first = decoder.first_request(build_series_spec())
        continued = decoder.decode_page(first, series_envelope())
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'types': 'engineStates',
            'limit': '512',
            'after': 'cursor-0002',
        }
        assert continued.advance.durable_progress is None
        terminal_envelope: dict[str, JsonValue] = {
            'data': [
                {
                    'id': '281474981110007',
                    'name': 'Truck 907',
                    'engineStates': [
                        {'time': '2026-01-01T12:59:56.881Z', 'value': 'Off'}
                    ],
                }
            ],
            'pagination': {'hasNextPage': False, 'endCursor': ''},
        }
        terminal = decoder.decode_page(next_spec, terminal_envelope)
        assert terminal.advance.next_spec is None
        assert len(terminal.records) == 1

    def test_continuation_without_cursor_raises_truncation_guard(self) -> None:
        # Inherited from the inner cursor decoder by delegation.
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True},
            'data': [],
        }
        with pytest.raises(ProviderResponseError, match='endCursor'):
            build_series_decoder().decode_page(build_series_spec(), envelope)

    @pytest.mark.parametrize(
        'vehicle',
        [
            # A present series value that is not a list.
            {'id': 'v', 'name': 'n', 'engineStates': 'On'},
            # A series element that is not a JSON object.
            {'id': 'v', 'name': 'n', 'engineStates': ['On']},
            # externalIds present but not a JSON object.
            {'id': 'v', 'name': 'n', 'externalIds': 'GSYNTH', 'engineStates': [{}]},
        ],
    )
    def test_malformed_vehicle_shapes_raise(self, vehicle: JsonObject) -> None:
        envelope: dict[str, JsonValue] = {
            'data': [vehicle],
            'pagination': {'hasNextPage': False},
        }
        with pytest.raises(ProviderResponseError):
            build_series_decoder().decode_page(build_series_spec(), envelope)
