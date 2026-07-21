"""Tests for fleetpull.network.decoders.samsara_reports.

Fixtures are synthetic, constructed in the verified envelope shape with
the API's real camelCase keys: the fuel-energy report surfaces' nested
record list (``data`` is an OBJECT holding the per-surface report list)
and rows with NO event-time key, stamped from the sent
``startDate``/``endDate`` params (probed 2026-07-21).
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import PageDecoder
from fleetpull.network.contract.request import HttpMethod, RequestSpec
from fleetpull.network.decoders import SamsaraWindowReportPageDecoder
from fleetpull.vocabulary import JsonValue


def build_report_spec() -> RequestSpec:
    # The report tests' own spec: the fuel-energy surfaces' OWN window
    # param names (startDate/endDate, RFC3339 datetimes accepted
    # despite the names) -- the params the decoder copies back off the
    # sent spec onto every report row.
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/fleet/reports/vehicles/fuel-energy',
        params={
            'startDate': '2026-01-02T00:00:00Z',
            'endDate': '2026-01-03T00:00:00Z',
        },
    )


def build_report_decoder(
    report_key: str = 'vehicleReports',
) -> SamsaraWindowReportPageDecoder:
    return SamsaraWindowReportPageDecoder(
        records_key='data', report_key=report_key, results_limit=100
    )


def report_envelope(report_key: str = 'vehicleReports') -> dict[str, JsonValue]:
    """A synthetic fuel-energy continuation page in the captured shape.

    The nested envelope: `data` is an OBJECT whose only key is the
    per-surface report list. Rows carry NO event-time key of any kind.
    """
    return {
        'data': {
            report_key: [
                {'vehicle': {'id': '281474981110001'}, 'fuelConsumedMl': 168220},
                {'vehicle': {'id': '281474981110002'}, 'fuelConsumedMl': 30310},
            ],
        },
        'pagination': {'hasNextPage': True, 'endCursor': 'cursor-0003'},
    }


class TestSamsaraWindowReportPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = build_report_decoder()
        assert isinstance(decoder, SamsaraWindowReportPageDecoder)

    def test_first_request_carries_limit_and_no_after(self) -> None:
        # Exactly the cursor decoder's injection: limit merged onto the
        # builder's spec, no after, pre-existing params kept.
        prepared = build_report_decoder().first_request(build_report_spec())
        assert prepared.params is not None
        assert prepared.params['limit'] == '100'
        assert 'after' not in prepared.params
        assert prepared.params['startDate'] == '2026-01-02T00:00:00Z'
        assert prepared.params['endDate'] == '2026-01-03T00:00:00Z'

    @pytest.mark.parametrize('report_key', ['vehicleReports', 'driverReports'])
    def test_extracts_the_nested_report_list(self, report_key: str) -> None:
        # Both arms' report keys: the record list lives one level under
        # `data`, which is an OBJECT on this surface family.
        decoded = build_report_decoder(report_key).decode_page(
            build_report_spec(), report_envelope(report_key)
        )
        assert len(decoded.records) == 2
        vehicles = [record['vehicle'] for record in decoded.records]
        assert vehicles == [
            {'id': '281474981110001'},
            {'id': '281474981110002'},
        ]

    def test_stamps_every_report_with_the_sent_window(self) -> None:
        # The synthesized window-identity keys, copied VERBATIM from
        # the SENT spec's own startDate/endDate params -- the stats
        # triple's synthesized-identity precedent sourced from the
        # request rather than the record.
        decoded = build_report_decoder().decode_page(
            build_report_spec(), report_envelope()
        )
        for record in decoded.records:
            assert record['windowStartDate'] == '2026-01-02T00:00:00Z'
            assert record['windowEndDate'] == '2026-01-03T00:00:00Z'

    def test_the_stamp_rides_the_verbatim_report(self) -> None:
        decoded = build_report_decoder().decode_page(
            build_report_spec(), report_envelope()
        )
        first = decoded.records[0]
        assert first == {
            'vehicle': {'id': '281474981110001'},
            'fuelConsumedMl': 168220,
            'windowStartDate': '2026-01-02T00:00:00Z',
            'windowEndDate': '2026-01-03T00:00:00Z',
        }

    def test_the_stamp_wins_a_wire_key_collision(self) -> None:
        # Collision is impossible by census (no time-shaped key on any
        # walked report); if the wire ever grows one anyway, the stamp
        # -- the row's REQUIRED time identity, exactly what was asked
        # of the provider -- must survive: the documented merge order,
        # the inverse of the series decoder's reading-keys-win, pinned.
        envelope: dict[str, JsonValue] = {
            'data': {
                'vehicleReports': [
                    {
                        'vehicle': {'id': 'v'},
                        'windowStartDate': 'wire-collision',
                    }
                ],
            },
            'pagination': {'hasNextPage': False},
        }
        decoded = build_report_decoder().decode_page(build_report_spec(), envelope)
        assert decoded.records[0]['windowStartDate'] == '2026-01-02T00:00:00Z'

    @pytest.mark.parametrize(
        'params',
        [
            None,
            {},
            {'startDate': '2026-01-02T00:00:00Z'},
            {'endDate': '2026-01-03T00:00:00Z'},
            # The sibling surfaces' names are NOT this family's: a spec
            # built with startTime/endTime is a wiring bug, not a window.
            {
                'startTime': '2026-01-02T00:00:00Z',
                'endTime': '2026-01-03T00:00:00Z',
            },
        ],
    )
    def test_a_sent_spec_without_the_window_params_raises(
        self, params: dict[str, str] | None
    ) -> None:
        # A wiring bug surfaced loudly -- never silently unstamped rows,
        # which would strip every report of its time identity.
        spec = RequestSpec(
            method=HttpMethod.GET,
            url='https://api.example.com/fleet/reports/vehicles/fuel-energy',
            params=params,
        )
        with pytest.raises(ProviderResponseError, match='startDate'):
            build_report_decoder().decode_page(spec, report_envelope())

    @pytest.mark.parametrize(
        'envelope',
        [
            # data missing entirely.
            {'pagination': {'hasNextPage': False}},
            # data present but None.
            {'data': None, 'pagination': {'hasNextPage': False}},
            # data not an object (the flat-list shape would be a
            # different surface -- loud, never silently empty).
            {'data': [], 'pagination': {'hasNextPage': False}},
            # The report key missing inside the container.
            {'data': {}, 'pagination': {'hasNextPage': False}},
            # The report key present but not a list.
            {
                'data': {'vehicleReports': {}},
                'pagination': {'hasNextPage': False},
            },
            # A report element that is not a JSON object.
            {
                'data': {'vehicleReports': ['row']},
                'pagination': {'hasNextPage': False},
            },
        ],
    )
    def test_structurally_violating_shapes_raise(self, envelope: JsonValue) -> None:
        with pytest.raises(ProviderResponseError):
            build_report_decoder().decode_page(build_report_spec(), envelope)

    def test_the_cursor_passes_through_on_a_two_page_walk(self) -> None:
        # The pagination verdict is the shared cursor contract's: the
        # continuation merges `after` onto the SENT spec (the window
        # params persisting, so page two's rows stamp identically) and
        # the terminal page completes.
        decoder = build_report_decoder()
        first = decoder.first_request(build_report_spec())
        continued = decoder.decode_page(first, report_envelope())
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'startDate': '2026-01-02T00:00:00Z',
            'endDate': '2026-01-03T00:00:00Z',
            'limit': '100',
            'after': 'cursor-0003',
        }
        assert continued.advance.durable_progress is None
        terminal_envelope: dict[str, JsonValue] = {
            'data': {
                'vehicleReports': [
                    {'vehicle': {'id': '281474981110003'}, 'fuelConsumedMl': 9040}
                ],
            },
            'pagination': {'hasNextPage': False, 'endCursor': ''},
        }
        terminal = decoder.decode_page(next_spec, terminal_envelope)
        assert terminal.advance.next_spec is None
        assert len(terminal.records) == 1
        assert terminal.records[0]['windowStartDate'] == '2026-01-02T00:00:00Z'

    def test_continuation_without_cursor_raises_truncation_guard(self) -> None:
        # The shared cursor verdict's promised-continuation guard.
        envelope: dict[str, JsonValue] = {
            'data': {'vehicleReports': []},
            'pagination': {'hasNextPage': True},
        }
        with pytest.raises(ProviderResponseError, match='endCursor'):
            build_report_decoder().decode_page(build_report_spec(), envelope)

    def test_malformed_pagination_raises(self) -> None:
        envelope: dict[str, JsonValue] = {'data': {'vehicleReports': []}}
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            build_report_decoder().decode_page(build_report_spec(), envelope)
