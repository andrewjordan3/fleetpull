"""Tests for fleetpull.network.decoders.motive.

Fixtures are synthetic, constructed in the verified envelope shape: a
per-endpoint top-level wrapper list plus the ``pagination`` block; the
window-report fixtures add the utilization surfaces' captured facts
(rows with NO time identity, stamped from the sent
``start_date``/``end_date`` date labels -- probed 2026-07-21).
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import PageDecoder
from fleetpull.network.contract.request import HttpMethod, RequestSpec
from fleetpull.network.decoders import (
    MotiveWindowReportPageDecoder,
    MotiveWrappedListPageDecoder,
    MotiveWrappedSinglePageDecoder,
)
from fleetpull.vocabulary import JsonValue


def build_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/v1/vehicles',
    )


def build_decoder() -> MotiveWrappedListPageDecoder:
    return MotiveWrappedListPageDecoder(
        list_key='vehicles', item_key='vehicle', per_page=100
    )


def build_envelope(*, page_no: int, per_page: int, total: int) -> dict[str, JsonValue]:
    return {
        'vehicles': [{'vehicle': {'id': 1}}, {'vehicle': {'id': 2}}],
        'pagination': {'page_no': page_no, 'per_page': per_page, 'total': total},
    }


class TestMotiveWrappedListPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = build_decoder()
        assert isinstance(decoder, MotiveWrappedListPageDecoder)

    def test_first_request_sets_page_one_and_size(self) -> None:
        prepared = build_decoder().first_request(build_spec())
        assert prepared.params == {'page_no': '1', 'per_page': '100'}

    def test_unwraps_records(self) -> None:
        decoded = build_decoder().decode_page(
            build_spec(), build_envelope(page_no=1, per_page=2, total=10)
        )
        assert decoded.records == [{'id': 1}, {'id': 2}]

    def test_advance_continues_with_echoed_next_page(self) -> None:
        decoded = build_decoder().decode_page(
            build_spec(), build_envelope(page_no=1, per_page=2, total=10)
        )
        assert decoded.advance.next_spec is not None
        assert decoded.advance.next_spec.params == {'page_no': '2', 'per_page': '2'}
        assert decoded.advance.durable_progress is None

    def test_advance_completes_on_last_page(self) -> None:
        decoded = build_decoder().decode_page(
            build_spec(), build_envelope(page_no=5, per_page=2, total=10)
        )
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress is None

    def test_missing_record_key_raises(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'page_no': 1, 'per_page': 2, 'total': 10}
        }
        with pytest.raises(ProviderResponseError, match='missing the record key'):
            build_decoder().decode_page(build_spec(), envelope)

    def test_malformed_pagination_raises(self) -> None:
        envelope: dict[str, JsonValue] = {
            'vehicles': [{'vehicle': {'id': 1}}],
            'pagination': {'page_no': '1', 'per_page': 2, 'total': 10},
        }
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            build_decoder().decode_page(build_spec(), envelope)


def build_single_page_decoder() -> MotiveWrappedSinglePageDecoder:
    return MotiveWrappedSinglePageDecoder(
        list_key='vehicle_locations', item_key='vehicle_location'
    )


class TestMotiveWrappedSinglePageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = build_single_page_decoder()
        assert isinstance(decoder, MotiveWrappedSinglePageDecoder)

    def test_first_request_is_identity(self) -> None:
        spec = build_spec()
        assert build_single_page_decoder().first_request(spec) is spec

    def test_unwraps_records(self) -> None:
        envelope: dict[str, JsonValue] = {
            'vehicle_locations': [
                {'vehicle_location': {'id': 1}},
                {'vehicle_location': {'id': 2}},
            ]
        }
        decoded = build_single_page_decoder().decode_page(build_spec(), envelope)
        assert decoded.records == [{'id': 1}, {'id': 2}]

    def test_is_always_terminal(self) -> None:
        envelope: dict[str, JsonValue] = {
            'vehicle_locations': [{'vehicle_location': {'id': 1}}]
        }
        decoded = build_single_page_decoder().decode_page(build_spec(), envelope)
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress is None

    def test_empty_list_decodes_to_no_records(self) -> None:
        envelope: dict[str, JsonValue] = {'vehicle_locations': []}
        decoded = build_single_page_decoder().decode_page(build_spec(), envelope)
        assert decoded.records == []
        assert decoded.advance.next_spec is None

    def test_missing_list_key_raises(self) -> None:
        envelope: dict[str, JsonValue] = {'other': []}
        with pytest.raises(ProviderResponseError, match='missing the record key'):
            build_single_page_decoder().decode_page(build_spec(), envelope)

    def test_missing_item_key_raises(self) -> None:
        envelope: dict[str, JsonValue] = {'vehicle_locations': [{'other': {'id': 1}}]}
        with pytest.raises(ProviderResponseError, match='missing the item key'):
            build_single_page_decoder().decode_page(build_spec(), envelope)


def build_report_spec() -> RequestSpec:
    # A spec as the shared Motive date-range builder renders it for a
    # [2026-01-05, 2026-01-06) one-day unit: the INCLUSIVE
    # start_date/end_date label pair the decoder copies back off the
    # sent spec onto every rollup row.
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/v2/vehicle_utilization',
        params={'start_date': '2026-01-05', 'end_date': '2026-01-05'},
    )


def build_report_decoder() -> MotiveWindowReportPageDecoder:
    return MotiveWindowReportPageDecoder(
        list_key='vehicle_utilizations',
        item_key='vehicle_utilization',
        per_page=100,
    )


def report_envelope(*, page_no: int, per_page: int, total: int) -> dict[str, JsonValue]:
    """A synthetic utilization page in the captured wrapped-list shape.

    Rows carry NO date or time identity of any kind -- the probe's
    central fact; the only timestamp-shaped keys on a decoded record
    are the decoder's stamps.
    """
    return {
        'vehicle_utilizations': [
            {'vehicle_utilization': {'vehicle': {'id': 9900101}, 'total_fuel': 45.9}},
            {'vehicle_utilization': {'vehicle': {'id': 9900102}, 'total_fuel': 0.0}},
        ],
        'pagination': {'page_no': page_no, 'per_page': per_page, 'total': total},
    }


class TestMotiveWindowReportPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = build_report_decoder()
        assert isinstance(decoder, MotiveWindowReportPageDecoder)

    def test_first_request_sets_page_one_and_size_keeping_the_window(self) -> None:
        # Exactly the wrapped-list decoder's injection, layered onto the
        # builder's window params.
        prepared = build_report_decoder().first_request(build_report_spec())
        assert prepared.params == {
            'start_date': '2026-01-05',
            'end_date': '2026-01-05',
            'page_no': '1',
            'per_page': '100',
        }

    def test_unwraps_the_wrapped_records(self) -> None:
        decoded = build_report_decoder().decode_page(
            build_report_spec(), report_envelope(page_no=2, per_page=2, total=3)
        )
        vehicles = [record['vehicle'] for record in decoded.records]
        assert vehicles == [{'id': 9900101}, {'id': 9900102}]

    def test_stamps_every_record_with_the_sent_window(self) -> None:
        # The synthesized window-identity keys, copied VERBATIM from
        # the SENT spec's own start_date/end_date date labels -- the
        # fuel-energy pair's request-sourced stamp on Motive wire.
        decoded = build_report_decoder().decode_page(
            build_report_spec(), report_envelope(page_no=2, per_page=2, total=3)
        )
        for record in decoded.records:
            assert record['windowStartDate'] == '2026-01-05'
            assert record['windowEndDate'] == '2026-01-05'

    def test_the_stamp_rides_the_verbatim_record(self) -> None:
        decoded = build_report_decoder().decode_page(
            build_report_spec(), report_envelope(page_no=2, per_page=2, total=3)
        )
        assert decoded.records[0] == {
            'vehicle': {'id': 9900101},
            'total_fuel': 45.9,
            'windowStartDate': '2026-01-05',
            'windowEndDate': '2026-01-05',
        }

    def test_the_stamp_wins_a_wire_key_collision(self) -> None:
        # Collision is impossible by census (no time-shaped key on any
        # sampled row); if the wire ever grows one anyway, the stamp --
        # the row's REQUIRED time identity, exactly what was asked of
        # the provider -- must survive: the documented merge order,
        # pinned (the fuel-energy decoder's order, mirrored).
        envelope: dict[str, JsonValue] = {
            'vehicle_utilizations': [
                {'vehicle_utilization': {'windowStartDate': 'wire-collision'}}
            ],
            'pagination': {'page_no': 1, 'per_page': 100, 'total': 1},
        }
        decoded = build_report_decoder().decode_page(build_report_spec(), envelope)
        assert decoded.records[0]['windowStartDate'] == '2026-01-05'

    @pytest.mark.parametrize(
        'params',
        [
            None,
            {},
            {'start_date': '2026-01-05'},
            {'end_date': '2026-01-05'},
            # The Samsara report family's camelCase names are NOT this
            # wire's: a spec built with them is a wiring bug, not a
            # window.
            {'startDate': '2026-01-05', 'endDate': '2026-01-05'},
        ],
    )
    def test_a_sent_spec_without_the_window_params_raises(
        self, params: dict[str, str] | None
    ) -> None:
        # A wiring bug surfaced loudly -- never silently unstamped rows,
        # which would strip every rollup of its time identity.
        spec = RequestSpec(
            method=HttpMethod.GET,
            url='https://api.example.com/v2/vehicle_utilization',
            params=params,
        )
        with pytest.raises(ProviderResponseError, match='start_date'):
            build_report_decoder().decode_page(
                spec, report_envelope(page_no=1, per_page=2, total=2)
            )

    @pytest.mark.parametrize(
        'envelope',
        [
            # The list key missing entirely.
            {'pagination': {'page_no': 1, 'per_page': 2, 'total': 2}},
            # The list key present but not a list.
            {
                'vehicle_utilizations': {},
                'pagination': {'page_no': 1, 'per_page': 2, 'total': 2},
            },
            # A wrapper missing the item key.
            {
                'vehicle_utilizations': [{'other': {'id': 1}}],
                'pagination': {'page_no': 1, 'per_page': 2, 'total': 2},
            },
            # A wrapped record that is not a JSON object.
            {
                'vehicle_utilizations': [{'vehicle_utilization': 'row'}],
                'pagination': {'page_no': 1, 'per_page': 2, 'total': 2},
            },
        ],
    )
    def test_structurally_violating_shapes_raise(self, envelope: JsonValue) -> None:
        with pytest.raises(ProviderResponseError):
            build_report_decoder().decode_page(build_report_spec(), envelope)

    def test_malformed_pagination_raises(self) -> None:
        envelope: dict[str, JsonValue] = {
            'vehicle_utilizations': [],
            'pagination': {'page_no': '1', 'per_page': 2, 'total': 2},
        }
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            build_report_decoder().decode_page(build_report_spec(), envelope)

    def test_the_offset_cursor_passes_through_on_a_two_page_walk(self) -> None:
        # The pagination verdict is the shared offset contract's: the
        # continuation merges page_no+1 at the echoed size onto the
        # SENT spec (the window params persisting, so page two's rows
        # stamp identically) and the terminal page completes under the
        # page_no * per_page >= total rule.
        decoder = build_report_decoder()
        first = decoder.first_request(build_report_spec())
        continued = decoder.decode_page(
            first, report_envelope(page_no=1, per_page=2, total=3)
        )
        next_spec = continued.advance.next_spec
        assert next_spec is not None
        assert next_spec.params == {
            'start_date': '2026-01-05',
            'end_date': '2026-01-05',
            'page_no': '2',
            'per_page': '2',
        }
        assert continued.advance.durable_progress is None
        terminal = decoder.decode_page(
            next_spec, report_envelope(page_no=2, per_page=2, total=3)
        )
        assert terminal.advance.next_spec is None
        assert terminal.advance.durable_progress is None
        assert terminal.records[0]['windowStartDate'] == '2026-01-05'
