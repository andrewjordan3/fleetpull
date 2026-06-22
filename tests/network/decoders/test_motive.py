"""Tests for fleetpull.network.decoders.motive.

Fixtures are synthetic, constructed in the verified envelope shape: a
per-endpoint top-level wrapper list plus the ``pagination`` block.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import PageDecoder
from fleetpull.network.contract.request import HttpMethod, JsonValue, RequestSpec
from fleetpull.network.decoders import (
    MotiveWrappedListPageDecoder,
    MotiveWrappedSinglePageDecoder,
)


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
