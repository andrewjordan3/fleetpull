"""Tests for fleetpull.network.decoders.single_page.

Fixtures are synthetic, in the verified shape: a top-level record list
under the configured key, with no pagination metadata. Unlike the
paginator it ports, the decoder validates the record list, so a
missing key now raises rather than silently completing.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import PageDecoder
from fleetpull.network.contract.request import HttpMethod, JsonValue, RequestSpec
from fleetpull.network.decoders import SinglePageDecoder


def build_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/v3/vehicle_locations',
        params={'date': '2026-06-01'},
    )


class TestSinglePageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = SinglePageDecoder(records_key='items')
        assert isinstance(decoder, SinglePageDecoder)

    def test_first_request_is_identity(self) -> None:
        spec = build_spec()
        assert SinglePageDecoder(records_key='items').first_request(spec) is spec

    def test_extracts_records_and_completes(self) -> None:
        envelope: dict[str, JsonValue] = {'items': [{'id': 1}]}
        decoded = SinglePageDecoder(records_key='items').decode_page(
            build_spec(), envelope
        )
        assert decoded.records == [{'id': 1}]
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress is None

    def test_missing_record_key_raises(self) -> None:
        with pytest.raises(ProviderResponseError, match='missing the record key'):
            SinglePageDecoder(records_key='items').decode_page(
                build_spec(), {'other': []}
            )
