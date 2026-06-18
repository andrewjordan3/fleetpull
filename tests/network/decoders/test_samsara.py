"""Tests for fleetpull.network.decoders.samsara.

Fixtures are synthetic, constructed in the verified envelope shape
with the API's real camelCase keys — the alias handling is part of
what is under test.
"""

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import PageDecoder
from fleetpull.network.contract.request import HttpMethod, JsonValue, RequestSpec
from fleetpull.network.decoders import SamsaraCursorPageDecoder


def build_spec() -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.GET,
        url='https://api.example.com/fleet/vehicles/stats',
        params={'types': 'gps'},
    )


class TestSamsaraCursorPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = SamsaraCursorPageDecoder(records_key='data')
        assert isinstance(decoder, SamsaraCursorPageDecoder)

    def test_first_request_is_identity_without_after(self) -> None:
        spec = build_spec()
        prepared = SamsaraCursorPageDecoder(records_key='data').first_request(spec)
        assert prepared is spec
        assert prepared.params is not None
        assert 'after' not in prepared.params

    def test_cursor_advance_merges_after(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True, 'endCursor': 'cursor-0001'},
            'data': [{'id': 'a'}],
        }
        decoded = SamsaraCursorPageDecoder(records_key='data').decode_page(
            build_spec(), envelope
        )
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
        decoded = SamsaraCursorPageDecoder(records_key='data').decode_page(
            build_spec(), envelope
        )
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress is None
        assert decoded.records == [{'id': 'a'}]

    def test_continuation_without_cursor_raises_truncation_guard(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True},
            'data': [{'id': 'a'}],
        }
        with pytest.raises(ProviderResponseError, match='endCursor'):
            SamsaraCursorPageDecoder(records_key='data').decode_page(
                build_spec(), envelope
            )

    def test_continuation_with_empty_cursor_raises_truncation_guard(self) -> None:
        envelope: dict[str, JsonValue] = {
            'pagination': {'hasNextPage': True, 'endCursor': ''},
            'data': [{'id': 'a'}],
        }
        with pytest.raises(ProviderResponseError, match='endCursor'):
            SamsaraCursorPageDecoder(records_key='data').decode_page(
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
            SamsaraCursorPageDecoder(records_key='data').decode_page(
                build_spec(), envelope
            )
