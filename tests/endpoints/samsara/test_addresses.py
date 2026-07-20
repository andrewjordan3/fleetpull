"""Tests for fleetpull.endpoints.samsara.addresses.

The binding is the vehicles template verbatim: the shared snapshot
spec-builder and the cursor decoder, at the 512 limit tier probed
directly on this endpoint (limit=512 HTTP 200, limit=513 HTTP 400,
captured 2026-07-20 -- the vehicles/drivers tier, not idling's 200). No
completeness check is declared because the cursor walk is complete by
construction (continuation is explicit per page, and the decoder fails
loudly on a promised continuation without a cursor); no roster is
sourced or consumed. The walk test at the bottom drives the real
decoder over the capture set's two-page fixture walk.
"""

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara.addresses import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.samsara import Address
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.samsara_addresses_capture import (
    ADDRESSES_PAGE_RESPONSE,
    ADDRESSES_TERMINAL_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[Address]:
    return build_endpoint(SamsaraConfig())


class TestAddressesSpecBuilder:
    def test_builds_the_static_get(self) -> None:
        endpoint = _build_endpoint()
        assert isinstance(endpoint.spec_builder, StaticGetSpecBuilder)
        spec = endpoint.spec_builder.build_spec(resume=None, member_values={})
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/addresses'

    def test_configured_base_url_is_used(self) -> None:
        config = SamsaraConfig(base_url='https://alt.example.test/')
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=None, member_values={}
        )
        # The config strips the trailing slash so the path joins cleanly.
        assert spec.url == 'https://alt.example.test/addresses'


class TestBuildAddressesEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'addresses'
        assert endpoint.quota_scope is QuotaScope.SAMSARA
        assert endpoint.storage_kind is StorageKind.SINGLE
        assert endpoint.response_model is Address
        assert isinstance(endpoint.sync_mode, SnapshotMode)
        assert endpoint.request_shape == SingleFetch()
        assert endpoint.completeness_check is None

    def test_the_decoder_is_the_cursor_walk_at_the_probed_tier(self) -> None:
        # 512 is THIS endpoint's probed maximum (513 -> HTTP 400,
        # captured 2026-07-20); limit tiers are per-endpoint, never
        # assumed from a sibling (the idling_events 200-tier lesson).
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        assert isinstance(decoder, SamsaraCursorPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.results_limit == 512


class TestTwoPageWalk:
    """The real decoder over the capture set's two-page fixture walk."""

    def test_the_cursor_advance_reaches_the_terminal(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        first = decoder.first_request(
            endpoint.spec_builder.build_spec(resume=None, member_values={})
        )
        assert first.params == {'limit': '512'}
        page_one = decoder.decode_page(first, ADDRESSES_PAGE_RESPONSE)
        assert len(page_one.records) == 3
        follow_up = page_one.advance.next_spec
        assert follow_up is not None
        # The after-advance merges onto the sent spec, limit intact.
        assert follow_up.params == {
            'limit': '512',
            'after': '00000000-0000-0000-0000-000000000031',
        }
        terminal = decoder.decode_page(follow_up, ADDRESSES_TERMINAL_RESPONSE)
        assert len(terminal.records) == 1
        assert terminal.advance.next_spec is None
