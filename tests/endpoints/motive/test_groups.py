"""Tests for fleetpull.endpoints.motive.groups.

The binding is the vehicles template verbatim: the shared static-GET
builder and the existing Motive wrapped-list decoder at the configured
page size (``per_page`` 50 and 100 both honored live, captured
2026-07-21). The walk test at the bottom drives the real decoder over
the capture set's two-page fixture walk.
"""

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive.groups import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.motive import Group
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.motive_groups_capture import (
    GROUPS_PAGE_1_RESPONSE,
    GROUPS_PAGE_2_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[Group]:
    return build_endpoint(MotiveConfig(base_url='https://api.example.test'))


class TestBuildGroupsEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.MOTIVE
        assert endpoint.name == 'groups'
        assert endpoint.response_model is Group
        assert endpoint.quota_scope is QuotaScope.MOTIVE
        assert endpoint.storage_kind is StorageKind.SINGLE
        assert isinstance(endpoint.sync_mode, SnapshotMode)
        assert endpoint.request_shape == SingleFetch()
        assert endpoint.completeness_check is None

    def test_uses_the_motive_wrapped_list_decoder(self) -> None:
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, MotiveWrappedListPageDecoder)
        assert decoder.list_key == 'groups'
        assert decoder.item_key == 'group'

    def test_decoder_page_size_follows_config(self) -> None:
        endpoint = build_endpoint(
            MotiveConfig(base_url='https://api.example.test', records_per_page=50)
        )
        decoder = endpoint.page_decoder
        assert isinstance(decoder, MotiveWrappedListPageDecoder)
        assert decoder.per_page == 50

    def test_spec_builder_joins_config_base_url_to_path(self) -> None:
        endpoint = _build_endpoint()
        assert isinstance(endpoint.spec_builder, StaticGetSpecBuilder)
        spec = endpoint.spec_builder.build_spec(resume=None, member_values={})
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.example.test/v1/groups'

    def test_base_url_default_flows_through(self) -> None:
        endpoint = build_endpoint(MotiveConfig())
        spec = endpoint.spec_builder.build_spec(resume=None, member_values={})
        assert spec.url == 'https://api.gomotive.com/v1/groups'


class TestTwoPageWalk:
    """The real decoder over the capture set's two-page fixture walk."""

    def test_the_offset_advance_reaches_the_terminal(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        first = decoder.first_request(
            endpoint.spec_builder.build_spec(resume=None, member_values={})
        )
        assert first.params == {'page_no': '1', 'per_page': '100'}
        page_one = decoder.decode_page(first, GROUPS_PAGE_1_RESPONSE)
        assert len(page_one.records) == 3
        follow_up = page_one.advance.next_spec
        assert follow_up is not None
        # The advance echoes the SERVER's page size (the fixture's 3),
        # never the sent per_page.
        assert follow_up.params == {'page_no': '2', 'per_page': '3'}
        terminal = decoder.decode_page(follow_up, GROUPS_PAGE_2_RESPONSE)
        assert len(terminal.records) == 2
        assert terminal.advance.next_spec is None

    def test_the_walk_unwraps_the_item_key(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        first = decoder.first_request(
            endpoint.spec_builder.build_spec(resume=None, member_values={})
        )
        page_one = decoder.decode_page(first, GROUPS_PAGE_1_RESPONSE)
        assert [record['id'] for record in page_one.records] == [90001, 90002, 90003]
