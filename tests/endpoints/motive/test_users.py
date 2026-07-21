"""Tests for fleetpull.endpoints.motive.users.

The binding is the vehicles template verbatim: the shared static-GET
builder and the existing Motive wrapped-list decoder at the configured
page size (``per_page`` 50 and 100 both honored live, captured
2026-07-21). One dataset despite the role-partitioned record shape —
the role column carries the split, so no sweep and no second endpoint
(DESIGN section 8). The walk test at the bottom drives the real decoder
over the capture set's two-page fixture walk.
"""

from fleetpull.config import MotiveConfig
from fleetpull.endpoints.motive.users import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.models.motive import User
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import MotiveWrappedListPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope
from tests.motive_users_capture import (
    USERS_PAGE_1_RESPONSE,
    USERS_PAGE_2_RESPONSE,
)


def _build_endpoint() -> EndpointDefinition[User]:
    return build_endpoint(MotiveConfig(base_url='https://api.example.test'))


class TestBuildUsersEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.MOTIVE
        assert endpoint.name == 'users'
        assert endpoint.response_model is User
        assert endpoint.quota_scope is QuotaScope.MOTIVE
        assert endpoint.storage_kind is StorageKind.SINGLE
        assert isinstance(endpoint.sync_mode, SnapshotMode)
        assert endpoint.request_shape == SingleFetch()
        assert endpoint.completeness_check is None

    def test_uses_the_motive_wrapped_list_decoder(self) -> None:
        decoder = _build_endpoint().page_decoder
        assert isinstance(decoder, MotiveWrappedListPageDecoder)
        assert decoder.list_key == 'users'
        assert decoder.item_key == 'user'

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
        assert spec.url == 'https://api.example.test/v1/users'

    def test_base_url_default_flows_through(self) -> None:
        endpoint = build_endpoint(MotiveConfig())
        spec = endpoint.spec_builder.build_spec(resume=None, member_values={})
        assert spec.url == 'https://api.gomotive.com/v1/users'


class TestTwoPageWalk:
    """The real decoder over the capture set's two-page fixture walk."""

    def test_the_offset_advance_reaches_the_terminal(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        first = decoder.first_request(
            endpoint.spec_builder.build_spec(resume=None, member_values={})
        )
        assert first.params == {'page_no': '1', 'per_page': '100'}
        page_one = decoder.decode_page(first, USERS_PAGE_1_RESPONSE)
        assert len(page_one.records) == 2
        follow_up = page_one.advance.next_spec
        assert follow_up is not None
        # The advance echoes the SERVER's page size (the fixture's 2),
        # never the sent per_page.
        assert follow_up.params == {'page_no': '2', 'per_page': '2'}
        terminal = decoder.decode_page(follow_up, USERS_PAGE_2_RESPONSE)
        assert len(terminal.records) == 2
        assert terminal.advance.next_spec is None

    def test_the_walk_unwraps_the_item_key(self) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        first = decoder.first_request(
            endpoint.spec_builder.build_spec(resume=None, member_values={})
        )
        page_one = decoder.decode_page(first, USERS_PAGE_1_RESPONSE)
        assert [record['id'] for record in page_one.records] == [800001, 800002]
