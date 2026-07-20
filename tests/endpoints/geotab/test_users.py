"""Tests for fleetpull.endpoints.geotab.users.

The sort assertion is the load-bearing positive here: id-sort seek
paging is supported for ``User`` (proven live 2026-07-16, never assumed
from Device -- ExceptionEvent rejects the same composition outright),
and the first-page shape (``sortBy: id``, ascending, an EXPLICIT null
``offset``) is the probed one the decoder advances from.
"""

from fleetpull.config import GeotabAuthConfig, GeotabConfig
from fleetpull.endpoints.geotab._get_requests import (
    GeotabGetSpecBuilder,
    GetCountOfCheck,
)
from fleetpull.endpoints.geotab.users import build_endpoint
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SingleFetch,
    SnapshotMode,
    StorageKind,
)
from fleetpull.models.geotab import User
from fleetpull.network.contract import HttpMethod
from fleetpull.network.decoders import GeotabGetPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope


def _build_endpoint() -> EndpointDefinition[User]:
    return build_endpoint(GeotabConfig())


class TestUsersSpecBuilder:
    def test_builds_the_probed_first_page_shape(self) -> None:
        endpoint = _build_endpoint()
        assert isinstance(endpoint.spec_builder, GeotabGetSpecBuilder)
        spec = endpoint.spec_builder.build_spec(resume=None, member_values={})
        assert spec.method is HttpMethod.POST
        assert spec.url == 'https://my.geotab.com/apiv1'
        assert isinstance(spec.json_body, dict)
        assert spec.json_body['method'] == 'Get'
        params = spec.json_body['params']
        assert isinstance(params, dict)
        assert params['typeName'] == 'User'
        assert params['resultsLimit'] == 5000
        # The probed shape: an EXPLICIT null offset, never an absent key.
        assert params['sort'] == {
            'sortBy': 'id',
            'sortDirection': 'asc',
            'offset': None,
        }

    def test_credentials_are_never_written_here(self) -> None:
        # The session strategy injects params.credentials; the builder
        # must leave the slot untouched.
        spec = _build_endpoint().spec_builder.build_spec(resume=None, member_values={})
        assert isinstance(spec.json_body, dict)
        params = spec.json_body['params']
        assert isinstance(params, dict)
        assert 'credentials' not in params

    def test_configured_auth_server_is_used(self) -> None:
        config = GeotabConfig(
            auth=GeotabAuthConfig(
                username='user@example.com',
                password='synthetic-password-123',
                database='synthetic_db',
                server='alt.example.test',
            )
        )
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=None, member_values={}
        )
        assert spec.url == 'https://alt.example.test/apiv1'


class TestBuildUsersEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.GEOTAB
        assert endpoint.name == 'users'
        assert endpoint.quota_scope is QuotaScope.GEOTAB_GET
        assert endpoint.storage_kind is StorageKind.SINGLE
        assert endpoint.response_model is User
        assert isinstance(endpoint.sync_mode, SnapshotMode)
        assert endpoint.request_shape == SingleFetch()

    def test_the_decoder_is_the_seek_walk_decoder(self) -> None:
        endpoint = _build_endpoint()
        assert isinstance(endpoint.page_decoder, GeotabGetPageDecoder)

    def test_the_completeness_check_counts_users(self) -> None:
        endpoint = _build_endpoint()
        check = endpoint.completeness_check
        assert isinstance(check, GetCountOfCheck)
        assert check.type_name == 'User'
        assert check.server == 'my.geotab.com'

    def test_the_check_follows_the_configured_server(self) -> None:
        config = GeotabConfig(
            auth=GeotabAuthConfig(
                username='user@example.com',
                password='synthetic-password-123',
                database='synthetic_db',
                server='alt.example.test',
            )
        )
        check = build_endpoint(config).completeness_check
        assert isinstance(check, GetCountOfCheck)
        assert check.server == 'alt.example.test'
