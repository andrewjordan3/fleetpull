"""Runtime tests for the public ``fetch`` verb, faked at the httpx boundary.

``httpx.MockTransport`` is injected by monkeypatching ``httpx.Client``
(the transport-test seam) so the entire real composition -- registry
discovery, auth ingress, limiter, retry, page decoding, validation,
frame construction -- runs under every test with no live network
anywhere. Responses use each provider's real wire shape: Motive's
``{"vehicles": [{"vehicle": {...}}], "pagination": {...}}`` envelopes
(synthetic identifiers), GeoTab's committed 2026-07-09 capture set
(``tests/geotab_devices_capture.py``), and Samsara's committed
2026-07-20 drivers capture set (``tests/samsara_drivers_capture.py``).
"""

import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

import httpx
import polars as pl
import pytest

from fleetpull import (
    AuthenticationError,
    ConfigurationError,
    Endpoints,
    ProviderResponseError,
    fetch,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ParamSweep,
    RequestShape,
    ResumeValue,
    RosterFanOut,
    SingleFetch,
    SnapshotMode,
    StorageKind,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SinglePageDecoder
from fleetpull.roster import RosterKey
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.api.conftest import (
    SYNTHETIC_GEOTAB_PASS,
    SYNTHETIC_MOTIVE_KEY,
    SYNTHETIC_SAMSARA_TOKEN,
    Handler,
    install_transport,
    vehicle_record,
)
from tests.geotab_devices_capture import (
    AUTHENTICATE_SUCCESS_JSON,
    SEEK_PAGE_1_RESPONSE,
    SEEK_PAGE_2_RESPONSE,
    SEEK_TERMINAL_RESPONSE,
)
from tests.samsara_drivers_capture import DRIVER_RECORDS


def _paged_vehicles_handler(total_pages: int) -> Handler:
    """One vehicle per page; the pagination echo drives the page loop."""

    def handler(request: httpx.Request) -> httpx.Response:
        page_no = int(request.url.params['page_no'])
        return httpx.Response(
            200,
            json={
                'vehicles': [{'vehicle': vehicle_record(page_no)}],
                'pagination': {'page_no': page_no, 'per_page': 1, 'total': total_pages},
            },
        )

    return handler


def _empty_vehicles_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            'vehicles': [],
            'pagination': {'page_no': 1, 'per_page': 100, 'total': 0},
        },
    )


def test_happy_path_spans_pages_and_types_the_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_transport(monkeypatch, _paged_vehicles_handler(total_pages=3))
    frame = fetch(Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY)
    assert frame.height == 3
    assert frame['vehicle_id'].to_list() == [1, 2, 3]
    assert frame.schema['vehicle_id'] == pl.Int64
    assert frame.schema['created_at'] == pl.Datetime(time_unit='us', time_zone='UTC')


def test_credential_header_reaches_the_wire(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_headers: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers['X-API-Key'])
        return _paged_vehicles_handler(total_pages=1)(request)

    install_transport(monkeypatch, handler)
    fetch(Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY)
    assert seen_headers == [SYNTHETIC_MOTIVE_KEY]


def test_empty_listing_yields_zero_rows_with_the_full_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_transport(monkeypatch, _paged_vehicles_handler(total_pages=1))
    populated_schema = fetch(
        Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY
    ).schema

    install_transport(monkeypatch, _empty_vehicles_handler)
    empty_frame = fetch(Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY)
    assert empty_frame.height == 0
    assert empty_frame.schema == populated_schema


def test_auth_provider_mismatch_makes_no_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_http_expected(request: httpx.Request) -> httpx.Response:
        raise AssertionError('the mismatch must be rejected before any request')

    install_transport(monkeypatch, no_http_expected)
    with pytest.raises(ConfigurationError) as raised:
        fetch(Endpoints.Motive.vehicles, auth={'api_key': SYNTHETIC_MOTIVE_KEY})
    assert 'bare API-key string' in str(raised.value)


def test_success_status_with_non_json_body_is_a_provider_response_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A TLS-intercepting proxy's block page: HTTP 200, HTML body.
    def block_page(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text='<html><body>Access blocked by proxy</body></html>',
            headers={'content-type': 'text/html'},
        )

    install_transport(monkeypatch, block_page)
    with pytest.raises(ProviderResponseError):
        fetch(Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY)


def test_no_raise_path_ever_carries_the_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 401 with Motive's observed body: AuthenticationError whose message and
    # repr must carry provider text only, never the credential fetch was given.
    def rejects_credentials(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={'error_message': 'invalid API key'})

    install_transport(monkeypatch, rejects_credentials)
    with pytest.raises(AuthenticationError) as raised:
        fetch(Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY)
    assert SYNTHETIC_MOTIVE_KEY not in str(raised.value)
    assert SYNTHETIC_MOTIVE_KEY not in repr(raised.value)


class _GeotabHandler:
    """One JSON-RPC route: Authenticate, seek Get pages in order, GetCountOf.

    Records every Authenticate body and every data call's injected
    ``params.credentials`` so the e2e test can prove the session stack
    composed: one Authenticate, session credentials on every data call.
    """

    def __init__(self) -> None:
        self.authenticate_bodies: list[JsonObject] = []
        self.data_credentials: list[JsonObject] = []
        self._pages = iter(
            [SEEK_PAGE_1_RESPONSE, SEEK_PAGE_2_RESPONSE, SEEK_TERMINAL_RESPONSE]
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if body['method'] == 'Authenticate':
            self.authenticate_bodies.append(body)
            return httpx.Response(200, text=AUTHENTICATE_SUCCESS_JSON)
        self.data_credentials.append(body['params']['credentials'])
        if body['method'] == 'GetCountOf':
            # CONSTRUCTED: the count matching the two committed pages (the
            # captured envelope carries the live fleet's 5,666).
            return httpx.Response(200, json={'result': 6, 'jsonrpc': '2.0'})
        assert body['method'] == 'Get'
        return httpx.Response(200, json=next(self._pages))


def test_geotab_devices_end_to_end_through_the_session_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The first live-shaped proof of auth stack + transport POST +
    # classifier + seek decoder + completeness guard, end to end (the
    # once-unwired auth stack's composition gap): Authenticate success, two
    # captured Get pages, the empty terminal page, a matching GetCountOf.
    handler = _GeotabHandler()
    install_transport(monkeypatch, handler)
    frame = fetch(
        Endpoints.Geotab.devices,
        auth={
            'username': 'user@example.com',
            'password': SYNTHETIC_GEOTAB_PASS,
            'database': 'exampledb',
        },
    )
    assert frame.height == 6
    assert frame['id'].to_list() == ['b101', 'b102', 'b105', 'b106', 'b107', 'b10A']
    assert frame.schema['active_to'] == pl.Datetime(time_unit='us', time_zone='UTC')
    # Exactly one Authenticate fired for the whole walk.
    assert len(handler.authenticate_bodies) == 1
    # Every data call -- three Get pages and the GetCountOf -- carried the
    # session credentials the strategy injected.
    assert len(handler.data_credentials) == 4
    assert all(
        credentials['sessionId'] == 'SyntheticSessionId000001'
        and credentials['database'] == 'exampledb'
        for credentials in handler.data_credentials
    )


class _SweepModel(ResponseModel):
    id: int
    status: str


@dataclass(frozen=True, slots=True)
class _StatusSweepSpecBuilder:
    """Merge the sweep's member binding as query parameters (a sweep builder)."""

    base_url: str
    path: str

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        return RequestSpec(
            method=HttpMethod.GET,
            url=f'{self.base_url}{self.path}',
            params=dict(member_values),
        )


def _synthetic_definition(shape: RequestShape) -> EndpointDefinition[_SweepModel]:
    """A snapshot definition with the given shape, keyed as Motive vehicles."""
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicles',
        spec_builder=_StatusSweepSpecBuilder(
            base_url='https://x.test', path='/v1/items'
        ),
        page_decoder=SinglePageDecoder(records_key='data'),
        response_model=_SweepModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
        request_shape=shape,
    )


class _StubRegistry:
    """A registry double serving one synthetic definition for any key."""

    def __init__(self, definition: EndpointDefinition[_SweepModel]) -> None:
        self._definition = definition

    def get(self, provider: Provider, name: str) -> EndpointDefinition[_SweepModel]:
        return self._definition


def _install_definition(monkeypatch: pytest.MonkeyPatch, shape: RequestShape) -> None:
    """Route fetch's registry lookup to a synthetic shaped definition."""
    fetch_module = importlib.import_module('fleetpull.api.fetch')
    registry = _StubRegistry(_synthetic_definition(shape))

    # typing-justified: a monkeypatched stand-in mirrors the factory's shape
    def stub_build_registry(provider_configs: object) -> _StubRegistry:
        return registry

    monkeypatch.setattr(fetch_module, 'build_endpoint_registry', stub_build_registry)


def test_param_sweep_endpoint_is_served_and_unions_the_sweeps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # fetch serves every stateless request shape through the shared seam:
    # a snapshot ParamSweep fans one chain per declared value and the
    # returned frame is the union of the sweeps.
    def sweep_handler(request: httpx.Request) -> httpx.Response:
        status = request.url.params['status']
        record_id = {'active': 1, 'deactivated': 2}[status]
        return httpx.Response(200, json={'data': [{'id': record_id, 'status': status}]})

    install_transport(monkeypatch, sweep_handler)
    _install_definition(
        monkeypatch, ParamSweep(param='status', values=('active', 'deactivated'))
    )
    frame = fetch(Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY)
    assert frame.height == 2
    assert sorted(frame['status'].to_list()) == ['active', 'deactivated']
    assert sorted(frame['id'].to_list()) == [1, 2]


def test_single_fetch_shape_still_serves_through_the_seam(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def single_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={'data': [{'id': 9, 'status': 'active'}]})

    install_transport(monkeypatch, single_handler)
    _install_definition(monkeypatch, SingleFetch())
    frame = fetch(Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY)
    assert frame['id'].to_list() == [9]


def test_roster_fan_out_endpoint_is_refused_before_any_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # fetch's in-memory contract: no roster state, so a RosterFanOut shape
    # is refused loudly by the shared seam -- never a silent partial fetch.
    def no_http_expected(request: httpx.Request) -> httpx.Response:
        raise AssertionError('the refusal must fire before any request')

    install_transport(monkeypatch, no_http_expected)
    _install_definition(
        monkeypatch,
        RosterFanOut(
            roster=RosterKey(Provider.MOTIVE, 'vehicle_ids'),
            member_key='vehicle_id',
        ),
    )
    with pytest.raises(ConfigurationError, match='no roster source') as raised:
        fetch(Endpoints.Motive.vehicles, auth=SYNTHETIC_MOTIVE_KEY)
    assert 'sync' in str(raised.value)


def test_samsara_drivers_serves_the_two_sweep_snapshot_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The first REAL ParamSweep endpoint through fetch: the Samsara
    # drivers binding fans one cursor chain per activation status, both
    # statuses reach the wire, and the frame is the union of the sweeps
    # (the two-sweep completeness story, captured 2026-07-20).
    statuses_requested: list[str] = []
    bearer_headers: list[str] = []

    def drivers_handler(request: httpx.Request) -> httpx.Response:
        status = request.url.params['driverActivationStatus']
        statuses_requested.append(status)
        bearer_headers.append(request.headers['Authorization'])
        sweep_records = [
            record
            for record in DRIVER_RECORDS
            if record['driverActivationStatus'] == status
        ]
        return httpx.Response(
            200,
            json={
                'data': sweep_records,
                'pagination': {'endCursor': '', 'hasNextPage': False},
            },
        )

    install_transport(monkeypatch, drivers_handler)
    frame = fetch(Endpoints.Samsara.drivers, auth=SYNTHETIC_SAMSARA_TOKEN)
    # Both sweeps fired (chains may run concurrently; order-free check)
    # and every request carried the bearer credential.
    assert sorted(statuses_requested) == ['active', 'deactivated']
    assert set(bearer_headers) == {f'Bearer {SYNTHETIC_SAMSARA_TOKEN}'}
    # The frame is the union, in member order (active chain first).
    assert frame.height == 3
    assert frame['driver_activation_status'].to_list() == [
        'active',
        'active',
        'deactivated',
    ]
    assert frame.schema['driver_activation_status'] == pl.String
    assert frame['carrier_settings__dot_number'].to_list() == [100001] * 3
    assert frame.schema['carrier_settings__dot_number'] == pl.Int64


def test_geotab_fetch_failure_never_carries_the_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def rejects_everything(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text='no route')

    install_transport(monkeypatch, rejects_everything)
    with pytest.raises(ProviderResponseError) as raised:
        fetch(
            Endpoints.Geotab.devices,
            auth={
                'username': 'user@example.com',
                'password': SYNTHETIC_GEOTAB_PASS,
                'database': 'exampledb',
            },
        )
    assert SYNTHETIC_GEOTAB_PASS not in str(raised.value)
    assert SYNTHETIC_GEOTAB_PASS not in repr(raised.value)
