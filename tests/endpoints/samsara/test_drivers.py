"""Tests for fleetpull.endpoints.samsara.drivers.

The binding declares the first ``ParamSweep`` (the two-sweep
activation-status listing, probe-settled 2026-07-20): its leaf builder
merges the sweep's member binding verbatim as query parameters, the
existing cursor decoder is reused unchanged (the ``after`` advance
merges onto the sent spec, so the status parameter persists across the
walk), and no completeness check is declared -- continuation is
explicit per page and the sweep vocabulary is API-enforced (any other
value is a loud HTTP 400).

The sweep-chain test at the bottom drives the whole seam: the real
definition through ``resolve_request_driver``, the resolved member-
agnostic ``FanOutRequestDriver``, and the real cursor decoder against a
canned paging client, proving each chain's status parameter rides every
page and no cursor leaks across chains.
"""

from collections.abc import Iterator

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.samsara.drivers import (
    SamsaraDriversSpecBuilder,
    build_endpoint,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ParamSweep,
    SnapshotMode,
    StorageKind,
)
from fleetpull.models.samsara import Driver
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import HttpMethod, PageDecoder, RequestSpec
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.orchestrator.drivers import FanOutRequestDriver
from fleetpull.orchestrator.fanout import FetchPool
from fleetpull.orchestrator.shape_resolution import resolve_request_driver
from fleetpull.vocabulary import JsonValue, Provider, QuotaScope
from tests.orchestrator.serial_executor import SerialExecutor
from tests.samsara_drivers_capture import (
    DRIVERS_PAGE_RESPONSE,
    DRIVERS_TERMINAL_RESPONSE,
)

_STATUS_PARAM = 'driverActivationStatus'


def _build_endpoint() -> EndpointDefinition[Driver]:
    return build_endpoint(SamsaraConfig())


class TestDriversSpecBuilder:
    def test_builds_the_get_with_the_member_binding_as_params(self) -> None:
        endpoint = _build_endpoint()
        assert isinstance(endpoint.spec_builder, SamsaraDriversSpecBuilder)
        spec = endpoint.spec_builder.build_spec(
            resume=None, member_values={_STATUS_PARAM: 'active'}
        )
        assert spec.method is HttpMethod.GET
        assert spec.url == 'https://api.samsara.com/fleet/drivers'
        # The member binding merges verbatim: the sweep's member key IS
        # the wire query parameter.
        assert spec.params == {_STATUS_PARAM: 'active'}

    def test_configured_base_url_is_used(self) -> None:
        config = SamsaraConfig(base_url='https://alt.example.test/')
        spec = build_endpoint(config).spec_builder.build_spec(
            resume=None, member_values={_STATUS_PARAM: 'deactivated'}
        )
        # The config strips the trailing slash so the path joins cleanly.
        assert spec.url == 'https://alt.example.test/fleet/drivers'
        assert spec.params == {_STATUS_PARAM: 'deactivated'}


class TestBuildDriversEndpoint:
    def test_binds_the_static_facts(self) -> None:
        endpoint = _build_endpoint()
        assert endpoint.provider is Provider.SAMSARA
        assert endpoint.name == 'drivers'
        assert endpoint.quota_scope is QuotaScope.SAMSARA
        assert endpoint.storage_kind is StorageKind.SINGLE
        assert endpoint.response_model is Driver
        assert isinstance(endpoint.sync_mode, SnapshotMode)
        assert endpoint.completeness_check is None

    def test_declares_the_two_value_param_sweep(self) -> None:
        # The wire param exactly, active first: the union of the two
        # sweeps is the one complete dataset (the default listing is
        # only the active set -- captured 2026-07-20).
        endpoint = _build_endpoint()
        assert endpoint.request_shape == ParamSweep(
            param=_STATUS_PARAM, values=('active', 'deactivated')
        )

    def test_the_decoder_is_the_unchanged_cursor_walk_at_the_documented_max(
        self,
    ) -> None:
        endpoint = _build_endpoint()
        decoder = endpoint.page_decoder
        assert isinstance(decoder, SamsaraCursorPageDecoder)
        assert decoder.records_key == 'data'
        assert decoder.results_limit == 512


class _SerialPoolSource:
    """A FetchPoolSource handing one synchronous same-thread pool."""

    def __init__(self) -> None:
        self.pool = FetchPool(executor=SerialExecutor(), submission_window=2)

    def pool_for(self, provider: Provider) -> FetchPool:
        return self.pool


class _SweepWalkClient(TransportClient):
    """Serves canned envelopes per status, running the real decoder loop.

    Replays ``TransportClient.fetch_pages``'s decode loop (first-request
    injection, decode, follow ``next_spec``) over per-status envelope
    scripts, recording every SENT spec's params -- the datum the sweep
    test asserts page by page. Opens no real pool (no
    ``super().__init__``).
    """

    def __init__(self, envelopes_by_status: dict[str, list[JsonValue]]) -> None:
        self._envelopes_by_status = envelopes_by_status
        self.sent_params: list[dict[str, str]] = []

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        sent: RequestSpec | None = page_decoder.first_request(spec)
        assert sent is not None
        assert sent.params is not None
        envelopes = iter(self._envelopes_by_status[sent.params[_STATUS_PARAM]])
        while sent is not None:
            assert sent.params is not None
            self.sent_params.append(dict(sent.params))
            decoded = page_decoder.decode_page(sent, next(envelopes))
            yield FetchedPage(
                records=decoded.records,
                durable_progress=decoded.advance.durable_progress,
            )
            sent = decoded.advance.next_spec


class TestTwoSweepChain:
    """The whole sweep seam end to end: shape resolution -> fan-out
    driver -> real spec builder and cursor decoder -> canned pages."""

    def _run_the_sweep(self) -> tuple[_SweepWalkClient, list[FetchedPage]]:
        definition = _build_endpoint()
        driver = resolve_request_driver(
            definition, fetch_pools=_SerialPoolSource(), roster_members=None
        )
        assert isinstance(driver, FanOutRequestDriver)
        assert driver.members == ('active', 'deactivated')
        assert driver.member_key == _STATUS_PARAM
        active_terminal: JsonValue = {
            'data': [],
            'pagination': {'endCursor': '', 'hasNextPage': False},
        }
        client = _SweepWalkClient(
            {
                'active': [DRIVERS_PAGE_RESPONSE, active_terminal],
                'deactivated': [DRIVERS_TERMINAL_RESPONSE],
            }
        )
        pages = list(driver.record_batches(definition, client, None))
        return client, pages

    def test_the_status_param_rides_every_page_of_its_chain(self) -> None:
        client, _pages = self._run_the_sweep()
        active_page_1, active_page_2, deactivated_page_1 = client.sent_params
        # Chain 1, page 1: limit plus the sweep's status, no cursor.
        assert active_page_1 == {'limit': '512', _STATUS_PARAM: 'active'}
        # Chain 1, page 2: the after-advance merges onto the sent spec,
        # so the status (and limit) persist beside the cursor.
        assert active_page_2 == {
            'limit': '512',
            _STATUS_PARAM: 'active',
            'after': '00000000-0000-0000-0000-000000000021',
        }
        # Chain 2, page 1: the deactivated sweep starts fresh -- no
        # `after` leaked from chain 1's terminal.
        assert deactivated_page_1 == {'limit': '512', _STATUS_PARAM: 'deactivated'}

    def test_records_from_both_sweeps_land_active_then_deactivated(self) -> None:
        _client, pages = self._run_the_sweep()
        statuses_in_stream = [
            record['driverActivationStatus']
            for page in pages
            for record in page.records
        ]
        assert statuses_in_stream == ['active', 'active', 'deactivated']
