"""Tests for fleetpull.api.sync -- the config-driven verb, no live network.

The integration tests run the whole real composition (state DB, stores,
discovered registries, limiter, clients, run executor) against
``httpx.MockTransport`` via the transport-test seam, from a config file
in ``tmp_path``. Wire shapes are each provider's real envelopes --
Motive's synthetic-identifier bodies and GeoTab's committed 2026-07-09
capture set (``tests/geotab_devices_capture.py``).
"""

import json
import sqlite3
import threading
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import ClassVar, NoReturn, cast

import httpx
import polars as pl
import pytest

import fleetpull.api.sync as sync_module
from fleetpull import ConfigurationError, Sync, SyncFailuresError
from fleetpull.endpoints import EndpointRegistry, build_roster_registry
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.exceptions import EndpointFailure, ProviderResponseError
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator import (
    EndpointRunner,
    FetchPoolRegistry,
    RosterMachinery,
)
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.api.conftest import (
    SYNTHETIC_GEOTAB_PASS,
    SYNTHETIC_MOTIVE_KEY,
    SYNTHETIC_SAMSARA_TOKEN,
    install_transport,
    vehicle_record,
)
from tests.geotab_devices_capture import (
    AUTHENTICATE_SUCCESS_JSON,
    SEEK_PAGE_1_RESPONSE,
    SEEK_PAGE_2_RESPONSE,
    SEEK_TERMINAL_RESPONSE,
)
from tests.orchestrator.doubles import StubPageDecoder


@pytest.fixture(autouse=True)
def _no_ambient_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip the credential variables so a developer's shell never leaks in."""
    monkeypatch.delenv('MOTIVE_API_KEY', raising=False)
    monkeypatch.delenv('GEOTAB_PASSWORD', raising=False)
    monkeypatch.delenv('SAMSARA_API_KEY', raising=False)


def _write_config(
    tmp_path: Path, *, endpoints: str = '[vehicles, vehicle_locations]', extra: str = ''
) -> Path:
    config_path = tmp_path / 'config.yaml'
    # The rate limit is deliberately generous: the fixed default_start_date
    # against the real clock plans one work unit per elapsed week, so the
    # fan-out's request count grows over calendar time -- the default burst
    # of 10 would make these tests sleep on the real token bucket. Only
    # max_concurrency stays at the default 2: the overlap barrier depends
    # on exactly two workers.
    config_path.write_text(
        'sync:\n'
        '  default_start_date: 2026-06-01\n'
        'storage:\n'
        f'  dataset_root: {tmp_path / "data"}\n'
        f'{extra}'
        'providers:\n'
        '  motive:\n'
        f"    api_key: '{SYNTHETIC_MOTIVE_KEY}'\n"
        f'    endpoints: {endpoints}\n'
        '    rate_limit:\n'
        '      requests_per_period: 6000\n'
        '      period_seconds: 60.0\n'
        '      burst: 1000\n'
        '      max_concurrency: 2\n',
        encoding='utf-8',
    )
    return config_path


def _write_geotab_config(
    tmp_path: Path,
    *,
    endpoints: str = '[devices]',
    include_motive: bool = False,
    motive_endpoints: str = '[vehicles]',
) -> Path:
    """A geotab-enabled config; optionally with the standard Motive block."""
    config_path = tmp_path / 'config.yaml'
    motive_block = (
        '  motive:\n'
        f"    api_key: '{SYNTHETIC_MOTIVE_KEY}'\n"
        f'    endpoints: {motive_endpoints}\n'
        '    rate_limit:\n'
        '      requests_per_period: 6000\n'
        '      period_seconds: 60.0\n'
        '      burst: 1000\n'
        '      max_concurrency: 2\n'
        if include_motive
        else ''
    )
    config_path.write_text(
        'sync:\n'
        '  default_start_date: 2026-06-01\n'
        'storage:\n'
        f'  dataset_root: {tmp_path / "data"}\n'
        'providers:\n'
        f'{motive_block}'
        '  geotab:\n'
        '    auth:\n'
        '      username: user@example.com\n'
        f"      password: '{SYNTHETIC_GEOTAB_PASS}'\n"
        '      database: exampledb\n'
        f'    endpoints: {endpoints}\n',
        encoding='utf-8',
    )
    return config_path


class _GeotabRpcHandler:
    """The GeoTab JSON-RPC route for sync runs.

    Serves the captured Authenticate success, the committed seek pages
    (cycling every three ``Get`` calls, so any number of harvests is
    servable), and a ``GetCountOf`` count the test scripts.
    """

    def __init__(self, count: int = 6) -> None:
        self._count = count
        self._get_calls = 0
        self.credentials_seen: list[JsonObject] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if body['method'] == 'Authenticate':
            return httpx.Response(200, text=AUTHENTICATE_SUCCESS_JSON)
        self.credentials_seen.append(body['params']['credentials'])
        if body['method'] == 'GetCountOf':
            return httpx.Response(200, json={'result': self._count, 'jsonrpc': '2.0'})
        pages = [SEEK_PAGE_1_RESPONSE, SEEK_PAGE_2_RESPONSE, SEEK_TERMINAL_RESPONSE]
        page = pages[self._get_calls % len(pages)]
        self._get_calls += 1
        return httpx.Response(200, json=page)


def _vehicles_response(*vehicle_ids: int) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            'vehicles': [{'vehicle': vehicle_record(v)} for v in vehicle_ids],
            'pagination': {'page_no': 1, 'per_page': 100, 'total': len(vehicle_ids)},
        },
    )


def _locations_response(vehicle_id: str) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            'vehicle_locations': [
                {
                    'vehicle_location': {
                        'id': f'00000000-0000-4000-8000-00000000000{vehicle_id}',
                        'located_at': '2026-06-02T12:00:00Z',
                        'lat': 41.85,
                        'lon': -87.65,
                        'type': 'breadcrumb',
                    }
                }
            ]
        },
    )


def _happy_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == '/v1/vehicles':
        return _vehicles_response(1, 2)
    if request.url.path.startswith('/v3/vehicle_locations/'):
        return _locations_response(request.url.path.rsplit('/', 1)[1])
    return httpx.Response(404, text='no route')


def _ledger_rows(dataset_root: Path) -> list[tuple[str, str, str]]:
    connection = sqlite3.connect(dataset_root / '.fleetpull' / 'state.sqlite3')
    try:
        return connection.execute(
            'SELECT endpoint, mode, status FROM runs ORDER BY run_id'
        ).fetchall()
    finally:
        connection.close()


class TestConstruction:
    def test_unknown_endpoint_name_names_provider_name_and_valid_set(
        self, tmp_path: Path
    ) -> None:
        config_path = _write_config(tmp_path, endpoints='[vehiclez]')
        with pytest.raises(ConfigurationError) as raised:
            Sync(config_path)
        message = str(raised.value)
        assert 'motive' in message
        assert 'vehiclez' in message
        assert 'vehicle_locations, vehicles' in message

    def test_unknown_samsara_endpoint_names_the_samsara_valid_set(
        self, tmp_path: Path
    ) -> None:
        # Selection is validated per provider against the catalog; an
        # unknown Samsara name fails loudly, never skips silently.
        config_path = tmp_path / 'config.yaml'
        config_path.write_text(
            'sync:\n  default_start_date: 2026-06-01\n'
            f'storage:\n  dataset_root: {tmp_path / "data"}\n'
            'providers:\n'
            '  samsara:\n'
            f"    api_key: '{SYNTHETIC_SAMSARA_TOKEN}'\n"
            '    endpoints: [vehiclez]\n'
        )
        with pytest.raises(ConfigurationError) as raised:
            Sync(config_path)
        message = str(raised.value)
        assert 'samsara' in message
        assert 'vehiclez' in message
        assert 'vehicles' in message

    def test_zero_enabled_providers_is_an_error(self, tmp_path: Path) -> None:
        config_path = tmp_path / 'config.yaml'
        config_path.write_text(
            'sync:\n  default_start_date: 2026-06-01\n'
            f'storage:\n  dataset_root: {tmp_path / "data"}\n'
            'providers: {}\n'
        )
        with pytest.raises(ConfigurationError, match='nothing to sync'):
            Sync(config_path)

    def test_from_yaml_errors_propagate(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigurationError, match='config file not found'):
            Sync(tmp_path / 'absent.yaml')

    def test_construction_errors_never_leak_the_secret(self, tmp_path: Path) -> None:
        config_path = _write_config(tmp_path, endpoints='[vehiclez]')
        with pytest.raises(ConfigurationError) as raised:
            Sync(config_path)
        assert SYNTHETIC_MOTIVE_KEY not in str(raised.value)
        assert SYNTHETIC_MOTIVE_KEY not in repr(raised.value)


class TestRun:
    def test_end_to_end_writes_parquet_state_and_ledger(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_transport(monkeypatch, _happy_handler)
        Sync(_write_config(tmp_path)).run()
        dataset_root = tmp_path / 'data'
        snapshot = pl.read_parquet(dataset_root / 'motive/vehicles/data.parquet')
        assert snapshot.height == 2
        partition = pl.read_parquet(
            dataset_root / 'motive/vehicle_locations/date=2026-06-02/part.parquet'
        )
        assert partition.height == 2  # one breadcrumb per fanned-out vehicle
        connection = sqlite3.connect(dataset_root / '.fleetpull' / 'state.sqlite3')
        try:
            cursor_rows = connection.execute(
                "SELECT endpoint FROM cursors WHERE provider = 'motive'"
            ).fetchall()
        finally:
            connection.close()
        assert ('vehicle_locations',) in cursor_rows  # the watermark committed
        statuses = {(row[0], row[2]) for row in _ledger_rows(dataset_root)}
        assert ('vehicles', 'succeeded') in statuses
        assert ('vehicle_locations', 'succeeded') in statuses

    def test_feeder_runs_before_its_consumer_regardless_of_config_order(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_transport(monkeypatch, _happy_handler)
        config_path = _write_config(tmp_path, endpoints='[vehicle_locations, vehicles]')
        Sync(config_path).run()
        run_order = [row[0] for row in _ledger_rows(tmp_path / 'data')]
        assert run_order.index('vehicles') < run_order.index('vehicle_locations')

    def test_one_failure_is_isolated_and_aggregated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def locations_fail(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/v1/vehicles':
                return _vehicles_response(1)
            return httpx.Response(404, text='no route')  # FATAL -> public error

        install_transport(monkeypatch, locations_fail)
        with pytest.raises(SyncFailuresError) as raised:
            Sync(_write_config(tmp_path)).run()
        failures = raised.value.failures
        assert [(f.provider, f.endpoint) for f in failures] == [
            ('motive', 'vehicle_locations')
        ]
        assert 'vehicle_locations' in str(raised.value)
        # The sibling committed independently before the aggregate raised.
        snapshot = pl.read_parquet(tmp_path / 'data/motive/vehicles/data.parquet')
        assert snapshot.height == 1

    def test_a_non_fleetpull_error_propagates_raw(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def explode(*args: object, **kwargs: object) -> NoReturn:
            raise RuntimeError('planted bug')

        install_transport(monkeypatch, _happy_handler)
        monkeypatch.setattr(sync_module, 'run_endpoint', explode)
        with pytest.raises(RuntimeError, match='planted bug'):
            Sync(_write_config(tmp_path)).run()

    def test_run_narrates_start_and_finish_on_stderr(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # capsys, not caplog: setup_logger sets propagate=False on the
        # package logger, so records never reach the root logger caplog
        # listens on -- the configured stderr handler is the observable
        # (the tests/logger/test_setup.py confirmation-record precedent).
        install_transport(monkeypatch, _happy_handler)
        Sync(_write_config(tmp_path)).run()
        captured_stderr = capsys.readouterr().err
        assert 'sync started:' in captured_stderr
        assert 'providers=[motive]' in captured_stderr
        assert 'endpoints=2' in captured_stderr
        assert 'motive.vehicles' in captured_stderr
        assert 'motive.vehicle_locations' in captured_stderr
        assert str(tmp_path / 'data') in captured_stderr
        assert 'sync finished:' in captured_stderr
        assert 'succeeded=2' in captured_stderr
        assert 'failed=0' in captured_stderr
        assert 'elapsed_seconds=' in captured_stderr

    def test_finish_narration_counts_a_failed_endpoint(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # The finish line lands before the failure aggregate raises, with
        # the failed endpoint counted.
        def locations_fail(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/v1/vehicles':
                return _vehicles_response(1)
            return httpx.Response(404, text='no route')

        install_transport(monkeypatch, locations_fail)
        with pytest.raises(SyncFailuresError):
            Sync(_write_config(tmp_path)).run()
        captured_stderr = capsys.readouterr().err
        assert 'sync finished:' in captured_stderr
        assert 'succeeded=1' in captured_stderr
        assert 'failed=1' in captured_stderr

    def test_run_failures_never_leak_the_secret(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def all_fail(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text='no route')

        install_transport(monkeypatch, all_fail)
        with pytest.raises(SyncFailuresError) as raised:
            Sync(_write_config(tmp_path)).run()
        assert SYNTHETIC_MOTIVE_KEY not in str(raised.value)
        assert SYNTHETIC_MOTIVE_KEY not in repr(raised.value)
        assert all(
            SYNTHETIC_MOTIVE_KEY not in repr(failure.error)
            for failure in raised.value.failures
        )


class TestDedupFlagThreading:
    """storage.drop_exact_duplicates threads config -> runner -> compaction."""

    @staticmethod
    def _duplicating_handler(request: httpx.Request) -> httpx.Response:
        # The same vehicle twice: an exact-duplicate row at write time.
        if request.url.path == '/v1/vehicles':
            return _vehicles_response(1, 1)
        return httpx.Response(404, text='no route')

    def test_default_true_drops_the_planted_duplicate(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_transport(monkeypatch, self._duplicating_handler)
        Sync(_write_config(tmp_path, endpoints='[vehicles]')).run()
        snapshot = pl.read_parquet(tmp_path / 'data/motive/vehicles/data.parquet')
        assert snapshot.height == 1

    def test_false_preserves_the_duplicate(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_transport(monkeypatch, self._duplicating_handler)
        config_path = _write_config(
            tmp_path,
            endpoints='[vehicles]',
            extra='  drop_exact_duplicates: false\n',
        )
        Sync(config_path).run()
        snapshot = pl.read_parquet(tmp_path / 'data/motive/vehicles/data.parquet')
        assert snapshot.height == 2


def _fleetpull_worker_threads() -> list[threading.Thread]:
    return [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith('fleetpull-')
    ]


class TestFanOutConcurrency:
    """The per-provider executor, observed through the whole composition."""

    def test_fan_out_overlaps_requests_through_the_real_executor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Real overlap, proven not assumed: every vehicle_locations request
        # parks on a two-party barrier, so the fetch completes only if two
        # requests are in flight at once. A serial fan-out sends one chain
        # at a time -- its lone request would wait until the barrier's
        # timeout converts the deadlock into a loud BrokenBarrierError --
        # so a passing run requires the real per-provider pool (Motive's
        # default max_concurrency of 2 supplies exactly two workers).
        barrier = threading.Barrier(2)

        def overlapping_handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/v1/vehicles':
                return _vehicles_response(1, 2)
            if request.url.path.startswith('/v3/vehicle_locations/'):
                barrier.wait(timeout=10)
                return _locations_response(request.url.path.rsplit('/', 1)[1])
            return httpx.Response(404, text='no route')

        install_transport(monkeypatch, overlapping_handler)
        Sync(_write_config(tmp_path)).run()
        partition = pl.read_parquet(
            tmp_path / 'data/motive/vehicle_locations/date=2026-06-02/part.parquet'
        )
        assert partition.height == 2

    def test_no_worker_threads_outlive_a_successful_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_transport(monkeypatch, _happy_handler)
        Sync(_write_config(tmp_path)).run()
        assert _fleetpull_worker_threads() == []

    def test_no_worker_threads_outlive_a_failed_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def all_fail(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text='no route')

        install_transport(monkeypatch, all_fail)
        with pytest.raises(SyncFailuresError):
            Sync(_write_config(tmp_path)).run()
        assert _fleetpull_worker_threads() == []


class TestGeotabRun:
    """The GeoTab vertical under Sync: config to parquet through the
    session stack, the seek walk, and the completeness guard."""

    def test_geotab_only_config_runs_end_to_end(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler = _GeotabRpcHandler()
        install_transport(monkeypatch, handler)
        Sync(_write_geotab_config(tmp_path)).run()
        snapshot = pl.read_parquet(tmp_path / 'data/geotab/devices/data.parquet')
        assert snapshot.height == 6
        assert set(snapshot['id'].to_list()) == {
            'bF7C22',
            'bF7C19',
            'bF7C24',
            'bF7C1C',
            'bF7C25',
            'bF7C18',
        }
        statuses = {(row[0], row[2]) for row in _ledger_rows(tmp_path / 'data')}
        assert ('devices', 'succeeded') in statuses
        # Every data call (three Get pages + the GetCountOf) rode the session.
        assert len(handler.credentials_seen) == 4
        assert all(
            credentials['sessionId'] == 'SyntheticSessionId000001'
            for credentials in handler.credentials_seen
        )

    def test_both_providers_run_in_one_sync(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        geotab_handler = _GeotabRpcHandler()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/apiv1':
                return geotab_handler(request)
            if request.url.path == '/v1/vehicles':
                return _vehicles_response(1, 2)
            return httpx.Response(404, text='no route')

        install_transport(monkeypatch, handler)
        Sync(_write_geotab_config(tmp_path, include_motive=True)).run()
        motive = pl.read_parquet(tmp_path / 'data/motive/vehicles/data.parquet')
        geotab = pl.read_parquet(tmp_path / 'data/geotab/devices/data.parquet')
        assert motive.height == 2
        assert geotab.height == 6
        statuses = {(row[0], row[2]) for row in _ledger_rows(tmp_path / 'data')}
        assert ('vehicles', 'succeeded') in statuses
        assert ('devices', 'succeeded') in statuses

    def test_unknown_geotab_endpoint_names_the_geotab_valid_set(
        self, tmp_path: Path
    ) -> None:
        config_path = _write_geotab_config(tmp_path, endpoints='[devicez]')
        with pytest.raises(ConfigurationError) as raised:
            Sync(config_path)
        message = str(raised.value)
        assert 'geotab' in message
        assert 'devicez' in message
        assert 'devices' in message

    def test_count_mismatch_fails_the_run_without_leaking_the_password(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A mismatched count fails the run loudly after the one harvest;
        # the failure aggregates and no repr anywhere carries the password.
        install_transport(monkeypatch, _GeotabRpcHandler(count=999))
        with pytest.raises(SyncFailuresError) as raised:
            Sync(_write_geotab_config(tmp_path)).run()
        failures = raised.value.failures
        assert [(f.provider, f.endpoint) for f in failures] == [('geotab', 'devices')]
        assert '999' in repr(failures[0].error)  # the counts are named...
        for rendering in (
            str(raised.value),
            repr(raised.value),
            repr(failures[0].error),
        ):
            assert SYNTHETIC_GEOTAB_PASS not in rendering  # ...the secret never


class TestGeotabTripsEnablement:
    def test_trips_selection_validates_with_no_widening(self, tmp_path: Path) -> None:
        # The windowed GeoTab endpoint rides the composition the devices
        # vertical already widened; construction validates the selection
        # against the catalog (the run itself is the live script's proof).
        config_path = _write_geotab_config(tmp_path, endpoints='[devices, trips]')
        Sync(config_path)


class TestStaging:
    """``_staged_by_provider``: the pure carve behind the staged queues."""

    # An interleaved config-order selection covering every stage shape:
    # a provider with a feeder and a consumer (Motive), one with a feeder
    # and two consumers including a snapshot non-feeder (Samsara), and one
    # with no feeder at all (GeoTab -- devices is snapshot but sources no
    # roster, so feeder-hood, not snapshot-hood, is what stages it).
    _SELECTION: ClassVar[list[tuple[Provider, str]]] = [
        (Provider.GEOTAB, 'devices'),
        (Provider.MOTIVE, 'vehicle_locations'),
        (Provider.SAMSARA, 'trips'),
        (Provider.MOTIVE, 'vehicles'),
        (Provider.SAMSARA, 'vehicles'),
        (Provider.SAMSARA, 'drivers'),
        (Provider.GEOTAB, 'trips'),
    ]

    def test_split_is_by_feeder_hood_not_snapshot_hood(self) -> None:
        staged = sync_module._staged_by_provider(
            self._SELECTION, build_roster_registry()
        )
        assert staged[Provider.MOTIVE].feeders == ('vehicles',)
        assert staged[Provider.SAMSARA].feeders == ('vehicles',)
        # Snapshot endpoints sourcing no roster stay consumers: geotab
        # devices and samsara drivers have no dependents to barrier for.
        assert staged[Provider.GEOTAB].feeders == ()
        assert 'drivers' in staged[Provider.SAMSARA].consumers

    def test_config_order_is_preserved_within_each_stage(self) -> None:
        staged = sync_module._staged_by_provider(
            self._SELECTION, build_roster_registry()
        )
        assert staged[Provider.SAMSARA].consumers == ('trips', 'drivers')
        assert staged[Provider.GEOTAB].consumers == ('devices', 'trips')
        assert staged[Provider.MOTIVE].consumers == ('vehicle_locations',)

    def test_queue_order_matches_the_retired_feeder_first_serial_order(self) -> None:
        # Feeders + consumers concatenated is exactly the order the serial
        # queue ran: the provider's config-order subsequence, stably sorted
        # feeders-first -- now the reporting contract, not an execution
        # order.
        staged = sync_module._staged_by_provider(
            self._SELECTION, build_roster_registry()
        )
        motive = staged[Provider.MOTIVE]
        samsara = staged[Provider.SAMSARA]
        geotab = staged[Provider.GEOTAB]
        assert motive.feeders + motive.consumers == ('vehicles', 'vehicle_locations')
        assert samsara.feeders + samsara.consumers == ('vehicles', 'trips', 'drivers')
        assert geotab.feeders + geotab.consumers == ('devices', 'trips')

    def test_providers_are_keyed_in_first_appearance_order(self) -> None:
        staged = sync_module._staged_by_provider(
            self._SELECTION, build_roster_registry()
        )
        assert list(staged) == [Provider.GEOTAB, Provider.MOTIVE, Provider.SAMSARA]


class _StubRecord(ResponseModel):
    value: str


def _stub_definition(name: str) -> EndpointDefinition[ResponseModel]:
    """A minimal Motive-flavored definition; only its identity is consulted."""
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name=name,
        spec_builder=StaticGetSpecBuilder(base_url='https://api.test', path=f'/{name}'),
        page_decoder=StubPageDecoder(),
        response_model=_StubRecord,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )


def _queue_work(endpoint_names: Sequence[str]) -> sync_module._ProviderQueueWork:
    """A work bundle for direct queue tests: only the registry is real.

    The runner, rosters, and fetch pools are inert casts -- these tests
    replace ``run_endpoint`` wholesale, so nothing ever touches them.
    """
    return sync_module._ProviderQueueWork(
        registry=EndpointRegistry([_stub_definition(name) for name in endpoint_names]),
        runner=cast(EndpointRunner, object()),
        rosters=cast(RosterMachinery, object()),
        fetch_pools=cast(FetchPoolRegistry, object()),
    )


def _run_queue(
    stages: sync_module._ProviderStages, work: sync_module._ProviderQueueWork
) -> list[EndpointFailure]:
    """Run the queue on a dedicated worker thread, the way ``run()`` does.

    Keeps the queue worker's thread rename off pytest's main thread, and
    re-raises a queue bug through the future exactly like the real drain.
    """
    with ThreadPoolExecutor(max_workers=1) as queue_pool:
        return queue_pool.submit(
            sync_module._run_provider_queue, Provider.MOTIVE, stages, work
        ).result(timeout=30)


@contextmanager
def _queue_in_flight(
    stages: sync_module._ProviderStages, work: sync_module._ProviderQueueWork
) -> Iterator[Future[list[EndpointFailure]]]:
    """The queue running on its own worker thread, yielded mid-flight.

    For choreographies that observe or release parked endpoints while the
    queue runs; leaving the ``with`` block joins the worker, so the future
    is done once the block exits. Every parked script must carry its own
    bounded wait -- the join blocks until each one releases or times out.
    """
    with ThreadPoolExecutor(max_workers=1) as queue_pool:
        yield queue_pool.submit(
            sync_module._run_provider_queue, Provider.MOTIVE, stages, work
        )


class _ScriptedRunEndpoint:
    """A ``run_endpoint`` double dispatching per endpoint name.

    Records ``start:<name>`` / ``end:<name>`` events under a lock (an
    ``end`` lands only when the script returns), then runs the endpoint's
    script -- block on an event, raise, or return -- so tests choreograph
    interleavings deterministically with no sleeps.
    """

    def __init__(self, scripts: dict[str, Callable[[], None]]) -> None:
        self._scripts = scripts
        self._events_lock = threading.Lock()
        self.events: list[str] = []

    def _record(self, event: str) -> None:
        with self._events_lock:
            self.events.append(event)

    def __call__(
        self,
        definition: EndpointDefinition[ResponseModel],
        runner: EndpointRunner,
        rosters: RosterMachinery,
        fetch_pools: FetchPoolRegistry,
    ) -> None:
        self._record(f'start:{definition.name}')
        self._scripts.get(definition.name, lambda: None)()
        self._record(f'end:{definition.name}')


class TestStagedQueue:
    """``_run_provider_queue``: the staged, stop-event-guarded endpoint tasks."""

    def test_consumers_within_one_provider_overlap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # True overlap, proven not assumed: both consumers park on a
        # two-party barrier, so the stage completes only if both are in
        # flight at once. The retired serial queue would park its lone
        # first endpoint until the timeout converts the deadlock into a
        # loud BrokenBarrierError.
        rendezvous = threading.Barrier(2)

        def _park() -> None:
            rendezvous.wait(timeout=10)

        recorder = _ScriptedRunEndpoint({'alpha': _park, 'beta': _park})
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        failures = _run_queue(
            sync_module._ProviderStages(feeders=(), consumers=('alpha', 'beta')),
            _queue_work(['alpha', 'beta']),
        )
        assert failures == []
        assert {'end:alpha', 'end:beta'} <= set(recorder.events)

    def test_the_feeder_completes_before_any_consumer_starts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The stage join is the barrier, proven while the feeder is parked
        # mid-run rather than inferred from event order after the fact: a
        # merged single stage starts a consumer within its thread-spawn
        # latency, so the parked window catches it deterministically where
        # an unparked feeder would win the spawn race by timing luck.
        feeder_parked = threading.Event()
        release_feeder = threading.Event()
        consumer_started = threading.Event()

        def _feeder_parks() -> None:
            feeder_parked.set()
            assert release_feeder.wait(timeout=10), 'feeder never released'

        recorder = _ScriptedRunEndpoint(
            {
                'feeder': _feeder_parks,
                'alpha': consumer_started.set,
                'beta': consumer_started.set,
            }
        )
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        stages = sync_module._ProviderStages(
            feeders=('feeder',), consumers=('alpha', 'beta')
        )
        with _queue_in_flight(
            stages, _queue_work(['feeder', 'alpha', 'beta'])
        ) as queue_future:
            assert feeder_parked.wait(timeout=10), 'feeder never started'
            # A regression-detection window, never passing-path
            # synchronization: behind the barrier no consumer can start
            # while the feeder is parked, so this wait expires untouched;
            # without the barrier a consumer starts within it and fails
            # the test loudly.
            assert not consumer_started.wait(timeout=0.5), (
                'a consumer started while the feeder was still parked: '
                'the feeder barrier is gone'
            )
            release_feeder.set()
            assert queue_future.result(timeout=30) == []
        feeder_end = recorder.events.index('end:feeder')
        assert feeder_end < recorder.events.index('start:alpha')
        assert feeder_end < recorder.events.index('start:beta')

    def test_a_feeder_operational_failure_still_runs_the_consumers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _feeder_fails() -> None:
            raise ProviderResponseError(
                provider='motive', endpoint='feeder', detail='listing 404'
            )

        recorder = _ScriptedRunEndpoint({'feeder': _feeder_fails})
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        failures = _run_queue(
            sync_module._ProviderStages(
                feeders=('feeder',), consumers=('alpha', 'beta')
            ),
            _queue_work(['feeder', 'alpha', 'beta']),
        )
        assert [(f.provider, f.endpoint) for f in failures] == [('motive', 'feeder')]
        assert isinstance(failures[0].error, ProviderResponseError)
        assert {'end:alpha', 'end:beta'} <= set(recorder.events)

    def test_operational_failures_report_in_queue_order_not_completion_order(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Completion order is forced to invert queue order: alpha fails
        # only after beta's failure has landed. The report still reads
        # queue order -- the submission-order drain, never a re-sort.
        beta_failed = threading.Event()

        def _alpha_fails_second() -> None:
            assert beta_failed.wait(timeout=10), 'beta never failed'
            raise ProviderResponseError(
                provider='motive', endpoint='alpha', detail='second to land'
            )

        def _beta_fails_first() -> None:
            beta_failed.set()
            raise ProviderResponseError(
                provider='motive', endpoint='beta', detail='first to land'
            )

        recorder = _ScriptedRunEndpoint(
            {'alpha': _alpha_fails_second, 'beta': _beta_fails_first}
        )
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        failures = _run_queue(
            sync_module._ProviderStages(feeders=(), consumers=('alpha', 'beta')),
            _queue_work(['alpha', 'beta']),
        )
        assert [f.endpoint for f in failures] == ['alpha', 'beta']

    def test_a_bug_lets_the_in_flight_sibling_finish_then_reraises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Beta's bug lands while alpha is provably in flight; alpha still
        # finishes (its end event is recorded), and the bug re-raises only
        # after the stage joined. Beta waits for alpha's start before
        # raising: without that handshake, beta's whole task can run
        # before alpha's thread is ever scheduled, and the stop event
        # would skip alpha instead of letting it outlive the bug.
        alpha_started = threading.Event()
        beta_raised = threading.Event()

        def _alpha_outlives_the_bug() -> None:
            alpha_started.set()
            assert beta_raised.wait(timeout=10), 'beta never raised'

        def _beta_bug() -> None:
            assert alpha_started.wait(timeout=10), 'alpha never started'
            beta_raised.set()
            raise RuntimeError('planted bug')

        recorder = _ScriptedRunEndpoint(
            {'alpha': _alpha_outlives_the_bug, 'beta': _beta_bug}
        )
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        with pytest.raises(RuntimeError, match='planted bug'):
            _run_queue(
                sync_module._ProviderStages(feeders=(), consumers=('alpha', 'beta')),
                _queue_work(['alpha', 'beta']),
            )
        assert 'end:alpha' in recorder.events

    def test_the_first_bug_by_queue_order_wins_over_an_earlier_completion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Two bugs, completion order inverted: beta's lands first in time,
        # alpha's wins deterministically because the drain re-raises in
        # queue order, never by completion timing. Beta waits for alpha's
        # start before raising: without that handshake, beta's whole task
        # can run before alpha's thread is ever scheduled, and the stop
        # event would skip alpha -- leaving beta's bug to win by default.
        alpha_started = threading.Event()
        beta_raised = threading.Event()

        def _alpha_bug_second() -> None:
            alpha_started.set()
            assert beta_raised.wait(timeout=10), 'beta never raised'
            raise RuntimeError('alpha bug')

        def _beta_bug_first() -> None:
            assert alpha_started.wait(timeout=10), 'alpha never started'
            beta_raised.set()
            raise RuntimeError('beta bug')

        recorder = _ScriptedRunEndpoint(
            {'alpha': _alpha_bug_second, 'beta': _beta_bug_first}
        )
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        with pytest.raises(RuntimeError, match='alpha bug'):
            _run_queue(
                sync_module._ProviderStages(feeders=(), consumers=('alpha', 'beta')),
                _queue_work(['alpha', 'beta']),
            )

    def test_a_stage_one_bug_skips_stage_two_entirely(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The feeder parks before raising, so the consumer's absence is
        # owed to the stage boundary alone: a merged single stage would
        # have started alpha inside the parked window, long before the
        # bug's stop event could win any scheduling race.
        feeder_parked = threading.Event()
        release_feeder = threading.Event()
        alpha_started = threading.Event()

        def _feeder_parks_then_bugs() -> None:
            feeder_parked.set()
            assert release_feeder.wait(timeout=10), 'feeder never released'
            raise RuntimeError('feeder bug')

        recorder = _ScriptedRunEndpoint(
            {'feeder': _feeder_parks_then_bugs, 'alpha': alpha_started.set}
        )
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        stages = sync_module._ProviderStages(feeders=('feeder',), consumers=('alpha',))
        with _queue_in_flight(stages, _queue_work(['feeder', 'alpha'])) as queue_future:
            assert feeder_parked.wait(timeout=10), 'feeder never started'
            # The same regression-detection window as the barrier test:
            # untouched behind the barrier, tripped within microseconds by
            # a merged stage.
            assert not alpha_started.wait(timeout=0.5), (
                'the consumer started while the feeder was still parked: '
                'the feeder barrier is gone'
            )
            release_feeder.set()
            with pytest.raises(RuntimeError, match='feeder bug'):
                queue_future.result(timeout=30)
        assert 'start:alpha' not in recorder.events

    def test_a_bug_sets_the_stop_event_before_escaping(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The write side of the stop contract (the skip test below pins
        # the read side): DESIGN section 7 promises a bug sets the queue's
        # stop event before escaping its future, and only this assertion
        # keeps that sentence from silently going false.
        def _bug() -> None:
            raise RuntimeError('planted bug')

        recorder = _ScriptedRunEndpoint({'alpha': _bug})
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        queue_run = sync_module._StagedQueueRun(Provider.MOTIVE, _queue_work(['alpha']))
        with pytest.raises(RuntimeError, match='planted bug'):
            queue_run.run_stage(('alpha',))
        assert queue_run._stop_running.is_set()

    def test_a_set_stop_event_skips_the_task_without_running_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The unstarted-work skip, pinned deterministically: with the stop
        # event already set, the task returns the skip sentinel and the
        # endpoint never runs -- no failure recorded, nothing executed.
        recorder = _ScriptedRunEndpoint({})
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        queue_run = sync_module._StagedQueueRun(Provider.MOTIVE, _queue_work(['alpha']))
        queue_run._stop_running.set()
        assert queue_run.run_stage(('alpha',)) == []
        assert recorder.events == []

    def test_an_empty_stage_spawns_nothing_and_reports_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        recorder = _ScriptedRunEndpoint({})
        monkeypatch.setattr(sync_module, 'run_endpoint', recorder)
        queue_run = sync_module._StagedQueueRun(Provider.MOTIVE, _queue_work([]))
        assert queue_run.run_stage(()) == []
        assert recorder.events == []


class TestProviderParallelism:
    """The queue-per-provider grain: staged-concurrent within a provider,
    providers concurrent (DESIGN section 7's records, 2026-07-20)."""

    def test_provider_queues_run_concurrently(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # True overlap, proven not assumed: Motive's one vehicles request
        # and GeoTab's one Authenticate each park on a two-party barrier,
        # so the sync completes only if both providers are in flight at
        # once. The old serial endpoint loop ran one provider to
        # completion before starting the next -- its lone parked handler
        # would wait until the timeout converts the deadlock into a loud
        # BrokenBarrierError. No sleeps: the barrier is the rendezvous.
        rendezvous = threading.Barrier(2)
        geotab_handler = _GeotabRpcHandler()

        def overlapping_handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/v1/vehicles':
                rendezvous.wait(timeout=10)
                return _vehicles_response(1, 2)
            if request.url.path == '/apiv1':
                if json.loads(request.content)['method'] == 'Authenticate':
                    rendezvous.wait(timeout=10)
                return geotab_handler(request)
            return httpx.Response(404, text='no route')

        install_transport(monkeypatch, overlapping_handler)
        Sync(_write_geotab_config(tmp_path, include_motive=True)).run()
        motive = pl.read_parquet(tmp_path / 'data/motive/vehicles/data.parquet')
        geotab = pl.read_parquet(tmp_path / 'data/geotab/devices/data.parquet')
        assert motive.height == 2
        assert geotab.height == 6

    def test_failures_aggregate_in_provider_then_queue_order(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Both Motive endpoints fail (the feeder 404s in stage one, then
        # the consumer's cold-start roster refresh fails on the same route
        # in stage two) and GeoTab's count mismatch fails devices: the
        # aggregate carries queue order within a provider (feeders then
        # consumers), provider config order across providers -- Motive
        # before GeoTab regardless of which queue finished first.
        geotab_handler = _GeotabRpcHandler(count=999)

        def split_failure_handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/apiv1':
                return geotab_handler(request)
            return httpx.Response(404, text='no route')

        install_transport(monkeypatch, split_failure_handler)
        config_path = _write_geotab_config(
            tmp_path,
            include_motive=True,
            motive_endpoints='[vehicles, vehicle_locations]',
        )
        with pytest.raises(SyncFailuresError) as raised:
            Sync(config_path).run()
        assert [(f.provider, f.endpoint) for f in raised.value.failures] == [
            ('motive', 'vehicles'),
            ('motive', 'vehicle_locations'),
            ('geotab', 'devices'),
        ]

    def test_a_bug_stops_its_queue_while_the_other_provider_completes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A non-FleetpullError is a bug: the Motive vehicles fetch raises
        # RuntimeError (nothing on the fetch path catches it), which stops
        # Motive's queue -- vehicle_locations never starts -- while
        # GeoTab's queue completes fully; the bug re-raises raw after
        # every queue has joined (no leaked worker threads).
        geotab_handler = _GeotabRpcHandler()

        def buggy_motive_handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/v1/vehicles':
                raise RuntimeError('planted bug')
            if request.url.path == '/apiv1':
                return geotab_handler(request)
            return httpx.Response(404, text='no route')

        install_transport(monkeypatch, buggy_motive_handler)
        config_path = _write_geotab_config(
            tmp_path,
            include_motive=True,
            motive_endpoints='[vehicles, vehicle_locations]',
        )
        with pytest.raises(RuntimeError, match='planted bug'):
            Sync(config_path).run()
        endpoints_run = [row[0] for row in _ledger_rows(tmp_path / 'data')]
        assert 'vehicle_locations' not in endpoints_run  # the queue stopped
        geotab = pl.read_parquet(tmp_path / 'data/geotab/devices/data.parquet')
        assert geotab.height == 6  # the sibling queue was untouched
        assert _fleetpull_worker_threads() == []  # joined before the re-raise
