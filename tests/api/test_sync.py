"""Tests for fleetpull.api.sync -- the config-driven verb, no live network.

The integration tests run the whole real composition (state DB, stores,
discovered registries, limiter, clients, run executor) against
``httpx.MockTransport`` via the transport-test seam, from a config file
in ``tmp_path``. Wire shapes are Motive's real envelopes; every
identifier is synthetic.
"""

import sqlite3
import ssl
import threading
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn

import httpx
import polars as pl
import pytest

import fleetpull.api.sync as sync_module
from fleetpull import ConfigurationError, Sync, SyncFailuresError
from fleetpull.vocabulary import JsonValue

_SYNTHETIC_KEY = 'synthetic-motive-key-000'

# The genuine class, captured before any test monkeypatches httpx.Client
# (the transport-test precedent).
_REAL_CLIENT_CLS = httpx.Client

_Handler = Callable[[httpx.Request], httpx.Response]


@pytest.fixture(autouse=True)
def _no_ambient_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip MOTIVE_API_KEY so a developer's shell never leaks into tests."""
    monkeypatch.delenv('MOTIVE_API_KEY', raising=False)


def _install_transport(monkeypatch: pytest.MonkeyPatch, handler: _Handler) -> None:
    """Route every httpx.Client the composition builds through ``handler``."""
    mock_transport = httpx.MockTransport(handler)

    def client_factory(
        *, verify: ssl.SSLContext | bool = True, timeout: httpx.Timeout | None = None
    ) -> httpx.Client:
        # verify is ignored -- the mock transport short-circuits real TLS.
        return _REAL_CLIENT_CLS(transport=mock_transport, timeout=timeout)

    monkeypatch.setattr(httpx, 'Client', client_factory)


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
        f"    api_key: '{_SYNTHETIC_KEY}'\n"
        f'    endpoints: {endpoints}\n'
        '    rate_limit:\n'
        '      requests_per_period: 6000\n'
        '      period_seconds: 60.0\n'
        '      burst: 1000\n'
        '      max_concurrency: 2\n',
        encoding='utf-8',
    )
    return config_path


def _vehicle_record(vehicle_id: int) -> dict[str, JsonValue]:
    return {
        'id': vehicle_id,
        'company_id': 77,
        'number': f'UNIT-{vehicle_id}',
        'status': 'active',
        'ifta': False,
        'created_at': '2026-01-01T00:00:00Z',
        'updated_at': '2026-01-02T00:00:00Z',
    }


def _vehicles_response(*vehicle_ids: int) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            'vehicles': [{'vehicle': _vehicle_record(v)} for v in vehicle_ids],
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
        assert _SYNTHETIC_KEY not in str(raised.value)
        assert _SYNTHETIC_KEY not in repr(raised.value)


class TestRun:
    def test_end_to_end_writes_parquet_state_and_ledger(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_transport(monkeypatch, _happy_handler)
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
        _install_transport(monkeypatch, _happy_handler)
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

        _install_transport(monkeypatch, locations_fail)
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

        _install_transport(monkeypatch, _happy_handler)
        monkeypatch.setattr(sync_module, 'run_endpoint', explode)
        with pytest.raises(RuntimeError, match='planted bug'):
            Sync(_write_config(tmp_path)).run()

    def test_run_failures_never_leak_the_secret(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def all_fail(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text='no route')

        _install_transport(monkeypatch, all_fail)
        with pytest.raises(SyncFailuresError) as raised:
            Sync(_write_config(tmp_path)).run()
        assert _SYNTHETIC_KEY not in str(raised.value)
        assert _SYNTHETIC_KEY not in repr(raised.value)
        assert all(
            _SYNTHETIC_KEY not in repr(failure.error)
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
        _install_transport(monkeypatch, self._duplicating_handler)
        Sync(_write_config(tmp_path, endpoints='[vehicles]')).run()
        snapshot = pl.read_parquet(tmp_path / 'data/motive/vehicles/data.parquet')
        assert snapshot.height == 1

    def test_false_preserves_the_duplicate(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_transport(monkeypatch, self._duplicating_handler)
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

        _install_transport(monkeypatch, overlapping_handler)
        Sync(_write_config(tmp_path)).run()
        partition = pl.read_parquet(
            tmp_path / 'data/motive/vehicle_locations/date=2026-06-02/part.parquet'
        )
        assert partition.height == 2

    def test_no_worker_threads_outlive_a_successful_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_transport(monkeypatch, _happy_handler)
        Sync(_write_config(tmp_path)).run()
        assert _fleetpull_worker_threads() == []

    def test_no_worker_threads_outlive_a_failed_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def all_fail(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text='no route')

        _install_transport(monkeypatch, all_fail)
        with pytest.raises(SyncFailuresError):
            Sync(_write_config(tmp_path)).run()
        assert _fleetpull_worker_threads() == []
