"""Threaded fan-out through the real runner: equivalence, cancellation, streaming.

Drives ``EndpointRunner`` with a real ``FanOutRequestDriver`` over a routed,
thread-safe stub client -- the full validate -> frame -> stage -> compact
consumer path against real parquet under ``tmp_path`` -- proving the
concurrency vertical's contracts where they matter: identical output to the
serial path, first-failure cancellation with clean staging, and the streaming
property (the writer consumes while later pieces are not yet fetched).
"""

import itertools
import threading
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from fleetpull.config import (
    FleetpullConfig,
    ProvidersConfig,
    StorageConfig,
    SyncConfig,
)
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import ProviderResponseError
from fleetpull.incremental import (
    DateWatermark,
    FeedBootstrap,
    FeedToken,
    IncrementalCursor,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import (
    DecodedPage,
    HttpMethod,
    PageAdvance,
    PageDecoder,
    RequestSpec,
)
from fleetpull.orchestrator.drivers import FanOutRequestDriver
from fleetpull.orchestrator.fanout import FetchPool
from fleetpull.orchestrator.outcome import Executed
from fleetpull.orchestrator.runner import EndpointRunner, RunStateAccess
from fleetpull.paths import endpoint_directory
from fleetpull.state import StateDatabase, WorkUnitStore, migrate_to_head
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import JsonObject, JsonValue, Provider, QuotaScope
from tests.orchestrator.serial_executor import SerialExecutor

_PLACEHOLDER = 'vehicle_id'
_CLOCK_NOW = datetime(2026, 6, 16, tzinfo=UTC)
_GATE_TIMEOUT_SECONDS = 5.0


class _WatermarkModel(ResponseModel):
    occurred_at: datetime
    label: str


class _StubPageDecoder:
    """A PageDecoder double; the routed client bypasses it entirely."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


@dataclass(frozen=True, slots=True)
class _MemberSpecBuilder:
    """Builds a URL ending in the member key, so the routed client can route."""

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        member = path_values[_PLACEHOLDER]
        return RequestSpec(method=HttpMethod.GET, url=f'https://x.test/v3/loc/{member}')


class _RoutedClient(TransportClient):
    """Serves each member's canned pages, keyed off the request URL.

    Thread-safe by construction (immutable routing table; the request log is
    lock-guarded), unlike a sequenced stub -- worker threads may fetch any
    member at any time. ``on_fetch`` runs on the worker thread before the
    member's pages are served; tests plant gates and failures there.
    """

    def __init__(
        self,
        pages_by_member: dict[str, list[list[JsonObject]]],
        on_fetch: Callable[[str], None] | None = None,
    ) -> None:
        self._pages_by_member = pages_by_member
        self._on_fetch = on_fetch
        self._lock = threading.Lock()
        self.requested: list[str] = []

    def fetch_pages(
        self, spec: RequestSpec, page_decoder: PageDecoder, quota_scope: str
    ) -> Iterator[FetchedPage]:
        member = spec.url.rsplit('/', 1)[-1]
        with self._lock:
            self.requested.append(member)
        if self._on_fetch is not None:
            self._on_fetch(member)
        for records in self._pages_by_member[member]:
            yield FetchedPage(records=records, durable_progress=None)


class _RecordingRecorder:
    """A RunRecorder capturing the run lifecycle calls."""

    def __init__(self) -> None:
        self.completed: list[tuple[int, int]] = []
        self.failed: list[tuple[int, str]] = []

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        return 1

    def start_window_run(
        self, provider: Provider, endpoint: str, *, window: tuple[datetime, datetime]
    ) -> int:
        return 1

    def start_feed_run(
        self, provider: Provider, endpoint: str, *, start: FeedBootstrap | FeedToken
    ) -> int:
        raise AssertionError('a watermark run must never open a feed run')

    def complete_run(
        self, run_id: int, *, row_count: int, to_version: str | None = None
    ) -> None:
        self.completed.append((run_id, row_count))

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        self.failed.append((run_id, error_detail))

    def coverage_frontier(self, provider: Provider, endpoint: str) -> datetime | None:
        return None


class _StubCursorAccess:
    """A CursorAccess with no stored cursor, recording its writes."""

    def __init__(self) -> None:
        self.set_calls: list[IncrementalCursor] = []

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        return None

    def set_cursor(
        self, provider: Provider, endpoint: str, cursor: IncrementalCursor
    ) -> None:
        self.set_calls.append(cursor)


class _ClientSourceOf:
    """A ClientSource handing back one prebuilt client."""

    def __init__(self, client: TransportClient) -> None:
        self._client = client

    def client_for(self, provider: Provider) -> TransportClient:
        return self._client


def _definition() -> EndpointDefinition[_WatermarkModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='locations',
        spec_builder=_MemberSpecBuilder(),
        page_decoder=_StubPageDecoder(),
        response_model=_WatermarkModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=1)),
        event_time_column='occurred_at',
    )


def _make_runner(
    recorder: _RecordingRecorder,
    dataset_root: Path,
    client: TransportClient,
    cursor_access: _StubCursorAccess,
) -> EndpointRunner:
    # The resolved window is [2026-06-10, 2026-06-15): default start below,
    # trailing edge one cutoff day before the frozen clock. Five days fits
    # one default chunk, so the run is the single-unit degenerate case.
    clock = FrozenClock(start_time_utc=_CLOCK_NOW)
    database = StateDatabase(dataset_root / 'state.sqlite3')
    database.initialize()
    migrate_to_head(database)
    return EndpointRunner(
        _ClientSourceOf(client),
        RunStateAccess(
            recorder=recorder,
            cursors=cursor_access,
            units=WorkUnitStore(database, clock),
        ),
        clock,
        FleetpullConfig(
            sync=SyncConfig(default_start_date=date(2026, 6, 10)),
            storage=StorageConfig(dataset_root=dataset_root),
            providers=ProvidersConfig(),
        ),
    )


def _record(occurred_at: str, label: str) -> JsonObject:
    return {'occurred_at': occurred_at, 'label': label}


def _fleet_pages() -> dict[str, list[list[JsonObject]]]:
    """Three members, two pages each, landing on two shared dates."""
    return {
        member: [
            [
                _record(f'2026-06-12T0{index}:00:00Z', f'{member}-p1-a'),
                _record(f'2026-06-13T0{index}:00:00Z', f'{member}-p1-b'),
            ],
            [_record(f'2026-06-13T1{index}:00:00Z', f'{member}-p2-a')],
        ]
        for index, member in enumerate(['veh-1', 'veh-2', 'veh-3'])
    }


def _partition_bytes(endpoint_dir: Path) -> dict[str, bytes]:
    return {
        part_file.parent.name: part_file.read_bytes()
        for part_file in sorted(endpoint_dir.glob('date=*/part.parquet'))
    }


def test_threaded_run_matches_the_serial_run_byte_for_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Order-independence demonstrated: same parquet bytes, same watermark.

    Shard files are uuid-named and compaction folds them in sorted-name
    order, so byte-stability across ANY two runs -- two serial runs
    included -- requires pinning the shard names; the patch below replaces
    the uuid with a per-run counter so the comparison isolates exactly the
    variable under test: worker scheduling.
    """

    def run_once(
        dataset_root: Path, pool: FetchPool
    ) -> tuple[dict[str, bytes], list[IncrementalCursor], int]:
        counter = itertools.count()
        monkeypatch.setattr(
            'fleetpull.storage.files.uuid4',
            lambda: SimpleNamespace(hex=f'{next(counter):08d}'),
        )
        recorder = _RecordingRecorder()
        cursor_access = _StubCursorAccess()
        client = _RoutedClient(_fleet_pages())
        runner = _make_runner(recorder, dataset_root, client, cursor_access)
        driver = FanOutRequestDriver(
            members=['veh-1', 'veh-2', 'veh-3'],
            path_placeholder=_PLACEHOLDER,
            fetch_pool=pool,
        )
        outcome = runner.run(_definition(), driver)
        assert isinstance(outcome, Executed)
        endpoint_dir = endpoint_directory(dataset_root, 'motive', 'locations')
        return (
            _partition_bytes(endpoint_dir),
            cursor_access.set_calls,
            (outcome.records_fetched),
        )

    serial_pool = FetchPool(executor=SerialExecutor(), submission_window=4)
    serial_bytes, serial_cursors, serial_rows = run_once(
        tmp_path / 'serial', serial_pool
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        threaded_pool = FetchPool(executor=executor, submission_window=4)
        threaded_bytes, threaded_cursors, threaded_rows = run_once(
            tmp_path / 'threaded', threaded_pool
        )

    assert sorted(serial_bytes) == ['date=2026-06-12', 'date=2026-06-13']
    assert threaded_bytes == serial_bytes
    assert threaded_cursors == serial_cursors
    assert serial_cursors == [
        DateWatermark(watermark=datetime(2026, 6, 13, 12, tzinfo=UTC))
    ]
    assert threaded_rows == serial_rows == 9


def test_worker_failure_cancels_pending_cleans_staging_and_fails_the_run(
    tmp_path: Path,
) -> None:
    """The first piece's failure fails the run exactly as the serial loop would.

    Deterministic through the synchronous executor: the window primes the
    first two members, the first member's planted transport failure wins
    before anything yields, members beyond the in-flight horizon are never
    requested, nothing reaches the writer, and no staging survives.
    """
    members = [f'veh-{index}' for index in range(6)]
    planted = ProviderResponseError(detail='planted transport failure')

    def fail_first(member: str) -> None:
        if member == 'veh-0':
            raise planted

    client = _RoutedClient(
        {member: [[_record('2026-06-12T08:00:00Z', member)]] for member in members},
        on_fetch=fail_first,
    )
    recorder = _RecordingRecorder()
    cursor_access = _StubCursorAccess()
    dataset_root = tmp_path / 'data'
    runner = _make_runner(recorder, dataset_root, client, cursor_access)
    driver = FanOutRequestDriver(
        members=members,
        path_placeholder=_PLACEHOLDER,
        fetch_pool=FetchPool(executor=SerialExecutor(), submission_window=2),
    )

    with pytest.raises(ProviderResponseError, match='planted transport failure'):
        runner.run(_definition(), driver)

    # The in-flight horizon is the two-piece window; veh-2.. were never
    # requested, so no rate-budget token would ever have been spent on them.
    assert client.requested == ['veh-0', 'veh-1']
    assert recorder.failed
    assert recorder.completed == []
    assert cursor_access.set_calls == []
    # Nothing yielded, so nothing was written or staged: the endpoint
    # directory was never even created.
    endpoint_dir = endpoint_directory(dataset_root, 'motive', 'locations')
    assert not endpoint_dir.exists()


def test_writer_consumes_batches_while_later_pieces_are_not_yet_fetched(
    tmp_path: Path,
) -> None:
    """The streaming property, end to end: real threads, real writer.

    Members past the first two are gated: their pages are served only after
    the consumer has observably written batch one (the observer fires before
    each write, so by its second call the first frame is on disk as a staged
    shard). A collect-everything implementation -- ``executor.map`` into a
    list, or gathering all futures before writing -- fetches every member
    before the first write, so the gate never opens and the run dies on the
    gate timeout instead of passing.
    """
    members = [f'veh-{index}' for index in range(5)]
    first_batch_written = threading.Event()
    observed_frames = 0

    def gate_later_members(member: str) -> None:
        if int(member.rsplit('-', 1)[-1]) >= 2 and not first_batch_written.wait(
            timeout=_GATE_TIMEOUT_SECONDS
        ):
            raise TimeoutError(
                'gate never opened: no batch was written while later pieces '
                'were pending -- collect-all behavior'
            )

    def observe(frame: pl.DataFrame) -> None:
        nonlocal observed_frames
        observed_frames += 1
        # By the second observation, frame one is already written: the
        # consumer loop runs observe(f1) -> write(f1) -> observe(f2).
        if observed_frames == 2:
            first_batch_written.set()

    client = _RoutedClient(
        {
            member: [
                [_record('2026-06-12T08:00:00Z', f'{member}-a')],
                [_record('2026-06-13T09:00:00Z', f'{member}-b')],
            ]
            for member in members
        },
        on_fetch=gate_later_members,
    )
    recorder = _RecordingRecorder()
    dataset_root = tmp_path / 'data'
    runner = _make_runner(recorder, dataset_root, client, _StubCursorAccess())
    with ThreadPoolExecutor(max_workers=2) as executor:
        driver = FanOutRequestDriver(
            members=members,
            path_placeholder=_PLACEHOLDER,
            fetch_pool=FetchPool(executor=executor, submission_window=2),
        )
        outcome = runner.run(_definition(), driver, observe)

    assert isinstance(outcome, Executed)
    assert outcome.records_fetched == 10
    assert sorted(client.requested) == sorted(members)
