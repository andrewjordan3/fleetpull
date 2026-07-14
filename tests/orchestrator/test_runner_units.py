"""The plan-and-drive unit loop through the real runner and work-unit store.

Every windowed run is planned as work units and driven serially ascending;
these tests prove the loop's settled semantics end to end against real
parquet and a real migrated work-units store: boundary invariance (unit
count never changes the bytes), per-day commits under the degenerate
one-day chunk, crash resume from the units ledger (no completed unit
refetched), orphan re-claim, fail-fast with the completed prefix intact,
and persisted-boundary stability across a ``backfill_chunk_days`` change.
Deterministic throughout: canned drivers, a frozen clock, pinned shard
names for the byte comparisons.
"""

import itertools
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

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
    StaticGetSpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import ProviderResponseError
from fleetpull.incremental import (
    DateWatermark,
    DateWindow,
    FeedBootstrap,
    FeedToken,
    IncrementalCursor,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import DecodedPage, PageAdvance, RequestSpec
from fleetpull.orchestrator.outcome import Executed
from fleetpull.orchestrator.runner import EndpointRunner, RunStateAccess
from fleetpull.state import (
    StateDatabase,
    WorkUnitSpec,
    WorkUnitStore,
    migrate_to_head,
)
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import JsonObject, JsonValue, Provider, QuotaScope

_CLOCK_NOW = datetime(2026, 6, 16, tzinfo=UTC)
_ENDPOINT = 'locations'

# The resolved window for a cold start: default start 2026-06-10, trailing
# edge one cutoff day before the frozen clock -> [2026-06-10, 2026-06-15).
_WINDOW_START = datetime(2026, 6, 10, tzinfo=UTC)
_WINDOW_END = datetime(2026, 6, 15, tzinfo=UTC)


class _WatermarkModel(ResponseModel):
    occurred_at: datetime


class _StubPageDecoder:
    """A PageDecoder double; the canned driver bypasses it entirely."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


class _StubClient(TransportClient):
    """A hollow client; the canned driver never calls it."""

    def __init__(self) -> None:
        pass


class _StubClientSource:
    """A ClientSource handing a hollow client for any provider."""

    def __init__(self) -> None:
        self._client = _StubClient()

    def client_for(self, provider: Provider) -> TransportClient:
        return self._client


class _RecordingRecorder:
    """A RunRecorder capturing the per-unit run lifecycle."""

    def __init__(self) -> None:
        self.windows: list[tuple[datetime, datetime]] = []
        self.completed: list[tuple[int, int]] = []
        self.failed: list[tuple[int, str]] = []

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        raise AssertionError('a windowed run must never open a snapshot run')

    def start_window_run(
        self, provider: Provider, endpoint: str, *, window: tuple[datetime, datetime]
    ) -> int:
        self.windows.append(window)
        return len(self.windows)

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


class _FaithfulCursorAccess:
    """A CursorAccess whose reads reflect its writes (the real store's shape)."""

    def __init__(self, cursor: IncrementalCursor | None = None) -> None:
        self._cursor = cursor
        self.set_calls: list[IncrementalCursor] = []

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        return self._cursor

    def set_cursor(
        self, provider: Provider, endpoint: str, cursor: IncrementalCursor
    ) -> None:
        self._cursor = cursor
        self.set_calls.append(cursor)


class _WindowRecordingDriver:
    """A RequestDriver recording each unit's window; optionally failing on one.

    Serves the same canned pages to every unit -- the runner's per-unit
    window filter routes each row to the one unit whose window holds it, so
    the recorded windows double as the run's exact fetch log (the request
    count of the crash-resume assertions).
    """

    def __init__(
        self, batches: list[list[JsonObject]], fail_on: DateWindow | None = None
    ) -> None:
        self.windows: list[DateWindow] = []
        self._batches = batches
        self._fail_on = fail_on
        self.failure = ProviderResponseError(detail='planted unit fetch failure')

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        assert isinstance(resume, DateWindow)
        self.windows.append(resume)
        if self._fail_on is not None and resume == self._fail_on:
            raise self.failure
        for records in self._batches:
            yield FetchedPage(records=records, durable_progress=None)


def _definition() -> EndpointDefinition[_WatermarkModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name=_ENDPOINT,
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v3/loc'),
        page_decoder=_StubPageDecoder(),
        response_model=_WatermarkModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=1)),
        event_time_column='occurred_at',
    )


def _open_store(root: Path) -> WorkUnitStore:
    """The work-unit store over ``root``'s state database (created if absent)."""
    database = StateDatabase(root / 'state.sqlite3')
    database.initialize()
    migrate_to_head(database)
    return WorkUnitStore(database, FrozenClock(start_time_utc=_CLOCK_NOW))


def _make_runner(
    recorder: _RecordingRecorder,
    root: Path,
    cursors: _FaithfulCursorAccess,
    chunk_days: int,
) -> EndpointRunner:
    return EndpointRunner(
        _StubClientSource(),
        RunStateAccess(recorder=recorder, cursors=cursors, units=_open_store(root)),
        FrozenClock(start_time_utc=_CLOCK_NOW),
        FleetpullConfig(
            sync=SyncConfig(
                default_start_date=date(2026, 6, 10), backfill_chunk_days=chunk_days
            ),
            storage=StorageConfig(dataset_root=root),
            providers=ProvidersConfig(),
        ),
    )


def _fleet_batches() -> list[list[JsonObject]]:
    """Two pages spanning four of the window's five days (2026-06-13 empty)."""
    return [
        [
            {'occurred_at': '2026-06-10T08:00:00Z'},
            {'occurred_at': '2026-06-11T09:00:00Z'},
            {'occurred_at': '2026-06-12T10:00:00Z'},
        ],
        [
            {'occurred_at': '2026-06-10T20:00:00Z'},
            {'occurred_at': '2026-06-14T12:00:00Z'},
        ],
    ]


def _daily_window(start_day: int) -> DateWindow:
    return DateWindow(
        start=datetime(2026, 6, start_day, tzinfo=UTC),
        end=datetime(2026, 6, start_day + 1, tzinfo=UTC),
    )


def _pin_shard_names(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the uuid shard names with a fresh deterministic counter.

    Shard files are uuid-named and compaction folds them in sorted-name
    order, so byte-stability across runs requires pinning; a monotone
    counter preserves each partition's insertion order.
    """
    counter = itertools.count()
    monkeypatch.setattr(
        'fleetpull.storage.files.uuid4',
        lambda: SimpleNamespace(hex=f'{next(counter):08d}'),
    )


def _partition_bytes(root: Path) -> dict[str, bytes]:
    endpoint_dir = root / 'motive' / _ENDPOINT
    return {
        part_file.parent.name: part_file.read_bytes()
        for part_file in sorted(endpoint_dir.glob('date=*/part.parquet'))
    }


def _run_to_completion(
    root: Path, chunk_days: int, monkeypatch: pytest.MonkeyPatch
) -> tuple[_RecordingRecorder, _FaithfulCursorAccess, Executed]:
    """One uninterrupted invocation over the mock fleet, shard names pinned."""
    _pin_shard_names(monkeypatch)
    recorder = _RecordingRecorder()
    cursors = _FaithfulCursorAccess()
    runner = _make_runner(recorder, root, cursors, chunk_days)
    outcome = runner.run(_definition(), _WindowRecordingDriver(_fleet_batches()))
    assert isinstance(outcome, Executed)
    return recorder, cursors, outcome


def test_single_and_multi_unit_plans_write_identical_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Boundary invariance: the unit decomposition is a transactional choice,
    # never a data-shape choice -- one unit and three units over the same
    # window land byte-identical partitions and the same watermark.
    single_recorder, single_cursors, single_outcome = _run_to_completion(
        tmp_path / 'single', chunk_days=7, monkeypatch=monkeypatch
    )
    multi_recorder, multi_cursors, multi_outcome = _run_to_completion(
        tmp_path / 'multi', chunk_days=2, monkeypatch=monkeypatch
    )
    assert single_recorder.windows == [(_WINDOW_START, _WINDOW_END)]
    assert multi_recorder.windows == [
        (datetime(2026, 6, 10, tzinfo=UTC), datetime(2026, 6, 12, tzinfo=UTC)),
        (datetime(2026, 6, 12, tzinfo=UTC), datetime(2026, 6, 14, tzinfo=UTC)),
        (datetime(2026, 6, 14, tzinfo=UTC), _WINDOW_END),
    ]
    single_bytes = _partition_bytes(tmp_path / 'single')
    assert sorted(single_bytes) == [
        'date=2026-06-10',
        'date=2026-06-11',
        'date=2026-06-12',
        'date=2026-06-14',
    ]
    assert _partition_bytes(tmp_path / 'multi') == single_bytes
    assert multi_cursors.set_calls[-1] == single_cursors.set_calls[-1]
    assert single_outcome.records_fetched == multi_outcome.records_fetched == 5


def test_one_day_chunks_commit_per_day_with_identical_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The degenerate config: one-day units mean one ledger row per day and a
    # watermark advance as each ascending day completes (the truth
    # invariant, observable), with the same final bytes as one big unit.
    reference_root = tmp_path / 'reference'
    _run_to_completion(reference_root, chunk_days=7, monkeypatch=monkeypatch)
    recorder, cursors, _ = _run_to_completion(
        tmp_path / 'daily', chunk_days=1, monkeypatch=monkeypatch
    )
    assert recorder.windows == [
        (_daily_window(day).start, _daily_window(day).end) for day in range(10, 15)
    ]
    # 2026-06-13 held no rows: its unit completes without advancing.
    assert cursors.set_calls == [
        DateWatermark(watermark=datetime(2026, 6, 10, 20, tzinfo=UTC)),
        DateWatermark(watermark=datetime(2026, 6, 11, 9, tzinfo=UTC)),
        DateWatermark(watermark=datetime(2026, 6, 12, 10, tzinfo=UTC)),
        DateWatermark(watermark=datetime(2026, 6, 14, 12, tzinfo=UTC)),
    ]
    assert _partition_bytes(tmp_path / 'daily') == _partition_bytes(reference_root)


def test_crash_resume_drives_only_the_remaining_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Fail-fast at unit three of five, then resume: the completed prefix is
    # never refetched (the driver's window log is the request count), the
    # failed unit is claimable again, and the final state equals an
    # uninterrupted run's, byte for byte.
    root = tmp_path / 'resumed'
    _pin_shard_names(monkeypatch)
    first_recorder = _RecordingRecorder()
    cursors = _FaithfulCursorAccess()
    runner = _make_runner(first_recorder, root, cursors, chunk_days=1)
    failing_driver = _WindowRecordingDriver(_fleet_batches(), fail_on=_daily_window(12))

    with pytest.raises(ProviderResponseError, match='planted') as raised:
        runner.run(_definition(), failing_driver)

    assert raised.value is failing_driver.failure
    assert failing_driver.windows == [
        _daily_window(10),
        _daily_window(11),
        _daily_window(12),
    ]
    assert cursors.set_calls[-1] == DateWatermark(
        watermark=datetime(2026, 6, 11, 9, tzinfo=UTC)
    )
    assert len(first_recorder.failed) == 1
    assert sorted(_partition_bytes(root)) == ['date=2026-06-10', 'date=2026-06-11']
    progress = _open_store(root).progress(Provider.MOTIVE, _ENDPOINT)
    assert (progress.done, progress.failed, progress.pending) == (2, 1, 2)

    # The resumed invocation re-claims the failed unit and the pending tail,
    # ascending; the residual re-plan collapses onto the already-done daily
    # units, so nothing before the failure point is requested again.
    _pin_shard_names(monkeypatch)
    resumed_driver = _WindowRecordingDriver(_fleet_batches())
    second_runner = _make_runner(_RecordingRecorder(), root, cursors, chunk_days=1)
    outcome = second_runner.run(_definition(), resumed_driver)

    assert isinstance(outcome, Executed)
    assert resumed_driver.windows == [
        _daily_window(12),
        _daily_window(13),
        _daily_window(14),
    ]
    assert cursors.set_calls[-1] == DateWatermark(
        watermark=datetime(2026, 6, 14, 12, tzinfo=UTC)
    )
    final_progress = _open_store(root).progress(Provider.MOTIVE, _ENDPOINT)
    assert (final_progress.done, final_progress.failed, final_progress.pending) == (
        5,
        0,
        0,
    )
    uninterrupted_root = tmp_path / 'uninterrupted'
    _, uninterrupted_cursors, _ = _run_to_completion(
        uninterrupted_root, chunk_days=1, monkeypatch=monkeypatch
    )
    assert _partition_bytes(root) == _partition_bytes(uninterrupted_root)
    assert cursors.set_calls[-1] == uninterrupted_cursors.set_calls[-1]


def test_orphaned_claimed_unit_is_reclaimed_and_rerun_whole(
    tmp_path: Path,
) -> None:
    # A unit found claimed at run start is a prior invocation's orphan (one
    # driver per state database); it is reset and re-run whole -- and it
    # outranks the watermark, which already sits at the fleet's maximum.
    root = tmp_path / 'orphan'
    store = _open_store(root)
    store.enqueue(
        [
            WorkUnitSpec(
                provider=Provider.MOTIVE,
                endpoint=_ENDPOINT,
                partition_key=None,
                chunk_start=_WINDOW_START,
                chunk_end=_WINDOW_END,
            )
        ]
    )
    orphaned = store.claim_next(Provider.MOTIVE, _ENDPOINT, max_attempts=10)
    assert orphaned is not None  # left claimed: the simulated crash

    cursors = _FaithfulCursorAccess(
        DateWatermark(watermark=datetime(2026, 6, 14, 12, tzinfo=UTC))
    )
    driver = _WindowRecordingDriver(_fleet_batches())
    runner = _make_runner(_RecordingRecorder(), root, cursors, chunk_days=7)
    outcome = runner.run(_definition(), driver)

    assert isinstance(outcome, Executed)
    # First the orphaned unit, whole, despite the current watermark; then
    # the residual margin (watermark less lookback, floored) as a new unit.
    assert driver.windows == [
        DateWindow(start=_WINDOW_START, end=_WINDOW_END),
        DateWindow(start=datetime(2026, 6, 13, tzinfo=UTC), end=_WINDOW_END),
    ]
    # Neither drive observed anything past the seeded watermark: no advance.
    assert cursors.set_calls == []
    progress = _open_store(root).progress(Provider.MOTIVE, _ENDPOINT)
    assert (progress.done, progress.claimed) == (2, 0)


def test_persisted_unit_boundaries_survive_a_chunk_size_change(
    tmp_path: Path,
) -> None:
    # Boundary stability: units planned at one chunk size are honored as
    # stored when re-claimed under another; only the residual planning uses
    # the new size.
    root = tmp_path / 'stability'
    cursors = _FaithfulCursorAccess()
    failing_driver = _WindowRecordingDriver(
        _fleet_batches(),
        fail_on=DateWindow(start=datetime(2026, 6, 13, tzinfo=UTC), end=_WINDOW_END),
    )
    first_runner = _make_runner(_RecordingRecorder(), root, cursors, chunk_days=3)
    with pytest.raises(ProviderResponseError, match='planted'):
        first_runner.run(_definition(), failing_driver)
    assert failing_driver.windows == [
        DateWindow(start=_WINDOW_START, end=datetime(2026, 6, 13, tzinfo=UTC)),
        DateWindow(start=datetime(2026, 6, 13, tzinfo=UTC), end=_WINDOW_END),
    ]

    resumed_driver = _WindowRecordingDriver(_fleet_batches())
    second_runner = _make_runner(_RecordingRecorder(), root, cursors, chunk_days=1)
    outcome = second_runner.run(_definition(), resumed_driver)

    assert isinstance(outcome, Executed)
    # The persisted two-day unit is re-claimed with its stored bounds; the
    # residual margin (floored from the new watermark) plans at the new
    # one-day size.
    assert resumed_driver.windows == [
        DateWindow(start=datetime(2026, 6, 13, tzinfo=UTC), end=_WINDOW_END),
        _daily_window(13),
        _daily_window(14),
    ]
