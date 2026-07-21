"""The plan-and-drive unit loop through the real runner and work-unit store.

Every windowed run is planned as work units, driven ``backfill_unit_workers``
at a time, and watermarked by the prefix-advance rule (DESIGN section 5,
2026-07-20); these tests prove the loop's settled semantics end to end
against real parquet and a real migrated work-units store: boundary
invariance (unit count never changes the bytes), per-day prefix commits
under the degenerate one-day chunk, crash resume from the units ledger (no
completed unit refetched), the run-start prefix heal, orphan re-claim,
fail-fast with the completed prefix intact, persisted-boundary stability
across a ``backfill_chunk_days`` change, serial/parallel equivalence, and
the concurrent-prefix-commit guard. Deterministic throughout: canned
drivers, a frozen clock, pinned shard names for the byte comparisons, and
``workers=1`` wherever an assertion pins drive order (the parallel path has
its own order-free tests).
"""

import threading
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

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
from fleetpull.incremental import DateWatermark, DateWindow, IncrementalCursor
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.orchestrator.outcome import Executed
from fleetpull.orchestrator.runner import EndpointRunner
from fleetpull.orchestrator.spine import RunStateAccess
from fleetpull.state import (
    ClaimedWorkUnit,
    CursorStore,
    StateDatabase,
    WorkUnitSpec,
    WorkUnitStore,
    migrate_to_head,
)
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.orchestrator.doubles import (
    StubClientSource,
    StubPageDecoder,
    open_work_unit_store,
    partition_bytes,
    pin_shard_names,
)

_CLOCK_NOW = datetime(2026, 6, 16, tzinfo=UTC)
_ENDPOINT = 'locations'

# The resolved window for a cold start: default start 2026-06-10, trailing
# edge one cutoff day before the frozen clock -> [2026-06-10, 2026-06-15).
_WINDOW_START = datetime(2026, 6, 10, tzinfo=UTC)
_WINDOW_END = datetime(2026, 6, 15, tzinfo=UTC)


class _WatermarkModel(ResponseModel):
    occurred_at: datetime


class _RecordingRecorder:
    """A RunRecorder capturing the per-unit run lifecycle (lock-guarded --
    parallel unit workers open and close runs concurrently)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.windows: list[DateWindow] = []
        self.completed: list[tuple[int, int]] = []
        self.failed: list[tuple[int, str]] = []

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        raise AssertionError('a windowed run must never open a snapshot run')

    def start_feed_run(
        self, provider: Provider, endpoint: str, *, from_version: str
    ) -> int:
        raise AssertionError('a windowed run must never open a feed run')

    def start_window_run(
        self, provider: Provider, endpoint: str, *, window: DateWindow
    ) -> int:
        with self._lock:
            self.windows.append(window)
            return len(self.windows)

    def complete_run(
        self, run_id: int, *, row_count: int, to_version: str | None = None
    ) -> None:
        assert to_version is None, 'a windowed run completion carries no to_version'
        with self._lock:
            self.completed.append((run_id, row_count))

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        with self._lock:
            self.failed.append((run_id, error_detail))

    def coverage_frontier(self, provider: Provider, endpoint: str) -> datetime | None:
        return None


class _FaithfulCursorAccess:
    """A CursorAccess mirroring the real store: reads reflect writes, and the
    advance carries the store's forward-only guard, lock-guarded because
    parallel unit workers commit prefixes concurrently.

    ``applied_advances`` lists exactly the advances that moved the cursor, in
    commit order; a refused (not-strictly-forward) advance leaves no trace,
    as in the store.
    """

    def __init__(self, cursor: IncrementalCursor | None = None) -> None:
        self._lock = threading.Lock()
        self._cursor = cursor
        self.applied_advances: list[datetime] = []

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        with self._lock:
            return self._cursor

    def advance_watermark_forward(
        self, provider: Provider, endpoint: str, observed: datetime
    ) -> bool:
        with self._lock:
            stored = self._cursor
            if isinstance(stored, DateWatermark) and observed <= stored.watermark:
                return False
            self._cursor = DateWatermark(watermark=observed)
            self.applied_advances.append(observed)
            return True

    def commit_feed_token(
        self, provider: Provider, endpoint: str, to_version: str
    ) -> None:
        raise AssertionError('a windowed run must never commit a feed token')


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


def _definition(
    fixed_unit_days: int | None = None,
) -> EndpointDefinition[_WatermarkModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name=_ENDPOINT,
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v3/loc'),
        page_decoder=StubPageDecoder(),
        response_model=_WatermarkModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(
            lookback=timedelta(days=1),
            cutoff=timedelta(days=1),
            fixed_unit_days=fixed_unit_days,
        ),
        event_time_column='occurred_at',
    )


def _open_store(root: Path) -> WorkUnitStore:
    """The work-unit store over ``root``'s state database (created if absent)."""
    return open_work_unit_store(root, FrozenClock(start_time_utc=_CLOCK_NOW))


def _make_runner(
    recorder: _RecordingRecorder,
    root: Path,
    cursors: _FaithfulCursorAccess,
    chunk_days: int,
    workers: int = 1,
) -> EndpointRunner:
    # Serial units by default: the drive-order and request-log assertions
    # need the deterministic path. The equivalence test drives the same plan
    # with workers=4 to prove the prefix rule's order-independence.
    return EndpointRunner(
        StubClientSource(),
        RunStateAccess(recorder=recorder, cursors=cursors, units=_open_store(root)),
        FrozenClock(start_time_utc=_CLOCK_NOW),
        FleetpullConfig(
            sync=SyncConfig(
                default_start_date=date(2026, 6, 10),
                backfill_chunk_days=chunk_days,
                backfill_unit_workers=workers,
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


def _partition_bytes(root: Path) -> dict[str, bytes]:
    return partition_bytes(root / 'motive' / _ENDPOINT)


def _run_to_completion(
    root: Path, chunk_days: int, monkeypatch: pytest.MonkeyPatch
) -> tuple[_RecordingRecorder, _FaithfulCursorAccess, Executed]:
    """One uninterrupted invocation over the mock fleet, shard names pinned."""
    pin_shard_names(monkeypatch)
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
    assert single_recorder.windows == [DateWindow(start=_WINDOW_START, end=_WINDOW_END)]
    assert multi_recorder.windows == [
        DateWindow(
            start=datetime(2026, 6, 10, tzinfo=UTC),
            end=datetime(2026, 6, 12, tzinfo=UTC),
        ),
        DateWindow(
            start=datetime(2026, 6, 12, tzinfo=UTC),
            end=datetime(2026, 6, 14, tzinfo=UTC),
        ),
        DateWindow(start=datetime(2026, 6, 14, tzinfo=UTC), end=_WINDOW_END),
    ]
    single_bytes = _partition_bytes(tmp_path / 'single')
    assert sorted(single_bytes) == [
        'date=2026-06-10',
        'date=2026-06-11',
        'date=2026-06-12',
        'date=2026-06-14',
    ]
    assert _partition_bytes(tmp_path / 'multi') == single_bytes
    assert multi_cursors.applied_advances[-1] == single_cursors.applied_advances[-1]
    assert single_outcome.records_fetched == multi_outcome.records_fetched == 5


def test_a_declared_fixed_unit_width_overrides_the_configured_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The fixed-unit-width declaration (WatermarkMode.fixed_unit_days):
    # a window-grain rollup endpoint's unit width is part of the row's
    # meaning, so the declaration wins over sync.backfill_chunk_days --
    # under the same 7-day config, the declaring endpoint tiles the
    # resolved 5-day window into five one-day units while the
    # None-declaring sibling still plans one config-width unit.
    pin_shard_names(monkeypatch)
    declared_driver = _WindowRecordingDriver(_fleet_batches())
    declared_runner = _make_runner(
        _RecordingRecorder(),
        tmp_path / 'declared',
        _FaithfulCursorAccess(),
        chunk_days=7,
    )
    declared_outcome = declared_runner.run(
        _definition(fixed_unit_days=1), declared_driver
    )
    assert isinstance(declared_outcome, Executed)
    assert declared_driver.windows == [_daily_window(day) for day in range(10, 15)]

    pin_shard_names(monkeypatch)
    config_driver = _WindowRecordingDriver(_fleet_batches())
    config_runner = _make_runner(
        _RecordingRecorder(),
        tmp_path / 'config',
        _FaithfulCursorAccess(),
        chunk_days=7,
    )
    config_outcome = config_runner.run(_definition(), config_driver)
    assert isinstance(config_outcome, Executed)
    assert config_driver.windows == [DateWindow(start=_WINDOW_START, end=_WINDOW_END)]
    # The decomposition stays a transactional choice: both plans land
    # byte-identical partitions (the boundary-invariance property).
    assert _partition_bytes(tmp_path / 'declared') == _partition_bytes(
        tmp_path / 'config'
    )


def test_one_day_chunks_commit_per_day_with_identical_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The degenerate config: one-day units mean one ledger row per day and a
    # prefix commit advancing the watermark as each ascending day completes
    # (the truth invariant, observable), with the same final bytes as one
    # big unit.
    reference_root = tmp_path / 'reference'
    _run_to_completion(reference_root, chunk_days=7, monkeypatch=monkeypatch)
    recorder, cursors, _ = _run_to_completion(
        tmp_path / 'daily', chunk_days=1, monkeypatch=monkeypatch
    )
    assert recorder.windows == [_daily_window(day) for day in range(10, 15)]
    # 2026-06-13 held no rows: its unit records a NULL observation, the
    # prefix maximum is unchanged, and the forward-only guard applies
    # nothing for it.
    assert cursors.applied_advances == [
        datetime(2026, 6, 10, 20, tzinfo=UTC),
        datetime(2026, 6, 11, 9, tzinfo=UTC),
        datetime(2026, 6, 12, 10, tzinfo=UTC),
        datetime(2026, 6, 14, 12, tzinfo=UTC),
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
    pin_shard_names(monkeypatch)
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
    assert cursors.applied_advances[-1] == datetime(2026, 6, 11, 9, tzinfo=UTC)
    assert len(first_recorder.failed) == 1
    assert sorted(_partition_bytes(root)) == ['date=2026-06-10', 'date=2026-06-11']
    progress = _open_store(root).progress(Provider.MOTIVE, _ENDPOINT)
    assert (progress.done, progress.failed, progress.pending) == (2, 1, 2)

    # The resumed invocation re-claims the failed unit and the pending tail,
    # ascending. The residual plan then RELEASES and re-drives the lookback
    # margin behind the freshly advanced watermark (floor(06-14T12 - 1d) =
    # 06-13) -- the release-then-enqueue pairing, DESIGN section 5
    # corrected 2026-07-21: day-aligned tiles re-tile onto identical unit
    # keys, so without the release the margin would silently collapse onto
    # the kept done rows. Nothing before the margin is requested again.
    pin_shard_names(monkeypatch)
    resumed_driver = _WindowRecordingDriver(_fleet_batches())
    second_runner = _make_runner(_RecordingRecorder(), root, cursors, chunk_days=1)
    outcome = second_runner.run(_definition(), resumed_driver)

    assert isinstance(outcome, Executed)
    assert resumed_driver.windows == [
        _daily_window(12),
        _daily_window(13),
        _daily_window(14),
        _daily_window(13),
        _daily_window(14),
    ]
    assert cursors.applied_advances[-1] == datetime(2026, 6, 14, 12, tzinfo=UTC)
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
    assert cursors.applied_advances[-1] == uninterrupted_cursors.applied_advances[-1]


def test_the_next_runs_lookback_margin_refetches_done_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # THE RELEASE-THEN-ENQUEUE TRIPWIRE (DESIGN section 5, corrected
    # 2026-07-21): day-aligned units re-tile onto identical natural keys,
    # so without the planner's release the idempotent enqueue collapses
    # the lookback overlap onto the kept done rows and the late-arrival
    # refetch -- the lookback's entire purpose on mutating-rollup
    # surfaces -- silently never happens. The next day's run must
    # RE-drive the margin days (2026-06-13/14 here), not just the new
    # one. Any change that makes this drive only [15, 16) has revived
    # the collapse.
    root = tmp_path / 'daily'
    _, cursors, _ = _run_to_completion(root, chunk_days=1, monkeypatch=monkeypatch)
    assert cursors.applied_advances[-1] == datetime(2026, 6, 14, 12, tzinfo=UTC)

    # One day later: trailing edge 2026-06-16, stored watermark
    # 2026-06-14T12 -> residual [floor(06-13T12), 06-16) = [06-13, 06-16).
    pin_shard_names(monkeypatch)
    next_day_driver = _WindowRecordingDriver(_fleet_batches())
    next_day_runner = EndpointRunner(
        StubClientSource(),
        RunStateAccess(
            recorder=_RecordingRecorder(), cursors=cursors, units=_open_store(root)
        ),
        FrozenClock(start_time_utc=datetime(2026, 6, 17, tzinfo=UTC)),
        FleetpullConfig(
            sync=SyncConfig(
                default_start_date=date(2026, 6, 10),
                backfill_chunk_days=1,
                backfill_unit_workers=1,
            ),
            storage=StorageConfig(dataset_root=root),
            providers=ProvidersConfig(),
        ),
    )
    outcome = next_day_runner.run(_definition(), next_day_driver)

    assert isinstance(outcome, Executed)
    assert next_day_driver.windows == [
        _daily_window(13),
        _daily_window(14),
        _daily_window(15),
    ]
    # The refetched margin re-observes the same maximum; the forward-only
    # guard applies nothing new, and the rewritten partition set is
    # unchanged (2026-06-13 and -15 hold no rows).
    assert cursors.applied_advances[-1] == datetime(2026, 6, 14, 12, tzinfo=UTC)
    progress = _open_store(root).progress(Provider.MOTIVE, _ENDPOINT)
    assert (progress.done, progress.failed, progress.pending) == (6, 0, 0)
    assert sorted(_partition_bytes(root)) == [
        'date=2026-06-10',
        'date=2026-06-11',
        'date=2026-06-12',
        'date=2026-06-14',
    ]


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
    orphaned = store.claim_next(Provider.MOTIVE, _ENDPOINT)
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
    # Neither drive observed anything past the seeded watermark: every
    # prefix commit is refused by the forward-only guard, nothing applies.
    assert cursors.applied_advances == []
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


def test_startup_prefix_heal_advances_the_cursor_before_any_claim(
    tmp_path: Path,
) -> None:
    # A crash between a unit's done-mark and its prefix commit leaves the
    # observation recorded but the cursor behind -- simulated by driving the
    # store directly (mark_done recorded, no commit ever ran). The fresh
    # invocation's run-start commit_prefix heals the cursor BEFORE any
    # claim: the healed advance is the first applied, ahead of every
    # unit-completion commit, even with a claimable unit waiting.
    root = tmp_path / 'healed'
    store = _open_store(root)
    store.enqueue(
        [
            WorkUnitSpec(
                provider=Provider.MOTIVE,
                endpoint=_ENDPOINT,
                partition_key=None,
                chunk_start=datetime(2026, 6, 10, tzinfo=UTC),
                chunk_end=datetime(2026, 6, 11, tzinfo=UTC),
            ),
            WorkUnitSpec(
                provider=Provider.MOTIVE,
                endpoint=_ENDPOINT,
                partition_key=None,
                chunk_start=datetime(2026, 6, 11, tzinfo=UTC),
                chunk_end=datetime(2026, 6, 12, tzinfo=UTC),
            ),
        ]
    )
    crashed = store.claim_next(Provider.MOTIVE, _ENDPOINT)
    assert crashed is not None
    store.mark_done(crashed.unit_id, observed_max=datetime(2026, 6, 10, 20, tzinfo=UTC))

    cursors = _FaithfulCursorAccess()
    driver = _WindowRecordingDriver(_fleet_batches())
    runner = _make_runner(_RecordingRecorder(), root, cursors, chunk_days=1)
    outcome = runner.run(_definition(), driver)

    assert isinstance(outcome, Executed)
    # The heal's advance lands first; the pending unit is only claimed and
    # driven afterward (its window is the first the driver sees).
    assert cursors.applied_advances[0] == datetime(2026, 6, 10, 20, tzinfo=UTC)
    assert driver.windows[0] == _daily_window(11)
    # The rest of the run proceeds normally: the pending unit and the
    # residual advance the prefix past the healed value.
    assert cursors.applied_advances == [
        datetime(2026, 6, 10, 20, tzinfo=UTC),
        datetime(2026, 6, 11, 9, tzinfo=UTC),
        datetime(2026, 6, 12, 10, tzinfo=UTC),
        datetime(2026, 6, 14, 12, tzinfo=UTC),
    ]


def test_multi_worker_run_matches_the_serial_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The prefix rule's order-independence end to end: workers=1 runs
    # inline on the calling thread (no pool); a 4-worker run of the same
    # five-unit plan must land identical bytes, an identical merged
    # outcome, and the same final watermark, whatever the completion order.
    pin_shard_names(monkeypatch)
    serial_cursors = _FaithfulCursorAccess()
    serial_runner = _make_runner(
        _RecordingRecorder(), tmp_path / 'serial', serial_cursors, chunk_days=1
    )
    serial_outcome = serial_runner.run(
        _definition(), _WindowRecordingDriver(_fleet_batches())
    )

    pin_shard_names(monkeypatch)
    parallel_cursors = _FaithfulCursorAccess()
    parallel_runner = _make_runner(
        _RecordingRecorder(),
        tmp_path / 'parallel',
        parallel_cursors,
        chunk_days=1,
        workers=4,
    )
    parallel_outcome = parallel_runner.run(
        _definition(), _WindowRecordingDriver(_fleet_batches())
    )

    assert isinstance(serial_outcome, Executed)
    assert isinstance(parallel_outcome, Executed)
    assert parallel_outcome.records_fetched == serial_outcome.records_fetched == 5
    assert parallel_outcome.latest_observed == serial_outcome.latest_observed
    assert parallel_outcome.write.rows_written == serial_outcome.write.rows_written
    assert _partition_bytes(tmp_path / 'parallel') == _partition_bytes(
        tmp_path / 'serial'
    )
    # The final applied advance is the plan's maximum either way; so is the
    # cursor a read-back sees.
    assert (
        parallel_cursors.applied_advances[-1]
        == serial_cursors.applied_advances[-1]
        == datetime(2026, 6, 14, 12, tzinfo=UTC)
    )
    assert parallel_cursors.get_cursor(
        Provider.MOTIVE, _ENDPOINT
    ) == serial_cursors.get_cursor(Provider.MOTIVE, _ENDPOINT)


class _ScriptedUnits:
    """A UnitQueue serving only scripted prefix observations, in arrival order.

    The race test drives ``_commit_watermark_prefix`` directly, so every
    other queue method is unreachable and says so loudly.
    """

    def __init__(self, observations: list[datetime]) -> None:
        self._lock = threading.Lock()
        self._observations = observations

    def enqueue(self, units: list[WorkUnitSpec]) -> int:
        raise AssertionError('the race test never enqueues')

    def release_done_units(
        self, provider: Provider, endpoint: str, *, window: DateWindow
    ) -> int:
        raise AssertionError('the race test never releases')

    def reset_claimed_to_pending(self, provider: Provider, endpoint: str) -> int:
        raise AssertionError('the race test never resets')

    def claim_next(self, provider: Provider, endpoint: str) -> ClaimedWorkUnit | None:
        raise AssertionError('the race test never claims')

    def mark_done(self, unit_id: int, *, observed_max: datetime | None) -> None:
        raise AssertionError('the race test never completes units')

    def mark_failed(self, unit_id: int, *, error_detail: str) -> None:
        raise AssertionError('the race test never fails units')

    def done_prefix_observation(
        self, provider: Provider, endpoint: str
    ) -> datetime | None:
        with self._lock:
            return self._observations.pop(0)


def test_concurrent_prefix_commits_cannot_regress_the_cursor(tmp_path: Path) -> None:
    # Two _commit_watermark_prefix invocations with interleaved prefix
    # reads: the stale thread's whole advance is held until the fresh
    # commit lands, proving the runner-level choreography cannot regress
    # the cursor. The finer race -- a read-compare-write straddling the
    # fresh commit INSIDE the store method -- is pinned by the store-level
    # guard-placement test (tests/state/test_cursors.py).
    stale_observation = datetime(2026, 6, 11, 9, tzinfo=UTC)
    fresh_observation = datetime(2026, 6, 14, 12, tzinfo=UTC)
    database = StateDatabase(tmp_path / 'state.sqlite3')
    database.initialize()
    migrate_to_head(database)
    cursor_store = CursorStore(database, FrozenClock(start_time_utc=_CLOCK_NOW))
    fresh_committed = threading.Event()
    advance_results: dict[datetime, bool] = {}
    results_lock = threading.Lock()

    class _GatedCursors:
        """The real store, with the stale write held until the fresh commit lands."""

        def get_cursor(
            self, provider: Provider, endpoint: str
        ) -> IncrementalCursor | None:
            return cursor_store.get_cursor(provider, endpoint)

        def advance_watermark_forward(
            self, provider: Provider, endpoint: str, observed: datetime
        ) -> bool:
            if observed == stale_observation:
                assert fresh_committed.wait(timeout=5)
            advanced = cursor_store.advance_watermark_forward(
                provider, endpoint, observed
            )
            with results_lock:
                advance_results[observed] = advanced
            if observed == fresh_observation:
                fresh_committed.set()
            return advanced

        def commit_feed_token(
            self, provider: Provider, endpoint: str, to_version: str
        ) -> None:
            raise AssertionError('a windowed run must never commit a feed token')

    runner = EndpointRunner(
        StubClientSource(),
        RunStateAccess(
            recorder=_RecordingRecorder(),
            cursors=_GatedCursors(),
            units=_ScriptedUnits([stale_observation, fresh_observation]),
        ),
        FrozenClock(start_time_utc=_CLOCK_NOW),
        FleetpullConfig(
            sync=SyncConfig(default_start_date=date(2026, 6, 10)),
            storage=StorageConfig(dataset_root=tmp_path),
            providers=ProvidersConfig(),
        ),
    )
    commit_threads = [
        threading.Thread(
            target=runner._watermark_drive._commit_watermark_prefix,
            args=(Provider.MOTIVE, _ENDPOINT),
        )
        for _ in range(2)
    ]
    for commit_thread in commit_threads:
        commit_thread.start()
    for commit_thread in commit_threads:
        commit_thread.join(timeout=10)
        assert not commit_thread.is_alive()

    assert advance_results == {fresh_observation: True, stale_observation: False}
    assert cursor_store.get_cursor(Provider.MOTIVE, _ENDPOINT) == DateWatermark(
        watermark=fresh_observation
    )
