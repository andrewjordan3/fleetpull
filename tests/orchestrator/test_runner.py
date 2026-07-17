"""Tests for fleetpull.orchestrator.runner."""

import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

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
    FeedMode,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import ConfigurationError, ProviderResponseError
from fleetpull.incremental import (
    DateWatermark,
    FeedToken,
    IncrementalCursor,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.outcome import CaughtUp, Executed
from fleetpull.orchestrator.runner import (
    ClientSource,
    CursorAccess,
    EndpointRunner,
    RunStateAccess,
)
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.orchestrator.doubles import (
    CannedDriver,
    FailingDriver,
    StubClientSource,
    StubPageDecoder,
    open_work_unit_store,
)

# The _make_runner default clock instant, and the trailing edge it implies for a
# one-day cutoff: midnight(2026-06-16) - 1 day.
_CLOCK_NOW = datetime(2026, 6, 16, tzinfo=UTC)
_TRAILING_EDGE = datetime(2026, 6, 15, tzinfo=UTC)


class _SnapshotModel(ResponseModel):
    id: int
    name: str


class _WatermarkModel(ResponseModel):
    occurred_at: datetime


class _EmptyClientSource:
    """A ClientSource that rejects every provider (resolve-before-open ordering)."""

    def client_for(self, provider: Provider) -> TransportClient:
        raise ConfigurationError('no client', provider=provider.value)


class _RecordingRecorder:
    """A RunRecorder capturing the run lifecycle calls."""

    def __init__(self) -> None:
        self.started: list[tuple[Provider, str]] = []
        self.windows: list[tuple[datetime, datetime]] = []
        self.completed: list[tuple[int, int]] = []
        self.failed: list[tuple[int, str]] = []
        self.frontier: datetime | None = None

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        self.started.append((provider, endpoint))
        return len(self.started)

    def start_window_run(
        self, provider: Provider, endpoint: str, *, window: tuple[datetime, datetime]
    ) -> int:
        self.started.append((provider, endpoint))
        self.windows.append(window)
        return len(self.started)

    def complete_run(self, run_id: int, *, row_count: int) -> None:
        self.completed.append((run_id, row_count))

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        self.failed.append((run_id, error_detail))

    def coverage_frontier(self, provider: Provider, endpoint: str) -> datetime | None:
        return self.frontier


class _CompleteFailingRecorder(_RecordingRecorder):
    """A RunRecorder whose complete_run raises (a completion-write failure)."""

    def complete_run(self, run_id: int, *, row_count: int) -> None:
        raise RuntimeError('completion write failed')


class _FailRunFailingRecorder(_RecordingRecorder):
    """A RunRecorder whose fail_run also raises (to test masking-prevention)."""

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        raise RuntimeError('ledger down')


class _StubCursorAccess:
    """A CursorAccess with a settable stored cursor that records its writes."""

    def __init__(self, cursor: IncrementalCursor | None = None) -> None:
        self._cursor = cursor
        self.set_calls: list[tuple[Provider, str, IncrementalCursor]] = []

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        return self._cursor

    def set_cursor(
        self, provider: Provider, endpoint: str, cursor: IncrementalCursor
    ) -> None:
        self.set_calls.append((provider, endpoint, cursor))


class _ApplyingCursorAccess(_StubCursorAccess):
    """A CursorAccess whose writes apply, so a read-back sees the new cursor."""

    def set_cursor(
        self, provider: Provider, endpoint: str, cursor: IncrementalCursor
    ) -> None:
        super().set_cursor(provider, endpoint, cursor)
        self._cursor = cursor


def _snapshot_definition() -> EndpointDefinition[_SnapshotModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicles',
        spec_builder=StaticGetSpecBuilder(
            base_url='https://x.test', path='/v1/vehicles'
        ),
        page_decoder=StubPageDecoder(),
        response_model=_SnapshotModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )


def _watermark_definition() -> EndpointDefinition[_WatermarkModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='locations',
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v3/loc'),
        page_decoder=StubPageDecoder(),
        response_model=_WatermarkModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=1)),
        event_time_column='occurred_at',
    )


def _feed_definition() -> EndpointDefinition[_SnapshotModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='feed',
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v1/feed'),
        page_decoder=StubPageDecoder(),
        response_model=_SnapshotModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=FeedMode(),
    )


# A test construction helper centralizing the runner's collaborators plus the
# cold-start date, which do not bundle into a meaningful object -- so the
# >5-arg count is intrinsic here.
def _make_runner(  # noqa: PLR0913
    recorder: _RecordingRecorder,
    tmp_path: Path,
    *,
    client_source: ClientSource | None = None,
    clock: FrozenClock | None = None,
    cursor_access: CursorAccess | None = None,
    default_start_date: date = date(2024, 1, 1),
) -> EndpointRunner:
    run_clock = clock or FrozenClock(start_time_utc=_CLOCK_NOW)
    return EndpointRunner(
        client_source or StubClientSource(),
        RunStateAccess(
            recorder=recorder,
            cursors=cursor_access or _StubCursorAccess(),
            units=open_work_unit_store(tmp_path, run_clock),
        ),
        run_clock,
        FleetpullConfig(
            sync=SyncConfig(default_start_date=default_start_date),
            storage=StorageConfig(dataset_root=tmp_path),
            providers=ProvidersConfig(),
        ),
    )


def _wm_batch(*occurred_ats: datetime) -> list[JsonObject]:
    return [{'occurred_at': moment.isoformat()} for moment in occurred_ats]


def test_snapshot_run_executes_writes_and_records(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path)
    records: list[JsonObject] = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]
    outcome = runner.run(_snapshot_definition(), CannedDriver([records]))
    assert isinstance(outcome, Executed)
    assert outcome.records_fetched == 2
    assert outcome.write.rows_written == 2
    assert recorder.completed == [(1, 2)]
    assert recorder.failed == []
    written = tmp_path / 'motive' / 'vehicles' / 'data.parquet'
    assert written.exists()
    assert pl.read_parquet(written).height == 2


def test_empty_snapshot_writes_empty_dataset_and_completes(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path)
    outcome = runner.run(_snapshot_definition(), CannedDriver([[]]))
    assert isinstance(outcome, Executed)
    assert outcome.records_fetched == 0
    assert recorder.completed == [(1, 0)]
    written = tmp_path / 'motive' / 'vehicles' / 'data.parquet'
    assert written.exists()
    assert pl.read_parquet(written).height == 0


def test_fetch_failure_records_failure_and_reraises(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path)
    with pytest.raises(RuntimeError, match='fetch blew up'):
        runner.run(_snapshot_definition(), FailingDriver())
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_validation_failure_records_failure_and_reraises(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path)
    bad_batch: list[JsonObject] = [{'name': 'missing id'}]
    with pytest.raises(ProviderResponseError):
        runner.run(_snapshot_definition(), CannedDriver([bad_batch]))
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_completion_failure_records_failure_and_reraises(tmp_path: Path) -> None:
    recorder = _CompleteFailingRecorder()
    runner = _make_runner(recorder, tmp_path)
    records: list[JsonObject] = [{'id': 1, 'name': 'a'}]
    with pytest.raises(RuntimeError, match='completion write failed'):
        runner.run(_snapshot_definition(), CannedDriver([records]))
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_fail_run_failure_does_not_mask_original_error(tmp_path: Path) -> None:
    recorder = _FailRunFailingRecorder()
    runner = _make_runner(recorder, tmp_path)
    with pytest.raises(RuntimeError, match='fetch blew up'):
        runner.run(_snapshot_definition(), FailingDriver())


def test_unresolvable_client_opens_no_run(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path, client_source=_EmptyClientSource())
    with pytest.raises(ConfigurationError):
        runner.run(_snapshot_definition(), CannedDriver([]))
    assert recorder.started == []


def test_feed_mode_is_not_yet_executable(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path)
    with pytest.raises(NotImplementedError):
        runner.run(_feed_definition(), CannedDriver([]))


class TestBatchObserver:
    def test_snapshot_run_hands_each_validated_batch_to_the_observer(
        self, tmp_path: Path
    ) -> None:
        # The observer sees post-validation frames: model field names (the
        # records-layer flatten), not wire aliases, one row per validated
        # record -- the contract the feeder tap relies on.
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)
        observed: list[pl.DataFrame] = []
        records: list[JsonObject] = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]
        outcome = runner.run(
            _snapshot_definition(), CannedDriver([records]), observed.append
        )
        assert isinstance(outcome, Executed)
        assert [frame.columns for frame in observed] == [['id', 'name']]
        assert observed[0].height == 2

    def test_watermark_run_observes_the_window_filtered_frames(
        self, tmp_path: Path
    ) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path, default_start_date=date(2026, 6, 12))
        observed: list[pl.DataFrame] = []
        batch = _wm_batch(
            datetime(2026, 6, 11, 8, tzinfo=UTC),  # out of window: filtered
            datetime(2026, 6, 13, 9, tzinfo=UTC),  # in window
        )
        runner.run(_watermark_definition(), CannedDriver([batch]), observed.append)
        assert len(observed) == 1
        assert observed[0].height == 1

    def test_observer_failure_fails_the_run(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)

        def exploding_observer(frame: pl.DataFrame) -> None:
            raise ValueError('observer blew up')

        records: list[JsonObject] = [{'id': 1, 'name': 'a'}]
        with pytest.raises(ValueError, match='observer blew up'):
            runner.run(
                _snapshot_definition(), CannedDriver([records]), exploding_observer
            )
        assert recorder.completed == []
        assert len(recorder.failed) == 1


class TestWatermarkRun:
    def test_cold_start_runs_from_default_and_advances_cursor(
        self, tmp_path: Path
    ) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess()
        runner = _make_runner(
            recorder,
            tmp_path,
            cursor_access=cursor,
            default_start_date=date(2026, 6, 12),
        )
        batch = _wm_batch(
            datetime(2026, 6, 12, 8, tzinfo=UTC),
            datetime(2026, 6, 13, 9, tzinfo=UTC),
            datetime(2026, 6, 14, 10, tzinfo=UTC),
        )
        outcome = runner.run(_watermark_definition(), CannedDriver([batch]))
        assert isinstance(outcome, Executed)
        assert outcome.records_fetched == 3
        assert recorder.windows == [(datetime(2026, 6, 12, tzinfo=UTC), _TRAILING_EDGE)]
        assert cursor.set_calls == [
            (
                Provider.MOTIVE,
                'locations',
                DateWatermark(watermark=datetime(2026, 6, 14, 10, tzinfo=UTC)),
            )
        ]
        assert recorder.completed == [(1, 3)]
        assert (tmp_path / 'motive' / 'locations' / 'date=2026-06-14').exists()

    def test_caught_up_opens_no_run(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 16, 8, tzinfo=UTC))
        )
        clock = FrozenClock(start_time_utc=datetime(2026, 6, 16, 12, tzinfo=UTC))
        runner = _make_runner(recorder, tmp_path, clock=clock, cursor_access=cursor)
        outcome = runner.run(
            _watermark_definition(),
            CannedDriver([_wm_batch(datetime(2026, 6, 16, 9, tzinfo=UTC))]),
        )
        assert isinstance(outcome, CaughtUp)
        assert recorder.started == []
        assert cursor.set_calls == []
        assert not (tmp_path / 'motive' / 'locations').exists()

    def test_steady_advance_window_starts_at_watermark_minus_lookback(
        self, tmp_path: Path
    ) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 13, tzinfo=UTC))
        )
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        batch = _wm_batch(
            datetime(2026, 6, 12, 8, tzinfo=UTC),
            datetime(2026, 6, 14, 10, tzinfo=UTC),
        )
        runner.run(_watermark_definition(), CannedDriver([batch]))
        assert recorder.windows == [(datetime(2026, 6, 12, tzinfo=UTC), _TRAILING_EDGE)]
        assert cursor.set_calls == [
            (
                Provider.MOTIVE,
                'locations',
                DateWatermark(watermark=datetime(2026, 6, 14, 10, tzinfo=UTC)),
            )
        ]

    def test_no_cursor_resumes_from_the_coverage_frontier_without_lookback(
        self, tmp_path: Path
    ) -> None:
        # Resume arm 2 (DESIGN section 4): no stored cursor, but a succeeded
        # run's coverage frontier exists. The window starts at the frontier
        # floored to its UTC midnight -- no lookback applies (that is arm 1's
        # re-fetch margin) and the cold-start anchor is outranked. A
        # lookback-applied start would be 2026-06-12; an arm-3 start would be
        # the 2026-06-10 anchor.
        recorder = _RecordingRecorder()
        recorder.frontier = datetime(2026, 6, 13, 4, tzinfo=UTC)
        cursor = _StubCursorAccess()
        runner = _make_runner(
            recorder,
            tmp_path,
            cursor_access=cursor,
            default_start_date=date(2026, 6, 10),
        )
        batch = _wm_batch(datetime(2026, 6, 14, 9, tzinfo=UTC))
        outcome = runner.run(_watermark_definition(), CannedDriver([batch]))
        assert isinstance(outcome, Executed)
        assert recorder.windows == [(datetime(2026, 6, 13, tzinfo=UTC), _TRAILING_EDGE)]
        assert cursor.set_calls == [
            (
                Provider.MOTIVE,
                'locations',
                DateWatermark(watermark=datetime(2026, 6, 14, 9, tzinfo=UTC)),
            )
        ]

    def test_late_day_watermark_refetches_the_boundary_day_whole_not_a_sliver(
        self, tmp_path: Path
    ) -> None:
        # The live sliver defect (mode a): a watermark of 2026-06-14T23:59:59
        # less the 1-day lookback resolved an unfloored start of
        # 2026-06-13T23:59:59; the day-granular fetch returned the whole
        # boundary day, in_window kept only the final second, and wholesale
        # replacement rewrote date=2026-06-13 as a seconds-wide sliver.
        # Floored, the window starts at the boundary day's midnight and the
        # full refetched day survives; a record before the floored start (the
        # provider-overshoot case) is still dropped.
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 14, 23, 59, 59, tzinfo=UTC))
        )
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        batch = _wm_batch(
            datetime(2026, 6, 12, 23, 0, tzinfo=UTC),  # overshoot: before start
            datetime(2026, 6, 13, 0, 30, tzinfo=UTC),
            datetime(2026, 6, 13, 12, 0, tzinfo=UTC),
            datetime(2026, 6, 13, 23, 59, 59, tzinfo=UTC),
        )
        outcome = runner.run(_watermark_definition(), CannedDriver([batch]))
        assert isinstance(outcome, Executed)
        assert recorder.windows == [(datetime(2026, 6, 13, tzinfo=UTC), _TRAILING_EDGE)]
        assert outcome.records_fetched == 3
        part = pl.read_parquet(
            tmp_path / 'motive' / 'locations' / 'date=2026-06-13' / 'part.parquet'
        )
        assert part.height == 3
        assert not (tmp_path / 'motive' / 'locations' / 'date=2026-06-12').exists()
        # Watermark semantics unchanged: max kept event time (2026-06-13) is
        # not strictly past the stored watermark, so no advance.
        assert cursor.set_calls == []

    def test_boundary_partition_with_data_is_never_pruned_on_resume(
        self, tmp_path: Path
    ) -> None:
        # The latent mode (b) of the same defect: under the unfloored window
        # [2026-06-13T23:59:59, ...), a boundary day whose refetch held no
        # records in that final second was covered-but-unwritten, and the
        # prune deleted its complete partition outright. Floored, the day's
        # refetched rows are kept and written, so the partition is replaced,
        # never pruned.
        endpoint_dir = tmp_path / 'motive' / 'locations'
        prior_partition = endpoint_dir / 'date=2026-06-13'
        prior_partition.mkdir(parents=True)
        pl.DataFrame(
            {'occurred_at': [datetime(2026, 6, 13, 8, tzinfo=UTC)]}
        ).write_parquet(prior_partition / 'part.parquet')
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 14, 23, 59, 59, tzinfo=UTC))
        )
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        refetched = _wm_batch(datetime(2026, 6, 13, 8, tzinfo=UTC))
        outcome = runner.run(_watermark_definition(), CannedDriver([refetched]))
        assert isinstance(outcome, Executed)
        assert outcome.write.deleted_partitions == []
        assert (prior_partition / 'part.parquet').exists()
        assert pl.read_parquet(prior_partition / 'part.parquet').height == 1

    def test_non_advancing_observation_holds_the_cursor(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 14, 12, tzinfo=UTC))
        )
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        batch = _wm_batch(
            datetime(2026, 6, 13, 13, tzinfo=UTC),
            datetime(2026, 6, 14, 9, tzinfo=UTC),
        )
        runner.run(_watermark_definition(), CannedDriver([batch]))
        assert cursor.set_calls == []
        assert recorder.completed == [(1, 2)]

    def test_empty_fetch_completes_without_advancing(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess()
        runner = _make_runner(
            recorder,
            tmp_path,
            cursor_access=cursor,
            default_start_date=date(2026, 6, 12),
        )
        outcome = runner.run(_watermark_definition(), CannedDriver([[]]))
        assert isinstance(outcome, Executed)
        assert outcome.records_fetched == 0
        assert cursor.set_calls == []
        assert recorder.completed == [(1, 0)]

    def test_out_of_window_rows_are_dropped(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess()
        runner = _make_runner(
            recorder,
            tmp_path,
            cursor_access=cursor,
            default_start_date=date(2026, 6, 12),
        )
        batch = _wm_batch(
            datetime(2026, 6, 11, 8, tzinfo=UTC),  # before start (06-12): out
            datetime(2026, 6, 13, 9, tzinfo=UTC),  # in
            datetime(2026, 6, 15, 1, tzinfo=UTC),  # at/after end (06-15): out
        )
        outcome = runner.run(_watermark_definition(), CannedDriver([batch]))
        assert isinstance(outcome, Executed)
        assert outcome.records_fetched == 1
        assert cursor.set_calls == [
            (
                Provider.MOTIVE,
                'locations',
                DateWatermark(watermark=datetime(2026, 6, 13, 9, tzinfo=UTC)),
            )
        ]
        assert (tmp_path / 'motive' / 'locations' / 'date=2026-06-13').exists()
        assert not (tmp_path / 'motive' / 'locations' / 'date=2026-06-15').exists()

    def test_guard_a_future_watermark_raises_before_opening_a_run(
        self, tmp_path: Path
    ) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 20, tzinfo=UTC))
        )
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        with pytest.raises(ConfigurationError, match='future'):
            runner.run(_watermark_definition(), CannedDriver([]))
        assert recorder.started == []
        assert cursor.set_calls == []

    def test_guard_b_future_event_fails_the_run(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess()
        runner = _make_runner(
            recorder,
            tmp_path,
            cursor_access=cursor,
            default_start_date=date(2026, 6, 12),
        )
        batch = _wm_batch(datetime(2026, 6, 17, 8, tzinfo=UTC))  # after now (06-16)
        with pytest.raises(ProviderResponseError):
            runner.run(_watermark_definition(), CannedDriver([batch]))
        assert len(recorder.failed) == 1
        assert recorder.completed == []
        assert cursor.set_calls == []

    def test_feed_cursor_on_a_watermark_endpoint_raises(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(FeedToken(from_version='v1'))
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        with pytest.raises(ConfigurationError, match='feed cursor'):
            runner.run(_watermark_definition(), CannedDriver([]))
        assert recorder.started == []

    def test_fetch_failure_records_failure_and_reraises(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path, default_start_date=date(2026, 6, 12))
        with pytest.raises(RuntimeError, match='fetch blew up'):
            runner.run(_watermark_definition(), FailingDriver())
        assert recorder.completed == []
        assert len(recorder.failed) == 1

    def test_validation_failure_records_failure_and_reraises(
        self, tmp_path: Path
    ) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path, default_start_date=date(2026, 6, 12))
        bad_batch: list[JsonObject] = [{'wrong': 'shape'}]
        with pytest.raises(ProviderResponseError):
            runner.run(_watermark_definition(), CannedDriver([bad_batch]))
        assert recorder.completed == []
        assert len(recorder.failed) == 1

    def test_cursor_is_written_before_completion(self, tmp_path: Path) -> None:
        # Crash ordering: set_cursor lands before complete_run; a completion
        # failure leaves the cursor advanced (and the run merely running).
        recorder = _CompleteFailingRecorder()
        cursor = _StubCursorAccess()
        runner = _make_runner(
            recorder,
            tmp_path,
            cursor_access=cursor,
            default_start_date=date(2026, 6, 12),
        )
        batch = _wm_batch(datetime(2026, 6, 13, 9, tzinfo=UTC))
        with pytest.raises(RuntimeError, match='completion write failed'):
            runner.run(_watermark_definition(), CannedDriver([batch]))
        assert cursor.set_calls == [
            (
                Provider.MOTIVE,
                'locations',
                DateWatermark(watermark=datetime(2026, 6, 13, 9, tzinfo=UTC)),
            )
        ]
        assert recorder.completed == []
        assert len(recorder.failed) == 1

    def test_fail_run_failure_does_not_mask_original_error(
        self, tmp_path: Path
    ) -> None:
        recorder = _FailRunFailingRecorder()
        runner = _make_runner(recorder, tmp_path, default_start_date=date(2026, 6, 12))
        with pytest.raises(RuntimeError, match='fetch blew up'):
            runner.run(_watermark_definition(), FailingDriver())


def _info_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        log_record.getMessage()
        for log_record in caplog.records
        if log_record.levelno == logging.INFO
    ]


class TestNarration:
    """The runner's INFO progress lines (DESIGN section 13's settled policy).

    Substring assertions on ``getMessage()`` only -- never whole formatted
    lines -- so format tweaks do not break the pins.
    """

    def test_snapshot_run_narrates_start_and_completion(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)
        records: list[JsonObject] = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]
        with caplog.at_level(logging.INFO):
            runner.run(_snapshot_definition(), CannedDriver([records]))
        started_lines = [
            message
            for message in _info_messages(caplog)
            if 'endpoint started:' in message
        ]
        assert len(started_lines) == 1
        assert 'provider=motive' in started_lines[0]
        assert 'endpoint=vehicles' in started_lines[0]
        assert 'mode=snapshot' in started_lines[0]
        completed_lines = [
            message
            for message in _info_messages(caplog)
            if 'endpoint complete:' in message
        ]
        assert len(completed_lines) == 1
        assert 'records_fetched=2' in completed_lines[0]
        assert 'rows_written=2' in completed_lines[0]
        assert 'duplicates_dropped=0' in completed_lines[0]
        assert 'files_written=1' in completed_lines[0]
        assert 'deleted_partitions=0' in completed_lines[0]
        assert 'elapsed_seconds=' in completed_lines[0]

    def test_watermark_run_narrates_the_plan_and_each_unit(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path, default_start_date=date(2026, 6, 12))
        batch = _wm_batch(datetime(2026, 6, 13, 9, tzinfo=UTC))
        with caplog.at_level(logging.INFO):
            runner.run(_watermark_definition(), CannedDriver([batch]))
        info_messages = _info_messages(caplog)
        started_lines = [
            message for message in info_messages if 'endpoint started:' in message
        ]
        assert len(started_lines) == 1
        assert 'mode=watermark' in started_lines[0]
        planned_lines = [
            message for message in info_messages if 'window planned:' in message
        ]
        assert len(planned_lines) == 1
        assert 'provider=motive' in planned_lines[0]
        assert 'endpoint=locations' in planned_lines[0]
        assert 'window_start=2026-06-12T00:00:00Z' in planned_lines[0]
        assert 'window_end=2026-06-15T00:00:00Z' in planned_lines[0]
        assert 'claimable_units=1' in planned_lines[0]
        unit_lines = [
            message for message in info_messages if 'unit complete:' in message
        ]
        assert len(unit_lines) == 1
        assert 'window_start=2026-06-12T00:00:00Z' in unit_lines[0]
        assert 'window_end=2026-06-15T00:00:00Z' in unit_lines[0]
        assert 'records_fetched=1' in unit_lines[0]

    def test_caught_up_run_narrates_no_completion_line(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 16, 8, tzinfo=UTC))
        )
        clock = FrozenClock(start_time_utc=datetime(2026, 6, 16, 12, tzinfo=UTC))
        runner = _make_runner(recorder, tmp_path, clock=clock, cursor_access=cursor)
        with caplog.at_level(logging.INFO):
            runner.run(_watermark_definition(), CannedDriver([[]]))
        info_messages = _info_messages(caplog)
        assert any('caught up:' in message for message in info_messages)
        assert not any('endpoint complete:' in message for message in info_messages)


class TestMetadataProjection:
    def test_snapshot_run_writes_metadata_json(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)
        records: list[JsonObject] = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]
        runner.run(_snapshot_definition(), CannedDriver([records]))
        metadata_file = tmp_path / 'motive' / 'vehicles' / 'metadata.json'
        assert json.loads(metadata_file.read_text(encoding='utf-8')) == {
            'schema_version': 1,
            'provider': 'motive',
            'endpoint': 'vehicles',
            'sync_mode': 'snapshot',
            'generated_at': '2026-06-16T00:00:00Z',
            'last_run': {
                'records_fetched': 2,
                'rows_written': 2,
                'duplicates_dropped': 0,
                'files_written': 1,
                'deleted_partitions': [],
                'window_start': None,
                'window_end': None,
            },
            'cursor': None,
        }

    def test_watermark_run_carries_the_window_and_the_advanced_cursor(
        self, tmp_path: Path
    ) -> None:
        # The applying stub makes the post-run read-back see the advanced
        # cursor, mirroring the real store's behavior.
        recorder = _RecordingRecorder()
        cursor = _ApplyingCursorAccess()
        runner = _make_runner(
            recorder,
            tmp_path,
            cursor_access=cursor,
            default_start_date=date(2026, 6, 12),
        )
        batch = _wm_batch(datetime(2026, 6, 13, 9, tzinfo=UTC))
        runner.run(_watermark_definition(), CannedDriver([batch]))
        metadata_file = tmp_path / 'motive' / 'locations' / 'metadata.json'
        document = json.loads(metadata_file.read_text(encoding='utf-8'))
        assert document['sync_mode'] == 'watermark'
        assert document['last_run']['window_start'] == '2026-06-12T00:00:00Z'
        assert document['last_run']['window_end'] == '2026-06-15T00:00:00Z'
        assert document['cursor'] == {
            'kind': 'date_watermark',
            'value': '2026-06-13T09:00:00Z',
        }

    def test_caught_up_writes_no_metadata(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 16, 8, tzinfo=UTC))
        )
        clock = FrozenClock(start_time_utc=datetime(2026, 6, 16, 12, tzinfo=UTC))
        runner = _make_runner(recorder, tmp_path, clock=clock, cursor_access=cursor)
        outcome = runner.run(_watermark_definition(), CannedDriver([[]]))
        assert isinstance(outcome, CaughtUp)
        assert not (tmp_path / 'motive' / 'locations' / 'metadata.json').exists()

    def test_write_oserror_leaves_the_run_executed_and_logs_at_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        def failing_write(endpoint_directory: Path, text: str) -> None:
            raise OSError('disk full')

        monkeypatch.setattr(
            'fleetpull.orchestrator.runner.write_metadata_json', failing_write
        )
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)
        records: list[JsonObject] = [{'id': 1, 'name': 'a'}]
        with caplog.at_level(logging.ERROR, logger='fleetpull.orchestrator.runner'):
            outcome = runner.run(_snapshot_definition(), CannedDriver([records]))
        assert isinstance(outcome, Executed)
        assert recorder.completed == [(1, 1)]
        assert recorder.failed == []
        error_records = [
            record for record in caplog.records if record.levelno == logging.ERROR
        ]
        assert len(error_records) == 1
        assert error_records[0].exc_info is not None
