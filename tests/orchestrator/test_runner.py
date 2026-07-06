"""Tests for fleetpull.orchestrator.runner."""

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from fleetpull.config import SyncConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    ResumeValue,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import ConfigurationError, ProviderResponseError
from fleetpull.incremental import (
    DateWatermark,
    DateWindow,
    FeedToken,
    IncrementalCursor,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.network.contract import (
    DecodedPage,
    JsonObject,
    JsonValue,
    PageAdvance,
    RequestSpec,
)
from fleetpull.orchestrator.outcome import CaughtUp, Executed
from fleetpull.orchestrator.runner import ClientSource, CursorAccess, EndpointRunner
from fleetpull.timing import FrozenClock
from fleetpull.vocabulary import Provider, QuotaScope

# The _make_runner default clock instant, and the trailing edge it implies for a
# one-day cutoff: midnight(2026-06-16) - 1 day.
_CLOCK_NOW = datetime(2026, 6, 16, tzinfo=UTC)
_TRAILING_EDGE = datetime(2026, 6, 15, tzinfo=UTC)


class _SnapshotModel(ResponseModel):
    id: int
    name: str


class _WatermarkModel(ResponseModel):
    occurred_at: datetime


class _StubPageDecoder:
    """A PageDecoder double; the canned driver bypasses it, so it is never called."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        return spec

    def decode_page(self, sent: RequestSpec, envelope: JsonValue) -> DecodedPage:
        return DecodedPage(
            records=[], advance=PageAdvance(next_spec=None, durable_progress=None)
        )


class _StubClient(TransportClient):
    """A hollow client; the canned driver never calls it (no ``super().__init__``)."""

    def __init__(self) -> None:
        pass


class _StubClientSource:
    """A ClientSource handing a hollow client for any provider."""

    def __init__(self) -> None:
        self._client = _StubClient()

    def client_for(self, provider: Provider) -> TransportClient:
        return self._client


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


class _CannedDriver:
    """A RequestDriver yielding pre-set record pages, ignoring the client."""

    def __init__(self, batches: list[list[JsonObject]]) -> None:
        self._batches = batches

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        for batch in self._batches:
            yield FetchedPage(records=batch, durable_progress=None)


class _FailingDriver:
    """A RequestDriver that raises when driven."""

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        raise RuntimeError('fetch blew up')


def _snapshot_definition() -> EndpointDefinition[_SnapshotModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='vehicles',
        spec_builder=StaticGetSpecBuilder(
            base_url='https://x.test', path='/v1/vehicles'
        ),
        page_decoder=_StubPageDecoder(),
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
        page_decoder=_StubPageDecoder(),
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
        page_decoder=_StubPageDecoder(),
        response_model=_SnapshotModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=FeedMode(),
    )


# A test construction helper centralizing the runner's five-dependency constructor:
# its parameters are the runner's own collaborators plus the cold-start date, which
# do not bundle into a meaningful object -- so the >5-arg count is intrinsic here.
def _make_runner(  # noqa: PLR0913
    recorder: _RecordingRecorder,
    tmp_path: Path,
    *,
    client_source: ClientSource | None = None,
    clock: FrozenClock | None = None,
    cursor_access: CursorAccess | None = None,
    default_start_date: date = date(2024, 1, 1),
) -> EndpointRunner:
    return EndpointRunner(
        client_source or _StubClientSource(),
        recorder,
        clock or FrozenClock(start_time_utc=_CLOCK_NOW),
        cursor_access or _StubCursorAccess(),
        SyncConfig(default_start_date=default_start_date, dataset_root=tmp_path),
    )


def _wm_batch(*occurred_ats: datetime) -> list[JsonObject]:
    return [{'occurred_at': moment.isoformat()} for moment in occurred_ats]


def test_snapshot_run_executes_writes_and_records(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path)
    records: list[JsonObject] = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]
    outcome = runner.run(_snapshot_definition(), _CannedDriver([records]))
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
    outcome = runner.run(_snapshot_definition(), _CannedDriver([[]]))
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
        runner.run(_snapshot_definition(), _FailingDriver())
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_validation_failure_records_failure_and_reraises(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path)
    bad_batch: list[JsonObject] = [{'name': 'missing id'}]
    with pytest.raises(ProviderResponseError):
        runner.run(_snapshot_definition(), _CannedDriver([bad_batch]))
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_completion_failure_records_failure_and_reraises(tmp_path: Path) -> None:
    recorder = _CompleteFailingRecorder()
    runner = _make_runner(recorder, tmp_path)
    records: list[JsonObject] = [{'id': 1, 'name': 'a'}]
    with pytest.raises(RuntimeError, match='completion write failed'):
        runner.run(_snapshot_definition(), _CannedDriver([records]))
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_fail_run_failure_does_not_mask_original_error(tmp_path: Path) -> None:
    recorder = _FailRunFailingRecorder()
    runner = _make_runner(recorder, tmp_path)
    with pytest.raises(RuntimeError, match='fetch blew up'):
        runner.run(_snapshot_definition(), _FailingDriver())


def test_unresolvable_client_opens_no_run(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path, client_source=_EmptyClientSource())
    with pytest.raises(ConfigurationError):
        runner.run(_snapshot_definition(), _CannedDriver([]))
    assert recorder.started == []


def test_feed_mode_is_not_yet_executable(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = _make_runner(recorder, tmp_path)
    with pytest.raises(NotImplementedError):
        runner.run(_feed_definition(), _CannedDriver([]))


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
            _snapshot_definition(), _CannedDriver([records]), observed.append
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
        runner.run(_watermark_definition(), _CannedDriver([batch]), observed.append)
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
                _snapshot_definition(), _CannedDriver([records]), exploding_observer
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
        outcome = runner.run(_watermark_definition(), _CannedDriver([batch]))
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
            _CannedDriver([_wm_batch(datetime(2026, 6, 16, 9, tzinfo=UTC))]),
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
        runner.run(_watermark_definition(), _CannedDriver([batch]))
        assert recorder.windows == [(datetime(2026, 6, 12, tzinfo=UTC), _TRAILING_EDGE)]
        assert cursor.set_calls == [
            (
                Provider.MOTIVE,
                'locations',
                DateWatermark(watermark=datetime(2026, 6, 14, 10, tzinfo=UTC)),
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
        outcome = runner.run(_watermark_definition(), _CannedDriver([batch]))
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
        outcome = runner.run(_watermark_definition(), _CannedDriver([refetched]))
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
        runner.run(_watermark_definition(), _CannedDriver([batch]))
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
        outcome = runner.run(_watermark_definition(), _CannedDriver([[]]))
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
        outcome = runner.run(_watermark_definition(), _CannedDriver([batch]))
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
            runner.run(_watermark_definition(), _CannedDriver([]))
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
            runner.run(_watermark_definition(), _CannedDriver([batch]))
        assert len(recorder.failed) == 1
        assert recorder.completed == []
        assert cursor.set_calls == []

    def test_feed_cursor_on_a_watermark_endpoint_raises(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(FeedToken(from_version='v1'))
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        with pytest.raises(ConfigurationError, match='feed cursor'):
            runner.run(_watermark_definition(), _CannedDriver([]))
        assert recorder.started == []

    def test_fetch_failure_records_failure_and_reraises(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path, default_start_date=date(2026, 6, 12))
        with pytest.raises(RuntimeError, match='fetch blew up'):
            runner.run(_watermark_definition(), _FailingDriver())
        assert recorder.completed == []
        assert len(recorder.failed) == 1

    def test_validation_failure_records_failure_and_reraises(
        self, tmp_path: Path
    ) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path, default_start_date=date(2026, 6, 12))
        bad_batch: list[JsonObject] = [{'wrong': 'shape'}]
        with pytest.raises(ProviderResponseError):
            runner.run(_watermark_definition(), _CannedDriver([bad_batch]))
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
            runner.run(_watermark_definition(), _CannedDriver([batch]))
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
            runner.run(_watermark_definition(), _FailingDriver())


class TestRunBackfillUnit:
    def _window(self, start_day: int, end_day: int) -> DateWindow:
        return DateWindow(
            start=datetime(2026, 6, start_day, tzinfo=UTC),
            end=datetime(2026, 6, end_day, tzinfo=UTC),
        )

    def test_executes_the_given_window(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess()
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        window = self._window(1, 4)
        batch = _wm_batch(datetime(2026, 6, 2, 9, tzinfo=UTC))
        outcome = runner.run_backfill_unit(
            _watermark_definition(), _CannedDriver([batch]), window
        )
        assert isinstance(outcome, Executed)
        assert outcome.records_fetched == 1
        assert recorder.windows == [(window.start, window.end)]
        assert recorder.completed == [(1, 1)]
        assert (tmp_path / 'motive' / 'locations' / 'date=2026-06-02').exists()

    def test_advances_no_cursor(self, tmp_path: Path) -> None:
        # A strictly-forward observation would advance the watermark arm; the
        # backfill entry suppresses the advance (advance=None).
        recorder = _RecordingRecorder()
        cursor = _StubCursorAccess(
            DateWatermark(watermark=datetime(2026, 6, 1, tzinfo=UTC))
        )
        runner = _make_runner(recorder, tmp_path, cursor_access=cursor)
        batch = _wm_batch(datetime(2026, 6, 3, 12, tzinfo=UTC))
        outcome = runner.run_backfill_unit(
            _watermark_definition(), _CannedDriver([batch]), self._window(1, 4)
        )
        assert isinstance(outcome, Executed)
        assert cursor.set_calls == []
        assert recorder.completed == [(1, 1)]

    def test_fans_whole_roster_into_chunk_partitions(self, tmp_path: Path) -> None:
        # Several members' pages over one chunk: the partition is replaced with
        # every member's rows (the in-full refetch), finalized once.
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)
        member_one = _wm_batch(
            datetime(2026, 6, 2, 8, tzinfo=UTC), datetime(2026, 6, 3, 8, tzinfo=UTC)
        )
        member_two = _wm_batch(datetime(2026, 6, 2, 9, tzinfo=UTC))
        driver = _CannedDriver([member_one, member_two])
        outcome = runner.run_backfill_unit(
            _watermark_definition(), driver, self._window(1, 5)
        )
        assert isinstance(outcome, Executed)
        assert outcome.records_fetched == 3
        locations = tmp_path / 'motive' / 'locations'
        june_two = pl.read_parquet(locations / 'date=2026-06-02' / 'part.parquet')
        june_three = pl.read_parquet(locations / 'date=2026-06-03' / 'part.parquet')
        assert june_two.height == 2
        assert june_three.height == 1
        assert recorder.completed == [(1, 3)]

    def test_out_of_window_rows_are_dropped(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)
        batch = _wm_batch(
            datetime(2026, 6, 1, 8, tzinfo=UTC),  # before start (06-02): out
            datetime(2026, 6, 3, 9, tzinfo=UTC),  # in
            datetime(2026, 6, 5, 1, tzinfo=UTC),  # at/after end (06-05): out
        )
        outcome = runner.run_backfill_unit(
            _watermark_definition(), _CannedDriver([batch]), self._window(2, 5)
        )
        assert isinstance(outcome, Executed)
        assert outcome.records_fetched == 1
        locations = tmp_path / 'motive' / 'locations'
        assert (locations / 'date=2026-06-03').exists()
        assert not (locations / 'date=2026-06-01').exists()
        assert not (locations / 'date=2026-06-05').exists()

    def test_future_event_fails_the_run(self, tmp_path: Path) -> None:
        # The window is in the past, but a row dated after the clock instant trips
        # the future-event guard -- proving ``now`` from the context is wired through.
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)
        batch = _wm_batch(datetime(2026, 6, 17, 8, tzinfo=UTC))  # after now (06-16)
        with pytest.raises(ProviderResponseError):
            runner.run_backfill_unit(
                _watermark_definition(), _CannedDriver([batch]), self._window(1, 20)
            )
        assert len(recorder.failed) == 1
        assert recorder.completed == []

    def test_fetch_failure_records_failure_and_reraises(self, tmp_path: Path) -> None:
        recorder = _RecordingRecorder()
        runner = _make_runner(recorder, tmp_path)
        with pytest.raises(RuntimeError, match='fetch blew up'):
            runner.run_backfill_unit(
                _watermark_definition(), _FailingDriver(), self._window(1, 4)
            )
        assert recorder.completed == []
        assert len(recorder.failed) == 1
