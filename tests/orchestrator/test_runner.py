"""Tests for fleetpull.orchestrator.runner."""

from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
    WatermarkMode,
)
from fleetpull.exceptions import ConfigurationError, ProviderResponseError
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.network.contract import (
    DecodedPage,
    JsonObject,
    JsonValue,
    PageAdvance,
    RequestSpec,
)
from fleetpull.orchestrator.outcome import Executed
from fleetpull.orchestrator.runner import EndpointRunner
from fleetpull.vocabulary import Provider, QuotaScope


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
        self.completed: list[tuple[int, int]] = []
        self.failed: list[tuple[int, str]] = []

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        self.started.append((provider, endpoint))
        return len(self.started)

    def complete_run(self, run_id: int, *, row_count: int) -> None:
        self.completed.append((run_id, row_count))

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        self.failed.append((run_id, error_detail))


class _CompleteFailingRecorder(_RecordingRecorder):
    """A RunRecorder whose complete_run raises (a completion-write failure)."""

    def complete_run(self, run_id: int, *, row_count: int) -> None:
        raise RuntimeError('completion write failed')


class _FailRunFailingRecorder(_RecordingRecorder):
    """A RunRecorder whose fail_run also raises (to test masking-prevention)."""

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        raise RuntimeError('ledger down')


class _CannedDriver:
    """A RequestDriver yielding pre-set record batches, ignoring the client."""

    def __init__(self, batches: list[list[JsonObject]]) -> None:
        self._batches = batches

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[list[JsonObject]]:
        yield from self._batches


class _FailingDriver:
    """A RequestDriver that raises when driven."""

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[list[JsonObject]]:
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
        storage_kind=StorageKind.SINGLE,
        sync_mode=WatermarkMode(lookback=timedelta(days=1), cutoff=timedelta(days=1)),
        event_time_column='occurred_at',
    )


def test_snapshot_run_executes_writes_and_records(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = EndpointRunner(_StubClientSource(), recorder, tmp_path)
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
    runner = EndpointRunner(_StubClientSource(), recorder, tmp_path)
    outcome = runner.run(_snapshot_definition(), _CannedDriver([[]]))
    assert isinstance(outcome, Executed)
    assert outcome.records_fetched == 0
    assert recorder.completed == [(1, 0)]
    written = tmp_path / 'motive' / 'vehicles' / 'data.parquet'
    assert written.exists()
    assert pl.read_parquet(written).height == 0


def test_fetch_failure_records_failure_and_reraises(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = EndpointRunner(_StubClientSource(), recorder, tmp_path)
    with pytest.raises(RuntimeError, match='fetch blew up'):
        runner.run(_snapshot_definition(), _FailingDriver())
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_validation_failure_records_failure_and_reraises(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = EndpointRunner(_StubClientSource(), recorder, tmp_path)
    bad_batch: list[JsonObject] = [{'name': 'missing id'}]
    with pytest.raises(ProviderResponseError):
        runner.run(_snapshot_definition(), _CannedDriver([bad_batch]))
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_completion_failure_records_failure_and_reraises(tmp_path: Path) -> None:
    recorder = _CompleteFailingRecorder()
    runner = EndpointRunner(_StubClientSource(), recorder, tmp_path)
    records: list[JsonObject] = [{'id': 1, 'name': 'a'}]
    with pytest.raises(RuntimeError, match='completion write failed'):
        runner.run(_snapshot_definition(), _CannedDriver([records]))
    assert recorder.completed == []
    assert len(recorder.failed) == 1


def test_fail_run_failure_does_not_mask_original_error(tmp_path: Path) -> None:
    recorder = _FailRunFailingRecorder()
    runner = EndpointRunner(_StubClientSource(), recorder, tmp_path)
    with pytest.raises(RuntimeError, match='fetch blew up'):
        runner.run(_snapshot_definition(), _FailingDriver())


def test_unresolvable_client_opens_no_run(tmp_path: Path) -> None:
    recorder = _RecordingRecorder()
    runner = EndpointRunner(_EmptyClientSource(), recorder, tmp_path)
    with pytest.raises(ConfigurationError):
        runner.run(_snapshot_definition(), _CannedDriver([]))
    assert recorder.started == []


def test_watermark_mode_is_not_yet_executable(tmp_path: Path) -> None:
    runner = EndpointRunner(_StubClientSource(), _RecordingRecorder(), tmp_path)
    with pytest.raises(NotImplementedError):
        runner.run(_watermark_definition(), _CannedDriver([]))
