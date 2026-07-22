"""Tests for fleetpull.orchestrator.streaming."""

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage, TransportClient
from fleetpull.orchestrator.batch import ProcessedBatch, WindowContext
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.streaming import stream_processed_batches
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.orchestrator.doubles import (
    CannedDriver,
    FailingDriver,
    StubClient,
    StubPageDecoder,
)

_WINDOW = DateWindow(
    start=datetime(2026, 6, 1, tzinfo=UTC),
    end=datetime(2026, 6, 3, tzinfo=UTC),
)
_NOW = datetime(2026, 6, 10, tzinfo=UTC)


class _EventModel(ResponseModel):
    id: int
    occurred_at: datetime


class _ResumeRecordingDriver:
    """A RequestDriver capturing the ``resume`` it was driven with."""

    def __init__(self) -> None:
        self.resume: ResumeValue = None

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        self.resume = resume
        yield FetchedPage(records=[], durable_progress=None)


class _OnePageThenRaisingDriver:
    """A RequestDriver that yields one page, then raises on the next pull."""

    def record_batches(
        self,
        definition: EndpointDefinition[ResponseModel],
        client: TransportClient,
        resume: ResumeValue,
    ) -> Iterator[FetchedPage]:
        yield FetchedPage(records=[], durable_progress=None)
        raise RuntimeError('second page blew up')


def _definition() -> EndpointDefinition[_EventModel]:
    return EndpointDefinition(
        provider=Provider.MOTIVE,
        name='events',
        spec_builder=StaticGetSpecBuilder(base_url='https://x.test', path='/v1/e'),
        page_decoder=StubPageDecoder(),
        response_model=_EventModel,
        quota_scope=QuotaScope.MOTIVE,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
    )


def _context() -> WindowContext:
    return WindowContext(window=_WINDOW, now=_NOW, event_time_column='occurred_at')


def _record(identifier: int, occurred_at: datetime) -> JsonObject:
    return {'id': identifier, 'occurred_at': occurred_at.isoformat()}


def _stream(
    driver: RequestDriver, resume: ResumeValue, context: WindowContext | None
) -> list[ProcessedBatch]:
    return list(
        stream_processed_batches(_definition(), driver, StubClient(), resume, context)
    )


def test_yields_one_processed_batch_per_page() -> None:
    page_a = [_record(1, datetime(2026, 6, 1, 8, tzinfo=UTC))]
    page_b = [
        _record(2, datetime(2026, 6, 2, tzinfo=UTC)),
        _record(3, datetime(2026, 6, 4, tzinfo=UTC)),
    ]
    processed = _stream(CannedDriver([page_a, page_b]), None, None)
    assert len(processed) == 2
    assert processed[0].frame.get_column('id').to_list() == [1]
    assert processed[1].frame.get_column('id').to_list() == [2, 3]


def test_snapshot_path_does_not_filter_or_fold() -> None:
    batch = [
        _record(1, datetime(2026, 6, 1, 8, tzinfo=UTC)),
        _record(2, datetime(2026, 6, 5, tzinfo=UTC)),  # outside _WINDOW, but kept
    ]
    processed = _stream(CannedDriver([batch]), None, None)
    assert processed[0].frame.height == 2
    assert processed[0].latest_event_time is None


def test_watermark_path_filters_and_folds() -> None:
    batch = [
        _record(1, datetime(2026, 5, 31, 23, tzinfo=UTC)),  # before start: out
        _record(2, datetime(2026, 6, 1, 8, tzinfo=UTC)),  # in
        _record(3, datetime(2026, 6, 2, 9, tzinfo=UTC)),  # in
        _record(4, datetime(2026, 6, 3, tzinfo=UTC)),  # exactly end: out
    ]
    processed = _stream(CannedDriver([batch]), _WINDOW, _context())
    assert processed[0].frame.get_column('id').to_list() == [2, 3]
    assert processed[0].latest_event_time == datetime(2026, 6, 2, 9, tzinfo=UTC)


def test_forwards_resume_to_the_driver() -> None:
    driver = _ResumeRecordingDriver()
    _stream(driver, _WINDOW, _context())
    assert driver.resume is _WINDOW


def test_is_lazy_one_page_before_the_next_is_fetched() -> None:
    stream = stream_processed_batches(
        _definition(), _OnePageThenRaisingDriver(), StubClient(), None, None
    )
    first = next(stream)  # the first page frames without pulling the second
    assert first.frame.height == 0
    with pytest.raises(RuntimeError, match='second page blew up'):
        list(stream)


def test_propagates_driver_fetch_failure() -> None:
    stream = stream_processed_batches(
        _definition(), FailingDriver(), StubClient(), None, None
    )
    with pytest.raises(RuntimeError, match='fetch blew up'):
        list(stream)


def test_future_event_is_dropped_not_raised() -> None:
    # A record materializing after the run clock (e.g. during a long sync) falls
    # outside the resume window, so the window filter drops it. It is an expected,
    # handled condition -- the stream yields a batch with no in-window rows rather
    # than raising.
    batch = [_record(1, datetime(2026, 6, 11, tzinfo=UTC))]  # after _NOW (06-10)
    processed = _stream(CannedDriver([batch]), _WINDOW, _context())
    assert processed[0].frame.height == 0
    assert processed[0].latest_event_time is None
