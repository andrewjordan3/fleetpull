"""Tests for fleetpull.orchestrator.batch."""

from datetime import UTC, datetime

import pytest

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StaticGetSpecBuilder,
    StorageKind,
)
from fleetpull.exceptions import ProviderResponseError
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.batch import (
    WindowContext,
    combine_latest_event_time,
    process_batch,
)
from fleetpull.vocabulary import JsonObject, Provider, QuotaScope
from tests.orchestrator.doubles import StubPageDecoder

_WINDOW = DateWindow(
    start=datetime(2026, 6, 1, tzinfo=UTC),
    end=datetime(2026, 6, 3, tzinfo=UTC),
)
_NOW = datetime(2026, 6, 10, tzinfo=UTC)


class _EventModel(ResponseModel):
    id: int
    occurred_at: datetime


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


def test_snapshot_path_frames_without_filtering_or_fold() -> None:
    batch = [
        _record(1, datetime(2026, 6, 1, 8, tzinfo=UTC)),
        _record(2, datetime(2026, 6, 5, tzinfo=UTC)),
    ]
    processed = process_batch(batch, _definition(), context=None)
    assert processed.frame.height == 2
    assert processed.latest_event_time is None


def test_watermark_path_keeps_in_window_and_folds_the_max() -> None:
    batch = [
        _record(1, datetime(2026, 6, 1, 8, tzinfo=UTC)),
        _record(2, datetime(2026, 6, 2, 9, tzinfo=UTC)),
    ]
    processed = process_batch(batch, _definition(), _context())
    assert processed.frame.height == 2
    assert processed.latest_event_time == datetime(2026, 6, 2, 9, tzinfo=UTC)


def test_watermark_path_drops_out_of_window_rows() -> None:
    batch = [
        _record(1, datetime(2026, 5, 31, 23, tzinfo=UTC)),  # before start: out
        _record(2, datetime(2026, 6, 1, tzinfo=UTC)),  # exactly start: in
        _record(3, datetime(2026, 6, 2, 12, tzinfo=UTC)),  # inside: in
        _record(4, datetime(2026, 6, 3, tzinfo=UTC)),  # exactly end: out
    ]
    processed = process_batch(batch, _definition(), _context())
    assert processed.frame.get_column('id').to_list() == [2, 3]
    # The late, out-of-window row (06-03) does not advance the fold.
    assert processed.latest_event_time == datetime(2026, 6, 2, 12, tzinfo=UTC)


def test_watermark_path_future_event_raises_before_filtering() -> None:
    batch = [
        _record(1, datetime(2026, 6, 1, 8, tzinfo=UTC)),
        _record(2, datetime(2026, 6, 11, tzinfo=UTC)),  # after now: a guard trip
    ]
    with pytest.raises(ProviderResponseError):
        process_batch(batch, _definition(), _context())


def test_empty_batch_snapshot_path() -> None:
    processed = process_batch([], _definition(), context=None)
    assert processed.frame.height == 0
    assert processed.latest_event_time is None


def test_empty_batch_watermark_path() -> None:
    processed = process_batch([], _definition(), _context())
    assert processed.frame.height == 0
    assert processed.latest_event_time is None


def test_combine_latest_event_time_is_none_tolerant_and_takes_the_max() -> None:
    earlier = datetime(2026, 6, 1, tzinfo=UTC)
    later = datetime(2026, 6, 2, tzinfo=UTC)
    assert combine_latest_event_time(None, None) is None
    assert combine_latest_event_time(None, later) == later
    assert combine_latest_event_time(earlier, None) == earlier
    assert combine_latest_event_time(earlier, later) == later
    assert combine_latest_event_time(later, earlier) == later
