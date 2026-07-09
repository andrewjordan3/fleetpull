"""Tests for fleetpull.orchestrator.unit_loop -- the claim-and-drive choreography."""

from datetime import UTC, datetime

import pytest

from fleetpull.incremental import DateWindow
from fleetpull.orchestrator.outcome import Executed
from fleetpull.orchestrator.unit_loop import drive_claimable_units
from fleetpull.state import ClaimedWorkUnit, WorkUnitSpec, WorkUnitStatus
from fleetpull.storage import WriteResult
from fleetpull.vocabulary import Provider

_ENDPOINT = 'locations'


def _spec(start_day: int, end_day: int) -> WorkUnitSpec:
    return WorkUnitSpec(
        provider=Provider.MOTIVE,
        endpoint=_ENDPOINT,
        partition_key=None,
        chunk_start=datetime(2026, 6, start_day, tzinfo=UTC),
        chunk_end=datetime(2026, 6, end_day, tzinfo=UTC),
    )


def _executed(marker: int) -> Executed:
    return Executed(
        records_fetched=marker,
        write=WriteResult(rows_written=marker, duplicates_dropped=0, files_written=1),
    )


class _FakeQueue:
    """An in-memory UnitQueue mirroring the store's lifecycle semantics."""

    def __init__(self, specs: list[WorkUnitSpec]) -> None:
        self._rows: list[dict[str, WorkUnitSpec | WorkUnitStatus | int]] = [
            {'spec': spec, 'status': WorkUnitStatus.PENDING, 'attempts': 0}
            for spec in specs
        ]
        self.mark_failed_error: Exception | None = None

    def enqueue(self, units: list[WorkUnitSpec]) -> int:
        self._rows.extend(
            {'spec': spec, 'status': WorkUnitStatus.PENDING, 'attempts': 0}
            for spec in units
        )
        return len(units)

    def reset_claimed_to_pending(self, provider: Provider, endpoint: str) -> int:
        reset = 0
        for row in self._rows:
            if row['status'] is WorkUnitStatus.CLAIMED:
                row['status'] = WorkUnitStatus.PENDING
                reset += 1
        return reset

    def claim_next(
        self, provider: Provider, endpoint: str, *, max_attempts: int
    ) -> ClaimedWorkUnit | None:
        for unit_id, row in enumerate(self._rows, start=1):
            claimable = row['status'] in (
                WorkUnitStatus.PENDING,
                WorkUnitStatus.FAILED,
            )
            attempts = row['attempts']
            assert isinstance(attempts, int)
            if claimable and attempts < max_attempts:
                row['status'] = WorkUnitStatus.CLAIMED
                row['attempts'] = attempts + 1
                spec = row['spec']
                assert isinstance(spec, WorkUnitSpec)
                return ClaimedWorkUnit(
                    unit_id=unit_id, spec=spec, attempt_count=attempts + 1
                )
        return None

    def mark_done(self, unit_id: int) -> None:
        assert self._rows[unit_id - 1]['status'] is WorkUnitStatus.CLAIMED
        self._rows[unit_id - 1]['status'] = WorkUnitStatus.DONE

    def mark_failed(self, unit_id: int, *, error_detail: str) -> None:
        if self.mark_failed_error is not None:
            raise self.mark_failed_error
        assert self._rows[unit_id - 1]['status'] is WorkUnitStatus.CLAIMED
        self._rows[unit_id - 1]['status'] = WorkUnitStatus.FAILED

    def statuses(self) -> list[WorkUnitStatus]:
        return [
            row['status']
            for row in self._rows
            if isinstance(row['status'], WorkUnitStatus)
        ]


class _RecordingDrive:
    """A drive_unit double recording each window, optionally failing on one."""

    def __init__(self, fail_on: DateWindow | None = None) -> None:
        self.windows: list[DateWindow] = []
        self._fail_on = fail_on
        self.failure = RuntimeError('planted unit failure')

    def __call__(self, window: DateWindow) -> Executed:
        self.windows.append(window)
        if self._fail_on is not None and window == self._fail_on:
            raise self.failure
        return _executed(len(self.windows))


def test_drives_every_unit_ascending_and_marks_each_done() -> None:
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12), _spec(12, 13)])
    drive = _RecordingDrive()
    outcomes = drive_claimable_units(queue, Provider.MOTIVE, _ENDPOINT, drive)
    assert [window.start.day for window in drive.windows] == [10, 11, 12]
    assert [outcome.records_fetched for outcome in outcomes] == [1, 2, 3]
    assert queue.statuses() == [WorkUnitStatus.DONE] * 3


def test_empty_queue_drives_nothing() -> None:
    queue = _FakeQueue([])
    drive = _RecordingDrive()
    assert drive_claimable_units(queue, Provider.MOTIVE, _ENDPOINT, drive) == []
    assert drive.windows == []


def test_first_failure_marks_the_unit_failed_and_aborts_the_rest() -> None:
    fail_window = DateWindow(
        start=datetime(2026, 6, 11, tzinfo=UTC), end=datetime(2026, 6, 12, tzinfo=UTC)
    )
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12), _spec(12, 13), _spec(13, 14)])
    drive = _RecordingDrive(fail_on=fail_window)
    with pytest.raises(RuntimeError, match='planted unit failure') as raised:
        drive_claimable_units(queue, Provider.MOTIVE, _ENDPOINT, drive)
    assert raised.value is drive.failure
    # The completed prefix stands, the failed unit is claimable again, and
    # the units beyond it were never claimed.
    assert queue.statuses() == [
        WorkUnitStatus.DONE,
        WorkUnitStatus.FAILED,
        WorkUnitStatus.PENDING,
        WorkUnitStatus.PENDING,
    ]
    assert [window.start.day for window in drive.windows] == [10, 11]


def test_failed_unit_is_reclaimed_by_a_later_pass() -> None:
    fail_window = DateWindow(
        start=datetime(2026, 6, 10, tzinfo=UTC), end=datetime(2026, 6, 11, tzinfo=UTC)
    )
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12)])
    with pytest.raises(RuntimeError, match='planted unit failure'):
        drive_claimable_units(
            queue, Provider.MOTIVE, _ENDPOINT, _RecordingDrive(fail_on=fail_window)
        )
    retry = _RecordingDrive()
    outcomes = drive_claimable_units(queue, Provider.MOTIVE, _ENDPOINT, retry)
    assert [window.start.day for window in retry.windows] == [10, 11]
    assert len(outcomes) == 2
    assert queue.statuses() == [WorkUnitStatus.DONE, WorkUnitStatus.DONE]


def test_mark_failed_failure_never_masks_the_unit_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fail_window = DateWindow(
        start=datetime(2026, 6, 10, tzinfo=UTC), end=datetime(2026, 6, 11, tzinfo=UTC)
    )
    queue = _FakeQueue([_spec(10, 11)])
    queue.mark_failed_error = RuntimeError('state database down')
    drive = _RecordingDrive(fail_on=fail_window)
    with pytest.raises(RuntimeError, match='planted unit failure'):
        drive_claimable_units(queue, Provider.MOTIVE, _ENDPOINT, drive)
    assert any(
        'failed to record work unit' in record.message for record in caplog.records
    )
    # Left claimed; the next invocation's startup reset recovers it.
    assert queue.statuses() == [WorkUnitStatus.CLAIMED]
