"""Tests for fleetpull.orchestrator.unit_loop -- the claim-and-drive choreography.

The fake queue is lock-guarded: the loop under test claims from worker
threads, and the fake must mirror the store's atomic-claim semantics
(WAL-serialized in production, a mutex here) or the tests would race where
the real store cannot.
"""

import logging
import threading
from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from fleetpull.incremental import DateWindow
from fleetpull.orchestrator.outcome import Executed
from fleetpull.orchestrator.unit_loop import UnitCrew, drive_claimable_units
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


def _executed(marker: int, observed: datetime | None = None) -> Executed:
    return Executed(
        records_fetched=marker,
        write=WriteResult(rows_written=marker, duplicates_dropped=0, files_written=1),
        latest_observed=observed,
    )


class _FakeQueue:
    """An in-memory UnitQueue mirroring the store's lifecycle semantics."""

    def __init__(self, specs: list[WorkUnitSpec]) -> None:
        self._lock = threading.Lock()
        self._rows: list[dict[str, WorkUnitSpec | WorkUnitStatus | int]] = [
            {'spec': spec, 'status': WorkUnitStatus.PENDING, 'attempts': 0}
            for spec in specs
        ]
        self.observations: dict[int, datetime | None] = {}
        self.mark_failed_error: Exception | None = None

    def enqueue(self, units: list[WorkUnitSpec]) -> int:
        with self._lock:
            self._rows.extend(
                {'spec': spec, 'status': WorkUnitStatus.PENDING, 'attempts': 0}
                for spec in units
            )
        return len(units)

    def release_done_units(
        self, provider: Provider, endpoint: str, *, window: DateWindow
    ) -> int:
        with self._lock:
            kept = [
                row
                for row in self._rows
                if not (
                    row['status'] is WorkUnitStatus.DONE
                    and isinstance(spec := row['spec'], WorkUnitSpec)
                    and spec.chunk_start >= window.start
                    and spec.chunk_end <= window.end
                )
            ]
            released = len(self._rows) - len(kept)
            self._rows = kept
        return released

    def reset_claimed_to_pending(self, provider: Provider, endpoint: str) -> int:
        with self._lock:
            reset = 0
            for row in self._rows:
                if row['status'] is WorkUnitStatus.CLAIMED:
                    row['status'] = WorkUnitStatus.PENDING
                    reset += 1
            return reset

    def claim_next(self, provider: Provider, endpoint: str) -> ClaimedWorkUnit | None:
        with self._lock:
            for unit_id, row in enumerate(self._rows, start=1):
                claimable = row['status'] in (
                    WorkUnitStatus.PENDING,
                    WorkUnitStatus.FAILED,
                )
                attempts = row['attempts']
                assert isinstance(attempts, int)
                if claimable:
                    row['status'] = WorkUnitStatus.CLAIMED
                    row['attempts'] = attempts + 1
                    spec = row['spec']
                    assert isinstance(spec, WorkUnitSpec)
                    return ClaimedWorkUnit(
                        unit_id=unit_id, spec=spec, attempt_count=attempts + 1
                    )
            return None

    def mark_done(self, unit_id: int, *, observed_max: datetime | None) -> None:
        with self._lock:
            assert self._rows[unit_id - 1]['status'] is WorkUnitStatus.CLAIMED
            self._rows[unit_id - 1]['status'] = WorkUnitStatus.DONE
            self.observations[unit_id] = observed_max

    def mark_failed(self, unit_id: int, *, error_detail: str) -> None:
        if self.mark_failed_error is not None:
            raise self.mark_failed_error
        with self._lock:
            assert self._rows[unit_id - 1]['status'] is WorkUnitStatus.CLAIMED
            self._rows[unit_id - 1]['status'] = WorkUnitStatus.FAILED

    def done_prefix_observation(
        self, provider: Provider, endpoint: str
    ) -> datetime | None:
        with self._lock:
            best: datetime | None = None
            for row in self._rows:
                if row['status'] is not WorkUnitStatus.DONE:
                    break
                unit_id = self._rows.index(row) + 1
                observed = self.observations.get(unit_id)
                if observed is not None and (best is None or observed > best):
                    best = observed
            return best

    def statuses(self) -> list[WorkUnitStatus]:
        return [
            row['status']
            for row in self._rows
            if isinstance(row['status'], WorkUnitStatus)
        ]


class _RecordingDrive:
    """A drive_unit double recording each window, optionally failing on one."""

    def __init__(self, fail_on: DateWindow | None = None) -> None:
        self._lock = threading.Lock()
        self.windows: list[DateWindow] = []
        self._fail_on = fail_on
        self.failure = RuntimeError('planted unit failure')

    def __call__(self, window: DateWindow) -> Executed:
        with self._lock:
            self.windows.append(window)
            marker = len(self.windows)
        if self._fail_on is not None and window == self._fail_on:
            raise self.failure
        # The unit's observation sits just inside its window.
        return _executed(marker, observed=window.end)


class _PrefixRecorder:
    """A commit_prefix double counting invocations, thread-safely."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.calls = 0

    def __call__(self) -> None:
        with self._lock:
            self.calls += 1


class _FaithfulPrefixCommitter:
    """A commit_prefix double mirroring the runner's real prefix commit.

    Reads the fake queue's contiguous done-prefix and advances a
    forward-only watermark, recording every read and every applied advance
    (lock-guarded; workers invoke it concurrently) -- so the tests can
    assert exactly when the watermark moved and to what.
    """

    def __init__(self, queue: _FakeQueue) -> None:
        self._queue = queue
        self._lock = threading.Lock()
        self.watermark: datetime | None = None
        self.reads: list[datetime | None] = []
        self.applied: list[datetime] = []

    def __call__(self) -> None:
        observation = self._queue.done_prefix_observation(Provider.MOTIVE, _ENDPOINT)
        with self._lock:
            self.reads.append(observation)
            if observation is not None and (
                self.watermark is None or observation > self.watermark
            ):
                self.watermark = observation
                self.applied.append(observation)


def _drive(
    queue: _FakeQueue,
    drive: Callable[[DateWindow], Executed],
    *,
    workers: int = 1,
    commit_prefix: Callable[[], None] | None = None,
) -> list[Executed]:
    crew = UnitCrew(
        queue=queue,
        provider=Provider.MOTIVE,
        endpoint=_ENDPOINT,
        drive_unit=drive,
        commit_prefix=commit_prefix if commit_prefix is not None else _PrefixRecorder(),
    )
    return drive_claimable_units(crew, workers=workers)


def test_drives_every_unit_ascending_and_marks_each_done() -> None:
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12), _spec(12, 13)])
    drive = _RecordingDrive()
    outcomes = _drive(queue, drive)
    assert [window.start.day for window in drive.windows] == [10, 11, 12]
    assert [outcome.records_fetched for outcome in outcomes] == [1, 2, 3]
    assert queue.statuses() == [WorkUnitStatus.DONE] * 3


def test_mark_done_records_each_units_observation() -> None:
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12)])
    _drive(queue, _RecordingDrive())
    assert queue.observations == {
        1: datetime(2026, 6, 11, tzinfo=UTC),
        2: datetime(2026, 6, 12, tzinfo=UTC),
    }


def test_commit_prefix_runs_after_every_completion() -> None:
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12), _spec(12, 13)])
    recorder = _PrefixRecorder()
    _drive(queue, _RecordingDrive(), commit_prefix=recorder)
    assert recorder.calls == 3


def test_rejects_a_workerless_call() -> None:
    with pytest.raises(ValueError, match='workers must be at least 1'):
        _drive(_FakeQueue([]), _RecordingDrive(), workers=0)


def test_empty_queue_drives_nothing() -> None:
    queue = _FakeQueue([])
    drive = _RecordingDrive()
    assert _drive(queue, drive) == []
    assert drive.windows == []


def test_parallel_workers_drive_every_unit_exactly_once() -> None:
    specs = [_spec(day, day + 1) for day in range(1, 13)]
    queue = _FakeQueue(specs)
    drive = _RecordingDrive()
    recorder = _PrefixRecorder()
    outcomes = _drive(queue, drive, workers=4, commit_prefix=recorder)
    assert len(outcomes) == 12
    assert queue.statuses() == [WorkUnitStatus.DONE] * 12
    # Every unit driven exactly once, whatever the interleaving.
    assert sorted(window.start.day for window in drive.windows) == list(range(1, 13))
    assert recorder.calls == 12


def test_first_failure_marks_the_unit_failed_and_stops_new_claims() -> None:
    fail_window = DateWindow(
        start=datetime(2026, 6, 11, tzinfo=UTC), end=datetime(2026, 6, 12, tzinfo=UTC)
    )
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12), _spec(12, 13), _spec(13, 14)])
    drive = _RecordingDrive(fail_on=fail_window)
    with pytest.raises(RuntimeError, match='planted unit failure') as raised:
        _drive(queue, drive)
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


def test_parallel_failure_lets_in_flight_units_finish() -> None:
    # Worker one fails its unit while worker two is mid-drive: the
    # in-flight unit completes and commits; nothing new is claimed. The
    # sibling here holds until mark_failed lands, which the loop orders
    # AFTER the stop signal -- so in THIS scripted interleaving its next
    # claim deterministically sees the stop. (In general the ordering only
    # narrows the same-invocation retry window -- a sibling already past
    # its stop check can still reclaim the failed unit once; this test
    # scripts the closed side deliberately.)
    fail_window = DateWindow(
        start=datetime(2026, 6, 1, tzinfo=UTC), end=datetime(2026, 6, 2, tzinfo=UTC)
    )
    sibling_in_flight = threading.Event()
    failure_marked = threading.Event()

    class _SignallingQueue(_FakeQueue):
        def mark_failed(self, unit_id: int, *, error_detail: str) -> None:
            super().mark_failed(unit_id, error_detail=error_detail)
            failure_marked.set()

    class _CoordinatedDrive(_RecordingDrive):
        def __call__(self, window: DateWindow) -> Executed:
            if window == fail_window:
                # Fail only once the sibling is genuinely in flight.
                assert sibling_in_flight.wait(timeout=5)
                return super().__call__(window)
            sibling_in_flight.set()
            assert failure_marked.wait(timeout=5)
            return super().__call__(window)

    queue = _SignallingQueue([_spec(1, 2), _spec(2, 3), _spec(3, 4)])
    drive = _CoordinatedDrive(fail_on=fail_window)
    with pytest.raises(RuntimeError, match='planted unit failure'):
        _drive(queue, drive, workers=2)
    statuses = queue.statuses()
    assert statuses[0] is WorkUnitStatus.FAILED
    assert statuses[1] is WorkUnitStatus.DONE
    # The third unit was never claimed: the stop signal preceded the
    # failed-mark the sibling waited on.
    assert statuses[2] is WorkUnitStatus.PENDING


def test_out_of_order_completion_holds_the_watermark_to_the_prefix() -> None:
    # The prefix-advance rule's hold-back, driven deterministically: unit 2
    # completes and commits while unit 1 is still in flight, so its commit
    # reads an empty done-prefix (in-flight unit 1 gates it) and applies
    # nothing -- the watermark never advances past unit 1's window. Once
    # unit 1 completes, the prefix covers both units and the watermark
    # advances straight to the full prefix maximum.
    unit_one_window = DateWindow(
        start=datetime(2026, 6, 10, tzinfo=UTC), end=datetime(2026, 6, 11, tzinfo=UTC)
    )
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12)])
    first_commit_done = threading.Event()

    class _SignallingCommitter(_FaithfulPrefixCommitter):
        def __call__(self) -> None:
            super().__call__()
            first_commit_done.set()

    class _GatedDrive(_RecordingDrive):
        def __call__(self, window: DateWindow) -> Executed:
            if window == unit_one_window:
                # Hold unit 1 until its sibling has completed AND committed.
                assert first_commit_done.wait(timeout=5)
            return super().__call__(window)

    committer = _SignallingCommitter(queue)
    _drive(queue, _GatedDrive(), workers=2, commit_prefix=committer)
    assert queue.statuses() == [WorkUnitStatus.DONE, WorkUnitStatus.DONE]
    # Unit 2's commit saw a gated prefix; unit 1's saw the full maximum.
    assert committer.reads == [None, datetime(2026, 6, 12, tzinfo=UTC)]
    assert committer.applied == [datetime(2026, 6, 12, tzinfo=UTC)]
    assert committer.watermark == datetime(2026, 6, 12, tzinfo=UTC)


def test_failed_gap_freezes_the_prefix_until_a_later_pass_closes_it() -> None:
    # Gap-then-close: unit 1 fails only after units 2 and 3 completed on the
    # sibling worker, so their commits read a gated prefix and the watermark
    # never moves. Re-driving unit 1 on a later pass closes the gap and the
    # prefix advances over all three units at once.
    fail_window = DateWindow(
        start=datetime(2026, 6, 10, tzinfo=UTC), end=datetime(2026, 6, 11, tzinfo=UTC)
    )
    tail_window = DateWindow(
        start=datetime(2026, 6, 12, tzinfo=UTC), end=datetime(2026, 6, 13, tzinfo=UTC)
    )
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12), _spec(12, 13)])
    tail_done = threading.Event()

    class _TailFirstDrive(_RecordingDrive):
        def __call__(self, window: DateWindow) -> Executed:
            if window == fail_window:
                # Fail only once the sibling has driven the whole tail.
                assert tail_done.wait(timeout=5)
            outcome = super().__call__(window)
            if window == tail_window:
                tail_done.set()
            return outcome

    committer = _FaithfulPrefixCommitter(queue)
    with pytest.raises(RuntimeError, match='planted unit failure'):
        _drive(
            queue,
            _TailFirstDrive(fail_on=fail_window),
            workers=2,
            commit_prefix=committer,
        )
    # Both tail commits read the gated prefix: the failed unit 1 froze it.
    assert queue.statuses() == [
        WorkUnitStatus.FAILED,
        WorkUnitStatus.DONE,
        WorkUnitStatus.DONE,
    ]
    assert committer.reads == [None, None]
    assert committer.applied == []
    assert committer.watermark is None

    # The later pass re-drives only unit 1; its completion closes the gap
    # and the prefix advances over all three units in one commit.
    _drive(queue, _RecordingDrive(), commit_prefix=committer)
    assert queue.statuses() == [WorkUnitStatus.DONE] * 3
    assert committer.applied == [datetime(2026, 6, 13, tzinfo=UTC)]
    assert committer.watermark == datetime(2026, 6, 13, tzinfo=UTC)


def test_every_unit_failure_is_logged_with_its_unit_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # No silent drops: a failing unit logs an exception record naming its
    # provider, endpoint, and unit id -- including the one whose exception
    # re-raises after the join.
    fail_window = DateWindow(
        start=datetime(2026, 6, 10, tzinfo=UTC), end=datetime(2026, 6, 11, tzinfo=UTC)
    )
    queue = _FakeQueue([_spec(10, 11)])
    with (
        caplog.at_level(logging.ERROR),
        pytest.raises(RuntimeError, match='planted unit failure'),
    ):
        _drive(queue, _RecordingDrive(fail_on=fail_window))
    failure_records = [
        record for record in caplog.records if 'unit failed:' in record.getMessage()
    ]
    assert len(failure_records) == 1
    assert 'provider=motive' in failure_records[0].getMessage()
    assert f'endpoint={_ENDPOINT}' in failure_records[0].getMessage()
    assert 'unit_id=1' in failure_records[0].getMessage()
    assert failure_records[0].exc_info is not None


def test_failed_unit_is_reclaimed_by_a_later_pass() -> None:
    fail_window = DateWindow(
        start=datetime(2026, 6, 10, tzinfo=UTC), end=datetime(2026, 6, 11, tzinfo=UTC)
    )
    queue = _FakeQueue([_spec(10, 11), _spec(11, 12)])
    with pytest.raises(RuntimeError, match='planted unit failure'):
        _drive(queue, _RecordingDrive(fail_on=fail_window))
    retry = _RecordingDrive()
    outcomes = _drive(queue, retry)
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
        _drive(queue, drive)
    assert any(
        'failed to record work unit' in record.message for record in caplog.records
    )
    # Left claimed; the next invocation's startup reset recovers it.
    assert queue.statuses() == [WorkUnitStatus.CLAIMED]
