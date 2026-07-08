"""Tests for fleetpull.orchestrator.fanout -- the bounded fan-out channel."""

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import pytest

from fleetpull.orchestrator.fanout import FetchPool, stream_pieces
from tests.orchestrator.serial_executor import SerialExecutor

# Generous ceiling for event waits in the real-thread tests: converts a
# would-be deadlock into a loud, fast-enough failure.
_GATE_TIMEOUT_SECONDS = 5.0


def _serial_pool(window: int = 2) -> FetchPool:
    return FetchPool(executor=SerialExecutor(), submission_window=window)


class _RecordingTasks:
    """Piece tasks that record execution order and serve canned items."""

    def __init__(self, pieces: list[list[str]]) -> None:
        self._pieces = pieces
        self.executed: list[int] = []

    def task(self, index: int) -> list[str]:
        self.executed.append(index)
        return self._pieces[index]

    def all_tasks(self) -> list[Callable[[], list[str]]]:
        return [partial(self.task, index) for index in range(len(self._pieces))]


def test_window_below_one_is_rejected() -> None:
    with pytest.raises(ValueError, match='submission_window'):
        FetchPool(executor=SerialExecutor(), submission_window=0)


def test_yields_every_piece_item_in_task_order() -> None:
    tasks = _RecordingTasks([['a1', 'a2'], ['b1'], [], ['d1', 'd2', 'd3']])
    items = list(stream_pieces(tasks.all_tasks(), _serial_pool(window=2)))
    assert items == ['a1', 'a2', 'b1', 'd1', 'd2', 'd3']
    assert tasks.executed == [0, 1, 2, 3]


def test_no_tasks_yields_nothing() -> None:
    assert list(stream_pieces([], _serial_pool())) == []


def test_submission_is_lazy_and_bounded_by_the_window() -> None:
    # The deterministic form of the streaming bound: with window W, at the
    # moment piece k yields, exactly min(task_count, W + k) tasks have ever
    # run -- fetched-but-unconsumed never exceeds W + 1 pieces, regardless of
    # how many tasks exist.
    window = 2
    tasks = _RecordingTasks([[f'p{index}'] for index in range(6)])
    stream = stream_pieces(tasks.all_tasks(), _serial_pool(window=window))
    assert tasks.executed == []  # nothing runs before the consumer asks
    for consumed_pieces in range(1, 7):
        assert next(stream) == f'p{consumed_pieces - 1}'
        assert len(tasks.executed) == min(6, window + consumed_pieces)
    assert list(stream) == []
    assert tasks.executed == [0, 1, 2, 3, 4, 5]


def test_backpressure_bound_holds_under_a_slowed_consumer() -> None:
    # Real threads, instrumented channel: each task records the count of
    # pieces fetch-started but not yet consumed at its own start. The
    # consumer dawdles between pieces so eager workers would race as far
    # ahead as the implementation lets them -- a collect-all or
    # submit-everything implementation peaks at the task count (12); the
    # bounded window must never exceed submission_window + 1 (= 5).
    window = 4
    task_count = 12
    lock = threading.Lock()
    started = 0
    consumed = 0
    peak_unconsumed = 0

    def fetch_piece(index: int) -> list[int]:
        nonlocal started, peak_unconsumed
        with lock:
            started += 1
            peak_unconsumed = max(peak_unconsumed, started - consumed)
        return [index]

    tasks = [partial(fetch_piece, index) for index in range(task_count)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        pool = FetchPool(executor=executor, submission_window=window)
        collected: list[int] = []
        for item in stream_pieces(tasks, pool):
            time.sleep(0.005)  # let workers run as far ahead as they can
            with lock:
                consumed += 1
            collected.append(item)
    assert collected == list(range(task_count))
    assert peak_unconsumed <= window + 1


def test_first_failure_wins_and_pending_pieces_are_never_started() -> None:
    window = 3
    planted = RuntimeError('planted piece failure')
    tasks = _RecordingTasks([[f'p{index}'] for index in range(8)])
    all_tasks = tasks.all_tasks()

    def failing_task() -> list[str]:
        tasks.executed.append(1)
        raise planted

    all_tasks[1] = failing_task
    stream = stream_pieces(all_tasks, _serial_pool(window=window))
    assert next(stream) == 'p0'  # the piece ahead of the failure still yields
    with pytest.raises(RuntimeError, match='planted piece failure') as raised:
        next(stream)
    assert raised.value is planted
    # The window primed pieces 0..2 and one top-up submitted piece 3; the
    # in-flight horizon ends there -- pieces 4..7 were never started.
    assert tasks.executed == [0, 1, 2, 3]


def test_discarded_in_flight_failure_is_logged_never_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    first_failure = RuntimeError('the first failure')
    later_failure = ValueError('a later in-flight failure')

    def fail_first() -> list[str]:
        raise first_failure

    def fail_later() -> list[str]:
        raise later_failure

    def unreachable() -> list[str]:
        raise AssertionError('past the horizon; must never run')

    tasks: list[Callable[[], list[str]]] = [fail_first, fail_later, unreachable]
    with (
        caplog.at_level(logging.ERROR, logger='fleetpull.orchestrator.fanout'),
        pytest.raises(RuntimeError, match='the first failure') as raised,
    ):
        list(stream_pieces(tasks, _serial_pool(window=2)))
    assert raised.value is first_failure
    discarded = [record for record in caplog.records if record.exc_info is not None]
    assert len(discarded) == 1
    assert discarded[0].exc_info is not None
    assert discarded[0].exc_info[1] is later_failure


def test_in_flight_pieces_finish_and_are_discarded_on_failure() -> None:
    # Real threads: piece 0 fails only once piece 1 is demonstrably running,
    # so the unwind cannot cancel it -- it must wait for piece 1 to finish
    # (in-flight requests are allowed to finish) and discard its result
    # (nothing past the failure is yielded).
    piece_one_started = threading.Event()
    release_gate = threading.Event()
    piece_one_finished = threading.Event()
    planted = RuntimeError('piece zero failed')

    def fail_and_release() -> list[str]:
        assert piece_one_started.wait(timeout=_GATE_TIMEOUT_SECONDS)
        release_gate.set()
        raise planted

    def finish_after_gate() -> list[str]:
        piece_one_started.set()
        assert release_gate.wait(timeout=_GATE_TIMEOUT_SECONDS)
        piece_one_finished.set()
        return ['discarded item']

    consumed: list[str] = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        pool = FetchPool(executor=executor, submission_window=2)
        with pytest.raises(RuntimeError, match='piece zero failed'):
            consumed.extend(stream_pieces([fail_and_release, finish_after_gate], pool))
    assert piece_one_finished.is_set()
    assert consumed == []


def test_closing_the_stream_stops_submission_and_unwinds() -> None:
    tasks = _RecordingTasks([[f'p{index}'] for index in range(5)])
    stream = stream_pieces(tasks.all_tasks(), _serial_pool(window=2))
    assert next(stream) == 'p0'
    stream.close()
    # Primed 0 and 1, topped up 2 before the first yield; the close must not
    # submit 3 or 4.
    assert tasks.executed == [0, 1, 2]
