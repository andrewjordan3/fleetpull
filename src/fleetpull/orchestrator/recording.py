# src/fleetpull/orchestrator/recording.py
"""Record-failure-without-masking: the orchestration layer's shared stance.

Every failure-recording write in this layer -- the run executor marking a run
failed, the roster coordinator marking a harvest run failed, the unit loop
marking a work unit failed -- touches SQLite, which can itself fail (a locked
or unwritable database); if it does, that secondary failure must never
replace the error that actually ended the work. ``record_failure_safely``
states that stance once: run the recording call, and on any recording failure
log it and swallow it so the original propagates. ``recorded_run`` is the
run-scoped composition every run-opening arm wraps its protected block in:
on any failure inside the block, record the run failed (safely) and re-raise
the original.
"""

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import partial
from typing import Protocol

__all__: list[str] = ['FailureRecorder', 'record_failure_safely', 'recorded_run']

logger = logging.getLogger(__name__)


class FailureRecorder(Protocol):
    """The one-method fail-a-run surface ``recorded_run`` needs."""

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        """Close a run as failed with an error detail."""
        ...


def record_failure_safely(record: Callable[[], None], subject: str) -> None:
    """Run a failure-recording call without masking the original error.

    Args:
        record: The recording call (``fail_run`` / ``mark_failed``, already
            bound to its arguments).
        subject: What is being marked failed, for the log line (e.g.
            ``'run 5'``, ``'work unit 7'``).

    Side Effects:
        Runs ``record``; on a recording failure, logs it with its traceback
        and swallows it so the caller's original error propagates.
    """
    try:
        record()
    except Exception:
        logger.exception(
            'failed to record %s as failed after an earlier error', subject
        )


@contextmanager
def recorded_run(recorder: FailureRecorder, run_id: int) -> Iterator[None]:
    """Run the block; on any failure, record the run failed and re-raise.

    The shared spine of every run-opening arm's protected block: a failure
    inside marks the run failed through ``record_failure_safely`` (so a
    recording failure never masks the original) and the original error
    propagates unchanged.

    Args:
        recorder: The ledger surface whose ``fail_run`` closes the run.
        run_id: The open run to mark failed on a block failure.

    Yields:
        Nothing -- the protected block runs in the ``with`` body.

    Raises:
        Exception: Whatever the block raised, unchanged.

    Side Effects:
        On a block failure, records the run failed (best-effort).
    """
    try:
        yield
    except Exception as error:
        record_failure_safely(
            partial(recorder.fail_run, run_id, error_detail=str(error)),
            f'run {run_id}',
        )
        raise
