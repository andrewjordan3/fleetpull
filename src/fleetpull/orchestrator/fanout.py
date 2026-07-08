# src/fleetpull/orchestrator/fanout.py
"""The bounded fan-out channel: worker-fetched pieces, streamed to one consumer.

``stream_pieces`` is the concurrency spine of a fan-out run (DESIGN §7): the
caller supplies one task per (member x window) piece, workers on the injected
executor fetch whole pieces, and the consumer thread -- the only thread that
ever validates, frames, or writes -- receives each piece's items as a lazy
stream. The channel is a bounded submission window over futures, drained in
submission order:

- **Bounded, never collected.** At most ``pool.submission_window`` pieces are
  outstanding, plus the one being yielded -- the fetched-but-unconsumed count
  never exceeds ``submission_window + 1``, a function of the pool size, never
  of the member count. When the consumer lags, no new piece is submitted, so
  workers idle instead of burning rate-budget tokens on results that cannot
  yet be written.
- **Submission-order draining.** The consumer waits on the oldest outstanding
  future, so items yield in exactly the order a serial loop would produce
  them -- correctness never depends on which piece completes first, and a
  synchronous same-thread executor reproduces the serial path verbatim.
- **First failure wins.** The first exception the consumer encounters
  re-raises after the window unwinds: not-yet-started pieces are cancelled
  (unsubmitted members are never submitted at all), in-flight pieces finish
  and their results are discarded, and any discarded piece's own failure is
  logged, never raised over the first.

Workers touch the network and this channel, nothing else; the single-writer
invariant (DESIGN §3) is the consumer's side of the contract, not enforced
here. The limiter remains the only enforcement point on the wire -- the pool
merely supplies workers, each of which acquires tokens and the concurrency
semaphore exactly as the serial path does.
"""

import logging
from collections.abc import Callable, Generator, Iterable, Sequence
from concurrent.futures import Executor, Future
from dataclasses import dataclass

__all__: list[str] = ['FetchPool', 'stream_pieces']

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FetchPool:
    """One provider's fetch workers plus the channel bound they imply.

    The pair travels together because the bound is a property of the pool:
    ``Executor`` does not expose its worker count, so the composition root
    that sizes the executor also fixes the submission window (the registry
    sets it to twice the worker count -- workers always have a queued piece
    to pick up while the consumer writes). Tests inject a synchronous
    same-thread executor through this same seam.

    Attributes:
        executor: The worker supply for piece fetches. Lifecycle belongs to
            whoever composed it (``FetchPoolRegistry`` in production) --
            ``stream_pieces`` never shuts it down.
        submission_window: Maximum pieces outstanding at once (>= 1). The
            streaming bound is ``submission_window + 1`` fetched-but-unconsumed
            pieces (the window plus the piece being yielded).
    """

    executor: Executor
    submission_window: int

    def __post_init__(self) -> None:
        """Reject a window that could never stream.

        Raises:
            ValueError: ``submission_window`` is less than 1 -- a zero window
                would prime nothing and silently yield an empty run.
        """
        if self.submission_window < 1:
            raise ValueError(
                f'submission_window must be >= 1, got {self.submission_window}'
            )


def _discard_outstanding[PieceItem](
    outstanding: Iterable[Future[Sequence[PieceItem]]],
) -> None:
    """Unwind the window: cancel what has not started, discard what has.

    Cancels every outstanding future (a no-op for ones already running),
    waits for the in-flight ones to finish, and logs -- never raises -- any
    failure among the discarded results, so a winning first exception (or the
    consumer's own abandonment) is never masked by a straggler's.

    Args:
        outstanding: The window's remaining futures, oldest first.

    Side Effects:
        Blocks until every in-flight piece finishes; logs each discarded
        piece's exception at ERROR.
    """
    for future in outstanding:
        future.cancel()
    for future in outstanding:
        if future.cancelled():
            continue
        discarded_error = future.exception()
        if discarded_error is not None:
            logger.error(
                'discarding an in-flight fan-out piece that failed after the '
                'run was already unwinding',
                exc_info=discarded_error,
            )


def stream_pieces[PieceItem](
    piece_tasks: Iterable[Callable[[], Sequence[PieceItem]]],
    pool: FetchPool,
) -> Generator[PieceItem, None, None]:
    """Fetch pieces on the pool's workers; stream their items in task order.

    Primes the window with the first ``pool.submission_window`` tasks, then
    repeatedly: wait on the oldest future, submit the next task (so workers
    stay busy while the consumer processes), and yield the finished piece's
    items. Tasks past the window are submitted only as earlier pieces are
    consumed -- the backpressure that keeps fetched-but-unconsumed results at
    ``submission_window + 1`` pieces, never a function of task count.

    Args:
        piece_tasks: One callable per piece, each returning that piece's
            items; consumed lazily, in order. Each runs on a worker thread
            and must touch only what is safe there (the network, for a
            fan-out fetch).
        pool: The workers and the window bound, as one collaborator.

    Yields:
        Every piece's items, piece by piece, in task order -- the order a
        serial loop over ``piece_tasks`` would produce.

    Raises:
        Exception: The first failure among the pieces, re-raised unchanged
            after the window unwinds (later pieces cancelled or discarded;
            a discarded piece's own failure is logged, never raised).

    Side Effects:
        Submits work to ``pool.executor``; on failure or an abandoning
        consumer (``close()``), cancels and drains the window before
        returning. Never shuts the executor down.
    """
    tasks = iter(piece_tasks)
    window: list[Future[Sequence[PieceItem]]] = []
    try:
        for task in tasks:
            window.append(pool.executor.submit(task))
            if len(window) >= pool.submission_window:
                break
        while window:
            piece = window.pop(0).result()
            next_task = next(tasks, None)
            if next_task is not None:
                window.append(pool.executor.submit(next_task))
            yield from piece
    finally:
        # Reached with a non-empty window only when the stream is dying: a
        # piece's exception is propagating, or the consumer closed the
        # generator mid-stream. Either way the window unwinds before the
        # cause continues outward.
        _discard_outstanding(window)
