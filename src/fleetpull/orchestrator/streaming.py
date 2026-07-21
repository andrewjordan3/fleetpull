# src/fleetpull/orchestrator/streaming.py
"""The fetch-and-frame pipe: a driver's pages, validated and framed per batch.

``stream_processed_batches`` is the shared step the run executor's snapshot and
watermark arms drive, and the roster harvest (``harvest_roster_members``) drives
as well: it walks the
request driver's fetched pages and runs each through ``process_batch``, yielding
one ``ProcessedBatch`` per page. It is a lazy generator -- each page is framed and
handed to the caller before the next is fetched, so the per-page memory bound the
partitioned writer relies on is preserved (nothing is collected up front).

It is the non-feed pipe: ``process_batch`` transforms ``page.records`` and drops
``durable_progress`` (the feed cursor token the snapshot and watermark arms never
use), so the feed arm drives the driver's pages itself
(``FeedDrive``), where each page's token commits right after its
parquet lands. The pipe owns no state and
resolves no client -- the conductor (the runner, the refresh service) opens the
run, picks the provider's client, and consumes the stream.

The consumption half lives beside the pipe: ``BatchObserver`` is the generic
per-frame hook a caller may ride on a run (the caller boundary uses it to tap
feeder runs for roster reconciliation; the executor stays blind to what it
does), ``observe_batches`` threads one through a stream transparently, and
``drain_batches`` is the write-count-and-fold drain the snapshot arm and every
watermark unit run over their streams.
"""

from collections.abc import Callable, Iterator
from datetime import datetime

import polars as pl

from fleetpull.endpoints.shared import EndpointDefinition, ResumeValue
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.batch import (
    ProcessedBatch,
    WindowContext,
    combine_latest_event_time,
    process_batch,
)
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.storage import DatasetWriter

__all__: list[str] = [
    'BatchObserver',
    'drain_batches',
    'observe_batches',
    'stream_processed_batches',
]

# The generic per-batch hook: called with each post-validation frame as the
# run streams. The run executor is blind to what an observer does; an observer
# exception fails the run like any other batch-processing failure.
type BatchObserver = Callable[[pl.DataFrame], None]


def observe_batches(
    batches: Iterator[ProcessedBatch], observer: BatchObserver | None
) -> Iterator[ProcessedBatch]:
    """Pass batches through, handing each post-validation frame to the observer.

    A transparent generator wrapper: with no observer the stream is yielded
    unchanged; with one, each batch's frame is observed before the batch is
    yielded onward, so memory stays bounded by one batch either way.

    Args:
        batches: The run's processed-batch stream.
        observer: The per-batch hook, or ``None`` for a bare pass-through.

    Yields:
        The batches, unchanged and in order.
    """
    if observer is None:
        yield from batches
        return
    for processed in batches:
        observer(processed.frame)
        yield processed


def drain_batches(
    batches: Iterator[ProcessedBatch], writer: DatasetWriter
) -> tuple[int, datetime | None]:
    """Consume a processed-batch stream: write, count, and fold as it arrives.

    The shared drain the snapshot arm and every watermark unit run: each
    batch's frame is handed to
    the writer as it yields (memory stays bounded by one batch) while the
    fetched-row count sums and the in-window event-time maximum folds. On
    the snapshot path every fold candidate is ``None`` (``process_batch``
    with ``context=None``), so the fold component is ``None`` there and the
    snapshot arm discards it.

    Args:
        batches: The run's processed-batch stream, ready to consume.
        writer: The run's dataset writer, handed each frame in order.

    Returns:
        The fetched-row count and the folded in-window maximum event time
        (``None`` on the snapshot path, or when no in-window event was
        observed).
    """
    records_fetched = 0
    latest_observed: datetime | None = None
    for processed in batches:
        writer.write(processed.frame)
        records_fetched += processed.frame.height
        latest_observed = combine_latest_event_time(
            latest_observed, processed.latest_event_time
        )
    return records_fetched, latest_observed


def stream_processed_batches(
    definition: EndpointDefinition[ResponseModel],
    driver: RequestDriver,
    client: TransportClient,
    resume: ResumeValue,
    context: WindowContext | None,
) -> Iterator[ProcessedBatch]:
    """Yield each of the driver's fetched pages, validated and framed.

    Drives ``driver.record_batches`` and runs each page's records through
    ``process_batch``, yielding the result. Lazy: a page is framed and yielded
    before the next is fetched, so memory stays bounded by one page. ``resume`` is
    what the driver injects into the first request (``None`` for a snapshot, the
    resolved window for a watermark or backfill run); ``context`` is the per-batch
    transform context (``None`` for a snapshot, the ``WindowContext`` for a
    watermark run). The two are separate -- a feed run would carry a version-token
    ``resume`` with no window ``context``.

    Args:
        definition: The endpoint being run (its response model, spec builder, and
            page decoder).
        driver: The request driver supplying the run's fetched pages.
        client: The provider's open transport client.
        resume: The resume value injected into the first request.
        context: The watermark per-batch context, or ``None`` for the snapshot
            validate-and-frame-only path.

    Yields:
        One ``ProcessedBatch`` per fetched page, in order.

    Raises:
        FleetpullError: A fetch, validation, framing, or guard failure, surfaced at
            iteration so the consuming run's ``try`` records the failure.

    Side Effects:
        Issues the driver's HTTP requests as the stream is consumed.
    """
    for page in driver.record_batches(definition, client, resume):
        yield process_batch(page.records, definition, context)
