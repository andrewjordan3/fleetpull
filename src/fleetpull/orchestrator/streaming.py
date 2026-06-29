# src/fleetpull/orchestrator/streaming.py
"""The fetch-and-frame pipe: a driver's pages, validated and framed per batch.

``stream_processed_batches`` is the shared step the run executor's snapshot and
watermark arms drive, and the roster refresh will drive as well: it walks the
request driver's fetched pages and runs each through ``process_batch``, yielding
one ``ProcessedBatch`` per page. It is a lazy generator -- each page is framed and
handed to the caller before the next is fetched, so the per-page memory bound the
partitioned writer relies on is preserved (nothing is collected up front).

It is the non-feed pipe: ``process_batch`` transforms ``page.records`` and drops
``durable_progress`` (the feed cursor token the snapshot and watermark arms never
use), so the feed arm drives its own when built. The pipe owns no state and
resolves no client -- the conductor (the runner, the refresh service) opens the
run, picks the provider's client, and consumes the stream.
"""

from collections.abc import Iterator

from fleetpull.endpoints.shared import EndpointDefinition, ResumeValue
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.batch import ProcessedBatch, WindowContext, process_batch
from fleetpull.orchestrator.drivers import RequestDriver

__all__: list[str] = ['stream_processed_batches']


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
