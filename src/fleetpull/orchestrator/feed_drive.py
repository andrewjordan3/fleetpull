# src/fleetpull/orchestrator/feed_drive.py
"""The feed drive: the run executor's per-page version-token arm.

``FeedDrive`` drives one feed endpoint's version-token stream (DESIGN
sections 4/14, built 2026-07-21): resume is the stored ``FeedToken``, or a
``FeedSeed`` at the sync-wide cold-start anchor when none is stored (the
seed rides ONLY the tokenless first run -- I4); each page then commits
independently in the per-page crash order parquet BEFORE token (I1/I2) --
the append writer lands the page's rows durably, then the page's
``toVersion`` commits through the store's kind-guarded feed write -- so a
crash between the two refetches exactly one page on the next run and its
rows land again as new appended rows, harmless under the stored-as-emitted
contract. The drive consumes the page stream directly
(``stream_processed_batches`` deliberately drops ``durable_progress`` and
stays the non-feed pipe).
"""

import logging
from collections.abc import Iterator

from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import FeedResume, FeedSeed, FeedToken
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import FetchedPage
from fleetpull.orchestrator.batch import process_batch
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.outcome import Executed, RunOutcome
from fleetpull.orchestrator.recording import recorded_run
from fleetpull.orchestrator.resume import resolve_feed_resume
from fleetpull.orchestrator.spine import RunnerSpine
from fleetpull.orchestrator.streaming import BatchObserver
from fleetpull.storage import DatasetWriter
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import Provider

__all__: list[str] = ['FeedDrive']

logger = logging.getLogger(__name__)


def _feed_resume_label(resume: FeedResume) -> str:
    """The ledger's ``from_version`` text for a feed run's resume value.

    A resumed run records the token verbatim; a seeded run records the
    self-describing ``seed:<iso8601>`` marker (the ``runs`` table requires a
    non-null ``from_version`` on a feed row, and the seed date IS what the
    run resumed from -- the convention is recorded on
    ``RunLedger.start_feed_run``).

    Args:
        resume: The run's resolved feed resume value.

    Returns:
        The token, or ``seed:<iso8601 start>`` for a seeded run.
    """
    match resume:
        case FeedToken(from_version=from_version):
            return from_version
        case FeedSeed(start=start):
            return f'seed:{to_iso8601(start)}'


def _log_feed_resume(provider: Provider, endpoint: str, resume: FeedResume) -> None:
    """Narrate a feed run's resume point at INFO: seeded or resumed.

    Args:
        provider: The endpoint's provider.
        endpoint: The endpoint name.
        resume: The run's resolved feed resume value.

    Side Effects:
        Emits one INFO line.
    """
    match resume:
        case FeedToken(from_version=from_version):
            logger.info(
                'feed run resumed: provider=%s endpoint=%s from_version=%s',
                provider.value,
                endpoint,
                from_version,
            )
        case FeedSeed(start=start):
            logger.info(
                'feed run seeded: provider=%s endpoint=%s from_date=%s',
                provider.value,
                endpoint,
                to_iso8601(start),
            )


class FeedDrive:
    """Drives one feed endpoint's version-token stream, page by page.

    Constructed once by ``EndpointRunner.__init__`` from the shared spine;
    ``run`` takes the endpoint, its request driver, and the optional
    per-frame observer.
    """

    def __init__(self, spine: RunnerSpine) -> None:
        """
        Args:
            spine: The shared drive kit -- collaborators plus the runner's
                writer-factory and projection seams.
        """
        self._spine = spine

    def run(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None,
    ) -> RunOutcome:
        """Run the feed arm: drive the version-token stream, page by page.

        Resume is the stored ``FeedToken`` used directly, or a ``FeedSeed``
        at the sync-wide cold-start anchor when no token is stored -- the
        seed rides ONLY the tokenless first run (I4; ``resolve_feed_resume``
        makes it structural). The client is resolved before the run is
        opened, so an unconfigured provider opens no dangling run. Each page
        then commits independently through ``_consume_feed_pages`` in the
        per-page crash order (parquet before token -- I1/I2); the ledger row
        closes with the run's total row count and final ``toVersion`` after
        the stream drains. A crash mid-stream leaves the run ``running``
        (diagnostic only, the §5 stance) with every completed page's parquet
        and token already committed -- the next run resumes from the last
        committed token and refetches exactly one page, whose rows append
        again as duplicates the stored-as-emitted contract absorbs (§4).

        Args:
            definition: The feed endpoint to run.
            driver: The request driver supplying the run's pages (a
                ``SingleRequestDriver`` -- feeds are single-chain).
            observer: The optional per-frame hook, handed each
                post-validation frame as the run streams.

        Returns:
            ``Executed`` with the fetched-row count and the append report.

        Raises:
            ConfigurationError: A watermark cursor is stored for this feed
                endpoint (cross-mode corruption, from ``resolve_feed_resume``
                -- raised before any run is opened), or a page carried no
                durable progress (a non-feed decoder wired to a feed
                endpoint).
            FleetpullError: A fetch, validation, write, or completion failure
                -- the run is recorded failed and the original error
                re-raised; pages committed before it stand.
        """
        provider = definition.provider
        name = definition.name
        state = self._spine.state
        client = self._spine.clients.client_for(provider)
        resume = resolve_feed_resume(
            state.cursors.get_cursor(provider, name),
            self._spine.sync.default_start_datetime,
            provider,
            name,
        )
        _log_feed_resume(provider, name, resume)
        run_id = state.recorder.start_feed_run(
            provider, name, from_version=_feed_resume_label(resume)
        )
        with recorded_run(state.recorder, run_id):
            writer = self._spine.make_writer(definition)
            pages = driver.record_batches(definition, client, resume)
            records_fetched, page_count, last_token = self._consume_feed_pages(
                definition, pages, writer, observer
            )
            write = writer.finalize()
            state.recorder.complete_run(
                run_id, row_count=records_fetched, to_version=last_token
            )
        logger.info(
            'feed complete: provider=%s endpoint=%s pages=%d records=%d to_version=%s',
            provider.value,
            name,
            page_count,
            records_fetched,
            last_token,
        )
        outcome = Executed(records_fetched=records_fetched, write=write)
        self._spine.projection.project(definition, outcome, window=None)
        return outcome

    def _consume_feed_pages(
        self,
        definition: EndpointDefinition[ResponseModel],
        pages: Iterator[FetchedPage],
        writer: DatasetWriter,
        observer: BatchObserver | None,
    ) -> tuple[int, int, str]:
        """Consume the feed stream: per page, parquet BEFORE token (I1/I2).

        The per-page transaction (DESIGN section 14): validate and frame the
        page (``process_batch`` with no window context -- the feed has no
        window, no future-event guard, no fold; whatever the stream emits is
        stored), hand the frame to the observer where one rides, append it
        durably (the feed writer's ``write`` is durable on return -- the
        append-log cell's contract), and only THEN commit the page's
        ``toVersion``. The token therefore never moves past unwritten data:
        a crash between the two loses only the token, and the next run
        refetches that one page. An empty page (the at-head terminal)
        appends nothing and re-commits its unchanged token -- the feed
        always has a cursor to write (section 5).

        Args:
            definition: The feed endpoint being run.
            pages: The driver's fetched-page stream, ready to consume.
            writer: The run's append writer, handed each frame in order.
            observer: The optional per-frame hook.

        Returns:
            The fetched-row count, the page count, and the final committed
            ``toVersion``.

        Raises:
            ConfigurationError: A page carried no ``durable_progress`` -- a
                non-feed decoder is wired to a feed endpoint, a construction
                bug surfaced before the page's rows are written.
            RuntimeError: The stream yielded no pages, violating the
                client's at-least-one-page contract.

        Side Effects:
            Appends part files and commits the feed cursor, page by page.
        """
        records_fetched = 0
        page_count = 0
        last_token: str | None = None
        for page in pages:
            token = page.durable_progress
            if token is None:
                raise ConfigurationError(
                    'feed page carries no durable progress',
                    provider=definition.provider.value,
                    endpoint=definition.name,
                    detail=(
                        'the endpoint declares FeedMode but its page decoder '
                        'yielded no resume token -- a non-feed decoder is wired '
                        'to a feed endpoint'
                    ),
                )
            processed = process_batch(page.records, definition, None)
            if observer is not None:
                observer(processed.frame)
            writer.write(processed.frame)
            self._spine.state.cursors.commit_feed_token(
                definition.provider, definition.name, token
            )
            records_fetched += processed.frame.height
            page_count += 1
            last_token = token
            logger.debug(
                'feed page appended: provider=%s endpoint=%s page=%d records=%d '
                'to_version=%s',
                definition.provider.value,
                definition.name,
                page_count,
                processed.frame.height,
                token,
            )
        if last_token is None:
            raise RuntimeError(
                'feed drive yielded no pages -- fetch_pages always drives at least one'
            )
        return records_fetched, page_count, last_token
