# src/fleetpull/orchestrator/runner.py
"""The run executor: run one endpoint to completion, once per (endpoint, run).

``EndpointRunner`` owns one endpoint's run transaction -- open the ledger run, build
the writer, drive the request driver, consume each record batch it yields (validate
-> frame -> write), finalize once, advance the cursor once, complete the run once --
and dispatches on the endpoint's ``sync_mode``. The snapshot and watermark arms are
built; the feed arm raises ``NotImplementedError`` until its prompt. The watermark
arm resolves its window, applies the two future-time guards, folds the observed
maximum, and writes parquet -> cursor -> ledger in that crash order; the pure resume
decisions live in ``orchestrator/resume.py`` and the per-batch transform in
``orchestrator/batch.py``, so the runner only orchestrates -- read state, call pure
functions, write state. Request cardinality and batch granularity are the driver's;
the runner is blind to both.

``run`` takes an optional ``BatchObserver``: a generic hook handed each
post-validation frame as the run streams. The runner knows nothing about what
an observer does with the frames (the caller boundary uses it to tap feeder
runs for roster reconciliation, but that knowledge lives entirely there) -- an
observer exception fails the run like any other batch-processing failure.

The runner depends on narrow Protocols rather than the concrete state and network
classes: ``ClientSource`` (the registry's ``client_for``), ``RunRecorder`` (the
ledger's lifecycle methods), and ``CursorAccess`` (the cursor store's get/set). It
opens no clients and reads no credentials -- the already-open client source hands it
the provider's client.
"""

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import polars as pl

from fleetpull.config import SyncConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    WatermarkMode,
)
from fleetpull.exceptions import ConfigurationError
from fleetpull.incremental import (
    DateWatermark,
    DateWindow,
    IncrementalCursor,
    resolve_resume_start,
    resolve_trailing_edge,
    window_or_none,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.batch import (
    ProcessedBatch,
    WindowContext,
    combine_latest_event_time,
)
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.resume import (
    resolve_watermark_start,
    should_advance_watermark,
)
from fleetpull.orchestrator.streaming import stream_processed_batches
from fleetpull.storage import select_writer
from fleetpull.timing import Clock
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'BatchObserver',
    'ClientSource',
    'CursorAccess',
    'EndpointRunner',
    'RunRecorder',
]

logger = logging.getLogger(__name__)

# The generic per-batch hook: called with each post-validation frame as the
# run streams. The runner is blind to what an observer does; an observer
# exception fails the run like any other batch-processing failure.
type BatchObserver = Callable[[pl.DataFrame], None]


def _observe_batches(
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


class ClientSource(Protocol):
    """The client-lookup surface the executor needs (a subset of the registry)."""

    def client_for(self, provider: Provider) -> TransportClient:
        """Return the open transport client for a provider."""
        ...


class RunRecorder(Protocol):
    """The run-recording surface the executor needs (a subset of RunLedger)."""

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        """Open a snapshot run and return its id."""
        ...

    def complete_run(self, run_id: int, *, row_count: int) -> None:
        """Close a run as succeeded with its row count."""
        ...

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        """Close a run as failed with an error detail."""
        ...

    def start_window_run(
        self, provider: Provider, endpoint: str, *, window: tuple[datetime, datetime]
    ) -> int:
        """Open a watermark run for a window and return its id."""
        ...

    def coverage_frontier(self, provider: Provider, endpoint: str) -> datetime | None:
        """Return the furthest window end a succeeded run has covered, if any."""
        ...


class CursorAccess(Protocol):
    """The cursor surface the watermark arm needs (a subset of CursorStore)."""

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        """Return the persisted cursor for a (provider, endpoint), or None."""
        ...

    def set_cursor(
        self, provider: Provider, endpoint: str, cursor: IncrementalCursor
    ) -> None:
        """Persist the cursor for a (provider, endpoint)."""
        ...


def _window_context(
    definition: EndpointDefinition[ResponseModel], window: DateWindow, now: datetime
) -> WindowContext:
    """Build the per-batch transform context, asserting the event-time column.

    A watermark endpoint always declares an ``event_time_column`` (the endpoint
    definition forbids otherwise); this narrows it for the type checker and fails
    loudly if that invariant is ever broken.

    Args:
        definition: The watermark endpoint being run.
        window: The run's half-open window.
        now: The run instant (the future-event guard's bound).

    Returns:
        The ``WindowContext`` for ``process_batch``.

    Raises:
        ConfigurationError: The endpoint declares no event-time column.
    """
    event_time_column = definition.event_time_column
    if event_time_column is None:
        raise ConfigurationError(
            'watermark endpoint has no event_time_column',
            provider=definition.provider.value,
            endpoint=definition.name,
        )
    return WindowContext(window=window, now=now, event_time_column=event_time_column)


@dataclass(frozen=True, slots=True)
class _WatermarkAdvance:
    """The watermark arm's intent to advance the cursor past its prior value.

    Distinguishes the watermark arm (advance, when the fold is strictly past
    ``prior``) from a backfill chunk (no advance -- ``_execute_window`` receives
    ``None``). ``prior`` is the stored cursor the fold must out-step (``None`` on a
    cold start, which any in-window observation out-steps).
    """

    prior: IncrementalCursor | None


class EndpointRunner:
    """Runs one endpoint to completion, dispatching on its sync mode.

    Constructed once with its five collaborators (client source, run recorder,
    clock, cursor access, sync config); ``run`` takes the endpoint and its request
    driver, so one instance runs every endpoint. The snapshot and watermark arms
    are built; the feed arm raises ``NotImplementedError``.
    """

    def __init__(
        self,
        client_source: ClientSource,
        run_recorder: RunRecorder,
        clock: Clock,
        cursor_access: CursorAccess,
        sync_config: SyncConfig,
    ) -> None:
        """
        Args:
            client_source: Hands out an open per-provider client (the registry).
            run_recorder: Records each run's lifecycle (the ledger).
            clock: Supplies the run instant (trailing edge, future-event guard).
            cursor_access: Reads the stored watermark and persists the advance.
            sync_config: Sync-wide settings (the dataset root and the cold-start
                anchor).
        """
        self._client_source = client_source
        self._run_recorder = run_recorder
        self._clock = clock
        self._cursor_access = cursor_access
        self._sync_config = sync_config
        self._dataset_root = sync_config.dataset_root

    def run(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None = None,
    ) -> RunOutcome:
        """Run one endpoint to completion and report the outcome.

        Args:
            definition: The endpoint to run.
            driver: The request driver supplying the run's record batches.
            observer: An optional generic hook handed each post-validation
                frame as the run streams; the runner is blind to what it does.

        Returns:
            The run outcome (``Executed`` for the snapshot arm).

        Raises:
            NotImplementedError: The endpoint's sync mode is watermark or feed
                (built in later prompts).
            FleetpullError: A fetch, validation, or write failure -- the run is
                recorded failed and the error propagates.
        """
        match definition.sync_mode:
            case SnapshotMode():
                return self._run_snapshot(definition, driver, observer)
            case WatermarkMode() as mode:
                return self._run_watermark(definition, driver, mode, observer)
            case FeedMode():
                raise NotImplementedError(
                    f'{type(definition.sync_mode).__name__} is not yet executable'
                )

    def run_backfill_unit(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        window: DateWindow,
    ) -> Executed:
        """Run one backfill chunk over a caller-given window, advancing no cursor.

        The backfill loop claims a chunk, builds a whole-roster fan-out driver and
        the chunk's window, and calls this. It runs the same window spine as the
        watermark arm -- the chunk run fans the whole roster, so the partition is
        replaced with every member's rows, exactly the in-full refetch the
        partitioned writer assumes -- but advances no global watermark: out-of-order
        chunks can't track a single max-event-time watermark, so the watermark is set
        once at backfill completion. The run is still recorded, so the coverage
        frontier advances date-wise.

        Args:
            definition: The watermark endpoint being backfilled.
            driver: The whole-roster fan-out driver for this chunk.
            window: The chunk's half-open window (the caller's, not resolved here).

        Returns:
            ``Executed`` with the fetched-row count and the write report.

        Raises:
            FleetpullError: A fetch, validation, write, or completion failure -- the
                run is recorded failed and the original error re-raised.
        """
        client = self._client_source.client_for(definition.provider)
        now = self._clock.now_utc()
        context = _window_context(definition, window, now)
        batches = stream_processed_batches(
            definition, driver, client, resume=window, context=context
        )
        return self._execute_window(definition, batches, context, advance=None)

    def _run_snapshot(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None,
    ) -> RunOutcome:
        """Run the snapshot arm: full fetch, full-replace write, record the run.

        Resolves the provider's client, opens a snapshot run, drives the batches,
        writes each, and completes the run with the fetched-row count. A snapshot has
        no resume value and no cursor, so it passes ``resume=None`` and advances no
        watermark. The client is resolved before the run is opened, so an
        unconfigured provider opens no dangling run. ``complete_run`` runs inside the
        protected block: a failure to record completion marks the run failed rather
        than leaving a zombie ``running`` row.

        Args:
            definition: The snapshot endpoint to run.
            driver: The request driver (a ``SingleRequestDriver`` for a snapshot).

        Returns:
            ``Executed`` with the fetched-row count and the write report.

        Raises:
            FleetpullError: A fetch, validation, write, or completion failure -- the
                run is recorded failed and the original error re-raised.
        """
        client = self._client_source.client_for(definition.provider)
        run_id = self._run_recorder.start_snapshot_run(
            definition.provider, definition.name
        )
        try:
            writer = select_writer(definition, self._dataset_root)
            records_fetched = 0
            batches = stream_processed_batches(
                definition, driver, client, resume=None, context=None
            )
            for processed in _observe_batches(batches, observer):
                writer.write(processed.frame)
                records_fetched += processed.frame.height
            write = writer.finalize()
            self._run_recorder.complete_run(run_id, row_count=records_fetched)
            return Executed(records_fetched=records_fetched, write=write)
        except Exception as error:
            self._fail_run_safely(run_id, error)
            raise

    def _run_watermark(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        mode: WatermarkMode,
        observer: BatchObserver | None,
    ) -> RunOutcome:
        """Run the watermark arm: resolve the window, then execute, advancing.

        Resolves the resume window from the stored watermark (less lookback), else
        the coverage frontier, else the cold-start anchor — the chosen start
        floored to its UTC midnight — with the end held back by the cutoff. A
        window that resolves to nothing is a ``CaughtUp`` no-op with no run
        opened. Otherwise it hands the resolved window to the shared spine,
        advancing the cursor on a strictly-forward observation.

        Args:
            definition: The watermark endpoint to run.
            driver: The request driver supplying the run's batches.
            mode: The endpoint's watermark mode (lookback and cutoff).

        Returns:
            ``Executed`` when a window was fetched, ``CaughtUp`` when the resume
            point had already reached the trailing edge.

        Raises:
            ConfigurationError: A stored watermark dated after ``now`` (Guard A), a
                cross-mode feed cursor on this endpoint, or a missing event-time
                column.
            FleetpullError: A fetch, validation, write, guard, or completion failure
                -- the run is recorded failed and the error re-raised.
        """
        client = self._client_source.client_for(definition.provider)
        now = self._clock.now_utc()
        end = resolve_trailing_edge(now, mode.cutoff)
        stored = self._cursor_access.get_cursor(definition.provider, definition.name)
        watermark_start = resolve_watermark_start(
            stored, mode.lookback, now, definition.provider, definition.name
        )
        frontier = self._run_recorder.coverage_frontier(
            definition.provider, definition.name
        )
        default_start = self._sync_config.default_start_datetime
        start = resolve_resume_start(watermark_start, frontier, default_start)
        window = window_or_none(start, end)
        if window is None:
            logger.info(
                'caught up: provider=%s endpoint=%s',
                definition.provider.value,
                definition.name,
            )
            return CaughtUp()
        context = _window_context(definition, window, now)
        batches = _observe_batches(
            stream_processed_batches(
                definition, driver, client, resume=window, context=context
            ),
            observer,
        )
        return self._execute_window(
            definition, batches, context, _WatermarkAdvance(prior=stored)
        )

    def _execute_window(
        self,
        definition: EndpointDefinition[ResponseModel],
        batches: Iterator[ProcessedBatch],
        context: WindowContext,
        advance: _WatermarkAdvance | None,
    ) -> Executed:
        """Run one window: open, consume/write/finalize, optionally advance, complete.

        The spine the watermark arm and a backfill chunk share. It consumes an
        already-built processed-batch stream (the arms construct it, wrapping
        in the batch observer where one applies), so the spine is blind to
        fetch mechanics. Opens a window run, writes each batch's in-window
        frame, folds the observed maximum, finalizes, and -- in the parquet ->
        cursor -> ledger crash order -- advances the cursor (when ``advance``
        is given and the fold is a strictly-forward step past its prior) before
        completing the run. ``advance`` is the watermark arm's intent; ``None``
        is a backfill chunk, which records the run and the coverage frontier
        but advances no global watermark.

        Args:
            definition: The endpoint being run.
            batches: The run's processed-batch stream, ready to consume.
            context: The window, run instant, and event-time column for the
                per-batch transform.
            advance: The cursor-advance intent, or ``None`` to advance nothing.

        Returns:
            ``Executed`` with the fetched-row count and the write report.

        Raises:
            FleetpullError: A fetch, validation, write, or completion failure -- the
                run is recorded failed and the original error re-raised.
        """
        window = context.window
        run_id = self._run_recorder.start_window_run(
            definition.provider, definition.name, window=(window.start, window.end)
        )
        try:
            writer = select_writer(definition, self._dataset_root, window=window)
            records_fetched = 0
            latest_observed: datetime | None = None
            for processed in batches:
                writer.write(processed.frame)
                records_fetched += processed.frame.height
                latest_observed = combine_latest_event_time(
                    latest_observed, processed.latest_event_time
                )
            write = writer.finalize()
            if (
                advance is not None
                and latest_observed is not None
                and should_advance_watermark(advance.prior, latest_observed)
            ):
                self._cursor_access.set_cursor(
                    definition.provider,
                    definition.name,
                    DateWatermark(watermark=latest_observed),
                )
            self._run_recorder.complete_run(run_id, row_count=records_fetched)
            return Executed(records_fetched=records_fetched, write=write)
        except Exception as error:
            self._fail_run_safely(run_id, error)
            raise

    def _fail_run_safely(self, run_id: int, error: Exception) -> None:
        """Record the run failed without masking the original error.

        ``fail_run`` touches SQLite, which can itself fail (a locked or unwritable
        database); if it does, that secondary failure must not replace the error
        that actually ended the run. Log it and let the original propagate.

        Args:
            run_id: The run to mark failed.
            error: The error that ended the run, recorded as the failure detail.

        Side Effects:
            Records the run failed; on a recording failure, logs and swallows it.
        """
        try:
            self._run_recorder.fail_run(run_id, error_detail=str(error))
        except Exception:
            logger.exception(
                'failed to record run %s as failed after an earlier error', run_id
            )
