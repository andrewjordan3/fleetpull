# src/fleetpull/orchestrator/runner.py
"""The run executor: run one endpoint to completion, once per (endpoint, run).

``EndpointRunner`` owns one endpoint's run and dispatches on its ``sync_mode``.
The snapshot arm fetches once and full-replaces. The watermark arm is the
unified plan-and-drive loop (DESIGN sections 4/5): re-claim any incomplete
work units and drive them serially ascending, then resolve the residual
window exactly as before -- watermark less lookback (floored), else coverage
frontier, else the cold-start anchor, against the cutoff trailing edge --
plan it into ``backfill_chunk_days`` units, and drive those. Every unit is
its own transaction: fetch the unit's window (the fan-out threads unchanged
within it), write parquet, advance the watermark on a strictly-forward
observation, record the ledger row -- parquet -> cursor -> ledger in that
crash order -- and mark the unit done. Serial ascending completion keeps
completed units a contiguous prefix, so every persisted watermark is true at
every instant; a crash resumes from the work-units ledger instead of
refetching the window. The feed arm executes GetFeed chains one page at a time, committing parquet before cursor and ledger state. The pure resume decisions live in ``orchestrator/resume.py``, the
per-batch transform in ``orchestrator/batch.py``, and the claim choreography
in ``orchestrator/unit_loop.py``, so the runner only orchestrates -- read
state, call pure functions, write state. Request cardinality and batch
granularity are the driver's; the runner is blind to both.

``run`` takes an optional ``BatchObserver``: a generic hook handed each
post-validation frame as the run streams. The runner knows nothing about what
an observer does with the frames (the caller boundary uses it to tap feeder
runs for roster reconciliation, but that knowledge lives entirely there) -- an
observer exception fails the run like any other batch-processing failure.

The runner depends on narrow Protocols rather than the concrete state and network
classes: ``ClientSource`` (the registry's ``client_for``) and the three
state-database surfaces bundled as ``RunStateAccess`` -- ``RunRecorder`` (the
ledger's lifecycle methods), ``CursorAccess`` (the cursor store's get/set), and
``UnitQueue`` (the work-unit claim queue). It opens no clients and reads no
credentials -- the already-open client source hands it the provider's client.
"""

import logging
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from typing import Protocol

import polars as pl

from fleetpull.config import FleetpullConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    WatermarkMode,
)
from fleetpull.exceptions import ConfigurationError, ProviderResponseError
from fleetpull.incremental import (
    DateWatermark,
    DateWindow,
    FeedBootstrap,
    FeedToken,
    IncrementalCursor,
    resolve_resume_start,
    resolve_trailing_edge,
    window_or_none,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.backfill import plan_backfill_units
from fleetpull.orchestrator.batch import (
    ProcessedBatch,
    WindowContext,
    combine_latest_event_time,
    process_batch,
)
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.resume import (
    resolve_watermark_start,
    should_advance_watermark,
)
from fleetpull.orchestrator.streaming import stream_processed_batches
from fleetpull.orchestrator.unit_loop import UnitQueue, drive_claimable_units
from fleetpull.storage import WriteResult, combine_write_results, select_writer
from fleetpull.timing import Clock
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'BatchObserver',
    'ClientSource',
    'CursorAccess',
    'EndpointRunner',
    'RunRecorder',
    'RunStateAccess',
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

    def complete_run(
        self, run_id: int, *, row_count: int, to_version: str | None = None
    ) -> None:
        """Close a run as succeeded with its row count."""
        ...

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        """Close a run as failed with an error detail."""
        ...

    def start_feed_run(
        self, provider: Provider, endpoint: str, *, start: FeedBootstrap | FeedToken
    ) -> int:
        """Open a feed run and return its id."""
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


@dataclass(frozen=True, slots=True)
class RunStateAccess:
    """The three state-database surfaces one endpoint run commits through.

    They always travel together -- the composition root builds all three
    over the one state database and the runner's crash order sequences
    them (parquet -> cursor -> ledger; units bracket the sequence) -- so
    they ride as one collaborator (the bundle rule).

    Attributes:
        recorder: The run ledger's lifecycle surface.
        cursors: The cursor store's get/set surface.
        units: The work-unit claim queue.
    """

    recorder: RunRecorder
    cursors: CursorAccess
    units: UnitQueue


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


class EndpointRunner:
    """Runs one endpoint to completion, dispatching on its sync mode.

    Constructed once with its four collaborators (client source, the bundled
    state surfaces, clock, the root config); ``run`` takes the endpoint and
    its request driver, so one instance runs every endpoint. Snapshot,
    watermark, and feed arms share the same provider-agnostic dispatch surface.
    """

    def __init__(
        self,
        client_source: ClientSource,
        state: RunStateAccess,
        clock: Clock,
        config: FleetpullConfig,
    ) -> None:
        """
        Args:
            client_source: Hands out an open per-provider client (the registry).
            state: The state-database surfaces -- the ledger, the cursor
                store, and the work-unit queue.
            clock: Supplies the run instant (trailing edge, future-event guard).
            config: The root config -- the container its composition root
                already holds. The runner reads exactly three values:
                ``sync.default_start_datetime`` (the cold-start anchor),
                ``sync.backfill_chunk_days`` (the unit width newly planned
                windows tile into), and ``storage.dataset_root`` (where the
                writers land).
        """
        self._client_source = client_source
        self._state = state
        self._clock = clock
        self._sync_config = config.sync
        self._dataset_root = config.storage.dataset_root
        self._drop_duplicates = config.storage.drop_exact_duplicates

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
            The run outcome -- ``Executed``, or ``CaughtUp`` when a windowed
            run had nothing to drive.

        Raises:
            FleetpullError: A fetch, validation, or write failure -- the run is
                recorded failed and the error propagates.
        """
        match definition.sync_mode:
            case SnapshotMode():
                return self._run_snapshot(definition, driver, observer)
            case WatermarkMode() as mode:
                return self._run_watermark(definition, driver, mode, observer)
            case FeedMode():
                return self._run_feed(definition, driver, observer)

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
        run_id = self._state.recorder.start_snapshot_run(
            definition.provider, definition.name
        )
        try:
            writer = select_writer(
                definition, self._dataset_root, drop_duplicates=self._drop_duplicates
            )
            records_fetched: int = 0
            batches = stream_processed_batches(
                definition, driver, client, resume=None, context=None
            )
            for processed in _observe_batches(batches, observer):
                writer.write(processed.frame)
                records_fetched += processed.frame.height
            write = writer.finalize()
            self._state.recorder.complete_run(run_id, row_count=records_fetched)
            return Executed(records_fetched=records_fetched, write=write)
        except Exception as error:
            self._fail_run_safely(run_id, error)
            raise

    def _run_feed(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None,
    ) -> RunOutcome:
        """Run the feed arm: one durable storage/cursor transaction per page."""
        if not self._drop_duplicates:
            raise ConfigurationError(
                'feed endpoints require storage.drop_exact_duplicates=true',
                provider=definition.provider.value,
                endpoint=definition.name,
            )
        client = self._client_source.client_for(definition.provider)
        stored = self._state.cursors.get_cursor(definition.provider, definition.name)
        match stored:
            case None:
                start: FeedBootstrap | FeedToken = FeedBootstrap(
                    from_date=self._sync_config.default_start_datetime
                )
            case FeedToken():
                start = stored
            case DateWatermark():
                raise ConfigurationError(
                    'date watermark stored for feed endpoint',
                    provider=definition.provider.value,
                    endpoint=definition.name,
                )
        run_id = self._state.recorder.start_feed_run(
            definition.provider, definition.name, start=start
        )
        try:
            records_fetched = 0
            write_results: list[WriteResult] = []
            final_token: str | None = None
            for page in driver.record_batches(definition, client, start):
                token = _feed_token_from_progress(definition, page.durable_progress)
                processed = process_batch(page.records, definition, context=None)
                if observer is not None:
                    observer(processed.frame)
                writer = select_writer(
                    definition,
                    self._dataset_root,
                    drop_duplicates=self._drop_duplicates,
                )
                writer.write(processed.frame)
                write_results.append(writer.finalize())
                self._state.cursors.set_cursor(
                    definition.provider, definition.name, FeedToken(from_version=token)
                )
                records_fetched += processed.frame.height
                final_token = token
            if final_token is None:
                raise ProviderResponseError(
                    provider=definition.provider.value,
                    endpoint=definition.name,
                    detail='feed run produced no durable progress',
                )
            self._state.recorder.complete_run(
                run_id, row_count=records_fetched, to_version=final_token
            )
            return Executed(
                records_fetched=records_fetched,
                write=combine_write_results(write_results),
            )
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
        """Run the watermark arm: the plan-and-drive unit loop.

        Incomplete units outrank the watermark: orphaned ``claimed`` units are
        reset (an in-progress unit found at run start is by definition
        orphaned -- fleetpull assumes a single driver per state database) and
        every claimable unit is re-claimed and driven, serially ascending.
        Then the residual window is resolved exactly as before -- the stored
        watermark less lookback (floored), else the coverage frontier, else
        the cold-start anchor, against the cutoff trailing edge -- planned
        into ``backfill_chunk_days`` units (a window smaller than one chunk is
        one unit: the daily run), and driven the same way. Each unit commits
        independently and advances the watermark on a strictly-forward
        observation; a failing unit returns to a claimable state and fails the
        endpoint (fail-fast). An invocation that drove nothing is ``CaughtUp``
        -- the resume point had reached the trailing edge, or every planned
        unit was already complete.

        Args:
            definition: The watermark endpoint to run.
            driver: The request driver supplying each unit's batches.
            mode: The endpoint's watermark mode (lookback and cutoff).
            observer: The optional per-frame hook, applied within every unit.

        Returns:
            ``Executed`` aggregated over the driven units, or ``CaughtUp``
            when nothing was driven.

        Raises:
            ConfigurationError: A stored watermark dated after ``now`` (Guard A), a
                cross-mode feed cursor on this endpoint, or a missing event-time
                column.
            FleetpullError: A fetch, validation, write, guard, or completion failure
                -- the failed unit's run is recorded failed, the unit returns to a
                claimable state, and the error re-raises.
        """
        provider = definition.provider
        name = definition.name
        # The cursor guards (Guard A, cross-mode) fire before any unit
        # drives; the residual resolution below re-derives this value after
        # the leftover units have advanced the cursor.
        resolve_watermark_start(
            self._state.cursors.get_cursor(provider, name),
            mode.lookback,
            self._clock.now_utc(),
            provider,
            name,
        )
        self._state.units.reset_claimed_to_pending(provider, name)
        drive_unit = partial(self._drive_unit, definition, driver, observer)
        outcomes = drive_claimable_units(self._state.units, provider, name, drive_unit)
        residual = self._resolve_residual_window(definition, mode)
        if residual is not None:
            chunk = timedelta(days=self._sync_config.backfill_chunk_days)
            self._state.units.enqueue(
                plan_backfill_units(provider, name, residual, chunk)
            )
            outcomes.extend(
                drive_claimable_units(self._state.units, provider, name, drive_unit)
            )
        if not outcomes:
            logger.info('caught up: provider=%s endpoint=%s', provider.value, name)
            return CaughtUp()
        return _merge_executed(outcomes)

    def _resolve_residual_window(
        self,
        definition: EndpointDefinition[ResponseModel],
        mode: WatermarkMode,
    ) -> DateWindow | None:
        """Resolve the not-yet-planned residual window, exactly as the resume chain.

        The same resolution the whole-window arm performed, run after the
        leftover units have driven: the stored watermark less the lookback
        margin (floored to its UTC midnight), else the coverage frontier,
        else the cold-start anchor -- against the cutoff-held trailing edge.
        Both bounds are midnight-aligned by construction, which is what makes
        the result plannable into whole-day units.

        Args:
            definition: The watermark endpoint being run.
            mode: The endpoint's watermark mode (lookback and cutoff).

        Returns:
            The residual ``DateWindow``, or ``None`` when the resume point
            has reached the trailing edge (nothing new to plan).

        Raises:
            ConfigurationError: A stored watermark dated after ``now`` (Guard
                A) or a cross-mode feed cursor (from
                ``resolve_watermark_start``).
        """
        now = self._clock.now_utc()
        end = resolve_trailing_edge(now, mode.cutoff)
        stored = self._state.cursors.get_cursor(definition.provider, definition.name)
        watermark_start = resolve_watermark_start(
            stored, mode.lookback, now, definition.provider, definition.name
        )
        frontier = self._state.recorder.coverage_frontier(
            definition.provider, definition.name
        )
        start = resolve_resume_start(
            watermark_start, frontier, self._sync_config.default_start_datetime
        )
        return window_or_none(start, end)

    def _drive_unit(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None,
        window: DateWindow,
    ) -> Executed:
        """Drive one unit: fetch its window, write, advance, record.

        The per-unit transaction the claim loop invokes: build the unit's
        batch stream (the fan-out threads the unit's (member x window) pieces
        on the ``FetchPool``, unchanged) and run it through the commit spine
        with the freshly read prior cursor, so each ascending unit's
        strictly-forward observation advances the watermark as it completes.

        Args:
            definition: The watermark endpoint being run.
            driver: The request driver supplying the unit's batches.
            observer: The optional per-frame hook.
            window: The unit's half-open window.

        Returns:
            The unit's ``Executed`` outcome.

        Raises:
            FleetpullError: A fetch, validation, write, or completion failure
                -- the unit's run is recorded failed and the error re-raised.
        """
        client = self._client_source.client_for(definition.provider)
        now = self._clock.now_utc()
        context = _window_context(definition, window, now)
        prior = self._state.cursors.get_cursor(definition.provider, definition.name)
        batches = _observe_batches(
            stream_processed_batches(
                definition, driver, client, resume=window, context=context
            ),
            observer,
        )
        return self._execute_window(definition, batches, context, prior)

    def _execute_window(
        self,
        definition: EndpointDefinition[ResponseModel],
        batches: Iterator[ProcessedBatch],
        context: WindowContext,
        prior: IncrementalCursor | None,
    ) -> Executed:
        """Run one window: open, consume/write/finalize, advance, complete.

        The per-unit commit spine. It consumes an already-built
        processed-batch stream (the caller constructs it, wrapping in the
        batch observer where one applies), so the spine is blind to fetch
        mechanics. Opens a window run, writes each batch's in-window frame,
        folds the observed maximum, finalizes, and -- in the parquet ->
        cursor -> ledger crash order -- advances the cursor when the fold is
        a strictly-forward step past ``prior`` before completing the run.

        Args:
            definition: The endpoint being run.
            batches: The unit's processed-batch stream, ready to consume.
            context: The window, run instant, and event-time column for the
                per-batch transform.
            prior: The cursor read at the unit's start -- the value a
                strictly-forward observation must out-step to advance.

        Returns:
            ``Executed`` with the fetched-row count and the write report.

        Raises:
            FleetpullError: A fetch, validation, write, or completion failure -- the
                run is recorded failed and the original error re-raised.
        """
        window = context.window
        run_id = self._state.recorder.start_window_run(
            definition.provider, definition.name, window=(window.start, window.end)
        )
        try:
            writer = select_writer(
                definition,
                self._dataset_root,
                window=window,
                drop_duplicates=self._drop_duplicates,
            )
            records_fetched: int = 0
            latest_observed: datetime | None = None
            for processed in batches:
                writer.write(processed.frame)
                records_fetched += processed.frame.height
                latest_observed = combine_latest_event_time(
                    latest_observed, processed.latest_event_time
                )
            write = writer.finalize()
            if latest_observed is not None and should_advance_watermark(
                prior, latest_observed
            ):
                self._state.cursors.set_cursor(
                    definition.provider,
                    definition.name,
                    DateWatermark(watermark=latest_observed),
                )
            self._state.recorder.complete_run(run_id, row_count=records_fetched)
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
            self._state.recorder.fail_run(run_id, error_detail=str(error))
        except Exception:
            logger.exception(
                'failed to record run %s as failed after an earlier error', run_id
            )


def _feed_token_from_progress(
    definition: EndpointDefinition[ResponseModel], durable_progress: str | None
) -> str:
    """Validate one feed page's durable progress token."""
    if not isinstance(durable_progress, str) or durable_progress == '':
        raise ProviderResponseError(
            provider=definition.provider.value,
            endpoint=definition.name,
            detail='feed page did not carry a valid durable progress token',
        )
    return durable_progress


def _merge_executed(outcomes: Sequence[Executed]) -> Executed:
    """Fold the driven units' outcomes into the invocation's one ``Executed``.

    Counts sum; the pruned partitions concatenate in drive order (a residual
    unit re-covering a leftover unit's dates may repeat one -- the report is
    informational, never consumed as a set).

    Args:
        outcomes: The per-unit outcomes, in drive order; at least one.

    Returns:
        The aggregated ``Executed``.
    """
    return Executed(
        records_fetched=sum(outcome.records_fetched for outcome in outcomes),
        write=combine_write_results([outcome.write for outcome in outcomes]),
    )
