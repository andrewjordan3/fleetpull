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

The runner depends on narrow Protocols rather than the concrete state and network
classes: ``ClientSource`` (the registry's ``client_for``), ``RunRecorder`` (the
ledger's lifecycle methods), and ``CursorAccess`` (the cursor store's get/set). It
opens no clients and reads no credentials -- the already-open client source hands it
the provider's client.
"""

import logging
from datetime import datetime
from typing import Protocol

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
    IncrementalCursor,
    resolve_resume_start,
    resolve_trailing_edge,
    window_or_none,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.batch import (
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
from fleetpull.storage import select_writer
from fleetpull.timing import Clock
from fleetpull.vocabulary import Provider

__all__: list[str] = ['ClientSource', 'CursorAccess', 'EndpointRunner', 'RunRecorder']

logger = logging.getLogger(__name__)


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
    ) -> RunOutcome:
        """Run one endpoint to completion and report the outcome.

        Args:
            definition: The endpoint to run.
            driver: The request driver supplying the run's record batches.

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
                return self._run_snapshot(definition, driver)
            case WatermarkMode() as mode:
                return self._run_watermark(definition, driver, mode)
            case FeedMode():
                raise NotImplementedError(
                    f'{type(definition.sync_mode).__name__} is not yet executable'
                )

    def _run_snapshot(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
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
            for page in driver.record_batches(definition, client, resume=None):
                processed = process_batch(page.records, definition, context=None)
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
    ) -> RunOutcome:
        """Run the watermark arm: resolve the window, fetch, write, advance.

        Resolves the resume window from the stored watermark (less lookback),
        else the coverage frontier, else the cold-start anchor, with the end
        held back by the cutoff. A window that resolves to nothing is a
        ``CaughtUp`` no-op with no run opened. Otherwise opens a window run and,
        in one protected block, writes each batch's in-window frame, folds the
        observed maximum, finalizes, advances the cursor (when the observation is
        a strictly-forward advance), and completes the run -- in that order, so a
        crash never leaves a frontier ahead of a committed watermark.

        Args:
            definition: The watermark endpoint to run.
            driver: The request driver supplying the run's record batches.
            mode: The endpoint's watermark mode (lookback and cutoff).

        Returns:
            ``Executed`` when a window was fetched, ``CaughtUp`` when the resume
            point had already reached the trailing edge.

        Raises:
            ConfigurationError: A stored watermark dated after ``now`` (Guard
                A), a cross-mode feed cursor on this endpoint, or a missing
                event-time column.
            FleetpullError: A fetch, validation, write, guard, or completion
                failure -- the run is recorded failed and the error re-raised.
        """
        client = self._client_source.client_for(definition.provider)
        event_time_column = definition.event_time_column
        if event_time_column is None:
            # EndpointDefinition forbids this for WatermarkMode; this narrows
            # for the type checker and fails loudly if that invariant breaks.
            raise ConfigurationError(
                'watermark endpoint has no event_time_column',
                provider=definition.provider.value,
                endpoint=definition.name,
            )
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
        run_id = self._run_recorder.start_window_run(
            definition.provider,
            definition.name,
            window=(window.start, window.end),
        )
        try:
            writer = select_writer(definition, self._dataset_root, window=window)
            context = WindowContext(
                window=window, now=now, event_time_column=event_time_column
            )
            records_fetched = 0
            latest_observed: datetime | None = None
            for page in driver.record_batches(definition, client, resume=window):
                processed = process_batch(page.records, definition, context)
                writer.write(processed.frame)
                records_fetched += processed.frame.height
                latest_observed = combine_latest_event_time(
                    latest_observed, processed.latest_event_time
                )
            write = writer.finalize()
            if latest_observed is not None and should_advance_watermark(
                stored, latest_observed
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
