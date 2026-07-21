# src/fleetpull/orchestrator/runner.py
"""The run executor: run one endpoint to completion, once per (endpoint, run).

``EndpointRunner`` owns one endpoint's run and dispatches on its ``sync_mode``.
The snapshot arm lives here: fetch once, full-replace, record the run. The
watermark arm is the ``WatermarkDrive`` (``orchestrator/watermark_drive.py``:
the plan-and-drive unit loop with the prefix-advance watermark rule) and the
feed arm is the ``FeedDrive`` (``orchestrator/feed_drive.py``: the per-page
parquet-before-token drive) -- both constructed in ``__init__`` from the same
``RunnerSpine`` (``orchestrator/spine.py``): the four collaborators plus the
runner-owned seams, the ONE writer-factory call site (``_writer_for``) and
the post-commit ``metadata.json`` projection
(``orchestrator/metadata_projection.py``). Every run-opening arm wraps its
protected block in the shared ``recorded_run`` spine
(``orchestrator/recording.py``), so a failure records the run failed without
masking the original error. Request cardinality and batch granularity are
the driver's; the runner is blind to both.

``run`` takes an optional ``BatchObserver``: a generic hook handed each
post-validation frame as the run streams. The runner knows nothing about what
an observer does with the frames (the caller boundary uses it to tap feeder
runs for roster reconciliation, but that knowledge lives entirely there) -- an
observer exception fails the run like any other batch-processing failure.

The runner depends on narrow Protocols rather than the concrete state and
network classes -- ``ClientSource``, ``RunRecorder``, ``CursorAccess``, and
``UnitQueue``, bundled as ``RunStateAccess`` (all on the spine module). It
opens no clients and reads no credentials -- the already-open client source
hands it the provider's client.
"""

import logging

from fleetpull.config import FleetpullConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.feed_drive import FeedDrive
from fleetpull.orchestrator.metadata_projection import (
    MetadataProjection,
    sync_mode_label,
)
from fleetpull.orchestrator.outcome import Executed, RunOutcome
from fleetpull.orchestrator.recording import recorded_run
from fleetpull.orchestrator.spine import ClientSource, RunnerSpine, RunStateAccess
from fleetpull.orchestrator.streaming import (
    BatchObserver,
    drain_batches,
    observe_batches,
    stream_processed_batches,
)
from fleetpull.orchestrator.watermark_drive import WatermarkDrive
from fleetpull.storage import DatasetWriter, select_writer
from fleetpull.timing import Clock

__all__: list[str] = ['EndpointRunner']

logger = logging.getLogger(__name__)


class EndpointRunner:
    """Runs one endpoint to completion, dispatching on its sync mode.

    Constructed once with its four collaborators (client source, the bundled
    state surfaces, clock, the root config); ``run`` takes the endpoint and
    its request driver, so one instance runs every endpoint. The snapshot
    arm lives on the class; the watermark and feed arms are the two drive
    classes built in ``__init__`` from the shared spine.
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
                already holds. The runner reads the ``sync`` section (the
                cold-start anchor, the unit width, and the unit worker
                count, handed to the drives on the spine) plus two storage
                values: ``storage.dataset_root`` (where the writers land)
                and ``storage.drop_exact_duplicates`` (the writers'
                exact-dedup switch).
        """
        self._dataset_root = config.storage.dataset_root
        self._drop_duplicates = config.storage.drop_exact_duplicates
        self._spine = RunnerSpine(
            clients=client_source,
            state=state,
            clock=clock,
            sync=config.sync,
            make_writer=self._writer_for,
            projection=MetadataProjection(
                state.cursors, clock, config.storage.dataset_root
            ),
        )
        self._watermark_drive = WatermarkDrive(self._spine)
        self._feed_drive = FeedDrive(self._spine)

    def run(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None = None,
    ) -> RunOutcome:
        """Run one endpoint to completion and report the outcome.

        Narrates at INFO: one start line at entry (provider, endpoint, sync
        mode) and, for an ``Executed`` outcome, one completion line with the
        merged counts and the endpoint's elapsed seconds (a monotonic delta
        captured at entry). A ``CaughtUp`` outcome narrates through the
        watermark arm's own 'caught up' line instead.

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
        endpoint_started = self._spine.clock.monotonic_seconds()
        logger.info(
            'endpoint started: provider=%s endpoint=%s mode=%s',
            definition.provider.value,
            definition.name,
            sync_mode_label(definition.sync_mode),
        )
        match definition.sync_mode:
            case SnapshotMode():
                outcome: RunOutcome = self._run_snapshot(definition, driver, observer)
            case WatermarkMode() as mode:
                outcome = self._watermark_drive.run(definition, driver, mode, observer)
            case FeedMode():
                outcome = self._feed_drive.run(definition, driver, observer)
        if isinstance(outcome, Executed):
            self._log_endpoint_complete(definition, outcome, endpoint_started)
        return outcome

    def _writer_for(
        self,
        definition: EndpointDefinition[ResponseModel],
        window: DateWindow | None = None,
    ) -> DatasetWriter:
        """Construct the endpoint's writer -- the ONE ``select_writer`` call site.

        Every arm's writer routes through here (the drives receive it as the
        spine's ``make_writer``), with the dataset root and the exact-dedup
        switch bound once at construction.

        Args:
            definition: The endpoint being run.
            window: The run's resolved resume window, for the incremental
                cells; ``None`` for the snapshot and feed cells.

        Returns:
            The endpoint's ``DatasetWriter`` for this run.
        """
        return select_writer(
            definition,
            self._dataset_root,
            window=window,
            drop_duplicates=self._drop_duplicates,
        )

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
        than leaving a zombie ``running`` row. The ``metadata.json`` projection
        writes after the protected block -- the run is committed by then, and a
        post-commit projection failure must never mark a succeeded run failed.

        Args:
            definition: The snapshot endpoint to run.
            driver: The shape-resolved request driver -- a
                ``SingleRequestDriver`` for a single-fetch snapshot, a
                ``FanOutRequestDriver`` for a ``ParamSweep`` one.
            observer: The optional per-frame hook, handed each post-validation
                frame as the run streams.

        Returns:
            ``Executed`` with the fetched-row count and the write report.

        Raises:
            FleetpullError: A fetch, validation, write, or completion failure -- the
                run is recorded failed and the original error re-raised.
        """
        recorder = self._spine.state.recorder
        client = self._spine.clients.client_for(definition.provider)
        run_id = recorder.start_snapshot_run(definition.provider, definition.name)
        with recorded_run(recorder, run_id):
            writer = self._writer_for(definition)
            batches = stream_processed_batches(
                definition, driver, client, resume=None, context=None
            )
            records_fetched, _ = drain_batches(
                observe_batches(batches, observer), writer
            )
            write = writer.finalize()
            recorder.complete_run(run_id, row_count=records_fetched)
        outcome = Executed(records_fetched=records_fetched, write=write)
        self._spine.projection.project(definition, outcome, window=None)
        return outcome

    def _log_endpoint_complete(
        self,
        definition: EndpointDefinition[ResponseModel],
        outcome: Executed,
        endpoint_started: float,
    ) -> None:
        """Narrate a committed endpoint's counts and elapsed time at INFO.

        Args:
            definition: The endpoint that just ran.
            outcome: The run's (merged) ``Executed`` outcome.
            endpoint_started: The ``monotonic_seconds`` reading captured at
                ``run()`` entry, so the elapsed value covers the whole
                endpoint (both arms, metadata projection included).
        """
        logger.info(
            'endpoint complete: provider=%s endpoint=%s records_fetched=%d '
            'rows_written=%d duplicates_dropped=%d files_written=%d '
            'deleted_partitions=%d elapsed_seconds=%.1f',
            definition.provider.value,
            definition.name,
            outcome.records_fetched,
            outcome.write.rows_written,
            outcome.write.duplicates_dropped,
            outcome.write.files_written,
            len(outcome.write.deleted_partitions),
            self._spine.clock.monotonic_seconds() - endpoint_started,
        )
