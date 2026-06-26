# src/fleetpull/orchestrator/runner.py
"""The run executor: run one endpoint to completion, once per (endpoint, run).

``EndpointRunner`` owns one endpoint's run transaction -- open the ledger run, build
the writer, drive the request driver, consume each record batch it yields (validate
-> frame -> write), finalize once, complete the run once -- and dispatches on the
endpoint's ``sync_mode``. The snapshot arm is built here; the watermark arm (window
resolution, the two future-time guards, the cursor advance, the parquet -> cursor ->
ledger ordering) and the feed arm raise ``NotImplementedError`` until their prompts.
Request cardinality and batch granularity are the driver's; the runner is blind to
both.

The runner depends on two narrow Protocols rather than the concrete state and
network classes: ``ClientSource`` (the registry's ``client_for``) and
``RunRecorder`` (the ledger's lifecycle methods). It opens no clients and reads no
credentials -- the already-open client source hands it the provider's client.
"""

import logging
from typing import Protocol

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    WatermarkMode,
)
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.outcome import Executed, RunOutcome
from fleetpull.paths import PathInput
from fleetpull.records import models_to_dataframe, validate_records
from fleetpull.storage import select_writer
from fleetpull.vocabulary import Provider

__all__: list[str] = ['ClientSource', 'EndpointRunner', 'RunRecorder']

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


class EndpointRunner:
    """Runs one endpoint to completion, dispatching on its sync mode.

    Constructed once with the client source, the run recorder, and the dataset root;
    ``run`` takes the endpoint and its request driver, so one instance runs every
    endpoint. The snapshot arm is built; the watermark and feed arms raise
    ``NotImplementedError``.
    """

    def __init__(
        self,
        client_source: ClientSource,
        run_recorder: RunRecorder,
        dataset_root: PathInput,
    ) -> None:
        """
        Args:
            client_source: Hands out an open per-provider client (the registry).
            run_recorder: Records each run's lifecycle (the ledger).
            dataset_root: The root directory the endpoint's dataset is written
                under.
        """
        self._client_source = client_source
        self._run_recorder = run_recorder
        self._dataset_root = dataset_root

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
            case WatermarkMode() | FeedMode():
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
            for batch in driver.record_batches(definition, client, resume=None):
                models = validate_records(batch, definition.response_model)
                frame = models_to_dataframe(models, definition.response_model)
                writer.write(frame)
                records_fetched += len(models)
            write = writer.finalize()
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
