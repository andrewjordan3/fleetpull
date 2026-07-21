# src/fleetpull/orchestrator/spine.py
"""The run executor's shared spine: its narrow protocols and drive bundle.

The narrow Protocols the run executor and its drive arms depend on instead of
the concrete state and network classes -- ``ClientSource`` (the registry's
``client_for``; the roster refresh coordinator shares this one declaration),
``RunRecorder`` (the ledger's lifecycle methods), and ``CursorAccess`` (the
cursor store's read and its two kind-guarded writes) -- with the three
state-database surfaces bundled as ``RunStateAccess`` and the whole drive kit
bundled as ``RunnerSpine``: the collaborators plus the runner-owned seams
(the one writer-factory call site, the metadata projection) every arm runs
through. They live here, below the runner and the drive modules alike, so the
runner can construct the drives without an import cycle.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from fleetpull.config import SyncConfig
from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.incremental import DateWindow, IncrementalCursor
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.metadata_projection import MetadataProjection
from fleetpull.orchestrator.unit_loop import UnitQueue
from fleetpull.storage import DatasetWriter
from fleetpull.timing import Clock
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'ClientSource',
    'CursorAccess',
    'RunRecorder',
    'RunStateAccess',
    'RunnerSpine',
    'WriterFactory',
]


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
        """Close a run as succeeded with its row count (and a feed run's token)."""
        ...

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        """Close a run as failed with an error detail."""
        ...

    def start_window_run(
        self, provider: Provider, endpoint: str, *, window: DateWindow
    ) -> int:
        """Open a watermark run for a window and return its id."""
        ...

    def start_feed_run(
        self, provider: Provider, endpoint: str, *, from_version: str
    ) -> int:
        """Open a feed run resuming from a token (or seed label) and return its id."""
        ...

    def coverage_frontier(self, provider: Provider, endpoint: str) -> datetime | None:
        """Return the furthest window end a succeeded run has covered, if any."""
        ...


class CursorAccess(Protocol):
    """The cursor surface the incremental arms need (a subset of CursorStore)."""

    def get_cursor(self, provider: Provider, endpoint: str) -> IncrementalCursor | None:
        """Return the persisted cursor for a (provider, endpoint), or None."""
        ...

    def advance_watermark_forward(
        self, provider: Provider, endpoint: str, observed: datetime
    ) -> bool:
        """Atomically advance the date watermark iff strictly forward."""
        ...

    def commit_feed_token(
        self, provider: Provider, endpoint: str, to_version: str
    ) -> None:
        """Commit the feed cursor, kind-guarded last-write-wins."""
        ...


@dataclass(frozen=True, slots=True)
class RunStateAccess:
    """The three state-database surfaces one endpoint run commits through.

    They always travel together -- the composition root builds all three
    over the one state database and the runner's per-unit crash order
    sequences them (parquet -> ledger completion -> unit done-mark ->
    watermark prefix commit) -- so they ride as one collaborator (the
    bundle rule).

    Attributes:
        recorder: The run ledger's lifecycle surface.
        cursors: The cursor store's read and kind-guarded-write surface.
        units: The work-unit claim queue.
    """

    recorder: RunRecorder
    cursors: CursorAccess
    units: UnitQueue


class WriterFactory(Protocol):
    """The runner's ONE writer-construction seam, as the drives receive it.

    A bound view of the runner's ``select_writer`` call site (dataset root
    and dedup switch already closed over), so every arm constructs its
    writer through one place and the routing face is called exactly once
    in the orchestration layer.
    """

    def __call__(
        self,
        definition: EndpointDefinition[ResponseModel],
        window: DateWindow | None = None,
    ) -> DatasetWriter:
        """Construct the endpoint's writer for this run (and window, if any)."""
        ...


@dataclass(frozen=True, slots=True)
class RunnerSpine:
    """The whole drive kit: collaborators plus the runner-owned seams.

    What the snapshot arm and both drive classes (the watermark and feed
    drives) run through, bundled once (the bundle rule) so the drives
    construct from a single parameter in ``EndpointRunner.__init__``.

    Attributes:
        clients: Hands out an open per-provider client (the registry).
        state: The state-database surfaces -- ledger, cursor store, unit
            queue.
        clock: Supplies the run instant (trailing edge, future-event guard).
        sync: The sync section -- the cold-start anchor, the unit width,
            and the unit worker count.
        make_writer: The runner's one writer-factory call site, bound.
        projection: The post-commit ``metadata.json`` projection.
    """

    clients: ClientSource
    state: RunStateAccess
    clock: Clock
    sync: SyncConfig
    make_writer: WriterFactory
    projection: MetadataProjection
