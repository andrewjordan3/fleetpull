# src/fleetpull/storage/writers.py
"""The dataset-writer contract and its routing face.

A ``DatasetWriter`` accepts this run's records in one or more pieces and finalizes
them onto disk as the endpoint's dataset. Each ``(StorageKind, SyncMode)`` cell is
its own writer, because the write semantics are not freely composable across the
two axes -- a floored watermark write *replaces* under date partitioning but
*clears and appends* under a single file, so the semantic depends on both axes at
once. The writer families live with their layouts -- the single-file family in
``storage/single_file.py``, the date-partitioned family in
``storage/partitioned.py``, the append-log feed cell in ``storage/append.py`` --
and this module owns the contract (``DatasetWriter``) and the one routing point
(``select_writer``).

The orchestrator drives every endpoint identically: ``select_writer`` returns the
cell's writer, the orchestrator calls ``write`` for each frame it has (once for a
snapshot, once per fanned-out unit for a partitioned watermark endpoint), then
``finalize`` once. The single-file date-window cell remains unbuilt.
"""

from typing import Protocol

import polars as pl

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    SnapshotMode,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.paths import PathInput, endpoint_directory
from fleetpull.storage.append import FeedAppendWriter
from fleetpull.storage.partitioned import WatermarkPartitionedWriter
from fleetpull.storage.result import WriteResult
from fleetpull.storage.single_file import SnapshotWriter

__all__: list[str] = ['DatasetWriter', 'select_writer']


class DatasetWriter(Protocol):
    """The storage write surface: accept records in pieces, finalize to disk.

    An implementation is constructed for one endpoint's one run, with any runtime
    context that run needs (e.g. an incremental ``window``). The orchestrator calls
    ``write`` for each frame it has, then ``finalize`` once. A plain Protocol --
    composed and called through ``select_writer``, never dynamically verified.
    """

    def write(self, frame: pl.DataFrame) -> None:
        """Accept one piece of this run's records.

        Args:
            frame: A validated, flattened frame for this endpoint. Called at least
                once per run (a zero-row but typed frame is valid).
        """
        ...

    def finalize(self) -> WriteResult:
        """Finalize the accepted records onto disk and report the write.

        Returns:
            The write report.
        """
        ...


def select_writer(
    definition: EndpointDefinition[ResponseModel],
    dataset_root: PathInput,
    *,
    window: DateWindow | None = None,
    drop_duplicates: bool = True,
) -> DatasetWriter:
    """Construct the writer for an endpoint's ``(StorageKind, SyncMode)`` cell.

    The single routing point. Resolves the endpoint directory under
    ``dataset_root`` and returns the cell's writer, constructed with the runtime
    context that cell needs. ``window`` is the run's resume window -- consumed by
    the incremental writers (the prune, the window-clear) and forbidden for the
    snapshot cell, which has no resume.

    Args:
        definition: The endpoint binding; supplies the provider / endpoint
            directory names and the storage-kind / sync-mode cell.
        dataset_root: The dataset root directory.
        window: The run's half-open resume window, for incremental cells.
        drop_duplicates: Whether the writer's compaction drops exact-duplicate
            rows (``storage.drop_exact_duplicates``, default on).

    Returns:
        The cell's ``DatasetWriter``.

    Raises:
        ValueError: A ``window`` was supplied for the snapshot or feed cell.
        RuntimeError: An event-time-requiring cell carries no
            ``event_time_column`` (via ``required_event_time_column`` --
            impossible past construction validation).
        NotImplementedError: The endpoint's cell is not yet built (the
            single-file date-window cell).

    Side Effects:
        None.
    """
    target_dir = endpoint_directory(
        dataset_root, definition.provider.value, definition.name
    )
    match (definition.storage_kind, definition.sync_mode):
        case (StorageKind.SINGLE, SnapshotMode()):
            if window is not None:
                raise ValueError(
                    f'{definition.provider.value}.{definition.name}: snapshot '
                    f'endpoints have no resume window.'
                )
            return SnapshotWriter(target_dir, drop_duplicates=drop_duplicates)
        case (StorageKind.APPEND_LOG, FeedMode()):
            if window is not None:
                raise ValueError(
                    f'{definition.provider.value}.{definition.name}: feed '
                    f'endpoints have no resume window.'
                )
            # drop_duplicates is deliberately not threaded: the append-log
            # cell performs no write-time dedup by design (stored-as-emitted,
            # DESIGN section 4; storage/append.py carries the rationale).
            return FeedAppendWriter(target_dir, definition.required_event_time_column)
        case (StorageKind.DATE_PARTITIONED, WatermarkMode()):
            if window is None:
                raise ValueError(
                    f'{definition.provider.value}.{definition.name}: a watermark '
                    f'date-partitioned endpoint requires a resume window.'
                )
            return WatermarkPartitionedWriter(
                target_dir,
                definition.required_event_time_column,
                window,
                drop_duplicates=drop_duplicates,
            )
        case _:
            raise NotImplementedError(
                f'no writer for storage_kind={definition.storage_kind} '
                f'sync_mode={type(definition.sync_mode).__name__} yet '
                f'(the single-file watermark cell is not built)'
            )
