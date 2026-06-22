# src/fleetpull/storage/writers.py
"""Dataset writers: the storage layer's write surface.

A ``DatasetWriter`` accepts this run's records in one or more pieces and finalizes
them onto disk as the endpoint's dataset. It supersedes the old ``persist`` +
``Layout`` + injected-merge model: each ``(StorageKind, SyncMode)`` cell is its own
writer, because the write semantics are not freely composable across the two axes
-- a floored watermark write *replaces* under date partitioning but *clears and
appends* under a single file, so the semantic depends on both axes at once, which
an injected per-``SyncMode`` merge could not express.

The orchestrator drives every endpoint identically: ``select_writer`` returns the
cell's writer, the orchestrator calls ``write`` for each frame it has (once for a
snapshot, once per fanned-out unit for a partitioned watermark endpoint), then
``finalize`` once. The writers are thin stateful shells over the pure storage
helpers (the atomic write, the exact dedup); the runtime resume ``window`` an
incremental writer needs is supplied at construction, not on ``write``.

Only the single-file family is built here (``SnapshotWriter``); the partitioned
family (staging + per-partition compaction + the prune) lands in Part 2.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Protocol

import polars as pl

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StorageKind,
)
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.paths import PathInput, endpoint_directory
from fleetpull.storage.atomic import atomic_write_parquet
from fleetpull.storage.files import data_file
from fleetpull.storage.frames import drop_exact_duplicates
from fleetpull.storage.result import WriteResult

__all__: list[str] = [
    'DatasetWriter',
    'SingleFileWriter',
    'SnapshotWriter',
    'select_writer',
]


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


class SingleFileWriter(ABC):
    """Writers whose dataset is one ``data.parquet`` rewritten each run.

    Shared substance: accumulate the run's frames, and on ``finalize`` dedup the
    subclass's finalized frame and atomically rewrite the single file. Subclasses
    supply ``_finalize_frame`` -- the per-cell write semantic, which decides
    whether the prior file is read at all. A snapshot does not read it (it
    replaces); the feed and watermark single-file cells do (they combine with
    prior rows), each reading through ``read_parquet_if_exists`` themselves. The
    base never reads -- the read is opt-in by the subclass.
    """

    def __init__(self, target_dir: Path) -> None:
        """Bind the writer to an endpoint directory.

        Args:
            target_dir: The endpoint directory holding ``data.parquet``.

        Side Effects:
            None.
        """
        self._target_dir = target_dir
        self._frames: list[pl.DataFrame] = []

    def write(self, frame: pl.DataFrame) -> None:
        """Accumulate one frame for this run.

        Args:
            frame: A validated, flattened frame for this endpoint.

        Side Effects:
            Holds a reference to ``frame`` until ``finalize``.
        """
        self._frames.append(frame)

    def finalize(self) -> WriteResult:
        """Dedup the subclass's finalized frame and atomically write it.

        Returns:
            The write report (``files_written`` is always ``1``).

        Side Effects:
            Atomically rewrites ``data.parquet`` under the endpoint directory.
        """
        frame = self._finalize_frame()
        before = frame.height
        deduped = drop_exact_duplicates(frame)
        atomic_write_parquet(deduped, data_file(self._target_dir))
        return WriteResult(
            rows_written=deduped.height,
            duplicates_dropped=before - deduped.height,
            files_written=1,
        )

    @abstractmethod
    def _finalize_frame(self) -> pl.DataFrame:
        """Produce the frame to dedup and write for this run.

        The per-cell write semantic. A replace cell returns its accumulated frame;
        a combine cell reads the prior file (via ``read_parquet_if_exists``) and
        merges it with the accumulated frame. Reading the prior file is the
        subclass's choice -- the base never reads.

        Returns:
            The frame to dedup and write.

        Side Effects:
            A combine subclass reads ``data.parquet``; a replace subclass does not.
        """
        ...

    def _accumulated(self) -> pl.DataFrame:
        """This run's accumulated ``write`` frames as one frame.

        Returns:
            The concatenation of every frame passed to ``write``. ``write`` must
            have been called at least once.

        Side Effects:
            None.
        """
        return pl.concat(self._frames)


class SnapshotWriter(SingleFileWriter):
    """``snapshot`` + ``single``: full replacement of the current-state dataset.

    A snapshot re-fetches its whole current-state dataset every run, so the result
    is just this run's accumulated frame and the prior file is never read -- it is
    overwritten by the atomic rename (DESIGN §3).
    """

    def _finalize_frame(self) -> pl.DataFrame:
        """Return this run's accumulated frame; the prior file is not read.

        Returns:
            The accumulated frame, replacing the prior dataset wholesale.

        Side Effects:
            None -- the prior ``data.parquet`` is overwritten without being read.
        """
        return self._accumulated()


def select_writer(
    definition: EndpointDefinition[ResponseModel],
    dataset_root: PathInput,
    *,
    window: DateWindow | None = None,
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

    Returns:
        The cell's ``DatasetWriter``.

    Raises:
        ValueError: A ``window`` was supplied for the snapshot cell.
        NotImplementedError: The endpoint's cell is not yet built (anything but
            ``snapshot`` + ``single``).

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
            return SnapshotWriter(target_dir)
        case _:
            raise NotImplementedError(
                f'no writer for storage_kind={definition.storage_kind} '
                f'sync_mode={type(definition.sync_mode).__name__} yet '
                f'(partitioned and feed writers land in Part 2)'
            )
