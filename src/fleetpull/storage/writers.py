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

The single-file family (``SnapshotWriter``) and the date-partitioned watermark cell
(``WatermarkPartitionedWriter``, staging + per-partition compaction + the prune) are
built; the feed cells (single and partitioned) fill with GeoTab.
"""

from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Protocol

import polars as pl

from fleetpull.endpoints.shared import (
    EndpointDefinition,
    SnapshotMode,
    StorageKind,
    WatermarkMode,
)
from fleetpull.incremental import DateWindow
from fleetpull.model_contract import ResponseModel
from fleetpull.paths import PathInput, endpoint_directory
from fleetpull.storage.atomic import atomic_write_parquet
from fleetpull.storage.files import data_file, partition_part_file
from fleetpull.storage.frames import drop_exact_duplicates
from fleetpull.storage.partitioning import prune_window_partitions, window_dates
from fleetpull.storage.read import read_parquet_if_exists
from fleetpull.storage.result import WriteResult
from fleetpull.storage.staging import (
    clear_partition_staging,
    compact_partition,
    stage_shard,
)

__all__: list[str] = [
    'DatasetWriter',
    'PartitionedWriter',
    'SingleFileWriter',
    'SnapshotWriter',
    'WatermarkPartitionedWriter',
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

    def __init__(self, target_dir: Path, *, drop_duplicates: bool = True) -> None:
        """Bind the writer to an endpoint directory.

        Args:
            target_dir: The endpoint directory holding ``data.parquet``.
            drop_duplicates: Whether ``finalize`` drops exact-duplicate rows
                (``storage.drop_exact_duplicates``, default on).

        Side Effects:
            None.
        """
        self._target_dir = target_dir
        self._drop_duplicates = drop_duplicates
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
        """Dedup (unless off) the subclass's finalized frame and write it.

        Returns:
            The write report (``files_written`` is always ``1``).

        Side Effects:
            Atomically rewrites ``data.parquet`` under the endpoint directory.
        """
        frame = self._finalize_frame()
        before = frame.height
        written = drop_exact_duplicates(frame) if self._drop_duplicates else frame
        atomic_write_parquet(written, data_file(self._target_dir))
        return WriteResult(
            rows_written=written.height,
            duplicates_dropped=before - written.height,
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


class PartitionedWriter(ABC):
    """Writers whose dataset is hive ``date=YYYY-MM-DD`` partitions.

    Shared substance: each ``write`` stages its piece as date-split shards
    (``stage_shard``); ``finalize`` folds each touched date's shards into that
    date's ``part.parquet`` (``compact_partition``) and reports. Two per-cell
    decisions are the subclass's: whether compaction folds in the existing
    partition (``_reads_existing`` -- append cells do, replace cells do not), and
    whether the run prunes the covered-but-empty dates (``_prunes`` -- a watermark
    refresh authoritatively replaces its window, so it prunes; an append-only feed
    does not). The ABC owns the staging and the finalize orchestration; the
    per-partition compaction is the shared ``compact_partition`` it drives.

    ``write`` carries the window tripwire: every staged partition date must lie
    in ``window_dates(window)``. Upstream, the resume window is day-aligned at
    resolution and the orchestrator window-filters each batch before writing,
    so a staged date outside the window means an upstream boundary was missed
    -- the require-inside half of the normalize-at-boundary doctrine (the
    canonical-UTC stance), raised loudly here rather than silently replacing or
    pruning a partition the run had no right to touch.
    """

    @property
    @abstractmethod
    def _reads_existing(self) -> bool:
        """Whether compaction folds the existing ``part.parquet`` into the result."""
        ...

    @property
    @abstractmethod
    def _prunes(self) -> bool:
        """Whether ``finalize`` deletes the window's covered-but-empty partitions."""
        ...

    def __init__(
        self,
        target_dir: Path,
        event_time_column: str,
        window: DateWindow,
        *,
        drop_duplicates: bool = True,
    ) -> None:
        """Bind the writer to an endpoint directory, event-time column, and window.

        Clears any stale shards a prior crashed run left under the window's covered
        dates before staging anything, so a later ``compact_partition`` folds only
        this run's shards, not a superseded row's pre-edit version. A covered date
        the clear empties (a crash before any ``part.parquet`` existed) has its
        now-empty ``date=`` directory removed too, upholding the
        no-empty-partition-directory invariant (DESIGN §3/§14).

        Assumes no overlapping writer for the same endpoint and window runs
        concurrently: the construction-time clear is destructive, so overlapping
        runs could delete each other's live staging. Orchestration must prevent
        overlapping runs for one endpoint.

        Args:
            target_dir: The endpoint directory holding the ``date=`` partitions.
            event_time_column: The UTC datetime column whose date keys the
                partitions.
            window: The run's half-open resume window -- the prune's bound and the
                covered-date set the construction-time staging clear sweeps.
            drop_duplicates: Whether compaction drops exact-duplicate rows
                (``storage.drop_exact_duplicates``, default on).

        Side Effects:
            Removes any existing ``staging/`` directory under the window's covered
            dates, and any ``date=`` directory the clear leaves empty.
        """
        self._target_dir = target_dir
        self._event_time_column = event_time_column
        self._window = window
        self._drop_duplicates = drop_duplicates
        self._covered_dates: frozenset[date] = frozenset(window_dates(window))
        self._written_dates: set[date] = set()
        clear_partition_staging(target_dir, self._covered_dates)

    def write(self, frame: pl.DataFrame) -> None:
        """Stage one fetched piece as date-split shards, inside the window only.

        Args:
            frame: One fetched piece (e.g. one vehicle's rows over the window).

        Raises:
            ValueError: A staged partition date lies outside the window's
                covered dates -- an upstream window filter missed rows (the
                interior tripwire; see the class docstring). The run fails
                loudly; the orphaned out-of-window shards are inert (``.shard``
                files are invisible to hive ``*.parquet`` reads) until a later
                run whose window covers that date sweeps them at construction.

        Side Effects:
            Writes ``.shard`` files under the touched dates' staging directories.
        """
        touched_dates = stage_shard(self._target_dir, frame, self._event_time_column)
        out_of_window = touched_dates - self._covered_dates
        if out_of_window:
            raise ValueError(
                f'staged partition dates outside the resume window: '
                f'{sorted(out_of_window)} not in {sorted(self._covered_dates)} '
                f'-- an upstream window filter missed these rows'
            )
        self._written_dates.update(touched_dates)

    def finalize(self) -> WriteResult:
        """Compact each partition, clear staging, prune if the cell prunes, and report.

        Returns:
            The write report -- rows and duplicates summed across the compacted
            partitions, ``files_written`` the partition count, and
            ``deleted_partitions`` the pruned dates (empty when the cell does not
            prune).

        Side Effects:
            Writes each touched ``part.parquet`` and clears its staging; deletes
            the pruned partition directories.
        """
        rows_written = 0
        duplicates_dropped = 0
        for partition_date in sorted(self._written_dates):
            existing = (
                read_parquet_if_exists(
                    partition_part_file(self._target_dir, partition_date)
                )
                if self._reads_existing
                else None
            )
            result = compact_partition(
                self._target_dir,
                partition_date,
                existing,
                drop_duplicates=self._drop_duplicates,
            )
            rows_written += result.rows_written
            duplicates_dropped += result.duplicates_dropped
        clear_partition_staging(self._target_dir, self._written_dates)
        deleted_partitions = (
            prune_window_partitions(self._target_dir, self._window, self._written_dates)
            if self._prunes
            else []
        )
        return WriteResult(
            rows_written=rows_written,
            duplicates_dropped=duplicates_dropped,
            files_written=len(self._written_dates),
            deleted_partitions=deleted_partitions,
        )


class WatermarkPartitionedWriter(PartitionedWriter):
    """``watermark`` + ``date_partitioned``: replace each covered partition, prune.

    The floored window refetches each covered date in full, so compaction replaces
    that date's ``part.parquet`` outright -- no existing read -- and the run prunes
    the covered dates that returned empty (a provider that deleted or edited
    records). Late, corrected, and deleted records all land through replacement,
    never a row-level merge (DESIGN §3/§4).
    """

    @property
    def _reads_existing(self) -> bool:
        """Replace, never fold in the prior partition."""
        return False

    @property
    def _prunes(self) -> bool:
        """The window is authoritatively replaced, so empty-refetch dates prune."""
        return True


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
            return SnapshotWriter(target_dir, drop_duplicates=drop_duplicates)
        case (StorageKind.DATE_PARTITIONED, WatermarkMode()):
            if window is None:
                raise ValueError(
                    f'{definition.provider.value}.{definition.name}: a watermark '
                    f'date-partitioned endpoint requires a resume window.'
                )
            event_time_column = definition.event_time_column
            if event_time_column is None:
                raise ValueError(
                    f'{definition.provider.value}.{definition.name}: a '
                    f'date-partitioned endpoint requires an event_time_column.'
                )
            return WatermarkPartitionedWriter(
                target_dir,
                event_time_column,
                window,
                drop_duplicates=drop_duplicates,
            )
        case _:
            raise NotImplementedError(
                f'no writer for storage_kind={definition.storage_kind} '
                f'sync_mode={type(definition.sync_mode).__name__} yet '
                f'(the single-file watermark cell and the feed cells are not built)'
            )
