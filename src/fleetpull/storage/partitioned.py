# src/fleetpull/storage/partitioned.py
"""The date-partitioned writer family: hive ``date=YYYY-MM-DD`` partitions.

``PartitionedWriter`` is the family's shared substance -- stage each piece as
date-split shards, fold each touched date's shards into its ``part.parquet``,
prune where the cell prunes -- and ``WatermarkPartitionedWriter`` is the
shipped ``(DATE_PARTITIONED, WatermarkMode)`` cell (replace each covered
partition, prune the covered-but-empty dates; DESIGN §3/§4). The feed cell is
deliberately NOT in this family: it appends numbered part files with per-write
durability and no window (``storage/append.py``). ``select_writer``
(``storage/writers.py``) is the routing face that constructs these.
"""

from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

import polars as pl

from fleetpull.incremental import DateWindow
from fleetpull.storage.files import partition_part_file
from fleetpull.storage.pruning import prune_window_partitions, window_dates
from fleetpull.storage.read import read_parquet_if_exists
from fleetpull.storage.result import WriteResult
from fleetpull.storage.staging import (
    clear_partition_staging,
    compact_partition,
    stage_shard,
)

__all__: list[str] = ['PartitionedWriter', 'WatermarkPartitionedWriter']


class PartitionedWriter(ABC):
    """Writers whose dataset is hive ``date=YYYY-MM-DD`` partitions.

    Shared substance: each ``write`` stages its piece as date-split shards
    (``stage_shard``); ``finalize`` folds each touched date's shards into that
    date's ``part.parquet`` (``compact_partition``) and reports. Two per-cell
    decisions are the subclass's: whether compaction folds in the existing
    partition (``_reads_existing`` -- fold-in cells would, replace cells do
    not), and whether the run prunes the covered-but-empty dates (``_prunes``
    -- a watermark refresh authoritatively replaces its window, so it prunes).
    The ABC owns the staging and the finalize orchestration; the per-partition
    compaction is the shared ``compact_partition`` it drives.

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
            tuple(
                prune_window_partitions(
                    self._target_dir, self._window, self._written_dates
                )
            )
            if self._prunes
            else ()
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
