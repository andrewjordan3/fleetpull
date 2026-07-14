# src/fleetpull/storage/staging.py
"""Date-partition staging and compaction: the write half of the date-partitioned
path (the prune in ``partitioning.py`` is the delete half).

A date-partitioned watermark endpoint fans out -- the writer receives this run's
records one piece at a time. ``stage_shard`` lands each piece immediately as date-split shards under
``date=YYYY-MM-DD/staging/``, so no piece is held in memory waiting for the rest;
``compact_partition`` folds each date's shards into that date's single
``part.parquet``; ``clear_endpoint_staging`` removes staging the writer is done with
-- at construction a crashed run's stale shards, at finalize the shards just folded.
Three stateless functions, each one concern; the writer (``writers.py``) orchestrates
them and decides per cell whether compaction folds in the existing partition and
whether the run prunes (DESIGN §3).

Shards carry a ``.shard`` extension, not ``.parquet``, so a hive read of the live
dataset (``scan_parquet`` globbing ``**/*.parquet``, a BigQuery external table)
never picks up a half-staged partition's shards mid-run -- the queryable surface is
the ``part.parquet`` files alone. Compaction reads the shards back by explicit
path, which is format-driven, not extension-driven.

Memory is bounded by the chunk, not the endpoint (DESIGN §3): a partition holds one
chunk's rows for one date, materialized only at compaction and released after.
High-volume endpoints stay in bounds by a smaller date-chunk, not by streaming.
"""

import shutil
from collections.abc import Collection
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from fleetpull.storage.atomic import atomic_write_parquet
from fleetpull.storage.files import (
    endpoint_staging_dir,
    partition_part_file,
    partition_staging_dir,
    partition_staging_shard,
)
from fleetpull.storage.frames import drop_exact_duplicates
from fleetpull.storage.partition import split_by_date

__all__: list[str] = [
    'CompactionResult',
    'clear_endpoint_staging',
    'clear_partition_staging',
    'compact_partition',
    'stage_shard',
]


@dataclass(frozen=True, slots=True)
class CompactionResult:
    """The row counts from compacting one partition.

    Attributes:
        rows_written: Rows in the written ``part.parquet`` (after dedup).
        duplicates_dropped: Exact-duplicate rows removed during compaction.
    """

    rows_written: int
    duplicates_dropped: int


def stage_shard(
    endpoint_dir: Path, frame: pl.DataFrame, event_time_column: str
) -> set[date]:
    """Land one fetched piece as date-split shards under the partitions' staging.

    Splits ``frame`` by the UTC date of ``event_time_column`` and writes each
    date's rows as a uniquely-named ``.shard`` file under that date's ``staging/``
    directory, atomically. An empty frame writes nothing. Returns the dates
    touched so the writer knows which partitions to compact.

    Args:
        endpoint_dir: The endpoint directory holding the ``date=`` partitions.
        frame: One fetched piece (e.g. one vehicle's rows over the window).
        event_time_column: The UTC datetime column whose date keys the partitions.

    Returns:
        The set of UTC dates that received a shard (empty for an empty frame).

    Side Effects:
        Writes one ``.shard`` file per date present, creating staging directories
        as needed.
    """
    touched: set[date] = set()
    for partition_date, sub_frame in split_by_date(frame, event_time_column):
        atomic_write_parquet(
            sub_frame, partition_staging_shard(endpoint_dir, partition_date)
        )
        touched.add(partition_date)
    return touched


def compact_partition(
    endpoint_dir: Path,
    partition_date: date,
    existing: pl.DataFrame | None,
    *,
    drop_duplicates: bool = True,
) -> CompactionResult:
    """Fold one date's staged shards into its ``part.parquet``.

    Reads every ``.shard`` under the date's ``staging/`` directory, concatenates
    them (and ``existing`` if the cell folds in the prior partition), drops exact
    duplicates unless the flag turns that off, and writes the result atomically
    to ``part.parquet``, replacing any prior file. The whole partition is
    materialized here; it is bounded by the chunk (DESIGN §3), not the endpoint.
    Folds and nothing else -- the writer clears the staging afterward
    (``clear_endpoint_staging``), keeping this single-concern.

    Args:
        endpoint_dir: The endpoint directory holding the ``date=`` partitions.
        partition_date: The date whose staged shards to compact; its ``staging/``
            directory must hold at least one shard.
        existing: The prior ``part.parquet`` contents to fold in (append cells), or
            ``None`` to replace the partition wholesale (watermark cells).
        drop_duplicates: Whether to drop exact-duplicate rows (DESIGN §6 --
            ``storage.drop_exact_duplicates``, default on). ``False`` writes the
            combined rows byte-for-byte.

    Returns:
        The written partition's row counts.

    Side Effects:
        Writes ``part.parquet``. Leaves the staging shards in place for the caller
        to clear.
    """
    staging_dir: Path = partition_staging_dir(endpoint_dir, partition_date)
    shard_frames: list[pl.DataFrame] = [
        pl.read_parquet(shard) for shard in sorted(staging_dir.glob('*.shard'))
    ]
    if existing is not None:
        shard_frames.append(existing)
    combined: pl.DataFrame = pl.concat(shard_frames)
    before: int = combined.height
    written: pl.DataFrame = (
        drop_exact_duplicates(combined) if drop_duplicates else combined
    )
    atomic_write_parquet(written, partition_part_file(endpoint_dir, partition_date))
    return CompactionResult(
        rows_written=written.height, duplicates_dropped=before - written.height
    )


def clear_endpoint_staging(endpoint_dir: Path) -> None:
    """Remove the endpoint-level temporary staging root if present.

    Args:
        endpoint_dir: The endpoint directory holding the ``date=`` partitions.

    Side Effects:
        Removes ``{endpoint_dir}/.staging`` and every temporary shard below it.
    """
    staging_root: Path = endpoint_staging_dir(endpoint_dir)
    if staging_root.exists():
        shutil.rmtree(staging_root)


def clear_partition_staging(
    endpoint_dir: Path, partition_dates: Collection[date]
) -> None:
    """Compatibility wrapper for clearing the endpoint-level staging root."""
    clear_endpoint_staging(endpoint_dir)
