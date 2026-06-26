# src/fleetpull/storage/staging.py
"""Date-partition staging and compaction: the write half of the date-partitioned
path (the prune in ``partitioning.py`` is the delete half).

A date-partitioned watermark endpoint fans out -- the writer receives this run's
records one piece at a time. ``stage_shard`` lands each piece immediately as
date-split shards under ``date=YYYY-MM-DD/staging/``, so no piece is held in memory
waiting for the rest; ``compact_partition`` then folds each date's shards into that
date's single ``part.parquet`` and clears the staging directory. Three stateless
functions; the writer (``writers.py``) orchestrates them and decides per cell
whether compaction folds in the existing partition and whether the run prunes
(DESIGN §3).

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
    partition_dir,
    partition_part_file,
    partition_staging_dir,
    partition_staging_shard,
)
from fleetpull.storage.frames import drop_exact_duplicates
from fleetpull.storage.partition import split_by_date

__all__: list[str] = [
    'CompactionResult',
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
    endpoint_dir: Path, partition_date: date, existing: pl.DataFrame | None
) -> CompactionResult:
    """Fold one date's staged shards into its ``part.parquet`` and clear staging.

    Reads every ``.shard`` under the date's ``staging/`` directory, concatenates
    them (and ``existing`` if the cell folds in the prior partition), drops exact
    duplicates, writes the result atomically to ``part.parquet`` -- replacing any
    prior file -- and removes the staging directory. The whole partition is
    materialized here; it is bounded by the chunk (DESIGN §3), not the endpoint.

    Args:
        endpoint_dir: The endpoint directory holding the ``date=`` partitions.
        partition_date: The date whose staged shards to compact; its ``staging/``
            directory must hold at least one shard.
        existing: The prior ``part.parquet`` contents to fold in (append cells),
            or ``None`` to replace the partition wholesale (watermark cells).

    Returns:
        The written partition's row counts.

    Side Effects:
        Writes ``part.parquet``; deletes the date's ``staging/`` directory.
    """
    staging_dir = partition_staging_dir(endpoint_dir, partition_date)
    shard_frames = [
        pl.read_parquet(shard) for shard in sorted(staging_dir.glob('*.shard'))
    ]
    if existing is not None:
        shard_frames.append(existing)
    combined = pl.concat(shard_frames)
    before = combined.height
    deduped = drop_exact_duplicates(combined)
    atomic_write_parquet(deduped, partition_part_file(endpoint_dir, partition_date))
    shutil.rmtree(staging_dir)
    return CompactionResult(
        rows_written=deduped.height, duplicates_dropped=before - deduped.height
    )


def clear_partition_staging(
    endpoint_dir: Path, partition_dates: Collection[date]
) -> None:
    """Remove any staged shards a crashed prior run left under these dates.

    For each date, removes its ``staging/`` directory if present, then removes the
    enclosing ``date=`` directory if that left it empty. A crash that died after the
    first ``stage_shard`` but before any ``compact_partition`` wrote a
    ``part.parquet`` leaves a ``date=`` holding only ``staging/``; an empty ``date=``
    directory must not survive (it would read as a partition with no data, the
    invariant ``delete_partition`` upholds), so the now-empty directory goes too.
    Only a genuinely empty directory is removed -- a surviving ``part.parquet`` or a
    crashed atomic-write temp keeps it. Lenient on a missing ``staging/``: a clean
    prior run removes its own staging at compaction, so absence is the normal case,
    the opposite stance from ``delete_partition``'s strictness. Called at writer
    construction, before any ``stage_shard``, so the only shards a later
    ``compact_partition`` folds are the live run's own (DESIGN §3/§14).

    Args:
        endpoint_dir: The endpoint directory holding the ``date=`` partitions.
        partition_dates: The dates whose staging to clear (the window's covered
            dates).

    Side Effects:
        Removes any existing ``staging/`` directory under those dates, and any
        ``date=`` directory left empty by that removal.
    """
    for partition_date in partition_dates:
        staging_dir = partition_staging_dir(endpoint_dir, partition_date)
        if not staging_dir.exists():
            continue
        shutil.rmtree(staging_dir)
        date_dir = partition_dir(endpoint_dir, partition_date)
        if not any(date_dir.iterdir()):
            date_dir.rmdir()
