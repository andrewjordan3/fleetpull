# src/fleetpull/storage/splitting.py
"""Date-partition splitting: group a records frame into per-UTC-date sub-frames.

The write-unit decomposition the date-partitioned layout (the ``StorageKind``
date_partitioned arm) iterates over. A fetch arrives as one frame whose rows may
span several calendar dates; the date-partitioned layout writes one parquet file
per date (``date=YYYY-MM-DD/part.parquet``), so something must say which rows
belong to which date's file. That is this module's only job: read the event-time
column, take the UTC calendar date off each row, and group the rows by it.

A pure leaf -- stdlib ``date`` and Polars only, nothing internal. It does not
touch the filesystem, does not know what a partition path looks like, does not
read existing data, and does not merge, dedup, or filter by window; those are the
layout's and the merge's concerns. Pure frame -> list of (date, frame).

The date is the UTC calendar date, because every timestamp in fleetpull is UTC
end to end; the partition boundary is therefore UTC midnight, which lines the
partitions up exactly with the half-open ``[start, end)`` fetch window so no
event double-counts at an edge. Row order within a partition is the input's order
unchanged -- this function imposes no sort, that being out of scope (parquet is
order-indifferent and the end use is not fleetpull's to anticipate).
"""

from datetime import date

import polars as pl

__all__: list[str] = ['split_by_date']

_PARTITION_DATE_COLUMN: str = '_partition_date'


def split_by_date(
    frame: pl.DataFrame, event_time_column: str
) -> list[tuple[date, pl.DataFrame]]:
    """Group a frame's rows into per-UTC-date sub-frames.

    Derives the UTC calendar date of each row from ``event_time_column`` and
    partitions the frame on it, returning one ``(date, sub_frame)`` pair per
    distinct date present. The derived date is dropped from each sub-frame before
    return -- the date lives in the partition path (hive ``date=YYYY-MM-DD``), not
    inside the file, so storing it would be redundant and could collide with the
    path-derived column on read. Every other column, and the precise
    ``event_time_column`` value itself, is carried through unchanged.

    An empty frame yields an empty list: an empty frame has no dates, hence zero
    partitions, so the caller writes nothing and no empty ``date=.../`` directory
    is created -- a missing date in the on-disk tree therefore always reflects a
    real absence of data, never a silent gap.

    Args:
        frame: The records frame to split; ``event_time_column`` must be a UTC
            datetime column. Expected to hold one fetch's worth of records (e.g.
            one vehicle's locations over a window), kept small upstream so the
            materialized per-date sub-frames stay small.
        event_time_column: Name of the UTC datetime column whose calendar date
            keys the partitions (e.g. ``'located_at'``).

    Returns:
        One ``(date, sub_frame)`` pair per distinct UTC date, each key a
        ``datetime.date`` and each sub-frame that date's rows with the derived
        date column removed. Empty when ``frame`` is empty. Rows within a
        sub-frame keep their input order; the order of the pairs is unspecified
        beyond being deterministic for a given input.

    Raises:
        polars.exceptions.ColumnNotFoundError: ``event_time_column`` is absent
            from ``frame`` -- a caller bug, surfaced unguarded by Polars.
        polars.exceptions.PolarsError: ``event_time_column`` is not a temporal
            dtype, so the date accessor cannot apply -- a caller bug, left to
            propagate as Polars' own error.

    Side Effects:
        None -- pure function; ``frame`` is not mutated.
    """
    if frame.height == 0:
        return []
    with_partition_date = frame.with_columns(
        pl.col(event_time_column).dt.date().alias(_PARTITION_DATE_COLUMN)
    )
    partitions = with_partition_date.partition_by(
        _PARTITION_DATE_COLUMN, maintain_order=True
    )
    result: list[tuple[date, pl.DataFrame]] = []
    for partition_frame in partitions:
        partition_date: date = partition_frame.get_column(_PARTITION_DATE_COLUMN)[0]
        result.append((partition_date, partition_frame.drop(_PARTITION_DATE_COLUMN)))
    return result
