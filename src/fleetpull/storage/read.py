# src/fleetpull/storage/read.py
"""Existence-tolerant parquet reads: the read sibling of the atomic write.

A writer that combines this run's records with what is already on disk reads the
prior file through here; a first run -- no file yet -- comes back as ``None``
rather than an error, so the writer's combine handles the empty case the same way
every time. The built cells all replace rather than combine (the snapshot writer
rewrites the single file; the partitioned watermark cell replaces its partitions),
so no writer reads today; the combining cells -- the single-file watermark cell
and the feed cells -- fill with GeoTab.
"""

from pathlib import Path

import polars as pl

__all__: list[str] = ['read_parquet_if_exists']


def read_parquet_if_exists(path: Path) -> pl.DataFrame | None:
    """Read a parquet file, or ``None`` if it does not exist.

    Args:
        path: The parquet file to read.

    Returns:
        The frame, or ``None`` when ``path`` does not exist (a first run).

    Side Effects:
        Reads ``path`` if present.
    """
    if path.exists():
        return pl.read_parquet(path)
    return None
