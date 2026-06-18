# src/fleetpull/storage/layout.py
"""Storage layouts: how an endpoint's dataset is located, read, and written as
parquet. The ``StorageKind`` axis.

A ``Layout`` owns the write-unit structure and the I/O; the merge semantics are
injected (the orthogonal ``SyncMode`` axis), so the two axes compose as 2 layouts
+ 3 merges rather than 6 fused handlers. ``single`` is one unit -- the whole
dataset as one ``data.parquet``; ``date_partitioned`` (added with its consumer) is
one unit per date partition. Per unit the layout reads the existing frame, applies
the injected merge, dedups the result, and writes it atomically.

Only ``SingleFileLayout`` exists now. ``Layout`` is a Protocol -- pure shape; the
shared substance (the atomic write, the dedup) is free functions both
implementations call, not an inherited base.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import polars as pl

from fleetpull.storage.atomic import atomic_write_parquet
from fleetpull.storage.files import data_file
from fleetpull.storage.merge import MergeFn, drop_exact_duplicates
from fleetpull.storage.result import PersistResult

__all__: list[str] = ['Layout', 'SingleFileLayout']


class Layout(Protocol):
    """A storage layout: locate, read, and write an endpoint's dataset.

    The ``StorageKind`` axis. An implementation persists this run's ``new_frame``
    under ``endpoint_dir`` as one or more parquet files, applying ``merge`` per
    write unit, and reports what it wrote.
    """

    def write_dataset(
        self, endpoint_dir: Path, new_frame: pl.DataFrame, merge: MergeFn
    ) -> PersistResult:
        """Persist ``new_frame`` under ``endpoint_dir`` and report the write."""
        ...


@dataclass(frozen=True, slots=True)
class SingleFileLayout:
    """The ``single`` layout: the whole dataset is one ``data.parquet``.

    One write unit, rewritten each run. For low-volume endpoints (DESIGN §3),
    where reading and rewriting the whole file each run is acceptable. The read is
    unconditional and mode-agnostic; ``merge_snapshot`` simply discards it.
    """

    def write_dataset(
        self, endpoint_dir: Path, new_frame: pl.DataFrame, merge: MergeFn
    ) -> PersistResult:
        """Read the existing file, merge, dedup, and atomically rewrite it.

        Args:
            endpoint_dir: The endpoint directory holding ``data.parquet``.
            new_frame: This run's freshly fetched frame.
            merge: The write-semantics function for the endpoint's sync mode.

        Returns:
            The write report (``files_written`` is always ``1``).
        """
        target: Path = data_file(endpoint_dir)
        existing: pl.DataFrame | None = _read_if_exists(target)
        merged: pl.DataFrame = merge(existing, new_frame)
        before: int = merged.height
        deduped: pl.DataFrame = drop_exact_duplicates(merged)
        atomic_write_parquet(deduped, target)
        return PersistResult(
            rows_written=deduped.height,
            duplicates_dropped=before - deduped.height,
            files_written=1,
        )


def _read_if_exists(target: Path) -> pl.DataFrame | None:
    """Read a parquet file if it exists, else ``None`` (a first run)."""
    if target.exists():
        return pl.read_parquet(target)
    return None
