# src/fleetpull/storage/atomic.py
"""The atomic parquet write: the storage layer's single durability primitive.

Every layout's every write goes through here. A parquet write is not atomic -- a
crash mid-write corrupts the file -- so the frame is written to a temp sibling and
renamed onto the target, which POSIX guarantees is atomic on one filesystem. The
prior target is untouched until the rename; a crash leaves either the old file or
the new, never a partial one (DESIGN §5 crash-safety). The temp is always cleaned
up: on success (already renamed away) or on failure.
"""

from pathlib import Path

import polars as pl

from fleetpull.polars_typing import ParquetCompression
from fleetpull.storage.files import temp_sibling_path

__all__: list[str] = ['atomic_write_parquet']


def atomic_write_parquet(
    frame: pl.DataFrame, target: Path, compression: ParquetCompression = 'snappy'
) -> None:
    """Write ``frame`` to ``target`` atomically via temp-then-rename.

    Ensures ``target``'s parent directory exists, writes the frame to a temp
    sibling, then atomically renames it onto ``target``. Never leaves a partial
    ``target``.

    Args:
        frame: The DataFrame to persist.
        target: The final parquet path.
        compression: Parquet compression codec. ``'snappy'`` by default (fast,
            BigQuery-friendly); a later config surface parameterizes it.

    Side Effects:
        Creates ``target``'s parent directory; writes and renames files.

    Raises:
        OSError: If the write or rename fails (the temp is cleaned up first).
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temp: Path = temp_sibling_path(target)
    try:
        frame.write_parquet(temp, compression=compression)
        temp.replace(target)
    finally:
        temp.unlink(missing_ok=True)
