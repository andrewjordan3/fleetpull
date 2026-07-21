# src/fleetpull/storage/atomic.py
"""The atomic file writes: the storage layer's durability primitives.

Every layout's every write goes through here. A file write is not atomic -- a
crash mid-write corrupts the file -- so the content is written to a temp sibling
and renamed onto the target, which POSIX guarantees is atomic on one filesystem.
The prior target is untouched until the rename; a crash leaves either the old
file or the new, never a partial one (DESIGN §5 crash-safety). The temp is
always cleaned up: on success (already renamed away) or on failure.
``atomic_write_parquet`` is the parquet write every dataset writer uses;
``atomic_write_text`` is the same temp-then-rename skeleton for the
``metadata.json`` projection's document text.
"""

import os
from pathlib import Path

import polars as pl

from fleetpull.polars_typing import ParquetCompression
from fleetpull.storage.files import temp_sibling_path

__all__: list[str] = ['atomic_write_parquet', 'atomic_write_text']


def _fsync_path(path: Path) -> None:
    """Flush one path's kernel buffers to stable storage.

    The open/fsync/close block the durable-rename recipe applies twice --
    to the temp file before the rename, to the parent directory after it.

    Args:
        path: The file or directory to fsync.

    Raises:
        OSError: The open or fsync failed.

    Side Effects:
        Issues an ``fsync`` system call.
    """
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_parquet(
    frame: pl.DataFrame,
    target: Path,
    compression: ParquetCompression = 'snappy',
    *,
    durable: bool = False,
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
        durable: When True, fsync the temp file before the rename and the
            parent directory after it -- the durable-rename recipe. The feed
            append cell requires it: its token commit is fsynced (SQLite),
            so a power loss must never persist a token whose page the page
            cache still held. The self-healing writers (replace-partition
            under lookback refetch, snapshot rewrite) stay non-durable --
            a lost write there is refetched by design.

    Side Effects:
        Creates ``target``'s parent directory; writes and renames files;
        when ``durable``, fsyncs the file and its directory.

    Raises:
        OSError: If the write, rename, or fsync fails (the temp is cleaned
            up first).
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temp: Path = temp_sibling_path(target)
    try:
        frame.write_parquet(temp, compression=compression)
        if durable:
            _fsync_path(temp)
        temp.replace(target)
        if durable:
            _fsync_path(target.parent)
    finally:
        temp.unlink(missing_ok=True)


def atomic_write_text(text: str, target: Path) -> None:
    """Write ``text`` to ``target`` atomically via temp-then-rename.

    The parquet write's temp-then-rename skeleton applied to document text:
    temp-sibling in the target's own directory, so the rename is
    same-filesystem and atomic; a crash leaves the prior file or the new one,
    never a partial. The target's parent directory is deliberately NOT
    created -- the one caller (the ``metadata.json`` projection) requires an
    absent endpoint directory to surface as ``OSError``, never be papered
    over with a ``mkdir``.

    Args:
        text: The complete document text to persist, written UTF-8.
        target: The final file path; its parent directory must exist.

    Raises:
        OSError: The write or rename failed -- including a missing parent
            directory (the temp is cleaned up first).

    Side Effects:
        Writes and renames files inside ``target``'s directory.
    """
    temp: Path = temp_sibling_path(target)
    try:
        temp.write_text(text, encoding='utf-8')
        temp.replace(target)
    finally:
        temp.unlink(missing_ok=True)
