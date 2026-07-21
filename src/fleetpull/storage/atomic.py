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

    The open/fsync/close block the durable-rename recipe applies to the
    temp file before the rename and to each directory of the durable
    chain after it.

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


def _durable_directory_chain(directory: Path) -> list[Path]:
    """The directories a durable write must fsync after its rename.

    Always ``directory`` itself -- the rename lands the file's entry
    there. When the write is about to CREATE missing ancestors (a feed
    page opening a new ``date=`` partition, or the first-ever write of an
    endpoint), each new directory's own entry lives one level up, so the
    chain extends through every missing ancestor to the first
    pre-existing one: a new-directory chain fsynced only at its deepest
    link is NOT durable -- power loss can drop the newly created
    directory (and the file inside it) from the un-fsynced parent while
    a later fsynced commit survives, persisting a cursor past lost data.

    Must be computed BEFORE the ``mkdir`` that creates the chain, while
    the missing set is still observable.

    Args:
        directory: The file's parent directory, existing or about to be
            created.

    Returns:
        ``directory`` first, then each missing ancestor's parent up to
        and including the first pre-existing directory. Just
        ``[directory]`` when it already exists.
    """
    chain = [directory]
    current = directory
    while not current.exists():
        chain.append(current.parent)
        current = current.parent
    return chain


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
            durable directory chain after it -- the parent, every ancestor
            this write newly created, and the first pre-existing ancestor
            (whose entry for the new chain must also reach stable
            storage). The feed append cell requires it: its token commit
            is fsynced (SQLite), so a power loss must never persist a
            token whose page -- or whose newly created partition
            directory -- the page cache still held. The self-healing
            writers (replace-partition under lookback refetch, snapshot
            rewrite) stay non-durable -- a lost write there is refetched
            by design.

    Side Effects:
        Creates ``target``'s parent directory; writes and renames files;
        when ``durable``, fsyncs the file and the directory chain.

    Raises:
        OSError: If the write, rename, or fsync fails (the temp is cleaned
            up first).
    """
    durable_chain = _durable_directory_chain(target.parent) if durable else []
    target.parent.mkdir(parents=True, exist_ok=True)
    temp: Path = temp_sibling_path(target)
    try:
        frame.write_parquet(temp, compression=compression)
        if durable:
            _fsync_path(temp)
        temp.replace(target)
        for directory in durable_chain:
            _fsync_path(directory)
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
