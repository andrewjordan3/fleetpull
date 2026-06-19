# src/fleetpull/storage/files.py
"""Storage file-path construction: the parquet-format-specific paths under an
endpoint directory.

Pure path arithmetic, no filesystem access. The single-layout data file and the
temp sibling used for atomic writes live here -- storage-specific, unlike the
shared endpoint-directory construction in ``paths``. The temp sibling sits in the
target's own directory so the rename that follows is same-filesystem and
therefore atomic.
"""

from pathlib import Path
from uuid import uuid4

__all__: list[str] = ['data_file', 'temp_sibling_path']

# The single-layout data file name (DESIGN §3). Date-partitioned part files
# (``part.parquet``) are the partitioned layout's concern, added with it.
_SINGLE_FILE_NAME: str = 'data.parquet'


def data_file(endpoint_dir: Path) -> Path:
    """The ``single``-layout data file under an endpoint directory.

    Args:
        endpoint_dir: The endpoint directory (from ``endpoint_directory``).

    Returns:
        ``{endpoint_dir}/data.parquet``.
    """
    return endpoint_dir / _SINGLE_FILE_NAME


def temp_sibling_path(target: Path) -> Path:
    """A unique temporary path beside ``target`` for an atomic write.

    Placed in ``target``'s own directory so the follow-up rename stays on one
    filesystem (POSIX guarantees same-filesystem rename atomicity). The unique
    suffix avoids colliding with a stale temp from an earlier interrupted write.

    Args:
        target: The final file the temp will be renamed onto.

    Returns:
        A hidden, uniquely-named sibling temp path.
    """
    return target.with_name(f'.{target.name}.{uuid4().hex}.tmp')
