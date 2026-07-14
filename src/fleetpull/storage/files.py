# src/fleetpull/storage/files.py
"""Storage file-path construction: the parquet-format-specific paths under an
endpoint directory.

Pure path arithmetic, no filesystem access. The single-layout data file and the
temp sibling used for atomic writes live here -- storage-specific, unlike the
shared endpoint-directory construction in ``paths``. The temp sibling sits in the
target's own directory so the rename that follows is same-filesystem and
therefore atomic.
"""

from datetime import date
from pathlib import Path
from uuid import uuid4

from fleetpull.paths import date_partition_segment

__all__: list[str] = [
    'data_file',
    'endpoint_staging_dir',
    'partition_dir',
    'partition_part_file',
    'partition_staging_dir',
    'partition_staging_shard',
    'temp_sibling_path',
]

# The single-layout data file name (DESIGN §3).
_SINGLE_FILE_NAME: str = 'data.parquet'

# The date-partitioned layout's per-partition part file name (DESIGN §3).
_PART_FILE_NAME: str = 'part.parquet'

# The endpoint-level staging root and shard suffix (DESIGN §3).
_STAGING_DIR_NAME: str = '.staging'


def data_file(endpoint_dir: Path) -> Path:
    """The ``single``-layout data file under an endpoint directory.

    Args:
        endpoint_dir: The endpoint directory (from ``endpoint_directory``).

    Returns:
        ``{endpoint_dir}/data.parquet``.
    """
    return endpoint_dir / _SINGLE_FILE_NAME


def partition_dir(endpoint_dir: Path, partition_date: date) -> Path:
    """The date-partition directory for one date under an endpoint directory.

    The single place the hive ``date=YYYY-MM-DD`` directory path is built;
    ``partition_part_file`` and the prune step both go through it, so the
    structural fact lives once.

    Args:
        endpoint_dir: The endpoint directory (from ``endpoint_directory``).
        partition_date: The partition's calendar date.

    Returns:
        ``{endpoint_dir}/date=YYYY-MM-DD``.

    Side Effects:
        None -- pure path arithmetic; no filesystem access.
    """
    return endpoint_dir / date_partition_segment(partition_date)


def partition_part_file(endpoint_dir: Path, partition_date: date) -> Path:
    """The date-partitioned part file for one date under an endpoint directory.

    Args:
        endpoint_dir: The endpoint directory (from ``endpoint_directory``).
        partition_date: The partition's calendar date.

    Returns:
        ``{endpoint_dir}/date=YYYY-MM-DD/part.parquet``.

    Side Effects:
        None -- pure path arithmetic; no filesystem access.
    """
    return partition_dir(endpoint_dir, partition_date) / _PART_FILE_NAME


def endpoint_staging_dir(endpoint_dir: Path) -> Path:
    """The endpoint-level temporary staging root."""
    return endpoint_dir / _STAGING_DIR_NAME


def partition_staging_dir(endpoint_dir: Path, partition_date: date) -> Path:
    """The staging directory for one date under the endpoint staging root."""
    return endpoint_staging_dir(endpoint_dir) / date_partition_segment(partition_date)


def partition_staging_shard(endpoint_dir: Path, partition_date: date) -> Path:
    """A unique ``.shard`` path under one date's staging directory."""
    return partition_staging_dir(endpoint_dir, partition_date) / (
        f'shard-{uuid4().hex}.shard'
    )


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
