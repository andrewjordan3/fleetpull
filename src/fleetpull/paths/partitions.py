# src/fleetpull/paths/partitions.py
"""Hive date-partition directory-name construction: the ``date=YYYY-MM-DD``
segment a date-partitioned dataset uses.

The shared, filesystem-neutral translation from a partition ``date`` to the
hive directory-name segment that encodes it, built for the write path (the
storage layer joins it under an endpoint directory). The segment grammar is
pinned by a direct test of this function's output; a strict inverse parser
existed here but was deleted with no production caller -- a future
directory-reading layer re-derives it from the pinned grammar if one ever
lands.

A pure leaf -- stdlib ``date`` only, imports nothing internal, never touches the
filesystem (directory creation and scanning are the writing/reading layers'
concerns). It lives in ``paths`` rather than ``storage`` because the hive
``date=`` convention is a shared structural fact about the dataset layout -- the
storage write path builds it and BigQuery external tables read it natively --
not parquet-specific arithmetic like the ``part.parquet`` filename, which is
storage's.
"""

from datetime import date

__all__: list[str] = ['date_partition_segment']

_PARTITION_PREFIX: str = 'date='


def date_partition_segment(partition_date: date) -> str:
    """Build the hive partition directory-name segment for a date.

    Args:
        partition_date: The partition's calendar date.

    Returns:
        The hive segment ``'date=YYYY-MM-DD'`` (e.g. ``'date=2026-06-01'``).

    Side Effects:
        None -- pure function.
    """
    return f'{_PARTITION_PREFIX}{partition_date.isoformat()}'
