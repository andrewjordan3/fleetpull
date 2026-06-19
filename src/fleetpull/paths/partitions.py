# src/fleetpull/paths/partitions.py
"""Hive date-partition directory-name construction: the ``date=YYYY-MM-DD``
segment a date-partitioned dataset uses, and its inverse.

The shared, filesystem-neutral translation between a partition ``date`` and the
hive directory-name segment that encodes it. The forward direction builds the
segment for the write path (the storage layer joins it under an endpoint
directory); the inverse recovers the date from a segment for the merge path,
which scans an endpoint directory and must turn the partition names it finds back
into dates to decide which partitions a fetch window overlaps.

A pure leaf -- stdlib ``date`` only, imports nothing internal, never touches the
filesystem (directory creation and scanning are the writing/merge layers'
concerns). It lives in ``paths`` rather than ``storage`` because the hive
``date=`` convention is a shared structural fact about the dataset layout -- the
storage read/merge path and the future metadata layer both read it, and BigQuery
external tables read it natively -- not parquet-specific arithmetic like the
``part.parquet`` filename, which is storage's.

The inverse is strict: a segment that is not a well-formed ``date=YYYY-MM-DD`` is
a corrupt or foreign directory entry, and the parser raises ``ValueError`` rather
than guessing. Deciding what is and is not a partition entry (skipping a stray
file, a ``.tmp``, a ``metadata.json``) is the scanner's concern one layer up; the
parser's only job is to turn a *claimed* partition name into a date or fail loud.
The raise is stdlib ``ValueError`` -- a corrupt entry is malformed input the
consuming boundary translates if it wants a typed failure, and keeping the raise
stdlib is what lets this module import nothing internal (the ``codec`` precedent).
"""

from datetime import date

__all__: list[str] = ['date_partition_segment', 'parse_date_partition_segment']

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


def parse_date_partition_segment(segment: str) -> date:
    """Recover the calendar date from a hive partition directory-name segment.

    The strict inverse of ``date_partition_segment``: a segment that lacks the
    ``date=`` prefix or whose remainder is not an ISO ``YYYY-MM-DD`` date is a
    corrupt or foreign directory entry and raises, rather than being silently
    coerced or skipped (skipping non-partition entries is the scanner's job).

    Args:
        segment: A directory-name segment claimed to be a date partition (e.g.
            ``'date=2026-06-01'``).

    Returns:
        The calendar date the segment encodes.

    Raises:
        ValueError: ``segment`` lacks the ``date=`` prefix, or its remainder is
            not a valid ISO ``YYYY-MM-DD`` date.

    Side Effects:
        None -- pure function.
    """
    if not segment.startswith(_PARTITION_PREFIX):
        raise ValueError(
            f'not a date partition segment: {segment!r} '
            f'(expected a {_PARTITION_PREFIX!r} prefix)'
        )
    date_text = segment.removeprefix(_PARTITION_PREFIX)
    try:
        return date.fromisoformat(date_text)
    except ValueError as parse_error:
        raise ValueError(
            f'malformed date in partition segment {segment!r}'
        ) from parse_error
