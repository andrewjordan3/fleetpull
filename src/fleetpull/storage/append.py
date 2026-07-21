# src/fleetpull/storage/append.py
"""The append-log writer: the feed cell's accumulate-only write path.

``FeedAppendWriter`` is the ``(APPEND_LOG, FeedMode)`` cell (DESIGN §3/§4).
Unlike every other writer, each ``write`` is durable the moment it returns:
the piece's rows are split by event date and each date's rows land as the
next-numbered ``part-NNNNN.parquet`` in that date's partition, through the
atomic temp-then-rename write. That per-write durability is what the feed
drive's per-page crash order stands on — a page's parquet must be on disk
before its token commits (§14's I1), so the feed cell cannot defer its bytes
to ``finalize`` the way the staged and single-file cells do; ``finalize``
only reports.

The cell is append-only in the strictest sense (§14's I3): it writes new
part files and never reads, rewrites, or deletes an existing one. Part
numbering is max-existing + 1, scanned per write — sound because parquet
merge per endpoint is single-writer (DESIGN §5) — and the chosen target is
still existence-checked before the rename, so a violated single-writer
assumption clobbers nothing and fails loudly instead.

Deliberately NO write-time exact dedup, unlike every other cell: the dataset
contract is stored-as-emitted (DESIGN §4) — re-emitted versions and a crash
window's refetched page land as new rows, and the consumer reconciles
calculated feeds by ``(id, max version)`` and active feeds by ``id``.
Deduping a crash-window duplicate would require reading and rewriting
already-landed part files, which is exactly what I3 forbids; within-page
duplicates are the provider's emission, kept verbatim.
"""

import re
from pathlib import Path
from typing import Final

import polars as pl

from fleetpull.exceptions import ProviderResponseError
from fleetpull.storage.atomic import atomic_write_parquet
from fleetpull.storage.files import append_part_file, partition_dir
from fleetpull.storage.result import WriteResult
from fleetpull.storage.splitting import split_by_date

__all__: list[str] = ['FeedAppendWriter']

# The strict shape of an append-log part file name. The scan parses numbers
# out of exactly this shape; anything else matching 'part-*.parquet' in an
# append-log partition is a foreign file and fails loudly (the corruption
# stance) rather than silently skewing the numbering.
_APPEND_PART_PATTERN: Final[re.Pattern[str]] = re.compile(r'part-(\d+)\.parquet\Z')


def _next_part_number(partition_directory: Path) -> int:
    """The next append part number for one partition: max existing + 1.

    Scans only the given partition directory (never the endpoint tree), so
    the cost is O(parts in this partition). A missing directory — the
    partition's first-ever part — starts at 1.

    Args:
        partition_directory: The ``date=YYYY-MM-DD`` directory to scan.

    Returns:
        The next part number (>= 1).

    Raises:
        ValueError: A ``part-*.parquet`` file in the partition does not match
            the strict ``part-NNNNN.parquet`` shape — a foreign file in an
            append-log partition is a dataset-layout violation, surfaced
            loudly rather than silently skewing the numbering.

    Side Effects:
        None -- reads directory listings only.
    """
    if not partition_directory.is_dir():
        return 1
    highest = 0
    for part_path in partition_directory.glob('part-*.parquet'):
        match = _APPEND_PART_PATTERN.fullmatch(part_path.name)
        if match is None:
            raise ValueError(
                f'foreign part file in append-log partition: {part_path} '
                f'does not match part-NNNNN.parquet'
            )
        highest = max(highest, int(match.group(1)))
    return highest + 1


class FeedAppendWriter:
    """``feed`` + ``append_log``: append numbered part files, touch nothing else.

    Each ``write`` lands its piece durably (one new part file per event date
    present), so the feed drive can commit that piece's token immediately
    after; ``finalize`` reports the accumulated counts and writes nothing.
    ``duplicates_dropped`` is always ``0`` — the cell performs no write-time
    dedup by design (stored-as-emitted; the module docstring carries the
    rationale).
    """

    def __init__(self, target_dir: Path, event_time_column: str) -> None:
        """Bind the writer to an endpoint directory and event-time column.

        Args:
            target_dir: The endpoint directory holding the ``date=`` partitions.
            event_time_column: The UTC datetime column whose date routes each
                row into its partition.

        Side Effects:
            None -- nothing is swept or cleared; prior parts are never touched.
        """
        self._target_dir = target_dir
        self._event_time_column = event_time_column
        self._rows_written = 0
        self._files_written = 0

    def write(self, frame: pl.DataFrame) -> None:
        """Append one piece durably: a new numbered part per event date present.

        Splits ``frame`` by the UTC date of the event-time column and writes
        each date's rows as that partition's next ``part-NNNNN.parquet``,
        atomically and DURABLY (the durable-rename recipe: the token commit
        is fsynced, so the page it covers must be too -- a power loss never
        persists a token past unwritten data). An empty frame writes nothing (an at-head feed page). The
        write is durable when this returns — the caller may commit the
        piece's feed token immediately after (§14's per-page crash order).

        Args:
            frame: One fetched piece (one feed page's validated rows).

        Raises:
            ProviderResponseError: A row's event-time value is null — a
                feed record without its partition key is a provider
                contract violation surfaced loudly BEFORE any part lands
                (never a mid-write stall behind an untyped error).
            ValueError: A foreign ``part-*.parquet`` file skews a partition's
                numbering (from the scan).
            RuntimeError: The scanned next part number already exists on disk
                — the single-writer assumption is violated; refusing keeps the
                append-only invariant (I3) intact instead of clobbering a
                landed part.

        Side Effects:
            Writes one new part file per event date present, creating
            partition directories as needed. Never modifies an existing file.
        """
        null_dated = frame[self._event_time_column].null_count() if frame.height else 0
        if null_dated:
            raise ProviderResponseError(
                detail=(
                    f'{null_dated} feed record(s) carry a null '
                    f'{self._event_time_column!r} -- the append-log partition '
                    f'key; refusing the page whole before any part lands'
                )
            )
        for partition_date, sub_frame in split_by_date(frame, self._event_time_column):
            part_number = _next_part_number(
                partition_dir(self._target_dir, partition_date)
            )
            target = append_part_file(self._target_dir, partition_date, part_number)
            if target.exists():
                raise RuntimeError(
                    f'append-log part collision: {target} already exists -- '
                    f'two writers on one endpoint violate the single-writer '
                    f'invariant (DESIGN section 5)'
                )
            atomic_write_parquet(sub_frame, target, durable=True)
            self._rows_written += sub_frame.height
            self._files_written += 1

    def finalize(self) -> WriteResult:
        """Report the accumulated append counts; write nothing.

        Every piece already landed durably in ``write`` (the per-page crash
        order requires it), so there is nothing left to persist.

        Returns:
            The write report — rows and part files appended this run,
            ``duplicates_dropped`` always ``0`` (no write-time dedup, by
            design), ``deleted_partitions`` always empty (nothing is ever
            deleted).

        Side Effects:
            None.
        """
        return WriteResult(
            rows_written=self._rows_written,
            duplicates_dropped=0,
            files_written=self._files_written,
        )
