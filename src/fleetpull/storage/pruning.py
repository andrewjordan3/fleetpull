# src/fleetpull/storage/pruning.py
"""Date-partition pruning: drop the partitions a refresh window covers but did
not write.

The delete half of the date-partitioned write path's two-step (write the fetched
partitions, then delete the covered-but-unwritten ones). A watermark refresh
authoritatively replaces its window ``[start, end)``: every ``date=`` partition
the window covers must, after the run, hold exactly this run's data for that date.
A covered date the fetch did not write is therefore stale -- the provider deleted
or edited every record it once held -- so its partition directory must go, the
directory-grain analogue of the row-level delete-by-window (DESIGN §3/§4).

Four stateless single-concern functions compose the prune, none aware of *when* in
the run they run. The driver sequences the delete after the full per-vehicle
fan-out, once ``written_dates`` is complete -- a partition holds the whole fleet's
rows for its date, so the written set is not final until the last vehicle is
processed -- but these functions take the finished set as a given:

- ``window_dates`` -- the covered calendar dates of a window (pure date math).
- ``existing_partition_dates`` -- which of a candidate date set exist on disk (the
  filesystem probe, candidate-driven, never a directory scan).
- ``delete_partition`` -- remove one partition directory (the single delete).
- ``prune_window_partitions`` -- the composer: covered, on disk, minus written,
  deleted.

The set arithmetic is ``window_dates(window) ∩ {on disk} - {written}``. The
``∩ window_dates`` term is the safety leash: it bounds the delete to the refresh
window, so history *outside* the window is never touched. Deleting on
``existed - written`` alone would erase every partition outside the window -- a
data-loss bug -- which is why the window intersection lives inside the composer,
enforced once here rather than trusted to every caller. The probe is
candidate-driven (``is_dir`` only the window's own dates), so its cost is
O(window), never O(dataset): listing the endpoint directory is the O(dataset) scan
partitioning exists to avoid.
"""

import shutil
from collections.abc import Collection
from datetime import date, timedelta
from pathlib import Path

from fleetpull.incremental import DateWindow
from fleetpull.storage.files import partition_dir

__all__: list[str] = [
    'delete_partition',
    'existing_partition_dates',
    'prune_window_partitions',
    'window_dates',
]


def window_dates(window: DateWindow) -> list[date]:
    """The calendar dates a half-open ``[start, end)`` window covers.

    A partition ``date=d`` is covered iff some instant of that day lies in
    ``[start, end)`` -- the dates ``start.date()`` through the window's
    ``last_covered_date`` inclusive (the half-open edge and its one-microsecond
    derivation are stated once, on ``DateWindow``).

    Args:
        window: The half-open ``[start, end)`` resume window.

    Returns:
        The covered dates in ascending order, ``start.date()`` first. Always at
        least one date, since ``window`` guarantees ``start < end``.

    Side Effects:
        None -- pure function.
    """
    first = window.start.date()
    last = window.last_covered_date
    span_days = (last - first).days + 1
    return [first + timedelta(days=offset) for offset in range(span_days)]


def existing_partition_dates(
    endpoint_dir: Path, candidate_dates: Collection[date]
) -> set[date]:
    """Which of ``candidate_dates`` have a partition directory on disk.

    Candidate-driven: probes only the ``date=`` directories for the dates handed
    in (``is_dir`` per candidate), never lists ``endpoint_dir``. The cost is
    therefore O(candidates), not O(dataset) -- the point of anchoring the prune to
    the refresh window rather than scanning the tree (DESIGN §3).

    Args:
        endpoint_dir: The endpoint directory holding the ``date=`` partitions.
        candidate_dates: The dates to probe (the window's covered dates).

    Returns:
        The subset of ``candidate_dates`` whose partition directory exists. Empty
        when none exist (e.g. a first run, or an endpoint directory not yet
        created).

    Side Effects:
        None -- reads directory existence only; creates and writes nothing.
    """
    return {
        candidate_date
        for candidate_date in candidate_dates
        if partition_dir(endpoint_dir, candidate_date).is_dir()
    }


def delete_partition(endpoint_dir: Path, partition_date: date) -> None:
    """Remove one date-partition directory and everything under it.

    Deletes the whole ``date=YYYY-MM-DD`` directory (the part file and any temp
    siblings), not just the part file, so no empty partition directory is left
    behind -- a present-but-empty ``date=`` directory would read as a date with
    data when scanned, the invariant ``split_by_date`` upholds by never creating
    empty partitions (DESIGN §3).

    Strict: the directory must exist. A missing directory is a caller bug -- the
    composer deletes only dates ``existing_partition_dates`` just confirmed present
    -- and a silent no-op on a *delete* would hide that logic error, so the
    underlying ``rmtree`` is allowed to raise.

    Args:
        endpoint_dir: The endpoint directory holding the ``date=`` partitions.
        partition_date: The calendar date whose partition directory to remove.

    Side Effects:
        Recursively deletes the partition directory from the filesystem.

    Raises:
        FileNotFoundError: The partition directory does not exist.
        OSError: The directory could not be removed (permissions, etc.).
    """
    shutil.rmtree(partition_dir(endpoint_dir, partition_date))


def prune_window_partitions(
    endpoint_dir: Path, window: DateWindow, written_dates: Collection[date]
) -> list[date]:
    """Delete the partitions ``window`` covers but this run did not write.

    Computes the stale set itself -- ``window_dates(window)`` intersected with the
    partitions present on disk, minus ``written_dates`` -- and deletes each. The
    intersection with the window is the safety leash that keeps the delete inside
    the refresh window (DESIGN §3); it lives here, not in the caller, so the bound
    is enforced in one tested place. Usually returns empty: in steady state every
    covered date gets data, so nothing is stale. That rarity is exactly why the
    path must be airtight -- a directory delete that almost never fires is where a
    latent bug hides.

    Args:
        endpoint_dir: The endpoint directory holding the ``date=`` partitions.
        window: The half-open ``[start, end)`` window this run refreshed.
        written_dates: The dates this run wrote a partition for (the keys of the
            per-date split). Order and duplicates do not matter.

    Returns:
        The dates whose partitions were deleted, ascending. Empty when no covered
        partition was stale.

    Side Effects:
        Deletes stale partition directories from the filesystem.

    Raises:
        OSError: A stale partition directory could not be removed.
    """
    covered = window_dates(window)
    present = existing_partition_dates(endpoint_dir, covered)
    stale = sorted(present - set(written_dates))
    for stale_date in stale:
        delete_partition(endpoint_dir, stale_date)
    return stale
