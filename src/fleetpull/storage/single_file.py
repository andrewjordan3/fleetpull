# src/fleetpull/storage/single_file.py
"""The single-file writer family: one ``data.parquet`` rewritten each run.

``SingleFileWriter`` is the family's shared substance -- accumulate the run's
frames, then dedup the subclass's finalized frame and atomically rewrite the
single file -- and ``SnapshotWriter`` is the shipped ``(SINGLE, SnapshotMode)``
cell (full replacement, the prior file never read). The unbuilt single-file
date-window cell will join this family as the combine subclass (DESIGN §3).
``select_writer`` (``storage/writers.py``) is the routing face that constructs
these.
"""

from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from fleetpull.storage.atomic import atomic_write_parquet
from fleetpull.storage.files import data_file
from fleetpull.storage.frames import dedup_counting
from fleetpull.storage.result import WriteResult

__all__: list[str] = ['SingleFileWriter', 'SnapshotWriter']


class SingleFileWriter(ABC):
    """Writers whose dataset is one ``data.parquet`` rewritten each run.

    Shared substance: accumulate the run's frames, and on ``finalize`` dedup the
    subclass's finalized frame and atomically rewrite the single file. Subclasses
    supply ``_finalize_frame`` -- the per-cell write semantic, which decides
    whether the prior file is read at all. A snapshot does not read it (it
    replaces); the unbuilt watermark single-file cell will (it combines with
    prior rows), reading through ``read_parquet_if_exists`` itself. The
    base never reads -- the read is opt-in by the subclass.
    """

    def __init__(self, target_dir: Path, *, drop_duplicates: bool = True) -> None:
        """Bind the writer to an endpoint directory.

        Args:
            target_dir: The endpoint directory holding ``data.parquet``.
            drop_duplicates: Whether ``finalize`` drops exact-duplicate rows
                (``storage.drop_exact_duplicates``, default on).

        Side Effects:
            None.
        """
        self._target_dir = target_dir
        self._drop_duplicates = drop_duplicates
        self._frames: list[pl.DataFrame] = []

    def write(self, frame: pl.DataFrame) -> None:
        """Accumulate one frame for this run.

        Args:
            frame: A validated, flattened frame for this endpoint.

        Side Effects:
            Holds a reference to ``frame`` until ``finalize``.
        """
        self._frames.append(frame)

    def finalize(self) -> WriteResult:
        """Dedup (unless off) the subclass's finalized frame and write it.

        Returns:
            The write report (``files_written`` is always ``1``).

        Side Effects:
            Atomically rewrites ``data.parquet`` under the endpoint directory.
        """
        written, duplicates_dropped = dedup_counting(
            self._finalize_frame(), enabled=self._drop_duplicates
        )
        atomic_write_parquet(written, data_file(self._target_dir))
        return WriteResult(
            rows_written=written.height,
            duplicates_dropped=duplicates_dropped,
            files_written=1,
        )

    @abstractmethod
    def _finalize_frame(self) -> pl.DataFrame:
        """Produce the frame to dedup and write for this run.

        The per-cell write semantic. A replace cell returns its accumulated frame;
        a combine cell reads the prior file (via ``read_parquet_if_exists``) and
        merges it with the accumulated frame. Reading the prior file is the
        subclass's choice -- the base never reads.

        Returns:
            The frame to dedup and write.

        Side Effects:
            A combine subclass reads ``data.parquet``; a replace subclass does not.
        """
        ...

    def _accumulated(self) -> pl.DataFrame:
        """This run's accumulated ``write`` frames as one frame.

        Returns:
            The concatenation of every frame passed to ``write``. ``write`` must
            have been called at least once.

        Side Effects:
            None.
        """
        return pl.concat(self._frames)


class SnapshotWriter(SingleFileWriter):
    """``snapshot`` + ``single``: full replacement of the current-state dataset.

    A snapshot re-fetches its whole current-state dataset every run, so the result
    is just this run's accumulated frame and the prior file is never read -- it is
    overwritten by the atomic rename (DESIGN §3).
    """

    def _finalize_frame(self) -> pl.DataFrame:
        """Return this run's accumulated frame; the prior file is not read.

        Returns:
            The accumulated frame, replacing the prior dataset wholesale.

        Side Effects:
            None -- the prior ``data.parquet`` is overwritten without being read.
        """
        return self._accumulated()
