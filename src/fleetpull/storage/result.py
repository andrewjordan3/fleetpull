# src/fleetpull/storage/result.py
"""The storage write report.

``PersistResult`` is what ``persist`` returns and what a layout's
``write_dataset`` produces -- the run ledger reads it (via the orchestrator, not
storage). The fleetpull analogue of fleet-telemetry-hub's merge stats.
"""

from dataclasses import dataclass

__all__: list[str] = ['PersistResult']


@dataclass(frozen=True, slots=True)
class PersistResult:
    """What one endpoint's persist wrote this run.

    Attributes:
        rows_written: Rows written to disk this run. For ``single`` (a full
            rewrite) this is the whole dataset; for ``date_partitioned`` it is
            the rows across the touched partitions, not the dataset total.
        duplicates_dropped: Exact-duplicate rows removed at write time.
        files_written: Parquet files written -- ``1`` for ``single``, the count
            of touched partitions for ``date_partitioned``. (Named for files,
            not partitions, so it reads correctly for the single-file case.)
    """

    rows_written: int
    duplicates_dropped: int
    files_written: int
