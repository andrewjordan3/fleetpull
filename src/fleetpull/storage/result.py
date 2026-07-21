# src/fleetpull/storage/result.py
"""The storage write report.

``WriteResult`` is what a ``DatasetWriter.finalize`` returns -- the run ledger
reads it (via the orchestrator, not storage). The fleetpull analogue of
fleet-telemetry-hub's merge stats.
"""

from dataclasses import dataclass
from datetime import date

__all__: list[str] = ['WriteResult']


@dataclass(frozen=True, slots=True)
class WriteResult:
    """What one endpoint's write produced this run.

    Attributes:
        rows_written: Rows written to disk this run. For ``single`` (a full
            rewrite) this is the whole dataset; for ``date_partitioned`` it is the
            rows across the touched partitions, not the dataset total.
        duplicates_dropped: Exact-duplicate rows removed at write time.
        files_written: Parquet files written -- ``1`` for ``single``, the count of
            touched partitions for ``date_partitioned``.
        deleted_partitions: The date partitions deleted this run -- the
            covered-but-empty dates a date-partitioned watermark refresh prunes.
            Empty for every other cell.
    """

    rows_written: int
    duplicates_dropped: int
    files_written: int
    deleted_partitions: tuple[date, ...] = ()
