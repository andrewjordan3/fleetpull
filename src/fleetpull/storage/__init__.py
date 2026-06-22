# src/fleetpull/storage/__init__.py
"""The storage layer: write a records DataFrame to parquet.

``select_writer`` returns the ``DatasetWriter`` for an endpoint's storage-kind /
sync-mode cell; the orchestrator drives it (``write`` per piece, then
``finalize``). Stateless: parquet only, no SQLite and no watermark commit (the
orchestrator sequences those). ``WriteResult`` is the write report."""

from fleetpull.storage.result import WriteResult
from fleetpull.storage.writers import DatasetWriter, select_writer

__all__: list[str] = ['DatasetWriter', 'WriteResult', 'select_writer']
