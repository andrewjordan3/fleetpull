# src/fleetpull/storage/__init__.py
"""The storage layer: write a records DataFrame to parquet.

``select_writer`` returns the ``DatasetWriter`` for an endpoint's storage-kind /
sync-mode cell; the orchestrator drives it (``write`` per piece, then
``finalize``). Stateless: parquet only, no SQLite and no watermark commit (the
orchestrator sequences those). ``WriteResult`` is the write report; ``in_window``
is the half-open ``[start, end)`` window-membership predicate the orchestrator
filters watermark batches with. ``MetadataSnapshot`` with its render/write pair
is the per-endpoint ``metadata.json`` projection the orchestrator writes after a
successful run (DESIGN §3)."""

from fleetpull.storage.frames import in_window
from fleetpull.storage.metadata import (
    MetadataSnapshot,
    render_metadata_json,
    write_metadata_json,
)
from fleetpull.storage.result import WriteResult
from fleetpull.storage.writers import DatasetWriter, select_writer

__all__: list[str] = [
    'DatasetWriter',
    'MetadataSnapshot',
    'WriteResult',
    'in_window',
    'render_metadata_json',
    'select_writer',
    'write_metadata_json',
]
