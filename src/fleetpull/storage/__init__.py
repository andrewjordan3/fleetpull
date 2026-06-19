# src/fleetpull/storage/__init__.py
"""The storage layer: persist a records DataFrame to parquet.

``persist`` is the single entry point -- it merges this run's frame into the
endpoint's dataset per its declared storage-kind and sync-mode axes and writes it
atomically. Stateless: parquet only, no SQLite and no watermark commit (the
orchestrator sequences those). ``PersistResult`` is the write report."""

from fleetpull.storage.persist import persist
from fleetpull.storage.result import PersistResult

__all__: list[str] = ['PersistResult', 'persist']
