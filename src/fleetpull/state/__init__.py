# src/fleetpull/state/__init__.py
"""SQLite operational state store: connection lifecycle, schema migrations, and integrity verification."""

from fleetpull.state.cursors import CursorKind, CursorStore
from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head
from fleetpull.state.run_ledger import RunLedger, RunStatus
from fleetpull.state.work_units import (
    ClaimedWorkUnit,
    WorkUnitProgress,
    WorkUnitSpec,
    WorkUnitStatus,
    WorkUnitStore,
)

__all__: list[str] = [
    'ClaimedWorkUnit',
    'CursorKind',
    'CursorStore',
    'RunLedger',
    'RunStatus',
    'StateDatabase',
    'WorkUnitProgress',
    'WorkUnitSpec',
    'WorkUnitStatus',
    'WorkUnitStore',
    'migrate_to_head',
]
