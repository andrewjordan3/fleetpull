# src/fleetpull/state/__init__.py
"""SQLite operational state store: connection lifecycle, schema migrations, and integrity verification."""

from fleetpull.state.cursors import CursorKind, CursorStore
from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head
from fleetpull.state.run_ledger import RunLedger, RunStatus

__all__: list[str] = [
    'CursorKind',
    'CursorStore',
    'RunLedger',
    'RunStatus',
    'StateDatabase',
    'migrate_to_head',
]
