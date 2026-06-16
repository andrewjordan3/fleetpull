# src/fleetpull/state/__init__.py
"""SQLite operational state store: connection lifecycle, schema migrations, and integrity verification."""

from fleetpull.state.cursors import CursorKind, CursorStore
from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head

__all__: list[str] = ['CursorKind', 'CursorStore', 'StateDatabase', 'migrate_to_head']
