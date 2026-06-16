# src/fleetpull/state/__init__.py
"""SQLite operational state store: connection lifecycle, schema migrations, and integrity verification."""

from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head

__all__: list[str] = ['StateDatabase', 'migrate_to_head']
