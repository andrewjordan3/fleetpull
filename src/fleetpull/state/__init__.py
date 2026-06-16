# src/fleetpull/state/__init__.py
"""SQLite operational state store: connection lifecycle and integrity verification."""

from fleetpull.state.database import StateDatabase

__all__: list[str] = ['StateDatabase']
