# src/fleetpull/state/__init__.py
"""SQLite operational state store: the database lifecycle (connections, schema
migrations, integrity verification) and the stores over it -- incremental
cursors, the run ledger, feeder rosters, and the backfill work-unit queue."""

from fleetpull.state.cursors import CursorKind, CursorStore
from fleetpull.state.database import StateDatabase
from fleetpull.state.migrations import migrate_to_head
from fleetpull.state.reconcile import RosterDelta, is_roster_stale, reconcile
from fleetpull.state.rosters import RosterStore
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
    'RosterDelta',
    'RosterStore',
    'RunLedger',
    'RunStatus',
    'StateDatabase',
    'WorkUnitProgress',
    'WorkUnitSpec',
    'WorkUnitStatus',
    'WorkUnitStore',
    'is_roster_stale',
    'migrate_to_head',
    'reconcile',
]
