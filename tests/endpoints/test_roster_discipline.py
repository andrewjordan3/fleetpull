# tests/endpoints/test_roster_discipline.py
"""Declaration-level roster discipline: every declared roster sources a
snapshot-mode feeder.

``reconcile`` is only correct over a *complete* listing, which only a
snapshot-mode feeder produces -- a watermark-mode feeder's windowed runs would
mass-count absences against every member outside the window. The runtime
paths are guarded (the orchestration entry's tap and the refresh
coordinator's harvest both reject a non-snapshot feeder); this closes the
same gap at build time, before any runtime path is reached: it walks every
provider leaf module for module-level ``RosterDefinition`` declarations and
resolves each ``source_endpoint`` against the discovered endpoint catalog.
"""

import importlib
from datetime import timedelta

from fleetpull.config import MotiveConfig
from fleetpull.endpoints import EndpointRegistry, build_endpoint_registry
from fleetpull.endpoints.registry import _iter_endpoint_leaf_modules
from fleetpull.endpoints.shared import SnapshotMode
from fleetpull.roster import RosterDefinition, RosterKey
from fleetpull.vocabulary import Provider


def _declared_rosters() -> list[RosterDefinition]:
    """Every module-level RosterDefinition in the provider leaf modules."""
    rosters: list[RosterDefinition] = []
    for module_name in _iter_endpoint_leaf_modules():
        module = importlib.import_module(module_name)
        rosters.extend(
            value
            for value in vars(module).values()
            if isinstance(value, RosterDefinition)
        )
    return rosters


def _snapshot_violations(
    rosters: list[RosterDefinition], registry: EndpointRegistry
) -> list[str]:
    """One diagnostic per roster whose feeder is not snapshot-mode."""
    violations: list[str] = []
    for roster in rosters:
        feeder = registry.get(roster.key.provider, roster.source_endpoint)
        if not isinstance(feeder.sync_mode, SnapshotMode):
            violations.append(
                f'roster {roster.key.provider.value}/{roster.key.name}: '
                f'source_endpoint {roster.source_endpoint!r} is '
                f'{type(feeder.sync_mode).__name__}, not SnapshotMode'
            )
    return violations


def test_every_declared_roster_sources_a_snapshot_feeder() -> None:
    rosters = _declared_rosters()
    assert rosters, 'discovery found no roster declarations'
    registry = build_endpoint_registry([MotiveConfig()])
    assert _snapshot_violations(rosters, registry) == []


def test_the_discipline_flags_a_watermark_sourced_roster() -> None:
    # The permanent negative shape (plant-and-fire kept as a test): a roster
    # declaring a watermark-mode feeder must be flagged.
    registry = build_endpoint_registry([MotiveConfig()])
    watermark_sourced = RosterDefinition(
        key=RosterKey(Provider.MOTIVE, 'bad_roster'),
        source_endpoint='vehicle_locations',
        source_column='vehicle_id',
        max_age=timedelta(days=1),
        eviction_threshold=None,
    )
    violations = _snapshot_violations([watermark_sourced], registry)
    assert len(violations) == 1
    assert 'vehicle_locations' in violations[0]
    assert 'WatermarkMode' in violations[0]
