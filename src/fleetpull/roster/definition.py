# src/fleetpull/roster/definition.py
"""The roster declaration: ``RosterDefinition`` -- a key, its source, and its policy.

The registry's record for one ``RosterKey``: the feeder endpoint and column its
members are listed from, and the staleness and eviction policy a refresh applies. The
consuming endpoint never sees this -- it carries only the ``RosterKey``; the registry
resolves the key to this when a refresh runs. Homed beside the key it references.
"""

from dataclasses import dataclass
from datetime import timedelta

from fleetpull.roster.key import RosterKey

__all__: list[str] = ['RosterDefinition']


@dataclass(frozen=True, slots=True)
class RosterDefinition:
    """One roster's source and refresh policy, keyed by its ``RosterKey``.

    Maps a ``RosterKey`` to the feeder endpoint and column its members are listed
    from, plus the staleness bound and eviction threshold a refresh applies. The
    source is named by endpoint and column (strings), not an ``EndpointDefinition`` --
    resolving the name to a runnable binding is the coordinator's job, against the
    endpoint catalog. ``max_age`` and ``eviction_threshold`` are the policy the pure
    ``is_roster_stale`` and ``reconcile`` already take.

    Attributes:
        key: The roster this defines.
        source_endpoint: The feeder endpoint whose listing supplies the members
            (e.g. ``'vehicles'``).
        source_column: The feeder frame column whose distinct values are the members
            (e.g. ``'vehicle_id'``) -- the model field name after the records-layer
            flatten, not the wire key.
        max_age: The staleness bound -- a refresh is due once the feeder's last
            success is older than this.
        eviction_threshold: Consecutive-miss count past which a member is evicted, or
            ``None`` for append-only (never evict).
    """

    key: RosterKey
    source_endpoint: str
    source_column: str
    max_age: timedelta
    eviction_threshold: int | None
