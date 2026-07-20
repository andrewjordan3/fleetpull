# src/fleetpull/roster/__init__.py
"""The roster layer: the fan-out roster's identity, declaration, and catalog.

A leaf both the endpoints layer (the ``RosterFanOut`` shape on a consuming endpoint) and
the state layer (the ``RosterStore``) key by -- ``state`` cannot import ``endpoints``,
so the shared roster identity sits below both. ``RosterKey`` is the opaque handle a
consumer carries; ``RosterDefinition`` is the registry's record of where a key's
members come from and the refresh policy; ``RosterRegistry`` resolves a key to its
definition. Imports only ``vocabulary`` and ``exceptions``, nothing higher.
"""

from fleetpull.roster.definition import RosterDefinition
from fleetpull.roster.key import RosterKey
from fleetpull.roster.registry import RosterRegistry

__all__: list[str] = ['RosterDefinition', 'RosterKey', 'RosterRegistry']
