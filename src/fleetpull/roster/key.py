# src/fleetpull/roster/key.py
"""The roster identity: ``RosterKey``, the opaque handle a consumer references.

A ``RosterKey`` names one roster -- ``(provider, name)`` -- and is the only roster
fact a fan-out consumer carries (its ``RosterFanOut.roster``): the consumer knows
*that* a roster of its keys exists, never where those keys come from. The mapping
from a key to its source endpoint and column lives in the ``RosterRegistry`` (the
``RosterDefinition``), and the persisted members live in the ``RosterStore``, both
keyed by this. Homed in this leaf so both the endpoints layer (the binding) and the
state layer (the store) can key by it -- ``state`` cannot import ``endpoints``, so the
shared identity sits below both.

``name`` is the logical roster, deliberately not the source column: ``'vehicle_ids'``
the roster, not ``'vehicle_id'`` the feeder field -- the gap is the decoupling.
"""

from dataclasses import dataclass

from fleetpull.vocabulary import Provider

__all__: list[str] = ['RosterKey']


@dataclass(frozen=True, slots=True)
class RosterKey:
    """The identity of one roster: a provider and a logical roster name.

    The opaque handle a fan-out consumer references and the key the registry and
    store map from. Two rosters are the same iff their provider and name match;
    nothing else (the source endpoint, the column, the policy) is part of the
    identity -- those live in the ``RosterDefinition`` the registry holds.

    Attributes:
        provider: The provider whose roster this is.
        name: The logical roster name (e.g. ``'vehicle_ids'``), distinct from the
            feeder column its members are listed from.
    """

    provider: Provider
    name: str
