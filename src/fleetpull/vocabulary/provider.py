# src/fleetpull/vocabulary/provider.py
"""
The closed set of telematics providers fleetpull extracts from.

Shared, dependency-free package vocabulary, sibling to ``ResponseCategory``:
an endpoint declares the provider it belongs to, and the composition root
keys per-provider auth and response classification on it. Homed in the leaf
that imports nothing internal so every layer can name a provider without
forming a cycle.
"""

from enum import StrEnum

__all__: list[str] = ['Provider']


class Provider(StrEnum):
    """
    Closed set of telematics providers.

    String values are the lowercase provider keys used in logging and
    quota-scope naming. A provider earns a member only when fleetpull
    implements its auth and response classification — which all three
    already have.
    """

    GEOTAB = 'geotab'
    MOTIVE = 'motive'
    SAMSARA = 'samsara'
