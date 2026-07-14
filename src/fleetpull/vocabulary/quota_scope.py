# src/fleetpull/vocabulary/quota_scope.py
"""
The closed set of quota scopes an endpoint definition declares.

Shared, dependency-free package vocabulary, sibling to ``Provider`` and
``ResponseCategory`` in the leaf. A ``QuotaScope`` is which token bucket an
endpoint spends from — the type of ``endpoint.quota_scope`` — not which
provider it belongs to. The members coincide with ``Provider``'s today, but the
two are separate vocabularies on purpose: they diverge the moment a provider
meters one endpoint apart from the rest (the §13 Samsara ``vehicle_locations``
case adds a new ``QuotaScope`` member while the ``Provider`` stays ``SAMSARA``),
and folding both into one type would be exactly the conflation this avoids.

GeoTab meters per method class, not per provider (§8, captured 2026-07-09),
so its scopes are method-class members: ``GEOTAB_GET`` is the Get-class data
scope endpoint definitions declare, and ``GEOTAB_AUTHENTICATE`` is the
dedicated Authenticate scope — auth-internal, never an endpoint declaration;
the composition root passes it to the authenticator factory by name. The
GetFeed-class scope joins with the feed vertical.
"""

from enum import StrEnum

__all__: list[str] = ['QuotaScope']


class QuotaScope(StrEnum):
    """
    Closed set of quota scopes (which token bucket an endpoint spends from).

    String values are the lowercase scope keys the ``RateLimiterRegistry`` is
    already keyed on (matching the provider-derived scope strings in use today),
    so a ``QuotaScope`` member passes to ``QuotaScopeLimiter(quota_scope: str)``
    transparently — it is a ``str``.

    Membership vs. limits: scope *membership* is this closed architectural set
    (code); scope *limits* are config (§7). Adding a scope is therefore a code
    change (a new member) plus a config change (its limits) — not config-only.
    """

    GEOTAB = 'geotab'
    GEOTAB_AUTHENTICATE = 'geotab_authenticate'
    GEOTAB_GET = 'geotab_get'
    GEOTAB_GET_FEED = 'geotab_get_feed'
    MOTIVE = 'motive'
    SAMSARA = 'samsara'
