# src/fleetpull/vocabulary/quota_scope.py
"""
The closed set of quota scopes an endpoint definition declares.

Shared, dependency-free package vocabulary, sibling to ``Provider`` and
``ResponseCategory`` in the leaf. A ``QuotaScope`` is which token bucket an
endpoint spends from — the type of ``endpoint.quota_scope`` — not which
provider it belongs to. The two are separate vocabularies on purpose — folding
both into one type would be exactly the conflation this avoids — and they
diverged when GeoTab metered its endpoints apart from the rest (the
method-class scopes below; the §13 Samsara ``vehicle_locations`` case would
add another ``QuotaScope`` member while the ``Provider`` stays ``SAMSARA``).

GeoTab meters per method class, not per provider (§8, captured 2026-07-09),
so its scopes are method-class members: ``GEOTAB_GET`` is the Get-class data
scope endpoint definitions declare, ``GEOTAB_FEED`` is the GetFeed-class
scope the feed endpoints declare (its own ~60/min budget, proven by the
2026-07-21 header-decrement probe — distinct from the ~650/min Get class),
and ``GEOTAB_AUTHENTICATE`` is the dedicated Authenticate scope —
auth-internal, never an endpoint declaration; the composition root passes it
to the authenticator factory by name.
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

    GEOTAB_AUTHENTICATE = 'geotab_authenticate'
    GEOTAB_FEED = 'geotab_feed'
    GEOTAB_GET = 'geotab_get'
    MOTIVE = 'motive'
    SAMSARA = 'samsara'
