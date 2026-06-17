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

The dedicated GeoTab Authenticate scope (§8) is deliberately not a member: it is
auth-internal, named at the composition root, and not an endpoint declaration.
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
    MOTIVE = 'motive'
    SAMSARA = 'samsara'
