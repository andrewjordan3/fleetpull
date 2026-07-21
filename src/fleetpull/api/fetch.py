# src/fleetpull/api/fetch.py
"""``fetch``: the snapshot-only, in-memory programmatic convenience verb.

The convenience charter (DESIGN §10): an endpoint identity, one
``auth=`` value, and almost nothing else, returning an eager typed
DataFrame with no state machinery -- no SQLite, no disk, no cursor, no
run ledger, no roster. ``fetch`` deliberately limits options; a caller
who wants windows, incremental resume, partitioned storage, or fan-out
is a sync user, not a fetch user with missing parameters.

Snapshot-only because the in-memory contract is only honest for
snapshots: a snapshot result is bounded by entity count, while a
windowed result grows with window width and fleet activity, unbounded by
anything the caller controls in memory. The exposure gate is the
``SnapshotEndpoint`` parameter type; a runtime guard backs it for the
audiences mypy never covers (notebooks foremost).

No member filtering, by design: ``fetch`` returns the endpoint's full
current listing. Filtering (by vehicle, by group, by status) presumes a
use case, and the scope refusal (§10) forbids exactly that presumption
-- consumers filter the returned frame themselves.

The composition is the state-free fetch trace (established by the
2026-07-06 pre-API audit):
provider configs from defaults → the discovery registry → the definition
by identity key → a ``ClientRuntime`` on internal defaults → the
auth-ingress profile (its ``ProviderProfileContext`` reusing the
runtime's HTTP posture and limiter registry, plus a clock held solely
for GeoTab session aging) → the shape-resolved request driver (the
shared ``resolve_request_driver`` seam with no roster source: the
stateless shapes a snapshot can declare -- single fetch, param sweep
-- resolve (a bisected window is watermark-only by construction and
never reaches this verb), a roster-backed shape (``RosterFanOut`` /
``BatchedRosterFanOut``) is refused loudly, and the
driver honors a declared completeness check, so a guarded snapshot is
verified on this verb exactly as under sync) → ``TransportClient`` →
``validate_records`` → ``models_to_dataframe``.
"""

import logging

import polars as pl

from fleetpull.api.auth_ingress import (
    AuthInput,
    ProviderProfileContext,
    build_provider_profile,
)
from fleetpull.api.identity import SnapshotEndpoint, WindowedEndpoint
from fleetpull.config import (
    GeotabConfig,
    HttpConfig,
    MotiveConfig,
    ProviderConfig,
    RetryConfig,
    SamsaraConfig,
)
from fleetpull.endpoints import build_endpoint_registry
from fleetpull.exceptions import ConfigurationError
from fleetpull.network.client import ClientRuntime, TransportClient
from fleetpull.network.limits import RateLimiterRegistry, rate_limits_from_configs
from fleetpull.orchestrator import FetchPoolRegistry, resolve_request_driver
from fleetpull.records import models_to_dataframe, validate_records
from fleetpull.timing import SystemClock
from fleetpull.vocabulary import JsonObject, Provider

__all__: list[str] = ['fetch']

logger = logging.getLogger(__name__)


def _default_provider_configs() -> dict[Provider, ProviderConfig]:
    """Every provider config the discovery registry needs, at pure defaults.

    One instance per provider package under ``endpoints/`` -- the
    registry walk builds every discovered leaf, so each provider with
    leaves needs its config here even when the requested endpoint
    belongs to another. Extends as provider endpoint packages land.

    Returns:
        The default-constructed provider configs by provider, whose
        values feed ``build_endpoint_registry`` and
        ``rate_limits_from_configs`` and whose keying sizes the
        requested provider's fetch pool.
    """
    return {
        Provider.MOTIVE: MotiveConfig(),
        Provider.GEOTAB: GeotabConfig(),
        Provider.SAMSARA: SamsaraConfig(),
    }


# typing-justified: ingress guard; input unknowable by design; object forces narrowing
def _require_snapshot_identity(endpoint: object) -> None:
    """Reject a non-snapshot identity before any client construction.

    The static gate (the ``SnapshotEndpoint`` parameter type) protects
    type-checked callers; this guard is the same gate for the
    convenience verb's unchecked audience. Typed on ``object`` so the
    narrowing is real work to the type checker rather than a
    statically-unreachable branch.

    Args:
        endpoint: Whatever the caller passed as the endpoint identity.

    Returns:
        None when ``endpoint`` is a ``SnapshotEndpoint``.

    Raises:
        ConfigurationError: ``endpoint`` is windowed-typed (naming the
            endpoint and its mode) or is no catalog identity at all.
    """
    if isinstance(endpoint, SnapshotEndpoint):
        return
    if isinstance(endpoint, WindowedEndpoint):
        raise ConfigurationError(
            'fetch is snapshot-only',
            provider=endpoint.provider.value,
            endpoint=endpoint.name,
            detail=(
                'this endpoint is windowed-mode; windowed retrieval is the '
                'config-driven sync path, not a fetch option'
            ),
        )
    raise ConfigurationError(
        'fetch requires a snapshot identity from the Endpoints catalog',
        detail=f'got {type(endpoint).__name__}',
    )


def fetch(
    endpoint: SnapshotEndpoint,
    auth: AuthInput,
    *,
    use_truststore: bool = False,
) -> pl.DataFrame:
    """Fetch one snapshot endpoint's full current listing into a DataFrame.

    End-to-end in memory: no SQLite, no disk, no cursor, no run ledger,
    no roster. Anything beyond this surface -- windows, incremental
    resume, partitioned storage, roster fan-out, member filtering -- is
    the config-driven sync path's territory, not a missing parameter
    here. The request driver is shape-resolved through the shared seam,
    so every stateless request shape (single fetch, param sweep) is
    served; a roster fan-out needs durable roster state and is refused.

    Args:
        endpoint: A snapshot-typed identity from the ``Endpoints``
            catalog (``Endpoints.Motive.vehicles``). Windowed identities
            fail the type checker and, for unchecked callers, the
            runtime guard.
        auth: The provider credential. Motive and Samsara take a bare
            API-key string; GeoTab takes named fields (a plain mapping
            or a ``GeotabAuthConfig``). Coerced immediately into
            ``SecretStr``-carrying internals; the raw value never
            appears in errors or logs.
        use_truststore: Build TLS contexts from the operating system's
            trust store -- required behind TLS-intercepting corporate
            proxies. Default False, coerced into the identically named
            ``HttpConfig.use_truststore``. Timeouts and any further
            transport posture are config-phase territory.

    Returns:
        An eager Polars DataFrame, dtype-coerced per the endpoint's
        response model. Column order is deliberately unspecified. An
        empty listing is a zero-row frame carrying the full typed schema
        -- never ``None``, never a schemaless frame.

    Raises:
        FleetpullError: Any operational failure -- always one of the
            four public subclasses below; every other exception type is
            internal and renameable.
        ConfigurationError: The identity is not snapshot-typed, the auth
            shape mismatches the endpoint's provider, the identity
            resolves to no registered endpoint, or the endpoint declares
            a roster-backed shape (``RosterFanOut`` /
            ``BatchedRosterFanOut``: roster state is sync territory;
            fetch is stateless by contract).
        AuthenticationError: The provider rejected the credential
            unfixably.
        RetriesExhaustedError: A retryable failure category exhausted
            its attempt budget.
        ProviderResponseError: A non-retryable or contract-violating
            provider response, including a 200 whose body is not JSON
            and records that fail model validation.

    Scope: retrieval, dtype coercion, and light structural normalization
    only -- no cross-endpoint joins, no unified schema, no assumed end
    use (DESIGN §10).
    """
    _require_snapshot_identity(endpoint)
    provider_configs = _default_provider_configs()
    registry = build_endpoint_registry(list(provider_configs.values()))
    definition = registry.get(endpoint.provider, endpoint.name)
    runtime = ClientRuntime(
        http_config=HttpConfig(use_truststore=use_truststore),
        retry_config=RetryConfig(),
        limiter_registry=RateLimiterRegistry(
            rate_limits_from_configs(list(provider_configs.values()))
        ),
    )
    # fetch holds a clock solely for GeoTab session aging; the state-free
    # trace otherwise needs none.
    profile = build_provider_profile(
        endpoint,
        auth,
        ProviderProfileContext(
            http_config=runtime.http_config,
            limiter_registry=runtime.limiter_registry,
            clock=SystemClock(),
        ),
    )
    fetch_workers = {
        endpoint.provider: (
            provider_configs[endpoint.provider].rate_limit.max_concurrency
        )
    }
    with FetchPoolRegistry(fetch_workers) as fetch_pools:
        # The shared shape seam with no roster source: every stateless
        # shape resolves; a roster-backed shape is refused loudly before any
        # transport pool opens. The resolved driver owns the chain (spec
        # build, page loop) and, when the definition declares a
        # completeness check, the post-stream count verification -- fetch
        # must not offer a weaker read of a guarded endpoint than sync
        # does.
        driver = resolve_request_driver(
            definition, fetch_pools=fetch_pools, roster_members=None
        )
        with TransportClient(profile, runtime) as client:
            raw_records: list[JsonObject] = [
                record
                for page in driver.record_batches(definition, client, None)
                for record in page.records
            ]
    logger.info(
        'Fetched %d %s.%s records across the snapshot listing.',
        len(raw_records),
        endpoint.provider.value,
        endpoint.name,
    )
    validated_models = validate_records(raw_records, definition.response_model)
    return models_to_dataframe(validated_models, definition.response_model)
