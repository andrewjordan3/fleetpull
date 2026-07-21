# src/fleetpull/config/providers.py
"""The provider config family: the shared base, the sections, the container.

One model family per file (house rule): ``ProviderConfig`` (the
per-provider contract), the concrete provider sections (``MotiveConfig``,
``GeotabConfig``, ``SamsaraConfig``), and ``ProvidersConfig`` (the
``providers:`` YAML container) evolve together.

The family also owns two provider facts consumed above the models:
``PROVIDER_CREDENTIAL_ENV_VARS`` (the conventional per-provider
credential environment variables the loading step merges from) and
``require_provider_credentials`` (the enablement invariant the root
config enforces at validation: endpoints listed with no credential is a
``ConfigurationError``).
"""

from collections.abc import Mapping
from typing import ClassVar

from pydantic import Field, SecretStr, field_validator

from fleetpull.config.base import ConfigModel
from fleetpull.config.geotab import GeotabAuthConfig
from fleetpull.config.rate_limit import RateLimitConfig
from fleetpull.exceptions import ConfigurationError
from fleetpull.vocabulary import QuotaScope

__all__: list[str] = [
    'PROVIDER_CREDENTIAL_ENV_VARS',
    'GeotabConfig',
    'MotiveConfig',
    'ProviderConfig',
    'ProvidersConfig',
    'SamsaraConfig',
    'require_provider_credentials',
]

# The conventional credential environment variable per provider -- the
# fallback the loading step merges when the YAML key is absent (a YAML
# literal wins). The mapping is asymmetric by credential shape: Motive's
# variable supplies the WHOLE credential (`api_key`); GeoTab's supplies
# the PASSWORD FIELD only (username, database, and server always come
# from the YAML `auth` section -- they are not secrets). New providers
# add their entry as they port.
PROVIDER_CREDENTIAL_ENV_VARS: Mapping[str, str] = {
    'motive': 'MOTIVE_API_KEY',
    'geotab': 'GEOTAB_PASSWORD',
    'samsara': 'SAMSARA_API_KEY',
}


def _validated_base_url(value: str) -> str:
    """Reject a schemeless URL and drop any trailing slash.

    The shared per-field check behind the ``base_url`` validators of the
    static-key providers (Motive, Samsara) -- generic URL hygiene, not
    provider semantics, so sharing it couples nothing.

    Args:
        value: The configured base URL.

    Returns:
        The base URL with no trailing slash.

    Raises:
        ValueError: When the URL carries no http(s) scheme.
    """
    if not value.startswith(('http://', 'https://')):
        raise ValueError('base_url must start with http:// or https://')
    return value.rstrip('/')


# The watermark-window defaults every provider section shares: a week
# of late-arrival margin, no trailing-edge holdback. Declared once here
# because the knobs are part of the per-provider contract on
# ``ProviderConfig``; per-provider YAML keys and a declared ``sync``
# value still override (provider key > sync key > default; the
# precedence lives in ``config/resolution.py``).
_DEFAULT_LOOKBACK_DAYS: int = 7
_DEFAULT_CUTOFF_DAYS: int = 0

_MOTIVE_DEFAULT_BASE_URL: str = 'https://api.gomotive.com'
_MOTIVE_MAX_RECORDS_PER_PAGE: int = 100

# Conservative default budget for the Motive scope. Motive's real published
# per-key limits remain unverified (DESIGN §13 open question; the documented
# /vehicle_locations limit was not observed to enforce, §8), so this default
# is the diagnostic's proven-safe posture: the live full-fleet fan-out ran
# under these values without a single 429. Tighten or raise once the real
# limits are pinned by probing.
_MOTIVE_DEFAULT_RATE_LIMIT: RateLimitConfig = RateLimitConfig(
    requests_per_period=60, period_seconds=60.0, burst=10, max_concurrency=2
)


class ProviderConfig(ConfigModel):
    """Base for per-provider configuration sections.

    Subclassed once per provider (``MotiveConfig``, ...). Carries the
    per-provider contract: each subclass binds its ``quota_scope`` and
    defaults its ``rate_limit``; the shared watermark window knobs
    (``lookback_days``, ``cutoff_days``) are declared here once for
    every provider; the model policy itself comes from ``ConfigModel``.

    Attributes:
        quota_scope: The quota scope this provider's budget governs. A
            ``ClassVar`` bound by each subclass -- provider identity is a
            code fact, not user configuration; a subclass that forgets to
            bind it fails loudly on first attribute access.
        rate_limit: The token-bucket budget for this provider's scope.
            Each subclass supplies its own documented default.
        endpoints: The endpoint names this provider syncs, as listed in
            the YAML section. Strings here -- validation against the
            endpoint catalog happens at ``Sync`` construction, above this
            tier, never in ``config`` -- but duplicates are rejected at
            validation (a duplicated name would run twice, concurrently).
            Default empty; a provider with no endpoints is disabled
            regardless of its credential.
        lookback_days: Late-arrival re-fetch margin in whole days for
            watermark endpoints -- how far before the stored watermark
            each resume re-fetches, so a record that landed after its
            event-time day is recovered and its partitions replaced.
            Optional per-provider YAML key; when absent, root-level
            resolution fans in a declared ``sync.lookback_days``, else
            this documented default stands (provider key > sync key >
            default). Non-negative; zero means no margin beyond the
            watermark's own date.
        cutoff_days: Trailing-edge holdback in whole days for watermark
            endpoints -- how far the resume window's end is held back
            from the clock, so a still-arriving day is never frozen as a
            complete partition. The complement of ``lookback_days``:
            both express the same provider data-latency concern from
            opposite ends, and both carry the same per-provider-key >
            ``sync``-key > default precedence. Optional; defaults to 0.
    """

    quota_scope: ClassVar[QuotaScope]

    rate_limit: RateLimitConfig
    endpoints: tuple[str, ...] = ()
    lookback_days: int = Field(default=_DEFAULT_LOOKBACK_DAYS, ge=0)
    cutoff_days: int = Field(default=_DEFAULT_CUTOFF_DAYS, ge=0)

    @field_validator('endpoints')
    @classmethod
    def _reject_duplicate_endpoints(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Reject a duplicated endpoint name loudly, naming every duplicate.

        A duplicated name would run twice -- concurrently, since endpoints
        within a provider run staged-concurrent -- so it is a configuration
        failure to surface, never a silent dedup.
        """
        duplicated = sorted({name for name in value if value.count(name) > 1})
        if duplicated:
            raise ValueError(
                f'endpoint names must be unique; duplicated: {", ".join(duplicated)}'
            )
        return value


class MotiveConfig(ProviderConfig):
    """
    User-facing Motive provider settings, one instance per run.

    Attributes:
        api_key: The Motive API credential for the config-driven sync
            path (``fetch`` takes its credential as an argument instead).
            Optional in YAML -- ``FleetpullConfig.from_yaml`` merges the
            ``MOTIVE_API_KEY`` environment variable when the key is
            absent. ``SecretStr`` from parse time on: masked in every
            repr and never logged.
        base_url: Root of the Motive API. Optional; defaults to Motive's
            documented production host. Must carry an http(s) scheme and
            is normalized to drop any trailing slash, so a spec-builder
            joins a leading-slash request path to it directly.
        records_per_page: Page size requested from paginated Motive
            endpoints. Optional; defaults to Motive's documented maximum.
            Bounded to ``1..100`` (the documented ceiling) so a typo
            cannot silently request an out-of-range page.
        rate_limit: The Motive scope's token-bucket budget. Optional;
            defaults to the conservative values the live diagnostic proved
            safe (Motive's real published limits are unverified -- DESIGN
            §13); see ``_MOTIVE_DEFAULT_RATE_LIMIT`` for the rationale.
    """

    quota_scope: ClassVar[QuotaScope] = QuotaScope.MOTIVE

    api_key: SecretStr | None = None
    base_url: str = Field(default=_MOTIVE_DEFAULT_BASE_URL)
    rate_limit: RateLimitConfig = Field(default=_MOTIVE_DEFAULT_RATE_LIMIT)
    records_per_page: int = Field(
        default=_MOTIVE_MAX_RECORDS_PER_PAGE, ge=1, le=_MOTIVE_MAX_RECORDS_PER_PAGE
    )

    @field_validator('base_url')
    @classmethod
    def _require_scheme_and_strip_slash(cls, value: str) -> str:
        """Apply the shared base-URL hygiene check (see the module helper)."""
        return _validated_base_url(value)


# The Get method-class budget, from the captured rate headers
# (2026-07-09): `X-Rate-Limit-Remaining: 649` after one call implies
# ~650/min -- a SINGLE datum, so this default errs conservative on burst
# and is revisited if live runs contradict it. The docs caveat that the
# headers may precede enforcement; fleetpull self-limits at the
# advertised budget regardless (DESIGN §8 probe-settled decision 3).
# max_concurrency mirrors the Motive posture and is inert until a GeoTab
# fan-out endpoint exists (no GeoTab endpoint declares a fan-out today).
_GEOTAB_DEFAULT_GET_RATE_LIMIT: RateLimitConfig = RateLimitConfig(
    requests_per_period=650, period_seconds=60.0, burst=100, max_concurrency=2
)

# The GetFeed method-class budget: ~60/min, from the 2026-07-21
# header-decrement probe (x-rate-limit-limit '1m' with remaining counting
# down call by call on GetFeed while the Get class sat at ~650/min --
# GetFeed is its OWN method class, not a Get-class spender). Burst stays
# conservative on the small budget, and max_concurrency 2 lets two feed
# ENDPOINTS interleave pages within a sync (each feed walk is itself
# strictly serial, one chain page after page) without letting a wider
# fan-out ever form on this class.
_GEOTAB_DEFAULT_FEED_RATE_LIMIT: RateLimitConfig = RateLimitConfig(
    requests_per_period=60, period_seconds=60.0, burst=10, max_concurrency=2
)

# The Authenticate method-class budget: 10/min, from the June 2026
# `OverLimitException` capture ("API calls quota exceeded. Maximum
# admitted 10 per 1m.", paired `retry-after: 56`) and the provider docs
# row (Status: Active). Authenticate fires rarely behind the session
# manager's single-flight lock, so burst stays small and max_concurrency
# is 1 -- inert by construction (nothing fans out on Authenticate; the
# call only ever takes limiter slots one at a time).
_GEOTAB_DEFAULT_AUTHENTICATE_RATE_LIMIT: RateLimitConfig = RateLimitConfig(
    requests_per_period=10, period_seconds=60.0, burst=2, max_concurrency=1
)


class GeotabConfig(ProviderConfig):
    """
    User-facing GeoTab provider settings, one instance per run.

    The inherited watermark window knobs apply since the ``trips``
    vertical (amended 2026-07-13; the earlier omission encoded a
    superseded feeds-only view of GeoTab incrementality -- windowed
    ``Get`` is GeoTab's history path today, and feeds remain the future
    incremental mechanism; DESIGN §4's amendment). For ``trips``, the
    ``lookback_days`` margin is also what absorbs GeoTab's Trip
    recalculation. Deliberately no ``base_url``: the API host is
    ``auth.server``, and the session strategy retargets every call to
    the host ``Authenticate`` resolves (DESIGN §8).

    Attributes:
        auth: The four-field GeoTab credential (username, password,
            database, server), nested -- it never flattens into the
            provider section. Optional in YAML for a disabled provider;
            enablement requires it (``require_provider_credentials``).
            ``from_yaml`` merges the ``GEOTAB_PASSWORD`` environment
            variable into an ``auth`` section missing its password.
        endpoints: The endpoint names this provider syncs (catalog
            validation happens at ``Sync`` construction, above this tier).
        rate_limit: The Get method-class budget (the scope ``devices``
            and ``trips`` declare); default from the captured 2026-07-09
            headers -- see ``_GEOTAB_DEFAULT_GET_RATE_LIMIT`` for the
            single-datum caveat.
        feed_rate_limit: The GetFeed method-class budget (the scope the
            feed endpoints declare); default ~60/min from the 2026-07-21
            header-decrement probe -- see
            ``_GEOTAB_DEFAULT_FEED_RATE_LIMIT``.
        authenticate_rate_limit: The Authenticate method-class budget;
            default 10/min from the June 2026 capture -- see
            ``_GEOTAB_DEFAULT_AUTHENTICATE_RATE_LIMIT``.
    """

    quota_scope: ClassVar[QuotaScope] = QuotaScope.GEOTAB_GET

    auth: GeotabAuthConfig | None = None
    rate_limit: RateLimitConfig = Field(default=_GEOTAB_DEFAULT_GET_RATE_LIMIT)
    feed_rate_limit: RateLimitConfig = Field(default=_GEOTAB_DEFAULT_FEED_RATE_LIMIT)
    authenticate_rate_limit: RateLimitConfig = Field(
        default=_GEOTAB_DEFAULT_AUTHENTICATE_RATE_LIMIT
    )


_SAMSARA_DEFAULT_BASE_URL: str = 'https://api.samsara.com'

# Conservative default budget for the provider-wide Samsara scope.
# Samsara's documented limits (developers.samsara.com/docs/rate-limits,
# fetched 2026-07-17; documented, not captured) are 150 requests/second
# per token and 200/second per organization, BUT individual endpoints
# carry tiered limits down to 100 requests per MINUTE. Until the
# per-endpoint scope split lands (DESIGN §7 anticipates it; each
# endpoint's tier is pinned as it ports), the provider-wide scope
# self-limits at that tightest documented tier so no endpoint can be
# over-driven by a provider-level default. Raise in config for known
# faster tiers; revisit as endpoints declare their own scopes.
_SAMSARA_DEFAULT_RATE_LIMIT: RateLimitConfig = RateLimitConfig(
    requests_per_period=100, period_seconds=60.0, burst=10, max_concurrency=2
)


class SamsaraConfig(ProviderConfig):
    """
    User-facing Samsara provider settings, one instance per run.

    Attributes:
        api_key: The Samsara API token for the config-driven sync path
            (``fetch`` takes its credential as an argument instead).
            Optional in YAML -- ``FleetpullConfig.from_yaml`` merges the
            ``SAMSARA_API_KEY`` environment variable when the key is
            absent. ``SecretStr`` from parse time on: masked in every
            repr and never logged. Travels as a bearer token; the
            ``Bearer`` prefix is the auth ingress's concern, never
            configured here.
        base_url: Root of the Samsara API. Optional; defaults to
            Samsara's documented production host. Must carry an http(s)
            scheme and is normalized to drop any trailing slash, so a
            spec-builder joins a leading-slash request path to it
            directly.
        rate_limit: The provider-wide Samsara scope's token-bucket
            budget. Optional; defaults to the tightest documented
            per-endpoint tier -- see ``_SAMSARA_DEFAULT_RATE_LIMIT`` for
            the rationale and the per-endpoint-scope revisit note.
    """

    quota_scope: ClassVar[QuotaScope] = QuotaScope.SAMSARA

    api_key: SecretStr | None = None
    base_url: str = Field(default=_SAMSARA_DEFAULT_BASE_URL)
    rate_limit: RateLimitConfig = Field(default=_SAMSARA_DEFAULT_RATE_LIMIT)

    @field_validator('base_url')
    @classmethod
    def _require_scheme_and_strip_slash(cls, value: str) -> str:
        """Apply the shared base-URL hygiene check (see the module helper)."""
        return _validated_base_url(value)


class ProvidersConfig(ConfigModel):
    """
    The per-provider configuration entries, one instance per run.

    An absent entry means the provider is simply not configured -- no
    warning, no error; the enablement rules apply only to entries that
    are present.

    Attributes:
        motive: The Motive provider section, or ``None`` when the YAML
            does not configure Motive.
        geotab: The GeoTab provider section, or ``None`` when the YAML
            does not configure GeoTab.
        samsara: The Samsara provider section, or ``None`` when the YAML
            does not configure Samsara.
    """

    motive: MotiveConfig | None = None
    geotab: GeotabConfig | None = None
    samsara: SamsaraConfig | None = None


def require_provider_credentials(providers: ProvidersConfig) -> None:
    """Enforce the credential half of enablement over every present provider.

    A provider that lists endpoints must have a credential; the root
    config calls this during validation, so the rule holds for YAML
    loading and direct construction alike. (The other half -- a
    credential with no endpoints -- is merely a disabled provider and
    warns at load time, outside validation.)

    Args:
        providers: The validated providers container.

    Raises:
        ConfigurationError: A provider lists endpoints but carries no
            credential, naming the YAML field and the conventional
            environment variable -- never any credential value.
    """
    motive = providers.motive
    if motive is not None and motive.endpoints and motive.api_key is None:
        environment_variable = PROVIDER_CREDENTIAL_ENV_VARS['motive']
        raise ConfigurationError(
            'provider credential missing',
            provider='motive',
            detail=(
                'endpoints are configured but no credential resolves; set '
                f"'providers.motive.api_key' in the YAML or the "
                f'{environment_variable} environment variable'
            ),
        )
    geotab = providers.geotab
    if geotab is not None and geotab.endpoints and geotab.auth is None:
        # The resolvable-password half of GeoTab enablement is structural:
        # GeotabAuthConfig requires its password, so a present `auth` that
        # reached validation carries one (YAML literal or the merged
        # GEOTAB_PASSWORD environment variable). This guard covers the
        # wholly absent credential section.
        environment_variable = PROVIDER_CREDENTIAL_ENV_VARS['geotab']
        raise ConfigurationError(
            'provider credential missing',
            provider='geotab',
            detail=(
                'endpoints are configured but no credential resolves; set '
                f"'providers.geotab.auth' (username, database, and optional "
                f'server in the YAML; the password from the YAML or the '
                f'{environment_variable} environment variable)'
            ),
        )
    samsara = providers.samsara
    if samsara is not None and samsara.endpoints and samsara.api_key is None:
        environment_variable = PROVIDER_CREDENTIAL_ENV_VARS['samsara']
        raise ConfigurationError(
            'provider credential missing',
            provider='samsara',
            detail=(
                'endpoints are configured but no credential resolves; set '
                f"'providers.samsara.api_key' in the YAML or the "
                f'{environment_variable} environment variable'
            ),
        )
