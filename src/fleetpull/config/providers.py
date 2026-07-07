# src/fleetpull/config/providers.py
"""The provider config family: the shared base, the sections, the container.

One model family per file (house rule): ``ProviderConfig`` (the
per-provider contract), the concrete provider sections (``MotiveConfig``
today; Samsara and GeoTab join as they port), and ``ProvidersConfig``
(the ``providers:`` YAML container) evolve together.

The family also owns two provider facts consumed above the models:
``PROVIDER_CREDENTIAL_ENV_VARS`` (the conventional per-provider
credential environment variables the loading step merges from) and
``require_provider_credentials`` (the enablement invariant the root
config enforces at validation: endpoints listed with no credential is a
``ConfigurationError``).
"""

import logging
from collections.abc import Mapping
from typing import ClassVar

from pydantic import Field, SecretStr, field_validator

from fleetpull.config.base import ConfigModel
from fleetpull.config.rate_limit import RateLimitConfig
from fleetpull.exceptions import ConfigurationError
from fleetpull.vocabulary import QuotaScope

__all__: list[str] = [
    'PROVIDER_CREDENTIAL_ENV_VARS',
    'MotiveConfig',
    'ProviderConfig',
    'ProvidersConfig',
    'require_provider_credentials',
]

logger = logging.getLogger(__name__)

# The conventional credential environment variable per provider -- the
# fallback the loading step merges when the YAML key is absent (a YAML
# literal wins). Motive's is the name the snapshot script has always
# read; new providers add their entry as they port.
PROVIDER_CREDENTIAL_ENV_VARS: Mapping[str, str] = {'motive': 'MOTIVE_API_KEY'}

_MOTIVE_DEFAULT_BASE_URL: str = 'https://api.gomotive.com'
_MOTIVE_MAX_RECORDS_PER_PAGE: int = 100
_MOTIVE_DEFAULT_LOOKBACK_DAYS: int = 7
_MOTIVE_DEFAULT_CUTOFF_DAYS: int = 0

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
    defaults its ``rate_limit``; the model policy itself comes from
    ``ConfigModel``.

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
            tier, never in ``config``. Default empty; a provider with no
            endpoints is disabled regardless of its credential.
    """

    quota_scope: ClassVar[QuotaScope]

    rate_limit: RateLimitConfig
    endpoints: tuple[str, ...] = ()


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
            endpoints -- how far the resume window's end is held back from
            the clock, so a still-arriving day is never frozen as a complete
            partition. The complement of ``lookback_days``: both express the
            same provider data-latency concern from opposite ends, and both
            carry the same per-provider-key > ``sync``-key > default
            precedence. Optional; defaults to 0.
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
    lookback_days: int = Field(default=_MOTIVE_DEFAULT_LOOKBACK_DAYS, ge=0)
    cutoff_days: int = Field(default=_MOTIVE_DEFAULT_CUTOFF_DAYS, ge=0)

    @field_validator('base_url')
    @classmethod
    def _require_scheme_and_strip_slash(cls, value: str) -> str:
        """Reject a schemeless URL and drop any trailing slash.

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


class ProvidersConfig(ConfigModel):
    """
    The per-provider configuration entries, one instance per run.

    An absent entry means the provider is simply not configured -- no
    warning, no error; the enablement rules apply only to entries that
    are present.

    Attributes:
        motive: The Motive provider section, or ``None`` when the YAML
            does not configure Motive.
    """

    motive: MotiveConfig | None = None


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
