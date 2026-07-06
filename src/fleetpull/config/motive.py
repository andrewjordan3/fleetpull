# src/fleetpull/config/motive.py
"""Motive provider configuration: the Motive-specific YAML settings.

The API base URL and the page size requested from paginated endpoints;
auth credentials and rate-limit settings attach when those config
surfaces are built. One module per config section (house rule).
"""

import logging
from typing import ClassVar

from pydantic import Field, field_validator

from fleetpull.config.provider import ProviderConfig
from fleetpull.config.rate_limit import RateLimitConfig
from fleetpull.vocabulary import QuotaScope

__all__: list[str] = ['MotiveConfig']

logger = logging.getLogger(__name__)

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


class MotiveConfig(ProviderConfig):
    """
    User-facing Motive provider settings, one instance per run.

    Attributes:
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
            Optional; defaults to 7. Non-negative, where zero means no
            margin beyond the watermark's own date.
        cutoff_days: Trailing-edge holdback in whole days for watermark
            endpoints -- how far the resume window's end is held back from
            the clock, so a still-arriving day is never frozen as a complete
            partition. The complement of ``lookback_days``: both express the
            same provider data-latency concern from opposite ends. Optional;
            defaults to 0. Non-negative, where zero adds no holdback beyond
            the resolver's own date alignment.
        rate_limit: The Motive scope's token-bucket budget. Optional;
            defaults to the conservative values the live diagnostic proved
            safe (Motive's real published limits are unverified -- DESIGN
            §13); see ``_MOTIVE_DEFAULT_RATE_LIMIT`` for the rationale.
    """

    quota_scope: ClassVar[QuotaScope] = QuotaScope.MOTIVE

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
