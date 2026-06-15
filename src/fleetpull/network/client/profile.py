# src/fleetpull/network/client/profile.py
"""The per-provider strategy bundle the client receives at construction."""

from dataclasses import dataclass

from fleetpull.network.contract import AuthStrategy, ResponseClassifier

__all__: list[str] = ['ProviderProfile']


@dataclass(frozen=True, slots=True)
class ProviderProfile:
    """
    The per-provider, per-construction dependencies of a transport client.

    Auth strategy and classifier are shared across all of a provider's
    endpoints (one session auth, one classifier). Pagination strategy and
    quota scope are deliberately NOT here — they are per-endpoint and arrive
    on each ``fetch_pages`` call.

    Attributes:
        auth: Credential injection for this provider.
        classifier: Response and transport classification for this provider.
    """

    auth: AuthStrategy
    classifier: ResponseClassifier
