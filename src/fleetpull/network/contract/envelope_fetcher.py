# src/fleetpull/network/contract/envelope_fetcher.py
"""The single-request fetch surface: one spec in, one parsed envelope out.

The contract-layer face of ``TransportClient.fetch_envelope`` — the
non-paging single request that still rides the whole per-attempt pipeline
(auth prepare, one limiter token per attempt, retry, classification).
Declared here so declaration-layer consumers (the ``CompletenessCheck``
protocol's implementations foremost) can type against the surface without
importing the client package; ``TransportClient`` satisfies it structurally.
"""

from typing import Protocol

from fleetpull.network.contract.request import RequestSpec
from fleetpull.vocabulary import JsonValue

__all__: list[str] = ['EnvelopeFetcher']


class EnvelopeFetcher(Protocol):
    """The one-method single-request surface (``TransportClient``'s shape)."""

    def fetch_envelope(self, spec: RequestSpec, quota_scope: str) -> JsonValue:
        """Execute one request outside any page loop; return its envelope.

        Args:
            spec: The credential-less request to execute.
            quota_scope: The rate-limit scope key the attempt spends from.

        Returns:
            The parsed success envelope; interpretation is the caller's.
        """
        ...
