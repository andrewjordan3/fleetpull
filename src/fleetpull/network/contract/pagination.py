# src/fleetpull/network/contract/pagination.py
"""The pagination contract: a per-provider strategy the client consults
after every successful page, so the client owns the loop and the
strategy owns the mechanics — the client is pagination-blind.

Verdict-versus-raise rule (DESIGN §8): ``advance`` returns a verdict
because the client dispatches between continue and complete; a
structurally violating pagination envelope has exactly one possible
action, so strategies raise ``ProviderResponseError`` directly rather
than encoding a single-action arm into the verdict.
"""

import logging
from dataclasses import dataclass
from typing import Protocol

from fleetpull.network.contract.request import JsonValue, RequestSpec

__all__: list[str] = [
    'PageAdvance',
    'PaginationStrategy',
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PageAdvance:
    """
    A strategy's verdict on one completed page.

    Attributes:
        next_spec: The request for the next page, or None when
            pagination is complete.
        durable_progress: Cursor progress that must outlive the fetch
            (GeoTab's ``toVersion``; the state layer's FeedToken commit
            value). None for providers whose cursors are fetch-private
            (the established inert pattern). Carried on EVERY page
            including the terminal one — the terminal page's value is
            the resume point.
    """

    next_spec: RequestSpec | None
    durable_progress: str | None


class PaginationStrategy(Protocol):
    """
    Per-provider pagination mechanics, stateless: the client threads
    the loop; the strategy computes. Protocol, not ABC — the three
    provider implementations share zero concrete behavior
    (total-arithmetic, flag-check, and count-comparison respectively);
    the one shared composition lives in
    ``validated_envelope_slice`` (``network/contract/envelopes.py``).
    """

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """
        Decorate the endpoint's base spec for the first page.

        Identity for providers whose first page needs nothing
        (Samsara, GeoTab); Motive merges its page parameters here.

        Args:
            spec: The endpoint definition's base request.

        Returns:
            The spec to actually send first.
        """
        ...

    def advance(self, sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
        """
        Compute the verdict for the page just received.

        Args:
            sent: The spec that produced this page (GeoTab reads
                ``resultsLimit`` from, and rewrites, its body).
            envelope: The parsed response body (the classifier's
                ``parsed_body`` when present; the client's parse
                otherwise) — strategies validate the provider-uniform
                pagination slice only; record extraction is not this
                layer's concern.

        Returns:
            The verdict.

        Raises:
            ProviderResponseError: When the envelope's pagination
                metadata is structurally violating (single-action
                rule).
            ValueError: When ``sent`` is malformed for this strategy —
                a caller bug, deliberately stdlib.
        """
        ...
