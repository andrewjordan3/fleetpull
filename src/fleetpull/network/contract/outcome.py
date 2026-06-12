# src/fleetpull/network/contract/outcome.py
"""The closed response-classification vocabulary and its carrier.

Produced exclusively by ``ResponseClassifier`` implementations;
consumed by the client, which dispatches on category.
"""

from dataclasses import dataclass, field
from enum import StrEnum

from fleetpull.network.contract.request import JsonValue

__all__: list[str] = ['ClassifiedResponse', 'ResponseCategory']


class ResponseCategory(StrEnum):
    """
    Closed vocabulary of "what the client does next."

    Each member earns its slot by demanding a distinct client action.
    Closure invariant (DESIGN.md §8): a new category is admissible only
    if it arrives with a new client action.
    """

    SUCCESS = 'success'  # parse and yield records
    TRANSIENT = 'transient'  # retry with backoff
    RATE_LIMITED = 'rate_limited'  # penalize the shared quota scope, then retry
    AUTH_FAILURE = (
        'auth_failure'  # ask the auth strategy whether one retry is worthwhile
    )
    FATAL = 'fatal'  # raise


@dataclass(frozen=True, slots=True)
class ClassifiedResponse:
    """
    A classifier's verdict on one response or transport failure.

    Fields are inert outside their category (the established
    required-with-default-inert pattern).

    Attributes:
        category: What the client does next.
        retry_after_seconds: Meaningful only for RATE_LIMITED; None
            means the provider sent no usable hint and the client
            applies its fallback penalty.
        detail: Human-readable context for failure paths. Decisions
            never read it.
        parsed_body: The parsed equivalent of the response's body text
            — the whole document, not extracted records. Populated only
            when classification itself required parsing the body; None
            on SUCCESS means the classifier did not parse and the
            client must. Inert outside SUCCESS. Excluded from ``repr``
            because it can hold a multi-megabyte structure that no log
            line may embed.
    """

    category: ResponseCategory
    retry_after_seconds: float | None = None
    detail: str | None = None
    parsed_body: JsonValue | None = field(default=None, repr=False)
