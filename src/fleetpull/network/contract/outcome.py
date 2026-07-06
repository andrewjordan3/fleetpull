# src/fleetpull/network/contract/outcome.py
"""The carrier for a classifier's verdict on one response.

Produced exclusively by ``ResponseClassifier`` implementations;
consumed by the client, which dispatches on ``category`` (the
``ResponseCategory`` vocabulary lives in ``fleetpull.vocabulary``).
"""

from dataclasses import dataclass, field

from fleetpull.vocabulary import JsonValue, ResponseCategory

__all__: list[str] = ['ClassifiedResponse']


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
