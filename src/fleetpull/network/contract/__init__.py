"""Provider-agnostic request contract: specs, auth strategies, classification."""

from fleetpull.network.contract.auth import (
    AuthStrategy,
    GeotabSessionAuth,
    StaticHeaderAuth,
)
from fleetpull.network.contract.classifier import ResponseClassifier
from fleetpull.network.contract.outcome import ClassifiedResponse, ResponseCategory
from fleetpull.network.contract.pagination import (
    PageAdvance,
    PaginationStrategy,
    validate_pagination_envelope,
)
from fleetpull.network.contract.request import (
    HttpMethod,
    JsonScalar,
    JsonValue,
    RequestSpec,
)

__all__: list[str] = [
    'AuthStrategy',
    'ClassifiedResponse',
    'GeotabSessionAuth',
    'HttpMethod',
    'JsonScalar',
    'JsonValue',
    'PageAdvance',
    'PaginationStrategy',
    'RequestSpec',
    'ResponseCategory',
    'ResponseClassifier',
    'StaticHeaderAuth',
    'validate_pagination_envelope',
]
