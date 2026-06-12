"""Provider-agnostic request contract: specs, auth strategies, classification."""

from fleetpull.network.contract.auth import (
    AuthStrategy,
    GeotabSessionAuth,
    StaticHeaderAuth,
)
from fleetpull.network.contract.classifier import ResponseClassifier
from fleetpull.network.contract.outcome import ClassifiedResponse, ResponseCategory
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
    'RequestSpec',
    'ResponseCategory',
    'ResponseClassifier',
    'StaticHeaderAuth',
]
