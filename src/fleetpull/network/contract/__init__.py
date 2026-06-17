# src/fleetpull/network/contract/__init__.py
"""The request/response contract: the shared vocabulary and protocols
every network layer speaks. Provider implementations (classifiers,
paginators, auth strategies) live in sibling packages and import this
surface through this face."""

from fleetpull.network.contract.auth import AuthStrategy
from fleetpull.network.contract.classifier import (
    SERVER_ERROR_FLOOR,
    SUCCESS_STATUS_RANGE,
    ResponseClassifier,
    body_snippet,
    retry_after_seconds_from_headers,
)
from fleetpull.network.contract.envelopes import validated_envelope_slice
from fleetpull.network.contract.outcome import ClassifiedResponse
from fleetpull.network.contract.pagination import PageAdvance, PaginationStrategy
from fleetpull.network.contract.request import (
    HttpMethod,
    JsonObject,
    JsonScalar,
    JsonValue,
    RequestSpec,
)

__all__: list[str] = [
    'SERVER_ERROR_FLOOR',
    'SUCCESS_STATUS_RANGE',
    'AuthStrategy',
    'ClassifiedResponse',
    'HttpMethod',
    'JsonObject',
    'JsonScalar',
    'JsonValue',
    'PageAdvance',
    'PaginationStrategy',
    'RequestSpec',
    'ResponseClassifier',
    'body_snippet',
    'retry_after_seconds_from_headers',
    'validated_envelope_slice',
]
