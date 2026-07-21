# src/fleetpull/network/contract/__init__.py
"""The request/response contract: the shared vocabulary and protocols
every network layer speaks. Provider implementations (classifiers,
page decoders, auth strategies) live in sibling packages and import
this surface through this face."""

from fleetpull.network.contract.auth import AuthStrategy
from fleetpull.network.contract.classifier import (
    SERVER_ERROR_FLOOR,
    SUCCESS_STATUS_RANGE,
    ResponseClassifier,
    body_snippet,
    retry_after_seconds_from_headers,
)
from fleetpull.network.contract.envelope_fetcher import EnvelopeFetcher
from fleetpull.network.contract.envelopes import (
    StrictEnvelopeSlice,
    require_child_object,
    require_record_list,
    unwrap_record_objects,
    validated_envelope_slice,
)
from fleetpull.network.contract.outcome import ClassifiedResponse
from fleetpull.network.contract.page_decoder import (
    DecodedPage,
    PageAdvance,
    PageDecoder,
)
from fleetpull.network.contract.request import HttpMethod, RequestSpec

__all__: list[str] = [
    'SERVER_ERROR_FLOOR',
    'SUCCESS_STATUS_RANGE',
    'AuthStrategy',
    'ClassifiedResponse',
    'DecodedPage',
    'EnvelopeFetcher',
    'HttpMethod',
    'PageAdvance',
    'PageDecoder',
    'RequestSpec',
    'ResponseClassifier',
    'StrictEnvelopeSlice',
    'body_snippet',
    'require_child_object',
    'require_record_list',
    'retry_after_seconds_from_headers',
    'unwrap_record_objects',
    'validated_envelope_slice',
]
