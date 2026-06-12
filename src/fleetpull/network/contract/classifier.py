# src/fleetpull/network/contract/classifier.py
"""
ResponseClassifier ABC and the shared helpers every classifier uses.

This module is the only one in the contract package allowed to import
httpx: the shared transport-exception mapping must name real exception
types, and the classifier is the transport boundary.
"""

import math
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Final

import httpx

from fleetpull.network.contract.outcome import ClassifiedResponse, ResponseCategory

__all__: list[str] = [
    'SERVER_ERROR_FLOOR',
    'SUCCESS_STATUS_RANGE',
    'ResponseClassifier',
    'body_snippet',
    'find_header',
    'parse_retry_after_seconds',
]

# Cap on body text carried into ClassifiedResponse.detail. Every
# classifier truncates through body_snippet; no inline magic numbers.
_BODY_SNIPPET_MAX_CHARS: Final[int] = 200

# Generic HTTP status boundaries shared by all classifiers — protocol
# semantics, not provider behavior, so they live in the base module.
SUCCESS_STATUS_RANGE: Final[range] = range(200, 300)
SERVER_ERROR_FLOOR: Final[int] = 500


def find_header(headers: Mapping[str, str], name: str) -> str | None:
    """
    Case-insensitive header lookup.

    HTTP headers are case-insensitive (captures show ``retry-after``
    lowercase from GeoTab), and this contract must not depend on
    httpx's case-insensitive mapping being the one passed in.

    Args:
        headers: The response headers.
        name: Header name, any casing.

    Returns:
        The header value, or None when absent.
    """
    normalized_name: str = name.lower()
    for header_name, header_value in headers.items():
        if header_name.lower() == normalized_name:
            return header_value
    return None


def parse_retry_after_seconds(value: str) -> float | None:
    """
    Parse the numeric-seconds form of a Retry-After value.

    Samsara sends fractional seconds (e.g. ``0.40235``); GeoTab sends
    integers (e.g. ``58``). The HTTP-date form is deliberately unparsed
    in v1. The consumer is the limiter's ``penalize(seconds)``, which
    raises on ``seconds <= 0`` — this helper must never hand it an
    invalid value, so anything that is not a finite positive number
    maps to None, which safely triggers the client's fallback penalty.

    Args:
        value: The raw header value.

    Returns:
        The positive, finite seconds value, or None for everything
        else (non-numeric strings, HTTP dates, zero, negatives, NaN,
        infinity).
    """
    try:
        seconds: float = float(value)
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds <= 0:
        return None
    return seconds


def body_snippet(body_text: str) -> str:
    """
    Truncate body text for use in ``ClassifiedResponse.detail``.

    Args:
        body_text: The raw response body.

    Returns:
        The text unchanged when within the cap; otherwise the first
        ``_BODY_SNIPPET_MAX_CHARS`` characters with a ``…`` marker.
    """
    if len(body_text) <= _BODY_SNIPPET_MAX_CHARS:
        return body_text
    return body_text[:_BODY_SNIPPET_MAX_CHARS] + '…'


class ResponseClassifier(ABC):
    """
    Per-provider response classifier — the SOLE producer of the
    classification vocabulary; the client only consumes it.

    ``classify_response`` is abstract because provider envelopes differ
    (GeoTab returns JSON-RPC errors inside HTTP 200, so a
    status-code-only client cannot see failure). The transport-exception
    mapping is concrete in the base because timeouts and connection
    failures sit below any provider envelope and must not vary per
    provider (Protocol-for-shape / ABC-for-substance, DESIGN.md §8).
    """

    @abstractmethod
    def classify_response(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body_text: str,
    ) -> ClassifiedResponse:
        """
        Classify one provider response.

        Args:
            status_code: The HTTP status code.
            headers: The response headers.
            body_text: The raw response body.

        Returns:
            The classified outcome the client dispatches on.
        """

    def classify_transport_exception(self, exception: Exception) -> ClassifiedResponse:
        """
        Classify an exception raised by the transport, shared across
        all providers.

        Args:
            exception: The exception the HTTP attempt raised.

        Returns:
            TRANSIENT for any ``httpx.TransportError`` (the hierarchy
            makes timeouts and connect/read/write failures subclasses,
            so one check covers them all), with the concrete exception
            class name in ``detail``.

        Raises:
            Exception: Any non-transport exception is re-raised
                untouched — a ValueError or KeyError reaching the
                classifier is a programming error, and classifying it
                would silence a bug.
        """
        if isinstance(exception, httpx.TransportError):
            return ClassifiedResponse(
                category=ResponseCategory.TRANSIENT,
                detail=f'transport error: {type(exception).__name__}',
            )
        raise exception
