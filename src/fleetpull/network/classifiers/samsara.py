# src/fleetpull/network/classifiers/samsara.py
"""Samsara response classifier (sources: normalized provider-behavior
verification, June 2026; rate-limit contract from official Samsara
documentation).

Classification reads status codes and structured fields, never
human-readable message text; ``detail`` carries messages for humans,
decisions never read them. Branch logic deliberately resembles sibling
classifiers without sharing code: provider classifiers evolve
independently (blast-radius over DRY).
"""

import json
from collections.abc import Mapping
from http import HTTPStatus

from fleetpull.network.contract import (
    SERVER_ERROR_FLOOR,
    SUCCESS_STATUS_RANGE,
    ClassifiedResponse,
    ResponseClassifier,
    body_snippet,
    retry_after_seconds_from_headers,
)
from fleetpull.vocabulary import JsonValue, ResponseCategory

__all__: list[str] = ['SamsaraResponseClassifier']


def _auth_failure_detail(body_text: str) -> str:
    """Extract the body's ``message`` when JSON, else a snippet."""
    try:
        parsed_body: JsonValue = json.loads(body_text)
    except json.JSONDecodeError:
        return body_snippet(body_text)
    if isinstance(parsed_body, dict):
        message: JsonValue = parsed_body.get('message')
        if isinstance(message, str):
            return message
    return body_snippet(body_text)


class SamsaraResponseClassifier(ResponseClassifier):
    """Classifies Samsara REST responses (observed shape:
    ``{"message": "invalid token", "requestId": ...}`` on 401)."""

    def classify_response(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body_text: str,
    ) -> ClassifiedResponse:
        """Classify one Samsara response by status code."""
        match status_code:
            case code if code in SUCCESS_STATUS_RANGE:
                return ClassifiedResponse(category=ResponseCategory.SUCCESS)
            case HTTPStatus.TOO_MANY_REQUESTS:
                # Retry-After is documented as fractional seconds
                # (e.g. 0.40235).
                return ClassifiedResponse(
                    category=ResponseCategory.RATE_LIMITED,
                    retry_after_seconds=retry_after_seconds_from_headers(headers),
                )
            case HTTPStatus.UNAUTHORIZED | HTTPStatus.FORBIDDEN:
                return ClassifiedResponse(
                    category=ResponseCategory.AUTH_FAILURE,
                    detail=_auth_failure_detail(body_text),
                )
            case code if code >= SERVER_ERROR_FLOOR:
                # Documented Samsara behavior: 5xx bodies are plain
                # strings, not JSON — never attempt JSON parsing here.
                return ClassifiedResponse(
                    category=ResponseCategory.TRANSIENT,
                    detail=f'HTTP {status_code}: {body_snippet(body_text)}',
                )
            case _:
                return ClassifiedResponse(
                    category=ResponseCategory.FATAL,
                    detail=f'HTTP {status_code}: {body_snippet(body_text)}',
                )
