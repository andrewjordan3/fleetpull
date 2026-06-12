# src/fleetpull/network/contract/classifiers/motive.py
"""
Motive response classifier (sources: M1 capture, June 2026; rate
limiting probed and never observed — generic HTTP posture).

Classification reads status codes and structured fields, never
human-readable message text; ``detail`` carries messages for humans,
decisions never read them. Branch logic deliberately resembles sibling
classifiers without sharing code: provider classifiers evolve
independently (blast-radius over DRY).
"""

import json
from collections.abc import Mapping

from fleetpull.network.contract.classifier import (
    SERVER_ERROR_FLOOR,
    SUCCESS_STATUS_RANGE,
    ResponseClassifier,
    body_snippet,
    find_header,
    parse_retry_after_seconds,
)
from fleetpull.network.contract.outcome import ClassifiedResponse, ResponseCategory
from fleetpull.network.contract.request import JsonValue

__all__: list[str] = ['MotiveResponseClassifier']


def _auth_failure_detail(body_text: str) -> str:
    """Extract the body's ``error_message`` when JSON, else a snippet."""
    try:
        parsed_body: JsonValue = json.loads(body_text)
    except json.JSONDecodeError:
        return body_snippet(body_text)
    if isinstance(parsed_body, dict):
        error_message: JsonValue = parsed_body.get('error_message')
        if isinstance(error_message, str):
            return error_message
    return body_snippet(body_text)


class MotiveResponseClassifier(ResponseClassifier):
    """Classifies Motive REST responses (observed shape:
    ``{"error_message": "invalid API key"}`` on 401)."""

    def classify_response(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body_text: str,
    ) -> ClassifiedResponse:
        """Classify one Motive response by status code."""
        match status_code:
            case code if code in SUCCESS_STATUS_RANGE:
                return ClassifiedResponse(category=ResponseCategory.SUCCESS)
            case 429:
                # Motive rate limiting was probed and never observed
                # (June 2026); this branch is built to the generic HTTP
                # contract.
                retry_after_header: str | None = find_header(headers, 'Retry-After')
                return ClassifiedResponse(
                    category=ResponseCategory.RATE_LIMITED,
                    retry_after_seconds=(
                        parse_retry_after_seconds(retry_after_header)
                        if retry_after_header is not None
                        else None
                    ),
                )
            case 401 | 403:
                return ClassifiedResponse(
                    category=ResponseCategory.AUTH_FAILURE,
                    detail=_auth_failure_detail(body_text),
                )
            case code if code >= SERVER_ERROR_FLOOR:
                return ClassifiedResponse(
                    category=ResponseCategory.TRANSIENT,
                    detail=f'HTTP {status_code}: {body_snippet(body_text)}',
                )
            case _:
                return ClassifiedResponse(
                    category=ResponseCategory.FATAL,
                    detail=f'HTTP {status_code}: {body_snippet(body_text)}',
                )
