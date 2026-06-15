# src/fleetpull/network/classifiers/geotab.py
"""GeoTab response classifier (sources: scrubbed provider-behavior
verification, June 2026).

GeoTab is JSON-RPC: every application-level failure arrives inside
HTTP 200, so this classifier is envelope-driven — a status-code-only
client cannot see failure. The discriminator is ``error.data.type``
(observed present in every captured failure); classification never
reads human-readable message text. ``detail`` carries
``error['message']`` for humans, decisions never read it. Branch logic
deliberately resembles sibling classifiers without sharing code:
provider classifiers evolve independently (blast-radius over DRY).
"""

import json
from collections.abc import Mapping

from fleetpull.network.contract import (
    SERVER_ERROR_FLOOR,
    ClassifiedResponse,
    JsonValue,
    ResponseClassifier,
    body_snippet,
    retry_after_seconds_from_headers,
)
from fleetpull.vocabulary import ResponseCategory

__all__: list[str] = ['GeotabResponseClassifier']


def _detail_with_message(prefix: str, error_message: str | None) -> str:
    """Append the envelope's human-readable message when present."""
    if error_message is None:
        return prefix
    return f'{prefix} ({error_message})'


def _classify_error_envelope(
    error_envelope: JsonValue, headers: Mapping[str, str]
) -> ClassifiedResponse:
    """Classify a JSON-RPC ``error`` member by ``data.type``."""
    error_message: str | None = None
    error_type: str | None = None
    if isinstance(error_envelope, dict):
        message_value: JsonValue = error_envelope.get('message')
        if isinstance(message_value, str):
            error_message = message_value
        data_value: JsonValue = error_envelope.get('data')
        if isinstance(data_value, dict):
            type_value: JsonValue = data_value.get('type')
            if isinstance(type_value, str):
                error_type = type_value

    if error_type is None:
        return ClassifiedResponse(
            category=ResponseCategory.FATAL,
            detail=_detail_with_message(
                'malformed GeoTab error envelope: missing data.type',
                error_message,
            ),
        )

    match error_type:
        case 'OverLimitException':
            return ClassifiedResponse(
                category=ResponseCategory.RATE_LIMITED,
                retry_after_seconds=retry_after_seconds_from_headers(headers),
                detail=error_message,
            )
        case 'InvalidUserException':
            # Captured June 2026: bad credentials and dead sessions BOTH
            # produce InvalidUserException, distinguished only by message
            # text, which we never match on. Disambiguation is contextual
            # and already structural: on a data call the auth strategy
            # invalidates and retries once; on the Authenticate call
            # itself the failure propagates out of the session manager
            # as fatal.
            return ClassifiedResponse(
                category=ResponseCategory.AUTH_FAILURE,
                detail=error_message,
            )
        case 'DbUnavailableException':
            # Documented GeoTab transient.
            return ClassifiedResponse(
                category=ResponseCategory.TRANSIENT,
                detail=error_message,
            )
        case _:
            # Fail loud on exception types we have never met rather
            # than guessing they are retryable.
            return ClassifiedResponse(
                category=ResponseCategory.FATAL,
                detail=_detail_with_message(
                    f'unrecognized GeoTab exception type: {error_type}',
                    error_message,
                ),
            )


class GeotabResponseClassifier(ResponseClassifier):
    """Classifies GeoTab JSON-RPC responses by envelope inspection."""

    def classify_response(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body_text: str,
    ) -> ClassifiedResponse:
        """Classify one GeoTab response: status first, then envelope."""
        if status_code >= SERVER_ERROR_FLOOR:
            # Infrastructure ahead of the API; no body parsing needed.
            return ClassifiedResponse(
                category=ResponseCategory.TRANSIENT,
                detail=f'HTTP {status_code}: {body_snippet(body_text)}',
            )

        try:
            parsed_body: JsonValue = json.loads(body_text)
        except json.JSONDecodeError:
            # Exactly what the load balancer's HTML error pages look
            # like from here.
            return ClassifiedResponse(
                category=ResponseCategory.FATAL,
                detail=(
                    f'HTTP {status_code}: unparseable (non-JSON) body: '
                    f'{body_snippet(body_text)}'
                ),
            )

        if isinstance(parsed_body, dict):
            if 'error' in parsed_body:
                return _classify_error_envelope(parsed_body['error'], headers)
            if 'result' in parsed_body:
                # The parse already paid for classification is handed
                # forward; the client must not parse the body again.
                return ClassifiedResponse(
                    category=ResponseCategory.SUCCESS, parsed_body=parsed_body
                )

        return ClassifiedResponse(
            category=ResponseCategory.FATAL,
            detail=(
                'malformed GeoTab JSON-RPC envelope: neither result nor '
                f'error present: {body_snippet(body_text)}'
            ),
        )
