"""Tests for the Samsara response classifier (fixtures: scrubbed
provider-behavior verification, June 2026; rate-limit contract from
official documentation)."""

import pytest

from fleetpull.network.classifiers.samsara import SamsaraResponseClassifier
from fleetpull.vocabulary import ResponseCategory
from tests.samsara_trips_capture import (
    TRIPS_MISSING_VEHICLE_ID_400_BODY,
    TRIPS_RANGE_CAP_400_BODY,
)

# Captured: invalid token (HTTP 401):
SAMSARA_INVALID_TOKEN_BODY = (
    '{"message": "invalid token", "requestId": "aaaabbbb-ccccdddd"}'
)

# Samsara — 5xx body (documented behavior: a PLAIN STRING, not JSON):
SAMSARA_5XX_BODY = 'Service Temporarily Unavailable'


@pytest.fixture
def classifier() -> SamsaraResponseClassifier:
    return SamsaraResponseClassifier()


class TestSamsaraClassifier:
    def test_2xx_is_success(self, classifier: SamsaraResponseClassifier) -> None:
        outcome = classifier.classify_response(200, {}, '{"data": []}')
        assert outcome.category is ResponseCategory.SUCCESS
        # Samsara classifies by status alone, so no parse is handed
        # forward; the client parses.
        assert outcome.parsed_body is None

    def test_429_with_fractional_retry_after(
        self, classifier: SamsaraResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(429, {'Retry-After': '0.40235'}, '')
        assert outcome.category is ResponseCategory.RATE_LIMITED
        assert outcome.retry_after_seconds == pytest.approx(0.40235)

    def test_401_is_auth_failure_with_message_detail(
        self, classifier: SamsaraResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(401, {}, SAMSARA_INVALID_TOKEN_BODY)
        assert outcome.category is ResponseCategory.AUTH_FAILURE
        assert outcome.detail == 'invalid token'

    def test_503_plain_string_body_is_transient_without_raising(
        self, classifier: SamsaraResponseClassifier
    ) -> None:
        # The never-JSON-parse rule on the 5xx branch under test: a
        # plain-string body must classify cleanly.
        outcome = classifier.classify_response(503, {}, SAMSARA_5XX_BODY)
        assert outcome.category is ResponseCategory.TRANSIENT
        assert outcome.detail is not None
        assert SAMSARA_5XX_BODY in outcome.detail

    def test_other_status_is_fatal(self, classifier: SamsaraResponseClassifier) -> None:
        outcome = classifier.classify_response(404, {}, 'Not Found')
        assert outcome.category is ResponseCategory.FATAL
        assert outcome.detail is not None
        assert '404' in outcome.detail

    @pytest.mark.parametrize(
        'body',
        [TRIPS_MISSING_VEHICLE_ID_400_BODY, TRIPS_RANGE_CAP_400_BODY],
        ids=['missing-vehicle-id', 'range-cap'],
    )
    def test_v1_text_plain_400_is_fatal_without_raising(
        self, classifier: SamsaraResponseClassifier, body: str
    ) -> None:
        # The v1 surface extends the plain-string-body posture beyond
        # 5xx: /v1/fleet/trips 400 bodies are TEXT/PLAIN rpc-error
        # strings (captured 2026-07-20). The catch-all FATAL arm must
        # classify them cleanly -- no JSON parse, no crash -- with the
        # body carried as human detail.
        outcome = classifier.classify_response(
            400, {'Content-Type': 'text/plain; charset=utf-8'}, body
        )
        assert outcome.category is ResponseCategory.FATAL
        assert outcome.detail is not None
        assert '400' in outcome.detail
        assert body in outcome.detail
