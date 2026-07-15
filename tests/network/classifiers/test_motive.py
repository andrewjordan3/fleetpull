"""Tests for the Motive response classifier (fixtures: scrubbed
provider-behavior verification, June 2026)."""

import pytest

from fleetpull.network.classifiers.motive import MotiveResponseClassifier
from fleetpull.vocabulary import ResponseCategory

# Captured: invalid API key (HTTP 401):
MOTIVE_INVALID_KEY_BODY = '{"error_message": "invalid API key"}'


@pytest.fixture
def classifier() -> MotiveResponseClassifier:
    return MotiveResponseClassifier()


class TestMotiveClassifier:
    def test_2xx_is_success(self, classifier: MotiveResponseClassifier) -> None:
        outcome = classifier.classify_response(200, {}, '{"vehicles": []}')
        assert outcome.category is ResponseCategory.SUCCESS
        # Motive classifies by status alone, so no parse is handed
        # forward; the client parses.
        assert outcome.parsed_body is None

    def test_429_with_retry_after_header(
        self, classifier: MotiveResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(429, {'Retry-After': '30'}, '')
        assert outcome.category is ResponseCategory.RATE_LIMITED
        assert outcome.retry_after_seconds == pytest.approx(30.0)

    def test_429_without_header_has_no_hint(
        self, classifier: MotiveResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(429, {}, '')
        assert outcome.category is ResponseCategory.RATE_LIMITED
        assert outcome.retry_after_seconds is None

    def test_401_is_auth_failure_with_error_message_detail(
        self, classifier: MotiveResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(401, {}, MOTIVE_INVALID_KEY_BODY)
        assert outcome.category is ResponseCategory.AUTH_FAILURE
        assert outcome.detail == 'invalid API key'

    def test_403_with_non_json_body_uses_snippet(
        self, classifier: MotiveResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(403, {}, 'Forbidden')
        assert outcome.category is ResponseCategory.AUTH_FAILURE
        assert outcome.detail == 'Forbidden'

    def test_5xx_is_transient(self, classifier: MotiveResponseClassifier) -> None:
        outcome = classifier.classify_response(502, {}, 'Bad Gateway')
        assert outcome.category is ResponseCategory.TRANSIENT
        assert outcome.detail is not None
        assert '502' in outcome.detail

    def test_other_status_is_fatal(self, classifier: MotiveResponseClassifier) -> None:
        outcome = classifier.classify_response(404, {}, 'Not Found')
        assert outcome.category is ResponseCategory.FATAL
        assert outcome.detail is not None
        assert '404' in outcome.detail
