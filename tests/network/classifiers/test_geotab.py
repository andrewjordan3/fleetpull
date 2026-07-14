"""Tests for the GeoTab response classifier (fixtures: normalized
provider-behavior verification, June 2026; constructed fixtures
marked)."""

import json

import pytest

from fleetpull.network.classifiers.geotab import GeotabResponseClassifier
from fleetpull.vocabulary import ResponseCategory

# Captured: Authenticate success:
GEOTAB_AUTHENTICATE_SUCCESS = (
    '{"result": {"credentials": {"database": "exampledb", "sessionId":'
    ' "SyntheticSessionId000001", "userName": "user@example.com"},'
    ' "path": "ThisServer"}, "jsonrpc": "2.0"}'
)

# Captured: invalid session on a data call (HTTP status was 200):
GEOTAB_INVALID_SESSION = (
    '{"error": {"message": "Invalid session @ \'exampledb\'", "code": -32000,'
    ' "data": {"id": "00000000-0000-0000-0000-000000000001",'
    ' "type": "InvalidUserException", "requestIndex": 0},'
    ' "name": "JSONRPCError", "errors": [{"message":'
    ' "Invalid session @ \'exampledb\'", "name": "InvalidUserException"}]},'
    ' "jsonrpc": "2.0", "requestIndex": 0}'
)

# Captured: bad credentials on Authenticate (HTTP 200; SAME type as
# the invalid-session capture, different message — which is exactly
# why decisions never read messages):
GEOTAB_BAD_CREDENTIALS = (
    '{"error": {"message": "Incorrect login credentials", "code": -32000,'
    ' "data": {"id": "00000000-0000-0000-0000-000000000002",'
    ' "type": "InvalidUserException", "requestIndex": 0},'
    ' "name": "JSONRPCError", "errors": [{"message":'
    ' "Incorrect login credentials", "name": "InvalidUserException"}]},'
    ' "jsonrpc": "2.0", "requestIndex": 0}'
)

# Captured: over limit (HTTP 200; paired header retry-after: 56):
GEOTAB_OVER_LIMIT = (
    '{"error": {"message": "API calls quota exceeded. Maximum admitted 10 per'
    ' 1m.", "code": -32000, "data": {"id":'
    ' "00000000-0000-0000-0000-000000000003", "type": "OverLimitException",'
    ' "requestIndex": 0}, "name": "JSONRPCError", "errors": [{"message": "API'
    ' calls quota exceeded. Maximum admitted 10 per 1m.", "name":'
    ' "OverLimitException"}]}, "jsonrpc": "2.0", "requestIndex": 0}'
)

# Captured: GetFeed success (trimmed):
GEOTAB_GETFEED_SUCCESS = (
    '{"result": {"data": [], "toVersion": "00000000034561f1"}, "jsonrpc": "2.0"}'
)

# CONSTRUCTED from the documented type, in the captured envelope shape:
GEOTAB_DB_UNAVAILABLE = (
    '{"error": {"message": "Database temporarily unavailable", "code": -32000,'
    ' "data": {"id": "00000000-0000-0000-0000-000000000004",'
    ' "type": "DbUnavailableException", "requestIndex": 0},'
    ' "name": "JSONRPCError", "errors": [{"message":'
    ' "Database temporarily unavailable", "name": "DbUnavailableException"}]},'
    ' "jsonrpc": "2.0", "requestIndex": 0}'
)

# CONSTRUCTED — unknown exception type in an otherwise valid envelope:
GEOTAB_UNKNOWN_EXCEPTION = (
    '{"error": {"message": "Something new", "code": -32000,'
    ' "data": {"id": "00000000-0000-0000-0000-000000000005",'
    ' "type": "SomeNewException", "requestIndex": 0},'
    ' "name": "JSONRPCError", "errors": [{"message":'
    ' "Something new", "name": "SomeNewException"}]},'
    ' "jsonrpc": "2.0", "requestIndex": 0}'
)

# Captured: load-balancer HTML page (shape trimmed; arrives with a
# 4xx status):
GEOTAB_HTML_ERROR_PAGE = (
    '<html><head><title>400 Bad Request</title></head>\n'
    '<body><h1>Error: Bad Request</h1></body></html>'
)

# CONSTRUCTED — error envelope missing the authoritative data.type:
GEOTAB_MISSING_TYPE = (
    '{"error": {"message": "mystery failure", "code": -32000,'
    ' "data": {"id": "00000000-0000-0000-0000-000000000006"}},'
    ' "jsonrpc": "2.0"}'
)


@pytest.fixture
def classifier() -> GeotabResponseClassifier:
    return GeotabResponseClassifier()


class TestGeotabSuccess:
    def test_authenticate_result_is_success(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(200, {}, GEOTAB_AUTHENTICATE_SUCCESS)
        assert outcome.category is ResponseCategory.SUCCESS

    def test_getfeed_result_is_success(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(200, {}, GEOTAB_GETFEED_SUCCESS)
        assert outcome.category is ResponseCategory.SUCCESS

    def test_success_hands_the_classification_parse_forward(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        # The classifier parsed the body to classify; the client must
        # never have to parse it again.
        outcome = classifier.classify_response(200, {}, GEOTAB_GETFEED_SUCCESS)
        assert outcome.parsed_body == json.loads(GEOTAB_GETFEED_SUCCESS)


class TestGeotabErrorEnvelopes:
    def test_invalid_session_in_http_200_is_auth_failure(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(200, {}, GEOTAB_INVALID_SESSION)
        assert outcome.category is ResponseCategory.AUTH_FAILURE
        assert outcome.detail is not None
        assert 'Invalid session' in outcome.detail

    def test_bad_credentials_same_type_same_category(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(200, {}, GEOTAB_BAD_CREDENTIALS)
        assert outcome.category is ResponseCategory.AUTH_FAILURE

    def test_over_limit_is_rate_limited_with_header_seconds(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(
            200, {'retry-after': '56'}, GEOTAB_OVER_LIMIT
        )
        assert outcome.category is ResponseCategory.RATE_LIMITED
        assert outcome.retry_after_seconds == pytest.approx(56.0)

    def test_db_unavailable_is_transient(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(200, {}, GEOTAB_DB_UNAVAILABLE)
        assert outcome.category is ResponseCategory.TRANSIENT

    def test_unknown_exception_type_is_fatal_naming_the_type(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(200, {}, GEOTAB_UNKNOWN_EXCEPTION)
        assert outcome.category is ResponseCategory.FATAL
        assert outcome.detail is not None
        assert 'SomeNewException' in outcome.detail

    def test_missing_data_type_is_fatal(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(200, {}, GEOTAB_MISSING_TYPE)
        assert outcome.category is ResponseCategory.FATAL
        assert outcome.detail is not None
        assert 'data.type' in outcome.detail


class TestGeotabNonEnvelopeBodies:
    def test_5xx_is_transient_before_any_parsing(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(503, {}, GEOTAB_HTML_ERROR_PAGE)
        assert outcome.category is ResponseCategory.TRANSIENT

    def test_html_body_with_4xx_is_fatal_via_unparseable_branch(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(400, {}, GEOTAB_HTML_ERROR_PAGE)
        assert outcome.category is ResponseCategory.FATAL
        assert outcome.detail is not None
        assert 'unparseable' in outcome.detail

    def test_envelope_with_neither_result_nor_error_is_fatal(
        self, classifier: GeotabResponseClassifier
    ) -> None:
        outcome = classifier.classify_response(200, {}, '{"jsonrpc": "2.0"}')
        assert outcome.category is ResponseCategory.FATAL
