"""Tests for fleetpull.network.contract.classifier."""

from collections.abc import Mapping

import httpx
import pytest

from fleetpull.network.contract.classifier import (
    _BODY_SNIPPET_MAX_CHARS,
    ResponseClassifier,
    body_snippet,
    retry_after_seconds_from_headers,
)
from fleetpull.network.contract.outcome import ClassifiedResponse, ResponseCategory


class MinimalClassifier(ResponseClassifier):
    """Concrete subclass exercising the shared base behavior only."""

    def classify_response(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body_text: str,
    ) -> ClassifiedResponse:
        return ClassifiedResponse(category=ResponseCategory.SUCCESS)


class TestClassifyTransportException:
    def test_timeout_is_transient_naming_the_class(self) -> None:
        outcome = MinimalClassifier().classify_transport_exception(
            httpx.TimeoutException('request timed out')
        )
        assert outcome.category is ResponseCategory.TRANSIENT
        assert outcome.detail is not None
        assert 'TimeoutException' in outcome.detail

    def test_connect_error_is_transient(self) -> None:
        outcome = MinimalClassifier().classify_transport_exception(
            httpx.ConnectError('connection refused')
        )
        assert outcome.category is ResponseCategory.TRANSIENT

    def test_non_transport_exception_reraises_untouched(self) -> None:
        programming_error = ValueError('not a transport problem')
        with pytest.raises(ValueError, match='not a transport problem') as excinfo:
            MinimalClassifier().classify_transport_exception(programming_error)
        assert excinfo.value is programming_error


class TestRetryAfterSecondsFromHeaders:
    @pytest.mark.parametrize(
        'header_name',
        [
            'Retry-After',  # canonical casing
            'retry-after',  # lowercase, as captured from GeoTab
        ],
    )
    def test_header_found_regardless_of_key_casing(self, header_name: str) -> None:
        assert retry_after_seconds_from_headers({header_name: '56'}) == pytest.approx(
            56.0
        )

    def test_absent_header_returns_none(self) -> None:
        assert (
            retry_after_seconds_from_headers({'Content-Type': 'application/json'})
            is None
        )

    @pytest.mark.parametrize(
        ('header_value', 'expected_seconds'),
        [('56', 56.0), ('0.40235', 0.40235)],
    )
    def test_finite_positive_values_parse(
        self, header_value: str, expected_seconds: float
    ) -> None:
        assert retry_after_seconds_from_headers(
            {'Retry-After': header_value}
        ) == pytest.approx(expected_seconds)

    @pytest.mark.parametrize(
        'invalid_value',
        [
            'Wed, 21 Oct 2026 07:28:00 GMT',  # HTTP-date form, unparsed in v1
            'soon',
            '0',
            '-3',
            'nan',
            'inf',
        ],
    )
    def test_non_finite_or_non_positive_values_map_to_none(
        self, invalid_value: str
    ) -> None:
        # The limiter's penalize(seconds) raises on seconds <= 0; this
        # contract must be unviolatable from here.
        assert retry_after_seconds_from_headers({'Retry-After': invalid_value}) is None


class TestBodySnippet:
    def test_short_text_unchanged(self) -> None:
        assert body_snippet('short body') == 'short body'

    def test_long_text_truncated_with_marker(self) -> None:
        long_text = 'a' * (_BODY_SNIPPET_MAX_CHARS + 100)
        snippet = body_snippet(long_text)
        assert snippet == 'a' * _BODY_SNIPPET_MAX_CHARS + '…'
