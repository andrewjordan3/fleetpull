# src/fleetpull/network/client/transport.py
"""The transport client: HTTP transport, retry, rate limiting, page decoding.

Owns one pooled httpx.Client and runs the per-attempt pipeline and the page
loop. Provider-blind, state-blind, storage-blind, name-blind: every provider
difference is absorbed by the injected ProviderProfile (auth + classifier) and
the per-endpoint page decoder. The client runs the decoder over each envelope
to emit a FetchedPage of records plus an opaque durable_progress cursor; it
interprets neither the records' field shapes nor the cursor.
"""

import json
import logging
from collections.abc import Iterator
from dataclasses import replace
from types import TracebackType
from typing import Self

import httpx

from fleetpull.exceptions import (
    AuthenticationError,
    ProviderResponseError,
    RetriesExhaustedError,
)
from fleetpull.network.client.page import FetchedPage
from fleetpull.network.client.profile import ProviderProfile
from fleetpull.network.client.runtime import ClientRuntime
from fleetpull.network.contract import (
    ClassifiedResponse,
    DecodedPage,
    PageDecoder,
    RequestSpec,
)
from fleetpull.network.posture import new_http_client
from fleetpull.network.retry import RetryDecision, decide_retry
from fleetpull.vocabulary import JsonValue, ResponseCategory

__all__: list[str] = ['TransportClient']

logger = logging.getLogger(__name__)

# The most of a non-JSON body a raised detail may carry: enough to identify
# a proxy block page or an HTML landing page, never the whole document.
_BODY_EXCERPT_LIMIT: int = 80


def _safe_body_excerpt(body_text: str) -> str:
    """Whitespace-collapsed head of a response body, safe for an error detail.

    Args:
        body_text: The raw response body.

    Returns:
        At most ``_BODY_EXCERPT_LIMIT`` characters, whitespace collapsed.
    """
    return ' '.join(body_text.split())[:_BODY_EXCERPT_LIMIT]


class TransportClient:
    """
    Pulls one endpoint's pages over HTTP with retry and rate limiting.

    Owns one pooled ``httpx.Client``, reused across every attempt and (since
    httpx clients are thread-safe) across every concurrent ``fetch_pages``
    call sharing this instance. Reentrant: ``fetch_pages`` keeps its entire
    working set local, so many threads may run it on one client without
    interference. Use as a context manager so the connection pool is closed.

    Invariants this client must never violate:
        - Every HTTP attempt consumes exactly one limiter token; every page
          is an attempt; ``request_slot()`` wraps exactly one HTTP call,
          never the page loop or the retry loop.
        - ``auth.prepare`` runs OUTSIDE the limiter slot: a GeoTab session
          refresh during prepare consumes the auth scope's token, not the
          data endpoint's, and a failed prepare wastes no data-scope token.
        - The limiter owns all rate-limit waiting. RATE_LIMITED penalizes the
          scope; the next ``request_slot()`` waits it out. The client never
          sleeps for a 429.
        - Per-page consecutive failure counters reset every page by
          construction (they are locals of ``_fetch_single_page``).
    """

    def __init__(self, profile: ProviderProfile, runtime: ClientRuntime) -> None:
        """
        Args:
            profile: The per-provider auth strategy and classifier.
            runtime: Process-global transport infrastructure shared across
                every provider's client.

        Side Effects:
            Constructs one pooled ``httpx.Client`` held for this client's
            lifetime; close it via the context-manager protocol.
        """
        self._profile: ProviderProfile = profile
        self._runtime: ClientRuntime = runtime
        self._http_client: httpx.Client = new_http_client(runtime.http_config)

    def __enter__(self) -> Self:
        """Return self; the pool is already open."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connection pool. Exceptions propagate (returns None)."""
        self._http_client.close()

    def fetch_pages(
        self,
        spec: RequestSpec,
        page_decoder: PageDecoder,
        quota_scope: str,
    ) -> Iterator[FetchedPage]:
        """
        Yield every page of one endpoint, page decoding transparent.

        One visible loop, terminating when the decoder returns no next spec.
        The decoder builds each next request and extracts each page's records;
        the client only sends the request and emits the records.

        Args:
            spec: The endpoint definition's credential-less base request.
            page_decoder: The endpoint's page decoder (per-endpoint, NOT
                per-provider).
            quota_scope: The endpoint's rate-limit scope key.

        Yields:
            One ``FetchedPage`` per page, in order, each carrying its records
            and its ``durable_progress`` (including the terminal page).

        Raises:
            RetriesExhaustedError: A retryable category exhausted its budget
                on a single page.
            AuthenticationError: Credentials are unfixable, or a second
                consecutive auth failure on one page.
            ProviderResponseError: A FATAL response, a structurally
                violating envelope raised by the page decoder, or a
                SUCCESS-classified response whose body is not JSON.
        """
        sent: RequestSpec | None = page_decoder.first_request(spec)
        while sent is not None:
            envelope: JsonValue = self._fetch_single_page(sent, quota_scope)
            decoded: DecodedPage = page_decoder.decode_page(sent, envelope)
            yield FetchedPage(
                records=decoded.records,
                durable_progress=decoded.advance.durable_progress,
            )
            sent = decoded.advance.next_spec

    def fetch_envelope(self, spec: RequestSpec, quota_scope: str) -> JsonValue:
        """
        Execute one request outside any page loop; return its envelope.

        The single-request surface for non-paging calls that must still
        ride the whole per-attempt pipeline -- auth prepare, one limiter
        token per attempt, retry, classification (the completeness
        guard's ``GetCountOf`` is the first consumer). Interpretation of
        the envelope is the caller's; the client stays shape-blind.

        Args:
            spec: The credential-less request to execute.
            quota_scope: The rate-limit scope key the attempt spends from.

        Returns:
            The parsed success envelope.

        Raises:
            RetriesExhaustedError: A retryable category exhausted its
                budget.
            AuthenticationError: Credentials are unfixable, or a second
                consecutive auth failure.
            ProviderResponseError: A FATAL response, or a
                SUCCESS-classified response whose body is not JSON.
        """
        return self._fetch_single_page(spec, quota_scope)

    def _fetch_single_page(self, sent: RequestSpec, quota_scope: str) -> JsonValue:
        """
        Drive the retry loop for ONE page; return its success envelope.

        The three failure counters are locals — they re-zero on entry, which
        IS the reset-on-success semantics: a budget is "N consecutive
        failures of this category on this page", so a long healthy fetch
        never accumulates toward exhaustion and a short fetch is not handed a
        long fetch's budget. The match over ``ResponseCategory`` is
        exhaustive; do not add ``case _``.

        Args:
            sent: The fully decorated spec for this page.
            quota_scope: The endpoint's rate-limit scope key.

        Returns:
            The parsed success envelope for this page.

        Raises:
            RetriesExhaustedError, AuthenticationError, ProviderResponseError
            as documented on ``fetch_pages``.
        """
        transient_failures: int = 0
        rate_limited_failures: int = 0
        auth_failures: int = 0
        while True:
            classified: ClassifiedResponse = self._attempt(sent, quota_scope)
            match classified.category:
                case ResponseCategory.SUCCESS:
                    # _attempt guarantees parsed_body is the envelope on
                    # SUCCESS; the guard narrows JsonValue | None and fires
                    # only on a contract violation.
                    envelope: JsonValue | None = classified.parsed_body
                    if envelope is None:
                        raise ProviderResponseError(
                            detail='classifier reported SUCCESS with no parsed body'
                        )
                    return envelope
                case ResponseCategory.TRANSIENT:
                    transient_failures += 1
                    transient_decision: RetryDecision = self._retry_or_exhaust(
                        ResponseCategory.TRANSIENT, transient_failures
                    )
                    self._runtime.sleeper.sleep(transient_decision.local_delay_seconds)
                case ResponseCategory.RATE_LIMITED:
                    rate_limited_failures += 1
                    self._retry_or_exhaust(
                        ResponseCategory.RATE_LIMITED, rate_limited_failures
                    )
                    self._penalize_scope(quota_scope, classified)
                case ResponseCategory.AUTH_FAILURE:
                    auth_failures += 1
                    # Sessions get exactly one refresh-and-retry per page; a
                    # static key (on_auth_failure False) or a second failure
                    # in a row is unfixable.
                    if auth_failures > 1 or not self._profile.auth.on_auth_failure():
                        raise AuthenticationError(detail=classified.detail)
                case ResponseCategory.FATAL:
                    raise ProviderResponseError(detail=classified.detail)

    def _retry_or_exhaust(
        self, category: ResponseCategory, failure_count: int
    ) -> RetryDecision:
        """
        Decide one more retry for a retryable category, or raise.

        Raising on exhaustion happens BEFORE the caller's category-specific
        action (sleep or penalize), so the exhausted attempt pays no delay
        and applies no penalty — pinned by the transport tests.

        Args:
            category: The retryable category being budgeted.
            failure_count: Consecutive failures of this category on this
                page, including the one just observed.

        Returns:
            The retry decision; the caller applies its category-specific
            action.

        Raises:
            RetriesExhaustedError: The category's failure budget is spent.
        """
        decision: RetryDecision = decide_retry(
            category,
            failure_count,
            self._runtime.retry_config,
            self._runtime.random_source,
        )
        if not decision.should_retry:
            raise RetriesExhaustedError(
                category=category,
                attempt_count=failure_count,
            )
        return decision

    def _attempt(self, sent: RequestSpec, quota_scope: str) -> ClassifiedResponse:
        """
        Run one attempt: prepare (outside the slot), then send (inside it).

        Returns one classification. prepare-time and send-time transport
        failures both route through ``classify_transport_exception`` (always
        TRANSIENT). Non-transport errors from the authenticator
        (AuthenticationError, ProviderResponseError) are not caught and
        propagate untouched. On SUCCESS the parsed envelope is guaranteed on
        ``parsed_body``: a status-only classifier (Motive/Samsara) leaves it
        None, so the body is parsed here; a classifier that already parsed
        (GeoTab) is left untouched, so the body is parsed at most once.

        Args:
            sent: The fully decorated spec for this attempt.
            quota_scope: The endpoint's rate-limit scope key.

        Returns:
            The classification of this attempt's response or transport failure.

        Side Effects:
            Consumes one limiter token for the data request; may drive a
            network Authenticate inside ``prepare`` (GeoTab), which consumes
            the auth scope's own token via the authenticator.
        """
        try:
            prepared: RequestSpec = self._profile.auth.prepare(sent)
        except httpx.TransportError as prepare_transport_error:
            return self._profile.classifier.classify_transport_exception(
                prepare_transport_error
            )

        with self._runtime.limiter_registry.get(quota_scope).request_slot():
            try:
                response: httpx.Response = self._http_client.request(
                    method=prepared.method,
                    url=prepared.url,
                    headers=dict(prepared.headers),
                    params=dict(prepared.params)
                    if prepared.params is not None
                    else None,
                    json=prepared.json_body,
                )
            except httpx.TransportError as send_transport_error:
                return self._profile.classifier.classify_transport_exception(
                    send_transport_error
                )

        # Classification runs after the slot releases — it is CPU, not a
        # request; the slot wrapped exactly the HTTP call.
        body_text: str = response.text
        classified: ClassifiedResponse = self._profile.classifier.classify_response(
            response.status_code, response.headers, body_text
        )
        if (
            classified.category is ResponseCategory.SUCCESS
            and classified.parsed_body is None
        ):
            try:
                parsed_envelope: JsonValue = json.loads(body_text)
            except json.JSONDecodeError:
                # A 200 serving non-JSON (a TLS-intercepting proxy's block
                # page, a base_url pointing at a web page) is sustained, not
                # transient: name it rather than burn the retry budget. The
                # detail carries a sanitized excerpt only -- a block page may
                # contain environment details that must never reach logs.
                raise ProviderResponseError(
                    detail=(
                        f'classifier reported SUCCESS but the body is not '
                        f'JSON (content-type '
                        f'{response.headers.get("content-type", "")!r}): '
                        f'{_safe_body_excerpt(body_text)!r}'
                    )
                ) from None
            classified = replace(classified, parsed_body=parsed_envelope)
        return classified

    def _penalize_scope(self, quota_scope: str, classified: ClassifiedResponse) -> None:
        """
        Penalize the whole quota scope for a 429, clamping to the fallback.

        ``penalize`` rejects non-positive seconds, so a missing or
        non-positive parsed Retry-After clamps to ``fallback_penalty_seconds``
        and the parsed value is logged for diagnosis.

        Args:
            quota_scope: The endpoint's rate-limit scope key.
            classified: The RATE_LIMITED verdict carrying the parsed
                Retry-After, when the provider sent a usable one.

        Side Effects:
            Extends the scope-wide pause; logs a WARNING when no usable
            Retry-After was present.
        """
        retry_after_seconds: float | None = classified.retry_after_seconds
        if retry_after_seconds is not None and retry_after_seconds > 0:
            penalty_seconds: float = retry_after_seconds
        else:
            logger.warning(
                'Rate-limited with no usable Retry-After (parsed=%r); applying '
                'fallback penalty of %.2f seconds.',
                retry_after_seconds,
                self._runtime.retry_config.fallback_penalty_seconds,
            )
            penalty_seconds = self._runtime.retry_config.fallback_penalty_seconds
        self._runtime.limiter_registry.get(quota_scope).penalize(penalty_seconds)
