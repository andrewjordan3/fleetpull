"""Tests for fleetpull.network.client.transport.

No real network and no real time anywhere. ``httpx.MockTransport`` (injected by
monkeypatching ``httpx.Client``, the same seam the authenticator tests use)
serves every response; a recording ``Sleeper`` double captures backoff without
waiting; a ``FixedFractionGenerator`` makes every jittered delay exact
arithmetic. Auth, classifier, and pagination are small test doubles.

The limiter registry is a recording fake, NOT a real ``RateLimiterRegistry``:
the real limiter's 429 penalty pauses on a ``threading.Condition`` timed in
wall-clock seconds, which a frozen clock cannot advance past, so a real
registry would hang the RATE_LIMITED retry tests. These tests assert only that
the client consults the registry, takes one slot per send, and penalizes with
the clamped value; the limiter's own real-time waiting has its own tests.
"""

import logging
import ssl
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager

import httpx
import pytest

from fleetpull.config import HttpConfig, RetryConfig
from fleetpull.exceptions import (
    AuthenticationError,
    ProviderResponseError,
    RetriesExhaustedError,
)
from fleetpull.network.client import (
    ClientRuntime,
    FetchedPage,
    ProviderProfile,
    TransportClient,
)
from fleetpull.network.contract import (
    ClassifiedResponse,
    HttpMethod,
    JsonValue,
    PageAdvance,
    RequestSpec,
    ResponseClassifier,
)
from fleetpull.network.limits import QuotaScopeLimiter, RateLimiterRegistry
from fleetpull.vocabulary import ResponseCategory

SCOPE = 'data_scope'

# The genuine class, captured before any test monkeypatches httpx.Client, so a
# test that builds two clients (each re-patching) still wraps the real client
# both times instead of a prior shim (the authenticator-test precedent).
_REAL_CLIENT_CLS = httpx.Client


# --------------------------------------------------------------------------- #
# Seams: recording doubles for the injected infrastructure.
# --------------------------------------------------------------------------- #
class RecordingSleeper:
    """Captures requested sleep durations without waiting (Sleeper double)."""

    def __init__(self) -> None:
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


class FixedFractionGenerator:
    """Feeds a preset jitter fraction so every backoff is exact arithmetic."""

    def __init__(self, fraction: float) -> None:
        self._fraction = fraction

    def random(self) -> float:
        return self._fraction


class RecordingLimiter(QuotaScopeLimiter):
    """Stub limiter: no-op slot, recording penalty.

    No ``super().__init__``: the client touches only ``request_slot`` and
    ``penalize``, both overridden here, so the real token bucket and condition
    variable are never constructed — which is the whole point, since the real
    penalty wait blocks on wall-clock time these tests must not spend.
    """

    def __init__(self) -> None:
        self.slot_acquisitions = 0
        self.penalties: list[float] = []

    @contextmanager
    def request_slot(self) -> Iterator[None]:
        self.slot_acquisitions += 1
        yield

    def penalize(self, seconds: float) -> None:
        self.penalties.append(seconds)


class RecordingRegistry(RateLimiterRegistry):
    """Stub registry returning one ``RecordingLimiter`` per scope.

    No ``super().__init__``: ``get`` is the only method the client calls, and
    it is overridden, so the real configured-scope map is never built.
    """

    def __init__(self) -> None:
        self.limiters: dict[str, RecordingLimiter] = {}
        self.scopes_requested: list[str] = []

    def get(self, quota_scope: str) -> QuotaScopeLimiter:
        self.scopes_requested.append(quota_scope)
        return self.limiters.setdefault(quota_scope, RecordingLimiter())


# --------------------------------------------------------------------------- #
# Seams: provider strategy doubles.
# --------------------------------------------------------------------------- #
class StubAuth:
    """AuthStrategy double: scripted prepare errors, configurable refresh."""

    def __init__(
        self,
        *,
        can_refresh: bool = False,
        prepare_errors: Sequence[Exception | None] = (),
    ) -> None:
        self.can_refresh = can_refresh
        self._prepare_errors = list(prepare_errors)
        self.prepare_calls = 0
        self.refresh_calls = 0

    def prepare(self, spec: RequestSpec) -> RequestSpec:
        index = self.prepare_calls
        self.prepare_calls += 1
        if index < len(self._prepare_errors):
            error = self._prepare_errors[index]
            if error is not None:
                raise error
        return spec

    def on_auth_failure(self) -> bool:
        self.refresh_calls += 1
        return self.can_refresh


class StubClassifier(ResponseClassifier):
    """ResponseClassifier double returning a preset verdict sequence.

    The final verdict repeats once the script is exhausted, so a one-element
    script means "always this verdict" (the exhaustion cases) and a two-element
    script means "this once, then that forever" (the recover-after-failure
    cases). Transport exceptions bypass this method entirely — the client calls
    the base ``classify_transport_exception``, so its real TRANSIENT mapping is
    exercised, not stubbed.
    """

    def __init__(self, verdicts: Sequence[ClassifiedResponse]) -> None:
        if not verdicts:
            raise ValueError('need at least one verdict')
        self._verdicts = list(verdicts)
        self.classify_calls = 0

    def classify_response(
        self, status_code: int, headers: Mapping[str, str], body_text: str
    ) -> ClassifiedResponse:
        index = min(self.classify_calls, len(self._verdicts) - 1)
        self.classify_calls += 1
        return self._verdicts[index]


class SinglePageStrategy:
    """PaginationStrategy double that yields exactly one page."""

    def __init__(self) -> None:
        self.first_request_calls = 0
        self.advance_calls = 0

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        self.first_request_calls += 1
        return spec

    def advance(self, sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
        self.advance_calls += 1
        return PageAdvance(next_spec=None, durable_progress='end')


class MultiPageStrategy:
    """PaginationStrategy double walking ``page_count`` pages then stopping.

    Each ``advance`` emits a durable cursor (``v1``, ``v2``, …) including the
    terminal page, and the next spec until the last page returns ``None``.
    """

    def __init__(self, page_count: int) -> None:
        self._page_count = page_count
        self.first_request_calls = 0
        self.advance_calls = 0

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        self.first_request_calls += 1
        return spec

    def advance(self, sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
        self.advance_calls += 1
        durable_progress = f'v{self.advance_calls}'
        if self.advance_calls < self._page_count:
            next_spec = sent.with_merged_params({'page': str(self.advance_calls)})
            return PageAdvance(next_spec=next_spec, durable_progress=durable_progress)
        return PageAdvance(next_spec=None, durable_progress=durable_progress)


class StubHandler:
    """MockTransport handler: constant 200 response, optionally raising first.

    ``errors_before_success`` is raised in order on the first calls (one per
    entry), then every later call returns a 200 carrying ``body_text`` — enough
    to model a flaky send that recovers without scripting every attempt.
    """

    def __init__(
        self,
        *,
        body_text: str = '{}',
        errors_before_success: Sequence[httpx.TransportError] = (),
    ) -> None:
        self._body_text = body_text
        self._errors = list(errors_before_success)
        self.request_count = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        index = self.request_count
        self.request_count += 1
        if index < len(self._errors):
            raise self._errors[index]
        return httpx.Response(200, text=self._body_text)


# --------------------------------------------------------------------------- #
# Builders.
# --------------------------------------------------------------------------- #
def make_spec() -> RequestSpec:
    return RequestSpec(method=HttpMethod.GET, url='https://example.test/data')


def make_retry_config(
    *,
    transient_max_failures: int = 3,
    rate_limited_max_failures: int = 10,
    base_seconds: float = 1.0,
) -> RetryConfig:
    return RetryConfig(
        transient_max_failures=transient_max_failures,
        transient_backoff_base_seconds=base_seconds,
        rate_limited_max_failures=rate_limited_max_failures,
    )


def make_runtime(
    retry_config: RetryConfig,
    sleeper: RecordingSleeper,
    registry: RecordingRegistry,
    *,
    fraction: float = 0.5,
) -> ClientRuntime:
    return ClientRuntime(
        http_config=HttpConfig(),
        retry_config=retry_config,
        limiter_registry=registry,
        random_source=FixedFractionGenerator(fraction),
        sleeper=sleeper,
    )


def make_client(
    monkeypatch: pytest.MonkeyPatch,
    handler: StubHandler,
    profile: ProviderProfile,
    runtime: ClientRuntime,
) -> TransportClient:
    """Build a client whose internal ``httpx.Client`` uses the mock transport."""
    mock_transport = httpx.MockTransport(handler)

    def client_factory(
        *, verify: ssl.SSLContext | bool = True, timeout: httpx.Timeout | None = None
    ) -> httpx.Client:
        # verify is ignored — the mock transport short-circuits real TLS.
        return _REAL_CLIENT_CLS(transport=mock_transport, timeout=timeout)

    monkeypatch.setattr(httpx, 'Client', client_factory)
    return TransportClient(profile, runtime)


# Verdict constructors — one per category, named so each test reads as intent.
def success(parsed_body: JsonValue | None = None) -> ClassifiedResponse:
    return ClassifiedResponse(
        category=ResponseCategory.SUCCESS, parsed_body=parsed_body
    )


def transient() -> ClassifiedResponse:
    return ClassifiedResponse(category=ResponseCategory.TRANSIENT, detail='blip')


def rate_limited(retry_after_seconds: float | None) -> ClassifiedResponse:
    return ClassifiedResponse(
        category=ResponseCategory.RATE_LIMITED,
        retry_after_seconds=retry_after_seconds,
    )


def auth_failure() -> ClassifiedResponse:
    return ClassifiedResponse(category=ResponseCategory.AUTH_FAILURE, detail='401')


def fatal() -> ClassifiedResponse:
    return ClassifiedResponse(category=ResponseCategory.FATAL, detail='400 bad request')


# --------------------------------------------------------------------------- #
# Dispatch matrix.
# --------------------------------------------------------------------------- #
class TestDispatchMatrix:
    def test_success_yields_envelope(self, monkeypatch: pytest.MonkeyPatch) -> None:
        registry = RecordingRegistry()
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([success({'data': [1, 2]})])
        )
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert pages == [FetchedPage(envelope={'data': [1, 2]}, durable_progress='end')]
        assert registry.limiters[SCOPE].slot_acquisitions == 1

    def test_transient_then_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        registry = RecordingRegistry()
        sleeper = RecordingSleeper()
        classifier = StubClassifier([transient(), success({'ok': True})])
        profile = ProviderProfile(auth=StubAuth(), classifier=classifier)
        runtime = make_runtime(make_retry_config(), sleeper, registry)
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert [page.envelope for page in pages] == [{'ok': True}]
        assert classifier.classify_calls == 2
        assert registry.limiters[SCOPE].slot_acquisitions == 2
        assert len(sleeper.sleeps) == 1

    def test_transient_exhausts_raises_with_terminal_count(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([transient()])
        )
        runtime = make_runtime(
            make_retry_config(transient_max_failures=2), RecordingSleeper(), registry
        )
        with (
            make_client(monkeypatch, StubHandler(), profile, runtime) as client,
            pytest.raises(RetriesExhaustedError) as exc_info,
        ):
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        # max_failures=2 retries failures 1,2 and exhausts on the 3rd.
        assert exc_info.value.attempt_count == 3
        assert exc_info.value.category is ResponseCategory.TRANSIENT

    def test_rate_limited_penalizes_then_retries(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        sleeper = RecordingSleeper()
        classifier = StubClassifier([rate_limited(5.0), success({'ok': 1})])
        profile = ProviderProfile(auth=StubAuth(), classifier=classifier)
        runtime = make_runtime(make_retry_config(), sleeper, registry)
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert [page.envelope for page in pages] == [{'ok': 1}]
        assert registry.limiters[SCOPE].penalties == [5.0]
        # The limiter, not the client, owns the 429 wait: no local sleep.
        assert sleeper.sleeps == []

    def test_rate_limited_exhausts_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([rate_limited(1.0)])
        )
        runtime = make_runtime(
            make_retry_config(rate_limited_max_failures=2), RecordingSleeper(), registry
        )
        with (
            make_client(monkeypatch, StubHandler(), profile, runtime) as client,
            pytest.raises(RetriesExhaustedError) as exc_info,
        ):
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert exc_info.value.attempt_count == 3
        assert exc_info.value.category is ResponseCategory.RATE_LIMITED
        # Exhaustion raises before penalizing, so only the retried failures paid.
        assert registry.limiters[SCOPE].penalties == [1.0, 1.0]

    def test_auth_failure_session_refreshes_once_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        auth = StubAuth(can_refresh=True)
        classifier = StubClassifier([auth_failure(), success({'ok': 1})])
        profile = ProviderProfile(auth=auth, classifier=classifier)
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert [page.envelope for page in pages] == [{'ok': 1}]
        assert auth.refresh_calls == 1
        assert auth.prepare_calls == 2  # re-prepared with fresh credentials

    def test_auth_failure_twice_raises_without_second_refresh(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        auth = StubAuth(can_refresh=True)
        profile = ProviderProfile(
            auth=auth, classifier=StubClassifier([auth_failure()])
        )
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with (
            make_client(monkeypatch, StubHandler(), profile, runtime) as client,
            pytest.raises(AuthenticationError),
        ):
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        # Second failure short-circuits on the count, never asking to refresh.
        assert auth.refresh_calls == 1

    def test_auth_failure_static_strategy_raises_without_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        auth = StubAuth(can_refresh=False)
        profile = ProviderProfile(
            auth=auth, classifier=StubClassifier([auth_failure()])
        )
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with (
            make_client(monkeypatch, StubHandler(), profile, runtime) as client,
            pytest.raises(AuthenticationError) as exc_info,
        ):
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert exc_info.value.detail == '401'
        assert auth.prepare_calls == 1  # no retry

    def test_fatal_raises_provider_response_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        profile = ProviderProfile(auth=StubAuth(), classifier=StubClassifier([fatal()]))
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with (
            make_client(monkeypatch, StubHandler(), profile, runtime) as client,
            pytest.raises(ProviderResponseError) as exc_info,
        ):
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert exc_info.value.detail == '400 bad request'


# --------------------------------------------------------------------------- #
# Per-page reset-on-success.
# --------------------------------------------------------------------------- #
class TestResetOnSuccess:
    def test_later_page_gets_a_fresh_transient_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # max_failures=1: each page tolerates one TRANSIENT retry then exhausts
        # on the second. Page 0 fails once then succeeds; page 1 then fails
        # TRANSIENT forever. If the counter leaked across pages, page 1 would
        # exhaust on its FIRST failure (one classify, no sleep). Reset means it
        # gets its own retry: two classifies and one sleep on page 1.
        registry = RecordingRegistry()
        sleeper = RecordingSleeper()
        classifier = StubClassifier(
            [transient(), success({'page': 0}), transient(), transient()]
        )
        profile = ProviderProfile(auth=StubAuth(), classifier=classifier)
        runtime = make_runtime(
            make_retry_config(transient_max_failures=1), sleeper, registry
        )
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            pages = client.fetch_pages(make_spec(), MultiPageStrategy(2), SCOPE)
            first_page = next(pages)
            with pytest.raises(RetriesExhaustedError) as exc_info:
                next(pages)

        assert first_page.envelope == {'page': 0}
        assert exc_info.value.attempt_count == 2  # page 1 used a full fresh budget
        assert classifier.classify_calls == 4  # 2 on page 0, 2 on page 1
        assert len(sleeper.sleeps) == 2  # one retry sleep per page


# --------------------------------------------------------------------------- #
# parsed_body completion.
# --------------------------------------------------------------------------- #
class TestParsedBodyCompletion:
    def test_parsed_body_used_verbatim_never_reparsed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Body is deliberately invalid JSON: if the client re-parsed it,
        # json.loads would raise. A clean pass proves it used the classifier's
        # already-parsed body.
        registry = RecordingRegistry()
        profile = ProviderProfile(
            auth=StubAuth(),
            classifier=StubClassifier([success({'from': 'classifier'})]),
        )
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        handler = StubHandler(body_text='THIS IS NOT JSON')
        with make_client(monkeypatch, handler, profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert pages[0].envelope == {'from': 'classifier'}

    def test_client_parses_body_when_classifier_left_it_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([success()])
        )
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        handler = StubHandler(body_text='{"parsed": "by_client"}')
        with make_client(monkeypatch, handler, profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert pages[0].envelope == {'parsed': 'by_client'}

    def test_success_with_no_envelope_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # SUCCESS with an unparsed body of literal JSON null leaves parsed_body
        # None, tripping the narrowing guard.
        registry = RecordingRegistry()
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([success()])
        )
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with (
            make_client(
                monkeypatch, StubHandler(body_text='null'), profile, runtime
            ) as client,
            pytest.raises(ProviderResponseError, match='no parsed body'),
        ):
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))


# --------------------------------------------------------------------------- #
# Prepare runs outside the limiter slot.
# --------------------------------------------------------------------------- #
class TestPrepareOutsideSlot:
    def test_prepare_transport_error_is_transient_and_retried(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        sleeper = RecordingSleeper()
        auth = StubAuth(prepare_errors=[httpx.ConnectError('dns'), None])
        profile = ProviderProfile(
            auth=auth, classifier=StubClassifier([success({'ok': 1})])
        )
        runtime = make_runtime(make_retry_config(), sleeper, registry)
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert [page.envelope for page in pages] == [{'ok': 1}]
        assert auth.prepare_calls == 2  # failed prepare re-run on retry
        assert len(sleeper.sleeps) == 1  # transient backoff slept once
        # The failed prepare consumed no data-scope token; only the live send did.
        assert registry.limiters[SCOPE].slot_acquisitions == 1

    def test_prepare_authentication_error_propagates_untouched(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        auth = StubAuth(prepare_errors=[AuthenticationError(detail='revoked')])
        profile = ProviderProfile(auth=auth, classifier=StubClassifier([success()]))
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with (
            make_client(monkeypatch, StubHandler(), profile, runtime) as client,
            pytest.raises(AuthenticationError) as exc_info,
        ):
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert exc_info.value.detail == 'revoked'
        assert auth.prepare_calls == 1  # not retried
        assert registry.scopes_requested == []  # never reached the slot


# --------------------------------------------------------------------------- #
# Send-time transport classification.
# --------------------------------------------------------------------------- #
class TestSendTransportError:
    def test_send_transport_error_is_transient_and_retried(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        sleeper = RecordingSleeper()
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([success({'ok': 1})])
        )
        runtime = make_runtime(make_retry_config(), sleeper, registry)
        handler = StubHandler(errors_before_success=[httpx.ReadError('reset')])
        with make_client(monkeypatch, handler, profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert [page.envelope for page in pages] == [{'ok': 1}]
        assert len(sleeper.sleeps) == 1
        # A failed send still consumed its token (it entered the slot); both
        # attempts therefore took a slot.
        assert registry.limiters[SCOPE].slot_acquisitions == 2


# --------------------------------------------------------------------------- #
# Rate-limit penalty value.
# --------------------------------------------------------------------------- #
class TestRateLimitPenalty:
    def test_usable_retry_after_penalizes_by_that_value(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        classifier = StubClassifier([rate_limited(7.5), success({'ok': 1})])
        profile = ProviderProfile(auth=StubAuth(), classifier=classifier)
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert registry.limiters[SCOPE].penalties == [7.5]

    @pytest.mark.parametrize('unusable', [None, 0.0, -3.0])
    def test_missing_or_nonpositive_retry_after_clamps_to_fallback(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        unusable: float | None,
    ) -> None:
        registry = RecordingRegistry()
        classifier = StubClassifier([rate_limited(unusable), success({'ok': 1})])
        profile = ProviderProfile(auth=StubAuth(), classifier=classifier)
        # Default fallback_penalty_seconds is 60.0.
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with (
            make_client(monkeypatch, StubHandler(), profile, runtime) as client,
            caplog.at_level(logging.WARNING),
        ):
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert registry.limiters[SCOPE].penalties == [60.0]
        assert 'fallback penalty' in caplog.text


# --------------------------------------------------------------------------- #
# Pagination across shapes.
# --------------------------------------------------------------------------- #
class TestPagination:
    def test_single_page_yields_exactly_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        strategy = SinglePageStrategy()
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([success({})])
        )
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), strategy, SCOPE))

        assert len(pages) == 1
        assert pages[0].durable_progress == 'end'
        assert strategy.advance_calls == 1

    def test_multi_page_walks_every_page_and_carries_terminal_cursor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        strategy = MultiPageStrategy(3)
        classifier = StubClassifier(
            [success({'page': 0}), success({'page': 1}), success({'page': 2})]
        )
        profile = ProviderProfile(auth=StubAuth(), classifier=classifier)
        runtime = make_runtime(make_retry_config(), RecordingSleeper(), registry)
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            pages = list(client.fetch_pages(make_spec(), strategy, SCOPE))

        assert [page.envelope for page in pages] == [
            {'page': 0},
            {'page': 1},
            {'page': 2},
        ]
        # durable_progress on every page including the terminal one.
        assert [page.durable_progress for page in pages] == ['v1', 'v2', 'v3']
        assert strategy.first_request_calls == 1
        assert strategy.advance_calls == 3


# --------------------------------------------------------------------------- #
# Context manager.
# --------------------------------------------------------------------------- #
class TestContextManager:
    def test_exit_closes_the_pool(self, monkeypatch: pytest.MonkeyPatch) -> None:
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([success({})])
        )
        runtime = make_runtime(
            make_retry_config(), RecordingSleeper(), RecordingRegistry()
        )
        client = make_client(monkeypatch, StubHandler(), profile, runtime)
        with client:
            assert not client._http_client.is_closed
        assert client._http_client.is_closed

    def test_exception_inside_block_propagates_and_still_closes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        profile = ProviderProfile(
            auth=StubAuth(), classifier=StubClassifier([success({})])
        )
        runtime = make_runtime(
            make_retry_config(), RecordingSleeper(), RecordingRegistry()
        )
        client = make_client(monkeypatch, StubHandler(), profile, runtime)
        with pytest.raises(ValueError, match='boom'), client:
            raise ValueError('boom')
        assert client._http_client.is_closed


# --------------------------------------------------------------------------- #
# Backoff determinism.
# --------------------------------------------------------------------------- #
class TestBackoffDeterminism:
    def test_sleeper_asked_for_the_jittered_delay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registry = RecordingRegistry()
        sleeper = RecordingSleeper()
        classifier = StubClassifier([transient(), success({'ok': 1})])
        profile = ProviderProfile(auth=StubAuth(), classifier=classifier)
        # First failure: envelope = min(cap, base * 2**0) = 2.0; fraction 0.5
        # => exactly 1.0 second. No real time elapses (recording sleeper).
        runtime = make_runtime(
            make_retry_config(base_seconds=2.0), sleeper, registry, fraction=0.5
        )
        with make_client(monkeypatch, StubHandler(), profile, runtime) as client:
            list(client.fetch_pages(make_spec(), SinglePageStrategy(), SCOPE))

        assert sleeper.sleeps == [1.0]
