# src/fleetpull/exceptions.py
"""The fleetpull exception hierarchy: operational errors consumers catch.

The hierarchy mirrors the response-classification vocabulary
(``ResponseCategory``) and inherits its closure invariant: a new
exception type is admissible only if it demands a distinct consumer
action. Programming errors stay stdlib ``ValueError`` /
``RuntimeError`` — a hierarchy that absorbs caller bugs invites broad
``except`` clauses that silence them.

Every member is a plain data carrier: typed fields for programmatic
handling, a composed human message for ``str()``. Instances never
carry raw request or response material (headers, bodies, request
specs, credentials-adjacent values) — every instance is safe to log.
"""

import logging

from fleetpull.vocabulary import ResponseCategory

__all__: list[str] = [
    'AuthenticationError',
    'ConfigurationError',
    'FleetpullError',
    'ProviderResponseError',
    'RetriesExhaustedError',
    'UnknownQuotaScopeError',
]

logger = logging.getLogger(__name__)


def _compose_message(
    head: str,
    provider: str | None,
    endpoint: str | None,
    detail: str | None,
) -> str:
    """
    Compose the human-readable message every hierarchy member carries.

    Single composition point so all members read uniformly:
    ``<head> [provider=..., endpoint=...]: <detail>``, with absent
    fields omitted.

    Args:
        head: Summary phrase supplied by the concrete class.
        provider: Provider name, when known.
        endpoint: Endpoint name, when known.
        detail: Human-readable context; programmatic handling never
            reads it.

    Returns:
        The composed message.
    """
    message: str = head
    context_parts: list[str] = []
    if provider is not None:
        context_parts.append(f'provider={provider}')
    if endpoint is not None:
        context_parts.append(f'endpoint={endpoint}')
    if context_parts:
        message = f'{message} [{", ".join(context_parts)}]'
    if detail is not None:
        message = f'{message}: {detail}'
    return message


class FleetpullError(Exception):
    """
    Root of the hierarchy. Never raised directly — raise the concrete
    member whose consumer action matches. Catching this means "any
    operational fleetpull failure."

    Attributes:
        provider: Provider name, when known.
        endpoint: Endpoint name, when known.
        detail: Human-readable context for the failure. Programmatic
            handling never reads it.
    """

    provider: str | None
    endpoint: str | None
    detail: str | None

    def __init__(
        self,
        message: str,
        *,
        provider: str | None = None,
        endpoint: str | None = None,
        detail: str | None = None,
    ) -> None:
        """
        Store the shared fields and compose the ``str()`` message.

        Keyword-only fields keep raise sites self-describing. The
        signature breaks ``BaseException``'s default pickle round-trip
        (which replays positional ``args``); pickling is deliberately
        unsupported — fleetpull concurrency is threads, and exceptions
        never cross a process boundary in this package.

        Args:
            message: Summary phrase; the concrete class supplies it.
            provider: Provider name, when known.
            endpoint: Endpoint name, when known.
            detail: Human-readable context; never read programmatically.
        """
        super().__init__(_compose_message(message, provider, endpoint, detail))
        self.provider = provider
        self.endpoint = endpoint
        self.detail = detail


class ConfigurationError(FleetpullError):
    """
    Local configuration or wiring is wrong; fix it before rerunning.

    The message is caller-supplied because configuration failures are
    bespoke by nature. Inherits the base ``__init__`` unchanged.
    """


class UnknownQuotaScopeError(ConfigurationError):
    """
    A request named a quota scope the registry was never configured
    with — a configuration bug. No defaults, no fallbacks.

    Attributes:
        scope: The unconfigured quota scope.
    """

    scope: str

    def __init__(self, scope: str, *, detail: str | None = None) -> None:
        """
        Compose the message from the offending scope.

        Args:
            scope: The unconfigured quota scope.
            detail: Human-readable context (e.g. the configured
                scopes); never read programmatically.
        """
        super().__init__(f'unknown quota scope: {scope!r}', detail=detail)
        self.scope = scope


class AuthenticationError(FleetpullError):
    """
    Authentication failed fatally: bad credentials or revoked access.
    Rerunning without fixing credentials cannot succeed.

    Raised when the one-retry-per-attempt-sequence auth path fails a
    second time, or when the authentication call itself is rejected.
    """

    def __init__(
        self,
        *,
        provider: str | None = None,
        endpoint: str | None = None,
        detail: str | None = None,
    ) -> None:
        """
        Compose the fixed head with the shared context fields.

        Args:
            provider: Provider name, when known.
            endpoint: Endpoint name, when known.
            detail: Human-readable context; never read programmatically.
        """
        super().__init__(
            'authentication failed',
            provider=provider,
            endpoint=endpoint,
            detail=detail,
        )


class ProviderResponseError(FleetpullError):
    """
    The provider returned a response that fleetpull classified as
    non-retryable or contract-violating.

    Covers both definitive refusals and structurally unusable
    responses: malformed envelopes, bodies that violate the provider
    contract, unrecognized failure types, unexpected status codes.

    Attributes:
        status_code: HTTP status of the offending response, when one
            exists. A ``category`` field is deliberately absent: every
            raise of this class corresponds to the FATAL
            classification, so the field would carry no information.
    """

    status_code: int | None

    def __init__(
        self,
        *,
        provider: str | None = None,
        endpoint: str | None = None,
        detail: str | None = None,
        status_code: int | None = None,
    ) -> None:
        """
        Compose the head, folding in the status code when present.

        Args:
            provider: Provider name, when known.
            endpoint: Endpoint name, when known.
            detail: Human-readable context; never read programmatically.
            status_code: HTTP status of the offending response, when
                one exists.
        """
        head: str = 'non-retryable provider response'
        if status_code is not None:
            head = f'{head} (HTTP {status_code})'
        super().__init__(head, provider=provider, endpoint=endpoint, detail=detail)
        self.status_code = status_code


class RetriesExhaustedError(FleetpullError):
    """
    A retryable classification kept recurring until its attempt budget
    ran out. Rerunning later is reasonable.

    A ``status_code`` field is deliberately absent, mirroring the
    ``ProviderResponseError`` precedent: ``category`` subsumes the raw
    status's programmatic value, and the final attempt's specifics are
    diagnostics — ``detail``'s job, folded in by the raise site.

    Attributes:
        category: The classification that exhausted its budget
            (TRANSIENT or RATE_LIMITED).
        attempt_count: Attempts made before giving up.
    """

    category: ResponseCategory | None
    attempt_count: int | None

    def __init__(
        self,
        *,
        provider: str | None = None,
        endpoint: str | None = None,
        detail: str | None = None,
        category: ResponseCategory | None = None,
        attempt_count: int | None = None,
    ) -> None:
        """
        Compose the head from the attempt count and category.

        Args:
            provider: Provider name, when known.
            endpoint: Endpoint name, when known.
            detail: Human-readable context; never read programmatically.
            category: The classification that exhausted its budget.
            attempt_count: Attempts made before giving up.
        """
        head: str = 'retry budget exhausted'
        if attempt_count is not None:
            head = f'{head} after {attempt_count} attempts'
        if category is not None:
            head = f'{head} ({category} responses)'
        super().__init__(head, provider=provider, endpoint=endpoint, detail=detail)
        self.category = category
        self.attempt_count = attempt_count
