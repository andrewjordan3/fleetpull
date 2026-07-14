# src/fleetpull/network/auth/strategies.py
"""The ``AuthStrategy`` implementations, beside the session manager.

``StaticHeaderAuth`` (Motive/Samsara static keys) and
``GeotabSessionAuth`` (GeoTab sessions) implement the ``AuthStrategy``
protocol declared in ``network/contract/auth.py``. They live here, in
``network/auth/``, because ``GeotabSessionAuth`` wraps the
``GeotabSessionManager`` it sits beside — keeping the implementations
out of the contract surface is what lets the surface stay free of any
``network/auth`` dependency.

Provider names appear only at the composition root that constructs
strategies; the client and everything downstream is provider-blind.
"""

import threading
from collections.abc import Mapping
from dataclasses import replace
from urllib.parse import SplitResult, urlsplit, urlunsplit

from pydantic import SecretStr

from fleetpull.network.auth.manager import GeotabSessionManager
from fleetpull.network.auth.models import GeotabSession
from fleetpull.network.contract import RequestSpec
from fleetpull.vocabulary import JsonValue

__all__: list[str] = ['GeotabSessionAuth', 'StaticHeaderAuth']


class StaticHeaderAuth:
    """
    Static API-key auth for Motive and Samsara.

    The composition root pre-formats the header value (e.g.
    ``'Bearer <token>'`` for Samsara) before wrapping it in
    ``SecretStr``; the secret is extracted only at the moment of use.
    """

    def __init__(self, header_name: str, header_value: SecretStr) -> None:
        """
        Args:
            header_name: The header carrying the credential.
            header_value: The pre-formatted credential value.
        """
        self._header_name: str = header_name
        self._header_value: SecretStr = header_value

    def prepare(self, spec: RequestSpec) -> RequestSpec:
        """Return a new spec with the credential header injected."""
        return spec.with_extra_headers(
            {self._header_name: self._header_value.get_secret_value()}
        )

    def on_auth_failure(self) -> bool:
        """A rejected static key cannot be fixed by retrying."""
        return False


class _ThreadLocalSession(threading.local):
    """Per-thread slot for the last session injected by prepare()."""

    last_prepared: GeotabSession | None = None


class GeotabSessionAuth:
    """
    Session auth for GeoTab: marries the session manager to the
    ``AuthStrategy`` protocol.

    The last-prepared session lives in a ``threading.local`` slot
    because the strategy instance is shared across worker threads.
    prepare → send → on_auth_failure always executes within one thread,
    but a plain instance attribute would let thread A's failure
    invalidate the fresher session thread B just prepared with,
    triggering a spurious refresh. The thread-local pins each failure
    to the session that actually failed.
    """

    def __init__(self, manager: GeotabSessionManager) -> None:
        """
        Args:
            manager: The single-flight session manager for this account.
        """
        self._manager: GeotabSessionManager = manager
        self._thread_local: _ThreadLocalSession = _ThreadLocalSession()

    def prepare(self, spec: RequestSpec) -> RequestSpec:
        """
        Inject session credentials into the JSON-RPC body and retarget
        the URL to the session's resolved host.

        Args:
            spec: A GeoTab JSON-RPC request spec; ``json_body`` is
                required.

        Returns:
            A new spec whose body carries ``params.credentials`` and
            whose URL host is the session's resolved host (scheme,
            path, and query untouched). The input spec and its body
            are never mutated.

        Raises:
            ValueError: If ``spec.json_body`` is None (every GeoTab
                request is a JSON-RPC POST with a body), or if the
                body's ``params`` exists but is not a mapping — both
                are programming errors in the endpoint layer.
        """
        # Validate before the session fetch: a body-less spec is a
        # programming error and must not trigger a real Authenticate
        # call (which would burn sessions against GeoTab's LRU cap).
        if spec.json_body is None:
            raise ValueError(
                'GeoTab requests require a JSON-RPC body; '
                'a body-less spec reaching GeoTab auth is a programming error'
            )
        session: GeotabSession = self._manager.get_session()

        # The strategy is the sole authority on credentials: an existing
        # 'credentials' key left by a caller is overwritten, never kept.
        credentials: dict[str, JsonValue] = {
            'database': session.database,
            'sessionId': session.session_id,
            'userName': session.username,
        }
        if 'params' not in spec.json_body:
            # Some JSON-RPC methods are parameterless apart from credentials.
            new_params: dict[str, JsonValue] = {'credentials': credentials}
        else:
            existing_params: JsonValue = spec.json_body['params']
            if not isinstance(existing_params, Mapping):
                raise ValueError(
                    f"json_body['params'] must be a mapping, "
                    f'got {type(existing_params).__name__}'
                )
            new_params = {**existing_params, 'credentials': credentials}
        new_body: dict[str, JsonValue] = {**spec.json_body, 'params': new_params}

        # Authenticate may have redirected the session to a different
        # server; every subsequent call must target it.
        url_parts: SplitResult = urlsplit(spec.url)
        retargeted_url: str = urlunsplit(
            url_parts._replace(netloc=session.resolved_host)
        )

        self._thread_local.last_prepared = session
        return replace(spec, url=retargeted_url, json_body=new_body)

    def on_auth_failure(self) -> bool:
        """
        Invalidate the session this thread last prepared with; one
        retry is worthwhile because fresh credentials are now cached.

        Returns:
            True, always — the session manager guarantees a usable
            replacement (or raises trying).

        Raises:
            RuntimeError: If no session was ever prepared on this
                thread — on_auth_failure without a prior prepare is a
                client-logic bug.
        """
        failed_session: GeotabSession | None = self._thread_local.last_prepared
        if failed_session is None:
            raise RuntimeError(
                'on_auth_failure called before any prepare on this thread'
            )
        self._manager.invalidate(failed_session)
        return True
