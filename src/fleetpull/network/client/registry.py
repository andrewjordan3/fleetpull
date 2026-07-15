# src/fleetpull/network/client/registry.py
"""Provider-keyed registry of transport clients: one open pool per provider.

The seam between endpoint execution and provider transport identity. A
``TransportClient`` is provider-scoped (it carries a provider's ``ProviderProfile``
and owns that provider's pooled ``httpx.Client``); an endpoint run is
endpoint-scoped. This registry lets the run executor ask for the client of
``definition.provider`` without owning a single client or pretending one client can
authenticate every provider. It owns the clients' lifecycle -- open every configured
provider's client on enter, close every pool on exit -- and nothing else; it builds
no profiles and reads no credentials, which is the composition root's job.

The one shared ``ClientRuntime`` passed to every client is what keeps cross-provider
quota enforced: every page attempt routes through that runtime's single
``RateLimiterRegistry`` (DESIGN §7, §14).
"""

from collections.abc import Mapping
from contextlib import ExitStack
from types import TracebackType
from typing import Self

from fleetpull.exceptions import ConfigurationError
from fleetpull.network.client.profile import ProviderProfile
from fleetpull.network.client.runtime import ClientRuntime
from fleetpull.network.client.transport import TransportClient
from fleetpull.vocabulary import Provider

__all__: list[str] = ['ProviderClientRegistry']


class ProviderClientRegistry:
    """Owns one open ``TransportClient`` per provider, keyed by ``Provider``.

    A resource-owning context manager. ``__enter__`` opens a client for every
    provider in the profile map (each construction opens that provider's connection
    pool) and returns self; ``__exit__`` closes every pool. ``client_for`` returns a
    provider's client; use it only inside the ``with`` block::

        with ProviderClientRegistry(profiles, runtime) as clients:
            client = clients.client_for(definition.provider)

    Failure modes are handled cleanly:

    - If one provider's client fails to open, the clients already opened are closed
      and the registry is left unentered. The client map is built off to the side
      and published only on full success, so a failed ``__enter__`` never leaves a
      half-open, half-closed map behind.
    - ``client_for`` outside an open ``with`` block raises ``RuntimeError`` -- using
      a resource-owning context manager before entering (or after exiting) it is a
      caller bug, kept distinct from a genuinely unconfigured provider, which is a
      ``ConfigurationError``. The two are separable because the registry tracks
      whether it is open, so a registry configured with zero providers is open and
      empty (``ConfigurationError`` on any lookup), not closed.
    """

    def __init__(
        self,
        profiles: Mapping[Provider, ProviderProfile],
        runtime: ClientRuntime,
    ) -> None:
        """
        Args:
            profiles: The per-provider auth/classifier bundle for each configured
                provider. A provider absent here has no client and is rejected by
                ``client_for`` while the registry is open.
            runtime: The one process-global transport runtime shared by every
                client (its limiter registry enforces cross-provider quota).

        Side Effects:
            None -- clients are opened on ``__enter__``, not here.
        """
        self._profiles: dict[Provider, ProviderProfile] = dict(profiles)
        self._runtime: ClientRuntime = runtime
        self._clients: dict[Provider, TransportClient] = {}
        self._stack: ExitStack = ExitStack()
        self._open: bool = False

    def __enter__(self) -> Self:
        """Open one client per configured provider; publish only on full success.

        Builds the provider-to-client map in a local dict and assigns it to the
        instance only after every client has opened, so a mid-open failure unwinds
        the pools already opened and leaves the registry unentered (``_clients``
        untouched, ``_open`` false).

        Side Effects:
            Constructs one pooled ``httpx.Client`` per provider. On a later
            construction failure, the earlier pools are closed before propagating.
        """
        clients: dict[Provider, TransportClient] = {}
        with ExitStack() as stack:
            for provider, profile in self._profiles.items():
                clients[provider] = stack.enter_context(
                    TransportClient(profile, self._runtime)
                )
            self._stack = stack.pop_all()
        self._clients = clients
        self._open = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Close every provider's connection pool, forwarding the exit context.

        Forwards ``exc_*`` to the owned ``ExitStack`` so each client's ``__exit__``
        receives the exit context and the suppression decision passes through (no
        client suppresses today, so exceptions propagate). The instance is marked
        closed before the pools are released so a close error still leaves the
        registry unusable rather than apparently-open.

        Side Effects:
            Closes every ``TransportClient`` opened in ``__enter__``.
        """
        self._open = False
        self._clients = {}
        return bool(self._stack.__exit__(exc_type, exc_value, traceback))

    def client_for(self, provider: Provider) -> TransportClient:
        """Return the transport client for a provider.

        Args:
            provider: The provider whose client to return (e.g.
                ``definition.provider``).

        Returns:
            The provider's open ``TransportClient``.

        Raises:
            RuntimeError: The registry is not open -- ``client_for`` was called
                outside an active ``with`` block (a caller bug).
            ConfigurationError: The registry is open but the provider has no
                configured client.
        """
        if not self._open:
            raise RuntimeError(
                'ProviderClientRegistry is not open; call client_for inside its '
                '`with` block'
            )
        client = self._clients.get(provider)
        if client is None:
            configured = ', '.join(sorted(p.value for p in self._clients)) or 'none'
            raise ConfigurationError(
                'no transport client configured for provider',
                provider=provider.value,
                detail=f'configured providers: {configured}',
            )
        return client
