# src/fleetpull/network/client/registry.py
"""Provider-keyed registry of transport clients: one open pool per provider.

The seam between endpoint execution and provider transport identity. A
``TransportClient`` is provider-scoped (it carries a provider's ``ProviderProfile``
and owns that provider's pooled ``httpx.Client``); an endpoint run is
endpoint-scoped. This registry lets the run executor ask for the client of
``definition.provider`` without owning a single client or pretending one client can
authenticate every provider. It owns the clients' lifecycle -- open every configured
provider's client on enter, close every pool on exit -- and nothing else; it builds
no profiles and reads no credentials, which is the composition root's job. The
lifecycle machinery itself (publish-on-success enter, closed-before-release
exit, the RuntimeError-vs-ConfigurationError lookup split) is the generic
``ProviderResourceRegistry``'s (``registry_base.py``); this subclass supplies
client construction and the error nouns.

The one shared ``ClientRuntime`` passed to every client is what keeps cross-provider
quota enforced: every page attempt routes through that runtime's single
``RateLimiterRegistry`` (DESIGN §7, §14).
"""

from collections.abc import Mapping
from contextlib import ExitStack
from typing import ClassVar

from fleetpull.network.client.profile import ProviderProfile
from fleetpull.network.client.registry_base import ProviderResourceRegistry
from fleetpull.network.client.runtime import ClientRuntime
from fleetpull.network.client.transport import TransportClient
from fleetpull.vocabulary import Provider

__all__: list[str] = ['ProviderClientRegistry']


class ProviderClientRegistry(ProviderResourceRegistry[TransportClient]):
    """Owns one open ``TransportClient`` per provider, keyed by ``Provider``.

    A resource-owning context manager (the generic base's semantics).
    ``client_for`` returns a provider's client; use it only inside the
    ``with`` block::

        with ProviderClientRegistry(profiles, runtime) as clients:
            client = clients.client_for(definition.provider)
    """

    _resource_noun: ClassVar[str] = 'transport client'
    _lookup_description: ClassVar[str] = 'client_for'

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
        super().__init__(profiles)
        self._profiles: dict[Provider, ProviderProfile] = dict(profiles)
        self._runtime: ClientRuntime = runtime

    def _open_resource(self, stack: ExitStack, provider: Provider) -> TransportClient:
        """Open one provider's transport client (its pooled ``httpx.Client``).

        Args:
            stack: The enter's unwind stack; the client's pool close registers
                here.
            provider: The provider whose client to open.

        Returns:
            The provider's open ``TransportClient``.

        Side Effects:
            Constructs one pooled ``httpx.Client``.
        """
        return stack.enter_context(
            TransportClient(self._profiles[provider], self._runtime)
        )

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
        return self._resource_for(provider)
