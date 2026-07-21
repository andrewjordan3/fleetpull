# src/fleetpull/network/client/registry_base.py
"""The generic provider-keyed resource registry the concrete registries subclass.

One resource per provider, owned as a context manager: the publish-on-success
enter (resources built off to the side on an ``ExitStack`` and assigned only
after every provider's opened, so a mid-open failure unwinds what opened and
leaves the registry unentered), the closed-before-release exit (the instance
is marked closed before the stack unwinds, so a close error still leaves the
registry unusable rather than apparently-open), and the two-way lookup split
-- ``RuntimeError`` for a lookup outside the ``with`` block (a caller bug),
``ConfigurationError`` for an unconfigured provider while open -- are each
stated once here. ``ProviderClientRegistry`` (transport clients) and the
orchestrator's ``FetchPoolRegistry`` (fetch worker pools) are thin
subclasses supplying construction and the error nouns.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import ExitStack
from types import TracebackType
from typing import ClassVar, Self

from fleetpull.exceptions import ConfigurationError
from fleetpull.vocabulary import Provider

__all__: list[str] = ['ProviderResourceRegistry']


class ProviderResourceRegistry[ResourceT](ABC):
    """Owns one open resource per provider, keyed by ``Provider``.

    A resource-owning context manager: ``__enter__`` opens every configured
    provider's resource and returns self; ``__exit__`` releases them all.
    Subclasses bind the vocabulary (``_resource_noun`` for the unconfigured-
    provider error, ``_lookup_description`` for the not-open error), open one
    provider's resource in ``_open_resource``, and expose their named lookup
    over ``_resource_for``.

    Attributes:
        _resource_noun: The resource's error noun (e.g. ``'transport
            client'``), completing ``'no <noun> configured for provider'``.
        _lookup_description: The public lookup method's name (e.g.
            ``'client_for'``), naming the misuse in the not-open error.
    """

    _resource_noun: ClassVar[str]
    _lookup_description: ClassVar[str]

    def __init__(self, providers: Iterable[Provider]) -> None:
        """
        Args:
            providers: The providers to open a resource for on ``__enter__``.
                A provider absent here has no resource and is rejected by the
                lookup while the registry is open.

        Side Effects:
            None -- resources are opened on ``__enter__``, not here.
        """
        self._providers: tuple[Provider, ...] = tuple(providers)
        self._resources: dict[Provider, ResourceT] = {}
        self._stack: ExitStack = ExitStack()
        self._open: bool = False

    @abstractmethod
    def _open_resource(self, stack: ExitStack, provider: Provider) -> ResourceT:
        """Open one provider's resource, registering its release on ``stack``.

        Args:
            stack: The enter's unwind stack; register whatever must release
                on exit (or on a later provider's open failure) here.
            provider: The provider whose resource to open.

        Returns:
            The opened resource.
        """
        ...

    def __enter__(self) -> Self:
        """Open one resource per configured provider; publish only on full success.

        Builds the provider-to-resource map in a local dict and assigns it to
        the instance only after every resource has opened, so a mid-open
        failure unwinds the resources already opened and leaves the registry
        unentered (``_resources`` untouched, ``_open`` false).

        Side Effects:
            Opens one resource per provider via ``_open_resource``. On a later
            open failure, the earlier resources are released before
            propagating.
        """
        resources: dict[Provider, ResourceT] = {}
        with ExitStack() as stack:
            for provider in self._providers:
                resources[provider] = self._open_resource(stack, provider)
            self._stack = stack.pop_all()
        self._resources = resources
        self._open = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Release every provider's resource, forwarding the exit context.

        Forwards ``exc_*`` to the owned ``ExitStack`` so each resource's
        ``__exit__`` receives the exit context and the suppression decision
        passes through. The instance is marked closed before the resources
        are released, so a release error still leaves the registry unusable
        rather than apparently-open.

        Side Effects:
            Releases every resource opened in ``__enter__``.
        """
        self._open = False
        self._resources = {}
        return bool(self._stack.__exit__(exc_type, exc_value, traceback))

    def _resource_for(self, provider: Provider) -> ResourceT:
        """Return the provider's open resource -- the shared lookup body.

        Args:
            provider: The provider whose resource to return.

        Returns:
            The provider's open resource.

        Raises:
            RuntimeError: The registry is not open -- the lookup was called
                outside an active ``with`` block (a caller bug), kept distinct
                from a genuinely unconfigured provider.
            ConfigurationError: The registry is open but the provider has no
                configured resource.
        """
        if not self._open:
            raise RuntimeError(
                f'{type(self).__name__} is not open; call '
                f'{self._lookup_description} inside its `with` block'
            )
        resource = self._resources.get(provider)
        if resource is None:
            configured = ', '.join(sorted(p.value for p in self._resources)) or 'none'
            raise ConfigurationError(
                f'no {self._resource_noun} configured for provider',
                provider=provider.value,
                detail=f'configured providers: {configured}',
            )
        return resource
