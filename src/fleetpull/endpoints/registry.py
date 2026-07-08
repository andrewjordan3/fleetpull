# src/fleetpull/endpoints/registry.py
"""The endpoint catalog: ``EndpointRegistry`` and the discovery walk.

``EndpointRegistry`` is a dumb, immutable map from ``(provider, name)`` to the
endpoint's ``EndpointDefinition``. It answers ``get(provider, name)``; a duplicate
key at construction is a wiring bug and raises ``ConfigurationError``. It knows
nothing about providers, discovery, or config -- it is handed a bag of definitions
and indexes them.

``build_endpoint_registry`` is the one place endpoints are enumerated. It discovers
every endpoint leaf by walking the ``endpoints.<provider>`` packages (skipping
``shared``) for modules exposing a ``build_endpoint`` factory, injects each factory's
provider config by matching the factory's annotated config type against the supplied
configs, calls it, and indexes the results. Adding an endpoint is adding one leaf
module: no manifest, no registration, no provider list here.

``build_roster_registry`` is its sibling over the same walk: rosters are declared
as public module-level ``RosterDefinition`` constants beside their feeders, in
exactly the leaf modules the endpoint walk visits, so they are discovered the
same way (AUD-05's close -- no hand-maintained registration list, no per-provider
export to drift). Adding a roster is declaring one constant in its feeder's
module.

Discovery reaches the leaf modules dynamically rather than through each provider
package's face. That is a deliberate, named exception to the clause-3 face-routing
rule the import-discipline test enforces: the walk depends only on the
``build_endpoint`` contract, not on any specific module, so it is generic enumeration
rather than static coupling. The replacement guardrail is the structural contract
test (``tests/endpoints/test_endpoint_contract.py``): every endpoint leaf must expose
a callable ``build_endpoint`` taking one ``ProviderConfig`` subclass, so a typo or a
missing factory fails loudly there rather than silently vanishing from the catalog.
"""

import importlib
import pkgutil
from collections.abc import Callable, Iterable, Iterator
from types import ModuleType
from typing import cast, get_type_hints

import fleetpull.endpoints
from fleetpull.config import ProviderConfig
from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.exceptions import ConfigurationError
from fleetpull.model_contract import ResponseModel
from fleetpull.roster import RosterDefinition, RosterRegistry
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'EndpointRegistry',
    'build_endpoint_registry',
    'build_roster_registry',
]

_FACTORY_NAME: str = 'build_endpoint'
_SHARED_PACKAGE: str = 'shared'

_EndpointFactory = Callable[[ProviderConfig], EndpointDefinition[ResponseModel]]


class EndpointRegistry:
    """An immutable catalog mapping ``(provider, name)`` to its definition.

    Built once from a bag of definitions, it answers ``get(provider, name)``. The
    map is private and frozen at construction; a duplicate ``(provider, name)`` is a
    wiring bug and raises. It holds no provider knowledge and does no discovery.

    Args:
        definitions: The endpoint definitions to catalog; their ``(provider, name)``
            keys must be distinct.

    Raises:
        ConfigurationError: Two definitions share a ``(provider, name)`` key.
    """

    def __init__(
        self, definitions: Iterable[EndpointDefinition[ResponseModel]]
    ) -> None:
        by_key: dict[tuple[Provider, str], EndpointDefinition[ResponseModel]] = {}
        for definition in definitions:
            key = (definition.provider, definition.name)
            if key in by_key:
                raise ConfigurationError(
                    'duplicate endpoint definition',
                    provider=definition.provider.value,
                    endpoint=definition.name,
                    detail=f'endpoint {definition.name!r} is defined twice',
                )
            by_key[key] = definition
        self._by_key = by_key

    def get(self, provider: Provider, name: str) -> EndpointDefinition[ResponseModel]:
        """Return the definition for a ``(provider, name)`` key.

        Args:
            provider: The endpoint's provider.
            name: The endpoint's name (e.g. ``'vehicles'``).

        Returns:
            The endpoint's definition.

        Raises:
            ConfigurationError: No definition is registered for the key -- a consumer
                references an endpoint the catalog does not declare.
        """
        try:
            return self._by_key[(provider, name)]
        except KeyError:
            raise ConfigurationError(
                'unknown endpoint',
                provider=provider.value,
                endpoint=name,
                detail=f'no endpoint definition registered for {name!r}',
            ) from None


def build_endpoint_registry(configs: Iterable[ProviderConfig]) -> EndpointRegistry:
    """Discover every endpoint leaf, build its definition, and catalog them.

    Walks the ``endpoints.<provider>`` packages for ``build_endpoint`` factories,
    injects each factory's annotated provider config from ``configs`` (matched by
    exact type), calls it, and returns the populated registry.

    Args:
        configs: The resolved provider config instances, one per provider whose
            endpoints should be built. Matched to each factory by the factory's
            annotated parameter type.

    Returns:
        The populated ``EndpointRegistry``.

    Raises:
        ConfigurationError: A leaf exposes no callable ``build_endpoint``, a factory's
            parameter is not exactly one ``ProviderConfig`` subclass, or no supplied
            config matches a factory's annotated type.
    """
    config_by_type: dict[type[ProviderConfig], ProviderConfig] = {
        type(config): config for config in configs
    }
    definitions: list[EndpointDefinition[ResponseModel]] = []
    for module_name in _iter_endpoint_leaf_modules():
        module = importlib.import_module(module_name)
        factory = _required_factory(module, module_name)
        config = _config_for_factory(factory, module_name, config_by_type)
        definitions.append(factory(config))
    return EndpointRegistry(definitions)


def build_roster_registry() -> RosterRegistry:
    """Discover every declared roster and catalog them.

    The sibling of ``build_endpoint_registry``, sharing the same leaf walk: a
    roster is a public module-level ``RosterDefinition`` constant declared
    beside its feeder, so it is discovered rather than hand-listed. Takes no
    configs -- roster declarations are constants, not factories.

    Returns:
        The populated ``RosterRegistry``.

    Raises:
        ConfigurationError: Two collected definitions share a ``RosterKey``
            (e.g. a constant re-exported into a second leaf module), from
            ``RosterRegistry`` construction.
    """
    definitions: list[RosterDefinition] = []
    for module_name in _iter_endpoint_leaf_modules():
        module = importlib.import_module(module_name)
        definitions.extend(_module_roster_definitions(module))
    return RosterRegistry(definitions)


def _module_roster_definitions(module: ModuleType) -> list[RosterDefinition]:
    """The public module-level roster declarations of one endpoint leaf.

    Only non-underscore names register: an underscore-prefixed definition is
    file-private by the naming rule and stays out of the catalog.

    Args:
        module: The imported leaf module.

    Returns:
        The module's declared ``RosterDefinition`` constants, in definition
        order (module namespace order).
    """
    return [
        value
        for name, value in vars(module).items()
        if not name.startswith('_') and isinstance(value, RosterDefinition)
    ]


def _iter_endpoint_leaf_modules() -> Iterator[str]:
    """Yield the dotted name of every endpoint leaf module.

    Walks the ``endpoints`` package for provider subpackages (skipping ``shared`` and
    any non-package), then each provider package for leaf modules (skipping
    ``__init__``, which ``iter_modules`` omits, and any ``_``-prefixed private
    module). Every yielded module is an endpoint leaf obligated to expose
    ``build_endpoint``.
    """
    root_path = fleetpull.endpoints.__path__
    root_name = fleetpull.endpoints.__name__
    for provider in pkgutil.iter_modules(root_path):
        if not provider.ispkg or provider.name == _SHARED_PACKAGE:
            continue
        package_name = f'{root_name}.{provider.name}'
        package = importlib.import_module(package_name)
        for leaf in pkgutil.iter_modules(package.__path__):
            if leaf.ispkg or leaf.name.startswith('_'):
                continue
            yield f'{package_name}.{leaf.name}'


def _required_factory(module: ModuleType, module_name: str) -> _EndpointFactory:
    """Return the module's ``build_endpoint``, or raise if it is missing.

    Args:
        module: The imported leaf module.
        module_name: Its dotted name, for the error.

    Returns:
        The module's ``build_endpoint`` callable.

    Raises:
        ConfigurationError: The module exposes no callable ``build_endpoint``.
    """
    factory = getattr(module, _FACTORY_NAME, None)
    if not callable(factory):
        raise ConfigurationError(
            'endpoint leaf missing build_endpoint',
            detail=f'{module_name!r} must expose a callable {_FACTORY_NAME!r}',
        )
    return cast(_EndpointFactory, factory)


def _config_for_factory(
    factory: _EndpointFactory,
    module_name: str,
    config_by_type: dict[type[ProviderConfig], ProviderConfig],
) -> ProviderConfig:
    """Resolve the config instance a factory's annotation asks for.

    Inspects the factory's single parameter, requires its type to be a
    ``ProviderConfig`` subclass, and returns the matching supplied instance.

    Args:
        factory: The leaf's ``build_endpoint`` callable.
        module_name: Its dotted name, for errors.
        config_by_type: The supplied configs, keyed by exact type.

    Returns:
        The config instance matching the factory's annotated parameter type.

    Raises:
        ConfigurationError: The factory does not take exactly one parameter, its
            parameter is not a ``ProviderConfig`` subclass, or no supplied config
            matches its annotated type.
    """
    hints = get_type_hints(factory)
    hints.pop('return', None)
    if len(hints) != 1:
        raise ConfigurationError(
            'endpoint factory arity',
            detail=f'{module_name!r} build_endpoint must take exactly one '
            f'annotated parameter',
        )
    (annotation,) = hints.values()
    if not (isinstance(annotation, type) and issubclass(annotation, ProviderConfig)):
        raise ConfigurationError(
            'endpoint factory parameter type',
            detail=f'{module_name!r} build_endpoint must annotate a '
            f'ProviderConfig subclass',
        )
    try:
        return config_by_type[annotation]
    except KeyError:
        raise ConfigurationError(
            'no config supplied for endpoint',
            detail=f'{module_name!r} requires {annotation.__name__} but no '
            f'instance was supplied',
        ) from None
