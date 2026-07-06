# src/fleetpull/roster/registry.py
"""The roster catalog: ``RosterRegistry``, ``RosterKey`` -> ``RosterDefinition``.

Holds the roster declarations and resolves them both ways. Forward
(``get``): a ``RosterKey`` to its ``RosterDefinition`` -- the source endpoint,
column, and policy a refresh needs; the consuming endpoint carries only the
key, and the coordinator asks the registry for the key's definition when it
refreshes. Reverse (``sourced_by``): which roster definitions a feeder
``(provider, endpoint)`` sources -- the lookup the feeder tap reads so every
execution of a feeder endpoint reconciles its rosters. Construction rejects
two definitions claiming the same key.
"""

from collections.abc import Iterable

from fleetpull.exceptions import ConfigurationError
from fleetpull.roster.definition import RosterDefinition
from fleetpull.roster.key import RosterKey
from fleetpull.vocabulary import Provider

__all__: list[str] = ['RosterRegistry']


class RosterRegistry:
    """An immutable catalog mapping each ``RosterKey`` to its ``RosterDefinition``.

    Built once from the roster definitions, it answers ``get(key)``. The map is
    private and frozen at construction; a duplicate key is a wiring bug and raises.

    Args:
        definitions: The roster definitions to catalog; their keys must be distinct.

    Raises:
        ConfigurationError: Two definitions share a ``RosterKey`` -- a wiring bug.
    """

    def __init__(self, definitions: Iterable[RosterDefinition]) -> None:
        by_key: dict[RosterKey, RosterDefinition] = {}
        for definition in definitions:
            if definition.key in by_key:
                raise ConfigurationError(
                    'duplicate roster definition',
                    provider=definition.key.provider.value,
                    detail=f'roster {definition.key.name!r} is defined twice',
                )
            by_key[definition.key] = definition
        self._by_key: dict[RosterKey, RosterDefinition] = by_key

    def get(self, key: RosterKey) -> RosterDefinition:
        """Return the definition for a roster key.

        Args:
            key: The roster to resolve.

        Returns:
            The roster's definition (source endpoint, column, and policy).

        Raises:
            ConfigurationError: No definition is registered for ``key`` -- a consumer
                references a roster the catalog does not declare.
        """
        try:
            return self._by_key[key]
        except KeyError:
            raise ConfigurationError(
                'unknown roster',
                provider=key.provider.value,
                detail=f'no roster definition registered for {key.name!r}',
            ) from None

    def sourced_by(
        self, provider: Provider, endpoint: str
    ) -> tuple[RosterDefinition, ...]:
        """Return the roster definitions sourced by one feeder endpoint.

        The reverse lookup the feeder tap reads: which rosters must be
        reconciled when this ``(provider, endpoint)`` runs. A linear scan --
        the catalog holds a handful of definitions -- in registration order.

        Args:
            provider: The feeder's provider.
            endpoint: The feeder's endpoint name (e.g. ``'vehicles'``).

        Returns:
            The definitions whose ``source_endpoint`` is this endpoint; empty
            when the endpoint sources no roster.
        """
        return tuple(
            definition
            for definition in self._by_key.values()
            if definition.key.provider is provider
            and definition.source_endpoint == endpoint
        )
