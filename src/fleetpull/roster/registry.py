# src/fleetpull/roster/registry.py
"""The roster catalog: ``RosterRegistry``, ``RosterKey`` -> ``RosterDefinition``.

Holds the roster declarations and resolves a ``RosterKey`` to its ``RosterDefinition``
-- the source endpoint, column, and policy a refresh needs. The consuming endpoint
carries only the key; the coordinator asks the registry for the key's definition when
it refreshes. Construction rejects two definitions claiming the same key. Forward
lookup only for now; the by-source reverse index a multi-roster feeder refresh needs
joins when the harvest is built.
"""

from collections.abc import Iterable

from fleetpull.exceptions import ConfigurationError
from fleetpull.roster.definition import RosterDefinition
from fleetpull.roster.key import RosterKey

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
