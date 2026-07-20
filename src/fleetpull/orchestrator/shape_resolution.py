# src/fleetpull/orchestrator/shape_resolution.py
"""The shared shape-to-driver seam: one ``RequestShape`` match, one driver out.

``resolve_request_driver`` is the single place a declared ``request_shape``
becomes a ``RequestDriver`` -- both composition roots call it (the
orchestration entry for sync, ``fetch`` for the in-memory verb), so a new
cardinality pattern is a new union member plus its arm here, never a new
field or a new branch anywhere else. The seam owns only the dispatch:
supplying roster members for a ``RosterFanOut`` -- registry lookup, refresh
policy, store read, the empty-roster guard -- stays with the caller, which
feeds them in through the ``RosterMemberSource`` callable. A stateless
caller (``fetch``) passes ``roster_members=None`` and every stateless
shape resolves; a ``RosterFanOut`` then fails loudly, because a roster is
durable operational state the stateless composition deliberately lacks.
"""

from collections.abc import Callable, Sequence
from typing import Protocol

from fleetpull.endpoints.shared import (
    BisectedWindowFetch,
    EndpointDefinition,
    ParamSweep,
    RosterFanOut,
    SingleFetch,
)
from fleetpull.exceptions import ConfigurationError
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.bisection import BisectingWindowDriver
from fleetpull.orchestrator.drivers import (
    FanOutRequestDriver,
    RequestDriver,
    SingleRequestDriver,
)
from fleetpull.orchestrator.fanout import FetchPool
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'FetchPoolSource',
    'RosterMemberSource',
    'resolve_request_driver',
]


class FetchPoolSource(Protocol):
    """The pool-lookup surface the resolution needs (``FetchPoolRegistry``'s shape)."""

    def pool_for(self, provider: Provider) -> FetchPool:
        """Return the fetch pool for a provider."""
        ...


# The caller's roster half of a RosterFanOut resolution: handed the declared
# shape, it returns the refreshed membership (or raises the caller's own
# roster failure). None marks a stateless caller with no roster state at all.
type RosterMemberSource = Callable[[RosterFanOut], Sequence[str]]


def resolve_request_driver(
    definition: EndpointDefinition[ResponseModel],
    *,
    fetch_pools: FetchPoolSource,
    roster_members: RosterMemberSource | None,
) -> RequestDriver:
    """Resolve a definition's declared request shape into a request driver.

    Args:
        definition: The endpoint whose ``request_shape`` routes.
        fetch_pools: Supplies the provider's fetch pool for the fanned shapes
            (``RosterFanOut`` / ``ParamSweep``); never consulted for the
            single-chain shapes.
        roster_members: The caller's roster membership source, invoked only
            for a ``RosterFanOut``; ``None`` for a stateless caller with no
            roster state.

    Returns:
        The ``SingleRequestDriver`` for ``SingleFetch``; the
        ``BisectingWindowDriver`` for ``BisectedWindowFetch``; the
        ``FanOutRequestDriver`` over the roster's members for
        ``RosterFanOut``, or over the declared values (``member_key`` =
        ``param``) for ``ParamSweep`` -- the driver is member-agnostic, so
        both fanned shapes share it.

    Raises:
        ConfigurationError: The shape is a ``RosterFanOut`` and no roster
            source is available -- the stateless-caller case.
        FleetpullError: Whatever the roster source raises resolving a
            ``RosterFanOut`` (an unregistered or empty roster, a cold-start
            refresh failure), propagated unswallowed.

    Side Effects:
        On the roster fan-out path: whatever the supplied source performs
        (a feeder listing and a store write when stale).
    """
    match definition.request_shape:
        case SingleFetch():
            return SingleRequestDriver()
        case BisectedWindowFetch() as shape:
            return BisectingWindowDriver(shape=shape)
        case ParamSweep() as sweep:
            return FanOutRequestDriver(
                members=sweep.values,
                member_key=sweep.param,
                fetch_pool=fetch_pools.pool_for(definition.provider),
            )
        case RosterFanOut() as fan_out:
            if roster_members is None:
                raise ConfigurationError(
                    'no roster source for a RosterFanOut endpoint',
                    provider=definition.provider.value,
                    endpoint=definition.name,
                    detail=(
                        f'the {fan_out.roster.name!r} roster fan-out needs '
                        f'durable roster state, which this stateless '
                        f'composition (the in-memory fetch verb) deliberately '
                        f'lacks; run it through the config-driven sync path'
                    ),
                )
            return FanOutRequestDriver(
                members=roster_members(fan_out),
                member_key=fan_out.member_key,
                fetch_pool=fetch_pools.pool_for(definition.provider),
            )
