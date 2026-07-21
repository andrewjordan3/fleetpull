# src/fleetpull/orchestrator/shape_resolution.py
"""The shared shape-to-driver seam: one ``RequestShape`` match, one driver out.

``resolve_request_driver`` is the single place a declared ``request_shape``
becomes a ``RequestDriver`` -- both composition roots call it (the
orchestration entry for sync, ``fetch`` for the in-memory verb), so a new
cardinality pattern is a new union member plus its arm here, never a new
field or a new branch anywhere else. The seam owns only the dispatch:
supplying roster members for the roster-backed shapes (``RosterFanOut`` /
``BatchedRosterFanOut``) -- registry lookup, refresh policy, store read,
the empty-roster guard -- stays with the caller, which feeds them in
through the ``RosterMemberSource`` callable. A stateless caller
(``fetch``) passes ``roster_members=None`` and every stateless shape
resolves; a roster-backed shape then fails loudly, because a roster is
durable operational state the stateless composition deliberately lacks.
"""

from collections.abc import Callable, Sequence
from typing import Protocol

from fleetpull.endpoints.shared import (
    BatchedRosterFanOut,
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
from fleetpull.roster import RosterKey
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


# The caller's roster half of a roster-backed resolution: handed the
# roster's key, it returns the refreshed membership (or raises the caller's
# own roster failure). Both roster-backed shapes name their roster with the
# same ``RosterKey``, so one key-shaped source serves them both -- one
# roster machinery path, no second seam, no synthetic shape. None marks a
# stateless caller with no roster state at all.
type RosterMemberSource = Callable[[RosterKey], Sequence[str]]


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
            (``RosterFanOut`` / ``BatchedRosterFanOut`` / ``ParamSweep``);
            never consulted for the single-chain shapes.
        roster_members: The caller's roster membership source, invoked only
            for the roster-backed shapes; ``None`` for a stateless caller
            with no roster state.

    Returns:
        The ``SingleRequestDriver`` for ``SingleFetch``; the
        ``BisectingWindowDriver`` for ``BisectedWindowFetch``; the
        ``FanOutRequestDriver`` over the roster's members for
        ``RosterFanOut``, over sorted comma-joined member batches for
        ``BatchedRosterFanOut``, or over the declared values
        (``member_key`` = ``param``) for ``ParamSweep`` -- the driver is
        member-agnostic, so every fanned shape shares it.

    Raises:
        ConfigurationError: The shape is roster-backed (``RosterFanOut`` /
            ``BatchedRosterFanOut``) and no roster source is available --
            the stateless-caller case.
        FleetpullError: Whatever the roster source raises resolving a
            roster-backed shape (an unregistered or empty roster, a
            cold-start refresh failure), propagated unswallowed.

    Side Effects:
        On the roster-backed paths: whatever the supplied source performs
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
            return FanOutRequestDriver(
                members=_require_roster_members(
                    definition, roster_members, fan_out.roster
                ),
                member_key=fan_out.member_key,
                fetch_pool=fetch_pools.pool_for(definition.provider),
            )
        case BatchedRosterFanOut() as batched:
            # The batched shape is transport packing over the plain roster
            # fan-out, so membership resolves through the identical source
            # call -- the same roster key -- and only then chunks into
            # comma-joined batch values. The driver stays member-agnostic:
            # each batch is simply one member string.
            members = _require_roster_members(
                definition, roster_members, batched.roster
            )
            return FanOutRequestDriver(
                members=_comma_joined_batches(members, batched.batch_size),
                member_key=batched.member_key,
                fetch_pool=fetch_pools.pool_for(definition.provider),
            )


def _require_roster_members(
    definition: EndpointDefinition[ResponseModel],
    roster_members: RosterMemberSource | None,
    roster: RosterKey,
) -> Sequence[str]:
    """Resolve a roster-backed shape's membership, refusing a stateless caller.

    Args:
        definition: The endpoint being resolved, for the error context.
        roster_members: The caller's roster membership source, or ``None``
            for a stateless caller with no roster state.
        roster: The roster the shape names -- a ``RosterFanOut``'s or a
            ``BatchedRosterFanOut``'s ``roster`` key alike.

    Returns:
        The refreshed membership, exactly as the source supplies it.

    Raises:
        ConfigurationError: No roster source is available -- the
            stateless-caller (in-memory ``fetch``) case.
        FleetpullError: Whatever the source raises (an unregistered or
            empty roster, a cold-start refresh failure), propagated
            unswallowed.

    Side Effects:
        Whatever the supplied source performs (a feeder listing and a
        store write when stale).
    """
    if roster_members is None:
        raise ConfigurationError(
            'no roster source for a roster fan-out endpoint',
            provider=definition.provider.value,
            endpoint=definition.name,
            detail=(
                f'the {roster.name!r} roster fan-out needs '
                f'durable roster state, which this stateless '
                f'composition (the in-memory fetch verb) deliberately '
                f'lacks; run it through the config-driven sync path'
            ),
        )
    return roster_members(roster)


def _comma_joined_batches(members: Sequence[str], batch_size: int) -> tuple[str, ...]:
    """Chunk members into sorted, comma-joined batch values, one per chain.

    Deterministic by construction: members sort before chunking, so
    identical rosters always produce identical batches regardless of the
    source's ordering. Pure -- no state, no side effects; the batch is
    transport packing only (records self-identify), so nothing here maps
    a member back to its batch.

    Args:
        members: The roster's member values, in any order. A member
            containing the join delimiter is rejected loudly: a comma
            inside one member would silently widen the batch on the wire
            (more ids than members -- past the API cap, or addressing an
            unintended asset), and no provider's roster ids carry commas.
        batch_size: The maximum members per batch; at least 1, enforced
            by ``BatchedRosterFanOut`` at declaration.

    Returns:
        The comma-joined batch strings, in sorted-member order -- fewer
        members than one batch yields a single batch.

    Raises:
        ConfigurationError: A member value contains a comma -- corrupt
            roster data surfaced loudly, never packed onto the wire.
    """
    comma_carriers = [member for member in members if ',' in member]
    if comma_carriers:
        raise ConfigurationError(
            'roster member contains the batch join delimiter',
            detail=(
                f'{len(comma_carriers)} member value(s) contain a comma '
                f'(first: {comma_carriers[0]!r}); a comma-joined batch '
                f'would carry more ids than members'
            ),
        )
    ordered = sorted(members)
    return tuple(
        ','.join(ordered[start : start + batch_size])
        for start in range(0, len(ordered), batch_size)
    )
