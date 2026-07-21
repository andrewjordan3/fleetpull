# src/fleetpull/orchestrator/entry.py
"""The orchestration entry: declarations in, run outcome out.

``run_endpoint`` is the caller boundary one layer above ``EndpointRunner``: it
resolves the endpoint's declared request driver and runs. The governing
principle: higher-level orchestrators and tools are polymorphic --
provider-agnostic and endpoint-agnostic. A caller invoking an endpoint never
knows or branches on the provider, its request shape, its sync mode, its
storage cell, or its record identity; every dispatch keys off
``EndpointDefinition`` declarations (the ``RequestShape`` union and
``select_writer`` already state this for their seams -- this module extends
it to driver resolution). The shape-to-driver dispatch itself lives on the
shared seam (``shape_resolution.resolve_request_driver``, which ``fetch``
also calls); what this entry owns is the roster half it feeds that seam: a
roster-backed shape -- ``RosterFanOut``, or the ``BatchedRosterFanOut``
that packs one into comma-joined batches -- resolves its ``RosterKey``
through the ``RosterRegistry``,
refreshes the membership via the coordinator -- which owns the entire
staleness policy, including best-effort degradation and the loud cold-start
failure; the entry never reasons about freshness -- then reads the members
from the store. An empty roster after the refresh raises
``ConfigurationError``, error-by-default (DESIGN section 13): a feeder that
listed nothing is a failure to surface, not an empty dataset to emit, and the
short-circuit keeps the writer's write-called-at-least-once precondition
intact (an ``allow_empty_roster`` escape joins ``RosterFanOut`` only when an
endpoint genuinely needs one). Every non-roster-backed shape touches no
roster machinery at all (unless the endpoint sources a roster -- below).

The entry also owns the feeder tap (the other half of the coupling rule:
every execution of a feeder endpoint updates its rosters and records a run;
parquet is written only when the user asked for that endpoint). It reverse-
looks-up the rosters the definition sources (``RosterRegistry.sourced_by``);
when any exist, it installs a generic batch observer that collects each
sourced roster's distinct ``source_column`` values -- values only, never
frames, so memory stays bounded by the distinct-key count -- and, after the
run returns ``Executed``, hands each collected listing to the coordinator's
``apply_listing``, whose reconcile guard rejects an empty listing loudly (a
roster is never reconciled to empty; the failure propagates and fails the
endpoint while the prior membership stands). On a failed run nothing is
applied; on ``CaughtUp`` nothing executed, so there is no listing to apply.
A sourced definition that is not snapshot-mode is rejected before anything
runs -- ``reconcile`` is only correct over a complete listing, which only a
snapshot-mode feeder produces, so a watermark-mode source is a wiring bug
(the same guard the refresh coordinator applies on its harvest route). The
runner stays roster-blind: it sees only the generic observer.

Stateful collaborators are narrow Protocols (``EndpointExecutor``,
``RosterRefresher``, ``RosterMembersReader``, and the seam's
``FetchPoolSource``) the composition root satisfies with the real runner,
coordinator, store, and fetch-pool registry; the ``RosterRegistry`` is the
immutable catalog passed concrete (the run-executor precedent, DESIGN
section 14). The three roster collaborators always travel together, so they
ride as one ``RosterMachinery`` bundle (the bundle rule).
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial
from typing import Protocol

import polars as pl

from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.exceptions import ConfigurationError
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.drivers import RequestDriver
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.roster_harvest import require_snapshot_feeder
from fleetpull.orchestrator.shape_resolution import (
    FetchPoolSource,
    resolve_request_driver,
)
from fleetpull.orchestrator.streaming import BatchObserver
from fleetpull.records import extract_roster_members
from fleetpull.roster import RosterDefinition, RosterKey, RosterRegistry

__all__: list[str] = [
    'EndpointExecutor',
    'RosterMachinery',
    'RosterMembersReader',
    'RosterRefresher',
    'run_endpoint',
]

logger = logging.getLogger(__name__)


class EndpointExecutor(Protocol):
    """The run surface the entry needs (a subset of ``EndpointRunner``)."""

    def run(
        self,
        definition: EndpointDefinition[ResponseModel],
        driver: RequestDriver,
        observer: BatchObserver | None = None,
    ) -> RunOutcome:
        """Run one endpoint to completion with the resolved driver."""
        ...


class RosterRefresher(Protocol):
    """The roster-policy surface the entry needs (``RosterRefreshCoordinator``'s shape)."""

    def refresh_if_stale(self, definition: RosterDefinition) -> None:
        """Re-list the roster's feeder and reconcile the membership if stale."""
        ...

    def apply_listing(self, definition: RosterDefinition, listed: set[str]) -> None:
        """Reconcile an executed feeder run's listed membership into the store."""
        ...


class RosterMembersReader(Protocol):
    """The member-read surface the entry needs (a subset of ``RosterStore``)."""

    def read_members(self, key: RosterKey) -> list[str]:
        """Return the roster's members, ascending; empty when none stored."""
        ...


@dataclass(frozen=True, slots=True)
class RosterMachinery:
    """The three roster collaborators a run consults, bundled.

    They always travel together -- the fan-out resolution reads all three and
    the feeder tap reads the catalog -- so they ride as one parameter (the
    bundle rule). The composition root builds it once per run from the
    discovered catalog, the refresh coordinator, and the roster store.

    Attributes:
        registry: The immutable roster catalog -- forward lookup for a
            fan-out binding, reverse lookup for the rosters a definition
            sources.
        refresher: The roster-policy coordinator; owns staleness,
            degradation, cold-start, and reconciliation whole.
        members: The stored-membership read surface.
    """

    registry: RosterRegistry
    refresher: RosterRefresher
    members: RosterMembersReader


class _RosterMemberCollector:
    """Collects each sourced roster's distinct members from observed batches.

    The feeder tap's accumulator: for every roster the running endpoint
    sources, it extracts the distinct ``source_column`` values from each
    post-validation frame the runner hands the observer. Values only, never
    frames -- memory stays bounded by the distinct-key count, not the run's
    row count.
    """

    def __init__(self, sourced: Sequence[RosterDefinition]) -> None:
        self._sourced = sourced
        self._members: dict[RosterKey, set[str]] = {
            definition.key: set() for definition in sourced
        }

    def observe(self, frame: pl.DataFrame) -> None:
        """Fold one post-validation frame's members into each sourced roster.

        Args:
            frame: A validated, flattened batch frame from the run.

        Raises:
            ValueError: A sourced ``source_column`` is absent from the frame
                (from ``extract_roster_members``) -- a wiring bug that fails
                the run loudly. Null and empty-string values do not raise; the
                extractor filters them loudly.
        """
        for definition in self._sourced:
            self._members[definition.key] |= extract_roster_members(
                frame, definition.source_column
            )

    def members_for(self, key: RosterKey) -> set[str]:
        """Return the collected membership for one sourced roster."""
        return self._members[key]


def run_endpoint(
    definition: EndpointDefinition[ResponseModel],
    runner: EndpointExecutor,
    rosters: RosterMachinery,
    fetch_pools: FetchPoolSource,
) -> RunOutcome:
    """Run one endpoint through its declared request driver, tapping feeder runs.

    The provider- and endpoint-agnostic entry: dispatch keys off the
    definition's declared ``request_shape`` (via the shared shape-resolution
    seam) and the roster catalog only. The roster-backed shapes
    (``RosterFanOut`` and ``BatchedRosterFanOut``) are fed a refreshed
    roster membership and fanned out over the provider's fetch pool;
    every other shape resolves with no roster machinery touched. When
    the endpoint sources any roster (``sourced_by``), the run is observed
    and -- on ``Executed`` -- each sourced roster is reconciled from the run's
    collected members, so every feeder execution updates its rosters (the
    coupling rule; the run row comes from the runner, and parquet was written
    because the user asked for this endpoint). An endpoint that neither fans
    out over a roster nor sources one touches no roster machinery at all.

    Args:
        definition: The endpoint to run, already resolved by the caller.
        runner: The run executor (``EndpointRunner``'s ``run`` surface).
        rosters: The roster catalog, policy coordinator, and stored-membership
            read, bundled.
        fetch_pools: The per-provider fetch pools (``FetchPoolRegistry``'s
            ``pool_for`` surface); consulted only on the fanned shapes.

    Returns:
        The run outcome (``Executed`` or ``CaughtUp``), unchanged from the
        runner.

    Raises:
        ConfigurationError: The shape names an unregistered roster; the
            roster is empty after the refresh (error-by-default -- a feeder
            that listed nothing is a failure to surface); or the definition
            sources a roster but is not snapshot-mode (the feeder-mode guard:
            reconcile is only correct over a complete listing, which only a
            snapshot feeder produces).
        FleetpullError: A cold-start roster refresh failed (propagated from
            the coordinator), or the run itself failed (propagated from the
            runner; a failed run applies nothing to any roster).

    Side Effects:
        Whatever the refresh and the run perform: network fetches, parquet
        writes, and state-store commits by the injected collaborators; on a
        successful feeder run, the sourced rosters' reconciled deltas.
    """
    sourced = rosters.registry.sourced_by(definition.provider, definition.name)
    if sourced:
        # The shared guard covers both reconcile routes: here the tap route,
        # and the refresh coordinator's harvest route. A wiring bug, not a
        # degradable condition.
        require_snapshot_feeder(definition, definition.name)
    driver = _resolve_driver(definition, rosters, fetch_pools)
    if not sourced:
        return runner.run(definition, driver)
    collector = _RosterMemberCollector(sourced)
    outcome = runner.run(definition, driver, collector.observe)
    match outcome:
        case Executed():
            for roster_definition in sourced:
                rosters.refresher.apply_listing(
                    roster_definition, collector.members_for(roster_definition.key)
                )
        case CaughtUp():
            # Nothing executed, so nothing was listed -- an unexecuted run
            # must not reconcile (an empty "listing" would count absences).
            pass
    return outcome


def _resolve_driver(
    definition: EndpointDefinition[ResponseModel],
    rosters: RosterMachinery,
    fetch_pools: FetchPoolSource,
) -> RequestDriver:
    """Resolve the definition's declared request shape into a request driver.

    A thin composition over the shared seam
    (``shape_resolution.resolve_request_driver``): the shape-to-driver
    dispatch is the seam's; what the entry contributes is the roster member
    source -- the stateful half only this composition has. Module-private by
    design: the shape distinctions are the entry's to hide, not the caller's
    to compose with.

    Args:
        definition: The endpoint whose ``request_shape`` routes.
        rosters: Resolves a ``RosterFanOut``'s ``RosterKey``, refreshes the
            membership when stale, and reads it back.
        fetch_pools: Supplies the provider's fetch pool for the fanned shapes.

    Returns:
        The seam's resolved driver.

    Raises:
        ConfigurationError: The shape names an unregistered roster (from
            the registry), or the roster is empty after the refresh.
        FleetpullError: A cold-start refresh failure, propagated unswallowed
            from the coordinator.

    Side Effects:
        On the roster fan-out path: whatever the refresh performs (a feeder
        listing and a store write when stale).
    """
    return resolve_request_driver(
        definition,
        fetch_pools=fetch_pools,
        roster_members=partial(_refreshed_roster_members, definition, rosters),
    )


def _refreshed_roster_members(
    definition: EndpointDefinition[ResponseModel],
    rosters: RosterMachinery,
    roster: RosterKey,
) -> Sequence[str]:
    """Resolve a named roster's refreshed membership -- the seam's feed.

    The roster machinery whole: registry lookup, the coordinator's refresh
    (staleness policy included), the store read, and the empty-roster guard.

    Args:
        definition: The endpoint being run, for the error context.
        rosters: The roster catalog, policy coordinator, and stored-membership
            read, bundled.
        roster: The roster the declared shape names.

    Returns:
        The roster's members, ascending, never empty.

    Raises:
        ConfigurationError: The shape names an unregistered roster (from the
            registry), or the roster is empty after the refresh
            (error-by-default -- a feeder that listed nothing is a failure to
            surface, not an empty dataset to emit).
        FleetpullError: A cold-start refresh failure, propagated unswallowed
            from the coordinator.

    Side Effects:
        Whatever the refresh performs (a feeder listing and a store write
        when stale).
    """
    roster_definition = rosters.registry.get(roster)
    rosters.refresher.refresh_if_stale(roster_definition)
    members = rosters.members.read_members(roster)
    if not members:
        raise ConfigurationError(
            'fan-out roster is empty',
            provider=definition.provider.value,
            endpoint=definition.name,
            detail=(
                f'roster {roster.name!r} holds no members after refresh; '
                f'a fan-out over nothing is a failure to surface, not an empty '
                f'dataset to emit'
            ),
        )
    logger.debug(
        'fan-out resolved: endpoint=%s roster=%s members=%d',
        definition.name,
        roster.name,
        len(members),
    )
    return members
