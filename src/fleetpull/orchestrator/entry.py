# src/fleetpull/orchestrator/entry.py
"""The orchestration entry: declarations in, run outcome out.

``run_endpoint`` is the caller boundary one layer above ``EndpointRunner``: it
resolves the endpoint's declared request driver and runs. The governing
principle: higher-level orchestrators and tools are polymorphic --
provider-agnostic and endpoint-agnostic. A caller invoking an endpoint never
knows or branches on the provider, whether the endpoint fans out, its sync
mode, its storage cell, or its record identity; every dispatch keys off
``EndpointDefinition`` declarations (``FanOutBinding`` and ``select_writer``
already state this for their seams -- this module extends it to driver
resolution). Driver resolution is therefore module-private: exposing a
resolve-driver step to callers would leak exactly the fan-out/single-fetch
distinction the declarations hide.

A ``fan_out=None`` definition gets the ``SingleRequestDriver``; the roster
machinery is never touched (unless the endpoint sources a roster -- below). A
declared binding resolves its ``RosterKey`` through the ``RosterRegistry``,
refreshes the membership via the coordinator -- which owns the entire
staleness policy, including best-effort degradation and the loud cold-start
failure; the entry never reasons about freshness -- then reads the members
from the store and fans out. An empty roster after the refresh raises
``ConfigurationError``, error-by-default (DESIGN section 13): a feeder that
listed nothing is a failure to surface, not an empty dataset to emit, and the
short-circuit keeps the writer's write-called-at-least-once precondition
intact (an ``allow_empty_roster`` escape joins ``FanOutBinding`` only when an
endpoint genuinely needs one).

The entry also owns the feeder tap (the other half of the coupling rule:
every execution of a feeder endpoint updates its rosters and records a run;
parquet is written only when the user asked for that endpoint). It reverse-
looks-up the rosters the definition sources (``RosterRegistry.sourced_by``);
when any exist, it installs a generic batch observer that collects each
sourced roster's distinct ``source_column`` values -- values only, never
frames, so memory stays bounded by the distinct-key count -- and, after the
run returns ``Executed``, hands each collected listing to the coordinator's
``apply_listing`` to reconcile unconditionally. On a failed run nothing is
applied; on ``CaughtUp`` nothing executed, so there is no listing to apply.
The runner stays roster-blind: it sees only the generic observer.

Stateful collaborators are narrow Protocols (``EndpointExecutor``,
``RosterRefresher``, ``RosterMembersReader``) the composition root satisfies
with the real runner, coordinator, and store; the ``RosterRegistry`` is the
immutable catalog passed concrete (the run-executor precedent, DESIGN
section 14).
"""

import logging
from collections.abc import Sequence
from typing import Protocol

import polars as pl

from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.exceptions import ConfigurationError
from fleetpull.model_contract import ResponseModel
from fleetpull.orchestrator.drivers import (
    FanOutRequestDriver,
    RequestDriver,
    SingleRequestDriver,
)
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.runner import BatchObserver
from fleetpull.records import extract_roster_members
from fleetpull.roster import RosterDefinition, RosterKey, RosterRegistry

__all__: list[str] = [
    'EndpointExecutor',
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
            ValueError: A sourced ``source_column`` is absent from the frame or
                holds a null (from ``extract_roster_members``) -- a wiring bug
                that fails the run loudly.
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
    roster_registry: RosterRegistry,
    roster_refresher: RosterRefresher,
    roster_members: RosterMembersReader,
) -> RunOutcome:
    """Run one endpoint through its declared request driver, tapping feeder runs.

    The provider- and endpoint-agnostic entry: dispatch keys off the
    definition's declared fields and the roster catalog only. ``fan_out=None``
    runs with the single-fetch driver; a declared binding is resolved to a
    refreshed roster membership and fanned out. When the endpoint sources any
    roster (``sourced_by``), the run is observed and -- on ``Executed`` -- each
    sourced roster is reconciled from the run's collected members, so every
    feeder execution updates its rosters (the coupling rule; the run row comes
    from the runner, and parquet was written because the user asked for this
    endpoint). An endpoint that neither fans out nor sources a roster touches
    no roster machinery at all.

    Args:
        definition: The endpoint to run, already resolved by the caller.
        runner: The run executor (``EndpointRunner``'s ``run`` surface).
        roster_registry: The roster catalog -- forward lookup for the fan-out
            binding, reverse lookup for the rosters this endpoint sources.
        roster_refresher: The roster-policy coordinator; owns staleness,
            degradation, cold-start, and reconciliation whole.
        roster_members: The stored-membership read (``RosterStore``'s
            ``read_members`` surface).

    Returns:
        The run outcome (``Executed`` or ``CaughtUp``), unchanged from the
        runner.

    Raises:
        ConfigurationError: The binding names an unregistered roster, or the
            roster is empty after the refresh (error-by-default -- a feeder
            that listed nothing is a failure to surface).
        FleetpullError: A cold-start roster refresh failed (propagated from
            the coordinator), or the run itself failed (propagated from the
            runner; a failed run applies nothing to any roster).

    Side Effects:
        Whatever the refresh and the run perform: network fetches, parquet
        writes, and state-store commits by the injected collaborators; on a
        successful feeder run, the sourced rosters' reconciled deltas.
    """
    driver = _resolve_driver(
        definition, roster_registry, roster_refresher, roster_members
    )
    sourced = roster_registry.sourced_by(definition.provider, definition.name)
    if not sourced:
        return runner.run(definition, driver)
    collector = _RosterMemberCollector(sourced)
    outcome = runner.run(definition, driver, collector.observe)
    match outcome:
        case Executed():
            for roster_definition in sourced:
                roster_refresher.apply_listing(
                    roster_definition, collector.members_for(roster_definition.key)
                )
        case CaughtUp():
            # Nothing executed, so nothing was listed -- an unexecuted run
            # must not reconcile (an empty "listing" would count absences).
            pass
    return outcome


def _resolve_driver(
    definition: EndpointDefinition[ResponseModel],
    roster_registry: RosterRegistry,
    roster_refresher: RosterRefresher,
    roster_members: RosterMembersReader,
) -> RequestDriver:
    """Resolve the definition's declared fan-out into a request driver.

    Module-private by design: the fan-out/single-fetch distinction is the
    entry's to hide, not the caller's to compose with.

    Args:
        definition: The endpoint whose ``fan_out`` declaration routes.
        roster_registry: Resolves the binding's ``RosterKey``.
        roster_refresher: Refreshes the membership when stale.
        roster_members: Reads the refreshed membership.

    Returns:
        The ``SingleRequestDriver`` for ``fan_out=None``; otherwise the
        ``FanOutRequestDriver`` over the roster's members with the binding's
        declared placeholder.

    Raises:
        ConfigurationError: The binding names an unregistered roster (from
            the registry), or the roster is empty after the refresh.
        FleetpullError: A cold-start refresh failure, propagated unswallowed
            from the coordinator.

    Side Effects:
        On the fan-out path: whatever the refresh performs (a feeder listing
        and a store write when stale).
    """
    binding = definition.fan_out
    if binding is None:
        return SingleRequestDriver()
    roster_definition = roster_registry.get(binding.roster)
    roster_refresher.refresh_if_stale(roster_definition)
    members = roster_members.read_members(binding.roster)
    if not members:
        raise ConfigurationError(
            'fan-out roster is empty',
            provider=definition.provider.value,
            endpoint=definition.name,
            detail=(
                f'roster {binding.roster.name!r} holds no members after refresh; '
                f'a fan-out over nothing is a failure to surface, not an empty '
                f'dataset to emit'
            ),
        )
    logger.debug(
        'fan-out resolved: endpoint=%s roster=%s members=%d',
        definition.name,
        binding.roster.name,
        len(members),
    )
    return FanOutRequestDriver(
        members=members, path_placeholder=binding.path_placeholder
    )
