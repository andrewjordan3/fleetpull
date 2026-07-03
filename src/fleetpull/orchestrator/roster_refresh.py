# src/fleetpull/orchestrator/roster_refresh.py
"""The roster refresh coordinator: make a stale roster's membership current.

``RosterRefreshCoordinator.refresh_if_stale`` is the demand-driven refresh a
fan-out consumer calls before it reads a roster. Given the roster's
``RosterDefinition`` -- the caller resolves the ``RosterKey`` against the roster
registry, the same way the run executor is handed an already-resolved
``EndpointDefinition`` -- it asks the ledger when the feeder last succeeded, and only
if ``is_roster_stale`` says a refresh is due re-lists the feeder, reconciles the
listing against the stored roster, and applies the delta. Its only output is the
store; it never reads members, builds a fan-out driver, or runs an endpoint. Those
are the consume half, owned by the orchestration entry (``orchestrator/entry.py``).

The full-listing requirement lives here: a roster needs its feeder's complete
current membership each refresh, so the feeder must be a snapshot endpoint. The
coordinator resolves the definition's ``source_endpoint`` to the feeder binding and
guards that (``ConfigurationError`` on a non-snapshot feeder) before harvesting; the
harvester itself stays ``sync_mode``-blind.

Failure is best-effort with one loud exception. A harvest ``FleetpullError`` (the
feeder is unreachable, or a page fails to validate) or ``ValueError`` (a missing
source column or a null member) degrades to the existing roster -- a stale verdict
is a refresh *attempt*, not a barrier to the fan-out. The exception is cold start:
when the store holds no members for the key, there is nothing to fall back to and a
silent empty roster would fan out over nothing, so the failure re-raises. Wiring
errors -- no feeder registered for the ``source_endpoint``, a non-snapshot feeder --
raise outside the best-effort path; they are bugs, not transient misses.
``RosterStore.apply`` is one transaction, so a mid-refresh failure leaves the roster
untouched.

Collaborators are injected, not assembled: the pure ``reconcile`` /
``is_roster_stale`` are called directly; the ``EndpointRegistry`` is the immutable
catalog passed concrete; the stateful surfaces are narrow Protocols (``RosterAccess``,
``LastSuccessReader``, ``ClientSource``) the composition root satisfies with the real
store, ledger, and client registry. ``ClientSource`` mirrors the run executor's; it is
redefined here rather than imported to keep the two orchestrator modules independent.
"""

import logging
from datetime import datetime
from typing import Protocol

from fleetpull.endpoints import EndpointRegistry
from fleetpull.endpoints.shared import SnapshotMode
from fleetpull.exceptions import ConfigurationError, FleetpullError
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.drivers import SingleRequestDriver
from fleetpull.orchestrator.roster_harvest import harvest_roster_members
from fleetpull.roster import RosterDefinition, RosterKey
from fleetpull.state import RosterDelta, is_roster_stale, reconcile
from fleetpull.timing import Clock
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'ClientSource',
    'LastSuccessReader',
    'RosterAccess',
    'RosterRefreshCoordinator',
]

logger = logging.getLogger(__name__)


class RosterAccess(Protocol):
    """The roster store surface the coordinator needs (a subset of RosterStore)."""

    def read_counts(self, key: RosterKey) -> dict[str, int]:
        """Return the roster as ``{member: absence_count}``."""
        ...

    def apply(self, key: RosterKey, delta: RosterDelta) -> None:
        """Apply a reconciliation delta in one transaction."""
        ...


class LastSuccessReader(Protocol):
    """The feeder's last-success read the coordinator needs (a subset of RunLedger)."""

    def last_success_at(self, provider: Provider, endpoint: str) -> datetime | None:
        """Return the feeder's latest successful run end, or ``None``."""
        ...


class ClientSource(Protocol):
    """The client-lookup surface the coordinator needs (a subset of the registry)."""

    def client_for(self, provider: Provider) -> TransportClient:
        """Return the open transport client for a provider."""
        ...


class RosterRefreshCoordinator:
    """Refresh a roster's stored membership when its feeder listing has gone stale.

    Built once with the endpoint catalog and the store / ledger / client surfaces, it
    answers ``refresh_if_stale(definition)``. It holds no per-refresh state and opens
    no clients -- the injected client source hands it the feeder provider's open
    client.

    Args:
        endpoint_registry: Resolves the feeder's ``(provider, name)`` to its binding.
        store: The roster store (read counts, apply a delta).
        ledger: The feeder's last-success read, for the staleness decision.
        client_source: The open transport client per provider.
        clock: The clock supplying ``now`` for the staleness decision.
    """

    def __init__(
        self,
        endpoint_registry: EndpointRegistry,
        store: RosterAccess,
        ledger: LastSuccessReader,
        client_source: ClientSource,
        clock: Clock,
    ) -> None:
        self._endpoint_registry = endpoint_registry
        self._store = store
        self._ledger = ledger
        self._client_source = client_source
        self._clock = clock

    def refresh_if_stale(self, definition: RosterDefinition) -> None:
        """Re-list the feeder and reconcile the roster if it is stale.

        Returns early when the roster is fresh. When stale, harvests the feeder's
        complete membership and applies the reconciliation; a harvest failure
        degrades to the existing roster unless the store is empty (cold start), where
        it re-raises.

        Args:
            definition: The roster to refresh -- its key, feeder ``source_endpoint``
                and ``source_column``, and staleness / eviction policy. The caller
                resolved it from a ``RosterKey`` against the roster registry.

        Raises:
            ConfigurationError: No feeder is registered for ``source_endpoint``, or the
                resolved feeder is not a snapshot endpoint -- wiring bugs, raised
                loudly.
            FleetpullError: The harvest failed on a cold start (no existing roster to
                fall back to); the underlying failure propagates.
            ValueError: The harvest failed on a cold start with a missing source
                column or a null member; the underlying failure propagates.

        Side Effects:
            On a due, successful refresh: issues the feeder's request chain and writes
            the reconciled delta to the store. Otherwise touches nothing.
        """
        provider = definition.key.provider
        last_success = self._ledger.last_success_at(
            provider, definition.source_endpoint
        )
        now = self._clock.now_utc()
        if not is_roster_stale(last_success, now, definition.max_age):
            return
        feeder = self._endpoint_registry.get(provider, definition.source_endpoint)
        if not isinstance(feeder.sync_mode, SnapshotMode):
            raise ConfigurationError(
                'roster feeder is not a snapshot endpoint',
                provider=provider.value,
                endpoint=definition.source_endpoint,
                detail='a roster source must be a full-listing (snapshot) endpoint',
            )
        client = self._client_source.client_for(provider)
        current = self._store.read_counts(definition.key)
        try:
            listed = harvest_roster_members(
                feeder, SingleRequestDriver(), client, definition.source_column
            )
        except (FleetpullError, ValueError) as failure:
            if not current:
                raise
            logger.warning(
                'roster %s/%s refresh failed; keeping %d existing members: %s',
                provider.value,
                definition.key.name,
                len(current),
                failure,
            )
            return
        self._store.apply(
            definition.key,
            reconcile(current, listed, definition.eviction_threshold),
        )
