# src/fleetpull/orchestrator/roster_refresh.py
"""The roster refresh coordinator: make a stale roster's membership current.

The coupling rule this module upholds (with the orchestration entry's feeder
tap): **every execution of a feeder endpoint updates its rosters and records a
run in the ledger**; parquet is written only when the user asked for that
endpoint. A coordinator harvest is such an execution -- it fetches, reconciles
the roster, and records a ``runs`` row for ``(provider, source_endpoint)`` --
so ``RunLedger.last_success_at`` is a sound staleness key in both directions:
a harvest is visible to the next staleness check, and a runner-driven feeder
run (which the entry taps into ``apply_listing``) never leaves the roster
behind the ledger. A run row certifies execution of the endpoint, not parquet
freshness.

``refresh_if_stale`` is the demand-driven refresh a fan-out consumer calls
before it reads a roster. Given the roster's ``RosterDefinition`` -- the
caller resolves the ``RosterKey`` against the roster registry, the same way
the run executor is handed an already-resolved ``EndpointDefinition`` -- it
asks the ledger when the feeder last succeeded, and only if a refresh is due
(``is_roster_stale``, or an empty stored roster, which is stale regardless of
the ledger verdict -- ledger history may predate the harvest/ledger coupling)
re-lists the feeder, reconciles the listing against the stored roster, applies
the delta, and completes the run. ``apply_listing`` is the feeder-tap handoff
and the reconcile choke point: the entry collects a successful feeder run's
listed members and hands them here, and the harvest routes its own listing
through the same method -- so the reconcile guard (a roster is never
reconciled to empty; an empty listing is a failed refresh, not a membership
fact) covers both routes once. Staleness gates only whether the coordinator
*initiates* a harvest, never whether an executed run's non-empty listing is
applied; the run row on the tap path was already recorded by the run
executor.

The full-listing requirement lives here: a roster needs its feeder's complete
current membership each refresh, so the feeder must be a snapshot endpoint. The
coordinator resolves the definition's ``source_endpoint`` to the feeder binding and
guards that (``ConfigurationError`` on a non-snapshot feeder) before harvesting; the
harvester itself stays ``sync_mode``-blind.

``refresh_if_stale`` is single-flight per roster key (added with the
intra-provider grain, DESIGN section 7, 2026-07-20): concurrent consumers of
one stale roster -- both cold-starting with their feeder unselected -- would
each harvest, wasting quota and recording duplicate ledger snapshot runs.
The coordinator therefore holds a per-``RosterKey`` lock across the method's
whole body (an outer lock guards get-or-create of the per-key locks), so the
second entrant re-runs the freshness check under the lock and returns early
on the now-fresh roster. Holding the lock across the harvest network call is
intended -- the waiting consumer needs the membership before it can fan out
-- and no lock nests inside (the store and ledger are
connection-per-operation), so there is no deadlock surface. Locks are
per-key: distinct rosters refresh concurrently.

Failure is best-effort with one loud exception. A harvest ``FleetpullError`` (the
feeder is unreachable, or a page fails to validate) or ``ValueError`` (a missing
source column) marks the run failed and degrades to the existing
roster -- a stale verdict is a refresh *attempt*, not a barrier to the fan-out. The
exception is cold start: when the store holds no members for the key, there is
nothing to fall back to and a silent empty roster would fan out over nothing, so the
failure re-raises (still marking the run failed). Wiring errors -- no feeder
registered for the ``source_endpoint``, a non-snapshot feeder -- raise before any
run row is opened; they are bugs, not executions. The crash order inside a
successful refresh mirrors the run executor's output-first discipline:
``RosterStore.apply`` (one transaction) lands before ``complete_run``, so a crash
between the two leaves the roster current and the run merely ``running`` -- the
next check re-harvests idempotently, never the reverse mode where a fresh ledger
masks a stale roster.

Collaborators are injected, not assembled: the pure ``reconcile`` /
``is_roster_stale`` are called directly; the ``EndpointRegistry`` is the immutable
catalog passed concrete; the stateful surfaces are narrow Protocols (``RosterAccess``,
``FeederRunLedger``, ``ClientSource``) the composition root satisfies with the real
store, ledger, and client registry. ``ClientSource`` mirrors the run executor's; it is
redefined here rather than imported to keep the two orchestrator modules independent.
"""

import logging
import threading
from datetime import datetime
from typing import Protocol

from fleetpull.endpoints import EndpointRegistry
from fleetpull.endpoints.shared import SnapshotMode
from fleetpull.exceptions import (
    ConfigurationError,
    FleetpullError,
    ProviderResponseError,
)
from fleetpull.network.client import TransportClient
from fleetpull.orchestrator.drivers import SingleRequestDriver
from fleetpull.orchestrator.roster_harvest import harvest_roster_members
from fleetpull.roster import RosterDefinition, RosterKey
from fleetpull.state import RosterDelta, is_roster_stale, reconcile
from fleetpull.timing import Clock
from fleetpull.vocabulary import Provider

__all__: list[str] = [
    'ClientSource',
    'FeederRunLedger',
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


class FeederRunLedger(Protocol):
    """The ledger surface the coordinator needs (a subset of ``RunLedger``).

    ``last_success_at`` keys the staleness decision; the run-recording trio
    makes a coordinator harvest visible to that same key -- every execution of
    a feeder endpoint records a run, so the freshness signal and the freshness
    events cannot diverge.
    """

    def last_success_at(self, provider: Provider, endpoint: str) -> datetime | None:
        """Return the feeder's latest successful run end, or ``None``."""
        ...

    def start_snapshot_run(self, provider: Provider, endpoint: str) -> int:
        """Open a snapshot run for a harvest and return its id."""
        ...

    def complete_run(self, run_id: int, *, row_count: int) -> None:
        """Close a run as succeeded with its row count."""
        ...

    def fail_run(self, run_id: int, *, error_detail: str) -> None:
        """Close a run as failed with an error detail."""
        ...


class ClientSource(Protocol):
    """The client-lookup surface the coordinator needs (a subset of the registry)."""

    def client_for(self, provider: Provider) -> TransportClient:
        """Return the open transport client for a provider."""
        ...


class RosterRefreshCoordinator:
    """Refresh a roster's stored membership when its feeder listing has gone stale.

    Built once with the endpoint catalog and the store / ledger / client surfaces, it
    answers ``refresh_if_stale(definition)``. Beyond the per-key single-flight
    locks (the coordinator is the stateful orchestrator here, so the lock map
    lives on it) it holds no per-refresh state and opens no clients -- the
    injected client source hands it the feeder provider's open client.

    Args:
        endpoint_registry: Resolves the feeder's ``(provider, name)`` to its binding.
        store: The roster store (read counts, apply a delta).
        ledger: The feeder's ledger surface -- the last-success read for the
            staleness decision, and the run recording a harvest performs.
        client_source: The open transport client per provider.
        clock: The clock supplying ``now`` for the staleness decision.
    """

    def __init__(
        self,
        endpoint_registry: EndpointRegistry,
        store: RosterAccess,
        ledger: FeederRunLedger,
        client_source: ClientSource,
        clock: Clock,
    ) -> None:
        self._endpoint_registry = endpoint_registry
        self._store = store
        self._ledger = ledger
        self._client_source = client_source
        self._clock = clock
        self._key_locks_guard = threading.Lock()
        self._key_locks: dict[RosterKey, threading.Lock] = {}

    def refresh_if_stale(self, definition: RosterDefinition) -> None:
        """Re-list the feeder, reconcile the roster, and record the run, if stale.

        Single-flight per roster key: the whole body runs under the key's
        lock, so of two concurrent consumers hitting the same stale roster,
        exactly one harvests -- the second re-runs the freshness check under
        the lock and returns early on the now-fresh roster. The lock is held
        across the harvest network call deliberately (the waiting consumer
        needs the membership before it can fan out), and distinct keys never
        contend.

        Returns early when the roster is fresh -- non-empty and inside the
        staleness bound. An empty stored roster is stale regardless of the
        ledger verdict (ledger history may predate the harvest/ledger
        coupling, and a fresh ledger must not mask a roster with nothing to
        fan out over). When a refresh is due, the harvest is an execution of
        the feeder endpoint: it opens a snapshot run, harvests the complete
        membership, applies the reconciliation, and completes the run -- store
        before ledger, so a crash between the two re-harvests rather than
        masking. A harvest failure marks the run failed and degrades to the
        existing roster, unless the store is empty (cold start), where it
        re-raises.

        Args:
            definition: The roster to refresh -- its key, feeder ``source_endpoint``
                and ``source_column``, and staleness / eviction policy. The caller
                resolved it from a ``RosterKey`` against the roster registry.

        Raises:
            ConfigurationError: No feeder is registered for ``source_endpoint``, or the
                resolved feeder is not a snapshot endpoint -- wiring bugs, raised
                loudly before any run row is opened.
            FleetpullError: The harvest failed on a cold start (no existing roster to
                fall back to); the underlying failure propagates.
            ValueError: The harvest failed on a cold start with a missing source
                column; the underlying failure propagates.

        Side Effects:
            Holds the roster's single-flight lock for the duration. On a due
            refresh: issues the feeder's request chain and records a run in
            the ledger; on success, also writes the reconciled delta to the
            store. Otherwise touches nothing.
        """
        with self._refresh_lock(definition.key):
            self._refresh_if_due(definition)

    def _refresh_lock(self, key: RosterKey) -> threading.Lock:
        """Get or create the roster's single-flight lock.

        Args:
            key: The roster whose lock to return (hashable frozen identity).

        Returns:
            The per-key lock, created on first use under the outer guard.
        """
        with self._key_locks_guard:
            return self._key_locks.setdefault(key, threading.Lock())

    def _refresh_if_due(self, definition: RosterDefinition) -> None:
        """The refresh body ``refresh_if_stale`` runs under the key's lock."""
        provider = definition.key.provider
        current = self._store.read_counts(definition.key)
        last_success = self._ledger.last_success_at(
            provider, definition.source_endpoint
        )
        now = self._clock.now_utc()
        if current and not is_roster_stale(last_success, now, definition.max_age):
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
        logger.info(
            'roster refresh started: provider=%s roster=%s feeder=%s members_held=%d',
            provider.value,
            definition.key.name,
            definition.source_endpoint,
            len(current),
        )
        run_id = self._ledger.start_snapshot_run(provider, definition.source_endpoint)
        try:
            listed = harvest_roster_members(
                feeder, SingleRequestDriver(), client, definition.source_column
            )
            # Routed through the shared reconcile body so its guard (a
            # roster is never reconciled to empty) covers the harvest inside
            # this same failed-refresh path: an empty listing marks the run
            # failed and degrades exactly like a failed HTTP refresh. The
            # key's single-flight lock is already held here, so the locked
            # ``apply_listing`` wrapper would deadlock.
            self._reconcile_listing(definition, listed)
        except (FleetpullError, ValueError) as failure:
            self._fail_run_safely(run_id, failure)
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
        # A harvest run's row count is the distinct-member count of the
        # listing -- the rows this execution produced for its consumer.
        self._ledger.complete_run(run_id, row_count=len(listed))
        logger.info(
            'roster refreshed: provider=%s roster=%s members=%d',
            provider.value,
            definition.key.name,
            len(listed),
        )

    def apply_listing(self, definition: RosterDefinition, listed: set[str]) -> None:
        """Reconcile an executed feeder run's listed membership into the store.

        The feeder tap's route into the reconcile choke point
        (``_reconcile_listing``): the orchestration entry hands a successful
        runner-driven feeder run's distinct ``source_column`` values here.
        Runs under the roster key's single-flight lock -- the same lock
        ``refresh_if_stale`` holds around its harvest -- so every reconcile
        for one key serializes, whichever route wrote it: two
        read-reconcile-write sequences on one roster can never interleave
        and corrupt absence counts. Records no ledger row: the tap route's
        run was recorded by the run executor.

        Args:
            definition: The sourced roster -- its key and eviction policy.
            listed: The feeder run's complete listed membership.

        Raises:
            ProviderResponseError: ``listed`` is empty (the reconcile guard);
                the store is untouched.

        Side Effects:
            Holds the roster's single-flight lock for the duration; writes
            the reconciled delta to the store (one transaction).
        """
        with self._refresh_lock(definition.key):
            self._reconcile_listing(definition, listed)

    def _reconcile_listing(
        self, definition: RosterDefinition, listed: set[str]
    ) -> None:
        """The reconcile body both write routes run under the key's lock.

        The reconcile guard lives here so both routes -- the feeder tap
        (``apply_listing``) and the coordinator's own harvest
        (``_refresh_if_due``, which already holds the lock) -- are covered
        once: **a roster is never reconciled to empty**. An empty listing --
        the provider returned nothing, or every record's member value
        filtered out -- is a failed refresh, not a membership fact:
        reconciling it would mass-increment absence counts and, with an
        eviction threshold, evict the entire roster through systematic
        provider garbage. The prior roster stays intact and the caller's
        failed-refresh semantics apply (the harvest degrades exactly like a
        failed HTTP refresh; the tap propagates, failing the endpoint
        loudly). A non-empty listing is applied whole -- staleness gates
        only whether the coordinator *initiates* a harvest, never whether an
        executed run's complete listing is reconciled.

        Args:
            definition: The sourced roster -- its key and eviction policy.
            listed: The feeder run's complete listed membership.

        Raises:
            ProviderResponseError: ``listed`` is empty (the reconcile guard);
                the store is untouched.

        Side Effects:
            Writes the reconciled delta to the store (one transaction).
        """
        if not listed:
            raise ProviderResponseError(
                provider=definition.key.provider.value,
                endpoint=definition.source_endpoint,
                detail=(
                    f'feeder listed no members for roster '
                    f'{definition.key.name!r}; a roster is never reconciled '
                    f'to empty -- the prior membership stands'
                ),
            )
        current = self._store.read_counts(definition.key)
        self._store.apply(
            definition.key,
            reconcile(current, listed, definition.eviction_threshold),
        )

    def _fail_run_safely(self, run_id: int, error: Exception) -> None:
        """Record the harvest run failed without masking the original error.

        The run executor's stance: ``fail_run`` touches SQLite, which can
        itself fail; that secondary failure must not replace the harvest
        failure driving the degrade-or-reraise decision. Log it and move on.

        Args:
            run_id: The harvest run to mark failed.
            error: The harvest failure, recorded as the detail.

        Side Effects:
            Records the run failed; on a recording failure, logs and swallows it.
        """
        try:
            self._ledger.fail_run(run_id, error_detail=str(error))
        except Exception:
            logger.exception(
                'failed to record harvest run %s as failed after an earlier error',
                run_id,
            )
