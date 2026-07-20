# src/fleetpull/api/sync.py
"""``Sync``: the config-driven public verb (DESIGN section 10).

Constructed on a path to a YAML config; ``run()`` returns ``None`` and
signals failure by raising. Where ``fetch`` is the in-memory convenience
verb, ``Sync`` is the pipeline: it composes the full machinery -- state
database, stores, registries, clients, the run executor -- from one
validated ``FleetpullConfig`` and runs every selected endpoint.

Construction is validation only: the config loads (``from_yaml``), every
selected endpoint name is checked against the public catalog (the
validation deliberately absent from the config tier, which sits below
the catalog), and zero enabled providers is a ``ConfigurationError`` --
a sync that syncs nothing is a configuration failure to surface, not a
no-op. Nothing global mutates and nothing but the config file is read.

``run()`` applies the logging section first, then composes and executes.
The concurrency ladder (DESIGN section 7, completed 2026-07-20):
providers run concurrently, one queue worker each; within a provider,
endpoints run in two concurrent stages -- the selected feeders
(endpoints sourcing a roster, derived via ``sourced_by``, never a
user-facing key; feeders never cross providers) first, a barrier, then
the consumers -- and within a fan-out endpoint, units and members run
concurrently on that provider's fetch pool. Queue order -- feeders then
consumers, config order within each -- is exactly the order the retired
serial queue executed in, surviving as the reporting contract, not an
execution order. Endpoints commit independently: one endpoint's
operational failure (the ``FleetpullError`` family) is recorded while
its queue continues; any other exception is a bug that stops the
queue's unstarted work (in-flight siblings finish and commit) and
re-raises -- the first by queue order within a provider, the first by
provider order across providers -- once every queue has joined. A run
with failures ends by raising ``SyncFailuresError`` carrying them in
queue order within each provider, providers in config order. Only the
selected set runs: an unselected feeder is never run on a consumer's
behalf -- roster freshness stays the refresh coordinator's job at
fan-out time (single-flight per roster key).
"""

import logging
import threading
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from pydantic import SecretStr

from fleetpull.api.auth_ingress import (
    ProviderProfileContext,
    build_provider_profile,
)
from fleetpull.api.catalog import available_endpoints
from fleetpull.api.identity import EndpointIdentity
from fleetpull.config import (
    FleetpullConfig,
    GeotabAuthConfig,
    GeotabConfig,
    MotiveConfig,
    SamsaraConfig,
)
from fleetpull.endpoints import (
    EndpointRegistry,
    build_endpoint_registry,
    build_roster_registry,
)
from fleetpull.endpoints.shared import EndpointDefinition
from fleetpull.exceptions import (
    ConfigurationError,
    EndpointFailure,
    FleetpullError,
    SyncFailuresError,
)
from fleetpull.logger import setup_logger
from fleetpull.model_contract import ResponseModel
from fleetpull.network.client import (
    ClientRuntime,
    ProviderClientRegistry,
    ProviderProfile,
)
from fleetpull.network.limits import RateLimiterRegistry, rate_limits_from_configs
from fleetpull.orchestrator import (
    EndpointRunner,
    FetchPoolRegistry,
    RosterMachinery,
    RosterRefreshCoordinator,
    RunStateAccess,
    run_endpoint,
)
from fleetpull.roster import RosterRegistry
from fleetpull.state import (
    CursorStore,
    RosterStore,
    RunLedger,
    StateDatabase,
    WorkUnitStore,
    migrate_to_head,
)
from fleetpull.timing import SystemClock
from fleetpull.vocabulary import Provider

__all__: list[str] = ['Sync']

logger = logging.getLogger(__name__)

# The concrete provider-section union: new providers widen it as they port.
type _ProviderSection = MotiveConfig | GeotabConfig | SamsaraConfig

# The catalog as a lookup table: every public identity by registry key.
_CATALOG: dict[tuple[Provider, str], EndpointIdentity] = {
    (identity.provider, identity.name): identity for identity in available_endpoints()
}


class Sync:
    """
    The config-driven sync run: one YAML file in, datasets and state out.

    Construction validates; ``run()`` executes. One instance is one
    validated configuration; ``run()`` may be called repeatedly (each
    call is an independent, freshly composed run against the same
    config).
    """

    def __init__(self, config_path: Path | str) -> None:
        """Load and validate the configuration; compose nothing yet.

        Args:
            config_path: The YAML configuration file to load.

        Raises:
            ConfigurationError: The config file is missing, unparseable,
                or schema-invalid (from ``FleetpullConfig.from_yaml``);
                a selected endpoint name is not in the public catalog
                (naming the provider, the bad name, and the valid
                names); or zero providers are enabled -- a sync that
                syncs nothing is a configuration failure to surface.

        Side Effects:
            Reads the config file and the credential environment
            variables (via ``from_yaml``, which also logs the
            credential-without-endpoints WARNING). Nothing else.
        """
        self._config = FleetpullConfig.from_yaml(config_path)
        self._selection = _validated_selection(self._config)

    def run(self) -> None:
        """Run every selected endpoint; raise if any failed.

        Applies the logging section first (``setup_logger``), then
        composes the run from the validated config: the state database
        at the resolved ``state.database_path`` (migrated to head), the
        stores, the discovered endpoint and roster registries, the
        limiter registry from the precedence-resolved provider configs,
        one transport client per enabled provider through the auth
        ingress, and one fetch pool per enabled provider sized by its
        ``rate_limit.max_concurrency`` (the fan-out workers; shut down
        when the run ends, success or failure). Endpoints run as one
        staged queue per enabled provider, the queues concurrent (one
        worker thread each): stage one runs the selected feeders
        concurrently among themselves (derived from the roster
        bindings; config order within each stage), the stage join is
        the barrier no consumer crosses, and stage two runs the
        consumers concurrently. Endpoints commit independently:
        parquet and state land per endpoint as each finishes, so a
        sibling's failure never rolls anything back, in its own queue
        or another provider's. A non-``FleetpullError`` is a bug that
        stops its queue's unstarted endpoints (in-flight siblings
        finish and commit) while the other queues finish; after every
        queue joins, the first bug -- by queue order within a
        provider, provider order across providers -- re-raises.

        The run narrates at INFO: a start line naming the enabled
        providers, the validated selection, and the dataset root; a
        finish line with the succeeded/failed endpoint counts and the
        elapsed seconds (a ``monotonic_seconds`` delta on the run's own
        clock -- the whole run's wall clock, the slowest queue rather
        than the queues' sum -- emitted before any failure aggregate
        raises).

        Returns:
            None. Every selected endpoint ran and committed.

        Raises:
            SyncFailuresError: One or more endpoints failed with an
                operational (``FleetpullError``-family) error; carries
                the failures in queue order within each provider
                (feeders then consumers, config order within each),
                providers in config order. Successful siblings are
                already committed.
            FleetpullError: An operational failure outside any single
                endpoint's run (e.g. a cold-start roster refresh
                propagated by the entry) -- always one of the public
                subclasses (``ConfigurationError``,
                ``AuthenticationError``, ``RetriesExhaustedError``,
                ``ProviderResponseError``).

        Side Effects:
            Configures the ``fleetpull`` logger from the config's
            logging section; creates/migrates the SQLite state database;
            fetches over the network (one queue worker thread per
            enabled provider, one short-lived endpoint task pool per
            non-empty stage, plus that provider's fan-out fetch
            workers, all joined before this returns); writes parquet
            under ``storage.dataset_root``; records runs, cursors, and
            roster state.

        Scope: retrieval, dtype coercion, and light structural
        normalization only -- no cross-endpoint joins, no unified
        schema, no assumed end use (DESIGN section 10).
        """
        setup_logger(self._config.logging)
        clock = SystemClock()
        run_started = clock.monotonic_seconds()
        logger.info(
            'sync started: providers=[%s] endpoints=%d selection=[%s] dataset_root=%s',
            ', '.join(provider.value for provider, _ in self._enabled_providers()),
            len(self._selection),
            ', '.join(f'{provider.value}.{name}' for provider, name in self._selection),
            self._config.storage.dataset_root,
        )
        provider_configs = self._discovery_provider_configs()
        endpoint_registry = build_endpoint_registry(provider_configs)
        roster_registry = build_roster_registry()
        stages_by_provider = _staged_by_provider(self._selection, roster_registry)
        database = StateDatabase(_required_database_path(self._config))
        database.initialize()
        migrate_to_head(database)
        cursor_store = CursorStore(database, clock)
        run_ledger = RunLedger(database, clock)
        roster_store = RosterStore(database)
        unit_store = WorkUnitStore(database, clock)
        limiter_registry = RateLimiterRegistry(
            rate_limits_from_configs(provider_configs)
        )
        runtime = ClientRuntime(
            http_config=self._config.http,
            retry_config=self._config.retry,
            limiter_registry=limiter_registry,
        )
        profile_context = ProviderProfileContext(
            http_config=self._config.http,
            limiter_registry=limiter_registry,
            clock=clock,
        )
        fetch_workers = {
            provider: config.rate_limit.max_concurrency
            for provider, config in self._enabled_providers()
        }
        with (
            ProviderClientRegistry(
                self._provider_profiles(profile_context), runtime
            ) as clients,
            FetchPoolRegistry(fetch_workers) as fetch_pools,
        ):
            coordinator = RosterRefreshCoordinator(
                endpoint_registry, roster_store, run_ledger, clients, clock
            )
            runner = EndpointRunner(
                clients,
                RunStateAccess(
                    recorder=run_ledger, cursors=cursor_store, units=unit_store
                ),
                clock,
                self._config,
            )
            work = _ProviderQueueWork(
                registry=endpoint_registry,
                runner=runner,
                rosters=RosterMachinery(
                    registry=roster_registry,
                    refresher=coordinator,
                    members=roster_store,
                ),
                fetch_pools=fetch_pools,
            )
            # One worker per enabled provider; exiting the block joins them
            # all, so every queue has finished before anything raises. The
            # futures dict is keyed in the fixed provider-config order (the
            # enabled roll-call), which _collected_failures turns into the
            # documented cross-provider order.
            with ThreadPoolExecutor(
                max_workers=len(stages_by_provider),
                thread_name_prefix='fleetpull-sync',
            ) as queue_pool:
                queue_futures = {
                    provider: queue_pool.submit(
                        _run_provider_queue,
                        provider,
                        stages_by_provider[provider],
                        work,
                    )
                    for provider, _ in self._enabled_providers()
                }
        failures = _collected_failures(queue_futures)
        logger.info(
            'sync finished: succeeded=%d failed=%d elapsed_seconds=%.1f',
            len(self._selection) - len(failures),
            len(failures),
            clock.monotonic_seconds() - run_started,
        )
        if failures:
            raise SyncFailuresError(tuple(failures))

    def _enabled_providers(self) -> list[tuple[Provider, _ProviderSection]]:
        """The enabled providers, leaning on the validated enablement invariant.

        A validated config guarantees a provider with endpoints has a
        credential (``require_provider_credentials``), so enabled reduces to
        "endpoints non-empty". Typed as the concrete section union in a fixed
        provider order (Motive, GeoTab, Samsara -- ``ProvidersConfig`` field
        order).
        """
        return [
            (provider, section)
            for provider, section in _provider_sections(self._config)
            if section is not None and section.endpoints
        ]

    def _discovery_provider_configs(self) -> list[_ProviderSection]:
        """One config per provider package: the YAML section, or pure defaults.

        The discovery walk builds every leaf it finds and requires a config
        per provider package regardless of enablement, so a provider absent
        from the YAML is represented by its default-constructed config (the
        ``fetch`` precedent). Enabled providers contribute their YAML
        instances, so the limiter budgets derived from this list are the
        user's; a disabled provider's default config merely registers inert
        scopes nothing spends from.
        """
        defaults: dict[Provider, _ProviderSection] = {
            Provider.MOTIVE: MotiveConfig(),
            Provider.GEOTAB: GeotabConfig(),
            Provider.SAMSARA: SamsaraConfig(),
        }
        return [
            section if section is not None else defaults[provider]
            for provider, section in _provider_sections(self._config)
        ]

    def _provider_profiles(
        self, context: ProviderProfileContext
    ) -> dict[Provider, ProviderProfile]:
        """One client profile per enabled provider, through the auth ingress."""
        profiles: dict[Provider, ProviderProfile] = {}
        for provider, provider_config in self._enabled_providers():
            identity = _CATALOG[(provider, provider_config.endpoints[0])]
            credential = _required_credential(provider, provider_config)
            profiles[provider] = build_provider_profile(identity, credential, context)
        return profiles


def _provider_sections(
    config: FleetpullConfig,
) -> list[tuple[Provider, _ProviderSection | None]]:
    """Every provider section, present or not, in the fixed provider order.

    The single provider roll-call in this module: adding a provider means
    extending this list and nothing else here.
    """
    return [
        (Provider.MOTIVE, config.providers.motive),
        (Provider.GEOTAB, config.providers.geotab),
        (Provider.SAMSARA, config.providers.samsara),
    ]


def _validated_selection(config: FleetpullConfig) -> list[tuple[Provider, str]]:
    """The selected endpoints, catalog-validated, in config order.

    Args:
        config: The loaded configuration.

    Returns:
        Every enabled provider's selected ``(provider, name)`` keys, in the
        order the config lists them.

    Raises:
        ConfigurationError: A selected name is not in the public catalog, or
            zero providers are enabled.
    """
    selection: list[tuple[Provider, str]] = []
    for provider, section in _provider_sections(config):
        if section is None or not section.endpoints:
            continue
        for name in section.endpoints:
            if (provider, name) not in _CATALOG:
                valid_names = ', '.join(
                    sorted(
                        identity_name
                        for identity_provider, identity_name in _CATALOG
                        if identity_provider is provider
                    )
                )
                raise ConfigurationError(
                    'unknown endpoint name',
                    provider=provider.value,
                    endpoint=name,
                    detail=f'valid {provider.value} endpoints: {valid_names}',
                )
            selection.append((provider, name))
    if not selection:
        raise ConfigurationError(
            'nothing to sync',
            detail=(
                'no provider is enabled (a provider is enabled when its '
                'credential resolves and its endpoints list is non-empty)'
            ),
        )
    return selection


@dataclass(frozen=True, slots=True)
class _ProviderStages:
    """One provider's staged endpoint queue: feeders, then consumers.

    Queue order -- the order the failure contract reports in -- is the
    concatenation, feeders then consumers, each in config order: exactly
    the feeder-first order the retired serial queue executed in. Stage
    membership is feeder-hood (the endpoint sources a roster), never
    snapshot-hood, because the barrier between the stages exists for one
    dependency only: a consumer's fan-out must not race the reconcile of
    a roster a selected sibling feeder is about to refresh.

    Attributes:
        feeders: The endpoints sourcing any roster, in config order.
        consumers: Every other selected endpoint, in config order.
    """

    feeders: tuple[str, ...]
    consumers: tuple[str, ...]


def _staged_by_provider(
    selection: Sequence[tuple[Provider, str]], roster_registry: RosterRegistry
) -> dict[Provider, _ProviderStages]:
    """Carve the selection into each provider's two-stage queue, order kept.

    The pure staging step behind the intra-provider grain (DESIGN section
    7): per provider, the selected endpoints split into feeders --
    endpoints whose ``sourced_by`` is non-empty, derived from the roster
    bindings and never a user-facing key -- and consumers (everything
    else), each preserving config order. Single-level by construction: a
    feeder is snapshot-mode (the reconcile guards enforce it) and never
    consumes a roster itself, so feeder chains cannot exist today, and
    feeders never cross providers. An unselected feeder is never enlisted
    on a consumer's behalf -- roster freshness stays the refresh
    coordinator's job at fan-out time.

    Args:
        selection: The catalog-validated ``(provider, name)`` keys, in
            config order.
        roster_registry: The discovered roster catalog.

    Returns:
        Each selected provider's stages, keyed in first-appearance
        (provider-config) order.
    """
    feeders: dict[Provider, list[str]] = {}
    consumers: dict[Provider, list[str]] = {}
    for provider, name in selection:
        feeders.setdefault(provider, [])
        consumers.setdefault(provider, [])
        stage = feeders if roster_registry.sourced_by(provider, name) else consumers
        stage[provider].append(name)
    return {
        provider: _ProviderStages(
            feeders=tuple(feeders[provider]), consumers=tuple(consumers[provider])
        )
        for provider in feeders
    }


@dataclass(frozen=True, slots=True)
class _ProviderQueueWork:
    """The shared collaborators every provider queue runs its endpoints through.

    One instance serves every queue worker and endpoint task; the four ride
    as one parameter because they always travel together into
    ``run_endpoint`` (the bundle rule). Safe to share by construction: the
    registry is an immutable catalog, the runner keeps all per-run state
    local, the roster machinery's state lives in connection-per-operation
    stores (rosters are provider-scoped, so one queue's rosters are never
    another's, and concurrent consumers of one roster serialize through the
    refresh coordinator's per-key single-flight lock), and the fetch pools
    are per-provider.

    Attributes:
        registry: The discovered endpoint catalog (definition lookup).
        runner: The run executor, shared by every queue.
        rosters: The roster catalog, policy coordinator, and member read.
        fetch_pools: The per-provider fetch pools.
    """

    registry: EndpointRegistry
    runner: EndpointRunner
    rosters: RosterMachinery
    fetch_pools: FetchPoolRegistry


@dataclass(frozen=True, slots=True)
class _SkippedEndpoint:
    """The skip sentinel an endpoint task returns instead of running.

    Returned when the stop event was already set at task start: a sibling's
    bug stopped the queue, so unstarted work is skipped -- never run, never
    failed. Distinct from ``None`` (a clean run) so the task's contract
    states the skip explicitly rather than conflating it with success.
    """


class _StagedQueueRun:
    """The shared state one provider queue's staged endpoint tasks race on.

    Exactly one thing is cross-thread: the stop event a bug sets before it
    escapes through its future, checked by every task before it runs --
    in-flight siblings finish and commit; only unstarted work is skipped
    (the ``_CrewDrive`` stop semantics). Operational failures never set it:
    the queue continues, exactly as the serial queue did. Everything else
    stays local -- each stage's futures are drained in submission (queue)
    order after the stage's pool joins, so failures collect in queue order
    and the first bug re-raises deterministically by queue order, never by
    completion timing, with no locks and no re-sort. The class exists so
    the task function reads as the endpoint run it is, with the
    synchronization named rather than threaded through arguments.
    """

    def __init__(self, provider: Provider, work: _ProviderQueueWork) -> None:
        self._provider = provider
        self._work = work
        self._stop_running = threading.Event()

    def run_stage(self, stage_names: Sequence[str]) -> list[EndpointFailure]:
        """Run one stage's endpoints concurrently; join; report in queue order.

        An empty stage spawns nothing; a single-endpoint stage degenerates
        to the serial path's behavior on one short-lived task thread. The
        ``with`` block joins every task before the drain, so the barrier
        between the feeder stage and the consumer stage is this join.

        Args:
            stage_names: The stage's endpoint names, in queue order.

        Returns:
            The stage's operational failures, in queue order.

        Raises:
            Exception: The first bug by queue order, re-raised from its
                future after the join -- never a ``FleetpullError`` from
                ``run_endpoint`` (those are collected, not raised).

        Side Effects:
            Runs every endpoint not skipped by the stop event (network
            fetches, parquet writes, state commits); logs each operational
            failure at ERROR with its traceback.
        """
        if not stage_names:
            return []
        with ThreadPoolExecutor(
            max_workers=len(stage_names),
            thread_name_prefix=f'fleetpull-sync-{self._provider.value}',
        ) as stage_pool:
            endpoint_futures = [
                stage_pool.submit(
                    self._run_endpoint_task,
                    self._work.registry.get(self._provider, name),
                )
                for name in stage_names
            ]
        return [
            outcome
            for outcome in (future.result() for future in endpoint_futures)
            if isinstance(outcome, EndpointFailure)
        ]

    def _run_endpoint_task(
        self, definition: EndpointDefinition[ResponseModel]
    ) -> EndpointFailure | _SkippedEndpoint | None:
        """Run one resolved endpoint, unless a sibling bug stopped the queue.

        Args:
            definition: The endpoint to run, resolved at submission.

        Returns:
            ``None`` on a clean run, the ``EndpointFailure`` on an
            operational failure, or the skip sentinel when the stop event
            was set before this task ran.

        Side Effects:
            Renames the current thread to
            ``fleetpull-sync-<provider>-<endpoint>`` for log attribution;
            whatever ``run_endpoint`` performs; sets the stop event before
            a bug escapes through this task's future.
        """
        threading.current_thread().name = (
            f'fleetpull-sync-{self._provider.value}-{definition.name}'
        )
        if self._stop_running.is_set():
            logger.debug(
                'endpoint skipped: provider=%s endpoint=%s '
                '(a sibling bug stopped the queue)',
                self._provider.value,
                definition.name,
            )
            return _SkippedEndpoint()
        try:
            run_endpoint(
                definition,
                self._work.runner,
                self._work.rosters,
                self._work.fetch_pools,
            )
        except FleetpullError as failure:
            logger.exception(
                'endpoint failed: provider=%s endpoint=%s',
                self._provider.value,
                definition.name,
            )
            return EndpointFailure(self._provider.value, definition.name, failure)
        except Exception:
            # Stop BEFORE the bug escapes, so a sibling's next stop check
            # sees it; the bug itself re-raises at the queue-order drain.
            self._stop_running.set()
            raise
        return None


def _run_provider_queue(
    provider: Provider, stages: _ProviderStages, work: _ProviderQueueWork
) -> list[EndpointFailure]:
    """Run one provider's staged queue, collecting its operational failures.

    The per-provider queue worker: stage one runs the selected feeders
    concurrently among themselves (they reconcile distinct roster keys, so
    they cannot interfere; the set is usually zero or one), the stage join
    is the barrier no consumer crosses before every feeder has finished,
    and stage two runs the consumers concurrently. A ``FleetpullError``
    records an ``EndpointFailure`` and the queue continues; any other
    exception is a bug that stops the queue's unstarted endpoints and
    propagates through this worker's future -- a stage-one bug skips stage
    two entirely -- to re-raise after every queue has joined. The thread
    is renamed to the provider first, so any stack trace attributes
    cleanly.

    Args:
        provider: The queue's provider.
        stages: The provider's staged endpoints, in queue order.
        work: The shared collaborators the endpoints run through.

    Returns:
        The queue's failures, in queue order (feeders then consumers,
        config order within each).

    Side Effects:
        Renames the current thread to ``fleetpull-sync-<provider>``;
        spawns one short-lived endpoint task pool per non-empty stage;
        runs every endpoint (network fetches, parquet writes, state
        commits); logs each operational failure at ERROR with its
        traceback.
    """
    threading.current_thread().name = f'fleetpull-sync-{provider.value}'
    queue_run = _StagedQueueRun(provider, work)
    failures = queue_run.run_stage(stages.feeders)
    failures.extend(queue_run.run_stage(stages.consumers))
    return failures


def _collected_failures(
    queue_futures: dict[Provider, Future[list[EndpointFailure]]],
) -> list[EndpointFailure]:
    """Fold the joined queues into the documented two-level failure order.

    Iterates the futures in their key order -- provider-config order, the
    caller's contract -- flattening each queue's queue-order failures, so
    the aggregate reads queue order within a provider, provider-config
    order across providers. A queue that died on a bug re-raises it here
    (``Future.result``), and the iteration order makes the first bug by
    provider order win deterministically, never by completion timing. Every
    future is already done (the pool's ``with`` block joined the workers),
    so nothing here blocks.

    Args:
        queue_futures: One joined future per provider queue, keyed in
            provider-config order.

    Returns:
        Every queue's failures, in the two-level order.

    Raises:
        Exception: The first queue's bug in provider order, re-raised
            unchanged -- never a ``FleetpullError`` (those are collected,
            not raised, by the workers).
    """
    failures: list[EndpointFailure] = []
    for queue_future in queue_futures.values():
        failures.extend(queue_future.result())
    return failures


def _required_database_path(config: FleetpullConfig) -> Path:
    """The resolved state database path; a missing one is a wiring bug.

    ``from_yaml`` always resolves it (the root invariant), and ``Sync``
    only loads via ``from_yaml`` -- this narrows the field's optional
    type and trips loudly if that invariant is ever bypassed.
    """
    database_path = config.state.database_path
    if database_path is None:
        raise ConfigurationError(
            'state database path unresolved',
            detail='state.database_path is unset; load the config via from_yaml',
        )
    return database_path


def _required_credential(
    provider: Provider, config: _ProviderSection
) -> SecretStr | GeotabAuthConfig:
    """Narrow an enabled provider's credential; absence is a wiring bug.

    The enablement validator guarantees it for any validated config; this
    trips loudly if that invariant is ever bypassed by direct construction.
    The return is the ingress ``AuthInput`` shape for the section's provider:
    the ``SecretStr`` key for the static-key providers (Motive, Samsara),
    GeoTab's four-field credential whole (its password's ``SecretStr``
    passes straight through -- no unwrap/rewrap).
    """
    match config:
        case MotiveConfig() | SamsaraConfig():
            credential: SecretStr | GeotabAuthConfig | None = config.api_key
        case GeotabConfig():
            credential = config.auth
    if credential is None:
        raise ConfigurationError(
            'provider credential missing',
            provider=provider.value,
            detail='an enabled provider must carry a credential (validated at load)',
        )
    return credential
