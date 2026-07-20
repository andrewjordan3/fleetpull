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
Endpoints run as one serial queue per enabled provider, providers
concurrent (DESIGN section 7, activated 2026-07-20): the feeder-first
order derived from the roster bindings -- never a user-facing key -- is
partitioned stably by provider, so nothing reorders within a provider
(feeders never cross providers), and the finer concurrency grain -- the
fan-out within one endpoint, on that provider's fetch pool -- is
unchanged. Endpoints commit independently: one endpoint's operational
failure (the ``FleetpullError`` family) is recorded while its queue
continues; any other exception is a bug that stops that provider's
queue while the other queues finish, re-raised -- the first by provider
order -- once every queue has joined. A run with failures ends by
raising ``SyncFailuresError`` carrying them in run order within each
provider, providers in config order. Only the selected set runs: an
unselected feeder is never run on a consumer's behalf -- roster
freshness stays the refresh coordinator's job at fan-out time.
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
from fleetpull.exceptions import (
    ConfigurationError,
    EndpointFailure,
    FleetpullError,
    SyncFailuresError,
)
from fleetpull.logger import setup_logger
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
        serial queue per enabled provider -- feeders before their
        consumers within it (derived from the roster bindings; config
        order within ties) -- with the queues concurrent, one worker
        thread each, and commit independently: parquet and state land
        per endpoint as each finishes, so a sibling's failure never
        rolls anything back, in its own queue or another provider's. A
        non-``FleetpullError`` is a bug that stops its provider's queue
        (the remaining endpoints in that queue are not run) while the
        other queues finish; after every queue joins, the first bug by
        provider order re-raises.

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
                the failures in run order within each provider,
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
            enabled provider plus that provider's fan-out fetch
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
        ordered = _feeders_first(self._selection, roster_registry)
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
        queues = _partitioned_by_provider(ordered)
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
                max_workers=len(queues), thread_name_prefix='fleetpull-sync'
            ) as queue_pool:
                queue_futures = {
                    provider: queue_pool.submit(
                        _run_provider_queue, provider, queues[provider], work
                    )
                    for provider, _ in self._enabled_providers()
                }
        failures = _collected_failures(queue_futures)
        logger.info(
            'sync finished: succeeded=%d failed=%d elapsed_seconds=%.1f',
            len(ordered) - len(failures),
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


def _feeders_first(
    selection: list[tuple[Provider, str]], roster_registry: RosterRegistry
) -> list[tuple[Provider, str]]:
    """Order the selection so roster feeders run before their consumers.

    Derived from the roster bindings via ``sourced_by`` -- never a
    user-facing key: an endpoint that sources any roster ranks ahead of
    endpoints that source none, and the sort is stable, so config order
    stands within each rank. Single-level by construction: a feeder is
    snapshot-mode (the reconcile guards enforce it) and consumers are
    windowed, so feeder chains cannot exist today.

    Args:
        selection: The catalog-validated ``(provider, name)`` keys.
        roster_registry: The discovered roster catalog.

    Returns:
        The selection, feeders first, otherwise in the given order.
    """

    def _rank(key: tuple[Provider, str]) -> int:
        provider, name = key
        return 0 if roster_registry.sourced_by(provider, name) else 1

    return sorted(selection, key=_rank)


def _partitioned_by_provider(
    ordered: Sequence[tuple[Provider, str]],
) -> dict[Provider, list[str]]:
    """Partition the feeder-first ordering into per-provider queues, stably.

    A stable partition of the already-derived ordering -- never a
    re-derivation: each provider's queue is exactly its subsequence of
    ``ordered``, so feeder-first order holds within every queue (feeders
    never cross providers). Keys land in first-appearance order; the
    cross-provider order the failure contract documents is imposed by the
    caller, which keys the queue futures in provider-config order.

    Args:
        ordered: The feeder-first ``(provider, name)`` sequence.

    Returns:
        Each provider's endpoint names, in within-provider run order.
    """
    queues: dict[Provider, list[str]] = {}
    for provider, name in ordered:
        queues.setdefault(provider, []).append(name)
    return queues


@dataclass(frozen=True, slots=True)
class _ProviderQueueWork:
    """The shared collaborators every provider queue runs its endpoints through.

    One instance serves every queue worker; the four ride as one parameter
    because they always travel together into ``run_endpoint`` (the bundle
    rule). Safe to share by construction: the registry is an immutable
    catalog, the runner keeps all per-run state local, the roster
    machinery's state lives in connection-per-operation stores (and rosters
    are provider-scoped, so one queue's rosters are never another's), and
    the fetch pools are per-provider.

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


def _run_provider_queue(
    provider: Provider, endpoint_names: Sequence[str], work: _ProviderQueueWork
) -> list[EndpointFailure]:
    """Run one provider's queue serially, collecting its operational failures.

    The per-provider worker: endpoints run in the queue's feeder-first order
    on this one thread, exactly as the whole selection once ran serially. A
    ``FleetpullError`` records an ``EndpointFailure`` and the queue
    continues; any other exception is a bug that stops this queue -- the
    remaining endpoints are not run -- and propagates through the worker's
    future, to re-raise after every queue has joined. The thread is renamed
    to the provider first, so any stack trace attributes cleanly.

    Args:
        provider: The queue's provider.
        endpoint_names: The provider's endpoints, in run order.
        work: The shared collaborators the endpoints run through.

    Returns:
        The queue's failures, in run order.

    Side Effects:
        Renames the current thread to ``fleetpull-sync-<provider>``; runs
        every endpoint (network fetches, parquet writes, state commits);
        logs each operational failure at ERROR with its traceback.
    """
    threading.current_thread().name = f'fleetpull-sync-{provider.value}'
    failures: list[EndpointFailure] = []
    for name in endpoint_names:
        definition = work.registry.get(provider, name)
        try:
            run_endpoint(definition, work.runner, work.rosters, work.fetch_pools)
        except FleetpullError as failure:
            logger.exception(
                'endpoint failed: provider=%s endpoint=%s',
                provider.value,
                name,
            )
            failures.append(EndpointFailure(provider.value, name, failure))
    return failures


def _collected_failures(
    queue_futures: dict[Provider, Future[list[EndpointFailure]]],
) -> list[EndpointFailure]:
    """Fold the joined queues into the documented two-level failure order.

    Iterates the futures in their key order -- provider-config order, the
    caller's contract -- flattening each queue's run-order failures, so the
    aggregate reads run order within a provider, provider-config order
    across providers. A queue that died on a bug re-raises it here
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
