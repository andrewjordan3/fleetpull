# src/fleetpull/orchestrator/executors.py
"""Provider-keyed registry of fetch pools: one worker pool per provider.

The per-provider executor of DESIGN §7, owned by ``Sync``'s composition root
as a context-managed collaborator (the ``ProviderClientRegistry`` precedent):
``__enter__`` creates one ``ThreadPoolExecutor`` per configured provider,
sized ``max_workers = rate_limit.max_concurrency``, and ``__exit__`` shuts
every pool down deterministically -- success or failure, no leaked threads.
Per-provider pools by construction, so one provider's 429 penalty can never
park another provider's workers (the starvation fix), and a fan-out run's
in-flight ceiling equals the limiter's concurrency semaphore, which the pool
never replaces -- workers acquire tokens and the semaphore exactly as the
serial path did.

``pool_for`` hands out the provider's ``FetchPool`` -- the executor plus the
bounded-channel window ``stream_pieces`` enforces. The window is fixed here,
at the one place the worker count is known, to twice that count: a finishing
worker always has a queued piece to pick up while the consumer writes, and
the fetched-but-unconsumed bound stays a small multiple of the pool size.
"""

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from types import TracebackType
from typing import Self

from fleetpull.exceptions import ConfigurationError
from fleetpull.orchestrator.fanout import FetchPool
from fleetpull.vocabulary import Provider

__all__: list[str] = ['FetchPoolRegistry']

# The submission window as a multiple of the worker count: 2x keeps a queued
# piece ready for every finishing worker (plain double-buffering) while the
# streaming bound stays proportional to the pool, never the roster.
_SUBMISSION_WINDOW_FACTOR: int = 2


class FetchPoolRegistry:
    """Owns one fetch worker pool per provider, keyed by ``Provider``.

    A resource-owning context manager. ``__enter__`` creates every configured
    provider's ``ThreadPoolExecutor`` and returns self; ``__exit__`` shuts
    each one down with ``wait=True``, so no worker thread outlives the run
    that composed it, on success and on failure alike. ``pool_for`` returns a
    provider's ``FetchPool``; use it only inside the ``with`` block::

        with FetchPoolRegistry(workers_by_provider) as fetch_pools:
            pool = fetch_pools.pool_for(definition.provider)

    ``pool_for`` outside an open ``with`` block raises ``RuntimeError`` (a
    caller bug), distinct from an unconfigured provider, which raises
    ``ConfigurationError`` -- the ``ProviderClientRegistry`` semantics.
    """

    def __init__(self, workers_by_provider: Mapping[Provider, int]) -> None:
        """
        Args:
            workers_by_provider: Each configured provider's worker count --
                ``rate_limit.max_concurrency`` from that provider's resolved
                config, so the pool can never outrun the limiter's in-flight
                semaphore. A provider absent here has no pool and is rejected
                by ``pool_for`` while the registry is open.

        Side Effects:
            None -- pools are created on ``__enter__``, not here.
        """
        self._workers_by_provider: dict[Provider, int] = dict(workers_by_provider)
        self._pools: dict[Provider, FetchPool] = {}
        self._stack: ExitStack = ExitStack()
        self._open: bool = False

    def __enter__(self) -> Self:
        """Create one worker pool per configured provider.

        Side Effects:
            Constructs one ``ThreadPoolExecutor`` per provider (threads spawn
            lazily on first submit). On a later construction failure, the
            pools already created are shut down before propagating.
        """
        pools: dict[Provider, FetchPool] = {}
        with ExitStack() as stack:
            for provider, worker_count in self._workers_by_provider.items():
                executor = stack.enter_context(
                    ThreadPoolExecutor(
                        max_workers=worker_count,
                        thread_name_prefix=f'fleetpull-{provider.value}-fetch',
                    )
                )
                pools[provider] = FetchPool(
                    executor=executor,
                    submission_window=_SUBMISSION_WINDOW_FACTOR * worker_count,
                )
            self._stack = stack.pop_all()
        self._pools = pools
        self._open = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Shut every pool down, joining its workers, forwarding the context.

        ``ThreadPoolExecutor.__exit__`` is ``shutdown(wait=True)``: every
        worker thread is joined before this returns, so a failing run cannot
        leak threads into the caller. The instance is marked closed first, so
        a shutdown error still leaves the registry unusable rather than
        apparently-open.

        Side Effects:
            Joins every pool's worker threads.
        """
        self._open = False
        self._pools = {}
        return bool(self._stack.__exit__(exc_type, exc_value, traceback))

    def pool_for(self, provider: Provider) -> FetchPool:
        """Return the fetch pool for a provider.

        Args:
            provider: The provider whose pool to return (e.g.
                ``definition.provider``).

        Returns:
            The provider's ``FetchPool``.

        Raises:
            RuntimeError: The registry is not open -- ``pool_for`` was called
                outside an active ``with`` block (a caller bug).
            ConfigurationError: The registry is open but the provider has no
                configured pool.
        """
        if not self._open:
            raise RuntimeError(
                'FetchPoolRegistry is not open; call pool_for inside its `with` block'
            )
        pool = self._pools.get(provider)
        if pool is None:
            configured = ', '.join(sorted(p.value for p in self._pools)) or 'none'
            raise ConfigurationError(
                'no fetch pool configured for provider',
                provider=provider.value,
                detail=f'configured providers: {configured}',
            )
        return pool
