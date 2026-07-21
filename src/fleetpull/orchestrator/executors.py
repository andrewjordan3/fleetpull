# src/fleetpull/orchestrator/executors.py
"""Provider-keyed registry of fetch pools: one worker pool per provider.

The per-provider executor of DESIGN §7, owned by ``Sync``'s composition root
as a context-managed collaborator (the ``ProviderClientRegistry`` precedent):
``__enter__`` creates one ``ThreadPoolExecutor`` per configured provider,
sized ``max_workers = rate_limit.max_concurrency``, and ``__exit__`` shuts
every pool down deterministically -- success or failure, no leaked threads.
The lifecycle machinery (publish-on-success enter, closed-before-release
exit, the RuntimeError-vs-ConfigurationError lookup split) is the generic
``ProviderResourceRegistry``'s (the network client face); this subclass
supplies pool construction and the error nouns.
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

Since the intra-provider grain (DESIGN §7, 2026-07-20) one pool may serve
multiple concurrent fan-out endpoints: a submission window is
per-``stream_pieces``-call local state, so each concurrent stream keeps its
own window over the shared executor -- execution stays capped by
``max_workers`` and in-flight requests by the limiter's semaphore, so the
in-flight ceiling claim above holds unchanged and nothing here resized.
"""

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from typing import ClassVar

from fleetpull.network.client import ProviderResourceRegistry
from fleetpull.orchestrator.fanout import FetchPool
from fleetpull.vocabulary import Provider

__all__: list[str] = ['FetchPoolRegistry']

# The submission window as a multiple of the worker count: 2x keeps a queued
# piece ready for every finishing worker (plain double-buffering) while the
# streaming bound stays proportional to the pool, never the roster.
_SUBMISSION_WINDOW_FACTOR: int = 2


class FetchPoolRegistry(ProviderResourceRegistry[FetchPool]):
    """Owns one fetch worker pool per provider, keyed by ``Provider``.

    A resource-owning context manager (the generic base's semantics):
    ``__exit__`` shuts each pool down with ``wait=True``, so no worker thread
    outlives the run that composed it, on success and on failure alike.
    ``pool_for`` returns a provider's ``FetchPool``; use it only inside the
    ``with`` block::

        with FetchPoolRegistry(workers_by_provider) as fetch_pools:
            pool = fetch_pools.pool_for(definition.provider)
    """

    _resource_noun: ClassVar[str] = 'fetch pool'
    _lookup_description: ClassVar[str] = 'pool_for'

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
        super().__init__(workers_by_provider)
        self._workers_by_provider: dict[Provider, int] = dict(workers_by_provider)

    def _open_resource(self, stack: ExitStack, provider: Provider) -> FetchPool:
        """Create one provider's worker pool with its fixed submission window.

        ``ThreadPoolExecutor.__exit__`` is ``shutdown(wait=True)``: every
        worker thread is joined when the stack unwinds, so a failing run
        cannot leak threads into the caller.

        Args:
            stack: The enter's unwind stack; the pool's shutdown registers
                here.
            provider: The provider whose pool to create.

        Returns:
            The provider's ``FetchPool``.

        Side Effects:
            Constructs one ``ThreadPoolExecutor`` (threads spawn lazily on
            first submit).
        """
        worker_count = self._workers_by_provider[provider]
        executor = stack.enter_context(
            ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix=f'fleetpull-{provider.value}-fetch',
            )
        )
        return FetchPool(
            executor=executor,
            submission_window=_SUBMISSION_WINDOW_FACTOR * worker_count,
        )

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
        return self._resource_for(provider)
