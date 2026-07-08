"""Tests for fleetpull.orchestrator.executors -- the per-provider fetch pools."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from fleetpull.exceptions import ConfigurationError
from fleetpull.orchestrator.executors import FetchPoolRegistry
from fleetpull.vocabulary import Provider


def _fleetpull_worker_threads() -> list[threading.Thread]:
    return [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith('fleetpull-')
    ]


def test_pool_for_outside_the_with_block_is_a_caller_bug() -> None:
    registry = FetchPoolRegistry({Provider.MOTIVE: 2})
    with pytest.raises(RuntimeError, match='not open'):
        registry.pool_for(Provider.MOTIVE)


def test_pool_for_after_exit_is_a_caller_bug() -> None:
    registry = FetchPoolRegistry({Provider.MOTIVE: 2})
    with registry:
        pass
    with pytest.raises(RuntimeError, match='not open'):
        registry.pool_for(Provider.MOTIVE)


def test_unconfigured_provider_is_a_configuration_error() -> None:
    with (
        FetchPoolRegistry({Provider.MOTIVE: 2}) as pools,
        pytest.raises(ConfigurationError, match='no fetch pool'),
    ):
        pools.pool_for(Provider.SAMSARA)


def test_pool_is_sized_by_the_worker_count_with_a_doubled_window() -> None:
    with FetchPoolRegistry({Provider.MOTIVE: 3}) as pools:
        pool = pools.pool_for(Provider.MOTIVE)
        assert isinstance(pool.executor, ThreadPoolExecutor)
        assert pool.executor._max_workers == 3
        assert pool.submission_window == 6


def test_exit_joins_every_worker_thread_on_success() -> None:
    with FetchPoolRegistry({Provider.MOTIVE: 2}) as pools:
        executor = pools.pool_for(Provider.MOTIVE).executor
        assert executor.submit(threading.get_ident).result() is not None
        assert _fleetpull_worker_threads()  # a worker demonstrably spawned
    assert _fleetpull_worker_threads() == []
    with pytest.raises(RuntimeError, match='shutdown'):
        executor.submit(threading.get_ident)


def test_exit_joins_every_worker_thread_when_the_body_raises() -> None:
    with pytest.raises(RuntimeError, match='planted'):  # noqa: SIM117, PT012 -- the raise must happen inside the registry block
        with FetchPoolRegistry({Provider.MOTIVE: 2}) as pools:
            pools.pool_for(Provider.MOTIVE).executor.submit(threading.get_ident)
            raise RuntimeError('planted body failure')
    assert _fleetpull_worker_threads() == []
