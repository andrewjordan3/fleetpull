"""The snapshot exposure gate's two halves, proven by one negative shape.

The call under test is statically illegal: ``vehicle_locations`` is
windowed-typed and ``fetch`` accepts only ``SnapshotEndpoint``, so the
call carries ``# type: ignore[arg-type]``. mypy's ``warn_unused_ignores``
turns that comment into the static gate's permanent tripwire -- if the
windowed identity ever typechecks into ``fetch``, the ignore becomes
unused and the mypy gate fails. pytest executes the same call and asserts
the runtime guard raises ``ConfigurationError`` before any client is
constructed, covering the verb's unchecked audience (notebooks, where
mypy never runs).

``fleetpull.api.fetch`` is resolved via ``importlib`` because the api
face's ``from ...fetch import fetch`` shadows the submodule attribute
with the function.
"""

import importlib

import pytest

from fleetpull import ConfigurationError, Endpoints, fetch

_fetch_module = importlib.import_module('fleetpull.api.fetch')


class _ExplodingClient:
    """Stands in for TransportClient: constructing one fails the test."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise AssertionError('the guard must fire before any client construction')


def test_windowed_identity_is_rejected_before_client_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_fetch_module, 'TransportClient', _ExplodingClient)
    with pytest.raises(ConfigurationError) as raised:
        fetch(Endpoints.Motive.vehicle_locations, auth='k')  # type: ignore[arg-type]
    message = str(raised.value)
    assert 'vehicle_locations' in message
    assert 'snapshot-only' in message
    assert 'windowed' in message


def test_non_identity_input_is_rejected_before_client_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_fetch_module, 'TransportClient', _ExplodingClient)
    with pytest.raises(ConfigurationError) as raised:
        fetch('vehicles', auth='k')  # type: ignore[arg-type]
    assert 'str' in str(raised.value)
