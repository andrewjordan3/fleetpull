# src/fleetpull/orchestrator/__init__.py
"""The orchestration layer: run one endpoint to completion (DESIGN §14).

``run_endpoint`` is the caller boundary: it resolves the endpoint's declared
request driver through the shared shape seam and runs — callers never see the
shape distinctions. ``resolve_request_driver`` (``shape_resolution``) is that
seam: one match over the ``RequestShape`` union, called by both composition
roots (the entry for sync, ``fetch`` for the in-memory verb).
``EndpointRunner`` owns one endpoint's run transaction and dispatches on its
sync mode; a ``RequestDriver`` owns request cardinality.
``SingleRequestDriver`` streams one page-batch at a time;
``FanOutRequestDriver`` issues one chain per supplied member (roster members
or sweep values alike), fetched concurrently on its provider's ``FetchPool``
(one pool per provider, owned by the composition root's context-managed
``FetchPoolRegistry`` — DESIGN §7). ``run`` returns a ``RunOutcome``
(``Executed`` | ``CaughtUp``). External callers import these names here."""

from fleetpull.orchestrator.drivers import (
    FanOutRequestDriver,
    RequestDriver,
    SingleRequestDriver,
)
from fleetpull.orchestrator.entry import (
    RosterMachinery,
    run_endpoint,
)
from fleetpull.orchestrator.executors import FetchPoolRegistry
from fleetpull.orchestrator.fanout import FetchPool
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.roster_refresh import RosterRefreshCoordinator
from fleetpull.orchestrator.runner import EndpointRunner, RunStateAccess
from fleetpull.orchestrator.shape_resolution import (
    FetchPoolSource,
    resolve_request_driver,
)

__all__: list[str] = [
    'CaughtUp',
    'EndpointRunner',
    'Executed',
    'FanOutRequestDriver',
    'FetchPool',
    'FetchPoolRegistry',
    'FetchPoolSource',
    'RequestDriver',
    'RosterMachinery',
    'RosterRefreshCoordinator',
    'RunOutcome',
    'RunStateAccess',
    'SingleRequestDriver',
    'resolve_request_driver',
    'run_endpoint',
]
