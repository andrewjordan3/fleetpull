# src/fleetpull/orchestrator/__init__.py
"""The orchestration layer: run one endpoint to completion (DESIGN §14).

``run_endpoint`` is the caller boundary: it resolves the endpoint's declared
request driver (fan-out or single-fetch) and runs — callers never see the
distinction. ``EndpointRunner`` owns one endpoint's run transaction and dispatches on its sync
mode; a ``RequestDriver`` owns request cardinality. ``SingleRequestDriver`` streams
one page-batch at a time; ``FanOutRequestDriver`` issues one chain per supplied
member. ``run`` returns a ``RunOutcome`` (``Executed`` | ``CaughtUp``). External
callers import these names here."""

from fleetpull.orchestrator.drivers import (
    FanOutRequestDriver,
    RequestDriver,
    SingleRequestDriver,
)
from fleetpull.orchestrator.entry import run_endpoint
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.roster_refresh import RosterRefreshCoordinator
from fleetpull.orchestrator.runner import EndpointRunner

__all__: list[str] = [
    'CaughtUp',
    'EndpointRunner',
    'Executed',
    'FanOutRequestDriver',
    'RequestDriver',
    'RosterRefreshCoordinator',
    'RunOutcome',
    'SingleRequestDriver',
    'run_endpoint',
]
