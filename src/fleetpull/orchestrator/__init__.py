# src/fleetpull/orchestrator/__init__.py
"""The orchestration layer: run one endpoint to completion (DESIGN §14).

``EndpointRunner`` owns one endpoint's run transaction and dispatches on its sync
mode; a ``RequestDriver`` owns request cardinality (``SingleRequestDriver`` streams
one page-batch at a time). ``run`` returns a ``RunOutcome`` (``Executed`` |
``CaughtUp``). External callers import these names here."""

from fleetpull.orchestrator.drivers import RequestDriver, SingleRequestDriver
from fleetpull.orchestrator.outcome import CaughtUp, Executed, RunOutcome
from fleetpull.orchestrator.runner import EndpointRunner

__all__: list[str] = [
    'CaughtUp',
    'EndpointRunner',
    'Executed',
    'RequestDriver',
    'RunOutcome',
    'SingleRequestDriver',
]
