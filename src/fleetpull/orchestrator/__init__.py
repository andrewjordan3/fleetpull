# src/fleetpull/orchestrator/__init__.py
"""The orchestration layer's face: what the composition roots consume (DESIGN §14).

``run_endpoint`` is the caller boundary: it resolves the endpoint's declared
request driver through the shared shape seam (``resolve_request_driver`` --
the one match over the ``RequestShape`` union, called by both composition
roots) and runs. ``EndpointRunner`` owns one endpoint's run and dispatches on
its sync mode, constructed on the ``RunStateAccess`` state bundle;
``RosterMachinery`` bundles the roster collaborators a run consults and
``RosterRefreshCoordinator`` owns the roster staleness policy whole;
``FetchPoolRegistry`` owns one fan-out worker pool per provider (DESIGN §7).
Everything else in the package -- the drives, the drivers, the outcome union,
the unit loop -- is internal machinery its modules export directly."""

from fleetpull.orchestrator.entry import RosterMachinery, run_endpoint
from fleetpull.orchestrator.executors import FetchPoolRegistry
from fleetpull.orchestrator.roster_refresh import RosterRefreshCoordinator
from fleetpull.orchestrator.runner import EndpointRunner
from fleetpull.orchestrator.shape_resolution import resolve_request_driver
from fleetpull.orchestrator.spine import RunStateAccess

__all__: list[str] = [
    'EndpointRunner',
    'FetchPoolRegistry',
    'RosterMachinery',
    'RosterRefreshCoordinator',
    'RunStateAccess',
    'resolve_request_driver',
    'run_endpoint',
]
