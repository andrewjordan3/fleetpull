# src/fleetpull/api/__init__.py
"""The public data API: the ``Endpoints`` catalog, its identities, and ``fetch``.

The top tier of the package vertical: it composes everything below
(config, endpoints, network, records) behind DESIGN §10's two-verb
surface. ``sync``, the config-driven verb, joins this package at
roadmap item 6 and will compose the orchestrator entry -- which is why
this tier sits above ``orchestrator``, not beside it.
"""

from fleetpull.api.auth_ingress import AuthInput
from fleetpull.api.catalog import Endpoints, available_endpoints
from fleetpull.api.fetch import fetch
from fleetpull.api.identity import EndpointIdentity, SnapshotEndpoint, WindowedEndpoint
from fleetpull.api.sync import Sync

__all__: list[str] = [
    'AuthInput',
    'EndpointIdentity',
    'Endpoints',
    'SnapshotEndpoint',
    'Sync',
    'WindowedEndpoint',
    'available_endpoints',
    'fetch',
]
