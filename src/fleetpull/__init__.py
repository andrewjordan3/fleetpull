"""fleetpull: Pull fleet telematics data from provider APIs into typed Polars output.

Retrieval, dtype coercion, and light structural normalization only. Output
stays as close to the raw API responses as is reasonable. No cross-endpoint
merging, no unified schema, no assumed end use.

The public data API: ``fetch(Endpoints.Motive.vehicles, auth=...)`` returns
an eager typed DataFrame. Consumers catch ``FleetpullError`` or its four
public subclasses, all importable here; every other exception type is
internal. ``sync``, the config-driven verb, joins at roadmap item 6.
"""

from fleetpull.api import Endpoints, Sync, fetch
from fleetpull.exceptions import (
    AuthenticationError,
    ConfigurationError,
    FleetpullError,
    ProviderResponseError,
    RetriesExhaustedError,
    SyncFailuresError,
)

__version__: str = '0.1.0'

__all__: list[str] = [
    'AuthenticationError',
    'ConfigurationError',
    'Endpoints',
    'FleetpullError',
    'ProviderResponseError',
    'RetriesExhaustedError',
    'Sync',
    'SyncFailuresError',
    'fetch',
]
