"""fleetpull: Pull fleet telematics data from provider APIs into typed Polars output.

Retrieval, dtype coercion, and light structural normalization only. Output
stays as close to the raw API responses as is reasonable. No cross-endpoint
merging, no unified schema, no assumed end use.

The public data API: ``fetch(Endpoints.Motive.vehicles, auth=...)`` returns
an eager typed DataFrame, and ``Sync(config_path).run()`` executes a
configuration-driven parquet/state sync. Consumers catch ``FleetpullError``
or its public subclasses, all importable here; every other exception type is
internal.
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
