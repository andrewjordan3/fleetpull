"""fleetpull: Pull fleet telematics data from provider APIs into typed Polars output.

Retrieval, dtype coercion, and light structural normalization only. Output
stays as close to the raw API responses as is reasonable. No cross-endpoint
merging, no unified schema, no assumed end use.

Public API exports are added here as modules land. Do not import modules
that do not yet exist; this file must stay importable at every stage of
the build-out.
"""

__version__: str = '0.1.0'

__all__: list[str] = []
