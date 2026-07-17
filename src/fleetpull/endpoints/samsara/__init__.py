# src/fleetpull/endpoints/samsara/__init__.py
"""The Samsara endpoints package.

A provider package under the endpoints layer, present so the discovery
walk (``build_endpoint_registry`` importing ``endpoints.<provider>``)
has a package to visit; it holds no leaves yet -- the first Samsara
endpoint vertical adds a leaf module here with nothing to register (the
GeoTab package convention).
"""

__all__: list[str] = []
