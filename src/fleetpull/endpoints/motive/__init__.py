# src/fleetpull/endpoints/motive/__init__.py
"""The Motive endpoints package.

A provider package under the endpoints layer. It exposes no factory gather:
endpoint leaves are discovered by ``build_endpoint_registry`` walking this
package for modules exposing ``build_endpoint``, so a new Motive endpoint is
a new leaf module here with nothing to register. Import a specific factory
from its leaf module (``fleetpull.endpoints.motive.vehicles``) when one is
needed directly.
"""

__all__: list[str] = []
