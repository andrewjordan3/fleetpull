# src/fleetpull/endpoints/samsara/__init__.py
"""The Samsara endpoints package.

A provider package under the endpoints layer. It exposes no factory gather:
endpoint leaves are discovered by ``build_endpoint_registry`` walking this
package for modules exposing ``build_endpoint``, so a new Samsara endpoint is
a new leaf module here with nothing to register. Import a specific factory
from its leaf module (``fleetpull.endpoints.samsara.vehicles``) when one is
needed directly.
"""

__all__: list[str] = []
