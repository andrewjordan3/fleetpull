# src/fleetpull/endpoints/__init__.py
"""The endpoints layer: per-endpoint bindings and the endpoint catalog.

Provider bindings live in subpackage faces consumers import directly --
``fleetpull.endpoints.shared`` for the ``EndpointDefinition`` binding and the
shared spec-builders, ``fleetpull.endpoints.motive`` (and future provider
packages) for the binding factories. The catalog over those bindings --
``EndpointRegistry`` and ``build_endpoint_registry`` -- is re-exported here as the
layer's public lookup surface, so a consumer routes through this face rather than
reaching the ``registry`` submodule directly.
"""

from fleetpull.endpoints.registry import EndpointRegistry, build_endpoint_registry

__all__: list[str] = ['EndpointRegistry', 'build_endpoint_registry']
