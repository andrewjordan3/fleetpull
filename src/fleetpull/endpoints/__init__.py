# src/fleetpull/endpoints/__init__.py
"""The endpoints layer: per-endpoint bindings.

A namespace package; consumers import the subpackage faces directly --
``fleetpull.endpoints.shared`` for the ``EndpointDefinition`` binding and the
shared spec-builders, ``fleetpull.endpoints.motive`` (and future provider
packages) for the binding factories.
"""
