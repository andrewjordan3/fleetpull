"""Structural contract for endpoint leaves: every leaf exposes build_endpoint.

The discovery walk in ``fleetpull.endpoints.registry`` reaches endpoint leaf
modules directly rather than through provider faces -- a named exception to the
import-discipline clause-3 face-routing rule. This test is that exception's
replacement guardrail: it asserts every leaf the walk yields exposes a callable
``build_endpoint`` taking exactly one ``ProviderConfig`` subclass, so a typo'd
factory name or a wrong parameter type fails here, loudly, rather than silently
dropping the endpoint from the catalog.
"""

import importlib
from typing import get_type_hints

from fleetpull.config import ProviderConfig
from fleetpull.endpoints.registry import _iter_endpoint_leaf_modules


def test_every_leaf_exposes_a_well_formed_build_endpoint() -> None:
    leaf_names = list(_iter_endpoint_leaf_modules())
    assert leaf_names, 'discovery found no endpoint leaves'
    for module_name in leaf_names:
        module = importlib.import_module(module_name)
        factory = getattr(module, 'build_endpoint', None)
        assert callable(factory), f'{module_name} exposes no build_endpoint'
        hints = get_type_hints(factory)
        hints.pop('return', None)
        assert len(hints) == 1, f'{module_name} build_endpoint must take one parameter'
        (annotation,) = hints.values()
        assert isinstance(annotation, type), (
            f'{module_name} build_endpoint parameter must be a type'
        )
        assert issubclass(annotation, ProviderConfig), (
            f'{module_name} build_endpoint must annotate a ProviderConfig'
        )
