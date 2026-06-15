"""Provider-agnostic request contract: specs, auth, classification, pagination.

This package intentionally aggregates no surface — callers import the
submodules directly (``from fleetpull.network.contract.request import
RequestSpec``), the convention used everywhere in the tree. Keeping the
package ``__init__`` free of eager re-exports also keeps the
foundational ``fleetpull.exceptions`` module able to import
``ResponseCategory`` from ``contract.outcome`` without a package-init
import cycle (``exceptions`` -> ``contract.__init__`` ->
``envelopes``/``pagination`` -> ``exceptions``).
"""

__all__: list[str] = []
