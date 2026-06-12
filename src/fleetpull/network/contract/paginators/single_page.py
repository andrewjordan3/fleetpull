# src/fleetpull/network/contract/paginators/single_page.py
"""The strategy for unpaginated endpoints: exactly one page, always.

This replaces any is-paginated flag: unpaginated endpoints (e.g.
Motive's ``/v3/vehicle_locations``) use this strategy and the client's
loop never branches.
"""

from dataclasses import dataclass

from fleetpull.network.contract.pagination import PageAdvance
from fleetpull.network.contract.request import JsonValue, RequestSpec

__all__: list[str] = ['SinglePageStrategy']


@dataclass(frozen=True, slots=True)
class SinglePageStrategy:
    """One page, no decoration, no metadata: every advance completes."""

    def first_request(self, spec: RequestSpec) -> RequestSpec:
        """Return the spec unchanged; nothing to decorate."""
        return spec

    def advance(self, sent: RequestSpec, envelope: JsonValue) -> PageAdvance:
        """Complete unconditionally; the envelope carries no metadata."""
        return PageAdvance(next_spec=None, durable_progress=None)
