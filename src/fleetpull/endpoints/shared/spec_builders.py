# src/fleetpull/endpoints/shared/spec_builders.py
"""Shared spec-builders: SpecBuilder implementations with no per-provider
or per-endpoint behavior.

A snapshot endpoint's first request is just ``GET base_url + path``: it
translates no resume value (``SnapshotMode`` always passes
``resume=None``) and fans out over no path, so its spec-builder carries
no provider- or endpoint-specific logic and is shared across every
snapshot binding. Per-provider resume translation -- watermark windows,
feed tokens -- lives in dedicated builders beside their bindings, never
here.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass

from fleetpull.endpoints.shared.base import ResumeValue
from fleetpull.network.contract import HttpMethod, RequestSpec

__all__: list[str] = ['StaticGetSpecBuilder']

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StaticGetSpecBuilder:
    """Build a fixed ``GET base_url + path`` first request.

    The spec-builder for endpoints with no resume value and no URL-path
    fan-out (snapshots). Both ``build_spec`` arguments are accepted to
    satisfy the ``SpecBuilder`` protocol but are intentionally unused: a
    snapshot resumes from nothing, and a non-fan-out endpoint substitutes
    no path placeholders.

    Attributes:
        base_url: Root of the provider API, trailing-slash-normalized by
            the provider config so a leading-slash path joins directly.
        path: The endpoint's leading-slash request path (e.g.
            ``'/v1/vehicles'``).
    """

    base_url: str
    path: str

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the fixed first request.

        Args:
            resume: Accepted to satisfy the protocol; unused -- a snapshot
                resumes from nothing.
            path_values: Accepted to satisfy the protocol; unused -- there
                is no URL-path fan-out.

        Returns:
            A credential-less ``GET`` for ``base_url + path``. Auth headers
            are layered on by the client's ``ProviderProfile``; pagination
            parameters are injected by the page decoder's ``first_request``.
        """
        return RequestSpec(method=HttpMethod.GET, url=f'{self.base_url}{self.path}')
