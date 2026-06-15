# src/fleetpull/network/contract/auth.py
"""The auth-strategy protocol: pure shape, the contract every strategy implements.

Implementations live in ``network/auth/strategies.py`` (beside the
``GeotabSessionManager`` that ``GeotabSessionAuth`` wraps); keeping them
out of this surface module is what lets the contract face stay free of
any ``network/auth`` dependency.
"""

from typing import Protocol, runtime_checkable

from fleetpull.network.contract.request import RequestSpec

__all__: list[str] = ['AuthStrategy']


@runtime_checkable
class AuthStrategy(Protocol):
    """Provider-agnostic credential injection (DESIGN.md §8)."""

    def prepare(self, spec: RequestSpec) -> RequestSpec:
        """
        Inject credentials and return a new spec.

        Called fresh for EVERY HTTP attempt — every page, every retry
        (the prepare-per-attempt contract, symmetric with
        token-per-attempt). After a successful ``on_auth_failure``, the
        retry must carry the fresh credentials, so re-preparing is
        mandatory; implementations must keep the hot path cheap.

        Args:
            spec: The credential-less request description.

        Returns:
            A new spec carrying credentials; the input is unchanged.
        """
        ...

    def on_auth_failure(self) -> bool:
        """
        Answer "did I change anything that makes one retry worthwhile?"

        Performs the fix (if any) as a side effect.

        Returns:
            True when fresh credentials are now available and one retry
            is worthwhile; False when nothing can be fixed by retrying.
        """
        ...
