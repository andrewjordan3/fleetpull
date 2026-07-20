# src/fleetpull/endpoints/samsara/drivers.py
"""The Samsara drivers binding: the two-sweep activation-status snapshot,
the first ``ParamSweep`` consumer.

The default ``/fleet/drivers`` listing IS the active set exactly -- the
2026-07-20 probe matched it record-for-record against
``driverActivationStatus=active`` (460 ids, identical) -- while the
deactivated sweep returned 372 fully disjoint records INVISIBLE to the
default listing: 45% of the 832-driver population. The one complete
driver dataset is therefore the union of both sweeps, so this binding
declares ``request_shape=ParamSweep`` over the two statuses; the shared
shape-resolution seam fans one request chain per value through the
member-agnostic ``FanOutRequestDriver``, and the
``driver_activation_status`` column carries the split in the one stored
dataset.

No completeness check is declared, on two proofs. Continuation is
explicit per page (the vehicles cursor contract, proven per-type on
drivers: a limit=5 walk of 92 pages returned 460/460 unique ascending
ids with no boundary overlap or loss), and the sweep vocabulary is
API-enforced: ``driverActivationStatus`` is a strict closed enum whose
every probed variant -- case changes, comma-joins, repeated keys, bogus
values -- returned HTTP 400 naming the two admissible values, loudly,
never a silent empty listing. A typo'd sweep value can therefore never
masquerade as an empty partition.

The existing ``SamsaraCursorPageDecoder`` needed NO change for the
sweep: its advance merges ``after`` onto the SENT spec, so a
first-request query parameter persists across the whole walk -- proven
live by a limit=50 deactivated walk (8 pages, 50x7+22, 372/372 unique,
every record deactivated, a fresh cursor per page, the standard
terminal). The spec builder below exists because the shared
``StaticGetSpecBuilder`` deliberately ignores ``member_values`` (a
single-chain snapshot binds no member); a sweep chain binds one query
parameter, so this endpoint's builder merges the member binding
verbatim as query parameters (the Motive vehicle_locations
leaf-builder precedent, which renders its member into the URL path
instead).
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from fleetpull.config import SamsaraConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ParamSweep,
    ResumeValue,
    SnapshotMode,
    StorageKind,
)
from fleetpull.models.samsara import Driver
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import SamsaraCursorPageDecoder
from fleetpull.vocabulary import Provider, QuotaScope

__all__: list[str] = [
    'SamsaraDriversSpecBuilder',
    'build_endpoint',
]

_DRIVERS_PATH: Final[str] = '/fleet/drivers'
_RECORDS_KEY: Final[str] = 'data'

# The per-page record count. 512 is Samsara's documented list-endpoint
# maximum, accepted on /fleet/drivers (captured 2026-07-20); the cursor
# mechanics behind it were proven per-type by a limit=5 walk -- 92
# pages, 460/460 unique ascending ids, no boundary overlap or loss.
_RESULTS_LIMIT: Final[int] = 512

# The sweep's member key IS the wire query parameter, verbatim: the spec
# builder merges member_values into params unchanged, so declaring the
# exact wire token here leaves no translation seam to drift. The value
# set is API-closed -- any other value is a loud HTTP 400 (captured
# 2026-07-20), never a silent empty listing.
_ACTIVATION_STATUS_PARAM: Final[str] = 'driverActivationStatus'
_ACTIVATION_STATUS_VALUES: Final[tuple[str, ...]] = ('active', 'deactivated')


@dataclass(frozen=True, slots=True)
class SamsaraDriversSpecBuilder:
    """Build the per-sweep first request for drivers.

    The ``SpecBuilder`` for the drivers ``ParamSweep``: a fixed
    ``GET base_url + path`` carrying the chain's member binding as query
    parameters, verbatim -- each sweep chain's ``member_values`` is
    ``{'driverActivationStatus': <value>}``, and the decoder's ``after``
    advance merges onto this spec, so the status parameter persists
    across every page of the chain's walk (module docstring). Exists
    because the shared ``StaticGetSpecBuilder`` deliberately ignores
    ``member_values``; this endpoint's member IS a query parameter.

    Attributes:
        base_url: Root of the Samsara API, trailing-slash-normalized by
            the provider config so the leading-slash path joins
            directly.
        path: The endpoint's leading-slash request path
            (``'/fleet/drivers'``).
    """

    base_url: str
    path: str

    def build_spec(
        self, resume: ResumeValue, member_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build one sweep chain's first request.

        Args:
            resume: Accepted to satisfy the protocol; unused -- a
                snapshot resumes from nothing.
            member_values: The chain's member binding, merged verbatim
                as query parameters -- ``{'driverActivationStatus':
                <value>}`` for a sweep chain.

        Returns:
            A credential-less ``GET`` for ``base_url + path`` carrying
            the member binding as query parameters. Auth headers are
            layered on by the client's ``ProviderProfile``; pagination
            parameters are injected by the page decoder's
            ``first_request``.

        Side Effects:
            None.
        """
        return RequestSpec(
            method=HttpMethod.GET,
            url=f'{self.base_url}{self.path}',
            params=dict(member_values),
        )


def build_endpoint(config: SamsaraConfig) -> EndpointDefinition[Driver]:
    """Build the Samsara drivers two-sweep snapshot binding.

    A full-listing snapshot of the fleet's drivers, complete only as the
    union of the two activation-status sweeps (module docstring): the
    declared ``ParamSweep`` fans one cursor-walked chain per status, and
    every run fully replaces the single parquet file with that union.
    Records arrive as a top-level list under ``data``, walked by
    explicit cursor pages (``limit`` on page one, ``after`` merged
    thereafter, the status parameter persisting throughout), terminal on
    ``hasNextPage: false``.

    Args:
        config: The validated Samsara configuration; supplies the base
            URL the spec-builder joins to the drivers path.

    Returns:
        The frozen drivers ``EndpointDefinition``.
    """
    return EndpointDefinition(
        provider=Provider.SAMSARA,
        name='drivers',
        spec_builder=SamsaraDriversSpecBuilder(
            base_url=config.base_url, path=_DRIVERS_PATH
        ),
        page_decoder=SamsaraCursorPageDecoder(
            records_key=_RECORDS_KEY, results_limit=_RESULTS_LIMIT
        ),
        response_model=Driver,
        quota_scope=QuotaScope.SAMSARA,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
        request_shape=ParamSweep(
            param=_ACTIVATION_STATUS_PARAM, values=_ACTIVATION_STATUS_VALUES
        ),
    )
