# src/fleetpull/endpoints/geotab/devices.py
"""The GeoTab devices binding: a factory composing the devices snapshot
EndpointDefinition from resolved GeoTab configuration.

A binding cannot be a module-level constant because its spec-builder and
completeness check need the run's configured authentication host, known
only after the YAML config loads; so the endpoint is a factory taking a
validated ``GeotabConfig`` and returning the frozen
``EndpointDefinition`` the composition root hands to the client (the
Motive leaf convention).

Every request here is a JSON-RPC POST to ``https://{server}/apiv1``
whose ``params.credentials`` are injected by the session auth strategy,
never built here; the strategy also retargets each prepared request to
the session's resolved host, so the host this module writes is a
pre-auth placeholder that never reaches the wire on its own (DESIGN
section 8). Plain ``Get`` silently hard-caps at 5,000 records with no
continuation signal (captured 2026-07-09: ``GetCountOf`` 5,666 vs the
capped 5,000), which shapes both strategies composed here: the seek
walk (``sort`` by ``id`` ascending, offset carried by the decoder) and
the ``GetCountOfCheck`` truth instrument fired beside every harvest.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    ResumeValue,
    SnapshotMode,
    StorageKind,
)
from fleetpull.models.geotab import Device
from fleetpull.network.client import TransportClient
from fleetpull.network.contract import (
    HttpMethod,
    RequestSpec,
    validated_envelope_slice,
)
from fleetpull.network.decoders import GeotabGetPageDecoder
from fleetpull.vocabulary import JsonValue, Provider, QuotaScope

__all__: list[str] = ['GetCountOfCheck', 'build_endpoint']

logger = logging.getLogger(__name__)

# The JSON-RPC ingress path every GeoTab method POSTs to.
_API_PATH: Final[str] = '/apiv1'

# Pre-auth placeholder host for a default-constructed (credential-less)
# config -- mirrors GeotabAuthConfig's server default; the session
# strategy retargets every prepared request, so no request ever leaves
# for this host un-retargeted.
_DEFAULT_SERVER: Final[str] = 'my.geotab.com'

# The largest sound page under Get's silent 5,000-record cap.
_RESULTS_LIMIT: Final[int] = 5000

# Wire-protocol tokens: module-private Final constants, colocated with
# the strategies that emit them (the constants-scope precedent;
# deliberately unshared, even with the decoder module's own copies).
_METHOD_KEY: Final[str] = 'method'
_PARAMS_KEY: Final[str] = 'params'
_TYPE_NAME_KEY: Final[str] = 'typeName'
_RESULTS_LIMIT_KEY: Final[str] = 'resultsLimit'
_SORT_KEY: Final[str] = 'sort'
_SORT_BY_KEY: Final[str] = 'sortBy'
_SORT_DIRECTION_KEY: Final[str] = 'sortDirection'
_OFFSET_KEY: Final[str] = 'offset'
_GET_METHOD: Final[str] = 'Get'
_GET_COUNT_OF_METHOD: Final[str] = 'GetCountOf'
_ID_SORT: Final[str] = 'id'
_ASCENDING: Final[str] = 'asc'

_DEVICE_TYPE_NAME: Final[str] = 'Device'


def _server_host(config: GeotabConfig) -> str:
    """The authentication host the spec URLs are built on.

    Args:
        config: The validated GeoTab configuration.

    Returns:
        ``auth.server`` when a credential is configured; the placeholder
        default otherwise (a credential-less config still builds every
        discovered leaf -- the registry walk requires it -- but can never
        fetch, so the placeholder never reaches the wire).
    """
    if config.auth is not None:
        return config.auth.server
    return _DEFAULT_SERVER


@dataclass(frozen=True, slots=True)
class _GeotabGetSpecBuilder:
    """Build the seek walk's first ``Get`` request.

    The probed first-request shape (captured 2026-07-09, the seek
    boundary fixture): ``sort`` inside ``params`` with ``sortBy: id``,
    ``sortDirection: asc``, and an EXPLICIT null ``offset`` -- the
    probed shape, not an absent key. ``lastId`` is never written
    (probe-settled decision 1; the docs name it an ``ArgumentException``
    beside id-sort). Every request after this one is the decoder's.

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy after Authenticate).
        type_name: The GeoTab entity to walk (``'Device'``).
        results_limit: The page size; 5000 -- the largest sound page
            under the silent cap.
    """

    server: str
    type_name: str
    results_limit: int

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        """Build the walk's first request.

        Args:
            resume: Accepted to satisfy the protocol; unused -- a
                snapshot resumes from nothing.
            path_values: Accepted to satisfy the protocol; unused --
                there is no URL-path fan-out.

        Returns:
            A credential-less JSON-RPC POST; ``params.credentials`` and
            the resolved host are the session strategy's injections.
        """
        json_body: dict[str, JsonValue] = {
            _METHOD_KEY: _GET_METHOD,
            _PARAMS_KEY: {
                _TYPE_NAME_KEY: self.type_name,
                _RESULTS_LIMIT_KEY: self.results_limit,
                _SORT_KEY: {
                    _SORT_BY_KEY: _ID_SORT,
                    _SORT_DIRECTION_KEY: _ASCENDING,
                    _OFFSET_KEY: None,
                },
            },
        }
        return RequestSpec(
            method=HttpMethod.POST,
            url=f'https://{self.server}{_API_PATH}',
            json_body=json_body,
        )


class _GetCountOfEnvelope(BaseModel):
    """Envelope slice: ``GetCountOf`` returns the count under ``result``.

    strict=True so a stringly count fails loudly instead of coercing
    (and a boolean never passes as an integer); extra='ignore' per the
    house slice-model pattern.
    """

    model_config = ConfigDict(frozen=True, extra='ignore', strict=True)

    result: int


@dataclass(frozen=True, slots=True)
class GetCountOfCheck:
    """The GeoTab completeness check: ``GetCountOf`` as truth instrument.

    Fires one ``GetCountOf`` JSON-RPC request for the harvested entity
    through the same open client the harvest used -- session auth, the
    limiter (one token on the given scope, the token-per-attempt law),
    and the classifier all apply -- and reads the integer count through
    a private envelope slice. Declared on the devices definition so the
    single-fetch driver can prove the capped ``Get`` walk lost nothing
    (probe-settled decision 2).

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy, exactly as the data requests are).
        type_name: The GeoTab entity to count (``'Device'``).
    """

    server: str
    type_name: str

    def expected_count(self, client: TransportClient, quota_scope: str) -> int:
        """Return GeoTab's reported count of the harvested entity.

        Args:
            client: The open transport client the harvest ran on.
            quota_scope: The endpoint's rate-limit scope key
                (``GEOTAB_GET`` -- the count spends from the same
                method-class budget as the data pages).

        Returns:
            The provider-reported entity count.

        Raises:
            ProviderResponseError: The envelope's ``result`` is not an
                integer (via the slice model), or the request failed
                fatally (via the client).
        """
        spec = RequestSpec(
            method=HttpMethod.POST,
            url=f'https://{self.server}{_API_PATH}',
            json_body={
                _METHOD_KEY: _GET_COUNT_OF_METHOD,
                _PARAMS_KEY: {_TYPE_NAME_KEY: self.type_name},
            },
        )
        envelope = client.fetch_envelope(spec, quota_scope)
        return validated_envelope_slice(_GetCountOfEnvelope, envelope).result


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[Device]:
    """Build the GeoTab devices snapshot binding.

    A full-listing snapshot of the account's Device entities (tracked
    vehicles and trailer entries alike): no resume, a single parquet
    file, full replacement each run. Records arrive as a plain list
    under ``result``, walked by id-ascending seek pages under the
    silent 5,000-record ``Get`` cap, and every harvest is verified
    against ``GetCountOf`` before anything flows downstream.

    Args:
        config: The validated GeoTab configuration; supplies the
            authentication host the pre-auth spec URLs are built on.

    Returns:
        The frozen devices ``EndpointDefinition``.
    """
    server = _server_host(config)
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='devices',
        spec_builder=_GeotabGetSpecBuilder(
            server=server,
            type_name=_DEVICE_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabGetPageDecoder(),
        response_model=Device,
        quota_scope=QuotaScope.GEOTAB_GET,
        storage_kind=StorageKind.SINGLE,
        sync_mode=SnapshotMode(),
        completeness_check=GetCountOfCheck(server=server, type_name=_DEVICE_TYPE_NAME),
    )
