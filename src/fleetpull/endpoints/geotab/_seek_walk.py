# src/fleetpull/endpoints/geotab/_seek_walk.py
"""The GeoTab snapshot seek-walk machinery shared by the ``Get`` leaves.

Promoted out of the devices leaf when the users leaf became its second
consumer (the promotion-on-second-user rule): the seek-walk spec builder
and the ``GetCountOf`` completeness check are entity-generic --
``type_name`` is a parameter -- and both users and devices proved the
same per-type facts live (id-sort supported, the boundary advance exact,
``GetCountOf`` the truth instrument; Device captured 2026-07-09, User
2026-07-16). Underscore-prefixed so the registry walk skips it: this
module is machinery, not an endpoint leaf.

Plain ``Get`` silently hard-caps at 5,000 records with no continuation
signal; a captured ``GetCountOf`` above the cap proved records beyond
5,000 are invisible to bare ``Get``. That shapes both strategies here:
the seek walk (``sort`` by ``id`` ascending, offset carried by the
decoder) and the ``GetCountOfCheck`` truth instrument fired beside
every harvest.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from pydantic import BaseModel, ConfigDict

from fleetpull.endpoints.shared import ResumeValue
from fleetpull.network.client import TransportClient
from fleetpull.network.contract import (
    HttpMethod,
    RequestSpec,
    validated_envelope_slice,
)
from fleetpull.vocabulary import JsonValue

__all__: list[str] = ['GeotabGetSpecBuilder', 'GetCountOfCheck']

# The JSON-RPC ingress path every GeoTab method POSTs to.
_API_PATH: Final[str] = '/apiv1'

# Wire-protocol tokens: module-private Final constants, colocated with
# the strategies that emit them (the constants-scope precedent;
# deliberately unshared, even with the leaves' and decoder module's own
# copies).
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


@dataclass(frozen=True, slots=True)
class GeotabGetSpecBuilder:
    """Build the seek walk's first ``Get`` request.

    The probed first-request shape (captured 2026-07-09 on Device,
    re-proven 2026-07-16 on User -- id-sortability is per-type, never
    assumed): ``sort`` inside ``params`` with ``sortBy: id``,
    ``sortDirection: asc``, and an EXPLICIT null ``offset`` -- the
    probed shape, not an absent key. ``lastId`` is never written
    (probe-settled decision; the docs name it an ``ArgumentException``
    beside id-sort). Every request after this one is the decoder's.

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy after Authenticate).
        type_name: The GeoTab entity to walk (``'Device'``, ``'User'``).
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
    a private envelope slice. Declared on the snapshot definitions so
    the single-fetch driver can prove the capped ``Get`` walk lost
    nothing (probe-settled decision).

    Attributes:
        server: The pre-auth authentication host (retargeted by the
            session strategy, exactly as the data requests are).
        type_name: The GeoTab entity to count (``'Device'``, ``'User'``).
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
