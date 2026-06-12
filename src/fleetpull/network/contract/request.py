# src/fleetpull/network/contract/request.py
"""Request description: the spec endpoint definitions build, auth
strategies transform, and the client executes.

A ``RequestSpec`` is a pure description of one HTTP request — no
transport, no credentials until an ``AuthStrategy`` injects them.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Self

__all__: list[str] = ['HttpMethod', 'JsonScalar', 'JsonValue', 'RequestSpec']

# The actual type of a JSON document, recursively. Used for JSON-RPC
# bodies here and available to siblings.
type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]


class HttpMethod(StrEnum):
    """HTTP methods fleetpull uses.

    GET and POST only — fleetpull never writes to providers. Add
    members only when a real endpoint needs them.
    """

    GET = 'GET'
    POST = 'POST'


@dataclass(frozen=True, slots=True)
class RequestSpec:
    """
    Pure description of one HTTP request.

    Built by endpoint definitions, transformed by auth strategies,
    executed by the client. Frozen prevents rebinding, not mutation of
    the contained mappings — they are treated as immutable by
    convention.

    Attributes:
        method: The HTTP method.
        url: Fully qualified request URL.
        headers: Request headers (empty by default).
        params: Query parameters, or None.
        json_body: JSON request body (the GeoTab JSON-RPC envelope
            lives here), or None.
    """

    method: HttpMethod
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    params: Mapping[str, str] | None = None
    json_body: Mapping[str, JsonValue] | None = None

    def with_extra_headers(self, extra: Mapping[str, str]) -> Self:
        """
        Return a NEW spec whose headers merge existing and ``extra``.

        ``extra`` wins on key collisions. The original spec is
        unchanged.

        Args:
            extra: Headers to add or override.

        Returns:
            A new ``RequestSpec`` with the merged headers; every other
            field is preserved.
        """
        return replace(self, headers={**self.headers, **extra})

    def with_merged_params(self, overrides: Mapping[str, str]) -> Self:
        """
        Copy of this spec with ``overrides`` merged into ``params``
        (add-or-replace per key; existing keys not named are kept).

        Args:
            overrides: Query parameters to add or replace.

        Returns:
            The new spec; ``self`` is unchanged.
        """
        return replace(self, params={**(self.params or {}), **overrides})

    def with_json_body(self, json_body: Mapping[str, JsonValue]) -> Self:
        """
        Copy of this spec with ``json_body`` replaced wholesale.

        Args:
            json_body: The replacement body.

        Returns:
            The new spec; ``self`` is unchanged.
        """
        return replace(self, json_body=json_body)
