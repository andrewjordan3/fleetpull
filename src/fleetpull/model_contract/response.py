# src/fleetpull/model_contract/response.py
"""The shared config-policy base every per-record response model extends.

Carries only configuration — no fields, no methods, no schema-derivation helpers
(those are the records layer's concern, §9, and attach when it is built). Models
that extend ``ResponseModel`` stay pure API mirrors.
"""

from pydantic import BaseModel, ConfigDict

__all__: list[str] = ['ResponseModel']


class ResponseModel(BaseModel):
    """
    Config-policy base for response models; subclasses add only fields.

    Each ``model_config`` setting is deliberate:

    - ``frozen=True`` — verified inbound data is immutable once validated.
    - ``extra='ignore'`` — response models mirror evolving provider APIs, so a
      field a provider adds is dropped, not a crash (records derives the Polars
      schema from declared fields, so an unknown field has nowhere to land
      anyway). A deliberate departure from the house ``extra='forbid'`` default,
      justified for inbound mirrors specifically.
    - non-strict (the absence of ``strict=True``) — Pydantic's lax coercion turns
      Motive's stringly-typed numerics into typed values; ``strict=True`` would
      reject them, and the §9 coercion overrides only handle what lax cannot. (The
      GeoTab auth slice models use ``strict=True`` because auth responses are
      well-typed — the opposite case.)
    - ``populate_by_name=True`` — models alias camelCase wire keys to snake_case
      fields; this lets construction by either the alias or the field name
      succeed.
    - ``str_strip_whitespace=True`` — trims incoming strings as structural
      hygiene, never a semantic transform.
    - ``validate_default=True`` — house standard.
    """

    model_config = ConfigDict(
        frozen=True,
        extra='ignore',
        populate_by_name=True,
        str_strip_whitespace=True,
        validate_default=True,
    )
