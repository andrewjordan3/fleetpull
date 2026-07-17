# src/fleetpull/network/contract/envelopes.py
"""Validate-or-raise for provider response envelopes.

Relocated here from ``page_decoder.py`` at its second consumer (the
GeoTab authenticator): the composition — ``model_validate`` and, on
failure, raise ``ProviderResponseError`` carrying Pydantic's complaint
— is contract-layer semantics, not page-decoding semantics, so it is
named and homed neutrally where both consumers can reach it without a
name lie.
"""

from typing import cast

from pydantic import BaseModel, ValidationError

from fleetpull.exceptions import ProviderResponseError
from fleetpull.vocabulary import JsonObject, JsonValue

__all__: list[str] = [
    'require_record_list',
    'unwrap_record_objects',
    'validated_envelope_slice',
]


def validated_envelope_slice[ModelT: BaseModel](
    model_type: type[ModelT], envelope: JsonValue
) -> ModelT:
    """
    Validate a response envelope against a private slice model,
    translating failure into the contract's single-action raise.

    Shared rather than reimplemented per consumer because the
    composition — validate, and on failure raise
    ``ProviderResponseError`` carrying Pydantic's complaint — is layer
    semantics, not provider behavior.

    Args:
        model_type: The consumer's private envelope-slice model.
        envelope: The parsed response body (any shape; a non-dict
            fails validation like any other malformation).

    Returns:
        The validated slice.

    Raises:
        ProviderResponseError: When the envelope does not satisfy the
            slice model.
    """
    try:
        return model_type.model_validate(envelope)
    except ValidationError as error:
        raise ProviderResponseError(
            detail=f'malformed response envelope: {error}'
        ) from error


def _require_json_object(value: JsonValue) -> JsonObject:
    """Return ``value`` narrowed to a JSON object, or raise.

    Args:
        value: The parsed value under inspection.

    Returns:
        The value as a JSON object.

    Raises:
        ProviderResponseError: When the value is not a JSON object.
    """
    if not isinstance(value, dict):
        raise ProviderResponseError(detail='response envelope is not a JSON object')
    return value


def require_record_list(envelope: JsonValue, key: str) -> list[JsonObject]:
    """Return the record list at a top-level envelope key.

    Validates, in order: the envelope is a JSON object; ``key`` is
    present; its value is a list; every element is a JSON object.

    Args:
        envelope: The parsed response body.
        key: The top-level key whose value is the record list.

    Returns:
        The record list at ``key``, each element a JSON object.

    Raises:
        ProviderResponseError: When the envelope is not an object,
            ``key`` is absent, its value is not a list, or an element is
            not a JSON object — each with a message naming the failure.
    """
    response = _require_json_object(envelope)
    if key not in response:
        raise ProviderResponseError(
            detail=f'response envelope is missing the record key {key!r}'
        )
    records = response[key]
    if not isinstance(records, list):
        raise ProviderResponseError(detail=f'record key {key!r} is not a list')
    for record in records:
        if not isinstance(record, dict):
            raise ProviderResponseError(
                detail=f'record under {key!r} is not a JSON object'
            )
    return cast(list[JsonObject], records)


def unwrap_record_objects(
    wrappers: list[JsonObject], item_key: str
) -> list[JsonObject]:
    """Lift the inner object out of each single-key record wrapper.

    Motive returns each record wrapped — ``{"vehicle": {...}}`` inside
    the ``vehicles`` list; this lifts the inner object out of every
    wrapper. Each wrapper must carry ``item_key`` and its value must be
    a JSON object.

    Args:
        wrappers: The wrapper objects (already validated as objects).
        item_key: The key inside each wrapper holding the record.

    Returns:
        The unwrapped record objects.

    Raises:
        ProviderResponseError: When a wrapper lacks ``item_key`` or its
            value is not a JSON object.
    """
    records: list[JsonObject] = []
    for wrapper in wrappers:
        if item_key not in wrapper:
            raise ProviderResponseError(
                detail=f'record wrapper is missing the item key {item_key!r}'
            )
        inner = wrapper[item_key]
        if not isinstance(inner, dict):
            raise ProviderResponseError(
                detail=f'record under {item_key!r} is not a JSON object'
            )
        records.append(inner)
    return records
