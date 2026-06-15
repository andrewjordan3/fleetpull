# src/fleetpull/network/contract/envelopes.py
"""Validate-or-raise for provider response envelopes.

Relocated here from ``pagination.py`` at its second consumer (the
GeoTab authenticator): the composition — ``model_validate`` and, on
failure, raise ``ProviderResponseError`` carrying Pydantic's complaint
— is contract-layer semantics, not pagination semantics, so it is named
and homed neutrally where both consumers can reach it without a name
lie.
"""

import logging

from pydantic import BaseModel, ValidationError

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract.request import JsonValue

__all__: list[str] = ['validated_envelope_slice']

logger = logging.getLogger(__name__)


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
