# src/fleetpull/records/validation.py
"""Per-record validation: raw response dicts to typed model instances.

Validates each record dict into the endpoint's response model. Pydantic's
lax coercion (the model is non-strict) lands wire types; field-level
wire-cleaning, when an endpoint needs it, lives on the model as a
``field_validator(mode='before')``, not here. Validation fails fast and
loud on the first bad record, with safe-to-log context.
"""

from collections.abc import Sequence

from pydantic import BaseModel, ValidationError

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import JsonObject

__all__: list[str] = ['validate_records']


def validate_records[ModelT: BaseModel](
    records: Sequence[JsonObject], model_class: type[ModelT]
) -> list[ModelT]:
    """Validate raw record dicts into typed model instances.

    Args:
        records: The raw record objects (a page's ``records``).
        model_class: The response model to validate each record into.

    Returns:
        The validated model instances, one per input record, in order.

    Raises:
        ProviderResponseError: On the first record that fails validation,
            naming the model, the record's position, and which fields
            failed and how -- never the raw field values, so the error is
            safe to log.
    """
    validated: list[ModelT] = []
    for index, record in enumerate(records):
        try:
            validated.append(model_class.model_validate(record))
        except ValidationError as error:
            raise ProviderResponseError(
                detail=(
                    f'{model_class.__name__} record {index} failed '
                    f'validation: {_safe_summary(error)}'
                )
            ) from None
    return validated


def _safe_summary(error: ValidationError) -> str:
    """Summarize a ValidationError without exposing raw field values."""
    parts: list[str] = [
        f'{".".join(str(item) for item in entry["loc"])} ({entry["type"]})'
        for entry in error.errors()
    ]
    return '; '.join(parts)
