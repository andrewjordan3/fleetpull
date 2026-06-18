# src/fleetpull/records/flatten.py
"""Record flattening: a model instance to a flat ``{column: value}`` row.

The value side of the shared field walk -- it pulls each leaf's value by
the attribute path the same walk produced, so column names and values are
guaranteed aligned. A ``None`` nested block yields ``None`` for all of its
leaf columns; an enum value is reduced to its plain string.
"""

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from fleetpull.records.fields import FieldKind, FlatField

__all__: list[str] = ['flatten_record']


def flatten_record(
    record: BaseModel, flat_fields: Iterable[FlatField]
) -> dict[str, Any]:
    """Pull one flat row of values from a model instance.

    Args:
        record: The validated model instance to flatten.
        flat_fields: The leaf fields (from ``iter_flat_fields`` on the
            record's class), reused across every record of a batch.

    Returns:
        A ``{column: value}`` row. A missing (``None``) nested block
        yields ``None`` for each of its columns; enum values reduce to
        ``str``; scalars, datetimes, and list values pass through.
    """
    row: dict[str, Any] = {}
    for flat_field in flat_fields:
        value: Any = _pull(record, flat_field.path)
        if value is not None and flat_field.kind is FieldKind.ENUM:
            # All current enums are StrEnum, so str(member) is the wire
            # value ('active'), not 'VehicleStatus.ACTIVE'.
            value = str(value)
        row[flat_field.column] = value
    return row


def _pull(record: BaseModel, path: tuple[str, ...]) -> Any:
    """Walk the attribute path; short-circuit to ``None`` on a null block."""
    current: Any = record
    for attribute in path:
        if current is None:
            return None
        current = getattr(current, attribute)
    return current
