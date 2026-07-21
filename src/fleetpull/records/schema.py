# src/fleetpull/records/schema.py
"""Polars schema derivation: a model class to a ``{column: dtype}`` map.

Walks the shared field enumeration (``records/fields.py``) and maps each
leaf to a Polars dtype: ``int``/``float``/``str``/``bool`` to their
obvious scalars, ``date`` to a calendar-date column (a DATE-ONLY wire
value carries no instant to recover -- the Motive users ``joined_at``
precedent), ``datetime`` to tz-aware microsecond UTC, ``timedelta``
to a microsecond ``Duration``, enums to ``String``, ``list[scalar]`` to
``List`` of the inner dtype. Depends only on the model class, not on any
data, so the schema is computable before a record is fetched. A leaf the
map cannot place raises (fail fast); there is no override path yet.
"""

from datetime import date, datetime, timedelta

import polars as pl
from pydantic import BaseModel

from fleetpull.records.fields import FieldKind, FlatField, iter_flat_fields

__all__: list[str] = ['derive_schema']

# The closed scalar -> Polars dtype map. datetime is tz-aware microsecond
# UTC and timedelta a microsecond Duration, so every temporal column
# carries a uniform, parquet-friendly type.
_SCALAR_TO_POLARS: dict[type, pl.DataType] = {
    int: pl.Int64(),
    float: pl.Float64(),
    str: pl.String(),
    bool: pl.Boolean(),
    date: pl.Date(),
    datetime: pl.Datetime(time_unit='us', time_zone='UTC'),
    timedelta: pl.Duration(time_unit='us'),
}


def derive_schema(model_class: type[BaseModel]) -> dict[str, pl.DataType]:
    """Derive the Polars schema for a model's flattened columns.

    Args:
        model_class: The model whose flattened leaves define the columns.

    Returns:
        An insertion-ordered ``{column: dtype}`` map in field declaration
        order. Scalars, datetimes, and timedeltas map directly; enums map
        to ``pl.String``; ``list[scalar]`` maps to ``pl.List`` of the
        inner scalar's dtype.

    Raises:
        TypeError: If a leaf annotation has no dtype mapping.
        ValueError: If two leaves resolve to one column name -- a
            structural impossibility the double-underscore join prevents,
            raised as a guard that should never fire.
    """
    schema: dict[str, pl.DataType] = {}
    for flat_field in iter_flat_fields(model_class):
        if flat_field.column in schema:
            raise ValueError(
                f'{model_class.__name__}: duplicate flattened column '
                f'{flat_field.column!r} -- two fields resolve to one name.'
            )
        schema[flat_field.column] = _leaf_to_dtype(flat_field)
    return schema


def _leaf_to_dtype(flat_field: FlatField) -> pl.DataType:
    """Map one resolved leaf to its Polars dtype."""
    if flat_field.kind is FieldKind.ENUM:
        return pl.String()
    if flat_field.kind is FieldKind.LIST_OF_SCALAR:
        return pl.List(_SCALAR_TO_POLARS[flat_field.resolved])
    return _SCALAR_TO_POLARS[flat_field.resolved]
