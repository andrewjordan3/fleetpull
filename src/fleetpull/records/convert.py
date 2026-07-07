# src/fleetpull/records/convert.py
"""The records composition: validated model instances to a DataFrame.

Ties the stage's pure steps together -- derive the schema from the model
class, flatten every instance against the shared field walk, build the
frame with the explicit schema, and normalize empty strings. Per-record
validation (raw dict to model) is separate (``records/validation.py``);
composing it with the network client is the driver's job, since records
does not import the client.
"""

from collections.abc import Sequence
from typing import Any

import polars as pl

from fleetpull.model_contract import ResponseModel
from fleetpull.records.dataframe import build_dataframe, normalize_empty_strings
from fleetpull.records.fields import iter_flat_fields
from fleetpull.records.flatten import flatten_record
from fleetpull.records.schema import derive_schema

__all__: list[str] = ['models_to_dataframe']


def models_to_dataframe(
    records: Sequence[ResponseModel], model_class: type[ResponseModel]
) -> pl.DataFrame:
    """Build a typed, flattened DataFrame from validated model instances.

    Args:
        records: The validated model instances to materialize. An empty
            sequence yields a zero-row frame with the model's full schema.
        model_class: The model class whose fields define the schema;
            passed explicitly so an empty ``records`` still derives the
            full column set.

    Returns:
        A DataFrame with one row per record, flattened columns in
        declaration order, derived dtypes, and empty strings normalized to
        null.

    Raises:
        TypeError: If any field annotation cannot be mapped to a dtype.
    """
    schema: dict[str, pl.DataType] = derive_schema(model_class)
    flat_fields = tuple(iter_flat_fields(model_class))
    # typing-justified: flattened rows carry heterogeneous model-field values
    rows: list[dict[str, Any]] = [
        flatten_record(record=record, flat_fields=flat_fields) for record in records
    ]
    dataframe: pl.DataFrame = build_dataframe(rows=rows, schema=schema)
    return normalize_empty_strings(dataframe)
