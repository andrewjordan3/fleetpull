# src/fleetpull/records/dataframe.py
"""DataFrame construction and missing-value normalization.

Builds a Polars DataFrame from flat rows using an explicit derived schema
(construct-with-schema, never infer-then-cast), and normalizes empty
strings to null at the DataFrame boundary -- the models preserve ``""``
faithfully from the wire, but the frame uses one uniform missing-value
representation.
"""

from collections.abc import Sequence
from typing import Any

import polars as pl

__all__: list[str] = ['build_dataframe', 'normalize_empty_strings']


def build_dataframe(
    # typing-justified: row values are heterogeneous model-field values
    rows: Sequence[dict[str, Any]],
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Construct a typed DataFrame from flat rows and a derived schema.

    Args:
        rows: The flattened ``{column: value}`` rows. An empty sequence is
            valid and yields a zero-row frame with the right columns and
            dtypes.
        schema: The ``{column: dtype}`` map from ``derive_schema``;
            supplies both the column set/order and the dtypes.

    Returns:
        A DataFrame whose columns match the schema in declaration order,
        each at its derived dtype.
    """
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


def normalize_empty_strings(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Replace ``""`` with null on every String column.

    Args:
        dataframe: The frame to normalize.

    Returns:
        The frame with empty strings nulled on String columns; other
        dtypes are untouched (they cannot hold an empty string).
    """
    string_columns: list[str] = [
        name for name, dtype in dataframe.schema.items() if dtype == pl.String()
    ]
    if not string_columns:
        return dataframe
    return dataframe.with_columns(
        pl.when(pl.col(column).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(column))
        .alias(column)
        for column in string_columns
    )
