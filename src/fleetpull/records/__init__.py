# src/fleetpull/records/__init__.py
"""The records layer: validate response records and shape them into typed
Polars DataFrames. Per-record validation (``validate_records``) and the
model-to-DataFrame conversion (``models_to_dataframe``) are the two public
entry points; the driver composes validation with the network client."""

from fleetpull.records.convert import models_to_dataframe
from fleetpull.records.validation import validate_records

__all__: list[str] = ['models_to_dataframe', 'validate_records']
