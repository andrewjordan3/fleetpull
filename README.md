# fleetpull

fleetpull pulls fleet telematics data from provider APIs (Motive, Samsara,
GeoTab) and delivers it as typed, dtype-coerced, lightly normalized tabular
output — Polars DataFrames and parquet — staying as close to the raw API
responses as is reasonable. It does no cross-endpoint merging, builds no
unified schema, and loads no warehouse; downstream processing is the
consumer's concern.

**Status:** under active development; the public API has not yet stabilized.

Architecture and design rationale live in [DESIGN.md](DESIGN.md).

Licensed under the [Apache License 2.0](LICENSE).
