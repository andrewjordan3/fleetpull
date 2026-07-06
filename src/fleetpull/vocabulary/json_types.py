# src/fleetpull/vocabulary/json_types.py
"""JSON type vocabulary: the recursive shape of a JSON document.

The package-wide aliases for JSON data -- spoken by the network contract
(request bodies, response envelopes, page decoding), the records layer (raw
record dicts entering validation), and the orchestrator (batch plumbing).
Homed in ``vocabulary`` because the concept is generic JSON, not a network
contract: three layers speak it, so it lives in the dependency-free leaf
they all sit above, not in the transport package one of them happens to be.
"""

__all__: list[str] = ['JsonObject', 'JsonScalar', 'JsonValue']

# The actual type of a JSON document, recursively. Used for JSON-RPC
# bodies and response envelopes; available to every layer.
type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
# A single JSON object -- the shape one extracted record takes; the endpoints
# layer types the record extractor's output as ``list[JsonObject]``.
type JsonObject = dict[str, JsonValue]
