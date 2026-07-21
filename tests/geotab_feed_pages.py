# tests/geotab_feed_pages.py
"""The one GetFeed-envelope narrowing helper the feed capture modules share.

The same-provider feed capture modules all narrow the identical
``result.data`` shape; the helper lives once here (the
extract-duplicated-logic rule -- same provider, so the
cross-provider-boundary exception does not apply).
"""

from fleetpull.vocabulary import JsonObject, JsonValue

__all__: list[str] = ['feed_records']


def feed_records(envelope: dict[str, JsonValue]) -> list[JsonObject]:
    """Narrow a GetFeed envelope's ``result.data`` to its record list.

    The fixtures are known-good record lists; the asserts exist for the
    type checker (and would fail loudly if a fixture were ever edited
    into a different shape).
    """
    result = envelope['result']
    assert isinstance(result, dict)
    records = result['data']
    assert isinstance(records, list)
    narrowed: list[JsonObject] = []
    for record in records:
        assert isinstance(record, dict)
        narrowed.append(record)
    return narrowed
