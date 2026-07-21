"""Tests for fleetpull.network.decoders.geotab.

Feed fixtures upgraded to the 2026-07-09 live captures where shapes
match (the live-observed caught-up short page); remaining feed bodies
are synthetic,
constructed in the verified GetFeed shapes: zeroed-pattern version
strings, scrubbed ids, real camelCase keys. The seek-paging Get tests
run on the committed boundary capture
(``tests/geotab_devices_capture.py``).
"""

import json

import pytest

from fleetpull.exceptions import ProviderResponseError
from fleetpull.network.contract import PageDecoder
from fleetpull.network.contract.request import HttpMethod, RequestSpec
from fleetpull.network.decoders import GeotabFeedPageDecoder, GeotabGetPageDecoder
from fleetpull.vocabulary import JsonValue
from tests.geotab_devices_capture import (
    SEEK_PAGE_1_REQUEST,
    SEEK_PAGE_1_RESPONSE,
    SEEK_PAGE_2_REQUEST,
    SEEK_PAGE_2_RESPONSE,
    SEEK_TERMINAL_RESPONSE,
)
from tests.geotab_trips_capture import (
    TRIP_SEEK_PAGE_1_REQUEST,
    TRIP_SEEK_PAGE_1_RESPONSE,
    TRIP_SEEK_PAGE_2_REQUEST,
)

BOOTSTRAP_SEARCH: dict[str, JsonValue] = {'fromDate': '2026-06-01T00:00:00Z'}

# Captured: bare GetFeed request (2026-07-09) -- no fromVersion, no
# search: the cursor-at-now call. The token-advance request (the P5
# advance capture, not the P4 bootstrap page) is this body plus
# "fromVersion": "000000000014c3e0"; credentials are the session
# strategy's injection.
GETFEED_BARE_REQUEST_JSON: str = (
    '{"method": "GetFeed", "params": {"typeName": "LogRecord",'
    ' "resultsLimit": 3, "credentials": {"database": "exampledb",'
    ' "userName": "user@example.com",'
    ' "sessionId": "SyntheticSessionId000001"}}}'
)

# Captured: the bare call's response (2026-07-09) -- the
# empty-data-with-toVersion envelope shape; toVersion surfaces even
# when nothing streamed.
GETFEED_EMPTY_RESPONSE_JSON: str = (
    '{"result": {"data": [], "toVersion": "000000000014c3e0"}, "jsonrpc": "2.0"}'
)

# Captured: the token-advance page (2026-07-09; the P5 advance capture,
# not the P4 bootstrap) -- data-bearing, the six-field LogRecord shape,
# strict advance from the bare call's toVersion.
GETFEED_ADVANCE_RESPONSE_JSON: str = (
    '{"result": {"data": [{"latitude": 40.1000001, "longitude":'
    ' -100.1000001, "speed": 96, "dateTime": "2026-07-09T15:34:55.000Z",'
    ' "device": {"id": "b8E2"}, "id": "b14c3e1"}, {"latitude":'
    ' 40.2018276, "longitude": -100.2018276, "speed": 9, "dateTime":'
    ' "2026-07-09T15:34:56.000Z", "device": {"id": "b8E7"}, "id":'
    ' "b14c3e2"}, {"latitude": 40.3000003, "longitude": -100.3000003,'
    ' "speed": 0, "dateTime": "2026-07-09T15:34:56.000Z", "device":'
    ' {"id": "b8EB"}, "id": "b14c3e3"}], "toVersion":'
    ' "000000000014c3e3"}, "jsonrpc": "2.0"}'
)


def build_feed_spec(results_limit: int = 2) -> RequestSpec:
    return RequestSpec(
        method=HttpMethod.POST,
        url='https://resolved.example.geotab.com/apiv1',
        json_body={
            'method': 'GetFeed',
            'params': {
                'typeName': 'LogRecord',
                'resultsLimit': results_limit,
                'search': BOOTSTRAP_SEARCH,
            },
        },
    )


def build_envelope(record_count: int, to_version: str) -> dict[str, JsonValue]:
    records: list[JsonValue] = [
        {'id': f'synthetic-{record_index}'} for record_index in range(record_count)
    ]
    return {
        'result': {'data': records, 'toVersion': to_version},
        'jsonrpc': '2.0',
    }


class TestGeotabFeedPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = GeotabFeedPageDecoder()
        assert isinstance(decoder, GeotabFeedPageDecoder)

    def test_first_request_is_identity(self) -> None:
        spec = build_feed_spec()
        assert GeotabFeedPageDecoder().first_request(spec) is spec

    def test_advance_strips_search_and_sets_from_version(self) -> None:
        # Body upgraded to the captured advance page; assertions unchanged.
        decoded = GeotabFeedPageDecoder().decode_page(
            build_feed_spec(results_limit=3),
            json.loads(GETFEED_ADVANCE_RESPONSE_JSON),
        )
        assert len(decoded.records) == 3
        assert decoded.advance.next_spec is not None
        assert decoded.advance.next_spec.json_body is not None
        next_params = decoded.advance.next_spec.json_body['params']
        assert isinstance(next_params, dict)
        # Both properties of the rewrite, asserted explicitly:
        assert 'search' not in next_params
        assert next_params['fromVersion'] == '000000000014c3e3'
        # Untouched params survive the rewrite.
        assert next_params['typeName'] == 'LogRecord'
        assert next_params['resultsLimit'] == 3

    def test_full_page_continues(self) -> None:
        # Body upgraded to the captured advance page; assertions unchanged.
        decoded = GeotabFeedPageDecoder().decode_page(
            build_feed_spec(results_limit=3),
            json.loads(GETFEED_ADVANCE_RESPONSE_JSON),
        )
        assert [record['id'] for record in decoded.records] == [
            'b14c3e1',
            'b14c3e2',
            'b14c3e3',
        ]
        assert decoded.advance.next_spec is not None
        assert decoded.advance.durable_progress == '000000000014c3e3'

    def test_short_page_completes_with_durable_progress(self) -> None:
        decoded = GeotabFeedPageDecoder().decode_page(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=1, to_version='0000000000000002'),
        )
        assert decoded.records == [{'id': 'synthetic-0'}]
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress == '0000000000000002'

    def test_empty_page_completes_with_the_envelope_to_version(self) -> None:
        # Body upgraded to the captured bare-call (cursor-at-now) envelope;
        # assertions unchanged.
        decoded = GeotabFeedPageDecoder().decode_page(
            build_feed_spec(results_limit=3),
            json.loads(GETFEED_EMPTY_RESPONSE_JSON),
        )
        assert decoded.records == []
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress == '000000000014c3e0'

    def test_every_page_carries_durable_progress(self) -> None:
        decoder = GeotabFeedPageDecoder()
        continuing = decoder.decode_page(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=2, to_version='0000000000000004'),
        )
        terminal = decoder.decode_page(
            build_feed_spec(results_limit=2),
            build_envelope(record_count=0, to_version='0000000000000005'),
        )
        assert continuing.advance.durable_progress == '0000000000000004'
        assert terminal.advance.durable_progress == '0000000000000005'

    @pytest.mark.parametrize(
        'envelope',
        [
            {'result': {'data': []}, 'jsonrpc': '2.0'},  # toVersion missing
            {
                'result': {'data': 'not a list', 'toVersion': '0000000000000006'},
                'jsonrpc': '2.0',
            },
            {'jsonrpc': '2.0'},  # result missing entirely
        ],
    )
    def test_malformed_envelope_raises(self, envelope: JsonValue) -> None:
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            GeotabFeedPageDecoder().decode_page(build_feed_spec(), envelope)

    @pytest.mark.parametrize(
        'malformed_spec',
        [
            RequestSpec(method=HttpMethod.POST, url='https://x.example/apiv1'),
            RequestSpec(
                method=HttpMethod.POST,
                url='https://x.example/apiv1',
                json_body={'method': 'GetFeed'},
            ),
            RequestSpec(
                method=HttpMethod.POST,
                url='https://x.example/apiv1',
                json_body={'method': 'GetFeed', 'params': {'typeName': 'LogRecord'}},
            ),
            RequestSpec(
                method=HttpMethod.POST,
                url='https://x.example/apiv1',
                json_body={
                    'method': 'GetFeed',
                    'params': {'typeName': 'LogRecord', 'resultsLimit': 'many'},
                },
            ),
        ],
    )
    def test_malformed_sent_body_raises_value_error(
        self, malformed_spec: RequestSpec
    ) -> None:
        envelope = build_envelope(record_count=0, to_version='0000000000000007')
        with pytest.raises(ValueError, match='GeoTab feed request'):
            GeotabFeedPageDecoder().decode_page(malformed_spec, envelope)


def build_get_spec(
    body: dict[str, JsonValue] | None = None,
) -> RequestSpec:
    """A sent Get spec carrying the captured page-1 request body."""
    return RequestSpec(
        method=HttpMethod.POST,
        url='https://resolved.example.geotab.com/apiv1',
        json_body=body if body is not None else SEEK_PAGE_1_REQUEST,
    )


class TestGeotabGetPageDecoder:
    def test_satisfies_page_decoder_protocol(self) -> None:
        decoder: PageDecoder = GeotabGetPageDecoder()
        assert isinstance(decoder, GeotabGetPageDecoder)

    def test_first_request_is_identity(self) -> None:
        spec = build_get_spec()
        assert GeotabGetPageDecoder().first_request(spec) is spec

    def test_boundary_pair_advances_by_last_id(self) -> None:
        # The captured boundary: page 1's last id becomes page 2's offset,
        # exactly as the committed page-2 request shows.
        decoded = GeotabGetPageDecoder().decode_page(
            build_get_spec(), SEEK_PAGE_1_RESPONSE
        )
        assert [record['id'] for record in decoded.records] == [
            'b8E2',
            'b8E7',
            'b8F3',
        ]
        assert decoded.advance.next_spec is not None
        assert decoded.advance.next_spec.json_body is not None
        next_params = decoded.advance.next_spec.json_body['params']
        assert isinstance(next_params, dict)
        next_sort = next_params['sort']
        assert isinstance(next_sort, dict)
        captured_page_2_params = SEEK_PAGE_2_REQUEST['params']
        assert isinstance(captured_page_2_params, dict)
        assert next_sort == captured_page_2_params['sort']
        # Untouched params survive the rewrite (typeName, resultsLimit,
        # even the captured credentials the strategy owns).
        assert next_params['typeName'] == 'Device'
        assert next_params['resultsLimit'] == 3

    def test_the_captured_boundary_ids_are_hex_consecutive(self) -> None:
        # The no-loss/no-overlap seam, visible in the fixture itself:
        # page 1 ends at 0xb8F3 and page 2 begins at 0xb8F4.
        page_1_records = SEEK_PAGE_1_RESPONSE['result']
        page_2_records = SEEK_PAGE_2_RESPONSE['result']
        assert isinstance(page_1_records, list)
        assert isinstance(page_2_records, list)
        last_of_page_1 = page_1_records[-1]
        first_of_page_2 = page_2_records[0]
        assert isinstance(last_of_page_1, dict)
        assert isinstance(first_of_page_2, dict)
        last_id = last_of_page_1['id']
        first_id = first_of_page_2['id']
        assert isinstance(last_id, str)
        assert isinstance(first_id, str)
        assert int(last_id, 16) + 1 == int(first_id, 16)

    def test_empty_page_terminates_with_no_durable_progress(self) -> None:
        decoded = GeotabGetPageDecoder().decode_page(
            build_get_spec(), SEEK_TERMINAL_RESPONSE
        )
        assert decoded.records == []
        assert decoded.advance.next_spec is None
        assert decoded.advance.durable_progress is None

    def test_short_page_is_not_terminal(self) -> None:
        # Deliberately unlike the feed rule: a page shorter than
        # resultsLimit still advances -- only the empty page terminates.
        wide_body: dict[str, JsonValue] = {
            'method': 'Get',
            'params': {
                'typeName': 'Device',
                'resultsLimit': 5000,
                'sort': {'sortBy': 'id', 'sortDirection': 'asc', 'offset': None},
            },
        }
        decoded = GeotabGetPageDecoder().decode_page(
            build_get_spec(wide_body), SEEK_PAGE_2_RESPONSE
        )
        assert len(decoded.records) == 3  # 3 < 5000: short, yet not terminal
        assert decoded.advance.next_spec is not None
        assert decoded.advance.next_spec.json_body is not None
        next_params = decoded.advance.next_spec.json_body['params']
        assert isinstance(next_params, dict)
        next_sort = next_params['sort']
        assert isinstance(next_sort, dict)
        assert next_sort['offset'] == 'b91C'

    def test_last_id_key_is_never_written(self) -> None:
        # Enforced by construction (probe-settled decision 1: the probe
        # bodies carried "lastId": null, tolerated; the build never sends
        # the key -- docs name it an ArgumentException beside id-sort).
        decoded = GeotabGetPageDecoder().decode_page(
            build_get_spec(), SEEK_PAGE_1_RESPONSE
        )
        assert decoded.advance.next_spec is not None
        assert decoded.advance.next_spec.json_body is not None
        assert 'lastId' not in json.dumps(decoded.advance.next_spec.json_body)

    @pytest.mark.parametrize(
        'envelope',
        [
            {'jsonrpc': '2.0'},  # result missing entirely
            {'result': 'not a list', 'jsonrpc': '2.0'},
            {'result': [{'id': 'b8E2'}, 'not an object'], 'jsonrpc': '2.0'},
        ],
    )
    def test_malformed_envelope_raises(self, envelope: JsonValue) -> None:
        with pytest.raises(ProviderResponseError, match='malformed response envelope'):
            GeotabGetPageDecoder().decode_page(build_get_spec(), envelope)

    def test_record_without_string_id_is_a_provider_violation(self) -> None:
        envelope: dict[str, JsonValue] = {
            'result': [{'name': 'synthetic-unit-001'}],
            'jsonrpc': '2.0',
        }
        with pytest.raises(ProviderResponseError, match='seek paging requires'):
            GeotabGetPageDecoder().decode_page(build_get_spec(), envelope)

    @pytest.mark.parametrize(
        'malformed_body',
        [
            None,  # no JSON-RPC body at all
            {'method': 'Get'},  # params missing
            {'method': 'Get', 'params': {'typeName': 'Device'}},  # no limit
            {  # no sort mapping
                'method': 'Get',
                'params': {'typeName': 'Device', 'resultsLimit': 3},
            },
        ],
    )
    def test_malformed_sent_body_raises_value_error(
        self, malformed_body: dict[str, JsonValue] | None
    ) -> None:
        spec = RequestSpec(
            method=HttpMethod.POST,
            url='https://x.example/apiv1',
            json_body=malformed_body,
        )
        with pytest.raises(ValueError, match='GeoTab Get request'):
            GeotabGetPageDecoder().decode_page(spec, SEEK_TERMINAL_RESPONSE)


def test_windowed_search_survives_the_seek_advance() -> None:
    # The load-bearing trips behavior, pinned against the captured pair
    # (2026-07-13): the seek rewrite must never drop or mutate the window
    # filter -- the advance spreads the sent params, so search rides
    # every page while sort.offset seeks.
    sent = RequestSpec(
        method=HttpMethod.POST,
        url='https://resolved.example.geotab.com/apiv1',
        json_body=TRIP_SEEK_PAGE_1_REQUEST,
    )
    decoded = GeotabGetPageDecoder().decode_page(sent, TRIP_SEEK_PAGE_1_RESPONSE)
    assert decoded.advance.next_spec is not None
    assert decoded.advance.next_spec.json_body is not None
    next_params = decoded.advance.next_spec.json_body['params']
    assert isinstance(next_params, dict)
    # The window filter is byte-identical to the sent one: both dates intact.
    assert next_params['search'] == {
        'fromDate': '2026-07-06T00:00:00Z',
        'toDate': '2026-07-13T00:00:00Z',
    }
    next_sort = next_params['sort']
    assert isinstance(next_sort, dict)
    assert next_sort['offset'] == 'b7F3A83E5'
    # The advance reproduces the captured page-2 request's sort exactly.
    captured_page_2_params = TRIP_SEEK_PAGE_2_REQUEST['params']
    assert isinstance(captured_page_2_params, dict)
    assert next_sort == captured_page_2_params['sort']
