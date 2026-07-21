"""Tests for fleetpull.models.samsara.asset_location.

Every fixture is the committed 2026-07-20 capture set
(``tests/samsara_asset_locations_capture.py``): five fully synthetic
reading records shaped by the 454-record page census (nested blocks
censused over 300). The census-preserved shapes (the str-shaped
``asset.id``, the four-key location core, the OBSERVED-EMPTY
``location.geofence`` dropped by extra-ignore, no speed key anywhere
despite the surface's name) are asserted here beside the model that
mirrors them; requiredness carries drop-key rejection teeth at every
level (the addresses precedent) -- the location core is required by
structural judgment, not whole-population census (module docstring of
the model), and only a loud rejection here keeps a future
optional-demotion from passing every gate.
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.samsara import (
    AssetLocation,
    AssetLocationAssetRef,
    AssetLocationFix,
)
from fleetpull.vocabulary import JsonObject
from tests.samsara_asset_locations_capture import ASSET_LOCATION_RECORDS

# The record's top-level census keys -- 454/454, all required.
_REQUIRED_KEYS = frozenset({'happenedAtTime', 'asset', 'location'})

# The location block's modeled keys -- 300/300 in the block census,
# required by structural judgment (a fix without coordinates mirrors
# nothing). `geofence` is deliberately NOT here: observed-empty, not
# modeled.
_REQUIRED_LOCATION_KEYS = frozenset(
    {'accuracyMeters', 'headingDegrees', 'latitude', 'longitude'}
)

_WINDOW_START = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
_WINDOW_END = datetime(2026, 1, 1, 13, 0, tzinfo=UTC)


def _location_block(record: JsonObject) -> JsonObject:
    location = record['location']
    assert isinstance(location, dict)
    return location


def _asset_block(record: JsonObject) -> JsonObject:
    asset = record['asset']
    assert isinstance(asset, dict)
    return asset


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_every_record_carries_the_required_keys(self) -> None:
        assert len(ASSET_LOCATION_RECORDS) == 5
        for record in ASSET_LOCATION_RECORDS:
            assert set(record) >= _REQUIRED_KEYS
            assert set(_location_block(record)) >= _REQUIRED_LOCATION_KEYS

    def test_every_geofence_is_an_empty_object(self) -> None:
        # 300/300 zero-keys in census: the block is present and empty on
        # every record -- the raw fixtures mirror it so validation below
        # proves extra-ignore drops it.
        for record in ASSET_LOCATION_RECORDS:
            assert _location_block(record)['geofence'] == {}

    def test_no_speed_key_anywhere(self) -> None:
        # Unobserved despite the surface's name (location-and-speed):
        # no speed key appeared in the census, so none rides a fixture.
        for record in ASSET_LOCATION_RECORDS:
            speed_shaped = {
                key
                for scope in (record, _location_block(record))
                for key in scope
                if 'speed' in key.lower()
            }
            assert speed_shaped == set()

    def test_the_asset_ref_is_a_single_string_id(self) -> None:
        # The asset block's ONLY observed key is `id`, a STRING on the
        # wire (the idling_events contrast: bare int there).
        for record in ASSET_LOCATION_RECORDS:
            asset = _asset_block(record)
            assert set(asset) == {'id'}
            assert isinstance(asset['id'], str)

    def test_pages_carry_multiple_recurring_assets(self) -> None:
        # Reading grain: multi-asset pages, the same asset recurring
        # across the walk -- per-record attribution is what makes the
        # batched fan-out pure transport packing.
        asset_ids = [_asset_block(record)['id'] for record in ASSET_LOCATION_RECORDS]
        assert len(set(asset_ids)) == 3
        assert len(asset_ids) > len(set(asset_ids))


class TestAssetLocationValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        record = {
            key: value
            for key, value in ASSET_LOCATION_RECORDS[0].items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            AssetLocation.model_validate(record)

    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_LOCATION_KEYS))
    def test_each_location_core_key_rejects_absence(self, required_key: str) -> None:
        # The structural-judgment requiredness with teeth: a fix
        # missing its core must fail loudly, never land an all-null row.
        record = dict(ASSET_LOCATION_RECORDS[0])
        record['location'] = {
            key: value
            for key, value in _location_block(ASSET_LOCATION_RECORDS[0]).items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            AssetLocation.model_validate(record)

    def test_an_asset_ref_without_id_rejects(self) -> None:
        record = dict(ASSET_LOCATION_RECORDS[0])
        record['asset'] = {}
        with pytest.raises(ValidationError):
            AssetLocation.model_validate(record)

    def test_every_record_validates_with_aware_in_window_times(self) -> None:
        validated = [
            AssetLocation.model_validate(record) for record in ASSET_LOCATION_RECORDS
        ]
        assert len(validated) == 5
        for reading in validated:
            assert reading.happened_at_time.tzinfo is not None
            # The probe's [start, end) anchoring evidence, preserved by
            # the fixtures: every reading inside the 12:00-13:00Z window.
            assert _WINDOW_START <= reading.happened_at_time < _WINDOW_END
            assert isinstance(reading.asset, AssetLocationAssetRef)
            assert isinstance(reading.location, AssetLocationFix)

    def test_the_first_record_pins_the_wire_values(self) -> None:
        reading = AssetLocation.model_validate(ASSET_LOCATION_RECORDS[0])
        assert reading.happened_at_time == datetime(2026, 1, 1, 12, 0, 3, tzinfo=UTC)
        assert reading.asset.id == '281474981110001'
        assert reading.location.accuracy_meters == 4.0
        assert reading.location.heading_degrees == 270
        assert isinstance(reading.location.heading_degrees, int)
        assert reading.location.latitude == pytest.approx(33.1001)
        assert reading.location.longitude == pytest.approx(-96.1001)

    def test_the_empty_geofence_is_ignored(self) -> None:
        # extra='ignore' drops the observed-empty block: nothing to
        # mirror, so no field exists to carry it.
        reading = AssetLocation.model_validate(ASSET_LOCATION_RECORDS[0])
        assert 'geofence' not in AssetLocationFix.model_fields
        assert reading.location.model_extra in (None, {})
