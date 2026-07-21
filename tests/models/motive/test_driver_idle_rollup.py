"""Tests for fleetpull.models.motive.driver_idle_rollup.

Every fixture is the committed 2026-07-21 capture set
(``tests/motive_driver_idle_rollups_capture.py``): three fully
synthetic window-stamped rollup records shaped by the 100-record
structurally uniform census. The census-preserved shapes (the BARE-INT
``idle_time``/``driving_time`` durations -- ints on this arm, floats on
the vehicle arm -- the shared 8-key ``UserSummary`` driver ref, and THE
NULL-DRIVER unattributed bucket row) are asserted here beside the model
that mirrors them; requiredness carries drop-key rejection teeth -- the
window stamps STRUCTURALLY and the metric core on the rollup-surface
posture (the model module docstring states the judgment) -- only a loud
rejection here keeps a future optional-demotion from passing every
gate. The window stamps are DATE LABELS lifted to UTC midnight by
``MotiveWindowStamp`` -- company-local day labels, never converted (the
documented caveat).
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fleetpull.models.motive import DriverIdleRollup, UserSummary
from tests.motive_driver_idle_rollups_capture import (
    DRIVER_IDLE_ROLLUP_RECORDS,
)

# The stamped record's full key set: the structurally uniform wire
# census (six keys) plus the two decoder-synthesized window stamps.
_STAMPED_KEYS = frozenset(
    {
        'windowStartDate',
        'windowEndDate',
        'driver',
        'utilization',
        'idle_time',
        'driving_time',
        'idle_fuel',
        'driving_fuel',
    }
)

# Required with teeth: the stamps structurally, the metric core per the
# rollup-surface posture. `driver` stays out of the drop-key sweep --
# it is nullable (the unattributed bucket), so absence must not crash.
_REQUIRED_KEYS = _STAMPED_KEYS - {'driver'}

# The populated driver ref's wire key set -- exactly the shared
# UserSummary 8-key shape, its fourth carrying surface.
_DRIVER_REF_KEYS = frozenset(
    {
        'id',
        'first_name',
        'last_name',
        'username',
        'email',
        'driver_company_id',
        'status',
        'role',
    }
)


class TestFixtureProperties:
    """The variant coverage the capture module promises."""

    def test_every_record_carries_the_full_stamped_key_set(self) -> None:
        assert len(DRIVER_IDLE_ROLLUP_RECORDS) == 3
        for record in DRIVER_IDLE_ROLLUP_RECORDS:
            assert set(record) == _STAMPED_KEYS

    def test_the_wire_shape_carries_no_row_time_identity(self) -> None:
        # The probe's central fact: rollup rows carry NO date or time
        # identity of any kind -- the `*_time` keys are duration
        # metrics; the row's only time-identity keys are the decoder's
        # stamps.
        for record in DRIVER_IDLE_ROLLUP_RECORDS:
            wire_keys = set(record) - {'windowStartDate', 'windowEndDate'}
            assert not any('date' in key.lower() for key in wire_keys)
            assert not any(key.endswith(('_at', 'Time')) for key in wire_keys)

    def test_populated_refs_carry_exactly_the_user_summary_shape(self) -> None:
        populated = [
            record['driver']
            for record in DRIVER_IDLE_ROLLUP_RECORDS
            if record['driver'] is not None
        ]
        assert len(populated) == 2
        for driver in populated:
            assert isinstance(driver, dict)
            assert set(driver) == _DRIVER_REF_KEYS

    def test_the_null_driver_bucket_row_appears(self) -> None:
        assert DRIVER_IDLE_ROLLUP_RECORDS[2]['driver'] is None

    def test_the_durations_are_bare_ints_on_the_wire(self) -> None:
        # Ints on this arm (floats on the vehicle arm) -- the fixture
        # preserves the wire's type split so the model mirrors it by
        # evidence, not by accident.
        for record in DRIVER_IDLE_ROLLUP_RECORDS:
            assert type(record['idle_time']) is int
            assert type(record['driving_time']) is int


class TestDriverIdleRollupValidation:
    @pytest.mark.parametrize('required_key', sorted(_REQUIRED_KEYS))
    def test_each_required_key_rejects_absence(self, required_key: str) -> None:
        # Requiredness with teeth: the window stamps structurally, the
        # metric core per the rollup-surface posture -- a record missing
        # any must fail loudly, never land nulls.
        record = {
            key: value
            for key, value in DRIVER_IDLE_ROLLUP_RECORDS[0].items()
            if key != required_key
        }
        with pytest.raises(ValidationError):
            DriverIdleRollup.model_validate(record)

    def test_every_record_validates_with_aware_window_stamps(self) -> None:
        validated = [
            DriverIdleRollup.model_validate(record)
            for record in DRIVER_IDLE_ROLLUP_RECORDS
        ]
        assert len(validated) == 3
        for rollup in validated:
            assert rollup.window_start.tzinfo is not None
            assert rollup.window_end.tzinfo is not None

    def test_the_date_labels_lift_to_their_utc_midnight_instants(self) -> None:
        # The stamps are the sent window's INCLUSIVE date labels
        # ('2026-01-05' both, at the fixed 1-day unit), lifted to UTC
        # midnight -- the label's day preserved exactly, never a
        # timezone conversion (the company-local caveat rides the
        # docstrings).
        rollup = DriverIdleRollup.model_validate(DRIVER_IDLE_ROLLUP_RECORDS[0])
        assert rollup.window_start == datetime(2026, 1, 5, tzinfo=UTC)
        assert rollup.window_end == datetime(2026, 1, 5, tzinfo=UTC)
        assert rollup.window_start == rollup.window_end

    def test_the_first_record_pins_the_wire_values(self) -> None:
        rollup = DriverIdleRollup.model_validate(DRIVER_IDLE_ROLLUP_RECORDS[0])
        driver = rollup.driver
        assert isinstance(driver, UserSummary)
        assert driver.user_id == 700101
        assert driver.first_name == 'Synthetic'
        assert driver.last_name == 'Driver101'
        assert driver.username == 'sdriver101'
        assert driver.email == 'synthetic.driver101@example.com'
        assert driver.driver_company_id == 'SYN-101'
        assert driver.status == 'active'
        assert driver.role == 'driver'
        assert rollup.utilization == 71.4
        assert rollup.idle_time == 1740
        assert rollup.driving_time == 20460
        assert rollup.idle_fuel == 2.8
        assert rollup.driving_fuel == 38.1

    def test_the_durations_stay_ints_on_the_model(self) -> None:
        # Bare ints mirrored as ints -- the vehicle arm's float
        # durations never leak onto this arm.
        rollup = DriverIdleRollup.model_validate(DRIVER_IDLE_ROLLUP_RECORDS[0])
        assert isinstance(rollup.idle_time, int)
        assert isinstance(rollup.driving_time, int)

    def test_a_refs_null_arms_land_none(self) -> None:
        # The union-lax UserSummary posture: this surface's ref can
        # null email/driver_company_id (fixture record 1).
        rollup = DriverIdleRollup.model_validate(DRIVER_IDLE_ROLLUP_RECORDS[1])
        driver = rollup.driver
        assert isinstance(driver, UserSummary)
        assert driver.email is None
        assert driver.driver_company_id is None
        assert driver.status == 'deactivated'

    def test_the_null_driver_bucket_validates_with_a_none_ref(self) -> None:
        # The unattributed rollup bucket: driver null on the wire lands
        # None, the metrics intact -- never dropped, never crashed.
        rollup = DriverIdleRollup.model_validate(DRIVER_IDLE_ROLLUP_RECORDS[2])
        assert rollup.driver is None
        assert rollup.idle_time == 5400
        assert rollup.utilization == 3.5

    def test_an_absent_driver_key_also_lands_none(self) -> None:
        # Nullable-with-default covers both null and absence -- either
        # wire shape of an unattributed bucket mirrors as None.
        record = {
            key: value
            for key, value in DRIVER_IDLE_ROLLUP_RECORDS[2].items()
            if key != 'driver'
        }
        rollup = DriverIdleRollup.model_validate(record)
        assert rollup.driver is None

    def test_a_non_label_window_stamp_rejects(self) -> None:
        # The stamp lift is strict: the builder only renders date
        # labels, so an RFC3339 datetime string arriving as a stamp is
        # a wiring drift that must fail loudly, not pass mangled.
        record = dict(DRIVER_IDLE_ROLLUP_RECORDS[0])
        record['windowEndDate'] = '2026-01-05T00:00:00Z'
        with pytest.raises(ValidationError):
            DriverIdleRollup.model_validate(record)
