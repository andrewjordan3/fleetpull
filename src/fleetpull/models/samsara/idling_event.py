# src/fleetpull/models/samsara/idling_event.py
"""Samsara IdlingEvent response model (``GET /idling/events``).

Written from captured live responses (2026-07-20 probe session: a
2,200-event census walked over 11 cursor pages), never from docs. The
census observed ZERO real nulls -- optionality is absence-shaped
(Samsara omits keys, the vehicles/drivers posture): every key below
except the three partial blocks was present on every record, so those
fields are required; ``operator`` (1546/2200 -- driver attribution when
known), ``airTemperatureMillicelsius`` (1833/2200), and ``address``
(552/2200) are optional. Within a carrying ``address`` block,
``addressTypes`` is itself absent on ~31 of the 552 blocks, so it is
optional inside the block too.

There is NO end key: the interval is start plus duration. Events were
only ever observed complete, with implied ends in the past even in a
last-30-minutes probe -- in-progress idles appear to materialize on
completion; the watermark lookback absorbs late materialization
(accepted residual, DESIGN §8). ``durationMilliseconds`` stays a
verbatim unit-suffixed int mirror -- no timedelta recovery: the value
is directly consumable, and recovery would presume a use.

``startTime`` is an RFC3339 string recovered as a tz-aware UTC datetime
by Pydantic's standard parse (the vehicles/drivers pattern, NOT the
trips epoch-ms path). ``ptoState`` is a plain ``str``, NOT an enum:
only ``'inactive'`` was observed in 2,200 records, but the value set is
not closed by evidence (unlike ``driverActivationStatus``'s 400-proven
closure), so membership is not enforced. ``fuelConsumedMilliliters`` is
MIXED int|float on the wire -- modeled ``float``, lax coercion lifting
the int shape. The two money blocks mirror verbatim as strings
(``{amount, currency}``) -- never parsed to a numeric type. NOTE the id
type split, mirrored exactly: ``address.id`` is a STRING while
``asset.id`` and ``operator.id`` are BARE INTs.

Wire keys are camelCase; fields are snake_case via the ``to_camel``
alias generator.
"""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = [
    'AssetRef',
    'FuelCost',
    'IdlingAddress',
    'IdlingEvent',
    'OperatorRef',
]


class AssetRef(ResponseModel):
    """The ``asset`` block: the event's vehicle reference.

    Present on every captured record -- events are fleet-wide with
    per-record asset attribution, which is what makes the endpoint a
    single fetch with no fan-out.

    Attributes:
        id: Samsara's asset id -- a BARE int on the wire, unlike the
            numeric-string ids of the vehicles/drivers surfaces.
    """

    id: int


class OperatorRef(ResponseModel):
    """The ``operator`` block: driver attribution when known (1546/2200).

    Attributes:
        id: Samsara's operator id -- a BARE int on the wire, like
            ``asset.id``.
    """

    id: int


class IdlingAddress(ResponseModel):
    """The ``address`` block: a matched defined-address reference (552/2200).

    Attributes:
        id: Samsara's address id -- a STRING on the wire, unlike the
            bare-int ``asset.id``/``operator.id`` beside it (mirrored
            exactly; never coerced to match its siblings).
        address_types: The defined address's type tags (element
            ``'yard'`` observed); absent on ~31 of the 552 captured
            address blocks, so optional within the block.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str
    address_types: list[str] | None = None


class FuelCost(ResponseModel):
    """A money block: the ``{amount, currency}`` shape.

    The one shape both cost fields (``fuelCost``, ``gaseousFuelCost``)
    carry, both keys present in every captured block. Mirrored verbatim
    as strings -- parsing money would presume a use.

    Attributes:
        amount: The monetary amount, a decimal string.
        currency: The currency code string.
    """

    amount: str
    currency: str


class IdlingEvent(ResponseModel):
    """One Samsara idling event, start-anchored per window.

    A pure mirror of the captured fields (module docstring). Field
    semantics and units are Samsara's; no value is derived or
    interpreted here. The interval is ``start_time`` plus
    ``duration_milliseconds`` -- there is no end key on the wire.

    Attributes:
        event_uuid: The event's id, a UUID string.
        start_time: Idle start (RFC3339, recovered tz-aware UTC) -- the
            endpoint's event time; retrieval is START-anchored on UTC
            (DESIGN §8), so retrieval and routing coincide natively.
        duration_milliseconds: Idle duration in milliseconds, a bare
            int mirrored verbatim (no end key exists; module
            docstring).
        asset: The vehicle reference (bare-int id).
        latitude: Event latitude, decimal degrees.
        longitude: Event longitude, decimal degrees.
        pto_state: Power-take-off state -- a plain string; only
            ``'inactive'`` observed in 2,200 records, but the value set
            is not evidence-closed, so no enum (module docstring).
        fuel_consumed_milliliters: Fuel consumed while idling, in
            milliliters -- MIXED int|float on the wire, modeled float.
        fuel_cost: The fuel cost money block, strings verbatim.
        gaseous_fuel_consumed_grams: Gaseous fuel consumed, in grams, a
            bare int.
        gaseous_fuel_cost: The gaseous-fuel cost money block, strings
            verbatim.
        operator: Driver attribution when known (1546/2200; bare-int
            id).
        air_temperature_millicelsius: Ambient air temperature in
            millidegrees Celsius, a bare int (1833/2200).
        address: The matched defined-address block (552/2200; STRING
            id).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    # Identity and the start+duration interval (no end key).
    event_uuid: str
    start_time: datetime
    duration_milliseconds: int

    # Attribution and position.
    asset: AssetRef
    latitude: float
    longitude: float

    # State and consumption (provider units mirrored verbatim).
    pto_state: str
    fuel_consumed_milliliters: float
    fuel_cost: FuelCost
    gaseous_fuel_consumed_grams: int
    gaseous_fuel_cost: FuelCost

    # The partial blocks (absence-shaped).
    operator: OperatorRef | None = None
    air_temperature_millicelsius: int | None = None
    address: IdlingAddress | None = None
