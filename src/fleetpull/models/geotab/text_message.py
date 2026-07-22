# src/fleetpull/models/geotab/text_message.py
"""GeoTab TextMessage response model (``GetFeed`` on ``typeName: TextMessage``).

Written from the 2026-07-21 feed wave three SCALE census (walked at
scale at the probed tenant), never from docs. A TextMessage is one
dispatch message between the office and a vehicle.

NO per-record ``version`` key AND NO ``dateTime`` key — the append-only
asymmetry (FaultData/LogRecord): append-only storage is trivially
complete and the consumer reconciles by ``id`` alone (DESIGN §4). The
event-time identity is ``sent`` (the send instant, 25,000/25,000), so
the binding anchors ``event_time_column='sent'`` and ``sent`` is a
REQUIRED datetime — storage partitions on it. The feed's own
``toVersion`` still advances (delivered/read receipts re-emit a message
under a newer FEED version); those re-emissions are stored-as-emitted,
the feed's versioning rather than a per-record ``version`` key.

Requiredness posture (the wave-two conservative stance, DESIGN §8): the
census is a TENANT-SCOPED observation (25,000 records), so structural
requiredness is limited to the record identity — ``id`` and ``sent``
(the event time) — and every other field is optional EVEN where the
census was total. The observed arms:

- ``device`` was object-only (``{id}``) at scale; it rides the shared
  ``bare_id_to_reference`` lift (the census-scope lesson). It is
  OPTIONAL — a text message has no required primary entity ref.
- ``messageContent`` is a NESTED block ``{contentType, ids}``; both
  keys are required WITHIN the block on the nested-block-required
  convention (200/200 nested, 25,000/25,000 present). ``ids`` is a
  PLAIN ``list[str]`` — the elements ARE strings on the wire, so this
  is a §9 list-of-scalar direct field, NOT the DutyStatusLog
  ``annotations`` id-object reduction (there the elements were
  ``{id}`` objects needing a strict lift).
- ``delivered`` and ``read`` are receipt datetimes present on
  24,995/25,000 — optional.

``activeFrom``/``activeTo``, ``delivered``, ``read``, and ``sent`` are
recovered tz-aware by validation, the GeoTab sibling idiom.
``contentType`` and ``recipient`` are census-open strings;
``isDirectionToVehicle`` is a bool, ``messageSize`` an int.
"""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel
from fleetpull.models.geotab.shared import bare_id_to_reference

__all__: list[str] = [
    'TextMessage',
    'TextMessageContent',
    'TextMessageDeviceRef',
]


class TextMessageDeviceRef(ResponseModel):
    """The message's device reference (the vehicle end of the exchange).

    Census-observed as an ``{id}`` object at scale; the shared coercion
    lifts a bare string defensively (the census-scope lesson).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    id: str


class TextMessageContent(ResponseModel):
    """The ``messageContent`` block: the message payload descriptor.

    Both keys are required WITHIN the block (the nested-block-required
    convention): a present ``messageContent`` block missing either is a
    shape change and must fail loudly. ``ids`` is a direct
    ``list[str]`` (the elements are strings on the wire, a §9
    list-of-scalar), NOT the ``annotations`` id-object reduction.
    """

    model_config = ConfigDict(alias_generator=to_camel)

    content_type: str
    ids: list[str]


class TextMessage(ResponseModel):
    """One GeoTab dispatch message from the TextMessage feed.

    The wave-two conservative mirror with the append-only asymmetry (the
    module docstring's posture): ``id`` / ``sent`` required, NO
    per-record ``version``, everything else optional even where
    census-total.

    Attributes:
        active_from: The message window's UTC start.
        active_to: The message window's UTC end.
        delivered: The delivery-receipt UTC instant (24,995/25,000).
        device: The message's device reference (object-only at scale;
            defensively lifted). Optional — no required primary ref.
        id: GeoTab's record id.
        is_direction_to_vehicle: Whether the message is office→vehicle.
        message_content: The message payload descriptor block.
        message_size: The message payload size.
        read: The read-receipt UTC instant (24,995/25,000).
        recipient: The message recipient address (census-open str).
        sent: The send UTC instant — the endpoint's event time (the
            event-time identity, in place of the absent ``dateTime``).
    """

    model_config = ConfigDict(alias_generator=to_camel)

    active_from: datetime | None = None
    active_to: datetime | None = None
    delivered: datetime | None = None
    device: Annotated[
        TextMessageDeviceRef | None, BeforeValidator(bare_id_to_reference)
    ] = None
    id: str
    is_direction_to_vehicle: bool | None = None
    message_content: TextMessageContent | None = None
    message_size: int | None = None
    read: datetime | None = None
    recipient: str | None = None
    sent: datetime
