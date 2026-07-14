"""GeoTab LogRecord response model for GetFeed position records."""

from datetime import datetime

from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

from fleetpull.model_contract import ResponseModel

__all__: list[str] = ['LogRecord', 'LogRecordDeviceRef']


class LogRecordDeviceRef(ResponseModel):
    """The device reference nested in a GeoTab LogRecord."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str | None = None


class LogRecord(ResponseModel):
    """A GeoTab LogRecord feed record."""

    model_config = ConfigDict(alias_generator=to_camel)

    id: str | None = None
    date_time: datetime | None = None
    latitude: float | None = None
    longitude: float | None = None
    speed: float | None = None
    device: LogRecordDeviceRef | None = None
