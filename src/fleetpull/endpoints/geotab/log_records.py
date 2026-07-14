"""The GeoTab LogRecord GetFeed binding."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from fleetpull.config import GeotabConfig
from fleetpull.endpoints.shared import (
    EndpointDefinition,
    FeedMode,
    ResumeValue,
    StorageKind,
)
from fleetpull.incremental import DateWindow, FeedBootstrap, FeedToken
from fleetpull.models.geotab import LogRecord
from fleetpull.network.contract import HttpMethod, RequestSpec
from fleetpull.network.decoders import GeotabFeedPageDecoder
from fleetpull.timing import to_iso8601
from fleetpull.vocabulary import JsonValue, Provider, QuotaScope

__all__: list[str] = ['build_endpoint']

_API_PATH: Final[str] = '/apiv1'
_DEFAULT_SERVER: Final[str] = 'my.geotab.com'
_RESULTS_LIMIT: Final[int] = 5000
_METHOD_KEY: Final[str] = 'method'
_PARAMS_KEY: Final[str] = 'params'
_TYPE_NAME_KEY: Final[str] = 'typeName'
_SEARCH_KEY: Final[str] = 'search'
_FROM_DATE_KEY: Final[str] = 'fromDate'
_FROM_VERSION_KEY: Final[str] = 'fromVersion'
_RESULTS_LIMIT_KEY: Final[str] = 'resultsLimit'
_GET_FEED_METHOD: Final[str] = 'GetFeed'
_LOG_RECORD_TYPE_NAME: Final[str] = 'LogRecord'


def _server_host(config: GeotabConfig) -> str:
    if config.auth is not None:
        return config.auth.server
    return _DEFAULT_SERVER


@dataclass(frozen=True, slots=True)
class _GeotabLogRecordFeedSpecBuilder:
    server: str
    type_name: str
    results_limit: int

    def build_spec(
        self, resume: ResumeValue, path_values: Mapping[str, str]
    ) -> RequestSpec:
        params: dict[str, JsonValue] = {
            _TYPE_NAME_KEY: self.type_name,
            _RESULTS_LIMIT_KEY: self.results_limit,
        }
        match resume:
            case FeedBootstrap():
                params[_SEARCH_KEY] = {_FROM_DATE_KEY: to_iso8601(resume.from_date)}
            case FeedToken():
                params[_FROM_VERSION_KEY] = resume.from_version
            case DateWindow() | None:
                raise TypeError(
                    '_GeotabLogRecordFeedSpecBuilder requires FeedBootstrap or FeedToken resume, '
                    f'got {type(resume).__name__}.'
                )
        return RequestSpec(
            method=HttpMethod.POST,
            url=f'https://{self.server}{_API_PATH}',
            json_body={_METHOD_KEY: _GET_FEED_METHOD, _PARAMS_KEY: params},
        )


def build_endpoint(config: GeotabConfig) -> EndpointDefinition[LogRecord]:
    """Build the GeoTab LogRecord feed binding."""
    return EndpointDefinition(
        provider=Provider.GEOTAB,
        name='log_records',
        spec_builder=_GeotabLogRecordFeedSpecBuilder(
            server=_server_host(config),
            type_name=_LOG_RECORD_TYPE_NAME,
            results_limit=_RESULTS_LIMIT,
        ),
        page_decoder=GeotabFeedPageDecoder(),
        response_model=LogRecord,
        quota_scope=QuotaScope.GEOTAB_GET_FEED,
        storage_kind=StorageKind.DATE_PARTITIONED,
        sync_mode=FeedMode(),
        event_time_column='date_time',
    )
