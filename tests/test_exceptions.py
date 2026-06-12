"""Tests for fleetpull.exceptions."""

import pytest

from fleetpull.exceptions import (
    AuthenticationError,
    ConfigurationError,
    FleetpullError,
    ProviderResponseError,
    RetriesExhaustedError,
    UnknownQuotaScopeError,
)
from fleetpull.network.contract.outcome import ResponseCategory

# One instance per leaf class, for hierarchy-wide parametrization.
LEAF_INSTANCES: list[FleetpullError] = [
    ConfigurationError('bad sync config'),
    UnknownQuotaScopeError('geotab'),
    AuthenticationError(detail='invalid token'),
    ProviderResponseError(status_code=404),
    RetriesExhaustedError(attempt_count=3),
]

LEAF_IDS: list[str] = [type(instance).__name__ for instance in LEAF_INSTANCES]


class TestMessageComposition:
    def test_full_context(self) -> None:
        error = ConfigurationError(
            'bad sync config',
            provider='motive',
            endpoint='vehicles',
            detail='lookback_days must be positive',
        )
        assert str(error) == (
            'bad sync config [provider=motive, endpoint=vehicles]: '
            'lookback_days must be positive'
        )

    def test_no_context(self) -> None:
        assert str(ConfigurationError('bad sync config')) == 'bad sync config'

    def test_provider_only(self) -> None:
        error = ConfigurationError('bad sync config', provider='motive')
        assert str(error) == 'bad sync config [provider=motive]'

    def test_endpoint_only(self) -> None:
        error = ConfigurationError('bad sync config', endpoint='vehicles')
        assert str(error) == 'bad sync config [endpoint=vehicles]'

    def test_detail_only(self) -> None:
        error = ConfigurationError('bad sync config', detail='missing key')
        assert str(error) == 'bad sync config: missing key'


class TestFieldAccess:
    def test_shared_fields(self) -> None:
        error = ConfigurationError(
            'bad sync config',
            provider='motive',
            endpoint='vehicles',
            detail='missing key',
        )
        assert error.provider == 'motive'
        assert error.endpoint == 'vehicles'
        assert error.detail == 'missing key'

    def test_shared_fields_default_to_none(self) -> None:
        error = ConfigurationError('bad sync config')
        assert error.provider is None
        assert error.endpoint is None
        assert error.detail is None

    def test_unknown_quota_scope_carries_the_scope(self) -> None:
        error = UnknownQuotaScopeError('geotab')
        assert error.scope == 'geotab'
        assert str(error) == "unknown quota scope: 'geotab'"

    def test_unknown_quota_scope_folds_detail_into_the_message(self) -> None:
        error = UnknownQuotaScopeError('geotab', detail='configured scopes: motive')
        assert str(error) == "unknown quota scope: 'geotab': configured scopes: motive"

    def test_authentication_error_fixed_head(self) -> None:
        error = AuthenticationError(provider='samsara', detail='invalid token')
        assert str(error) == 'authentication failed [provider=samsara]: invalid token'

    def test_provider_response_error_carries_status_code(self) -> None:
        error = ProviderResponseError(status_code=404)
        assert error.status_code == 404
        assert str(error) == 'non-retryable provider response (HTTP 404)'

    def test_provider_response_error_without_status_code(self) -> None:
        error = ProviderResponseError(detail='malformed envelope')
        assert error.status_code is None
        assert str(error) == 'non-retryable provider response: malformed envelope'

    def test_retries_exhausted_carries_all_fields(self) -> None:
        error = RetriesExhaustedError(
            category=ResponseCategory.RATE_LIMITED,
            attempt_count=5,
        )
        assert error.category is ResponseCategory.RATE_LIMITED
        assert error.attempt_count == 5


class TestRetriesExhaustedHead:
    def test_attempt_count_and_category(self) -> None:
        error = RetriesExhaustedError(
            category=ResponseCategory.TRANSIENT, attempt_count=5
        )
        assert (
            str(error)
            == 'retry budget exhausted after 5 attempts (transient responses)'
        )

    def test_attempt_count_only(self) -> None:
        error = RetriesExhaustedError(attempt_count=5)
        assert str(error) == 'retry budget exhausted after 5 attempts'

    def test_category_only(self) -> None:
        error = RetriesExhaustedError(category=ResponseCategory.RATE_LIMITED)
        assert str(error) == 'retry budget exhausted (rate_limited responses)'

    def test_neither(self) -> None:
        assert str(RetriesExhaustedError()) == 'retry budget exhausted'


class TestHierarchy:
    @pytest.mark.parametrize('error', LEAF_INSTANCES, ids=LEAF_IDS)
    def test_every_member_is_a_fleetpull_error(self, error: FleetpullError) -> None:
        assert isinstance(error, FleetpullError)

    def test_unknown_quota_scope_is_a_configuration_error(self) -> None:
        assert isinstance(UnknownQuotaScopeError('geotab'), ConfigurationError)

    @pytest.mark.parametrize('error', LEAF_INSTANCES, ids=LEAF_IDS)
    def test_raising_any_leaf_is_caught_by_the_root(
        self, error: FleetpullError
    ) -> None:
        with pytest.raises(FleetpullError):
            raise error
