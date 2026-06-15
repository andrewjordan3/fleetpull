"""Tests for fleetpull.vocabulary.response_category."""

import pytest

from fleetpull.vocabulary import ResponseCategory


class TestResponseCategory:
    def test_membership_is_the_closed_vocabulary(self) -> None:
        assert [category.name for category in ResponseCategory] == [
            'SUCCESS',
            'TRANSIENT',
            'RATE_LIMITED',
            'AUTH_FAILURE',
            'FATAL',
        ]

    @pytest.mark.parametrize(
        ('member', 'expected_value'),
        [
            (ResponseCategory.SUCCESS, 'success'),
            (ResponseCategory.TRANSIENT, 'transient'),
            (ResponseCategory.RATE_LIMITED, 'rate_limited'),
            (ResponseCategory.AUTH_FAILURE, 'auth_failure'),
            (ResponseCategory.FATAL, 'fatal'),
        ],
    )
    def test_values(self, member: ResponseCategory, expected_value: str) -> None:
        assert member.value == expected_value
