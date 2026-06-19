import pytest

from src.validation.data_quality import (
    assert_validation_results,
    check_duplicate_records,
    check_not_null,
    check_threshold,
    check_unique,
)


def test_validation_helpers_detect_common_quality_issues():
    records = [
        {"order_id": "o-1", "user_id": "u-1"},
        {"order_id": "o-1", "user_id": ""},
        {"order_id": "o-2", "user_id": "u-2"},
    ]

    null_result = check_not_null(records, ["order_id", "user_id"])
    unique_result = check_unique(records, "order_id")
    duplicate_result = check_duplicate_records(records, ["order_id"])

    assert not null_result.passed
    assert not unique_result.passed
    assert not duplicate_result.passed


def test_threshold_validation_and_assertion():
    passing = check_threshold(numerator=1, denominator=10, max_rate=0.2, check_name="exception_rate")
    failing = check_threshold(numerator=4, denominator=10, max_rate=0.2, check_name="exception_rate")

    assert passing.passed
    assert not failing.passed

    with pytest.raises(AssertionError, match="exception_rate"):
        assert_validation_results([passing, failing])
