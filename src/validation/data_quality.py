from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class ValidationResult:
    check_name: str
    passed: bool
    failed_count: int
    details: dict[str, Any]


def _records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [record for record in records if isinstance(record, dict)]


def check_not_null(records: Iterable[dict[str, Any]], fields: list[str], check_name: str = "not_null") -> ValidationResult:
    rows = _records(records)
    failures = [
        {"row_index": index, "missing_fields": [field for field in fields if row.get(field) in (None, "")]}
        for index, row in enumerate(rows)
    ]
    failures = [failure for failure in failures if failure["missing_fields"]]

    return ValidationResult(
        check_name=check_name,
        passed=len(failures) == 0,
        failed_count=len(failures),
        details={"fields": fields, "failures": failures[:50]},
    )


def check_unique(records: Iterable[dict[str, Any]], field: str, check_name: str = "unique") -> ValidationResult:
    rows = _records(records)
    values = [row.get(field) for row in rows if row.get(field) not in (None, "")]
    counts = Counter(values)
    duplicates = sorted([value for value, count in counts.items() if count > 1])

    return ValidationResult(
        check_name=check_name,
        passed=len(duplicates) == 0,
        failed_count=len(duplicates),
        details={"field": field, "duplicate_values": duplicates[:50]},
    )


def check_duplicate_records(
    records: Iterable[dict[str, Any]],
    key_fields: list[str],
    check_name: str = "duplicate_records",
) -> ValidationResult:
    rows = _records(records)
    keys = [
        tuple(row.get(field) for field in key_fields)
        for row in rows
        if all(row.get(field) not in (None, "") for field in key_fields)
    ]
    counts = Counter(keys)
    duplicates = [dict(zip(key_fields, key)) for key, count in counts.items() if count > 1]

    return ValidationResult(
        check_name=check_name,
        passed=len(duplicates) == 0,
        failed_count=len(duplicates),
        details={"key_fields": key_fields, "duplicate_keys": duplicates[:50]},
    )


def check_threshold(
    *,
    numerator: int,
    denominator: int,
    max_rate: float,
    check_name: str,
) -> ValidationResult:
    rate = numerator / denominator if denominator else 0.0
    passed = rate <= max_rate

    return ValidationResult(
        check_name=check_name,
        passed=passed,
        failed_count=0 if passed else 1,
        details={
            "numerator": numerator,
            "denominator": denominator,
            "rate": rate,
            "max_rate": max_rate,
        },
    )


def assert_validation_results(results: list[ValidationResult]) -> None:
    failed = [result for result in results if not result.passed]
    if failed:
        summary = "; ".join(
            f"{result.check_name} failed_count={result.failed_count}" for result in failed
        )
        raise AssertionError(summary)
