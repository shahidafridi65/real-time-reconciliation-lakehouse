from __future__ import annotations

from typing import Any


RAW_SCHEMAS = {
    "clickstream_events": {
        "required": ["event_id", "event_type", "user_id"],
        "types": {
            "event_id": str,
            "event_type": str,
            "user_id": (int, str),
            "event_time": str,
            "product_id": (int, str),
            "price": (int, float),
            "quantity": int,
        },
    },
    "server_logs": {
        "required": ["message"],
        "types": {
            "timestamp": str,
            "level": str,
            "message": str,
        },
    },
    "order_changes": {
        "required": ["order_id", "order_status", "order_placed_at"],
        "types": {
            "order_id": (int, str),
            "order_status": str,
            "order_placed_at": str,
            "user_id": (int, str),
            "product_id": (int, str),
            "quantity": int,
            "total_amount": (int, float),
            "payment_status": str,
        },
    },
    "shipment_data": {
        "required": ["order_id", "status", "status_last_updated_at"],
        "types": {
            "order_id": (int, str),
            "status": str,
            "status_last_updated_at": str,
            "carrier": str,
            "tracking_number": str,
        },
    },
}


POSTGRES_BRONZE_SCHEMAS = {
    "users": ["user_id", "full_name", "email", "country", "created_at"],
    "products": ["product_id", "product_name", "category", "price", "updated_at"],
    "orders": [
        "order_id",
        "user_id",
        "product_id",
        "quantity",
        "total_amount",
        "order_status",
        "payment_status",
        "order_placed_at",
    ],
}


def validate_records(source_name: str, records: list[dict[str, Any] | str]) -> list[dict[str, Any]]:
    if source_name not in RAW_SCHEMAS:
        raise ValueError(f"Unknown Bronze source schema: {source_name}")

    schema = RAW_SCHEMAS[source_name]
    validated: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        candidate: dict[str, Any]
        if isinstance(record, str) and source_name == "server_logs":
            candidate = {"message": record}
        elif isinstance(record, dict):
            candidate = record
        else:
            raise ValueError(f"Invalid record at index {index} for {source_name}: expected an object")

        if source_name == "clickstream_events" and "ts" not in candidate and "event_time" in candidate:
            candidate = {**candidate, "ts": candidate["event_time"]}

        missing_fields = [
            field for field in schema["required"] if candidate.get(field) in (None, "")
        ]
        if missing_fields:
            raise ValueError(
                f"Invalid record at index {index} for {source_name}: missing required fields {missing_fields}"
            )

        for field_name, expected_type in schema.get("types", {}).items():
            if field_name in candidate and candidate[field_name] is not None:
                if not isinstance(candidate[field_name], expected_type):
                    raise ValueError(
                        f"Invalid record at index {index} for {source_name}: field '{field_name}' has invalid type"
                    )

        validated.append(candidate)

    return validated


def validate_tabular_records(source_name: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    required = POSTGRES_BRONZE_SCHEMAS.get(source_name)
    if not required:
        return records

    for index, record in enumerate(records):
        missing_fields = [field for field in required if record.get(field) in (None, "")]
        if missing_fields:
            raise ValueError(
                f"Invalid record at index {index} for {source_name}: missing required fields {missing_fields}"
            )

    return records
