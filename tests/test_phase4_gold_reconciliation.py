from src.transformations.gold.reconciliation import (
    build_order_reconciliation_rows,
    summarize_reconciliation,
)


def test_order_reconciliation_detects_business_exceptions():
    orders = [
        {
            "order_id": "o-1",
            "user_id": "u-1",
            "product_id": "p-1",
            "status": "delivered",
            "payment_status": "pending",
            "order_placed_at": "2026-06-14T10:00:00Z",
        },
        {
            "order_id": "o-2",
            "user_id": "u-2",
            "product_id": "p-2",
            "status": "placed",
            "payment_status": "paid",
            "order_placed_at": "2026-06-14T10:00:00Z",
        },
    ]
    clickstream = [
        {
            "event_id": "evt-1",
            "event_type": "purchase",
            "user_id": "u-1",
            "product_id": "p-1",
            "event_time": "2026-06-14T10:05:00Z",
        },
        {
            "event_id": "evt-2",
            "event_type": "purchase",
            "user_id": "u-3",
            "product_id": "p-3",
            "event_time": "2026-06-14T11:00:00Z",
        },
    ]
    server_logs = [
        {
            "timestamp": "2026-06-14T10:02:00Z",
            "status_code": 504,
        }
    ]

    rows = build_order_reconciliation_rows(orders, clickstream, server_logs)

    statuses = {row["order_id"] or row["clickstream_event_id"]: row["reconciliation_status"] for row in rows}

    assert statuses["o-1"] == "payment_not_settled"
    assert statuses["o-2"] == "missing_purchase_event"
    assert statuses["evt-2"] == "purchase_without_order"
    assert any(row["has_gateway_timeout"] for row in rows if row["order_id"] == "o-1")


def test_reconciliation_summary_counts_exception_rate():
    rows = [
        {"reconciliation_status": "matched"},
        {"reconciliation_status": "missing_purchase_event"},
        {"reconciliation_status": "purchase_without_order"},
    ]

    summary = summarize_reconciliation(rows)

    assert summary["total_records"] == 3
    assert summary["matched_records"] == 1
    assert summary["exception_records"] == 2
    assert summary["exception_rate"] == 2 / 3
