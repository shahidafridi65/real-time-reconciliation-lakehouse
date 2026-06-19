import argparse
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reconciliation")


def _normalize_records(records: Any) -> list[dict[str, Any]]:
    if records is None:
        return []

    if isinstance(records, dict):
        records = [records]

    return [record for record in records if isinstance(record, dict)]


def reconcile_records(orders: Any, shipments: Any) -> dict[str, Any]:
    """Reconcile order and shipment records to highlight missing and duplicate orders."""
    order_records = _normalize_records(orders)
    shipment_records = _normalize_records(shipments)

    order_ids = [record.get("order_id") for record in order_records if record.get("order_id")]
    shipment_ids = {record.get("order_id") for record in shipment_records if record.get("order_id")}

    order_counter = Counter(order_ids)
    duplicate_orders = sorted([order_id for order_id, count in order_counter.items() if count > 1])
    matched_orders = [order_id for order_id in order_ids if order_id in shipment_ids]
    missing_shipments = [order_id for order_id in sorted(set(order_ids)) if order_id not in shipment_ids]

    total_records = len(order_records)
    reconciliation_rate = len(matched_orders) / total_records if total_records else 0.0

    return {
        "total_orders": total_records,
        "unique_orders": len(set(order_ids)),
        "matched_orders": len(matched_orders),
        "missing_shipments": missing_shipments,
        "duplicate_orders": duplicate_orders,
        "reconciliation_rate": reconciliation_rate,
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    return None


def build_order_reconciliation_rows(
    orders: Any,
    clickstream_events: Any,
    server_logs: Any | None = None,
    *,
    match_window_minutes: int = 60,
    timeout_window_minutes: int = 5,
) -> list[dict[str, Any]]:
    """Build order-level reconciliation rows for local validation and tests."""
    order_records = _normalize_records(orders)
    event_records = [
        record
        for record in _normalize_records(clickstream_events)
        if str(record.get("event_type", "")).lower() == "purchase"
    ]
    log_records = _normalize_records(server_logs)

    used_event_ids: set[Any] = set()
    rows: list[dict[str, Any]] = []

    for order in order_records:
        order_id = order.get("order_id")
        user_id = order.get("user_id")
        product_id = order.get("product_id")
        placed_at = _parse_timestamp(order.get("placed_at") or order.get("order_placed_at"))
        order_status = str(order.get("status") or order.get("order_status") or "").lower()
        payment_status = str(order.get("payment_status") or "").lower()

        candidates = []
        for event in event_records:
            if str(event.get("user_id")) != str(user_id):
                continue
            if product_id is not None and str(event.get("product_id")) != str(product_id):
                continue
            event_timestamp = _parse_timestamp(event.get("event_timestamp") or event.get("event_time"))
            if placed_at and event_timestamp:
                delay_minutes = abs((event_timestamp - placed_at).total_seconds()) / 60
                candidates.append((delay_minutes, event))

        candidates.sort(key=lambda item: item[0])
        best_delay = candidates[0][0] if candidates else None
        best_event = candidates[0][1] if candidates else None
        if best_event:
            used_event_ids.add(best_event.get("event_id"))

        gateway_timeout_count = 0
        if placed_at:
            for log in log_records:
                log_timestamp = _parse_timestamp(log.get("log_timestamp") or log.get("timestamp"))
                if not log_timestamp:
                    continue
                delta_minutes = abs((log_timestamp - placed_at).total_seconds()) / 60
                status_code = int(log.get("status_code", 0) or 0)
                if delta_minutes <= timeout_window_minutes and status_code == 504:
                    gateway_timeout_count += 1

        has_missing_clickstream = best_event is None
        has_delayed_clickstream = best_delay is not None and best_delay > match_window_minutes
        has_payment_mismatch = (
            payment_status in {"failed", "pending"} and order_status in {"shipped", "delivered"}
        ) or (
            payment_status == "paid" and order_status == "cancelled"
        )
        has_gateway_timeout = gateway_timeout_count > 0

        status = "matched"
        if has_missing_clickstream:
            status = "missing_purchase_event"
        elif has_delayed_clickstream:
            status = "delayed_purchase_event"
        elif payment_status in {"failed", "pending"} and order_status in {"shipped", "delivered"}:
            status = "payment_not_settled"
        elif payment_status == "paid" and order_status == "cancelled":
            status = "paid_cancelled_order"
        elif has_gateway_timeout:
            status = "gateway_timeout_near_order"

        rows.append({
            "reconciliation_record_type": "order",
            "order_id": order_id,
            "user_id": user_id,
            "product_id": product_id,
            "clickstream_event_id": best_event.get("event_id") if best_event else None,
            "event_delay_minutes": int(best_delay) if best_delay is not None else None,
            "gateway_timeout_count": gateway_timeout_count,
            "has_missing_clickstream": has_missing_clickstream,
            "has_delayed_clickstream": has_delayed_clickstream,
            "has_payment_mismatch": has_payment_mismatch,
            "has_gateway_timeout": has_gateway_timeout,
            "reconciliation_status": status,
        })

    for event in event_records:
        if event.get("event_id") in used_event_ids:
            continue
        rows.append({
            "reconciliation_record_type": "purchase_without_order",
            "order_id": None,
            "user_id": event.get("user_id"),
            "product_id": event.get("product_id"),
            "clickstream_event_id": event.get("event_id"),
            "event_delay_minutes": None,
            "gateway_timeout_count": 0,
            "has_missing_clickstream": False,
            "has_delayed_clickstream": False,
            "has_payment_mismatch": False,
            "has_gateway_timeout": False,
            "reconciliation_status": "purchase_without_order",
        })

    return rows


def summarize_reconciliation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_records = len(rows)
    status_counts = Counter(row.get("reconciliation_status") for row in rows)
    exception_records = sum(
        count for status, count in status_counts.items() if status and status != "matched"
    )

    return {
        "total_records": total_records,
        "matched_records": status_counts.get("matched", 0),
        "exception_records": exception_records,
        "exception_rate": exception_records / total_records if total_records else 0.0,
        "status_counts": dict(sorted(status_counts.items())),
    }


def write_reconciliation_report(report: dict[str, Any], output_dir: str | Path = "gold/reconciliation") -> str:
    """Write the reconciliation summary to a JSON file for downstream analysis."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "reconciliation_report.json"
    file_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Wrote reconciliation report to %s", file_path)
    return str(file_path)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate a basic order-versus-shipment reconciliation summary")
    parser.add_argument("--orders", required=True, help="Orders JSON file")
    parser.add_argument("--shipments", required=True, help="Shipments JSON file")
    parser.add_argument("--output-dir", default="gold/reconciliation", help="Output folder for the report")
    return parser.parse_args()


def main():
    args = parse_args()

    orders = json.loads(Path(args.orders).read_text(encoding="utf-8"))
    shipments = json.loads(Path(args.shipments).read_text(encoding="utf-8"))

    report = reconcile_records(orders, shipments)
    write_reconciliation_report(report, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
