import argparse
import json
import logging
from collections import Counter
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
