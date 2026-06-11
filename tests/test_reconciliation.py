from src.transformations.gold.reconciliation import reconcile_records


def test_reconcile_records_detects_missing_and_duplicate_orders():
    orders = [
        {"order_id": "o-1", "status": "paid"},
        {"order_id": "o-2", "status": "paid"},
        {"order_id": "o-2", "status": "paid"},
    ]
    shipments = [
        {"order_id": "o-1", "status": "shipped"},
    ]

    result = reconcile_records(orders, shipments)

    assert result['matched_orders'] == 1
    assert result['missing_shipments'] == ['o-2']
    assert result['duplicate_orders'] == ['o-2']
    assert result['reconciliation_rate'] == 1 / 3
