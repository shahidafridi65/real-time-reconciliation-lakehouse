import random
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from config.settings import MOCK_SHIPPING_API_HOST, MOCK_SHIPPING_API_PORT

app = FastAPI(title="Real-Time Reconciliation Shipping API")

CARRIERS = ["DHL", "FedEx", "BlueDart", "Delhivery"]
STATUSES = ["PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED"]


@app.get("/health")
def health():
    return {"status": "ok", "service": "shipping-api-mock"}


@app.get("/shipments")
def get_shipments(limit: int = 20):
    shipments = []

    for _ in range(limit):
        last_updated = datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 48))

        shipments.append({
            "order_id": random.randint(50000, 51000),
            "carrier": random.choice(CARRIERS),
            "tracking_number": f"TRK{random.randint(100000, 999999)}",
            "status": random.choice(STATUSES),
            "status_last_updated_at": last_updated.isoformat(),
            "events": [
                {"event": "PICKED_UP", "time": (last_updated - timedelta(days=2)).isoformat()},
                {"event": "IN_TRANSIT", "time": (last_updated - timedelta(days=1)).isoformat()},
                {"event": random.choice(STATUSES), "time": last_updated.isoformat()}
            ]
        })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "shipments": shipments
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=MOCK_SHIPPING_API_HOST, port=MOCK_SHIPPING_API_PORT)