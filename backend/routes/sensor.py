"""
Sensor data routes — GET /data, /batches, /alerts, /uids and POST /api/sensor-data.
"""

from fastapi import APIRouter, Query, Depends

from database import (
    fetch_record_rows,
    fetch_latest_batch_rows,
    fetch_latest_uid_rows,
    fetch_latest_alert_rows,
    row_to_dict,
    row_to_legacy_list,
    insert_sensor_data,
)
from config import BATCH_KEY_EXPR, UID_KEY_EXPR, RISK_PRIORITY
from schemas import SensorReading
from auth import get_current_user, require_role

router = APIRouter(tags=["Sensor Data"])


@router.get("/data")
def get_data(product_id: str | None = Query(default=None)):
    where_clause = ""
    params = ()
    if product_id:
        where_clause = f"sd.product_id = ? OR {BATCH_KEY_EXPR} = ? OR {UID_KEY_EXPR} = ?"
        params = (product_id, product_id, product_id)

    rows = fetch_record_rows(where_clause=where_clause, params=params, order_by="sd.id DESC", limit=50)
    legacy_rows = [row_to_legacy_list(row) for row in rows]
    latest = row_to_dict(rows[0]) if rows else None
    return {"data": legacy_rows, "latest": latest}


@router.get("/batches")
def get_batches():
    rows = fetch_latest_batch_rows()
    batches = [row_to_dict(row) for row in rows]
    active_batches = [row for row in batches if row["is_active"]]
    if active_batches:
        batches = active_batches
    batches.sort(key=lambda row: (RISK_PRIORITY[row["risk_level"]], row["id"]), reverse=True)
    return {"count": len(batches), "batches": batches}


@router.get("/alerts")
def get_alerts():
    alerts = fetch_latest_alert_rows()
    active_alerts = [row for row in alerts if row["is_active"]]
    if active_alerts:
        alerts = active_alerts
    return {"count": len(alerts), "alerts": alerts}


@router.get("/uids")
def get_uids():
    rows = fetch_latest_uid_rows()
    records = [row_to_dict(row) for row in rows]
    active_records = [row for row in records if row["is_active"]]
    if active_records:
        records = active_records
    records.sort(key=lambda row: (RISK_PRIORITY[row["risk_level"]], row["id"]), reverse=True)
    return {"count": len(records), "uids": records}


@router.post("/api/sensor-data")
def submit_sensor_data(
    reading: SensorReading,
    user=Depends(require_role("admin", "farmer")),
):
    """Submit sensor reading — requires admin or farmer role."""
    insert_sensor_data(reading.model_dump())
    return {"status": "recorded", "batch_id": reading.batch_id, "submitted_by": user["username"]}
