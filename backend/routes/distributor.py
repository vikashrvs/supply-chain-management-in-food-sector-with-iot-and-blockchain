"""
Distributor-specific API routes for FoodChain.
All endpoints require the 'distributor' role.
Distributors manage transfer events, checkpoints, and anomaly flags.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from auth import require_role
from database import (
    get_connection,
    fetch_record_rows,
    row_to_dict,
    fetch_latest_sensor_row,
    enrich_transfer_with_sensor,
)
from config import BATCH_KEY_EXPR
from schemas import TransferEventCreate, TransferEventResponse
from audit_logger import log_event
from services.fabric_client import FABRIC_AVAILABLE, submit_to_fabric_async

logger = logging.getLogger("foodchain.distributor")
router = APIRouter(prefix="/api/distributor", tags=["Distributor"])


# ── Batch Lookup (Scan/Search) ────────────────────────────────────────────────

@router.get("/batches/{batch_id}")
def lookup_batch(
    batch_id: str,
    user: dict = Depends(require_role("distributor")),
):
    """
    Look up a batch by ID. Distributors can scan any batch_id to view it.
    Returns batch info, current IoT status, and recent transfer events.
    """
    # Check producer batches table first
    batch_info = None
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,))
        row = cursor.fetchone()
        if row:
            batch_info = dict(row)

    # Get latest IoT readings
    sensor_rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id DESC",
        limit=5,
    )
    latest_readings = [row_to_dict(r) for r in sensor_rows]
    latest = latest_readings[0] if latest_readings else None

    # Get recent transfer events for this batch
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM batch_transfers WHERE batch_id = ? ORDER BY created_at DESC LIMIT 10",
            (batch_id,),
        )
        transfers = [
            enrich_transfer_with_sensor(
                dict(r),
                fetch_latest_sensor_row(cursor, r["batch_id"]),
            )
            for r in cursor.fetchall()
        ]

        # Get IoT summary from sensor_readings
        cursor.execute(
            """
            SELECT COUNT(*) AS cnt, AVG(temperature) AS avg_temp,
                   SUM(CASE WHEN alert_flag = 1 THEN 1 ELSE 0 END) AS alert_count
            FROM sensor_readings WHERE batch_id = ?
            """,
            (batch_id,),
        )
        iot_summary = cursor.fetchone()

    if not batch_info and not latest and iot_summary["cnt"] == 0:
        raise HTTPException(status_code=404, detail=f"Batch '{batch_id}' not found.")

    log_event(
        "BATCH_LOOKUP", "SUCCESS",
        user=user["username"], role=user["role"],
        batch_id=batch_id,
        detail=f"Distributor scanned batch",
    )

    return {
        "batch_id": batch_id,
        "batch_info": batch_info,
        "latest_iot": latest,
        "iot_summary": {
            "reading_count": iot_summary["cnt"] or 0,
            "avg_temp": round(iot_summary["avg_temp"] or 0, 2),
            "alert_count": iot_summary["alert_count"] or 0,
        },
        "recent_transfers": transfers,
    }


# ── Record Transfer Event ─────────────────────────────────────────────────────

@router.post("/transfers", response_model=TransferEventResponse, status_code=201)
def record_transfer(
    payload: TransferEventCreate,
    req: Request,
    user: dict = Depends(require_role("distributor")),
):
    """
    Record a transfer event for a batch.
    event_type: received | transferred | checkpoint | anomaly
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM batches WHERE batch_id = ?", (payload.batch_id,))
        if cursor.fetchone() is None:
            raise HTTPException(status_code=404, detail=f"Batch '{payload.batch_id}' not found.")
        cursor.execute(
            """
            INSERT INTO batch_transfers (
                batch_id, event_type, location_name, latitude, longitude,
                temperature, humidity, notes, anomaly_description,
                created_by, created_by_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.batch_id,
                payload.event_type,
                payload.location_name,
                None,
                None,
                None,
                None,
                payload.notes,
                payload.anomaly_description,
                user["username"],
                user.get("user_id"),
                now,
            ),
        )
        new_id = cursor.lastrowid

        # Update batch status if this is a meaningful event
        if payload.event_type == "received":
            cursor.execute(
                "UPDATE batches SET status = 'in_transit', updated_at = ? WHERE batch_id = ?",
                (now, payload.batch_id),
            )
        elif payload.event_type == "transferred":
            cursor.execute(
                "UPDATE batches SET status = 'transferred', updated_at = ? WHERE batch_id = ?",
                (now, payload.batch_id),
            )
        conn.commit()

    # Async Fabric submission for transfer events
    fabric_tx_id = None
    if FABRIC_AVAILABLE:
        fabric_record = {
            "batch_id": payload.batch_id,
            "event_type": payload.event_type,
            "distributor": user["username"],
            "timestamp": now,
            "location": payload.location_name,
        }

        def _update_fabric_tx(tx_id):
            if tx_id:
                try:
                    with get_connection() as upd_conn:
                        upd_conn.execute(
                            "UPDATE batch_transfers SET blockchain_tx_id = ? WHERE id = ?",
                            (tx_id, new_id),
                        )
                        upd_conn.commit()
                except Exception as e:
                    logger.warning("DB fabric_tx_id update failed: %s", e)

        submit_to_fabric_async(payload.batch_id, fabric_record, callback=_update_fabric_tx)

    log_event(
        f"BATCH_{payload.event_type.upper()}", "SUCCESS",
        user=user["username"], role=user["role"],
        batch_id=payload.batch_id,
        detail=f"Event '{payload.event_type}' at '{payload.location_name}'",
        ip_address=req.client.host if req.client else None,
    )
    logger.info(
        "TRANSFER | event=%s | batch=%s | distributor=%s",
        payload.event_type, payload.batch_id, user["username"],
    )

    with get_connection() as conn:
        latest_sensor = fetch_latest_sensor_row(conn.cursor(), payload.batch_id)
    enriched = enrich_transfer_with_sensor(
        {
            "id": new_id,
            "batch_id": payload.batch_id,
            "event_type": payload.event_type,
            "location_name": payload.location_name,
            "notes": payload.notes,
            "anomaly_description": payload.anomaly_description,
            "created_by": user["username"],
            "created_at": now,
            "blockchain_tx_id": fabric_tx_id,
        },
        latest_sensor,
    )
    return TransferEventResponse(
        id=new_id,
        batch_id=payload.batch_id,
        event_type=payload.event_type,
        location_name=payload.location_name,
        latitude=enriched.get("latitude"),
        longitude=enriched.get("longitude"),
        temperature=enriched.get("temperature"),
        humidity=enriched.get("humidity"),
        notes=payload.notes,
        anomaly_description=payload.anomaly_description,
        created_by=user["username"],
        created_at=now,
        blockchain_tx_id=enriched.get("blockchain_tx_id"),
        status=enriched.get("status"),
        device_id=enriched.get("device_id"),
        latest_iot=enriched.get("latest_iot"),
        block_hash=enriched.get("block_hash"),
        field_hash=enriched.get("field_hash"),
        fabric_tx_id=enriched.get("fabric_tx_id"),
    )


# ── Own Transfer History ──────────────────────────────────────────────────────

@router.get("/transfers", response_model=List[TransferEventResponse])
def list_my_transfers(
    batch_id: Optional[str] = Query(default=None),
    event_type: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    user: dict = Depends(require_role("distributor")),
):
    """List transfer events recorded by this distributor. Distributor only."""
    with get_connection() as conn:
        cursor = conn.cursor()
        where_parts = []
        params: list = []
        if user.get("role") != "admin":
            where_parts.append("created_by = ?")
            params.append(user["username"])

        if batch_id:
            where_parts.append("batch_id = ?")
            params.append(batch_id)
        if event_type:
            where_parts.append("event_type = ?")
            params.append(event_type)

        params.append(limit)
        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        cursor.execute(
            f"SELECT * FROM batch_transfers {where_sql} ORDER BY created_at DESC LIMIT ?",
            params,
        )
        rows = cursor.fetchall()
        enriched_rows = [
            enrich_transfer_with_sensor(
                dict(r),
                fetch_latest_sensor_row(cursor, r["batch_id"]),
            )
            for r in rows
        ]

    return [TransferEventResponse(**row) for row in enriched_rows]


# ── IoT Readings for a Batch ──────────────────────────────────────────────────

@router.get("/batches/{batch_id}/iot")
def get_batch_iot(
    batch_id: str,
    limit: int = Query(default=100, ge=1, le=300),
    user: dict = Depends(require_role("distributor")),
):
    """Get IoT sensor readings for a specific batch. Distributor only."""
    # Live sensor_readings
    sensor_rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id DESC",
        limit=limit,
    )
    sensor_dicts = [row_to_dict(r) for r in sensor_rows]

    return {
        "batch_id": batch_id,
        "iot_count": len(sensor_dicts),
        "sensor_count": len(sensor_dicts),
        "iot_readings": sensor_dicts,
        "sensor_readings": sensor_dicts,
    }


# ── Flag Anomaly ──────────────────────────────────────────────────────────────

@router.post("/anomalies")
def flag_anomaly(
    payload: TransferEventCreate,
    req: Request,
    user: dict = Depends(require_role("distributor")),
):
    """
    Flag an anomaly for a batch (cold-chain violation, packaging issue, etc.).
    This is a convenience wrapper that creates a transfer event with type='anomaly'.
    """
    payload.event_type = "anomaly"
    return record_transfer(payload, req, user)
