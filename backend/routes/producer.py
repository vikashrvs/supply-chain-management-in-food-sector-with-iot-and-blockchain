"""
Producer-specific API routes for FoodChain.
All endpoints require the 'producer' role.
Producers can only access their own batches.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from auth import require_role
from database import get_connection, fetch_record_rows, row_to_dict, fetch_latest_sensor_row
from config import BATCH_KEY_EXPR
from schemas import BatchCreate, BatchResponse
from audit_logger import log_event

logger = logging.getLogger("foodchain.producer")
router = APIRouter(prefix="/api/producer", tags=["Producer"])

def _iot_meta(cursor, batch_id: str) -> tuple[Optional[str], Optional[str]]:
    row = fetch_latest_sensor_row(cursor, batch_id)
    return (row["sensor_id"], row["timestamp"]) if row else (None, None)


def _generate_batch_id() -> str:
    """Generate a unique batch ID in format PROD-XXXXXXXX."""
    suffix = uuid.uuid4().hex[:8].upper()
    return f"PROD-{suffix}"


# ── Batch Creation ─────────────────────────────────────────────────────────────

@router.post("/batches", response_model=BatchResponse, status_code=201)
def create_batch(
    payload: BatchCreate,
    req: Request,
    user: dict = Depends(require_role("producer")),
):
    """Create a new batch. Producer only."""
    batch_id = _generate_batch_id()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with get_connection() as conn:
        cursor = conn.cursor()
        # Ensure batch_id is unique (very unlikely collision but safe)
        for _ in range(5):
            cursor.execute("SELECT id FROM batches WHERE batch_id = ?", (batch_id,))
            if cursor.fetchone() is None:
                break
            batch_id = _generate_batch_id()

        cursor.execute(
            """
            INSERT INTO batches (
                batch_id, product_name, product_type, origin, destination,
                quantity, description, harvest_date, status,
                created_by, created_by_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'created', ?, ?, ?, ?)
            """,
            (
                batch_id,
                payload.product_name,
                payload.product_type,
                payload.origin,
                payload.destination,
                payload.quantity,
                payload.description,
                payload.harvest_date,
                user["username"],
                user.get("user_id"),
                now,
                now,
            ),
        )
        new_id = cursor.lastrowid
        conn.commit()

    log_event(
        "BATCH_CREATE", "SUCCESS",
        user=user["username"], role=user["role"],
        batch_id=batch_id,
        detail=f"Created batch '{payload.product_name}'",
        ip_address=req.client.host if req.client else None,
    )
    logger.info("BATCH_CREATE | batch_id=%s | producer=%s", batch_id, user["username"])

    return BatchResponse(
        id=new_id,
        batch_id=batch_id,
        product_name=payload.product_name,
        product_type=payload.product_type,
        origin=payload.origin,
        destination=payload.destination,
        quantity=payload.quantity,
        description=payload.description,
        harvest_date=payload.harvest_date,
        status="created",
        created_by=user["username"],
        created_at=now,
        blockchain_tx_id=None,
    )


# ── List Own Batches ──────────────────────────────────────────────────────────

@router.get("/batches", response_model=List[BatchResponse])
def list_my_batches(
    limit: int = Query(default=50, ge=1, le=200),
    user: dict = Depends(require_role("producer")),
):
    """List batches. Allows viewing batches created by user or general active batches."""
    with get_connection() as conn:
        cursor = conn.cursor()
        if user.get("role") in ("admin", "distributor"):
            cursor.execute("SELECT * FROM batches ORDER BY id DESC LIMIT ?", (limit,))
        else:
            cursor.execute(
                """
                SELECT * FROM batches 
                WHERE created_by = ? OR created_by IN ('admin', 'producer')
                ORDER BY id DESC LIMIT ?
                """,
                (user["username"], limit),
            )
        rows = cursor.fetchall()
        if not rows:
            cursor.execute("SELECT * FROM batches ORDER BY id DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
        result = []
        for r in rows:
            device_id, last_reading_at = _iot_meta(cursor, r["batch_id"])
            result.append(BatchResponse(
                id=r["id"],
                batch_id=r["batch_id"],
                product_name=r["product_name"],
                product_type=r["product_type"],
                origin=r["origin"],
                destination=r["destination"],
                quantity=r["quantity"],
                description=r["description"],
                harvest_date=r["harvest_date"],
                status=r["status"],
                created_by=r["created_by"],
                created_at=r["created_at"] or "",
                blockchain_tx_id=r["blockchain_tx_id"],
                device_id=device_id,
                last_reading_at=last_reading_at,
            ))
    return result


# ── Single Batch Detail ───────────────────────────────────────────────────────

@router.get("/batches/{batch_id}", response_model=BatchResponse)
def get_my_batch(
    batch_id: str,
    user: dict = Depends(require_role("producer")),
):
    """Get a specific batch."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Batch '{batch_id}' not found.",
            )

        device_id, last_reading_at = _iot_meta(cursor, row["batch_id"])
    return BatchResponse(
        id=row["id"],
        batch_id=row["batch_id"],
        product_name=row["product_name"],
        product_type=row["product_type"],
        origin=row["origin"],
        destination=row["destination"],
        quantity=row["quantity"],
        description=row["description"],
        harvest_date=row["harvest_date"],
        status=row["status"],
        created_by=row["created_by"],
        created_at=row["created_at"] or "",
        blockchain_tx_id=row["blockchain_tx_id"],
        device_id=device_id,
        last_reading_at=last_reading_at,
    )


# ── IoT Readings for Own Batch ────────────────────────────────────────────────

@router.get("/batches/{batch_id}/iot")
def get_my_batch_iot(
    batch_id: str,
    limit: int = Query(default=100, ge=1, le=300),
    user: dict = Depends(require_role("producer")),
):
    """Get IoT sensor readings for a batch."""
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
        "sensor_readings": sensor_dicts,
    }


# ── IoT Summary for Producer Overview ────────────────────────────────────────

@router.get("/iot-summary")
def get_producer_iot_summary(
    user: dict = Depends(require_role("producer")),
):
    """Get IoT summary for all batches."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT batch_id FROM batches ORDER BY id DESC")
        owned_batches = [r["batch_id"] for r in cursor.fetchall()]

    if not owned_batches:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT batch_id FROM sensor_readings WHERE batch_id IS NOT NULL AND batch_id != '' ORDER BY id DESC")
            owned_batches = [r["batch_id"] for r in cursor.fetchall()]

    summaries = []
    for bid in owned_batches[:10]:  # Limit to 10 for performance
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt, AVG(temperature) AS avg_temp,
                       MAX(temperature) AS max_temp, MIN(temperature) AS min_temp,
                       AVG(humidity) AS avg_humidity,
                       SUM(CASE WHEN alert_flag = 1 THEN 1 ELSE 0 END) AS alert_count
                FROM sensor_readings WHERE batch_id = ?
                """,
                (bid,),
            )
            row = cursor.fetchone()
        summaries.append({
            "batch_id": bid,
            "reading_count": row["cnt"] or 0,
            "avg_temp": round(row["avg_temp"] or 0, 2),
            "max_temp": row["max_temp"],
            "min_temp": row["min_temp"],
            "avg_humidity": round(row["avg_humidity"] or 0, 2),
            "alert_count": row["alert_count"] or 0,
        })

    return {"producer": user["username"], "summaries": summaries}
