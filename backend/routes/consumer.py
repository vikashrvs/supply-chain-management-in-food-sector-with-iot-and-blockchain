"""
Consumer (public) API routes for FoodChain.
These endpoints are public — no authentication required.
Batch verification is read-only and safe-for-public display.
Never exposes: passwords, tokens, private keys, internal credentials, or system config.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from database import get_connection, fetch_record_rows, row_to_dict, fetch_batch_history
from config import BATCH_KEY_EXPR, SUPPLY_CHAIN_STAGES
from services.edge_health import summarize_product_history
from services.hash_chain import compute_block_hash
from utils import normalize_batch_id, normalize_stage
from audit_logger import log_event

logger = logging.getLogger("foodchain.consumer")
router = APIRouter(prefix="/api/consumer", tags=["Consumer"])

# Fields safe to expose to the public (no credentials/internal data)
_SAFE_IOT_FIELDS = [
    "timestamp", "temperature", "humidity", "latitude", "longitude",
    "current_stage", "current_stage_label", "status", "alert_status",
    "alert_flag", "alert_source", "origin_name", "destination_name",
    "is_active", "blockchain_verification", "block_hash", "fabric_tx_id",
    "risk_level", "health_label", "is_healthy", "alerts",
]

_SAFE_BATCH_FIELDS = [
    "batch_id", "product_name", "product_type", "origin", "destination",
    "quantity", "harvest_date", "status", "created_at",
]

_SAFE_PRODUCER_FIELDS = ["username", "role"]


def _sanitize_iot(record: dict) -> dict:
    """Strip sensitive fields from IoT record for public display."""
    return {k: v for k, v in record.items() if k in _SAFE_IOT_FIELDS}


@router.get("/verify/{batch_id}")
def verify_batch(
    batch_id: str,
    include_iot: bool = Query(default=True),
):
    """
    Public batch verification endpoint.
    Returns traceability information safe for consumer display.
    No authentication required. Read-only.
    """
    batch_id = batch_id.strip()
    if not batch_id or len(batch_id) > 60:
        raise HTTPException(status_code=400, detail="Invalid batch ID.")

    log_event("BATCH_LOOKUP", "SUCCESS", user=None, role="consumer", batch_id=batch_id,
              detail="Consumer batch verification")

    # ── 1. Batch registry info (from producer batches table) ─────────────────
    batch_info = None
    producer_info = None
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,))
        b = cursor.fetchone()
        if b:
            batch_info = {k: b[k] for k in _SAFE_BATCH_FIELDS if k in b.keys()}
            # Safe producer info (no password/token)
            cursor.execute(
                "SELECT username FROM users WHERE username = ?",
                (b["created_by"],),
            )
            pu = cursor.fetchone()
            if pu:
                producer_info = {"producer": pu["username"]}

    # ── 2. Sensor data history ────────────────────────────────────────────────
    sensor_rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id ASC",
        limit=200,
    )
    history = [row_to_dict(r) for r in sensor_rows]
    safe_history = [_sanitize_iot(r) for r in history]
    iot_readings = safe_history if include_iot else []
    latest = history[-1] if history else None
    safe_latest = _sanitize_iot(latest) if latest else None

    # ── 4. Supply-chain summary ───────────────────────────────────────────────
    summary = summarize_product_history(history) if history else {}

    # ── 5. Transfer events (safe fields only) ────────────────────────────────
    transfer_events = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT event_type, location_name, latitude, longitude,
                   temperature, humidity, notes, created_at
            FROM batch_transfers
            WHERE batch_id = ?
            ORDER BY created_at ASC
            """,
            (batch_id,),
        )
        transfer_events = [dict(r) for r in cursor.fetchall()]

    # ── 6. Blockchain verification ────────────────────────────────────────────
    blockchain_verified = False
    has_fabric_tx = False
    chain_intact = None
    if history:
        has_fabric_tx = any(r.get("fabric_tx_id") for r in history)
        blockchain_verified = any(r.get("block_hash") for r in history)
        # Quick chain integrity check
        prev_hash = "0" * 64
        chain_intact = True
        for row in sensor_rows:
            record_for_hash = {
                "batch_id": normalize_batch_id(row["batch_id"], row["product_id"]),
                "timestamp": row["timestamp"],
                "temperature": row["temperature"],
                "humidity": row["humidity"],
                "current_stage": normalize_stage(row["current_stage"], row["status"]),
            }
            expected = compute_block_hash(record_for_hash, prev_hash)
            if row["block_hash"] and row["block_hash"] != expected:
                chain_intact = False
                break
            prev_hash = expected

    # ── 7. Anomaly summary ────────────────────────────────────────────────────
    anomaly_count = sum(1 for r in safe_history if r.get("alert_flag"))
    anomaly_events = [r for r in safe_history if r.get("alert_flag")]

    # ── 8. Overall status ────────────────────────────────────────────────────
    if not batch_info and not history:
        raise HTTPException(
            status_code=404,
            detail=f"Batch '{batch_id}' not found. Please check the batch ID and try again.",
        )

    return {
        "batch_id": batch_id,
        "found": True,
        # Safe product/batch info
        "product": batch_info or {
            "batch_id": batch_id,
            "product_name": (latest or {}).get("product_name", f"Product {batch_id}"),
            "origin": (latest or {}).get("origin_name"),
            "destination": (latest or {}).get("destination_name"),
            "status": (latest or {}).get("status", "In Transit"),
        },
        "producer": producer_info,
        # Supply chain journey
        "supply_chain_stages": SUPPLY_CHAIN_STAGES,
        "supply_chain_summary": summary,
        "transfer_history": transfer_events,
        # IoT data
        "iot": {
            "reading_count": len(iot_readings),
            "sensor_count": len(safe_history),
            "latest": safe_latest,
            "anomaly_count": anomaly_count,
            "recent_anomalies": anomaly_events[-5:],  # Last 5 anomalies only
            "readings": iot_readings if len(iot_readings) <= 100 else iot_readings[-100:],
        },
        # Blockchain
        "blockchain": {
            "verified": blockchain_verified,
            "has_fabric_transactions": has_fabric_tx,
            "chain_integrity": chain_intact,
            "mode": "Hyperledger Fabric" if has_fabric_tx else "SHA-256 Hash Chain",
            "record_count": len(history),
        },
        # Quality & Safety report is rendered on client
        "ai_analysis_available": True,
    }


@router.get("/batches")
def search_batch(batch_id: str = Query(..., min_length=1, max_length=60)):
    """
    Quick batch existence check for consumer search box.
    Returns minimal info to confirm batch exists before full lookup.
    """
    batch_id = batch_id.strip()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT batch_id, product_name, status FROM batches WHERE batch_id = ?",
            (batch_id,),
        )
        row = cursor.fetchone()

    if row:
        return {"found": True, "batch_id": row["batch_id"],
                "product_name": row["product_name"], "status": row["status"]}

    # Check sensor_data / replay_telemetry as fallback
    sensor_rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id DESC",
        limit=1,
    )
    if sensor_rows:
        r = row_to_dict(sensor_rows[0])
        return {"found": True, "batch_id": batch_id,
                "product_name": r.get("product_name", f"Product {batch_id}"),
                "status": r.get("status", "In Transit")}

    return {"found": False, "batch_id": batch_id}
