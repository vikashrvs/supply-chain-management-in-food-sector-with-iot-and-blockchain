"""
Replay / IoT Live Feed routes for the FoodChain dashboard.

These endpoints are isolated legacy/demo endpoints. Operational dashboards
must use the role APIs and /data, which read live sensor_data populated by MQTT.
Demo fallback is available only when the caller explicitly passes demo=true.

Endpoints:
  GET /api/replay/transportation?batch_id=FC-001
      → Primary dashboard data feed (live temp/humidity/GPS from ESP32)
  GET /api/replay/dataset?batch_id=FC-001
      → Route / metadata for the map on first load
  POST /api/replay/start, /pause, /step, /reset
      → No-op stubs (kept for UI button compatibility)
"""

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from database import (
    get_connection,
    get_replay_batch_config,
    get_replay_batch_options,
    get_active_batch_options,
    get_latest_active_batch,
    row_to_dict,
    fetch_record_rows,
)
from config import BATCH_KEY_EXPR

logger = logging.getLogger("foodchain.replay")
router = APIRouter(prefix="/api/replay", tags=["Replay / Live IoT"])


# ── helpers ──────────────────────────────────────────────────────────────────

def _fetch_sensor_history(batch_id: str, limit: int = 50) -> list[dict]:
    """Return the most recent `limit` sensor_data records for a batch (newest last)."""
    rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id DESC",
        limit=limit,
    )
    return list(reversed([row_to_dict(row) for row in rows]))


def _batch_meta(batch_id: str, demo: bool = False) -> dict:
    """Pull metadata from live records, using demo config only by explicit opt-in."""
    cfg = get_replay_batch_config(batch_id) if demo else {}

    # Try to refine device_id / product from the actual DB records
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT sensor_id, product_name, origin_name, destination_name
                FROM sensor_data
                WHERE batch_id = ?
                ORDER BY id DESC LIMIT 1
                """,
                (batch_id,),
            )
            row = cursor.fetchone()
    except Exception:
        row = None

    device_id   = (row["sensor_id"]       if row and row["sensor_id"]       else cfg.get("device_id"))
    product     = (row["product_name"]    if row and row["product_name"]    else cfg.get("product"))
    origin      = (row["origin_name"]     if row and row["origin_name"]     else cfg.get("origin"))
    destination = (row["destination_name"] if row and row["destination_name"] else cfg.get("destination"))

    return {
        "batch_id":    batch_id,
        "device_id":   device_id,
        "product":     product,
        "origin":      origin,
        "destination": destination,
    }


# ── /api/replay/transportation ────────────────────────────────────────────────

@router.get("/transportation")
def get_transportation_state(
    batch_id: str | None = Query(default=None),
    demo: bool = Query(default=False),
):
    """
    Primary live-data feed for the dashboard sensor panel.

    Reads the LAST 50 real sensor_data records for the batch
    (populated by MQTT from the physical ESP32) and returns:
      - latest  : most recent reading (temp, humidity, GPS, stage …)
      - history : up to 50 records for the blockchain table
      - progress_pct, received_count, total_records
      - origin / destination / device_id
    """
    if not isinstance(batch_id, str) or not batch_id or batch_id in ("undefined", "null", "auto"):
        batch_id = get_latest_active_batch()
        if not batch_id and demo:
            batch_id = "FC-001"
    if not batch_id:
        raise HTTPException(status_code=404, detail="No live sensor batch is available.")

    history = _fetch_sensor_history(batch_id, limit=50)
    if not history and not demo:
        raise HTTPException(status_code=404, detail=f"No live sensor data exists for batch '{batch_id}'.")
    meta    = _batch_meta(batch_id, demo=demo)
    latest  = history[-1] if history else None

    # Progress: based on how many records exist (cap display at 100)
    received_count = len(history)
    total_records  = max(received_count, 100)   # at least 100 for a nice progress bar
    progress_pct   = min(100, round((received_count / total_records) * 100))

    status = "READY"
    if latest:
        ts_str = latest.get("timestamp", "")
        status = latest.get("transportation_status") or latest.get("status") or "IN TRANSIT"
        try:
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            mins_old = (datetime.now() - ts).total_seconds() / 60
            if mins_old > 10:
                status = "WAITING FOR ESP32"
        except Exception:
            pass

    return {
        "batch_id":       meta["batch_id"],
        "device_id":      meta["device_id"],
        "product":        meta["product"],
        "origin":         meta["origin"],
        "destination":    meta["destination"],
        "status":         status,
        "progress_pct":   progress_pct,
        "received_count": received_count,
        "total_records":  total_records,
        "latest":         latest,
        "history":        history,
        "available_batches": (
            get_active_batch_options()
            or (get_replay_batch_options() if demo else [])
        ),
    }


# ── /api/replay/dataset ───────────────────────────────────────────────────────

@router.get("/dataset")
def get_replay_dataset(
    batch_id: str | None = Query(default=None),
    demo: bool = Query(default=False),
):
    """
    Returns GPS route points and metadata for the map initialisation.
    Pulls actual coordinates from sensor_data records so the truck
    marker follows the real ESP32 GPS track.
    """
    if not isinstance(batch_id, str) or not batch_id or batch_id in ("undefined", "null", "auto"):
        batch_id = get_latest_active_batch()
        if not batch_id and demo:
            batch_id = "FC-001"
    if not batch_id:
        raise HTTPException(status_code=404, detail="No live sensor batch is available.")

    history = _fetch_sensor_history(batch_id, limit=200)
    if not history and not demo:
        raise HTTPException(status_code=404, detail=f"No live sensor data exists for batch '{batch_id}'.")
    meta    = _batch_meta(batch_id, demo=demo)

    records = [
        {
            "record_id":  r.get("id"),
            "latitude":   r.get("latitude"),
            "longitude":  r.get("longitude"),
            "timestamp":  r.get("timestamp"),
            "temperature": r.get("temperature"),
            "humidity":   r.get("humidity"),
        }
        for r in history
        if r.get("latitude") is not None and r.get("longitude") is not None
    ]

    return {
        "batch_id":    meta["batch_id"],
        "device_id":   meta["device_id"],
        "product":     meta["product"],
        "origin":      meta["origin"],
        "destination": meta["destination"],
        "records":     records,
        "available_batches": (
            get_active_batch_options()
            or (get_replay_batch_options() if demo else [])
        ),
    }


# ── Replay control stubs (UI buttons — no actual replay engine needed) ─────────

@router.post("/start")
def replay_start(
    batch_id:         str   = Query(default="FC-001"),
    interval_seconds: float = Query(default=2),
    reset:            bool  = Query(default=False),
    demo:             bool  = Query(default=False),
):
    """No-op control retained for the explicit legacy/demo page."""
    if not demo:
        raise HTTPException(status_code=403, detail="Replay is disabled unless demo=true.")
    logger.info("replay/start called (explicit demo opt-in) batch=%s", batch_id)
    return {"status": "demo-opt-in", "message": "Replay controls are available only for explicit demo mode."}

@router.post("/pause")
def replay_pause(demo: bool = Query(default=False)):
    if not demo:
        raise HTTPException(status_code=403, detail="Replay is disabled unless demo=true.")
    return {"status": "demo-opt-in", "message": "Replay pause acknowledged for explicit demo mode."}

@router.post("/step")
def replay_step(demo: bool = Query(default=False)):
    if not demo:
        raise HTTPException(status_code=403, detail="Replay is disabled unless demo=true.")
    return {"status": "demo-opt-in", "message": "Replay step acknowledged for explicit demo mode."}

@router.post("/reset")
def replay_reset(
    batch_id: str = Query(default="FC-001"),
    demo: bool = Query(default=False),
):
    if not demo:
        raise HTTPException(status_code=403, detail="Replay is disabled unless demo=true.")
    return {"status": "demo-opt-in", "message": f"Replay reset acknowledged for {batch_id}."}
