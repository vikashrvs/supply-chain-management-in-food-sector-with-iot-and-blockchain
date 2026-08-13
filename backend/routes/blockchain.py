"""
Blockchain verification routes — hash chain integrity + Fabric status.
"""

from fastapi import APIRouter, HTTPException

from database import fetch_record_rows
from utils import normalize_batch_id, normalize_stage
from config import BATCH_KEY_EXPR
from services.hash_chain import compute_block_hash
from services.fabric_client import get_fabric_status
from services.fabric_demo_pipeline import seed_demo_rows, register_pending_rows

router = APIRouter(tags=["Blockchain"])


@router.get("/verify/{batch_id}")
def verify_batch_integrity(batch_id: str):
    """Verify SHA256 hash chain integrity for a batch — proves tamper evidence."""
    rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id ASC",
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No records found for batch {batch_id}.")

    prev_hash = "0" * 64
    chain_intact = True
    chain = []

    for row in rows:
        raw_batch_id = normalize_batch_id(row["batch_id"], row["product_id"], f"LEGACY_BATCH_{row['id']:03d}")
        raw_stage    = normalize_stage(row["current_stage"], row["status"])
        record = {
            "batch_id":      raw_batch_id,
            "timestamp":     row["timestamp"],
            "temperature":   row["temperature"],
            "humidity":      row["humidity"],
            "current_stage": raw_stage,
        }

        expected_hash = compute_block_hash(record, prev_hash)
        actual_hash   = row["block_hash"]
        is_valid      = actual_hash == expected_hash

        if not is_valid:
            chain_intact = False

        chain.append({
            "id":            row["id"],
            "timestamp":     row["timestamp"],
            "stage":         raw_stage,
            "expected_hash": expected_hash[:16] + "...",
            "actual_hash":   (actual_hash[:16] + "...") if actual_hash else None,
            "valid":         is_valid,
        })
        prev_hash = expected_hash

    return {
        "batch_id":     batch_id,
        "chain_intact": chain_intact,
        "record_count": len(chain),
        "chain":        chain,
    }


@router.get("/api/blockchain/status")
def blockchain_status():
    """Get current blockchain layer status (Fabric + hash chain)."""
    return get_fabric_status()


@router.get("/api/fabric-status")
def fabric_status():
    """
    Direct endpoint for Fabric connectivity status.
    Dashboard polls this every 30s.
    - fabric_available: true  → Hyperledger Fabric is running (peer0 detected)
    - fabric_available: false → SHA-256 hash-chain-only mode (auto-backfill queued)
    Auto-backfill: backend runs start_background_checker every 30s, which calls
    backfill_pending_fabric_transactions() as soon as Fabric comes online.
    No manual intervention needed.
    """
    return get_fabric_status()


@router.post("/api/demo/register-db-batches")
def register_db_batches(limit: int = 1000, reset: bool = False):
    """Seed a small database-backed demo and submit pending rows to Fabric."""
    seed_demo_rows(reset=reset, limit=min(max(limit, 1), 50))
    result = register_pending_rows(limit=limit)
    return {
        "status": "ok",
        "fabric": get_fabric_status(),
        **result,
    }


@router.get("/api/agent-alerts")
def get_agent_alerts():
    """
    AI Agent rule-based anomaly scan across all latest batch readings and system services.
    Detects: temperature out of range, high humidity, stale IoT data, and Fabric service downtime.
    Returns structured alerts with severity levels, sender attribution, and AI recommendations.
    """
    from database import fetch_latest_batch_rows, row_to_dict
    from datetime import datetime
    from services.fabric_client import get_fabric_status

    alerts = []

    # Check 1: Fabric Blockchain Service Status (use cached state — do NOT re-run docker ps here)
    fabric_status = get_fabric_status()
    fabric_online = fabric_status["fabric_available"]
    if not fabric_online:
        alerts.append({
            "severity":       "critical",
            "rule":           "blockchain-downtime",
            "source":         "Ledger-Guard",
            "alert_source":   "Ledger-Guard",
            "alert_flag":     1,
            "batch_id":       "SYSTEM",
            "product":        "Hyperledger Fabric Node",
            "stage":          "blockchain",
            "timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message":        "Hyperledger Fabric blockchain service is down/unreachable. System fallback active (SHA-256 local hash chain).",
            "recommendation": "Verify Docker containers (peer0.org1.example.com) in WSL. Run start.bat to restore Fabric network.",
            "icon":           "fa-link-slash",
        })

    # Check 2: Sensor Anomalies across Active Batches
    rows    = fetch_latest_batch_rows()
    batches = [row_to_dict(row) for row in rows]

    for b in batches:
        temp    = b.get("temperature")
        humid   = b.get("humidity")
        stage   = (b.get("current_stage") or "transport").lower()
        batch   = b.get("batch_id")   or "UNKNOWN"
        product = b.get("product")    or "Food Stock"
        ts      = b.get("timestamp")  or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Temperature limits per supply chain stage
        temp_limits = {
            "field":     (0,  35),
            "warehouse": (2,  15),
            "transport": (2,  20),
            "retailer":  (2,  18),
            "consumer":  (0,  25),
        }
        t_min, t_max = temp_limits.get(stage, (0, 30))

        if temp is not None:
            if temp > t_max + 5:
                alerts.append({
                    "severity":       "critical",
                    "rule":           "temp-high",
                    "source":         "Aero",
                    "alert_source":   "Aero",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Temperature {temp:.1f}°C critically high (limit: {t_max}°C) at {stage} stage.",
                    "recommendation": "Inspect refrigeration unit immediately. Consider quarantining batch.",
                    "icon":           "fa-fire",
                })
            elif temp > t_max:
                alerts.append({
                    "severity":       "warning",
                    "rule":           "temp-high-warn",
                    "source":         "Aero",
                    "alert_source":   "Aero",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Temperature {temp:.1f}°C above optimal ({t_min}–{t_max}°C).",
                    "recommendation": "Monitor temperature. Check cooling equipment.",
                    "icon":           "fa-temperature-high",
                })
            elif temp < t_min:
                alerts.append({
                    "severity":       "warning",
                    "rule":           "temp-low",
                    "source":         "Aero",
                    "alert_source":   "Aero",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Temperature {temp:.1f}°C below minimum ({t_min}°C).",
                    "recommendation": "Check for freezing risk. Adjust storage temperature.",
                    "icon":           "fa-snowflake",
                })

        if humid is not None:
            if humid > 92:
                alerts.append({
                    "severity":       "critical",
                    "rule":           "humid-critical",
                    "source":         "Orion",
                    "alert_source":   "Orion",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Humidity {humid:.1f}% — critical risk of mold and spoilage.",
                    "recommendation": "Activate dehumidifiers immediately. Inspect for water ingress.",
                    "icon":           "fa-droplet",
                })
            elif humid > 80:
                alerts.append({
                    "severity":       "warning",
                    "rule":           "humid-high",
                    "source":         "Orion",
                    "alert_source":   "Orion",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Elevated humidity {humid:.1f}%. Monitor for spoilage.",
                    "recommendation": "Increase ventilation. Check packaging integrity.",
                    "icon":           "fa-cloud-rain",
                })

        # Stale IoT data check
        mins = b.get("minutes_since_update")
        if mins is not None and mins > 120:
            alerts.append({
                "severity":       "info",
                "rule":           "stale-data",
                "source":         "Data-Tron",
                "alert_source":   "Data-Tron",
                "alert_flag":     1,
                "batch_id":       batch,
                "product":        product,
                "stage":          stage,
                "timestamp":      ts,
                "message":        f"No sensor update for {mins} minutes.",
                "recommendation": "Check IoT sensor connectivity. Verify MQTT broker is running.",
                "icon":           "fa-wifi-slash",
            })

    # Sort: critical → warning → info
    order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda a: order.get(a["severity"], 3))

    return {"count": len(alerts), "alerts": alerts}

