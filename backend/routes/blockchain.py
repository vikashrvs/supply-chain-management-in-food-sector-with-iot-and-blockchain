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
    Code-based rule engine — scans all active batches and system services.
    No AI/LLM involved. Pure threshold and pattern checks.

    Rules checked:
      1. Blockchain service downtime
      2. Temperature critically high per stage
      3. Temperature above optimal per stage
      4. Temperature below minimum (freezing risk)
      5. Humidity critically high (mold risk)
      6. Humidity elevated (spoilage risk)
      7. Gas / environment sensor critically high
      8. Gas sensor elevated
      9. Stale IoT data (no update > 2h)
      10. MQTT data gap (no update > 10 min — ESP32 likely offline)
      11. Temperature spike (sudden rise > 5°C from previous reading)
    """
    from database import fetch_latest_batch_rows, row_to_dict, get_connection
    from datetime import datetime
    from services.fabric_client import get_fabric_status

    alerts = []
    now = datetime.now()

    # ── Rule 1: Blockchain / Fabric Service Status ────────────────────────────
    fabric_status = get_fabric_status()
    if not fabric_status["fabric_available"]:
        alerts.append({
            "severity":       "critical",
            "rule":           "blockchain-offline",
            "source":         "BlockchainMonitor",
            "alert_source":   "BlockchainMonitor",
            "alert_flag":     1,
            "batch_id":       "SYSTEM",
            "product":        "Hyperledger Fabric Node",
            "stage":          "blockchain",
            "timestamp":      now.strftime("%Y-%m-%d %H:%M:%S"),
            "message":        "Fabric blockchain service is offline. SHA-256 local hash chain fallback is active.",
            "recommendation": "Check Docker containers (peer0.org1.example.com) in WSL. Run start.bat to restore.",
            "icon":           "fa-link-slash",
        })

    # ── Per-stage temperature thresholds ─────────────────────────────────────
    # Format: stage -> (min_safe, max_safe)
    TEMP_LIMITS = {
        "field":     (5,  35),
        "processing":(2,  25),
        "warehouse": (2,  15),
        "transport": (2,  25),   # real ESP32 in ambient — 28°C is above ideal but not critical
        "retailer":  (2,  18),
        "consumer":  (0,  25),
    }
    TEMP_CRITICAL_MARGIN = 8   # °C above max before "critical" (not just warning)

    # Humidity thresholds
    HUMID_CRITICAL = 92   # % — mold risk
    HUMID_HIGH     = 80   # % — elevated spoilage risk

    # Gas thresholds (ppm)
    GAS_CRITICAL = 200
    GAS_HIGH     = 150

    # Stale data thresholds
    STALE_CRITICAL_MINS = 120   # 2 hours — sensor likely dead
    STALE_WARN_MINS     = 10    # 10 min  — ESP32 probably offline between 30s cycles

    # ── Fetch latest reading per active batch ─────────────────────────────────
    rows    = fetch_latest_batch_rows()
    batches = [row_to_dict(row) for row in rows]

    for b in batches:
        temp    = b.get("temperature")
        humid   = b.get("humidity")
        gas     = b.get("gas_value")
        stage   = (b.get("current_stage") or "transport").lower()
        batch   = b.get("batch_id")  or "UNKNOWN"
        product = b.get("product")   or "Food Stock"
        ts      = b.get("timestamp") or now.strftime("%Y-%m-%d %H:%M:%S")
        mins    = b.get("minutes_since_update")

        t_min, t_max = TEMP_LIMITS.get(stage, (0, 30))

        # ── Rule 2 & 3 & 4: Temperature ──────────────────────────────────────
        if temp is not None:
            if temp > t_max + TEMP_CRITICAL_MARGIN:
                alerts.append({
                    "severity":       "critical",
                    "rule":           "temp-critically-high",
                    "source":         "TempMonitor",
                    "alert_source":   "TempMonitor",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Temperature {temp:.1f}°C — CRITICALLY HIGH (safe max: {t_max}°C at {stage} stage).",
                    "recommendation": "Immediately inspect refrigeration unit. Consider quarantining batch.",
                    "icon":           "fa-fire",
                })
            elif temp > t_max:
                alerts.append({
                    "severity":       "warning",
                    "rule":           "temp-above-range",
                    "source":         "TempMonitor",
                    "alert_source":   "TempMonitor",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Temperature {temp:.1f}°C above safe range ({t_min}–{t_max}°C) at {stage} stage.",
                    "recommendation": "Monitor temperature. Check cooling equipment performance.",
                    "icon":           "fa-temperature-high",
                })
            elif temp < t_min:
                alerts.append({
                    "severity":       "warning",
                    "rule":           "temp-below-range",
                    "source":         "TempMonitor",
                    "alert_source":   "TempMonitor",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Temperature {temp:.1f}°C below minimum ({t_min}°C) — freezing risk.",
                    "recommendation": "Check for freezing damage. Adjust thermostat setting.",
                    "icon":           "fa-snowflake",
                })

        # ── Rule 5 & 6: Humidity ─────────────────────────────────────────────
        if humid is not None:
            if humid > HUMID_CRITICAL:
                alerts.append({
                    "severity":       "critical",
                    "rule":           "humidity-critical",
                    "source":         "HumidityMonitor",
                    "alert_source":   "HumidityMonitor",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Humidity {humid:.1f}% — critical risk of mold and spoilage.",
                    "recommendation": "Activate dehumidifiers immediately. Inspect packaging for water ingress.",
                    "icon":           "fa-droplet",
                })
            elif humid > HUMID_HIGH:
                alerts.append({
                    "severity":       "warning",
                    "rule":           "humidity-elevated",
                    "source":         "HumidityMonitor",
                    "alert_source":   "HumidityMonitor",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Humidity {humid:.1f}% — elevated, monitor for spoilage.",
                    "recommendation": "Improve ventilation. Check packaging integrity.",
                    "icon":           "fa-cloud-rain",
                })

        # ── Rule 7 & 8: Gas / Environment sensor ─────────────────────────────
        if gas is not None and gas > 0:
            if gas > GAS_CRITICAL:
                alerts.append({
                    "severity":       "critical",
                    "rule":           "gas-critical",
                    "source":         "GasMonitor",
                    "alert_source":   "GasMonitor",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Gas/VOC level {gas:.0f} ppm — critical. Possible spoilage or contamination.",
                    "recommendation": "Isolate batch. Inspect for spoilage, chemical contamination, or packaging failure.",
                    "icon":           "fa-skull-crossbones",
                })
            elif gas > GAS_HIGH:
                alerts.append({
                    "severity":       "warning",
                    "rule":           "gas-elevated",
                    "source":         "GasMonitor",
                    "alert_source":   "GasMonitor",
                    "alert_flag":     1,
                    "batch_id":       batch,
                    "product":        product,
                    "stage":          stage,
                    "timestamp":      ts,
                    "message":        f"Gas/VOC level {gas:.0f} ppm — elevated. Monitor closely.",
                    "recommendation": "Inspect product for early spoilage. Increase ventilation.",
                    "icon":           "fa-wind",
                })

        # ── Rule 9: Stale data (sensor likely dead) ───────────────────────────
        if mins is not None and mins > STALE_CRITICAL_MINS:
            alerts.append({
                "severity":       "critical",
                "rule":           "sensor-dead",
                "source":         "ConnectivityMonitor",
                "alert_source":   "ConnectivityMonitor",
                "alert_flag":     1,
                "batch_id":       batch,
                "product":        product,
                "stage":          stage,
                "timestamp":      ts,
                "message":        f"No sensor data for {mins:.0f} minutes — IoT device may be offline or failed.",
                "recommendation": "Check ESP32 power supply, Wi-Fi connection, and MQTT broker.",
                "icon":           "fa-tower-broadcast",
            })
        # ── Rule 10: Short gap (ESP32 between 30s pulses but missing) ─────────
        elif mins is not None and STALE_WARN_MINS < mins <= STALE_CRITICAL_MINS:
            alerts.append({
                "severity":       "info",
                "rule":           "sensor-gap",
                "source":         "ConnectivityMonitor",
                "alert_source":   "ConnectivityMonitor",
                "alert_flag":     0,
                "batch_id":       batch,
                "product":        product,
                "stage":          stage,
                "timestamp":      ts,
                "message":        f"Sensor update gap: {mins:.0f} min. ESP32 may be reconnecting.",
                "recommendation": "Monitor. If gap exceeds 2h, check MQTT broker and ESP32 connectivity.",
                "icon":           "fa-wifi",
            })

    # ── Rule 11: Temperature spike detection (last 2 readings per batch) ─────
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT batch_id, temperature, timestamp
                FROM sensor_readings
                WHERE batch_id IN (
                    SELECT DISTINCT COALESCE(NULLIF(batch_id,''), product_id)
                    FROM sensor_readings
                    WHERE batch_id IS NOT NULL AND batch_id != ''
                )
                AND id IN (
                    SELECT id FROM sensor_readings
                    WHERE batch_id IS NOT NULL
                    ORDER BY id DESC
                    LIMIT 100
                )
                ORDER BY batch_id, id DESC
                """
            )
            spike_rows = cursor.fetchall()

        # Group by batch, take last 2 readings
        from collections import defaultdict
        batch_readings = defaultdict(list)
        for r in spike_rows:
            batch_readings[r["batch_id"]].append(r["temperature"])

        for bid, temps in batch_readings.items():
            if len(temps) >= 2 and temps[0] is not None and temps[1] is not None:
                delta = temps[0] - temps[1]   # newest - previous
                if delta > 5:
                    alerts.append({
                        "severity":       "warning",
                        "rule":           "temp-spike",
                        "source":         "TrendMonitor",
                        "alert_source":   "TrendMonitor",
                        "alert_flag":     1,
                        "batch_id":       bid,
                        "product":        "Batch Stock",
                        "stage":          "transport",
                        "timestamp":      now.strftime("%Y-%m-%d %H:%M:%S"),
                        "message":        f"Sudden temperature rise of +{delta:.1f}°C detected ({temps[1]:.1f}°C → {temps[0]:.1f}°C).",
                        "recommendation": "Check for refrigeration failure or door opening event.",
                        "icon":           "fa-chart-line",
                    })
    except Exception:
        pass   # Spike detection is best-effort — never crash the main alert list

    # ── Sort: critical → warning → info ──────────────────────────────────────
    order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda a: order.get(a["severity"], 3))

    return {"count": len(alerts), "alerts": alerts}
