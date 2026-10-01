"""
Database layer for FoodChain backend.
All database functions extracted from monolithic main.py.
"""

import sqlite3
from datetime import datetime

import bcrypt

from config import (
    DB_PATH,
    SUPPLY_CHAIN_STAGES,
    STALE_BATCH_HOURS,
    RECORD_SELECT,
    BATCH_KEY_EXPR,
    UID_KEY_EXPR,
    RISK_PRIORITY,
)
from services.hash_chain import get_last_hash, compute_block_hash, compute_record_hash
from services.edge_health import evaluate_edge_health

# ── Password hashing context ────────────────────────────────────────────────
class PasswordContext:
    def hash(self, password: str) -> str:
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    def verify(self, password: str, password_hash: str) -> bool:
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except Exception:
            return False

pwd_context = PasswordContext()


# ── Connection ───────────────────────────────────────────────────────────────

def get_connection():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    return connection


# ── Schema helpers ───────────────────────────────────────────────────────────

def get_table_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row["name"] for row in cursor.fetchall()}


def ensure_column(cursor, table_name, column_name, definition):
    columns = get_table_columns(cursor, table_name)
    if column_name not in columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


# ── Normalize helpers (imported from utils to avoid circular imports) ────────
from utils import (
    normalize_stage,
    normalize_status,
    normalize_batch_id,
    normalize_product_uid,
    normalize_product,
    format_stage_label,
    format_range,
    parse_timestamp,
)


# ── Record builders ──────────────────────────────────────────────────────────

def build_record(data):
    location = data.get("location") or {}
    gas_value = data.get("gas_value")
    if gas_value is None:
        gas_value = data.get("environmental_value")
    batch_id = normalize_batch_id(
        data.get("batch_id"),
        product_id=data.get("product_id"),
        fallback="UNKNOWN_BATCH",
    )
    product_uid = normalize_product_uid(
        data.get("product_uid"),
        batch_id=batch_id,
        product_id=data.get("product_id"),
        fallback="UNKNOWN_UID",
    )
    product = normalize_product(data.get("product"), data.get("product_name"), batch_id)
    current_stage = normalize_stage(data.get("current_stage"), data.get("status"))
    status = normalize_status(data.get("status"), current_stage)

    return {
        "timestamp": data.get("timestamp") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": data.get("temperature"),
        "humidity": data.get("humidity"),
        "latitude": data.get("latitude") if data.get("latitude") is not None else location.get("lat"),
        "longitude": data.get("longitude") if data.get("longitude") is not None else location.get("lng"),
        "gas_value": gas_value,
        "product_id": normalize_batch_id(data.get("product_id"), batch_id),
        "status": status,
        "product_name": data.get("product_name") or product,
        "batch_id": batch_id,
        "product_uid": product_uid,
        "product": product,
        "sensor_id": (data.get("sensor_id") or "UNKNOWN_SENSOR").strip() or "UNKNOWN_SENSOR",
        "current_stage": current_stage,
        "transportation_status": data.get("transportation_status") or data.get("status"),
        "alert_status": data.get("alert_status") or "NORMAL",
        "telemetry_mode": data.get("telemetry_mode") or "Physical ESP32 Telemetry",
        "origin_name": data.get("origin_name"),
        "destination_name": data.get("destination_name"),
        "replay_record_id": data.get("record_id") or data.get("replay_record_id"),
        "alert_flag": data.get("alert_flag") if data.get("alert_flag") is not None else (1 if (data.get("alert_status") and data.get("alert_status") != "NORMAL") else 0),
        "alert_source": data.get("alert_source") or data.get("source") or "sensor",
    }


def row_to_dict(row):
    batch_id = normalize_batch_id(row["batch_id"], row["product_id"], f"LEGACY_BATCH_{row['id']:03d}")
    product_uid = normalize_product_uid(row["product_uid"], batch_id, row["product_id"], f"UID_{row['id']:03d}")
    product = normalize_product(row["product"], row["product_name"], batch_id)
    current_stage = normalize_stage(row["current_stage"], row["status"])
    status = normalize_status(row["status"], current_stage)
    edge_health = evaluate_edge_health(current_stage, row["temperature"], row["humidity"])
    gas_value = row["gas_value"] if "gas_value" in row.keys() else None
    if gas_value is not None and gas_value > 180:
        edge_health["alerts"].append(f"Gas/environment value {gas_value} ppm is above the demo limit (180 ppm).")
        edge_health["alert_count"] = len(edge_health["alerts"])
        edge_health["risk_level"] = "critical" if edge_health["alert_count"] >= 2 else "warning"
        edge_health["health_label"] = edge_health["risk_level"].title()
        edge_health["is_healthy"] = False
        edge_health["edge_decision"] = "Review refrigerated vehicle environment before the next checkpoint."
    parsed_timestamp = parse_timestamp(row["timestamp"])
    age_minutes = None
    is_active = False
    if parsed_timestamp is not None:
        age_minutes = max(int((datetime.now() - parsed_timestamp).total_seconds() // 60), 0)
        is_active = age_minutes <= STALE_BATCH_HOURS * 60

    block_hash = row["block_hash"] if row["block_hash"] else None

    return {
        "id": row["id"],
        "timestamp": row["timestamp"],
        "temperature": row["temperature"],
        "humidity": row["humidity"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "gas_value": gas_value,
        "location": {"lat": row["latitude"], "lng": row["longitude"]},
        "batch_id": batch_id,
        "product_uid": product_uid,
        "product": product,
        "product_name": product,
        "product_id": row["product_id"] or batch_id,
        "sensor_id": row["sensor_id"] or "UNKNOWN_SENSOR",
        "device_id": row["sensor_id"] or "UNKNOWN_SENSOR",
        "current_stage": current_stage,
        "current_stage_label": format_stage_label(current_stage),
        "current_stage_index": SUPPLY_CHAIN_STAGES.index(current_stage),
        "status": status,
        "block_hash": block_hash,
        "fabric_tx_id": row["fabric_tx_id"] if "fabric_tx_id" in row.keys() and row["fabric_tx_id"] else None,
        "blockchain_verification": "Blockchain Verified ✓" if block_hash else "Pending",
        "field_hash": row["field_hash"] if "field_hash" in row.keys() and row["field_hash"] else None,
        "transportation_status": row["transportation_status"] if "transportation_status" in row.keys() and row["transportation_status"] else status,
        "alert_status": row["alert_status"] if "alert_status" in row.keys() and row["alert_status"] else ("ALERT" if edge_health["alert_count"] else "NORMAL"),
        "telemetry_mode": row["telemetry_mode"] if "telemetry_mode" in row.keys() and row["telemetry_mode"] else "Physical ESP32 Telemetry",
        "origin_name": row["origin_name"] if "origin_name" in row.keys() else None,
        "destination_name": row["destination_name"] if "destination_name" in row.keys() else None,
        "replay_record_id": row["replay_record_id"] if "replay_record_id" in row.keys() else None,
        "alert_flag": row["alert_flag"] if "alert_flag" in row.keys() and row["alert_flag"] is not None else (1 if edge_health["alert_count"] else 0),
        "alert_source": row["alert_source"] if "alert_source" in row.keys() and row["alert_source"] else ("Aero" if edge_health["alert_count"] else "sensor"),
        "source": row["alert_source"] if "alert_source" in row.keys() and row["alert_source"] else ("Aero" if edge_health["alert_count"] else "sensor"),
        "is_active": is_active,
        "minutes_since_update": age_minutes,
        **edge_health,
    }


def row_to_legacy_list(row):
    record = row_to_dict(row)
    return [
        record["id"],            # row[0] - Batch ID
        record["timestamp"],     # row[1]
        record["temperature"],   # row[2]
        record["humidity"],      # row[3]
        record["latitude"],      # row[4]
        record["longitude"],     # row[5]
        record["product_uid"],   # row[6]
        record["status"],        # row[7]
        record["product_name"],  # row[8]
        record["field_hash"],    # row[9] - SHA-256 field integrity hash
    ]


# ── Query helpers ────────────────────────────────────────────────────────────

def fetch_rows(query, params=()):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()


def fetch_record_rows(where_clause="", params=(), order_by="sd.id DESC", limit=None):
    query = RECORD_SELECT
    if where_clause:
        query += f" WHERE {where_clause}"
    query += f" ORDER BY {order_by}"
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    return fetch_rows(query, params)


def fetch_latest_sensor_row(cursor, batch_id):
    """Return the newest canonical sensor record associated with a batch."""
    cursor.execute(
        """
        SELECT sd.*
        FROM sensor_data sd
        WHERE NULLIF(sd.batch_id, '') = ?
           OR NULLIF(sd.product_id, '') = ?
           OR sd.product_ref IN (
               SELECT id FROM product_registry WHERE batch_id = ?
           )
        ORDER BY sd.id DESC
        LIMIT 1
        """,
        (batch_id, batch_id, batch_id),
    )
    return cursor.fetchone()


def enrich_transfer_with_sensor(transfer, sensor_row):
    """Add canonical live telemetry and ledger fields to a transfer payload."""
    result = dict(transfer)
    if not sensor_row:
        result.setdefault("latest_iot", None)
        result.setdefault("device_id", None)
        return result

    sensor = row_to_dict(sensor_row)
    result["latest_iot"] = sensor
    result["device_id"] = sensor["device_id"]
    result["status"] = result.get("status") or sensor["status"]
    for field in ("temperature", "humidity", "latitude", "longitude"):
        if result.get(field) is None:
            result[field] = sensor[field]
    result["block_hash"] = result.get("block_hash") or sensor["block_hash"]
    result["field_hash"] = result.get("field_hash") or sensor["field_hash"]
    result["fabric_tx_id"] = (
        result.get("fabric_tx_id")
        or result.get("blockchain_tx_id")
        or sensor["fabric_tx_id"]
    )
    return result


def fetch_latest_batch_rows():
    return fetch_record_rows(
        where_clause=(
            "sd.id IN ("
            "SELECT MAX(id) FROM sensor_data "
            "GROUP BY COALESCE(NULLIF(batch_id, ''), NULLIF(product_id, ''), CAST(product_ref AS TEXT), "
            "printf('LEGACY_BATCH_%03d', id))"
            ")"
        )
    )


def fetch_latest_uid_rows():
    return fetch_record_rows(
        where_clause=(
            "sd.id IN ("
            "SELECT MAX(id) FROM sensor_data "
            "GROUP BY COALESCE(CAST(product_ref AS TEXT), NULLIF(product_uid, ''), NULLIF(batch_id, ''), "
            "NULLIF(product_id, ''), printf('LEGACY_UID_%03d', id))"
            ")"
        )
    )


def fetch_latest_alert_rows():
    latest_rows = [row_to_dict(row) for row in fetch_latest_uid_rows()]
    latest_rows = [row for row in latest_rows if row["alert_count"]]
    latest_rows.sort(key=lambda row: (RISK_PRIORITY[row["risk_level"]], row["id"]), reverse=True)
    return latest_rows


def fetch_batch_history(batch_id):
    return fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id ASC",
    )


def fetch_uid_history(product_uid):
    return fetch_record_rows(
        where_clause=f"{UID_KEY_EXPR} = ?",
        params=(product_uid,),
        order_by="sd.id ASC",
    )


# ── Data insertion ───────────────────────────────────────────────────────────

def upsert_product_registry(cursor, product_uid, batch_id, product, product_name, created_at):
    cursor.execute(
        """
        SELECT id
        FROM product_registry
        WHERE product_uid = ?
        """,
        (product_uid,),
    )
    row = cursor.fetchone()
    if row:
        cursor.execute(
            """
            UPDATE product_registry
            SET batch_id = COALESCE(NULLIF(?, ''), batch_id),
                product = COALESCE(NULLIF(?, ''), product),
                product_name = COALESCE(NULLIF(?, ''), product_name),
                created_at = COALESCE(created_at, ?)
            WHERE id = ?
            """,
            (batch_id, product, product_name, created_at, row["id"]),
        )
        return row["id"]

    cursor.execute(
        """
        INSERT INTO product_registry (product_uid, batch_id, product, product_name, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (product_uid, batch_id, product, product_name, created_at),
    )
    return cursor.lastrowid


def insert_sensor_data(data):
    record = build_record(data)

    # Compute blockchain hash chain (links this record to previous)
    prev_hash  = get_last_hash(record["batch_id"])
    block_hash = compute_block_hash(record, prev_hash)

    # Compute per-record field hash (tamper-detection fingerprint)
    field_hash = compute_record_hash(record)

    # Insert record immediately (fabric_tx_id = None until async submission completes)
    from services.fabric_client import FABRIC_AVAILABLE, submit_to_fabric_async

    with get_connection() as conn:
        cursor = conn.cursor()
        registry_id = upsert_product_registry(
            cursor,
            record["product_uid"],
            record["batch_id"],
            record["product"],
            record["product_name"],
            record["timestamp"],
        )
        cursor.execute(
            """
            INSERT INTO sensor_data (
                timestamp,
                temperature,
                humidity,
                latitude,
                longitude,
                gas_value,
                product_id,
                status,
                transportation_status,
                alert_status,
                telemetry_mode,
                origin_name,
                destination_name,
                replay_record_id,
                product_name,
                batch_id,
                product_uid,
                product,
                sensor_id,
                current_stage,
                product_ref,
                block_hash,
                field_hash,
                fabric_tx_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["timestamp"],
                record["temperature"],
                record["humidity"],
                record["latitude"],
                record["longitude"],
                record["gas_value"],
                record["product_id"],
                record["status"],
                record["transportation_status"],
                record["alert_status"],
                record["telemetry_mode"],
                record["origin_name"],
                record["destination_name"],
                record["replay_record_id"],
                record["product_name"],
                record["batch_id"],
                record["product_uid"],
                record["product"],
                record["sensor_id"],
                record["current_stage"],
                registry_id,
                block_hash,
                field_hash,
                None,   # fabric_tx_id starts NULL; async callback fills it in
            ),
        )
        row_id = cursor.lastrowid
        conn.commit()

    # Async Fabric submission — does NOT block MQTT/replay ingestion
    if FABRIC_AVAILABLE and should_submit_fabric_event(record):
        def _update_fabric_tx(tx_id):
            if tx_id:
                try:
                    with get_connection() as upd_conn:
                        upd_conn.execute(
                            "UPDATE sensor_data SET fabric_tx_id = ? WHERE id = ?",
                            (tx_id, row_id),
                        )
                        upd_conn.commit()
                except Exception as e:
                    import logging as _log
                    _log.getLogger(__name__).warning(f"DB fabric_tx_id update failed: {e}")

        submit_to_fabric_async(record["batch_id"], record, callback=_update_fabric_tx)


def should_submit_fabric_event(record):
    """Decide whether to submit this record to Hyperledger Fabric.
    
    Policy: submit ALL records when Fabric is online.
    We previously only submitted alerts/delivered to reduce load,
    but this left most fabric_tx_id fields as NULL in the DB.
    With async submission this no longer blocks the MQTT path.
    """
    return True  # Submit every sensor record to Fabric when online



def clear_demo_transportation_received(batch_id="FC-001"):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            DELETE FROM sensor_data
            WHERE batch_id = ? AND COALESCE(telemetry_mode, '') = 'Demo Telemetry / Replay Mode'
            """,
            (batch_id,),
        )
        conn.commit()


def fetch_replay_records(batch_id="FC-001"):
    return fetch_rows(
        """
        SELECT *
        FROM replay_telemetry
        WHERE batch_id = ?
        ORDER BY record_id ASC
        """,
        (batch_id,),
    )


DEMO_REPLAY_BATCHES = [
    {
        "batch_id": "FC-001",
        "device_id": "ESP32-01",
        "product": "Refrigerated Produce Batch",
        "product_uid": "UID-FC-001-DEMO",
        "origin": "Bengaluru Cold Storage Facility",
        "destination": "Mysuru Distribution Center",
        "start_time": datetime(2026, 8, 11, 9, 0, 0),
        "route": [
            (12.971599, 77.594566),
            (12.917800, 77.520900),
            (12.828400, 77.417600),
            (12.723900, 77.280900),
            (12.640200, 77.074300),
            (12.521800, 76.895100),
            (12.421200, 76.704700),
            (12.348500, 76.656500),
            (12.295810, 76.639381),
        ],
    },
    {
        "batch_id": "FC-002",
        "device_id": "ESP32-02",
        "product": "Cold Chain Dairy Batch",
        "product_uid": "UID-FC-002-DEMO",
        "origin": "Bengaluru Processing Facility",
        "destination": "Mandya Warehouse",
        "start_time": datetime(2026, 8, 11, 9, 20, 0),
        "route": [
            (12.935200, 77.624500),
            (12.888000, 77.551000),
            (12.799500, 77.436200),
            (12.713700, 77.283800),
            (12.646800, 77.115400),
            (12.574200, 76.983900),
            (12.521600, 76.895800),
        ],
    },
    {
        "batch_id": "FC-003",
        "device_id": "ESP32-03",
        "product": "Retail Distribution Produce Batch",
        "product_uid": "UID-FC-003-DEMO",
        "origin": "Bengaluru Warehouse",
        "destination": "Hassan Retail Distribution Hub",
        "start_time": datetime(2026, 8, 11, 9, 40, 0),
        "route": [
            (13.020600, 77.647900),
            (13.006800, 77.569100),
            (13.001300, 77.484900),
            (13.004400, 77.326600),
            (13.006900, 77.103900),
            (12.959100, 76.820500),
            (12.944700, 76.617300),
            (13.003300, 76.102800),
        ],
    },
]


def get_replay_batch_config(batch_id="FC-001"):
    """Legacy wrapper — kept for backward compatibility.
    Tries real DB first, then falls back to DEMO_REPLAY_BATCHES."""
    real = get_real_batch_meta(batch_id)
    if real:
        return real
    for batch in DEMO_REPLAY_BATCHES:
        if batch["batch_id"] == batch_id:
            return batch
    return DEMO_REPLAY_BATCHES[0]


def get_replay_batch_options():
    """Legacy wrapper — returns real batches first, demo as fallback."""
    real = get_active_batch_options()
    if real:
        return real
    return [
        {
            "batch_id": batch["batch_id"],
            "device_id": f'{batch["device_id"]} (Demo)',
            "product": batch["product"],
            "origin": batch["origin"],
            "destination": batch["destination"],
        }
        for batch in DEMO_REPLAY_BATCHES
    ]


def get_real_batch_meta(batch_id):
    """Get metadata for a real batch from batches table + sensor_data."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            # Try batches table first
            cursor.execute(
                "SELECT batch_id, product_name, origin, destination FROM batches WHERE batch_id = ?",
                (batch_id,),
            )
            row = cursor.fetchone()
            if row:
                # Also get device_id from latest sensor_data
                cursor.execute(
                    "SELECT sensor_id FROM sensor_data WHERE batch_id = ? ORDER BY id DESC LIMIT 1",
                    (batch_id,),
                )
                sensor_row = cursor.fetchone()
                return {
                    "batch_id": row["batch_id"],
                    "device_id": sensor_row["sensor_id"] if sensor_row else "ESP32-01",
                    "product": row["product_name"] or "Food Batch",
                    "origin": row["origin"] or "Origin",
                    "destination": row["destination"] or "Destination",
                }
            # Try sensor_data directly
            cursor.execute(
                """SELECT batch_id, sensor_id,
                          COALESCE(product_name, product) AS product,
                          origin_name, destination_name
                   FROM sensor_data WHERE batch_id = ? ORDER BY id DESC LIMIT 1""",
                (batch_id,),
            )
            srow = cursor.fetchone()
            if srow:
                return {
                    "batch_id": srow["batch_id"],
                    "device_id": srow["sensor_id"] or "ESP32-01",
                    "product": srow["product"] or "Food Batch",
                    "origin": srow["origin_name"] or "Origin",
                    "destination": srow["destination_name"] or "Destination",
                }
    except Exception:
        pass
    return None


def get_active_batch_options():
    """Get all real batches with sensor data, ordered by most recent activity."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    sd.batch_id,
                    COALESCE(b.product_name, sd.product_name, sd.product, 'Food Batch') AS product,
                    COALESCE(sd.sensor_id, 'ESP32-01') AS device_id,
                    COALESCE(b.origin, sd.origin_name, 'Origin') AS origin,
                    COALESCE(b.destination, sd.destination_name, 'Destination') AS destination,
                    MAX(sd.id) AS latest_id
                FROM sensor_data sd
                LEFT JOIN batches b ON b.batch_id = sd.batch_id
                WHERE sd.batch_id IS NOT NULL AND sd.batch_id != ''
                GROUP BY sd.batch_id
                ORDER BY latest_id DESC
                LIMIT 20
                """
            )
            rows = cursor.fetchall()
            return [
                {
                    "batch_id": r["batch_id"],
                    "device_id": r["device_id"],
                    "product": r["product"],
                    "origin": r["origin"],
                    "destination": r["destination"],
                }
                for r in rows
            ]
    except Exception:
        return []


def get_latest_active_batch():
    """Return the batch_id of the most recently active batch (latest sensor data)."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT batch_id FROM sensor_data
                WHERE batch_id IS NOT NULL AND batch_id != ''
                ORDER BY id DESC LIMIT 1
                """
            )
            row = cursor.fetchone()
            return row["batch_id"] if row else None
    except Exception:
        return None


def replay_row_to_payload(row):
    return {
        "record_id": row["record_id"],
        "device_id": row["device_id"],
        "sensor_id": row["device_id"],
        "batch_id": row["batch_id"],
        "product_uid": row["product_uid"],
        "product": row["product"],
        "product_name": row["product"],
        "product_id": row["batch_id"],
        "timestamp": row["timestamp"],
        "temperature": row["temperature"],
        "humidity": row["humidity"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "location": {"lat": row["latitude"], "lng": row["longitude"]},
        "gas_value": row["gas_value"],
        "current_stage": "consumer" if row["transportation_status"] == "DELIVERED" else "transport",
        "status": "Delivered" if row["transportation_status"] == "DELIVERED" else "In Transit",
        "transportation_status": row["transportation_status"],
        "alert_status": row["alert_status"],
        "telemetry_mode": "Demo Telemetry / Replay Mode",
        "origin_name": row["origin_name"],
        "destination_name": row["destination_name"],
        "alert_flag": row["alert_flag"] if "alert_flag" in row.keys() else (1 if row["alert_status"] != "NORMAL" else 0),
        "alert_source": row["alert_source"] if "alert_source" in row.keys() else "sensor",
        "source": row["alert_source"] if "alert_source" in row.keys() else "sensor",
    }


def seed_demo_replay_dataset(cursor):
    expected = len(DEMO_REPLAY_BATCHES) * 100
    cursor.execute("SELECT COUNT(*) AS count FROM replay_telemetry")
    if cursor.fetchone()["count"] == expected:
        return

    cursor.execute("DELETE FROM replay_telemetry")
    for batch in DEMO_REPLAY_BATCHES:
        for record in generate_demo_transportation_records(batch):
            cursor.execute(
                """
                INSERT INTO replay_telemetry (
                    record_id, device_id, batch_id, product_uid, product, timestamp,
                    temperature, humidity, latitude, longitude, gas_value,
                    transportation_status, alert_status, origin_name, destination_name,
                    telemetry_mode, alert_flag, alert_source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["record_id"],
                    record["device_id"],
                    record["batch_id"],
                    record["product_uid"],
                    record["product"],
                    record["timestamp"],
                    record["temperature"],
                    record["humidity"],
                    record["latitude"],
                    record["longitude"],
                    record["gas_value"],
                    record["transportation_status"],
                    record["alert_status"],
                    record["origin_name"],
                    record["destination_name"],
                    record["telemetry_mode"],
                    record["alert_flag"],
                    record["alert_source"],
                ),
            )


def generate_demo_transportation_records(batch):
    from datetime import timedelta

    route = batch["route"]

    points = []
    segments = len(route) - 1
    for index in range(100):
        position = (index / 99) * segments
        segment = min(int(position), segments - 1)
        fraction = position - segment
        lat1, lng1 = route[segment]
        lat2, lng2 = route[segment + 1]
        points.append((lat1 + (lat2 - lat1) * fraction, lng1 + (lng2 - lng1) * fraction))

    records = []
    for index, (lat, lng) in enumerate(points, start=1):
        if 45 <= index <= 53:
            temperature = 11.6 + ((index - 45) * 0.18)
            alert_status = "TEMPERATURE_EXCURSION"
            alert_flag = 1
            alert_source = "Aero"
        elif 54 <= index <= 58:
            temperature = 9.8 - ((index - 54) * 0.55)
            alert_status = "RECOVERING"
            alert_flag = 0
            alert_source = "Data-Tron"
        else:
            temperature = 6.4 + ((index % 9) * 0.12)
            alert_status = "NORMAL"
            alert_flag = 0
            alert_source = "sensor"

        gas_value = 118 + ((index * 7) % 22)
        if 68 <= index <= 70:
            gas_value = 190 + ((index - 68) * 6)
            alert_status = "ENVIRONMENT_ALERT"
            alert_flag = 1
            alert_source = "Orion"

        records.append({
            "record_id": index,
            "device_id": batch["device_id"],
            "batch_id": batch["batch_id"],
            "product_uid": batch["product_uid"],
            "product": batch["product"],
            "timestamp": (batch["start_time"] + timedelta(minutes=index - 1)).strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": round(temperature, 2),
            "humidity": round(66.5 + ((index * 5) % 17) * 0.45, 2),
            "latitude": round(lat, 6),
            "longitude": round(lng, 6),
            "gas_value": round(gas_value, 2),
            "transportation_status": "DELIVERED" if index == 100 else "IN TRANSIT",
            "alert_status": alert_status,
            "origin_name": batch["origin"],
            "destination_name": batch["destination"],
            "telemetry_mode": "Demo Telemetry / Replay Mode",
            "alert_flag": alert_flag,
            "alert_source": alert_source,
        })
    return records


# ── Migration ────────────────────────────────────────────────────────────────


def migrate_legacy_rows(cursor):
    sensor_columns = get_table_columns(cursor, "sensor_data")
    uid_expr = (
        "COALESCE(NULLIF(product_uid, ''), NULLIF(batch_id, ''), NULLIF(product_id, ''), "
        "printf('UID_%03d', id))"
        if "product_uid" in sensor_columns
        else "COALESCE(NULLIF(batch_id, ''), NULLIF(product_id, ''), printf('UID_%03d', id))"
    )

    cursor.execute(
        """
        UPDATE sensor_data
        SET batch_id = COALESCE(NULLIF(batch_id, ''), NULLIF(product_id, ''), printf('LEGACY_BATCH_%03d', id))
        WHERE batch_id IS NULL OR TRIM(batch_id) = ''
        """
    )
    cursor.execute(
        """
        UPDATE sensor_data
        SET product = COALESCE(NULLIF(product, ''), NULLIF(product_name, ''), printf('Product %s', batch_id))
        WHERE product IS NULL OR TRIM(product) = ''
        """
    )
    cursor.execute(
        """
        UPDATE sensor_data
        SET product_name = COALESCE(NULLIF(product_name, ''), product)
        WHERE product_name IS NULL OR TRIM(product_name) = ''
        """
    )
    cursor.execute(
        """
        UPDATE sensor_data
        SET sensor_id = COALESCE(NULLIF(sensor_id, ''), 'LEGACY_SENSOR')
        WHERE sensor_id IS NULL OR TRIM(sensor_id) = ''
        """
    )
    cursor.execute(
        """
        UPDATE sensor_data
        SET current_stage = CASE
            WHEN LOWER(COALESCE(current_stage, '')) IN ('field', 'warehouse', 'transport', 'retailer', 'consumer')
                THEN LOWER(current_stage)
            WHEN LOWER(COALESCE(status, '')) = 'delivered'
                THEN 'consumer'
            ELSE 'transport'
        END
        WHERE current_stage IS NULL OR TRIM(current_stage) = ''
        """
    )

    cursor.execute(
        f"""
        SELECT
            id,
            COALESCE(timestamp, ?) AS created_at,
            COALESCE(NULLIF(batch_id, ''), NULLIF(product_id, ''), printf('LEGACY_BATCH_%03d', id)) AS batch_id,
            {uid_expr} AS product_uid,
            COALESCE(NULLIF(product, ''), NULLIF(product_name, ''), printf('Product %s', COALESCE(NULLIF(batch_id, ''), NULLIF(product_id, ''), printf('LEGACY_BATCH_%03d', id)))) AS product,
            COALESCE(NULLIF(product_name, ''), NULLIF(product, ''), printf('Product %s', COALESCE(NULLIF(batch_id, ''), NULLIF(product_id, ''), printf('LEGACY_BATCH_%03d', id)))) AS product_name
        FROM sensor_data
        WHERE product_ref IS NULL
        ORDER BY id ASC
        """,
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
    )
    rows = cursor.fetchall()

    for row in rows:
        registry_id = upsert_product_registry(
            cursor,
            row["product_uid"],
            row["batch_id"],
            row["product"],
            row["product_name"],
            row["created_at"],
        )
        if "product_uid" in sensor_columns:
            cursor.execute(
                """
                UPDATE sensor_data
                SET product_ref = ?, batch_id = ?, product = ?, product_name = ?, product_uid = ?
                WHERE id = ?
                """,
                (registry_id, row["batch_id"], row["product"], row["product_name"], row["product_uid"], row["id"]),
            )
        else:
            cursor.execute(
                """
                UPDATE sensor_data
                SET product_ref = ?, batch_id = ?, product = ?, product_name = ?
                WHERE id = ?
                """,
                (registry_id, row["batch_id"], row["product"], row["product_name"], row["id"]),
            )

    # Populate any missing product_uid in sensor_data from product_registry
    cursor.execute(
        """
        UPDATE sensor_data
        SET product_uid = (
            SELECT pr.product_uid
            FROM product_registry pr
            WHERE pr.id = sensor_data.product_ref
        )
        WHERE (product_uid IS NULL OR TRIM(product_uid) = '') AND product_ref IS NOT NULL
        """
    )


# ── Seed default users ──────────────────────────────────────────────────────

def seed_default_users():
    """Insert default users if they don't exist."""
    default_users = [
        ("admin",       "admin123",       "admin"),
        ("producer",    "producer123",    "producer"),
        ("distributor", "distributor123", "distributor"),
        ("consumer",    "consumer123",    "consumer"),
        # Legacy aliases kept for backward compatibility
        ("farmer",      "farmer123",      "producer"),
        ("retailer",    "retail123",      "distributor"),
        ("distributer", "distributer123", "distributor"),
    ]
    with get_connection() as conn:
        cursor = conn.cursor()
        for username, password, role in default_users:
            cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
            if cursor.fetchone() is None:
                password_hash = pwd_context.hash(password)
                cursor.execute(
                    "INSERT INTO users (username, password_hash, role, is_active) VALUES (?, ?, ?, 1)",
                    (username, password_hash, role),
                )
        conn.commit()


# ── Database initialization ──────────────────────────────────────────────────

def init_db():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                temperature REAL,
                humidity REAL,
                latitude REAL,
                longitude REAL,
                gas_value REAL,
                product_id TEXT,
                status TEXT,
                transportation_status TEXT,
                alert_status TEXT,
                telemetry_mode TEXT,
                origin_name TEXT,
                destination_name TEXT,
                replay_record_id INTEGER,
                product_name TEXT,
                batch_id TEXT,
                product_uid TEXT,
                product TEXT,
                sensor_id TEXT,
                current_stage TEXT,
                product_ref INTEGER,
                block_hash TEXT
            )
            """
        )
        cursor.execute(
            "DROP TABLE IF EXISTS replay_telemetry"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS replay_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_id INTEGER NOT NULL,
                device_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                product_uid TEXT NOT NULL,
                product TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                temperature REAL NOT NULL,
                humidity REAL NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                gas_value REAL,
                transportation_status TEXT NOT NULL,
                alert_status TEXT NOT NULL,
                origin_name TEXT NOT NULL,
                destination_name TEXT NOT NULL,
                telemetry_mode TEXT NOT NULL,
                alert_flag INTEGER DEFAULT 0,
                alert_source TEXT DEFAULT 'sensor'
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS product_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_uid TEXT UNIQUE NOT NULL,
                batch_id TEXT,
                product TEXT,
                product_name TEXT,
                created_at TEXT
            )
            """
        )
        cursor.execute(
            "DROP TABLE IF EXISTS users"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN (
                    'admin','producer','distributor','consumer',
                    'farmer','warehouse','retailer','distributer'
                )),
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # ── New RBAC tables ──────────────────────────────────────────────────

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT UNIQUE NOT NULL,
                product_name TEXT NOT NULL,
                product_type TEXT,
                origin TEXT,
                destination TEXT,
                quantity TEXT,
                description TEXT,
                harvest_date TEXT,
                status TEXT NOT NULL DEFAULT 'created',
                created_by TEXT NOT NULL,
                created_by_id INTEGER,
                blockchain_tx_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by_id) REFERENCES users(id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS batch_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN (
                    'received','transferred','checkpoint','anomaly'
                )),
                location_name TEXT,
                latitude REAL,
                longitude REAL,
                temperature REAL,
                humidity REAL,
                notes TEXT,
                anomaly_description TEXT,
                created_by TEXT NOT NULL,
                created_by_id INTEGER,
                blockchain_tx_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by_id) REFERENCES users(id)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                result TEXT NOT NULL,
                username TEXT,
                role TEXT,
                batch_id TEXT,
                detail TEXT,
                ip_address TEXT,
                request_id TEXT
            )
            """
        )

        # Indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sensor_batch ON sensor_data(batch_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sensor_product_ref ON sensor_data(product_ref)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON sensor_data(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_registry_uid ON product_registry(product_uid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_replay_batch ON replay_telemetry(batch_id, record_id)")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_batch_record ON replay_telemetry(batch_id, record_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_batches_batch_id ON batches(batch_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_batches_created_by ON batches(created_by)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transfers_batch_id ON batch_transfers(batch_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_ts ON audit_logs(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_event ON audit_logs(event_type)")

        ensure_column(cursor, "sensor_data", "product_name", "TEXT")
        ensure_column(cursor, "sensor_data", "batch_id", "TEXT")
        ensure_column(cursor, "sensor_data", "product_uid", "TEXT")
        ensure_column(cursor, "sensor_data", "product", "TEXT")
        ensure_column(cursor, "sensor_data", "sensor_id", "TEXT")
        ensure_column(cursor, "sensor_data", "current_stage", "TEXT")
        ensure_column(cursor, "sensor_data", "product_ref", "INTEGER")
        ensure_column(cursor, "sensor_data", "block_hash", "TEXT")
        ensure_column(cursor, "sensor_data", "fabric_tx_id", "TEXT")
        ensure_column(cursor, "sensor_data", "field_hash", "TEXT")
        ensure_column(cursor, "sensor_data", "gas_value", "REAL")
        ensure_column(cursor, "sensor_data", "transportation_status", "TEXT")
        ensure_column(cursor, "sensor_data", "alert_status", "TEXT")
        ensure_column(cursor, "sensor_data", "telemetry_mode", "TEXT")
        ensure_column(cursor, "sensor_data", "origin_name", "TEXT")
        ensure_column(cursor, "sensor_data", "destination_name", "TEXT")
        ensure_column(cursor, "sensor_data", "replay_record_id", "INTEGER")
        ensure_column(cursor, "sensor_data", "alert_flag", "INTEGER DEFAULT 0")
        ensure_column(cursor, "sensor_data", "alert_source", "TEXT DEFAULT 'sensor'")
        ensure_column(cursor, "users", "is_active", "INTEGER NOT NULL DEFAULT 1")

        migrate_legacy_rows(cursor)
        # seed_demo_replay_dataset(cursor)  -- Disabled for real ESP32 IoT hardware
        conn.commit()

    seed_default_users()
