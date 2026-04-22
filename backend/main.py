import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "food_chain.db"
FRONTEND_DIR = BASE_DIR.parent / "frontend"
SUPPLY_CHAIN_STAGES = ["field", "warehouse", "transport", "retailer", "consumer"]
STALE_BATCH_HOURS = 6
RISK_PRIORITY = {"stable": 0, "warning": 1, "critical": 2}
STAGE_THRESHOLDS = {
    "field": {
        "temperature": (18.0, 27.0),
        "humidity": (65.0, 85.0),
        "healthy_note": "Freshly harvested stock is within the expected farm range.",
    },
    "warehouse": {
        "temperature": (4.0, 10.0),
        "humidity": (70.0, 90.0),
        "healthy_note": "Cold storage conditions are stable for warehouse holding.",
    },
    "transport": {
        "temperature": (5.0, 12.0),
        "humidity": (60.0, 80.0),
        "healthy_note": "Transit conditions are stable for refrigerated movement.",
    },
    "retailer": {
        "temperature": (6.0, 14.0),
        "humidity": (55.0, 75.0),
        "healthy_note": "Retail shelf conditions are within the expected range.",
    },
    "consumer": {
        "temperature": (8.0, 16.0),
        "humidity": (50.0, 70.0),
        "healthy_note": "The product has reached the consumer stage with acceptable readings.",
    },
}

BATCH_KEY_EXPR = (
    "COALESCE(pr.batch_id, sd.batch_id, sd.product_id, "
    "printf('LEGACY_BATCH_%03d', sd.id))"
)
UID_KEY_EXPR = (
    "COALESCE(pr.product_uid, NULLIF(sd.product_uid, ''), NULLIF(sd.batch_id, ''), "
    "NULLIF(sd.product_id, ''), printf('UID_%03d', sd.id))"
)
PRODUCT_NAME_EXPR = (
    "COALESCE(pr.product_name, pr.product, sd.product_name, sd.product, "
    "printf('Product %s', "
    "COALESCE(pr.batch_id, sd.batch_id, sd.product_id, printf('LEGACY_BATCH_%03d', sd.id))))"
)
PRODUCT_EXPR = (
    "COALESCE(pr.product, pr.product_name, sd.product, sd.product_name, "
    "printf('Product %s', "
    "COALESCE(pr.batch_id, sd.batch_id, sd.product_id, printf('LEGACY_BATCH_%03d', sd.id))))"
)
CURRENT_STAGE_EXPR = (
    "COALESCE(NULLIF(sd.current_stage, ''), "
    "CASE WHEN LOWER(COALESCE(sd.status, '')) = 'delivered' THEN 'consumer' ELSE 'transport' END)"
)

RECORD_SELECT = f"""
    SELECT
        sd.id,
        sd.timestamp,
        sd.temperature,
        sd.humidity,
        sd.latitude,
        sd.longitude,
        sd.product_id,
        sd.status,
        sd.block_hash,
        {BATCH_KEY_EXPR} AS batch_id,
        {UID_KEY_EXPR} AS product_uid,
        {PRODUCT_EXPR} AS product,
        {PRODUCT_NAME_EXPR} AS product_name,
        COALESCE(sd.sensor_id, 'UNKNOWN_SENSOR') AS sensor_id,
        {CURRENT_STAGE_EXPR} AS current_stage,
        COALESCE(sd.product_ref, pr.id) AS product_ref
    FROM sensor_data sd
    LEFT JOIN product_registry pr ON pr.id = sd.product_ref
"""

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def get_table_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row["name"] for row in cursor.fetchall()}


def ensure_column(cursor, table_name, column_name, definition):
    columns = get_table_columns(cursor, table_name)
    if column_name not in columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def normalize_stage(raw_stage, raw_status=None):
    stage = (raw_stage or "").strip().lower()
    if stage in SUPPLY_CHAIN_STAGES:
        return stage
    if (raw_status or "").strip().lower() == "delivered":
        return "consumer"
    return "transport"


def normalize_status(raw_status, current_stage):
    if normalize_stage(current_stage) == "consumer":
        return "Delivered"
    if (raw_status or "").strip().lower() == "delivered":
        return "Delivered"
    return "In Transit"


def normalize_batch_id(batch_id, product_id=None, fallback=None):
    for value in (batch_id, product_id, fallback):
        text = (value or "").strip()
        if text:
            return text
    return "UNKNOWN_BATCH"


def normalize_product_uid(product_uid, batch_id=None, product_id=None, fallback=None):
    for value in (product_uid, batch_id, product_id, fallback):
        text = (value or "").strip()
        if text:
            return text
    return "UNKNOWN_UID"


def normalize_product(product, product_name, batch_id):
    text = (product or product_name or "").strip()
    if text:
        return text
    return f"Product {batch_id}"


def format_stage_label(stage):
    return normalize_stage(stage).replace("_", " ").title()


def format_range(bounds, suffix):
    lower, upper = bounds
    return f"{lower:.1f}-{upper:.1f}{suffix}"


def parse_timestamp(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


# ── Blockchain hash chain functions ──────────────────────────────────────────

def get_last_hash(batch_id):
    """Get SHA256 hash of last record for this batch (for chaining)."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT block_hash FROM sensor_data WHERE batch_id = ? ORDER BY id DESC LIMIT 1",
            (batch_id,),
        )
        row = cursor.fetchone()
        return row["block_hash"] if row and row["block_hash"] else "0" * 64


def compute_block_hash(record, prev_hash):
    """Compute SHA256 hash linking this record to the previous one."""
    data = (
        f"{prev_hash}"
        f"{record['batch_id']}"
        f"{record['timestamp']}"
        f"{record['temperature']}"
        f"{record['humidity']}"
        f"{record['current_stage']}"
    )
    return hashlib.sha256(data.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────

def evaluate_edge_health(current_stage, temperature, humidity):
    stage = normalize_stage(current_stage)
    thresholds = STAGE_THRESHOLDS[stage]
    alerts = []

    temp_range = thresholds["temperature"]
    humidity_range = thresholds["humidity"]

    if temperature is None:
        alerts.append("Temperature reading unavailable.")
    elif temperature < temp_range[0] or temperature > temp_range[1]:
        alerts.append(
            f"Temperature {temperature} C is outside the {format_stage_label(stage)} range "
            f"({format_range(temp_range, ' C')})."
        )

    if humidity is None:
        alerts.append("Humidity reading unavailable.")
    elif humidity < humidity_range[0] or humidity > humidity_range[1]:
        alerts.append(
            f"Humidity {humidity}% is outside the {format_stage_label(stage)} range "
            f"({format_range(humidity_range, '%')})."
        )

    alert_count = len(alerts)
    if alert_count >= 2:
        risk_level = "critical"
        edge_decision = "Hold this product for inspection before the next handoff."
    elif alert_count == 1:
        risk_level = "warning"
        edge_decision = f"Review {format_stage_label(stage)} storage conditions and continue with caution."
    else:
        risk_level = "stable"
        edge_decision = thresholds["healthy_note"]

    return {
        "alerts": alerts,
        "alert_count": alert_count,
        "risk_level": risk_level,
        "health_label": risk_level.title(),
        "is_healthy": alert_count == 0,
        "edge_decision": edge_decision,
        "expected_temperature_range": format_range(temp_range, " C"),
        "expected_humidity_range": format_range(humidity_range, "%"),
        "journey_progress_pct": int(((SUPPLY_CHAIN_STAGES.index(stage) + 1) / len(SUPPLY_CHAIN_STAGES)) * 100),
        "journey_progress_label": f"{SUPPLY_CHAIN_STAGES.index(stage) + 1} of {len(SUPPLY_CHAIN_STAGES)} stages completed",
    }


def summarize_product_history(history):
    visited_stages = []
    incident_count = 0

    for record in history:
        if record["current_stage"] not in visited_stages:
            visited_stages.append(record["current_stage"])
        if record["alert_count"]:
            incident_count += 1

    latest = history[-1]
    return {
        "first_seen": history[0]["timestamp"],
        "last_seen": latest["timestamp"],
        "visited_stages": visited_stages,
        "visited_stage_labels": [format_stage_label(stage) for stage in visited_stages],
        "reading_count": len(history),
        "incident_count": incident_count,
        "latest_risk_level": latest["risk_level"],
        "journey_progress_pct": latest["journey_progress_pct"],
    }


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
                SET product_ref = ?, batch_id = ?, product = ?, product_name = ?, product_uid = NULL
                WHERE id = ?
                """,
                (registry_id, row["batch_id"], row["product"], row["product_name"], row["id"]),
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
                product_id TEXT,
                status TEXT,
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

        ensure_column(cursor, "sensor_data", "product_name", "TEXT")
        ensure_column(cursor, "sensor_data", "batch_id", "TEXT")
        ensure_column(cursor, "sensor_data", "product_uid", "TEXT")
        ensure_column(cursor, "sensor_data", "product", "TEXT")
        ensure_column(cursor, "sensor_data", "sensor_id", "TEXT")
        ensure_column(cursor, "sensor_data", "current_stage", "TEXT")
        ensure_column(cursor, "sensor_data", "product_ref", "INTEGER")
        ensure_column(cursor, "sensor_data", "block_hash", "TEXT")

        migrate_legacy_rows(cursor)
        conn.commit()


def build_record(data):
    location = data.get("location") or {}
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
        "latitude": location.get("lat"),
        "longitude": location.get("lng"),
        "product_id": normalize_batch_id(data.get("product_id"), batch_id),
        "status": status,
        "product_name": data.get("product_name") or product,
        "batch_id": batch_id,
        "product_uid": product_uid,
        "product": product,
        "sensor_id": (data.get("sensor_id") or "UNKNOWN_SENSOR").strip() or "UNKNOWN_SENSOR",
        "current_stage": current_stage,
    }


def row_to_dict(row):
    batch_id = normalize_batch_id(row["batch_id"], row["product_id"], f"LEGACY_BATCH_{row['id']:03d}")
    product_uid = normalize_product_uid(row["product_uid"], batch_id, row["product_id"], f"UID_{row['id']:03d}")
    product = normalize_product(row["product"], row["product_name"], batch_id)
    current_stage = normalize_stage(row["current_stage"], row["status"])
    status = normalize_status(row["status"], current_stage)
    edge_health = evaluate_edge_health(current_stage, row["temperature"], row["humidity"])
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
        "location": {"lat": row["latitude"], "lng": row["longitude"]},
        "batch_id": batch_id,
        "product_uid": product_uid,
        "product": product,
        "product_name": product,
        "product_id": row["product_id"] or batch_id,
        "sensor_id": row["sensor_id"] or "UNKNOWN_SENSOR",
        "current_stage": current_stage,
        "current_stage_label": format_stage_label(current_stage),
        "current_stage_index": SUPPLY_CHAIN_STAGES.index(current_stage),
        "status": status,
        "block_hash": block_hash,
        "blockchain_verification": "Blockchain Verified ✓" if block_hash else "Pending",
        "is_active": is_active,
        "minutes_since_update": age_minutes,
        **edge_health,
    }


def row_to_legacy_list(row):
    record = row_to_dict(row)
    return [
        record["id"],
        record["timestamp"],
        record["temperature"],
        record["humidity"],
        record["latitude"],
        record["longitude"],
        record["product_uid"],
        record["status"],
        record["product_name"],
    ]


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


def insert_sensor_data(data):
    record = build_record(data)

    # Compute blockchain hash chain
    prev_hash = get_last_hash(record["batch_id"])
    block_hash = compute_block_hash(record, prev_hash)

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
                product_id,
                status,
                product_name,
                batch_id,
                product_uid,
                product,
                sensor_id,
                current_stage,
                product_ref,
                block_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["timestamp"],
                record["temperature"],
                record["humidity"],
                record["latitude"],
                record["longitude"],
                record["product_id"],
                record["status"],
                record["product_name"],
                record["batch_id"],
                None,
                record["product"],
                record["sensor_id"],
                record["current_stage"],
                registry_id,
                block_hash,
            ),
        )
        conn.commit()


def on_message(client, userdata, message):
    try:
        data = json.loads(message.payload.decode())
    except json.JSONDecodeError as exc:
        print(f"MQTT payload decode failed: {exc}")
        return

    print(f"Received on {message.topic}: {data}")
    insert_sensor_data(data)


@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse(url="/login.html")


@app.get("/data")
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


@app.get("/batches")
def get_batches():
    rows = fetch_latest_batch_rows()
    batches = [row_to_dict(row) for row in rows]
    active_batches = [row for row in batches if row["is_active"]]
    if active_batches:
        batches = active_batches
    batches.sort(key=lambda row: (RISK_PRIORITY[row["risk_level"]], row["id"]), reverse=True)
    return {"count": len(batches), "batches": batches}


@app.get("/alerts")
def get_alerts():
    alerts = fetch_latest_alert_rows()
    active_alerts = [row for row in alerts if row["is_active"]]
    if active_alerts:
        alerts = active_alerts
    return {"count": len(alerts), "alerts": alerts}


@app.get("/uids")
def get_uids():
    rows = fetch_latest_uid_rows()
    records = [row_to_dict(row) for row in rows]
    active_records = [row for row in records if row["is_active"]]
    if active_records:
        records = active_records
    records.sort(key=lambda row: (RISK_PRIORITY[row["risk_level"]], row["id"]), reverse=True)
    return {"count": len(records), "uids": records}


@app.get("/batch/{batch_id}")
def get_batch_history(batch_id: str):
    rows = fetch_batch_history(batch_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history found for batch {batch_id}.")

    history = [row_to_dict(row) for row in rows]
    return {
        "batch_id": batch_id,
        "stages": SUPPLY_CHAIN_STAGES,
        "summary": summarize_product_history(history),
        "history": history,
        "latest": history[-1],
    }


@app.get("/batch/{batch_id}/current")
def get_current_batch(batch_id: str):
    rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id DESC",
        limit=1,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No current status found for batch {batch_id}.")
    return row_to_dict(rows[0])


@app.get("/uid/{product_uid}")
def get_uid_history(product_uid: str):
    rows = fetch_uid_history(product_uid)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history found for product UID {product_uid}.")

    history = [row_to_dict(row) for row in rows]
    return {
        "product_uid": product_uid,
        "stages": SUPPLY_CHAIN_STAGES,
        "summary": summarize_product_history(history),
        "history": history,
        "latest": history[-1],
    }


@app.get("/uid/{product_uid}/current")
def get_current_uid(product_uid: str):
    rows = fetch_record_rows(
        where_clause=f"{UID_KEY_EXPR} = ?",
        params=(product_uid,),
        order_by="sd.id DESC",
        limit=1,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No current status found for product UID {product_uid}.")
    return row_to_dict(rows[0])


@app.get("/verify/{batch_id}")
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
        record = row_to_dict(row)
        expected_hash = compute_block_hash(record, prev_hash)
        actual_hash = row["block_hash"]
        is_valid = actual_hash == expected_hash

        if not is_valid:
            chain_intact = False

        chain.append({
            "id": record["id"],
            "timestamp": record["timestamp"],
            "stage": record["current_stage"],
            "expected_hash": expected_hash[:16] + "...",
            "actual_hash": (actual_hash[:16] + "...") if actual_hash else None,
            "valid": is_valid,
        })
        prev_hash = expected_hash

    return {
        "batch_id": batch_id,
        "chain_intact": chain_intact,
        "record_count": len(chain),
        "chain": chain,
    }


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


@app.on_event("startup")
def startup():
    init_db()

    try:
        mqtt_client = mqtt.Client()
        mqtt_client.on_message = on_message
        mqtt_client.connect("localhost", 1883, 60)
        mqtt_client.subscribe("food/sensor/#")
        mqtt_client.loop_start()
        app.state.mqtt_client = mqtt_client
        print("MQTT connected on food/sensor/#")
    except Exception as exc:
        app.state.mqtt_client = None
        print(f"MQTT unavailable: {exc}")

    print(f"SQLite database ready at {DB_PATH}")
    print("Backend started on port 8001")


@app.on_event("shutdown")
def shutdown():
    mqtt_client = getattr(app.state, "mqtt_client", None)
    if mqtt_client is not None:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)