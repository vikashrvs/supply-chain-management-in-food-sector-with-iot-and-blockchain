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
        "field_hash": row["field_hash"] if "field_hash" in row.keys() and row["field_hash"] else None,
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

    # Try to submit to Hyperledger Fabric (returns txId or None)
    from services.fabric_client import submit_to_fabric, FABRIC_AVAILABLE
    fabric_tx_id = None
    if FABRIC_AVAILABLE:
        fabric_tx_id = submit_to_fabric(record["batch_id"], record)

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
                block_hash,
                field_hash,
                fabric_tx_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                record["product_uid"],
                record["product"],
                record["sensor_id"],
                record["current_stage"],
                registry_id,
                block_hash,
                field_hash,
                fabric_tx_id,   # Real Fabric txId when online, None when offline
            ),
        )
        conn.commit()


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
        ("admin", "admin123", "admin"),
        ("farmer", "farmer123", "farmer"),
        ("retailer", "retail123", "retailer"),
    ]
    with get_connection() as conn:
        cursor = conn.cursor()
        for username, password, role in default_users:
            cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
            if cursor.fetchone() is None:
                password_hash = pwd_context.hash(password)
                cursor.execute(
                    "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
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
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin','farmer','warehouse','retailer','consumer')),
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sensor_batch ON sensor_data(batch_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sensor_product_ref ON sensor_data(product_ref)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON sensor_data(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_registry_uid ON product_registry(product_uid)")

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

        migrate_legacy_rows(cursor)
        conn.commit()

    seed_default_users()
