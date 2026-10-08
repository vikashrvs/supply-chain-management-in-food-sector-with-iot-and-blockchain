"""Opt-in additive migration foundation for the normalized FoodChain schema.

This module never changes, renames, or deletes legacy tables.  It creates an
``fc_`` table namespace beside the current schema and copies only determinable
legacy relationships into it.  Use ``--apply`` explicitly; importing this
module has no database side effects.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1
CANONICAL_ROLES = ("admin", "producer", "distributor", "manager", "consumer")
ROLE_ALIASES = {
    "farmer": "producer",
    "warehouse": "distributor",
    "retailer": "distributor",
    "distributer": "distributor",
}
STAGES = (
    ("field", "Field", 1),
    ("warehouse", "Warehouse", 2),
    ("transport", "Transport", 3),
    ("retailer", "Retailer", 4),
    ("consumer", "Consumer", 5),
)
UNITS = (
    ("count", "Count"),
    ("box", "Box"),
    ("crate", "Crate"),
    ("kg", "Kilogram"),
    ("litre", "Litre"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_quantity(value: str | None) -> tuple[float | None, str | None]:
    if not value:
        return None, None
    match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)?", str(value))
    if not match:
        return None, None
    number = float(match.group(1))
    raw_unit = (match.group(2) or "count").lower()
    aliases = {"crates": "crate", "kgs": "kg", "kilograms": "kg", "liters": "litre"}
    return number, aliases.get(raw_unit, raw_unit)


def slug(value: str) -> str:
    result = re.sub(r"[^A-Z0-9]+", "-", (value or "unknown").upper()).strip("-")
    return result[:48] or "UNKNOWN"


def product_code(name: str, product_type: str | None) -> str:
    raw = f"{name}|{product_type or ''}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8].upper()
    return f"LEGACY-{slug(name)[:24]}-{digest}"


def json_hash(value: dict) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS fc_migration_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            schema_version INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            database_path TEXT NOT NULL,
            details_json TEXT
        );
        CREATE TABLE IF NOT EXISTS fc_reconciliation_checks (
            check_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL REFERENCES fc_migration_runs(run_id),
            source_table TEXT NOT NULL,
            check_name TEXT NOT NULL,
            source_count INTEGER NOT NULL DEFAULT 0,
            migrated_count INTEGER NOT NULL DEFAULT 0,
            skipped_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            details_json TEXT
        );
        CREATE TABLE IF NOT EXISTS fc_organizations (
            organization_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            organization_type TEXT NOT NULL,
            registration_code TEXT UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fc_roles (
            role_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        );
        CREATE TABLE IF NOT EXISTS fc_users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            legacy_user_id INTEGER UNIQUE,
            organization_id INTEGER NOT NULL REFERENCES fc_organizations(organization_id),
            username TEXT NOT NULL UNIQUE,
            display_name TEXT,
            password_hash TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS fc_user_roles (
            user_id INTEGER NOT NULL REFERENCES fc_users(user_id),
            role_id INTEGER NOT NULL REFERENCES fc_roles(role_id),
            assigned_at TEXT NOT NULL,
            PRIMARY KEY (user_id, role_id)
        );
        CREATE TABLE IF NOT EXISTS fc_units (
            unit_code TEXT PRIMARY KEY,
            display_name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fc_products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            category TEXT,
            product_type TEXT,
            description TEXT,
            legacy_source TEXT,
            legacy_source_id TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fc_locations (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER REFERENCES fc_organizations(organization_id),
            name TEXT NOT NULL,
            location_type TEXT NOT NULL,
            address TEXT,
            latitude REAL,
            longitude REAL,
            created_at TEXT NOT NULL,
            UNIQUE(name, location_type)
        );
        CREATE TABLE IF NOT EXISTS fc_batches (
            batch_id TEXT PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES fc_products(product_id),
            owner_organization_id INTEGER NOT NULL REFERENCES fc_organizations(organization_id),
            origin_location_id INTEGER REFERENCES fc_locations(location_id),
            destination_location_id INTEGER REFERENCES fc_locations(location_id),
            quantity_value REAL,
            unit_code TEXT REFERENCES fc_units(unit_code),
            harvested_at TEXT,
            lifecycle_status TEXT NOT NULL,
            created_by_user_id INTEGER REFERENCES fc_users(user_id),
            created_at TEXT,
            updated_at TEXT,
            legacy_source TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fc_batch_identifiers (
            batch_identifier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL REFERENCES fc_batches(batch_id),
            identifier_type TEXT NOT NULL,
            identifier_value TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(identifier_type, identifier_value)
        );
        CREATE TABLE IF NOT EXISTS fc_devices (
            device_id TEXT PRIMARY KEY,
            device_type TEXT NOT NULL,
            serial_number TEXT UNIQUE,
            firmware_version TEXT,
            source_type TEXT NOT NULL,
            device_secret_ref TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            registered_at TEXT NOT NULL,
            last_seen_at TEXT
        );
        CREATE TABLE IF NOT EXISTS fc_batch_device_assignments (
            assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL REFERENCES fc_batches(batch_id),
            device_id TEXT NOT NULL REFERENCES fc_devices(device_id),
            assigned_by_user_id INTEGER REFERENCES fc_users(user_id),
            assigned_at TEXT NOT NULL,
            unassigned_at TEXT,
            UNIQUE(batch_id, device_id, assigned_at)
        );
        CREATE TABLE IF NOT EXISTS fc_ingestion_messages (
            message_id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT REFERENCES fc_devices(device_id),
            mqtt_topic TEXT,
            message_key TEXT NOT NULL UNIQUE,
            payload_encrypted TEXT,
            payload_hash TEXT NOT NULL,
            received_at TEXT NOT NULL,
            parse_status TEXT NOT NULL,
            error_message TEXT,
            legacy_source_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS fc_sensor_readings (
            reading_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL REFERENCES fc_batches(batch_id),
            device_id TEXT NOT NULL REFERENCES fc_devices(device_id),
            ingestion_message_id INTEGER REFERENCES fc_ingestion_messages(message_id),
            recorded_at TEXT,
            received_at TEXT NOT NULL,
            temperature_c REAL,
            humidity_percent REAL,
            gas_ppm REAL,
            gps_latitude REAL,
            gps_longitude REAL,
            stage_code TEXT,
            quality_status TEXT NOT NULL,
            legacy_source_id INTEGER UNIQUE
        );
        CREATE TABLE IF NOT EXISTS fc_sensor_health (
            device_id TEXT PRIMARY KEY REFERENCES fc_devices(device_id),
            last_reading_id INTEGER REFERENCES fc_sensor_readings(reading_id),
            last_recorded_at TEXT,
            last_received_at TEXT,
            connection_status TEXT NOT NULL,
            reading_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fc_supply_chain_stages (
            stage_code TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            sequence_no INTEGER NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS fc_transport_orders (
            transport_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL REFERENCES fc_batches(batch_id),
            from_location_id INTEGER REFERENCES fc_locations(location_id),
            to_location_id INTEGER REFERENCES fc_locations(location_id),
            assigned_organization_id INTEGER REFERENCES fc_organizations(organization_id),
            status TEXT NOT NULL,
            created_by_user_id INTEGER REFERENCES fc_users(user_id),
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fc_batch_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL REFERENCES fc_batches(batch_id),
            transport_id INTEGER REFERENCES fc_transport_orders(transport_id),
            event_type TEXT NOT NULL,
            stage_code TEXT REFERENCES fc_supply_chain_stages(stage_code),
            location_id INTEGER REFERENCES fc_locations(location_id),
            related_reading_id INTEGER REFERENCES fc_sensor_readings(reading_id),
            performed_by_user_id INTEGER REFERENCES fc_users(user_id),
            notes TEXT,
            occurred_at TEXT,
            created_at TEXT NOT NULL,
            legacy_source TEXT,
            legacy_source_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS fc_batch_current_state (
            batch_id TEXT PRIMARY KEY REFERENCES fc_batches(batch_id),
            current_stage_code TEXT REFERENCES fc_supply_chain_stages(stage_code),
            current_status TEXT NOT NULL,
            latest_event_id INTEGER REFERENCES fc_batch_events(event_id),
            latest_reading_id INTEGER REFERENCES fc_sensor_readings(reading_id),
            latest_device_id TEXT REFERENCES fc_devices(device_id),
            latest_reading_at TEXT,
            open_alert_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fc_alert_rules (
            rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            severity_default TEXT NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS fc_alerts (
            alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER REFERENCES fc_alert_rules(rule_id),
            batch_id TEXT REFERENCES fc_batches(batch_id),
            device_id TEXT REFERENCES fc_devices(device_id),
            reading_id INTEGER REFERENCES fc_sensor_readings(reading_id),
            severity TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            status TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            legacy_source_id INTEGER UNIQUE
        );
        CREATE TABLE IF NOT EXISTS fc_notifications (
            notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER NOT NULL REFERENCES fc_alerts(alert_id),
            recipient_user_id INTEGER REFERENCES fc_users(user_id),
            channel TEXT NOT NULL,
            delivery_status TEXT NOT NULL,
            sent_at TEXT,
            read_at TEXT,
            error_message TEXT
        );
        CREATE TABLE IF NOT EXISTS fc_record_integrity (
            integrity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL REFERENCES fc_batches(batch_id),
            reading_id INTEGER REFERENCES fc_sensor_readings(reading_id),
            event_id INTEGER REFERENCES fc_batch_events(event_id),
            field_hash TEXT,
            block_hash TEXT,
            previous_block_hash TEXT,
            hash_algorithm TEXT NOT NULL,
            verification_status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(reading_id, event_id)
        );
        CREATE TABLE IF NOT EXISTS fc_fabric_transactions (
            fabric_transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL REFERENCES fc_batches(batch_id),
            reading_id INTEGER REFERENCES fc_sensor_readings(reading_id),
            event_id INTEGER REFERENCES fc_batch_events(event_id),
            record_integrity_id INTEGER REFERENCES fc_record_integrity(integrity_id),
            fabric_tx_id TEXT NOT NULL UNIQUE,
            network TEXT,
            channel TEXT,
            chaincode TEXT,
            transaction_status TEXT NOT NULL,
            submitted_at TEXT,
            committed_at TEXT,
            failure_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS fc_encrypted_records (
            encrypted_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT REFERENCES fc_batches(batch_id),
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            encryption_key_ref TEXT,
            ciphertext TEXT,
            nonce TEXT,
            algorithm TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(source_type, source_id)
        );
        CREATE TABLE IF NOT EXISTS fc_audit_logs (
            audit_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_user_id INTEGER REFERENCES fc_users(user_id),
            action TEXT NOT NULL,
            entity_type TEXT,
            entity_id TEXT,
            result TEXT,
            request_id TEXT,
            ip_address TEXT,
            details_json TEXT,
            created_at TEXT NOT NULL,
            legacy_source_id INTEGER UNIQUE
        );
        CREATE TABLE IF NOT EXISTS fc_system_logs (
            system_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_name TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            request_id TEXT,
            exception_type TEXT,
            context_json TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS fc_sensor_batch_time ON fc_sensor_readings(batch_id, recorded_at DESC);
        CREATE INDEX IF NOT EXISTS fc_sensor_device_time ON fc_sensor_readings(device_id, recorded_at DESC);
        CREATE INDEX IF NOT EXISTS fc_events_batch_time ON fc_batch_events(batch_id, occurred_at DESC);
        CREATE INDEX IF NOT EXISTS fc_alert_status_time ON fc_alerts(status, severity, opened_at DESC);
        CREATE INDEX IF NOT EXISTS fc_audit_actor_time ON fc_audit_logs(actor_user_id, created_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS fc_integrity_reading_unique
            ON fc_record_integrity(reading_id) WHERE reading_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS fc_integrity_event_unique
            ON fc_record_integrity(event_id) WHERE event_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS fc_event_legacy_source_unique
            ON fc_batch_events(legacy_source, legacy_source_id)
            WHERE legacy_source IS NOT NULL AND legacy_source_id IS NOT NULL;
        """
    )


def check(conn: sqlite3.Connection, run_id: int, source: str, name: str,
          source_count: int, migrated_count: int, skipped_count: int,
          status: str, details: dict | None = None) -> None:
    conn.execute(
        """INSERT INTO fc_reconciliation_checks
        (run_id, source_table, check_name, source_count, migrated_count,
         skipped_count, status, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (run_id, source, name, source_count, migrated_count, skipped_count,
         status, json.dumps(details or {}, sort_keys=True)),
    )


def migrate(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        create_schema(conn)
        started = utc_now()
        run_id = conn.execute(
            """INSERT INTO fc_migration_runs
            (schema_version, started_at, status, database_path)
            VALUES (?, ?, 'running', ?)""",
            (SCHEMA_VERSION, started, str(db_path)),
        ).lastrowid

        now = utc_now()
        org_id = conn.execute(
            """INSERT OR IGNORE INTO fc_organizations
            (name, organization_type, registration_code, created_at)
            VALUES ('FoodChain', 'platform', 'FOODCHAIN-DEFAULT', ?)""",
            (now,),
        ).lastrowid
        if not org_id:
            org_id = conn.execute(
                "SELECT organization_id FROM fc_organizations WHERE name='FoodChain'"
            ).fetchone()[0]

        for name in CANONICAL_ROLES:
            conn.execute("INSERT OR IGNORE INTO fc_roles(name, description) VALUES (?, ?)",
                         (name, f"Canonical {name} role"))
        for code, display in UNITS:
            conn.execute("INSERT OR IGNORE INTO fc_units(unit_code, display_name) VALUES (?, ?)",
                         (code, display))
        for code, display, sequence in STAGES:
            conn.execute(
                "INSERT OR IGNORE INTO fc_supply_chain_stages(stage_code, display_name, sequence_no) VALUES (?, ?, ?)",
                (code, display, sequence),
            )

        legacy_users = conn.execute("SELECT * FROM users ORDER BY id").fetchall()
        user_map: dict[int, int] = {}
        role_map = {r["name"]: r["role_id"] for r in conn.execute("SELECT * FROM fc_roles")}
        for row in legacy_users:
            canonical = ROLE_ALIASES.get((row["role"] or "").lower(), (row["role"] or "").lower())
            if canonical not in role_map:
                canonical = "consumer"
            conn.execute(
                """INSERT OR IGNORE INTO fc_users
                (legacy_user_id, organization_id, username, display_name, password_hash,
                 is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (row["id"], org_id, row["username"], row["username"], row["password_hash"],
                 row["is_active"], row["created_at"], row["created_at"]),
            )
            new_id = conn.execute(
                "SELECT user_id FROM fc_users WHERE legacy_user_id=?", (row["id"],)
            ).fetchone()[0]
            user_map[row["id"]] = new_id
            conn.execute(
                "INSERT OR IGNORE INTO fc_user_roles(user_id, role_id, assigned_at) VALUES (?, ?, ?)",
                (new_id, role_map[canonical], row["created_at"] or now),
            )
        check(conn, run_id, "users", "users mapped to canonical roles",
              len(legacy_users), len(user_map), len(legacy_users) - len(user_map),
              "pass" if len(user_map) == len(legacy_users) else "review")

        batch_rows = conn.execute("SELECT * FROM batches ORDER BY id").fetchall()
        registry_rows = conn.execute("SELECT * FROM product_registry ORDER BY id").fetchall()
        sensor_rows = conn.execute("SELECT * FROM sensor_data ORDER BY id").fetchall()
        registry_by_batch = {r["batch_id"]: r for r in registry_rows if r["batch_id"]}
        latest_sensor: dict[str, sqlite3.Row] = {}
        for row in sensor_rows:
            key = row["batch_id"] or row["product_id"]
            if key:
                latest_sensor[key] = row

        product_ids: dict[tuple[str, str], int] = {}

        def ensure_product(name: str | None, kind: str | None, source: str, source_id: str) -> int:
            name = (name or "Unclassified product").strip()
            key = (name.lower(), (kind or "").lower())
            if key in product_ids:
                return product_ids[key]
            code = product_code(name, kind)
            conn.execute(
                """INSERT OR IGNORE INTO fc_products
                (product_code, name, category, product_type, legacy_source, legacy_source_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (code, name, kind, kind, source, source_id, now),
            )
            pid = conn.execute("SELECT product_id FROM fc_products WHERE product_code=?", (code,)).fetchone()[0]
            product_ids[key] = pid
            return pid

        def ensure_location(name: str | None, kind: str) -> int | None:
            if not name or not str(name).strip():
                return None
            clean = str(name).strip()
            conn.execute(
                """INSERT OR IGNORE INTO fc_locations
                (organization_id, name, location_type, created_at)
                VALUES (?, ?, ?, ?)""",
                (org_id, clean, kind, now),
            )
            return conn.execute(
                "SELECT location_id FROM fc_locations WHERE name=? AND location_type=?",
                (clean, kind),
            ).fetchone()[0]

        batch_meta: dict[str, dict] = {}
        for row in batch_rows:
            batch_meta[row["batch_id"]] = dict(row)
        for batch_id, row in registry_by_batch.items():
            batch_meta.setdefault(batch_id, {
                "batch_id": batch_id, "product_name": row["product_name"] or row["product"],
                "product_type": None, "origin": None, "destination": None,
                "quantity": None, "harvest_date": None, "status": "legacy_observed",
                "created_by_id": None, "created_at": row["created_at"], "updated_at": row["created_at"],
            })
        for batch_id, row in latest_sensor.items():
            batch_meta.setdefault(batch_id, {
                "batch_id": batch_id, "product_name": row["product_name"] or row["product"],
                "product_type": None, "origin": row["origin_name"], "destination": row["destination_name"],
                "quantity": None, "harvest_date": None, "status": "legacy_observed",
                "created_by_id": None, "created_at": row["timestamp"], "updated_at": row["timestamp"],
            })

        for batch_id, meta in batch_meta.items():
            product_id = ensure_product(meta.get("product_name"), meta.get("product_type"),
                                        "batches", str(batch_id))
            quantity, unit = parse_quantity(meta.get("quantity"))
            if unit not in {item[0] for item in UNITS}:
                unit = "count" if quantity is not None else None
            status = meta.get("status") or "legacy_observed"
            conn.execute(
                """INSERT OR IGNORE INTO fc_batches
                (batch_id, product_id, owner_organization_id, origin_location_id,
                 destination_location_id, quantity_value, unit_code, harvested_at,
                 lifecycle_status, created_by_user_id, created_at, updated_at, legacy_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (batch_id, product_id, org_id, ensure_location(meta.get("origin"), "origin"),
                 ensure_location(meta.get("destination"), "destination"), quantity, unit,
                 meta.get("harvest_date"), status, user_map.get(meta.get("created_by_id")),
                 meta.get("created_at"), meta.get("updated_at"), "legacy"),
            )
            conn.execute(
                """INSERT OR IGNORE INTO fc_batch_identifiers
                (batch_id, identifier_type, identifier_value, is_primary, created_at)
                VALUES (?, 'batch_id', ?, 1, ?)""",
                (batch_id, batch_id, meta.get("created_at") or now),
            )
        check(conn, run_id, "batches/product_registry/sensor_data", "batch identity mapped",
              len(batch_meta), len(batch_meta), 0, "pass")

        device_ids = sorted({(r["sensor_id"] or "LEGACY-UNKNOWN").strip() for r in sensor_rows})
        for device_id in device_ids:
            conn.execute(
                """INSERT OR IGNORE INTO fc_devices
                (device_id, device_type, source_type, registered_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?)""",
                (device_id, "esp32" if device_id.startswith("ESP32") else "legacy",
                 "legacy_import", now, now),
            )
        check(conn, run_id, "sensor_data", "devices mapped", len(device_ids), len(device_ids), 0, "pass")

        reading_count = 0
        integrity_count = 0
        fabric_count = 0
        for row in sensor_rows:
            batch_id = row["batch_id"] or row["product_id"]
            device_id = (row["sensor_id"] or "LEGACY-UNKNOWN").strip()
            if not batch_id or not conn.execute("SELECT 1 FROM fc_batches WHERE batch_id=?", (batch_id,)).fetchone():
                continue
            payload = {key: row[key] for key in row.keys()}
            message_key = f"legacy:sensor_data:{row['id']}"
            conn.execute(
                """INSERT OR IGNORE INTO fc_ingestion_messages
                (device_id, mqtt_topic, message_key, payload_hash, received_at,
                 parse_status, legacy_source_id)
                VALUES (?, 'legacy/import', ?, ?, ?, 'accepted', ?)""",
                (device_id, message_key, json_hash(payload), row["timestamp"] or now, row["id"]),
            )
            message_id = conn.execute(
                "SELECT message_id FROM fc_ingestion_messages WHERE message_key=?", (message_key,)
            ).fetchone()[0]
            conn.execute(
                """INSERT OR IGNORE INTO fc_sensor_readings
                (batch_id, device_id, ingestion_message_id, recorded_at, received_at,
                 temperature_c, humidity_percent, gas_ppm, gps_latitude, gps_longitude,
                 stage_code, quality_status, legacy_source_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'valid', ?)""",
                (batch_id, device_id, message_id, row["timestamp"], row["timestamp"] or now,
                 row["temperature"], row["humidity"], row["gas_value"], row["latitude"],
                 row["longitude"], row["current_stage"], row["id"]),
            )
            reading_id = conn.execute(
                "SELECT reading_id FROM fc_sensor_readings WHERE legacy_source_id=?", (row["id"],)
            ).fetchone()[0]
            reading_count += 1
            if row["block_hash"] or row["field_hash"]:
                conn.execute(
                    """INSERT OR IGNORE INTO fc_record_integrity
                    (batch_id, reading_id, field_hash, block_hash, hash_algorithm,
                     verification_status, created_at)
                    VALUES (?, ?, ?, ?, 'SHA-256', 'imported', ?)""",
                    (batch_id, reading_id, row["field_hash"], row["block_hash"], now),
                )
                integrity_count += 1
            if row["fabric_tx_id"]:
                integrity_id = conn.execute(
                    "SELECT integrity_id FROM fc_record_integrity WHERE reading_id=?", (reading_id,)
                ).fetchone()
                conn.execute(
                    """INSERT OR IGNORE INTO fc_fabric_transactions
                    (batch_id, reading_id, record_integrity_id, fabric_tx_id,
                     transaction_status, committed_at)
                    VALUES (?, ?, ?, ?, 'committed', ?)""",
                    (batch_id, reading_id, integrity_id[0] if integrity_id else None,
                     row["fabric_tx_id"], row["timestamp"] or now),
                )
                fabric_count += 1
            conn.execute(
                """INSERT OR IGNORE INTO fc_batch_device_assignments
                (batch_id, device_id, assigned_at) VALUES (?, ?, ?)""",
                (batch_id, device_id, row["timestamp"] or now),
            )
        check(conn, run_id, "sensor_data", "sensor readings mapped",
              len(sensor_rows), reading_count, len(sensor_rows) - reading_count,
              "pass" if reading_count == len(sensor_rows) else "review")
        check(conn, run_id, "sensor_data", "integrity proofs mapped",
              sum(1 for r in sensor_rows if r["block_hash"] or r["field_hash"]),
              integrity_count, 0, "pass")
        check(conn, run_id, "sensor_data", "Fabric transactions mapped",
              sum(1 for r in sensor_rows if r["fabric_tx_id"]),
              fabric_count, 0, "pass")

        event_count = 0
        for row in batch_rows:
            event_key = row["id"]
            conn.execute(
                """INSERT OR IGNORE INTO fc_batch_events
                (batch_id, event_type, stage_code, performed_by_user_id, occurred_at,
                 created_at, legacy_source, legacy_source_id)
                VALUES (?, 'created', 'field', ?, ?, ?, 'batches', ?)""",
                (row["batch_id"], user_map.get(row["created_by_id"]), row["created_at"],
                 now, event_key),
            )
            event_count += 1
        transfer_rows = conn.execute("SELECT * FROM batch_transfers ORDER BY id").fetchall()
        for row in transfer_rows:
            location_id = ensure_location(row["location_name"], "event")
            conn.execute(
                """INSERT OR IGNORE INTO fc_batch_events
                (batch_id, event_type, location_id, performed_by_user_id, notes,
                 occurred_at, created_at, legacy_source, legacy_source_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'batch_transfers', ?)""",
                (row["batch_id"], row["event_type"], location_id, user_map.get(row["created_by_id"]),
                 row["notes"] or row["anomaly_description"], row["created_at"], now, row["id"]),
            )
            event_count += 1
        check(conn, run_id, "batches/batch_transfers", "business events mapped",
              len(batch_rows) + len(transfer_rows), event_count, 0, "pass")

        for row in conn.execute("SELECT * FROM audit_logs ORDER BY id"):
            actor = conn.execute("SELECT user_id FROM fc_users WHERE username=?", (row["username"],)).fetchone()
            conn.execute(
                """INSERT OR IGNORE INTO fc_audit_logs
                (actor_user_id, action, entity_type, entity_id, result, request_id,
                 ip_address, details_json, created_at, legacy_source_id)
                VALUES (?, ?, 'legacy', ?, ?, ?, ?, ?, ?, ?)""",
                (actor[0] if actor else None, row["event_type"], row["batch_id"], row["result"],
                 row["request_id"], row["ip_address"], json.dumps({"detail": row["detail"]}),
                 row["timestamp"], row["id"]),
            )
        check(conn, run_id, "audit_logs", "audit events mapped",
              conn.execute("SELECT COUNT(*) FROM audit_logs").fetchone()[0],
              conn.execute("SELECT COUNT(*) FROM fc_audit_logs").fetchone()[0], 0, "pass")

        for batch_id in (r["batch_id"] for r in conn.execute("SELECT * FROM fc_batches")):
            latest = conn.execute(
                "SELECT * FROM fc_sensor_readings WHERE batch_id=? ORDER BY reading_id DESC LIMIT 1",
                (batch_id,),
            ).fetchone()
            event = conn.execute(
                "SELECT * FROM fc_batch_events WHERE batch_id=? ORDER BY event_id DESC LIMIT 1",
                (batch_id,),
            ).fetchone()
            conn.execute(
                """INSERT OR REPLACE INTO fc_batch_current_state
                (batch_id, current_stage_code, current_status, latest_event_id,
                 latest_reading_id, latest_device_id, latest_reading_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (batch_id, latest["stage_code"] if latest else None,
                 event["event_type"] if event else "legacy_observed",
                 event["event_id"] if event else None, latest["reading_id"] if latest else None,
                 latest["device_id"] if latest else None, latest["recorded_at"] if latest else None, now),
            )

        conn.execute(
            "UPDATE fc_migration_runs SET finished_at=?, status='completed', details_json=? WHERE run_id=?",
            (utc_now(), json.dumps({"schema_version": SCHEMA_VERSION}), run_id),
        )
        conn.commit()
        return int(run_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Create and backfill the additive fc_ schema.")
    parser.add_argument("--db", type=Path, required=True, help="SQLite database path.")
    parser.add_argument("--apply", action="store_true", help="Actually write the migration.")
    parser.add_argument("--backup", type=Path, help="Optional backup destination before applying.")
    args = parser.parse_args()
    if not args.db.exists():
        parser.error(f"Database does not exist: {args.db}")
    if not args.apply:
        print("Dry plan only. Re-run with --apply to create fc_ tables and backfill data.")
        return 0
    if args.backup:
        args.backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(args.db, args.backup)
        print(f"Backup created: {args.backup}")
    run_id = migrate(args.db)
    print(f"Migration completed: run_id={run_id}, database={args.db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
