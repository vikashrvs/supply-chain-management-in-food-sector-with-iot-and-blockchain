from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta

from database import (
    get_connection,
    init_db,
    upsert_product_registry,
    build_record,
)
from services.fabric_client import check_fabric_connection, submit_to_fabric
from services.hash_chain import get_last_hash, compute_block_hash, compute_record_hash

DEMO_BATCHES = [
    {
        "batch_id": "DB-FAB-001",
        "product": "Organic Mango Batch",
        "product_uid": "UID-DB-FAB-001",
        "origin": "Bengaluru Cold Storage",
        "destination": "Mysuru Distribution",
        "device_id": "DB-SENSOR-01",
        "route": [
            (12.971599, 77.594566),
            (12.918000, 77.520000),
            (12.820000, 77.410000),
            (12.703000, 77.260000),
            (12.520000, 76.910000),
        ],
    },
    {
        "batch_id": "DB-FAB-002",
        "product": "Cold Chain Dairy Batch",
        "product_uid": "UID-DB-FAB-002",
        "origin": "Bengaluru Processing",
        "destination": "Mandya Cold Hub",
        "device_id": "DB-SENSOR-02",
        "route": [
            (12.935200, 77.624500),
            (12.882000, 77.540000),
            (12.794000, 77.430000),
            (12.690000, 77.289000),
            (12.560000, 76.960000),
        ],
    },
    {
        "batch_id": "DB-FAB-003",
        "product": "Fresh Produce Shipment",
        "product_uid": "UID-DB-FAB-003",
        "origin": "Bengaluru Warehouse",
        "destination": "Hassan Retail Hub",
        "device_id": "DB-SENSOR-03",
        "route": [
            (13.020600, 77.647900),
            (13.006000, 77.560000),
            (12.995000, 77.430000),
            (12.990000, 77.120000),
            (12.950000, 76.820000),
        ],
    },
]

STAGES = [
    "field",
    "processing",
    "transport",
    "warehouse",
    "retailer",
]


def _make_record(batch_cfg: dict, index: int, stage_name: str, timestamp: datetime):
    route = batch_cfg["route"]
    lat, lon = route[min(index, len(route) - 1)]
    temperature = 6.0 + (index % 4) * 0.8 + (0 if stage_name == "field" else 1.5)
    humidity = 68 + (index % 3) * 4
    gas_value = 90 + (index * 7) % 80
    return {
        "batch_id": batch_cfg["batch_id"],
        "product_id": batch_cfg["batch_id"],
        "product_uid": batch_cfg["product_uid"],
        "product": batch_cfg["product"],
        "product_name": batch_cfg["product"],
        "sensor_id": batch_cfg["device_id"],
        "current_stage": stage_name,
        "status": stage_name,
        "transportation_status": stage_name,
        "alert_status": "NORMAL",
        "telemetry_mode": "DB-backed blockchain demo",
        "temperature": temperature,
        "humidity": humidity,
        "latitude": lat,
        "longitude": lon,
        "gas_value": gas_value,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "origin_name": batch_cfg["origin"],
        "destination_name": batch_cfg["destination"],
        "source": "database-demo",
        "alert_flag": 0,
    }


def seed_demo_rows(reset: bool = False, limit: int = 50):
    """Seed a small set of product records into the database for Fabric registration."""
    init_db()
    with get_connection() as conn:
        if reset:
            conn.execute("DELETE FROM sensor_readings WHERE batch_id LIKE 'DB-FAB-%'")
            conn.execute("DELETE FROM product_registry WHERE batch_id LIKE 'DB-FAB-%'")
            conn.commit()

        count = conn.execute("SELECT COUNT(*) FROM sensor_readings WHERE batch_id LIKE 'DB-FAB-%'").fetchone()[0]
        if reset or count == 0:
            base = datetime.now() - timedelta(hours=2)
            for batch_cfg in DEMO_BATCHES:
                for idx in range(min(limit, 12)):
                    stage_name = STAGES[min(idx, len(STAGES) - 1)]
                    ts = base + timedelta(minutes=idx * 5)
                    record = _make_record(batch_cfg, idx, stage_name, ts)
                    normalized = build_record(record)
                    prev_hash = get_last_hash(normalized["batch_id"])
                    block_hash = compute_block_hash(normalized, prev_hash)
                    field_hash = compute_record_hash(normalized)
                    registry_id = upsert_product_registry(
                        conn,
                        normalized["product_uid"],
                        normalized["batch_id"],
                        normalized["product"],
                        normalized["product_name"],
                        normalized["timestamp"],
                    )
                    conn.execute(
                        """
                        INSERT INTO sensor_readings (
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
                            device_id,
                            current_stage,
                            product_ref,
                            block_hash,
                            previous_block_hash,
                            field_hash,
                            fabric_tx_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized["timestamp"],
                            normalized["temperature"],
                            normalized["humidity"],
                            normalized["latitude"],
                            normalized["longitude"],
                            normalized["gas_value"],
                            normalized["product_id"],
                            normalized["status"],
                            normalized["transportation_status"],
                            normalized["alert_status"],
                            normalized["telemetry_mode"],
                            normalized["origin_name"],
                            normalized["destination_name"],
                            idx + 1,
                            normalized["product_name"],
                            normalized["batch_id"],
                            normalized["product_uid"],
                            normalized["product"],
                            normalized["sensor_id"],
                            normalized["sensor_id"],
                            normalized["current_stage"],
                            registry_id,
                            block_hash,
                            prev_hash,
                            field_hash,
                            None,
                        ),
                    )
            conn.commit()

    total_rows = _pending_row_count(limit=limit)
    return {"seeded": total_rows, "mode": "db-backed-live-demo"}


def _pending_row_count(limit: int = 1000):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM sensor_readings WHERE batch_id LIKE 'DB-FAB-%' AND (fabric_tx_id IS NULL OR fabric_tx_id = '') LIMIT ?",
            (limit,),
        ).fetchone()
        return int(row[0])


def register_pending_rows(limit: int = 1000):
    """Submit any DB-backed demo rows that do not yet have a Fabric tx id."""
    check_fabric_connection()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM sensor_readings
            WHERE batch_id LIKE 'DB-FAB-%'
              AND (fabric_tx_id IS NULL OR fabric_tx_id = '')
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    updated = 0
    for row in rows:
        payload = {
            "batch_id": row["batch_id"],
            "temperature": row["temperature"],
            "humidity": row["humidity"],
            "current_stage": row["current_stage"],
            "product_name": row["product_name"],
            "sensor_id": row["sensor_id"],
            "timestamp": row["timestamp"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "gas_value": row["gas_value"],
            "alert_status": row["alert_status"],
            "transportation_status": row["transportation_status"],
            "telemetry_mode": row["telemetry_mode"],
        }
        tx_id = submit_to_fabric(row["batch_id"], payload)
        if tx_id:
            with get_connection() as conn:
                conn.execute("UPDATE sensor_readings SET fabric_tx_id = ? WHERE id = ?", (tx_id, row["id"]))
                conn.commit()
            updated += 1

    return {"submitted": updated, "pending": _pending_row_count(limit)}


def main():
    parser = argparse.ArgumentParser(description="DB-backed Fabric demo pipeline")
    parser.add_argument("--seed", action="store_true", help="Seed demo rows into SQLite")
    parser.add_argument("--reset", action="store_true", help="Clear any prior DB demo rows before seeding")
    parser.add_argument("--limit", type=int, default=12, help="Number of demo rows per batch to seed")
    parser.add_argument("--register", action="store_true", help="Submit pending DB rows to Fabric")
    args = parser.parse_args()

    if args.seed:
        print(json.dumps(seed_demo_rows(reset=args.reset, limit=args.limit), indent=2))

    if args.register:
        print(json.dumps(register_pending_rows(limit=max(1, args.limit * 3)), indent=2))

    if not args.seed and not args.register:
        print(json.dumps({"status": "no_action", "message": "Use --seed and/or --register"}, indent=2))


if __name__ == "__main__":
    main()
