import sqlite3
from pathlib import Path


DB_PATH = Path(__file__).resolve().parent / "food_chain.db"


def table_exists(conn, table_name):
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def main():
    print("=" * 70)
    print("FoodChain - Sensor Reading Migration")
    print("=" * 70)
    print(f"Database: {DB_PATH}")
    print()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        # ---------------------------------------------------------
        # 1. Check source table
        # ---------------------------------------------------------
        if not table_exists(conn, "sensor_data"):
            raise RuntimeError("Source table 'sensor_data' was not found.")

        source_count = conn.execute(
            "SELECT COUNT(*) FROM sensor_data"
        ).fetchone()[0]

        print(f"Source sensor_data rows : {source_count}")

        if source_count == 0:
            raise RuntimeError("sensor_data is empty. Migration stopped.")

        # ---------------------------------------------------------
        # 2. Create final sensor_readings table
        # ---------------------------------------------------------
        print("\nCreating sensor_readings table...")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY,

                timestamp TEXT NOT NULL,

                temperature REAL,
                humidity REAL,
                gas_value REAL,

                latitude REAL,
                longitude REAL,
                location TEXT,

                product_id TEXT,
                product_name TEXT,
                product_uid TEXT,
                product TEXT,

                batch_id TEXT NOT NULL,
                sensor_id TEXT,
                device_id TEXT,

                current_stage TEXT,

                status TEXT,
                transportation_status TEXT,
                alert_status TEXT,
                alert_flag INTEGER,
                alert_source TEXT,

                telemetry_mode TEXT,

                origin_name TEXT,
                destination_name TEXT,

                product_ref TEXT,

                block_hash TEXT,
                previous_block_hash TEXT,
                field_hash TEXT,
                fabric_tx_id TEXT,

                replay_record_id INTEGER,

                legacy_source_id INTEGER UNIQUE
            )
            """
        )

        # ---------------------------------------------------------
        # 3. Determine whether migration already contains rows
        # ---------------------------------------------------------
        existing_count = conn.execute(
            "SELECT COUNT(*) FROM sensor_readings"
        ).fetchone()[0]

        print(f"Existing sensor_readings rows: {existing_count}")

        if existing_count > 0:
            print(
                "\nWARNING: sensor_readings already contains data."
            )
            print(
                "This script will only insert missing legacy records."
            )

        # ---------------------------------------------------------
        # 4. Read ALL legacy sensor records
        #
        # IMPORTANT:
        # Deterministic order:
        # batch_id -> timestamp -> id
        #
        # This gives us the previous_block_hash relationship.
        # ---------------------------------------------------------
        rows = conn.execute(
            """
            SELECT
                id,
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
                fabric_tx_id,
                field_hash,
                gas_value,
                transportation_status,
                alert_status,
                telemetry_mode,
                origin_name,
                destination_name,
                replay_record_id,
                alert_flag,
                alert_source
            FROM sensor_data
            ORDER BY batch_id, timestamp, id
            """
        ).fetchall()

        print(f"Records to process          : {len(rows)}")

        # ---------------------------------------------------------
        # 5. Build previous_block_hash chain
        #
        # Existing block_hash values are NEVER recalculated.
        # ---------------------------------------------------------
        previous_hash_by_batch = {}

        inserted = 0
        skipped = 0

        for row in rows:
            legacy_id = row["id"]
            batch_id = row["batch_id"]

            if not batch_id:
                batch_key = "__NO_BATCH__"
            else:
                batch_key = batch_id

            # Skip if this legacy record was already migrated.
            already_exists = conn.execute(
                """
                SELECT 1
                FROM sensor_readings
                WHERE legacy_source_id = ?
                """,
                (legacy_id,),
            ).fetchone()

            if already_exists:
                # Still preserve chain state for subsequent rows.
                if row["block_hash"]:
                    previous_hash_by_batch[batch_key] = row["block_hash"]

                skipped += 1
                continue

            previous_block_hash = previous_hash_by_batch.get(batch_key)

            # Device ID:
            # Current legacy sensor_data uses sensor_id as the device
            # identifier in the existing application flow.
            device_id = row["sensor_id"]

            conn.execute(
                """
                INSERT INTO sensor_readings (
                    id,
                    timestamp,

                    temperature,
                    humidity,
                    gas_value,

                    latitude,
                    longitude,
                    location,

                    product_id,
                    product_name,
                    product_uid,
                    product,

                    batch_id,
                    sensor_id,
                    device_id,

                    current_stage,

                    status,
                    transportation_status,
                    alert_status,
                    alert_flag,
                    alert_source,

                    telemetry_mode,

                    origin_name,
                    destination_name,

                    product_ref,

                    block_hash,
                    previous_block_hash,
                    field_hash,
                    fabric_tx_id,

                    replay_record_id,

                    legacy_source_id
                )
                VALUES (
                    ?, ?,

                    ?, ?, ?,

                    ?, ?, ?,

                    ?, ?, ?, ?,

                    ?, ?, ?,

                    ?,

                    ?, ?, ?, ?, ?,

                    ?,

                    ?, ?,

                    ?,

                    ?, ?, ?, ?,

                    ?,

                    ?
                )
                """,
                (
                    row["id"],
                    row["timestamp"],

                    row["temperature"],
                    row["humidity"],
                    row["gas_value"],

                    row["latitude"],
                    row["longitude"],
                    row["location"]
                    if "location" in row.keys()
                    else None,

                    row["product_id"],
                    row["product_name"],
                    row["product_uid"],
                    row["product"],

                    row["batch_id"],
                    row["sensor_id"],
                    device_id,

                    row["current_stage"],

                    row["status"],
                    row["transportation_status"],
                    row["alert_status"],
                    row["alert_flag"],
                    row["alert_source"],

                    row["telemetry_mode"],

                    row["origin_name"],
                    row["destination_name"],

                    row["product_ref"],

                    row["block_hash"],
                    previous_block_hash,
                    row["field_hash"],
                    row["fabric_tx_id"],

                    row["replay_record_id"],

                    row["id"],
                ),
            )

            inserted += 1

            # The CURRENT record becomes the previous record
            # for the next reading of this batch.
            if row["block_hash"]:
                previous_hash_by_batch[batch_key] = row["block_hash"]

        # ---------------------------------------------------------
        # 6. Indexes
        # ---------------------------------------------------------
        print("\nCreating indexes...")

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_batch
            ON sensor_readings(batch_id)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_timestamp
            ON sensor_readings(timestamp)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_device
            ON sensor_readings(device_id)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_fabric_tx
            ON sensor_readings(fabric_tx_id)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_block_hash
            ON sensor_readings(block_hash)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sensor_readings_previous_hash
            ON sensor_readings(previous_block_hash)
            """
        )

        conn.commit()

        # ---------------------------------------------------------
        # 7. Verification
        # ---------------------------------------------------------
        new_count = conn.execute(
            "SELECT COUNT(*) FROM sensor_readings"
        ).fetchone()[0]

        block_hash_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_readings
            WHERE block_hash IS NOT NULL
              AND TRIM(block_hash) != ''
            """
        ).fetchone()[0]

        field_hash_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_readings
            WHERE field_hash IS NOT NULL
              AND TRIM(field_hash) != ''
            """
        ).fetchone()[0]

        fabric_tx_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_readings
            WHERE fabric_tx_id IS NOT NULL
              AND TRIM(fabric_tx_id) != ''
            """
        ).fetchone()[0]

        previous_hash_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_readings
            WHERE previous_block_hash IS NOT NULL
              AND TRIM(previous_block_hash) != ''
            """
        ).fetchone()[0]

        # ---------------------------------------------------------
        # 8. Compare important evidence with legacy table
        # ---------------------------------------------------------
        legacy_block_hash_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_data
            WHERE block_hash IS NOT NULL
              AND TRIM(block_hash) != ''
            """
        ).fetchone()[0]

        legacy_field_hash_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_data
            WHERE field_hash IS NOT NULL
              AND TRIM(field_hash) != ''
            """
        ).fetchone()[0]

        legacy_fabric_tx_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_data
            WHERE fabric_tx_id IS NOT NULL
              AND TRIM(fabric_tx_id) != ''
            """
        ).fetchone()[0]

        # ---------------------------------------------------------
        # 9. Verify every legacy record exists in new table
        # ---------------------------------------------------------
        missing_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_data s
            WHERE NOT EXISTS (
                SELECT 1
                FROM sensor_readings r
                WHERE r.legacy_source_id = s.id
            )
            """
        ).fetchone()[0]

        # ---------------------------------------------------------
        # 10. Verify hashes were preserved
        # ---------------------------------------------------------
        block_hash_mismatch = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_data s
            JOIN sensor_readings r
              ON r.legacy_source_id = s.id
            WHERE COALESCE(s.block_hash, '')
               != COALESCE(r.block_hash, '')
            """
        ).fetchone()[0]

        field_hash_mismatch = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_data s
            JOIN sensor_readings r
              ON r.legacy_source_id = s.id
            WHERE COALESCE(s.field_hash, '')
               != COALESCE(r.field_hash, '')
            """
        ).fetchone()[0]

        fabric_tx_mismatch = conn.execute(
            """
            SELECT COUNT(*)
            FROM sensor_data s
            JOIN sensor_readings r
              ON r.legacy_source_id = s.id
            WHERE COALESCE(s.fabric_tx_id, '')
               != COALESCE(r.fabric_tx_id, '')
            """
        ).fetchone()[0]

        # ---------------------------------------------------------
        # 11. Final report
        # ---------------------------------------------------------
        print("\n" + "=" * 70)
        print("MIGRATION RESULT")
        print("=" * 70)

        print(f"Legacy sensor_data rows       : {source_count}")
        print(f"New sensor_readings rows      : {new_count}")
        print(f"Inserted this run              : {inserted}")
        print(f"Already present/skipped        : {skipped}")

        print()
        print(f"Block hashes                   : {block_hash_count}")
        print(f"Field hashes                   : {field_hash_count}")
        print(f"Fabric TX IDs                  : {fabric_tx_count}")
        print(f"Previous block hashes          : {previous_hash_count}")

        print()
        print("LEGACY → NEW VERIFICATION")
        print("-" * 70)
        print(
            f"Block hash count               : "
            f"{legacy_block_hash_count} → {block_hash_count}"
        )
        print(
            f"Field hash count               : "
            f"{legacy_field_hash_count} → {field_hash_count}"
        )
        print(
            f"Fabric TX count                : "
            f"{legacy_fabric_tx_count} → {fabric_tx_count}"
        )

        print()
        print("INTEGRITY CHECKS")
        print("-" * 70)
        print(f"Missing migrated records       : {missing_count}")
        print(f"Block hash mismatches          : {block_hash_mismatch}")
        print(f"Field hash mismatches          : {field_hash_mismatch}")
        print(f"Fabric TX mismatches           : {fabric_tx_mismatch}")

        print()

        if (
            new_count == source_count
            and missing_count == 0
            and block_hash_mismatch == 0
            and field_hash_mismatch == 0
            and fabric_tx_mismatch == 0
        ):
            print("✅ MIGRATION VERIFICATION PASSED")
            print("Existing sensor_data evidence was preserved.")
        else:
            print("⚠️ VERIFICATION NEEDS REVIEW")
            print("DO NOT delete sensor_data yet.")

        print("=" * 70)

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()