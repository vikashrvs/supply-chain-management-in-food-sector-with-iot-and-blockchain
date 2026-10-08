import sys
import sqlite3
from pathlib import Path

# ------------------------------------------------------------
# FOODCHAIN SQLITE HASH-CHAIN VERIFIER
# Uses the project's own hash_chain.py implementation.
# DOES NOT modify the database.
# ------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
BACKEND = ROOT / "backend"
DB_PATH = BACKEND / "food_chain.db"

# Allow imports from backend/
sys.path.insert(0, str(BACKEND))

from services.hash_chain import compute_block_hash, compute_record_hash


def sha256_text(value):
    import hashlib
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def get_db():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def print_line(char="-", width=72):
    print(char * width)


def show(value):
    if value is None or value == "":
        return "NULL"
    return str(value)


def verify_batch(batch_id):
    conn = get_db()

    try:
        rows = conn.execute(
            """
            SELECT *
            FROM sensor_readings
            WHERE batch_id = ?
            ORDER BY id ASC
            """,
            (batch_id,),
        ).fetchall()

        if not rows:
            print(f"\nNO RECORDS FOUND FOR BATCH: {batch_id}")
            return False

        print()
        print_line("=")
        print("              FOODCHAIN SQLITE VERIFIER")
        print_line("=")

        print(f"BATCH: {batch_id}")
        print(f"RECORDS: {len(rows)}")
        print()

        chain_valid = True
        field_valid = True
        previous_hash_valid = True

        expected_previous = None

        for index, row in enumerate(rows, start=1):

            print_line()
            print(f"RECORD / BLOCK #{index}")
            print(f"SQLite ID       : {row['id']}")
            print(f"Timestamp       : {show(row['timestamp'])}")
            print(f"Batch ID        : {show(row['batch_id'])}")
            print(f"Product         : {show(row['product_name'] or row['product'])}")
            print(f"Temperature     : {show(row['temperature'])}")
            print(f"Humidity        : {show(row['humidity'])}")
            print(f"Latitude        : {show(row['latitude'])}")
            print(f"Longitude       : {show(row['longitude'])}")
            print(f"Sensor ID       : {show(row['sensor_id'])}")
            print(f"Stage           : {show(row['current_stage'])}")

            stored_previous = row["previous_block_hash"]
            stored_block = row["block_hash"]
            stored_field = row["field_hash"]

            # ------------------------------------------------
            # Previous hash linkage
            # ------------------------------------------------
            if index == 1:
                previous_ok = stored_previous in (None, "", "0" * 64)
            else:
                previous_ok = stored_previous == expected_previous

            if not previous_ok:
                previous_hash_valid = False

            # ------------------------------------------------
            # Build the same record structure used by backend
            # ------------------------------------------------
            record = {
                "timestamp": row["timestamp"],
                "temperature": row["temperature"],
                "humidity": row["humidity"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "gas_value": row["gas_value"],
                "product_id": row["product_id"],
                "status": row["status"],
                "product_name": row["product_name"],
                "batch_id": row["batch_id"],
                "product_uid": row["product_uid"],
                "product": row["product"],
                "sensor_id": row["sensor_id"],
                "current_stage": row["current_stage"],
            }

            # ------------------------------------------------
            # Recalculate block hash using project's function
            # ------------------------------------------------
            calculated_block = compute_block_hash(
                record,
                stored_previous
            )

            block_ok = calculated_block == stored_block

            # ------------------------------------------------
            # Recalculate field hash
            # ------------------------------------------------
            try:
                calculated_field = compute_record_hash(record)
                field_ok = calculated_field == stored_field
            except Exception:
                calculated_field = None
                field_ok = True

            if not block_ok:
                chain_valid = False

            if not field_ok:
                field_valid = False

            print()
            print("HASH INFORMATION")
            print("-" * 72)

            print("Previous Block Hash:")
            print(show(stored_previous))

            print()
            print("Stored Block Hash:")
            print(show(stored_block))

            print()
            print("Calculated Block Hash:")
            print(show(calculated_block))

            print()
            print("Stored Field Hash:")
            print(show(stored_field))

            if calculated_field:
                print()
                print("Calculated Field Hash:")
                print(calculated_field)

            print()
            print(
                "Previous Hash Link : "
                + ("VALID" if previous_ok else "INVALID")
            )

            print(
                "Block Hash         : "
                + ("VALID" if block_ok else "INVALID")
            )

            print(
                "Field Hash         : "
                + ("VALID" if field_ok else "INVALID")
            )

            print()
            print("Fabric TX ID:")
            print(show(row["fabric_tx_id"]))

            expected_previous = stored_block

        # ----------------------------------------------------
        # FINAL RESULT
        # ----------------------------------------------------
        print()
        print_line("=")
        print("FINAL SQLITE VERIFICATION")
        print_line("=")

        print(
            "Previous Hash Chain : "
            + ("✓ VALID" if previous_hash_valid else "✗ INVALID")
        )

        print(
            "Block Hashes        : "
            + ("✓ VALID" if chain_valid else "✗ INVALID")
        )

        print(
            "Field Hashes        : "
            + ("✓ VALID" if field_valid else "✗ INVALID")
        )

        overall = (
            previous_hash_valid
            and chain_valid
            and field_valid
        )

        print()
        if overall:
            print("✓ SQLITE HASH CHAIN INTEGRITY VERIFIED")
        else:
            print("✗ SQLITE HASH CHAIN VERIFICATION FAILED")

        print_line("=")

        return overall

    finally:
        conn.close()


def main():
    if len(sys.argv) >= 2:
        batch_id = sys.argv[1]
    else:
        # Automatically use the latest batch with sensor data
        conn = get_db()

        row = conn.execute(
            """
            SELECT batch_id
            FROM sensor_readings
            WHERE batch_id IS NOT NULL
              AND batch_id != ''
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

        conn.close()

        if not row:
            print("No sensor records found.")
            sys.exit(1)

        batch_id = row["batch_id"]

    print(f"\nVerifying batch: {batch_id}")

    result = verify_batch(batch_id)

    print()

    if result:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()