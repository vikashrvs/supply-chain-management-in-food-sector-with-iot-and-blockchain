"""Quick script to check if PROD-FFA0C5CF data exists in sensor_data."""
import sqlite3

conn = sqlite3.connect("food_chain.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Check sensor_data for this batch
cur.execute(
    "SELECT id, timestamp, temperature, humidity, batch_id, sensor_id, current_stage, telemetry_mode "
    "FROM sensor_data WHERE batch_id = ? ORDER BY id DESC LIMIT 10",
    ("PROD-FFA0C5CF",),
)
rows = cur.fetchall()
print(f"=== sensor_data rows for PROD-FFA0C5CF: {len(rows)} ===")
for r in rows:
    print(dict(r))

# Check batches table
cur.execute(
    "SELECT id, batch_id, product_name, status, created_by FROM batches WHERE batch_id = ?",
    ("PROD-FFA0C5CF",),
)
batch_rows = cur.fetchall()
print(f"\n=== batches table for PROD-FFA0C5CF: {len(batch_rows)} ===")
for r in batch_rows:
    print(dict(r))

# Check product_registry
cur.execute(
    "SELECT id, product_uid, batch_id, product, product_name FROM product_registry WHERE batch_id = ?",
    ("PROD-FFA0C5CF",),
)
pr_rows = cur.fetchall()
print(f"\n=== product_registry for PROD-FFA0C5CF: {len(pr_rows)} ===")
for r in pr_rows:
    print(dict(r))

# Check total recent sensor_data
cur.execute("SELECT COUNT(*) as cnt FROM sensor_data")
total = cur.fetchone()["cnt"]
print(f"\n=== Total sensor_data rows in DB: {total} ===")

# Check last 5 inserted records
cur.execute(
    "SELECT id, batch_id, temperature, humidity, timestamp, telemetry_mode FROM sensor_data ORDER BY id DESC LIMIT 5"
)
recent = cur.fetchall()
print("\n=== Last 5 sensor_data records ===")
for r in recent:
    print(dict(r))

conn.close()
