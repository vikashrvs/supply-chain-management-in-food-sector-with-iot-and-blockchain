"""
Poll sensor_readings every 5 seconds for up to 2 minutes.
Detects the moment a new row (id > 10133) appears and prints the full verification.
Run from: C:\Users\raj vikash\Desktop\food_chain\backend
"""
import sqlite3, time, sys

DB         = r'C:\Users\raj vikash\Desktop\food_chain\backend\food_chain.db'
BASELINE_ID = 10133   # last known id before test
TIMEOUT_S   = 120     # 2 minutes max

print("Polling sensor_readings for new row (id > {}) ...".format(BASELINE_ID))
print("ESP32 publishes every ~30s. Will detect within 30s of next publish.")
print()

start = time.time()
found = False

while time.time() - start < TIMEOUT_S:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM sensor_readings")
    sr_count = c.fetchone()[0]

    c.execute("""
        SELECT id, batch_id, sensor_id, device_id,
               temperature, humidity, timestamp,
               block_hash, previous_block_hash,
               field_hash, fabric_tx_id
        FROM sensor_readings
        WHERE id > ?
        ORDER BY id ASC
    """, (BASELINE_ID,))
    new_rows = c.fetchall()

    # Also check chain tail for previous row
    c.execute("SELECT id, block_hash FROM sensor_readings WHERE id = ?", (BASELINE_ID,))
    prev_row = c.fetchone()

    c.execute("SELECT COUNT(*) FROM sensor_data")
    sd_count = c.fetchone()[0]

    conn.close()

    if new_rows:
        found = True
        print("=" * 60)
        print("NEW ROW(S) DETECTED in sensor_readings!")
        print("=" * 60)

        for r in new_rows:
            chain_ok = (r['previous_block_hash'] == prev_row['block_hash']) if prev_row else None
            print()
            print("  id                  :", r['id'])
            print("  batch_id            :", r['batch_id'])
            print("  sensor_id           :", r['sensor_id'])
            print("  device_id           :", r['device_id'])
            print("  temperature         :", r['temperature'])
            print("  humidity            :", r['humidity'])
            print("  timestamp           :", r['timestamp'])
            print("  block_hash          :", r['block_hash'])
            print("  previous_block_hash :", r['previous_block_hash'])
            print("  field_hash          :", r['field_hash'])
            print("  fabric_tx_id        :", r['fabric_tx_id'])
            print()
            print("  [CHAIN]  prev row id={} block_hash={}".format(
                prev_row['id'] if prev_row else 'N/A',
                prev_row['block_hash'] if prev_row else 'N/A'))
            print("  [CHAIN]  new  prev_block_hash={}".format(r['previous_block_hash']))
            if chain_ok is True:
                print("  [CHAIN]  INTACT -- previous_block_hash matches prev row block_hash  PASS")
            elif chain_ok is False:
                print("  [CHAIN]  BROKEN -- MISMATCH  FAIL")
            else:
                print("  [CHAIN]  (could not verify -- prev row not found)")

        print()
        print("=" * 60)
        print("COUNTS")
        print("  sensor_data    rows:", sd_count, "  (must stay at 9332)")
        print("  sensor_readings rows:", sr_count, "  (must be > 9332)")
        print()
        print("sensor_data unchanged:", "PASS" if sd_count == 9332 else "FAIL -- UNEXPECTED CHANGE")
        print("sensor_readings grew  :", "PASS" if sr_count > 9332 else "FAIL")
        break
    else:
        elapsed = int(time.time() - start)
        print("  [{}s]  sensor_readings={} rows, no new row yet ...".format(elapsed, sr_count))
        time.sleep(5)

if not found:
    print()
    print("TIMEOUT -- no new row appeared in sensor_readings within", TIMEOUT_S, "seconds.")
    print("Check: is ESP32 powered on? Is MQTT broker reachable at 192.168.0.110:1883?")
    sys.exit(1)
