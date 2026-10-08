"""
Debug script: Check database state, test insert, and verify MQTT flow.
Run from backend/ directory with the venv active.
"""
import sys, json
sys.path.insert(0, '.')

from database import get_connection, insert_sensor_data
from schemas import SensorReading

print("=" * 60)
print("STEP 1: Database latest records")
print("=" * 60)
with get_connection() as conn:
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as cnt FROM sensor_data WHERE batch_id='FC-001'")
    print("FC-001 records:", cursor.fetchone()['cnt'])

    cursor.execute(
        "SELECT id, batch_id, temperature, humidity, sensor_id, telemetry_mode, timestamp "
        "FROM sensor_data ORDER BY id DESC LIMIT 5"
    )
    rows = cursor.fetchall()
    print("\nLatest 5 records in sensor_data:")
    for r in rows:
        print(" id=%s batch=%s temp=%s hum=%s mode=%s ts=%s" % (
            r['id'], r['batch_id'], r['temperature'], r['humidity'],
            r['telemetry_mode'], r['timestamp']
        ))

print()
print("=" * 60)
print("STEP 2: Test full MQTT->insert pipeline")
print("=" * 60)

payload = {
    "device_id": "ESP32-01",
    "sensor_id": "ESP32-01",
    "batch_id": "FC-001",
    "temperature": 28.3,
    "humidity": 65.6,
    "gas_value": 0,
    "latitude": 12.971599,
    "longitude": 77.594566,
    "current_stage": "transport",
    "status": "ALERT",
    "telemetry_mode": "Live ESP32 Hardware",
}

try:
    validated = SensorReading(**payload)
    print("Pydantic validation: PASS")
    insert_sensor_data(validated.model_dump())
    print("insert_sensor_data: PASS")
except Exception as e:
    import traceback
    print("FAIL:", type(e).__name__, str(e))
    traceback.print_exc()

print()
print("=" * 60)
print("STEP 3: Verify insert landed")
print("=" * 60)
with get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, batch_id, temperature, humidity, telemetry_mode, timestamp "
        "FROM sensor_data ORDER BY id DESC LIMIT 3"
    )
    for r in cursor.fetchall():
        print(" id=%s batch=%s temp=%s hum=%s mode=%s ts=%s" % (
            r['id'], r['batch_id'], r['temperature'], r['humidity'],
            r['telemetry_mode'], r['timestamp']
        ))

print()
print("=" * 60)
print("STEP 4: Check MQTT broker (port 1883)")
print("=" * 60)
import socket
s = socket.socket()
s.settimeout(3)
try:
    s.connect(("localhost", 1883))
    print("Mosquitto port 1883: OPEN - broker is reachable")
except Exception as e:
    print("Mosquitto port 1883: CLOSED -", e)
finally:
    s.close()

print("\nDone.")
