import json
import random
import time
import uuid
from datetime import datetime

import paho.mqtt.client as mqtt


BROKER = "localhost"
PORT = 1883
PUBLISH_INTERVAL_SECONDS = 5
STAGE_ADVANCE_INTERVAL = 3
ANOMALY_CHANCE = 0.28
STAGES = ["field", "warehouse", "transport", "retailer", "consumer"]

STAGE_CONDITIONS = {
    "field": {"temperature": (18.0, 27.0), "humidity": (65.0, 85.0)},
    "warehouse": {"temperature": (4.0, 10.0), "humidity": (70.0, 90.0)},
    "transport": {"temperature": (5.0, 12.0), "humidity": (60.0, 80.0)},
    "retailer": {"temperature": (6.0, 14.0), "humidity": (55.0, 75.0)},
    "consumer": {"temperature": (8.0, 16.0), "humidity": (50.0, 70.0)},
}

STAGE_LOCATIONS = {
    "field": {"lat": 12.940000, "lng": 77.520000},
    "warehouse": {"lat": 12.980000, "lng": 77.560000},
    "transport": {"lat": 13.010000, "lng": 77.610000},
    "retailer": {"lat": 13.040000, "lng": 77.650000},
    "consumer": {"lat": 13.070000, "lng": 77.690000},
}

BATCHES = [
    {
        "batch_id": "BATCH_001",
        "product_uid": f"UID-{uuid.uuid4().hex[:12].upper()}",
        "product": "Apples",
        "sensor_id": "SENSOR_A",
        "lat_offset": 0.0000,
        "lng_offset": 0.0000,
        "tick": 0,
    },
    {
        "batch_id": "BATCH_002",
        "product_uid": f"UID-{uuid.uuid4().hex[:12].upper()}",
        "product": "Mangoes",
        "sensor_id": "SENSOR_B",
        "lat_offset": 0.0120,
        "lng_offset": 0.0080,
        "tick": STAGE_ADVANCE_INTERVAL,
    },
    {
        "batch_id": "BATCH_003",
        "product_uid": f"UID-{uuid.uuid4().hex[:12].upper()}",
        "product": "Wheat",
        "sensor_id": "SENSOR_C",
        "lat_offset": -0.0100,
        "lng_offset": 0.0140,
        "tick": STAGE_ADVANCE_INTERVAL * 2,
    },
]


def get_stage(batch_tick):
    return STAGES[(batch_tick // STAGE_ADVANCE_INTERVAL) % len(STAGES)]


def build_location(batch, current_stage):
    base = STAGE_LOCATIONS[current_stage]
    return {
        "lat": round(base["lat"] + batch["lat_offset"] + random.uniform(-0.003, 0.003), 6),
        "lng": round(base["lng"] + batch["lng_offset"] + random.uniform(-0.003, 0.003), 6),
    }


def build_payload(batch):
    current_stage = get_stage(batch["tick"])
    climate = STAGE_CONDITIONS[current_stage]
    location = build_location(batch, current_stage)

    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "batch_id": batch["batch_id"],
        "product_uid": batch["product_uid"],
        "product": batch["product"],
        "product_name": batch["product"],
        "product_id": batch["batch_id"],
        "sensor_id": batch["sensor_id"],
        "current_stage": current_stage,
        "temperature": round(random.uniform(*climate["temperature"]), 2),
        "humidity": round(random.uniform(*climate["humidity"]), 2),
        "location": location,
        "status": "Delivered" if current_stage == "consumer" else "In Transit",
    }
    apply_anomaly(payload, climate)
    return payload


def apply_anomaly(payload, climate):
    if random.random() >= ANOMALY_CHANCE:
        return

    anomaly_mode = random.choice(["temp_high", "temp_low", "humidity_high", "humidity_low", "combo"])
    temp_min, temp_max = climate["temperature"]
    humidity_min, humidity_max = climate["humidity"]

    if anomaly_mode in {"temp_high", "combo"}:
        payload["temperature"] = round(temp_max + random.uniform(2.0, 5.0), 2)
    elif anomaly_mode == "temp_low":
        payload["temperature"] = round(temp_min - random.uniform(2.0, 5.0), 2)

    if anomaly_mode in {"humidity_high", "combo"}:
        payload["humidity"] = round(humidity_max + random.uniform(8.0, 16.0), 2)
    elif anomaly_mode == "humidity_low":
        payload["humidity"] = round(max(10.0, humidity_min - random.uniform(8.0, 16.0)), 2)

    payload["simulated_alert"] = anomaly_mode


def publish_batch(client, batch):
    payload = build_payload(batch)
    topic = f"food/sensor/{payload['batch_id']}"
    client.publish(topic, json.dumps(payload))
    print(f"Published to {topic}: {payload}")
    batch["tick"] = (batch["tick"] + 1) % (len(STAGES) * STAGE_ADVANCE_INTERVAL)


def main():
    client = mqtt.Client()
    client.connect(BROKER, PORT, 60)
    client.loop_start()

    print("Food supply chain sensor simulation started...")

    try:
        while True:
            for batch in BATCHES:
                publish_batch(client, batch)
            time.sleep(PUBLISH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Simulation stopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
