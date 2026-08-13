import json
import logging
import threading
import time

from database import clear_demo_transportation_received, fetch_replay_records, replay_row_to_payload

logger = logging.getLogger(__name__)


class ReplayManager:
    def __init__(self):
        self._lock = threading.RLock()
        self._thread = None
        self._paused = False
        self._running = False
        self._index = 0
        self._interval = 1.5
        self._batch_id = "FC-001"
        self._last_record_id = None
        self._total_records = 100

    def status(self):
        with self._lock:
            return {
                "running": self._running,
                "paused": self._paused,
                "batch_id": self._batch_id,
                "current_index": self._index,
                "total_records": self._total_records,
                "last_record_id": self._last_record_id,
                "interval_seconds": self._interval,
                "mode": "Demo Telemetry / Replay Mode",
                "future_source": "Physical ESP32 + DHT11/DHT22 + Gas + GPS",
            }

    def start(self, mqtt_client, batch_id="FC-001", interval_seconds=1.5, reset=False):
        if mqtt_client is None:
            return {"started": False, "detail": "MQTT client is unavailable. Start Mosquitto before replay."}

        with self._lock:
            if reset:
                clear_demo_transportation_received(batch_id)
                self._index = 0
                self._last_record_id = None
            self._batch_id = batch_id
            self._total_records = len(fetch_replay_records(batch_id))
            self._interval = max(0.2, float(interval_seconds))
            self._paused = False
            if self._running:
                return {"started": True, "detail": "Replay already running.", **self.status()}
            self._running = True

        self._thread = threading.Thread(target=self._run, args=(mqtt_client,), daemon=True)
        self._thread.start()
        return {"started": True, **self.status()}

    def pause(self):
        with self._lock:
            self._paused = True
        return self.status()

    def resume(self):
        with self._lock:
            self._paused = False
        return self.status()

    def reset(self, batch_id="FC-001"):
        with self._lock:
            self._running = False
            self._paused = False
            self._index = 0
            self._last_record_id = None
            self._total_records = len(fetch_replay_records(batch_id))
        clear_demo_transportation_received(batch_id)
        return self.status()

    def step_forward(self, mqtt_client):
        if mqtt_client is None:
            return {"stepped": False, "detail": "MQTT client is unavailable. Start Mosquitto before replay."}
        with self._lock:
            self._running = True
            self._paused = True
            rows = fetch_replay_records(self._batch_id)
            if not rows:
                return {"stepped": False, "detail": "No records found."}
            if self._index >= len(rows):
                self._index = 0
            index = self._index
            payload = replay_row_to_payload(rows[index])
            topic = f"food/sensor/replay/{payload['batch_id']}"
            mqtt_client.publish(topic, json.dumps(payload))
            logger.info("Replay stepped %s/%s for %s", index + 1, len(rows), payload["batch_id"])
            self._last_record_id = payload["record_id"]
            self._index += 1
        return {"stepped": True, **self.status()}


    def _run(self, mqtt_client):
        rows = fetch_replay_records(self._batch_id)
        while True:
            with self._lock:
                if not self._running:
                    return
                if self._index >= len(rows):
                    self._running = False
                    return
                paused = self._paused
                index = self._index
                interval = self._interval

            if paused:
                time.sleep(0.25)
                continue

            payload = replay_row_to_payload(rows[index])
            topic = f"food/sensor/replay/{payload['batch_id']}"
            mqtt_client.publish(topic, json.dumps(payload))
            logger.info("Replay published %s/%s for %s", index + 1, len(rows), payload["batch_id"])

            with self._lock:
                self._last_record_id = payload["record_id"]
                self._index += 1

            time.sleep(interval)


replay_manager = ReplayManager()
