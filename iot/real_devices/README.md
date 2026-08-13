# Real Devices

Future hardware work goes here.

Planned physical demo stack:

- ESP32 controller
- DHT11 or DHT22 temperature/humidity sensor
- Gas/environment sensor
- GPS module

Future source:

```text
Physical ESP32 + DHT11/DHT22 + Gas + GPS -> MQTT -> FastAPI -> sensor_data -> dashboard
```

Keep the MQTT payload compatible with `iot/prerecorded_replay/replay_payload_schema.json`.

