# Prerecorded Replay Data

This folder is for the current demo/review mode.

The backend seeds three deterministic demo batches into SQLite on startup:

- `FC-001` - Bengaluru Cold Storage Facility to Mysuru Distribution Center
- `FC-002` - Bengaluru Processing Facility to Mandya Warehouse
- `FC-003` - Bengaluru Warehouse to Hassan Retail Distribution Hub

Each batch has 100 sequential telemetry records and is replayed through MQTT using the same payload schema planned for the future ESP32 device.

Current source:

```text
replay_telemetry table -> MQTT -> FastAPI -> sensor_data -> dashboard
```

This is not real-time physical GPS tracking.

