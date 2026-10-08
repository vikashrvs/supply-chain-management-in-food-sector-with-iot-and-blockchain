# Real ESP32 operation and replay isolation

Date: 2026-10-01

## Change

- Physical ESP32/MQTT telemetry is the default operational source.
- Replay MQTT messages on `food/sensor/replay/#` are ignored unless `FOODCHAIN_ENABLE_REPLAY=1` is explicitly set before backend startup.
- Replay/live-feed routes no longer select `FC-001` or demo metadata implicitly.
- Legacy replay routes require `demo=true` for demo fallback and replay controls.
- The four new role dashboards do not call `/api/replay/*`; they use live role APIs and `/data`.
- Sensor payloads default to `Physical ESP32 Telemetry`.

## Runtime contract

1. Create a real batch first.
2. Assign or authorize the device for that batch.
3. Let ESP32 publish to MQTT.
4. Let the backend validate and persist the reading.
5. Let alerts, hash-chain proof, and Fabric submission run from the accepted reading.
6. Use the legacy single dashboard only for an intentional demo session.

Replay is never a valid source for the operational producer, distributor, business, or admin dashboards.
