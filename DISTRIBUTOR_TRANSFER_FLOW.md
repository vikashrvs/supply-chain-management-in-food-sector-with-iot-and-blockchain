# Distributor transfer flow

The new Distributor dashboard records business custody events only. A transfer
form may submit:

- an existing `batch_id`;
- an event type (`received`, `transferred`, `checkpoint`, or `anomaly`);
- a destination/facility name;
- notes or an anomaly description.

It must not submit temperature, humidity, GPS coordinates, device IDs, hashes,
or Fabric transaction IDs. The backend rejects unexpected fields and stores
those transfer columns as empty legacy-compatible values.

After the event is created, the API joins the batch to its latest canonical
`sensor_data` record. When available, the response includes:

- `latest_iot`;
- `temperature`, `humidity`;
- `latitude`, `longitude`;
- `device_id`;
- `block_hash`, `field_hash`;
- `fabric_tx_id`.

The ESP32/MQTT pipeline remains the only source for telemetry and integrity
data. The distributor records what happened to the shipment; the backend
records what the device measured.
