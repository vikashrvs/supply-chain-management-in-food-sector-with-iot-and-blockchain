# FoodChain Database Design Proposal

> **Status: design only.**
>
> This document does not change the current database, migrations, APIs, or application code. It defines the database logic that should be agreed on before implementation.

## 1. Design objective

FoodChain needs one authoritative place for each kind of information:

1. **Who** uses the system.
2. **What** product exists.
3. **Which batch** is being tracked.
4. **Which device** produced telemetry.
5. **What the device measured**, at what time, and where.
6. **What supply-chain event** happened to the batch.
7. **Which alert** was generated and why.
8. **Which integrity proof or Fabric transaction** belongs to a record.
9. **What a user did** in the system.

The current database does not consistently separate these concerns. The largest table, `sensor_data`, currently contains batch identity, product identity, telemetry, location, workflow status, alert fields, replay fields, and blockchain fields. That makes the database difficult to query, easy to contradict, and hard to extend safely.

The proposed model is normalized around **batches and append-only events**:

```text
users / roles
       |
       v
products --> batches --> batch_events
                 |           |
                 |           +--> event_locations
                 |
                 +--> devices --> telemetry_readings
                 |                    |
                 |                    +--> integrity_records
                 |
                 +--> alerts
```

## 2. Current database assessment

The current SQLite file contains these important tables:

| Current table | Approx. rows observed | Main concern |
|---|---:|---|
| `sensor_data` | 8,352 | Combines many unrelated domains and repeats product/batch fields on every reading. |
| `product_registry` | 10 | Stores product UID, batch ID, and display names together; some values are actually batch IDs. |
| `batches` | 4 | Useful domain table, but product, origin, destination, quantity, and user identity are partly denormalized. |
| `batch_transfers` | 4 | Represents workflow events but also duplicates temperature and humidity from telemetry. |
| `users` | 7 | Role is embedded as text and includes legacy aliases such as `farmer`, `retailer`, and `distributer`. |
| `replay_telemetry` | 0 | Demo/replay concerns are mixed into the production schema even when unused. |
| `audit_logs` | 65 | Useful, but actor identity and event semantics are stored as free text. |

### Specific problems to remove

#### A. One table has too many meanings

`sensor_data` contains all of the following:

- batch and product identifiers;
- product names;
- device identity;
- temperature, humidity, gas, and GPS;
- workflow stage and delivery status;
- alert state;
- replay metadata;
- SHA-256 fields;
- Fabric transaction ID;
- origin and destination names.

A telemetry row should primarily answer: **“What did device X measure at time T?”** It should not be the product catalog, shipment state, alert history, and blockchain transaction table at the same time.

#### B. Duplicate sources of truth

The same concepts appear in several places:

- `sensor_data.batch_id`
- `product_registry.batch_id`
- `batches.batch_id`
- `batch_transfers.batch_id`

Also, temperature and humidity appear in both `sensor_data` and `batch_transfers`. These values can disagree. The proposed rule is:

> **Telemetry belongs in `telemetry_readings`. A transfer/checkpoint event references the relevant telemetry reading; it does not copy sensor measurements unless the event is a manually recorded measurement with an explicit source.**

#### C. Product identity is unclear

`product_uid` is sometimes a real product identifier and sometimes equal to a batch ID. A batch is a quantity of a product; it is not the product itself. These must become separate concepts.

#### D. Status is overloaded

The current schema has `status`, `transportation_status`, and `current_stage`. These represent different ideas:

- **workflow stage:** field, warehouse, transport, retailer, consumer;
- **batch lifecycle:** created, active, completed, cancelled;
- **shipment event:** received, transferred, checkpoint, anomaly;
- **telemetry health:** online, stale, disconnected.

They should not be stored as interchangeable strings.

#### E. Replay data is mixed with real-device data

Replay records and physical ESP32 records may share the same telemetry shape, but their source and lifecycle are different. The source should be explicit, not inferred from `telemetry_mode` or `replay_record_id`.

#### F. Blockchain data is embedded in operational rows

`block_hash`, `field_hash`, and `fabric_tx_id` are integrity evidence. They should be related to a reading or event through a dedicated integrity table. This allows Fabric retries, multiple proofs, and verification history without modifying the original measurement row.

## 3. Proposed tables

The following is the recommended logical schema. Names are suggestions; the important part is the responsibility and relationship of each table.

### 3.1 Identity and access

#### `users`

One row per human or service account.

| Column | Meaning |
|---|---|
| `user_id` | Internal primary key. |
| `username` | Unique login name. |
| `password_hash` | Bcrypt/Argon2 hash only; never a password. |
| `display_name` | Human-readable name. |
| `is_active` | Whether the account can authenticate. |
| `created_at`, `updated_at` | UTC timestamps. |

#### `roles`

Controlled role catalog:

- `admin`
- `producer`
- `distributor`
- `manager`
- `consumer`

#### `user_roles`

Many-to-many link between users and roles. This avoids legacy role aliases being stored as separate business meanings.

```text
users 1 --- many user_roles many --- 1 roles
```

Legacy names such as `farmer`, `retailer`, and the misspelled `distributer` should be migration aliases only, not permanent canonical roles.

### 3.2 Product and batch master data

#### `products`

One row per product definition, independent of a shipment.

| Column | Meaning |
|---|---|
| `product_id` | Internal primary key. |
| `product_code` | Stable unique code. |
| `name` | Product name, such as milk or mango. |
| `product_type` | Organic, non-veg, dairy, etc. |
| `description` | Product-level description. |
| `created_at` | Creation timestamp. |

#### `batches`

One row per physical production batch.

| Column | Meaning |
|---|---|
| `batch_id` | Stable public identifier such as `PROD-BC5F65BF`. |
| `product_id` | Foreign key to `products`. |
| `quantity_value` | Numeric quantity. |
| `quantity_unit` | `kg`, `litre`, `crate`, etc. |
| `harvested_at` | Harvest/production time. |
| `origin_location_id` | Foreign key to `locations`. |
| `destination_location_id` | Foreign key to `locations`. |
| `lifecycle_status` | `created`, `in_transit`, `delivered`, `cancelled`, etc. |
| `created_by_user_id` | Foreign key to `users`. |
| `created_at`, `updated_at` | UTC timestamps. |

Do not store `50crates` in one text column. Store `quantity_value = 50` and `quantity_unit = 'crate'`.

#### `batch_identifiers`

Optional table for public identifiers attached to a batch:

- QR code value;
- product UID;
- external ERP identifier;
- legacy identifier.

This prevents `product_uid`, `batch_id`, and `product_id` from being treated as the same key.

### 3.3 Locations and devices

#### `locations`

Reusable named places:

| Column | Meaning |
|---|---|
| `location_id` | Primary key. |
| `name` | Bengaluru farm, Delhi warehouse, etc. |
| `location_type` | Farm, warehouse, processor, distributor, retailer, consumer. |
| `latitude`, `longitude` | Coordinates. |
| `address` | Optional human-readable address. |

`origin` and `destination` should reference this table instead of repeating free-text names on every row.

#### `devices`

One row per ESP32, gateway, or simulator.

| Column | Meaning |
|---|---|
| `device_id` | Stable identifier such as `ESP32-01`. |
| `device_type` | `esp32`, `gateway`, `replay`. |
| `serial_number` | Hardware serial if available. |
| `source_type` | `physical`, `replay`, `manual`. |
| `is_active` | Device lifecycle flag. |
| `last_seen_at` | Latest accepted telemetry time. |

#### `batch_devices`

A batch can be observed by more than one device over its lifecycle. This link records assignment:

| Column | Meaning |
|---|---|
| `batch_id` | Batch foreign key. |
| `device_id` | Device foreign key. |
| `assigned_at`, `unassigned_at` | Assignment window. |

### 3.4 Telemetry

#### `telemetry_readings`

The authoritative append-only table for sensor measurements.

| Column | Meaning |
|---|---|
| `reading_id` | Internal primary key. |
| `batch_id` | Foreign key to `batches`, nullable only for unassigned device diagnostics. |
| `device_id` | Foreign key to `devices`. |
| `recorded_at` | Time reported by the device, stored UTC. |
| `received_at` | Time accepted by the backend, stored UTC. |
| `temperature_c` | Temperature in Celsius. |
| `humidity_percent` | Relative humidity percentage. |
| `gas_ppm` | Gas/environment reading in ppm. |
| `latitude`, `longitude` | Reading location. |
| `stage_id` | Stage at the time of the reading. |
| `source_message_id` | MQTT message ID or source event ID. |
| `quality_status` | Valid, partial, rejected, estimated. |

Important rules:

1. Every row has a device and a timestamp.
2. `recorded_at` is device time; `received_at` is server time.
3. Do not overwrite readings. Corrections become a new row or a correction record.
4. Use numeric columns with units in the name.
5. Keep raw MQTT payloads separately if replay/debugging is required.

#### `telemetry_raw_messages` (optional but recommended)

Stores the original MQTT payload for forensic debugging:

- `raw_message_id`;
- topic;
- payload JSON;
- received timestamp;
- parse status;
- parsing error.

The dashboard should read normalized `telemetry_readings`, not raw JSON.

### 3.5 Supply-chain workflow

#### `supply_chain_stages`

Controlled stage catalog:

```text
field -> warehouse -> transport -> retailer -> consumer
```

#### `batch_events`

The append-only business event history for a batch.

| Column | Meaning |
|---|---|
| `event_id` | Primary key. |
| `batch_id` | Batch foreign key. |
| `event_type` | Created, received, transferred, checkpoint, delivered, anomaly. |
| `from_stage_id`, `to_stage_id` | Stage transition, if applicable. |
| `location_id` | Where the event occurred. |
| `telemetry_reading_id` | Optional reading associated with the event. |
| `notes` | Human-entered event note. |
| `created_by_user_id` | User or service that recorded it. |
| `occurred_at`, `created_at` | Event time and server insertion time. |

This replaces the current practice of putting transfer temperature/humidity beside the event and then separately storing sensor readings.

#### `batch_current_state` (optional read model)

A one-row-per-batch projection for fast dashboard reads:

- current stage;
- current lifecycle status;
- latest reading ID;
- latest device ID;
- latest reading time;
- current alert count.

This is a cache/projection, not a second source of truth. It can be rebuilt from `batches`, `batch_events`, and `telemetry_readings`.

### 3.6 Alerts and compliance

#### `alert_rules`

Defines rules such as temperature above stage limit, humidity risk, stale device, and blockchain unavailable.

#### `alerts`

One row per generated alert occurrence.

| Column | Meaning |
|---|---|
| `alert_id` | Primary key. |
| `rule_id` | Rule that generated it. |
| `batch_id` | Related batch, nullable for system alerts. |
| `device_id` | Related device, nullable. |
| `telemetry_reading_id` | Reading that triggered it, nullable. |
| `severity` | Info, warning, critical. |
| `message` | Rendered explanation. |
| `status` | Open, acknowledged, resolved. |
| `opened_at`, `acknowledged_at`, `resolved_at` | Alert lifecycle timestamps. |
| `acknowledged_by_user_id` | User who handled it. |

The sidebar badge should count open alerts from this table. The alert panel should display the same query, not a different security-event count.

### 3.7 Integrity and blockchain

#### `integrity_records`

Stores cryptographic evidence without polluting telemetry:

| Column | Meaning |
|---|---|
| `integrity_id` | Primary key. |
| `reading_id` | Related telemetry reading, nullable for event proofs. |
| `event_id` | Related batch event, nullable for reading proofs. |
| `field_hash` | Hash of canonical normalized fields. |
| `block_hash` | Hash-chain block value. |
| `previous_block_hash` | Previous link in the local chain. |
| `verification_status` | Pending, verified, failed. |
| `created_at` | Hash creation time. |

#### `fabric_transactions`

Tracks Fabric submission separately:

| Column | Meaning |
|---|---|
| `fabric_transaction_id` | Fabric transaction ID. |
| `reading_id` / `event_id` | Related business record. |
| `status` | Pending, committed, failed. |
| `submitted_at`, `committed_at` | Lifecycle timestamps. |
| `error_message` | Failure detail, if any. |

This supports retries and preserves the original telemetry even if Fabric is offline.

### 3.8 Audit and ingestion operations

#### `audit_events`

Security and administrative actions:

- login success/failure;
- user creation/deactivation;
- batch creation;
- manual transfer/checkpoint;
- alert acknowledgement;
- permission denial.

Use `actor_user_id` rather than only copying a username. Keep request ID, IP address, action, result, and timestamp.

#### `ingestion_runs` (optional)

Tracks MQTT/replay ingestion health:

- source;
- started/finished time;
- received count;
- accepted count;
- rejected count;
- error count.

## 4. Relationship rules

These rules should be enforced with foreign keys and unique constraints:

1. A **product** can have many batches.
2. A **batch** belongs to exactly one product.
3. A **batch** has many telemetry readings.
4. A **device** produces many telemetry readings.
5. A **batch** has many business events.
6. A business event may reference one telemetry reading, but does not duplicate it.
7. A telemetry reading may have zero or more integrity records.
8. A batch may have zero or more open/resolved alerts.
9. A user may have multiple roles, but role names are controlled.
10. Public IDs (`batch_id`, `device_id`, `product_code`) are unique and immutable.
11. Internal numeric IDs are database keys and should not be shown as business identifiers.

## 5. Recommended data ownership

| Information | Single authoritative table |
|---|---|
| Login account | `users` |
| Permission role | `roles`, `user_roles` |
| Product definition | `products` |
| Batch identity and lifecycle | `batches` |
| Batch identifier/QR aliases | `batch_identifiers` |
| Device metadata | `devices` |
| Device assignment | `batch_devices` |
| Temperature/humidity/GPS | `telemetry_readings` |
| Supply-chain movement | `batch_events` |
| Current dashboard state | `batch_current_state` projection |
| Alert occurrence | `alerts` |
| Alert rule | `alert_rules` |
| SHA-256 proof | `integrity_records` |
| Fabric submission | `fabric_transactions` |
| User/system action | `audit_events` |
| Original MQTT payload | `telemetry_raw_messages` |

## 6. How the current tables map to the target

This is a conceptual mapping, not an instruction to execute immediately.

| Current table/field | Target destination |
|---|---|
| `batches.product_name`, `product_type` | `products` plus `batches.product_id` |
| `batches.origin`, `destination` | `locations`, referenced by `batches` |
| `batches.quantity` | `quantity_value` and `quantity_unit` in `batches` |
| `sensor_data.temperature`, `humidity`, GPS, gas | `telemetry_readings` |
| `sensor_data.sensor_id` | `devices.device_id` |
| `sensor_data.current_stage` | `supply_chain_stages` and reading/event stage FK |
| `sensor_data.status` | `batches.lifecycle_status` or `batch_events.event_type`, depending on meaning |
| `sensor_data.alert_*` | `alerts` and `alert_rules` |
| `sensor_data.block_hash`, `field_hash` | `integrity_records` |
| `sensor_data.fabric_tx_id` | `fabric_transactions` |
| `sensor_data.replay_record_id`, `telemetry_mode` | ingestion/source metadata |
| `product_registry` | `products` plus `batch_identifiers` |
| `batch_transfers` | `batch_events`, with optional reading reference |
| `users.role` | `roles` and `user_roles` |
| `audit_logs` | `audit_events` |
| `replay_telemetry` | replay dataset/ingestion area, not production telemetry master |

## 7. Timestamp and unit policy

Use one policy everywhere:

- Store timestamps in UTC.
- Use ISO 8601 values with timezone information at the API boundary.
- Keep both `recorded_at` and `received_at` for IoT readings.
- Name unit-bearing columns explicitly: `temperature_c`, `humidity_percent`, `gas_ppm`, `quantity_value`, `quantity_unit`.
- Never compare timestamps stored in multiple free-text formats.
- Never use row insertion order as a substitute for event time.

The ESP32 cadence should be represented by the readings themselves. The dashboard may poll every five seconds, but the database must preserve the actual device `recorded_at` values and must not manufacture readings when a device is silent.

## 8. Indexes and integrity rules

At minimum, the implementation should later add:

```text
telemetry_readings(batch_id, recorded_at DESC)
telemetry_readings(device_id, recorded_at DESC)
telemetry_readings(recorded_at DESC)
batch_events(batch_id, occurred_at DESC)
alerts(status, severity, opened_at DESC)
integrity_records(reading_id)
fabric_transactions(status, submitted_at)
audit_events(actor_user_id, created_at DESC)
```

Constraints should include:

- foreign keys enabled;
- unique `users.username`;
- unique `devices.device_id`;
- unique `batches.batch_id`;
- unique `products.product_code`;
- non-negative quantity;
- humidity between 0 and 100;
- valid stage and lifecycle values;
- `resolved_at` required when alert status is `resolved`;
- no hard delete for telemetry, ledger evidence, alerts, or audit records.

## 9. Migration plan to use later

When implementation is approved, migrate in controlled phases:

1. **Freeze the target model** and agree on canonical names, units, statuses, and role aliases.
2. **Back up the SQLite file** and verify the backup can be restored.
3. **Create new tables beside the old tables**; do not drop old data first.
4. **Create products and batches** from the best available authoritative records.
5. **Create devices and locations** and map legacy text values.
6. **Copy `sensor_data` into telemetry readings**, preserving original row IDs in a legacy reference column.
7. **Convert transfer rows into batch events** and link them to the nearest relevant telemetry reading by batch and time.
8. **Convert hashes and Fabric IDs** into integrity and Fabric transaction records.
9. **Convert generated alert state into alert occurrences** with explicit open/resolved status.
10. **Rebuild dashboard read models** from the normalized tables.
11. **Run reconciliation reports** for orphaned batch IDs, duplicate products, missing devices, invalid timestamps, and conflicting measurements.
12. **Switch APIs one surface at a time**, starting with read-only endpoints.
13. **Keep a read-only legacy view or archive** until all dashboards and verification tools have been validated.

## 10. Reconciliation checks required before cutover

The migration must produce zero unexplained rows for:

- telemetry readings with no batch or device;
- batches with no product;
- transfer events with no batch;
- integrity records with no reading/event;
- Fabric IDs attached to more than one unrelated business record;
- duplicate public batch IDs;
- duplicate device IDs;
- invalid or future timestamps;
- temperature/humidity values outside physical sensor limits;
- role aliases that were not mapped to canonical roles.

## 11. Recommendation

For the current project, SQLite is adequate for one local backend and one ESP32 stream if foreign keys, WAL, indexes, and append-only rules are used correctly. The normalized design above should be implemented in SQLite first if the goal is a local demonstration. PostgreSQL becomes the better choice when multiple backend processes, many devices, concurrent operators, or production deployment are required.

The most important decision is not SQLite versus PostgreSQL. It is establishing one source of truth for each concept and preventing telemetry, workflow, alerts, and blockchain evidence from being stored as interchangeable fields in one table.

---

# 12. Final from-scratch design

The following is the design I would use if I owned this project and had to make it reliable, explainable, and strong enough for a serious demonstration. It is intentionally stricter than the current application because a professional database should prevent bad states instead of trying to repair them later.

## 12.1 The non-negotiable lifecycle

The system has one clear order:

```text
User creates batch
        |
        v
Batch receives its immutable identity
        |
        +--> device is assigned automatically or by an authorized assignment action
        |
        +--> ESP32 starts publishing telemetry for that batch
        |
        +--> backend creates sensor readings and GPS points automatically
        |
        +--> backend creates alerts automatically when rules are violated
        |
        +--> backend creates hash proof and Fabric submission automatically
        |
        +--> users record only business events such as received, moved, checked, delivered
```

### What a user may enter manually

- product selection;
- batch quantity and unit;
- harvest/production date;
- origin and destination selection;
- business workflow action;
- transfer/checkpoint note;
- user account and role administration.

### What a user must never enter manually

- temperature;
- humidity;
- gas reading;
- latitude or longitude;
- sensor timestamp;
- device ID in a transfer form;
- block hash;
- field hash;
- Fabric transaction ID.

Those values must come from the ESP32/MQTT/backend pipeline. A user can report a business event, but cannot pretend to be a sensor or blockchain node.

## 12.2 The table map

### A. Identity, access, and organizations

#### `organizations`

Represents a farm, processor, warehouse company, distributor, retailer, or FoodChain administrator.

```text
organization_id PK
name
organization_type
registration_code UNIQUE
is_active
created_at
```

#### `users`

Represents a human login account.

```text
user_id PK
organization_id FK -> organizations.organization_id
username UNIQUE
display_name
email
password_hash
is_active
created_at
updated_at
last_login_at
```

#### `roles`

Controlled values only: `admin`, `producer`, `distributor`, `manager`, `consumer`.

```text
role_id PK
name UNIQUE
description
```

#### `user_roles`

Allows one person to have more than one controlled permission set.

```text
user_id PK/FK
role_id PK/FK
assigned_by_user_id FK -> users.user_id
assigned_at
```

#### `audit_logs`

Security and administrative history. This is not sensor data.

```text
audit_log_id PK
actor_user_id FK -> users.user_id NULL
action
entity_type
entity_id
result
request_id
ip_address
details_json
created_at
```

Examples: login failure, batch created, user disabled, transfer accepted, alert acknowledged, permission denied.

### B. Product and batch master data

#### `products`

The reusable definition of a product. A product is not a batch.

```text
product_id PK
product_code UNIQUE
name
category
product_type
description
created_at
updated_at
```

#### `units`

Controlled measurement units.

```text
unit_code PK       -- kg, litre, crate, box
quantity_dimension -- mass, volume, count
```

#### `locations`

Known business places selected by users or registered by an administrator.

```text
location_id PK
organization_id FK -> organizations.organization_id NULL
name
location_type     -- farm, warehouse, processing, distributor, retailer
address
latitude NULL
longitude NULL
created_at
```

The location table stores planned/business locations. Live movement coordinates do not belong here; they belong to telemetry GPS points.

#### `batches`

The central aggregate. Every operational table below must ultimately point to a batch.

```text
batch_id PK                 -- public immutable ID, e.g. PROD-BC5F65BF
product_id FK -> products.product_id
owner_organization_id FK -> organizations.organization_id
origin_location_id FK -> locations.location_id
destination_location_id FK -> locations.location_id
quantity_value
unit_code FK -> units.unit_code
harvested_at
status                     -- created, active, delivered, cancelled
created_by_user_id FK -> users.user_id
created_at
updated_at
```

Rules:

1. `batch_id` is generated by the backend and never edited.
2. A batch cannot exist without a product, owner, quantity, and unit.
3. A sensor reading cannot exist without a batch.
4. A ledger proof cannot exist without a batch.
5. A batch can be created before telemetry starts; it is valid to have zero readings initially.

#### `batch_identifiers`

All external identities attached to a batch.

```text
batch_identifier_id PK
batch_id FK -> batches.batch_id
identifier_type       -- qr, product_uid, legacy_id, external_erp
identifier_value
is_primary
created_at
UNIQUE(identifier_type, identifier_value)
```

This prevents `product_uid`, QR values, legacy IDs, and batch IDs from being mixed into one column.

### C. Devices and automatic ingestion

#### `devices`

One row per physical ESP32 or approved simulator.

```text
device_id PK           -- ESP32-01
device_type            -- esp32, gateway, simulator
serial_number UNIQUE
firmware_version
source_type            -- physical, replay, manual_test
device_secret_ref      -- reference to secret storage, never raw secret
is_active
registered_at
last_seen_at
```

#### `batch_device_assignments`

Controls which device is allowed to report for which batch.

```text
assignment_id PK
batch_id FK -> batches.batch_id
device_id FK -> devices.device_id
assigned_by_user_id FK -> users.user_id NULL
assigned_at
unassigned_at NULL
```

This is the gate that enforces the requested behavior: a device becomes a batch sensor only after the batch exists and the assignment is valid.

#### `ingestion_messages`

Stores the original MQTT message for troubleshooting and replay detection.

```text
message_id PK
device_id FK -> devices.device_id NULL
mqtt_topic
message_key UNIQUE
payload_encrypted
payload_hash
received_at
parse_status       -- accepted, rejected, duplicate
error_message NULL
```

The raw message is retained, but dashboards do not query it directly.

#### `sensor_readings`

The normalized, append-only sensor table. This is the only authoritative table for measurements.

```text
reading_id PK
batch_id FK -> batches.batch_id NOT NULL
device_id FK -> devices.device_id NOT NULL
ingestion_message_id FK -> ingestion_messages.message_id NULL
recorded_at        -- device timestamp, UTC
received_at        -- backend timestamp, UTC
temperature_c NULL
humidity_percent NULL
gas_ppm NULL
gps_latitude NULL
gps_longitude NULL
stage_code NULL
quality_status     -- valid, partial, rejected
created_at
```

Database rules:

- `batch_id`, `device_id`, and `received_at` are required.
- Latitude/longitude are optional only when the device did not provide a GPS fix; they are not manually entered.
- Temperature/humidity are accepted only from validated device payloads.
- A reading is never updated to “fix” history; a correction is a new reading or a correction record.
- `recorded_at` and `received_at` are both kept so delayed MQTT messages can be identified.

#### `sensor_health`

One current status row per device, rebuilt from readings.

```text
device_id PK/FK -> devices.device_id
last_reading_id FK -> sensor_readings.reading_id
last_recorded_at
last_received_at
connection_status    -- online, stale, offline
reading_count
updated_at
```

This is a read model for dashboards, not a replacement for the append-only readings.

### D. Transportation and business events

#### `supply_chain_stages`

Controlled stage values:

```text
stage_code PK        -- field, warehouse, transport, retailer, consumer
display_name
sequence_no
```

#### `transport_orders`

Represents a planned movement of a batch.

```text
transport_id PK
batch_id FK -> batches.batch_id
from_location_id FK -> locations.location_id
to_location_id FK -> locations.location_id
assigned_organization_id FK -> organizations.organization_id
planned_departure_at
planned_arrival_at
actual_departure_at NULL
actual_arrival_at NULL
status               -- planned, active, completed, cancelled
created_by_user_id FK -> users.user_id
created_at
```

#### `batch_events`

Represents what happened to a batch operationally. It does not pretend to be a sensor reading.

```text
event_id PK
batch_id FK -> batches.batch_id NOT NULL
transport_id FK -> transport_orders.transport_id NULL
event_type           -- created, received, checkpoint, transferred, delivered, anomaly
stage_code FK -> supply_chain_stages.stage_code
location_id FK -> locations.location_id NULL
related_reading_id FK -> sensor_readings.reading_id NULL
performed_by_user_id FK -> users.user_id NULL
notes
occurred_at
created_at
```

When a distributor records “received,” the event stores who and where. If the backend has a current ESP32 reading at that time, it may link `related_reading_id`. It does not copy temperature, humidity, GPS, or hashes into this table.

#### `batch_current_state`

Fast dashboard projection, rebuilt from events and readings.

```text
batch_id PK/FK -> batches.batch_id
current_stage_code
current_status
latest_event_id
latest_reading_id
latest_device_id
latest_reading_at
open_alert_count
updated_at
```

This table is deliberately a projection. If it becomes wrong, it can be rebuilt from authoritative tables.

### E. Alerts and notifications

#### `alert_rules`

Configuration for threshold and connectivity rules.

```text
rule_id PK
rule_code UNIQUE
name
severity_default
stage_code NULL
temperature_min_c NULL
temperature_max_c NULL
humidity_min_percent NULL
humidity_max_percent NULL
stale_after_seconds NULL
is_enabled
```

#### `alerts`

Each generated alert occurrence.

```text
alert_id PK
rule_id FK -> alert_rules.rule_id
batch_id FK -> batches.batch_id NULL
device_id FK -> devices.device_id NULL
reading_id FK -> sensor_readings.reading_id NULL
severity            -- info, warning, critical
title
message
status              -- open, acknowledged, resolved
opened_at
acknowledged_at NULL
acknowledged_by_user_id FK -> users.user_id NULL
resolved_at NULL
resolved_by_user_id FK -> users.user_id NULL
```

The sidebar badge and the alert panel must use one query:

```text
WHERE status IN ('open', 'acknowledged')
```

Do not use security audit count as the operational alert count.

#### `notifications`

Delivery history for alerts.

```text
notification_id PK
alert_id FK -> alerts.alert_id
recipient_user_id FK -> users.user_id
channel              -- dashboard, email, telegram
delivery_status      -- pending, sent, failed, read
sent_at NULL
read_at NULL
error_message NULL
```

An alert is the fact; a notification is one delivery attempt of that fact.

### F. Hash chain, encryption, and Fabric

#### `record_integrity`

Local cryptographic evidence for a batch-related record.

```text
integrity_id PK
batch_id FK -> batches.batch_id NOT NULL
reading_id FK -> sensor_readings.reading_id NULL
event_id FK -> batch_events.event_id NULL
field_hash
block_hash
previous_block_hash NULL
hash_algorithm       -- SHA-256
verification_status   -- pending, verified, failed
created_at
```

The backend creates this automatically after a valid batch-linked reading or business event is accepted. No user form contains these fields.

#### `fabric_transactions`

Submission and confirmation state for Hyperledger Fabric.

```text
fabric_transaction_id PK
batch_id FK -> batches.batch_id NOT NULL
reading_id FK -> sensor_readings.reading_id NULL
event_id FK -> batch_events.event_id NULL
record_integrity_id FK -> record_integrity.integrity_id NULL
network
channel
chaincode
transaction_status    -- queued, submitted, committed, failed
submitted_at NULL
committed_at NULL
failure_reason NULL
```

Fabric IDs are generated by Fabric and written by the backend. They are never typed by a producer or distributor.

#### `encrypted_records`

Encrypted copies of sensitive raw payloads or personally sensitive values.

```text
encrypted_record_id PK
batch_id FK -> batches.batch_id NULL
source_type           -- mqtt_payload, audit_detail, user_private_data
source_id
encryption_key_ref
ciphertext
nonce
algorithm
created_at
```

Store key references, not encryption keys, in SQLite. Normal dashboard queries should not decrypt this table.

### G. Reporting and observability

#### `system_logs`

Application/runtime logs, separate from user audit history.

```text
system_log_id PK
service_name
level                 -- debug, info, warning, error
message
request_id NULL
exception_type NULL
context_json NULL
created_at
```

#### `reconciliation_runs`

Records database/data-quality checks.

```text
run_id PK
started_at
finished_at NULL
status
checked_rows
error_count
report_json
```

This makes it possible to prove that every reading, event, alert, and ledger proof is linked correctly.

## 12.3 How the complete flow works

### Step 1: Create a batch

The producer selects an existing product, enters quantity/unit and business locations, and submits. The backend creates:

1. `batches`;
2. a `batch_identifiers` row for the QR/public ID;
3. a `batch_events` row with `event_type = created`;
4. an initial `batch_current_state` projection.

No sensor, GPS, or Fabric data is fabricated at this step.

### Step 2: Assign or authorize a device

An authorized service or operator associates `ESP32-01` with the batch in `batch_device_assignments`. This is the only point where a device becomes allowed to publish for that batch.

### Step 3: Receive ESP32 data

For every MQTT message:

1. save the original payload in `ingestion_messages`;
2. validate device identity and assignment;
3. reject or quarantine messages for unknown/unassigned batches;
4. create one `sensor_readings` row;
5. update `sensor_health`;
6. evaluate `alert_rules`;
7. create `alerts` if necessary;
8. create `record_integrity`;
9. queue `fabric_transactions`;
10. update `batch_current_state`.

The user never enters the sensor values manually.

### Step 4: Record transportation

The distributor selects a batch and records a business event such as `received`, `checkpoint`, or `transferred`. The form asks for location/event information and notes only. The backend links the nearest valid reading when appropriate.

### Step 5: Show dashboards

Dashboards read:

- batch identity from `batches`;
- product name from `products`;
- latest measurement from `sensor_readings`;
- device health from `sensor_health`;
- route history from `transport_orders` and `batch_events`;
- alerts from `alerts`;
- proof state from `record_integrity` and `fabric_transactions`;
- user activity from `audit_logs`.

No dashboard needs to guess which copy of temperature, batch status, or Fabric ID is correct.

## 12.4 What should be visible to the administrator

The admin dashboard should have separate views:

| View | Reads from |
|---|---|
| Users and roles | `users`, `roles`, `user_roles`, `organizations` |
| Product catalog | `products`, `units` |
| Batches | `batches`, `batch_identifiers`, `batch_current_state` |
| Live sensor monitor | `devices`, `sensor_health`, latest `sensor_readings` |
| Raw ingestion | `ingestion_messages` |
| Transportation | `transport_orders`, `batch_events`, `locations` |
| Alerts and notifications | `alerts`, `notifications` |
| Hash-chain proof | `record_integrity` |
| Fabric status | `fabric_transactions` |
| Encrypted evidence | `encrypted_records` metadata only |
| User/security logs | `audit_logs` |
| Backend/runtime logs | `system_logs` |
| Data quality | `reconciliation_runs` |

## 12.5 The core design principle

Every table must answer one sentence:

| Table family | Sentence it answers |
|---|---|
| Identity | Who is allowed to do something? |
| Product/batch | What physical food is being tracked? |
| Device/ingestion | Which machine sent this message? |
| Sensor | What did the machine measure, and when? |
| Transportation | What business action happened to the batch? |
| Alerts | What needs attention, and why? |
| Integrity/Fabric | What proof was generated for the record? |
| Encryption | Which sensitive payload is protected? |
| Audit/system logs | Who did what, or what did the software do? |

If a future column cannot be explained by exactly one table's sentence, it probably belongs in another table.

## 12.6 Recommended implementation order after approval

This remains a plan only; no implementation is included in this task.

1. Freeze canonical names, units, statuses, roles, and timestamp policy.
2. Create the new tables beside the current tables.
3. Implement batch creation and verify that every batch gets a public identifier.
4. Implement device assignment validation.
5. Implement automatic MQTT-to-`sensor_readings` ingestion.
6. Implement automatic alert, hash, encryption, and Fabric pipelines.
7. Implement transportation events without copied telemetry columns.
8. Build dashboard read models and reconcile them against source tables.
9. Migrate historical records with a detailed exception report.
10. Switch read APIs gradually.
11. Make old tables read-only during a verification period.
12. Archive old tables only after every reconciliation check passes.

This order protects the most important rule in the system: **a batch is the root of operational truth, and all sensor, transportation, alert, encrypted, and ledger records must be traceable back to that batch.**
