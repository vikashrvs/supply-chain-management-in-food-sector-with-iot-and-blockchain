# FoodChain SCM — Database Reference Guide

## Database File Location

```
C:\Users\raj vikash\Desktop\food_chain\backend\food_chain.db
```

## How to Open in DB Browser for SQLite

1. Open **DB Browser for SQLite** (install from https://sqlitebrowser.org if not installed)
2. Click **"Open Database"**
3. Navigate to: `C:\Users\raj vikash\Desktop\food_chain\backend\`
4. Select **`food_chain.db`**
5. Click **"Open"**

> **Quick Tip:** Right-click `food_chain.db` in File Explorer → "Open with" → DB Browser for SQLite → tick "Always use this app". After that, double-clicking opens it instantly.

---

## Tables Overview

| Table | Rows | Purpose |
|---|---|---|
| `sensor_data` | ~4089 | Main IoT sensor readings from ESP32 / simulation |
| `product_registry` | ~3 | Master product catalog (UID to batch mapping) |
| `users` | 3 | Login accounts |

---

## Table 1: sensor_data (Main IoT Table)

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto row ID |
| `timestamp` | TEXT | Reading timestamp (YYYY-MM-DD HH:MM:SS) |
| `temperature` | REAL | Temperature in celsius |
| `humidity` | REAL | Relative humidity % |
| `latitude` | REAL | GPS latitude |
| `longitude` | REAL | GPS longitude |
| `batch_id` | TEXT | Batch identifier (e.g. BATCH_001) |
| `sensor_id` | TEXT | Which sensor sent this (e.g. SENSOR_A) |
| `current_stage` | TEXT | field / warehouse / transport / retailer / consumer |
| `status` | TEXT | In Transit or Delivered |
| `product_name` | TEXT | Product name (e.g. Apples) |
| `product_ref` | INTEGER FK | Links to product_registry.id |
| `block_hash` | TEXT | SHA-256 hash chain (blockchain integrity) |
| `fabric_tx_id` | TEXT | Hyperledger Fabric transaction ID (NULL if offline) |
| `field_hash` | TEXT | Per-record tamper detection fingerprint |

### Useful SQL Queries (paste in DB Browser Execute SQL tab)

```sql
-- Latest 10 sensor readings
SELECT id, timestamp, batch_id, sensor_id, temperature, humidity, current_stage
FROM sensor_data ORDER BY id DESC LIMIT 10;

-- All blockchain-verified readings (have Fabric TX)
SELECT id, timestamp, batch_id, fabric_tx_id, block_hash
FROM sensor_data WHERE fabric_tx_id IS NOT NULL ORDER BY id DESC;

-- Temperature violations (above normal transport range of 12 C)
SELECT id, batch_id, timestamp, temperature, current_stage
FROM sensor_data WHERE temperature > 12 AND current_stage = 'transport'
ORDER BY timestamp DESC;

-- Average temp and humidity per batch
SELECT batch_id, COUNT(*) as readings, ROUND(AVG(temperature),2) as avg_temp, ROUND(AVG(humidity),2) as avg_hum
FROM sensor_data GROUP BY batch_id;
```

---

## Table 2: product_registry

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Used as product_ref in sensor_data |
| `product_uid` | TEXT UNIQUE | e.g. UID-353581A7AE3B |
| `batch_id` | TEXT | e.g. BATCH_001 |
| `product` | TEXT | e.g. Apples |
| `product_name` | TEXT | Full display name |
| `created_at` | TEXT | Registration timestamp |

---

## Table 3: users

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Row ID |
| `username` | TEXT UNIQUE | Login name |
| `password_hash` | TEXT | bcrypt hash (never plain text) |
| `role` | TEXT | admin / farmer / warehouse / retailer / consumer |
| `created_at` | TEXT | Account creation time |

### Default Demo Accounts

| Username | Password | Role |
|---|---|---|
| admin | admin123 | admin |
| farmer | farmer123 | farmer |
| retailer | retail123 | retailer |

---

## How Blockchain Hashing Works

Each sensor reading has:
1. **block_hash** - SHA-256 hash linking this row to the previous (hash chain). Any tampering breaks all future hashes.
2. **field_hash** - SHA-256 fingerprint of just this row's data (tamper detection).
3. **fabric_tx_id** - When Hyperledger Fabric is running, this is the real blockchain transaction ID.

---

## Supply Chain Stage Thresholds (from config.py)

| Stage | Safe Temp (C) | Safe Humidity (%) |
|---|---|---|
| field | 18 - 27 | 65 - 85 |
| warehouse | 4 - 10 | 70 - 90 |
| transport | 5 - 12 | 60 - 80 |
| retailer | 6 - 14 | 55 - 75 |
| consumer | 8 - 16 | 50 - 70 |

Readings outside these ranges are flagged as warning or critical on the dashboard.
