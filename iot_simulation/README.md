# FoodChain IoT Sensor System

> Real IoT hardware integration for food supply chain monitoring using ESP32 + DHT22 + GPS

## Architecture

```
ESP32 Sensor Node                Edge Processor              Cloud Backend
┌──────────────────┐     MQTT    ┌─────────────────┐   MQTT   ┌─────────────┐
│ DHT22 (Temp/Hum) │────────────▶│ Raspberry Pi /  │─────────▶│ FastAPI     │
│ NEO-6M (GPS)     │  5s interval│ PC Python       │processed │ + SQLite    │
│ ESP32 DevKit     │             │                 │  data    │ + Dashboard │
│                  │             │ - Threshold     │         │             │
│ Edge Decision:   │             │   validation    │  alerts  │             │
│ OK/WARN/CRITICAL │             │ - Risk scoring  │─────────▶│             │
└──────────────────┘             │ - Alert         │         └─────────────┘
                                 │   escalation    │
                                 └─────────────────┘
```

## Hardware Bill of Materials (BOM)

| Component | Model | Qty | Purpose | Approx Cost |
|---|---|---|---|---|
| Microcontroller | ESP32 DevKit V1 | 1 | WiFi + MQTT + Processing | ₹450 |
| Temp/Humidity Sensor | DHT22 (AM2302) | 1 | Temperature & Humidity | ₹250 |
| GPS Module | NEO-6M | 1 | Location Tracking | ₹350 |
| Buzzer | Active Buzzer 5V | 1 | Alert Notifications | ₹30 |
| LED | Built-in on ESP32 | 1 | Status Indicator | — |
| Breadboard | Half-size | 1 | Prototyping | ₹60 |
| Jumper Wires | M-M, M-F | 10 | Connections | ₹40 |
| USB Cable | Micro-USB | 1 | Power + Programming | ₹50 |
| **Total** | | | | **~₹1,230** |

## Wiring Diagram

```
                    ESP32 DevKit V1
                 ┌──────────────────┐
                 │                  │
    DHT22        │  GPIO4  ◄── DATA │
   ┌─────┐       │  3.3V   ◄── VCC  │
   │     │       │  GND    ◄── GND  │
   └─────┘       │                  │
                 │                  │
    NEO-6M GPS   │  GPIO16 ◄── TX   │  (RX2)
   ┌─────────┐   │  GPIO17 ──▶ RX   │  (TX2)
   │         │   │  3.3V   ◄── VCC  │
   │         │   │  GND    ◄── GND  │
   └─────────┘   │                  │
                 │                  │
    Buzzer       │  GPIO15 ──▶ (+)  │
   ┌─────┐       │  GND    ◄── (-)  │
   │     │       │                  │
   └─────┘       │  GPIO2  = LED    │  (Built-in)
                 │                  │
                 └──────────────────┘
```

## Software Setup

### 1. Arduino IDE Setup (for ESP32 firmware)

1. Install [Arduino IDE 2.x](https://www.arduino.cc/en/software)
2. Add ESP32 board URL: `https://raw.githubusercontent.com/espressif/arduino-esp32/gh-pages/package_esp32_index.json`
3. Install **ESP32** board package from Board Manager
4. Install libraries via Library Manager:
   - `PubSubClient` by Nick O'Leary
   - `DHT sensor library` by Adafruit
   - `TinyGPSPlus` by Mikal Hart
   - `ArduinoJson` by Benoit Blanchon

5. Open `esp32_firmware/foodchain_sensor.ino`
6. Update configuration:
   ```cpp
   const char* WIFI_SSID     = "YourWiFiName";
   const char* WIFI_PASSWORD = "YourWiFiPassword";
   const char* MQTT_SERVER   = "192.168.1.100";  // Your PC's IP
   ```
7. Select board: **ESP32 Dev Module**
8. Upload!

### 2. Edge Processor Setup (Raspberry Pi or PC)

```bash
# Install dependency
pip install paho-mqtt

# Run edge processor
python edge_processor.py
```

### 3. Simulation Mode (No Hardware Required)

```bash
# Install dependency
pip install paho-mqtt

# Run simulation with demo banner
python sensor_simulation.py --mode demo

# Run normal simulation
python sensor_simulation.py
```

## MQTT Topics

| Topic | Publisher | Subscriber | Description |
|---|---|---|---|
| `food/sensor/{batch_id}` | ESP32 / Simulation | Edge Processor | Raw sensor readings |
| `food/processed` | Edge Processor | FastAPI Backend | Processed + risk-scored data |
| `food/alerts` | Edge Processor | Backend / Dashboard | Threshold violation alerts |

## Data Flow

1. **ESP32** reads DHT22 (temperature, humidity) and NEO-6M (GPS lat/lng) every 5 seconds
2. **Edge Computing on ESP32**: Local threshold check → OK / WARNING / CRITICAL
3. **MQTT Publish** to `food/sensor/BATCH_001`
4. **Edge Processor** (Python) subscribes, enriches data with risk scoring
5. **Processed data** forwarded to `food/processed` → Backend stores in SQLite
6. **Alerts** published to `food/alerts` if thresholds violated

## Edge Computing Logic

| Risk Level | Condition | Action |
|---|---|---|
| ✅ Stable | All readings within range | `PROCEED` |
| ⚠️ Warning | One parameter out of range | `CONTINUE_WITH_CAUTION` |
| 🚨 Critical | Two+ parameters out of range | `HOLD_FOR_INSPECTION` |
| 🔥 Escalated | 3+ consecutive alerts | Auto-escalation logged |

## Stage-Specific Thresholds

| Stage | Temperature (°C) | Humidity (%) |
|---|---|---|
| Field | 18 – 27 | 65 – 85 |
| Warehouse | 4 – 10 | 70 – 90 |
| Transport | 5 – 12 | 60 – 80 |
| Retailer | 6 – 14 | 55 – 75 |
| Consumer | 8 – 16 | 50 – 70 |

## File Structure

```
iot_simulation/
├── esp32_firmware/
│   └── foodchain_sensor.ino   # Real ESP32 Arduino firmware
├── edge_processor.py          # Edge computing MQTT processor
├── sensor_simulation.py       # Python simulation (no hardware)
└── README.md                  # This file
```
