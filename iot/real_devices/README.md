# Real Devices — ESP32 Hardware Integration

This directory contains the production Arduino C++ firmware and pinout wiring guides for connecting your physical ESP32 microcontroller with **DHT22** and **NEO-6M GPS** sensors to the FoodChain SCM system.

---

## Architecture Flow

```text
Physical ESP32 + DHT22 (GPIO 13) + NEO-6M GPS (GPIO 25/26) 
  └─► MQTT (Port 1883, Topic: food/sensor/ESP32-01) 
       └─► FastAPI Backend (backend/mqtt_handler.py) 
            └─► SQLite (sensor_data table) & Hyperledger Fabric Blockchain 
                 └─► FoodChain Real-Time Dashboard & Public Tracking
```

---

## Hardware Pinout Mapping

| Sensor | Sensor Pin | ESP32 GPIO Pin | Wire Color / Notes |
| :--- | :--- | :--- | :--- |
| **DHT22** | DATA (Pin 2) | **GPIO 13** | Digital Temp & Humidity |
| **DHT22** | VCC / GND | 3.3V or 5V / GND | Power |
| **NEO-6M GPS** | TX (Transmit) | **GPIO 25** (RX2) | Connect GPS TX to ESP32 RX2 |
| **NEO-6M GPS** | RX (Receive) | **GPIO 26** (TX2) | Connect GPS RX to ESP32 TX2 |
| **NEO-6M GPS** | VCC / GND | 3.3V or 5V / GND | Power |

---

## Quick Setup Steps

1. **Install Arduino IDE Libraries**:
   - `PubSubClient` (Nick O'Leary)
   - `DHT sensor library` (Adafruit)
   - `Adafruit Unified Sensor`
   - `TinyGPS++` (Mikal Hart)
   - `ArduinoJson` (Benoit Blanchon v6/v7)

2. **Configure Firmware**:
   - Open [`esp32_controller/esp32_controller.ino`](file:///c:/Users/raj%20vikash/Desktop/food_chain/iot/real_devices/esp32_controller/esp32_controller.ino) in Arduino IDE.
   - Update `WIFI_SSID` and `WIFI_PASSWORD`.
   - Broker IP is set to `192.168.0.101` and transmit interval set to `30000` ms (30s demo interval).

3. **Upload Firmware**:
   - Select Board: `ESP32 Dev Module`
   - Port: Select your ESP32 COM port
   - Upload & Open Serial Monitor (`115200` baud).
