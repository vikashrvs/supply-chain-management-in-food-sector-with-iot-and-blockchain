# DHT22 Temperature & Humidity Sensor Wiring Guide

## Overview
The **DHT22** (AM2302) is a capacitive humidity and thermistor sensor that provides digital readings of ambient temperature and relative humidity.

---

## Pinout Connections for ESP32

| DHT22 Pin | ESP32 Pin | Wire Color / Notes |
| :--- | :--- | :--- |
| **VCC** (Pin 1) | **3.3V** or **5V** | Power (Use 3.3V or 5V rail) |
| **DATA** (Pin 2) | **GPIO 4** | Digital Data Pin (10k pull-up resistor recommended between DATA and VCC) |
| **NC** (Pin 3) | *Not Connected* | Leave unconnected |
| **GND** (Pin 4) | **GND** | Ground |

---

## Technical Specifications
- **Operating Voltage**: 3.3V to 5V DC
- **Temperature Measurement Range**: -40°C to +80°C (Accuracy ±0.5°C)
- **Humidity Measurement Range**: 0% to 100% RH (Accuracy ±2-5%)
- **Sampling Rate**: 0.5 Hz (Once every 2 seconds)

---

## Arduino Libraries Required
In Arduino IDE, install via **Sketch -> Include Library -> Manage Libraries**:
1. `DHT sensor library` by Adafruit
2. `Adafruit Unified Sensor`
