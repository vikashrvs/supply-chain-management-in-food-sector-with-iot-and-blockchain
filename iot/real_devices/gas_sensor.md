# MQ Gas Sensor Wiring Guide

## Overview
MQ series gas sensors (such as MQ-2, MQ-135, or MQ-5) detect air quality, gas leakage, methane, smoke, and environmental pollutants.

---

## Pinout Connections for ESP32

| MQ Gas Sensor Pin | ESP32 Pin | Notes |
| :--- | :--- | :--- |
| **VCC** | **5V** (VIN) | Needs 5V power supply for internal heater element |
| **GND** | **GND** | Ground |
| **AO** (Analog Out) | **GPIO 34** | ESP32 ADC1 Pin (Input-only ADC pin, safe for 0-3.3V analog output) |
| **DO** (Digital Out) | *Optional / Unused* | Digital threshold output (Adjustable via onboard potentiometer) |

> **Note**: Allow 2-3 minutes of warm-up time after powering on the gas sensor for accurate analog readings.

---

## Technical Specifications
- **Operating Voltage**: 5V DC
- **Analog Output Range**: 0V to ~3.3V (ADC raw 0 to 4095 on ESP32)
- **Mapped FoodChain Scale**: 0 to 500 gas quality index units (Values > 250 trigger FoodChain anomaly alert)
