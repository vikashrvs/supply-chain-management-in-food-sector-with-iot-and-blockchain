# NEO-6M / NEO-8M GPS Module Wiring Guide

## Overview
The NEO-6M/NEO-8M GPS module provides real-time latitude, longitude, altitude, and timestamp via NMEA serial stream over UART.

---

## Pinout Connections for ESP32

| GPS Pin | ESP32 Pin | Wire Color / Notes |
| :--- | :--- | :--- |
| **VCC** | **3.3V** or **5V** | Power rail |
| **GND** | **GND** | Ground |
| **TX** (GPS Transmit) | **GPIO 16** (RX2) | Connect GPS TX to ESP32 RX2 (UART Hardware Serial 2) |
| **RX** (GPS Receive) | **GPIO 17** (TX2) | Connect GPS RX to ESP32 TX2 (UART Hardware Serial 2) |

---

## Operating Notes
- **Outdoors / Window Access**: Satellite lock requires clear view of the sky. Red LED on the GPS board blinks when satellite fix is established.
- **Baud Rate**: Default `9600` baud.
- **Fallback Coordinates**: If indoors or satellite signal is unavailable, the firmware safely falls back to default coordinates (`12.971599, 77.594566`).

---

## Arduino Library Required
In Arduino IDE, install via **Sketch -> Include Library -> Manage Libraries**:
- `TinyGPS++` by Mikal Hart
