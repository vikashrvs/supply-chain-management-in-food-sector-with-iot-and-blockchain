@echo off
title FoodChain SCM - Startup
color 0A
cls

echo.
echo  ============================================================
echo   FOODCHAIN SCM - VTU Project Startup
echo   IoT + Blockchain Supply Chain Management
echo  ============================================================
echo.

REM -- 0. Cleanup any previous Fabric network -----------------------
start "Fabric Cleanup" wsl -d Ubuntu bash "/mnt/c/Users/raj vikash/Desktop/food_chain/blockchain/stop_fabric.sh"

REM -- 1. Start Hyperledger Fabric via WSL Ubuntu -------------------
echo  [1/4] Starting Hyperledger Fabric (WSL Ubuntu)...
echo        This takes 2-3 minutes. Other services will start after.
echo.
start "Hyperledger Fabric" wsl -d Ubuntu bash "/mnt/c/Users/raj vikash/Desktop/food_chain/blockchain/start_fabric.sh"

echo  [FABRIC] Fabric starting in background (WSL window)...
echo  Waiting 15 seconds for Docker containers to initialize...
ping 127.0.0.1 -n 16 >nul

REM -- 2. Start Mosquitto MQTT Broker ------------------------------
echo  [2/4] Starting MQTT Broker (Mosquitto)...
start "MQTT Broker" cmd /k "color 0B && title MQTT Broker - FoodChain && echo. && echo  [MQTT] Checking Mosquitto MQTT Broker... && echo. && sc query mosquitto | findstr /i state && echo. && echo  [MQTT] Mosquitto service is running on port 1883 && echo  [MQTT] Broker ready for IoT sensor connections"

ping 127.0.0.1 -n 4 >nul

REM -- 3. Start FastAPI Backend -------------------------------------
echo  [3/4] Starting FastAPI Backend...
start "FoodChain Backend" cmd /k "color 0E && title FoodChain Backend - Port 8001 && echo. && echo  [API] FoodChain FastAPI Backend v2.0 && echo  [API] http://127.0.0.1:8001 && echo. && cd /d %~dp0backend && python main.py"

ping 127.0.0.1 -n 5 >nul

REM -- 4. Start IoT Sensor Simulation ------------------------------
echo  [4/4] Starting IoT Sensor Simulation...
start "IoT Sensor Simulation" cmd /k "color 0D && title IoT Sensor - FoodChain && echo. && echo  [IOT] Sensor Simulator - ESP32 DHT22 + GPS && echo  [IOT] Publishing to MQTT: food/sensor/# && echo. && cd /d %~dp0iot_simulation && python sensor_simulation.py"

REM -- Open browser -------------------------------------------------
echo.
echo  Opening dashboard in browser...
ping 127.0.0.1 -n 4 >nul
start http://127.0.0.1:8001/login.html

cls
echo.
echo  ============================================================
echo   ALL SERVICES STARTED
echo  ============================================================
echo.
echo   Hyperledger Fabric  - WSL Ubuntu window
echo   MQTT Broker        - port 1883
echo   FastAPI Backend     - http://127.0.0.1:8001
echo   IoT Sensor Sim      - MQTT publisher
echo.
echo   Login: admin / admin123
echo.
echo   NOTE: Fabric takes 2-3 min to fully start.
echo         Backend auto-detects when Fabric is ready.
echo  ============================================================
echo.
