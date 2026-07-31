@echo off
title FoodChain SCM - Shutdown
color 0C
cls

echo.
echo  ============================================================
echo   FOODCHAIN SCM - Stopping All Services
echo  ============================================================
echo.

echo  [1/4] Stopping Hyperledger Fabric (WSL Docker containers)...
start "Fabric Teardown" wsl -d Ubuntu bash "/mnt/c/Users/raj vikash/Desktop/food_chain/blockchain/stop_fabric.sh"
ping 127.0.0.1 -n 4 >nul

echo  [2/4] Stopping Python backend...
taskkill /f /im python.exe >nul 2>&1
taskkill /f /im python3.exe >nul 2>&1

echo  [3/4] Stopping Mosquitto MQTT broker...
taskkill /f /im mosquitto.exe >nul 2>&1

echo  [4/4] Closing FoodChain terminal windows...
taskkill /f /fi "WINDOWTITLE eq MQTT Broker*" >nul 2>&1
taskkill /f /fi "WINDOWTITLE eq FoodChain Backend*" >nul 2>&1
taskkill /f /fi "WINDOWTITLE eq IoT Sensor*" >nul 2>&1
taskkill /f /fi "WINDOWTITLE eq Hyperledger Fabric*" >nul 2>&1
taskkill /f /fi "WINDOWTITLE eq Fabric*" >nul 2>&1

echo.
echo  All FoodChain services stopped (including Hyperledger Fabric).
echo  You can now safely run start.bat for a clean restart.
echo.
ping 127.0.0.1 -n 4 >nul
