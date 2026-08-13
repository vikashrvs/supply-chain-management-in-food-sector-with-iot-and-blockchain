@echo off
setlocal EnableExtensions
title FoodChain SCM - Shutdown
color 0C
cls

set "PROJECT_DIR=C:\Users\raj vikash\Desktop\food_chain"
set "WSL_PROJECT_DIR=/mnt/c/Users/raj vikash/Desktop/food_chain"

echo.
echo ============================================================
echo   FOODCHAIN SCM - SHUTDOWN
echo ============================================================
echo.

REM ============================================================
REM 1. STOP BACKEND
REM ============================================================

echo [1/3] Stopping FoodChain FastAPI backend...

taskkill /f /im python.exe >nul 2>&1
taskkill /f /im python3.exe >nul 2>&1

echo [OK] Backend processes stopped.
echo.

REM ============================================================
REM 2. STOP MQTT
REM ============================================================

echo [2/3] Stopping MQTT broker...

taskkill /f /im mosquitto.exe >nul 2>&1

echo [OK] MQTT process stopped.
echo.

REM ============================================================
REM 3. STOP FABRIC - WAIT UNTIL FINISHED
REM ============================================================

echo [3/3] Stopping Hyperledger Fabric...
echo.
echo        Waiting for Fabric containers to stop...
echo.

wsl -d Ubuntu bash "%WSL_PROJECT_DIR%/blockchain/stop_fabric.sh"

if errorlevel 1 (
    echo.
    echo [WARNING] Fabric shutdown returned an error.
    echo          Checking Docker containers...
    echo.
)

echo.
echo [FABRIC] Checking remaining Fabric containers...

wsl -d Ubuntu bash -c "docker ps --format '{{.Names}}' | grep -E 'peer0\.|orderer\.example\.com|dev-peer0\.' || true"

echo.
echo ============================================================
echo   FOODCHAIN SCM STOPPED
echo ============================================================
echo.
echo   Backend       : stopped
echo   MQTT          : stopped
echo   Fabric        : shutdown requested
echo.
echo   You can safely run start.bat for a clean restart.
echo ============================================================
echo.

endlocal

timeout /t 3 /nobreak >nul