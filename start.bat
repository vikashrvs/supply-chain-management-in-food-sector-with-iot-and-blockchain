@echo off
setlocal EnableExtensions
title FoodChain SCM - Startup
color 0A
cls

set "PROJECT_DIR=C:\Users\raj vikash\Desktop\food_chain"
set "WSL_PROJECT_DIR=/mnt/c/Users/raj vikash/Desktop/food_chain"
set "BACKEND_DIR=%PROJECT_DIR%\backend"
set "PYTHON=%PROJECT_DIR%\.venv_win\Scripts\python.exe"
set "LOGIN_URL=http://127.0.0.1:8001/login.html"

echo.
echo ============================================================
echo   FOODCHAIN SCM - VTU PROJECT STARTUP
echo   IoT + Blockchain Supply Chain Management
echo ============================================================
echo.

REM ============================================================
REM 1. CLEAN OLD SERVICES
REM ============================================================

echo [1/4] Cleaning previous FoodChain services...
echo.

REM Stop old backend processes
taskkill /f /im python.exe >nul 2>&1
taskkill /f /im python3.exe >nul 2>&1

REM Stop old MQTT process if manually started
taskkill /f /im mosquitto.exe >nul 2>&1

echo [OK] Old Windows services cleaned.
echo.

REM ============================================================
REM 2. STOP OLD FABRIC NETWORK - WAIT UNTIL FINISHED
REM ============================================================

echo [2/4] Cleaning previous Hyperledger Fabric network...
echo.
echo        Please wait...
echo.

wsl -d Ubuntu bash "%WSL_PROJECT_DIR%/blockchain/stop_fabric.sh"

if errorlevel 1 (
    echo.
    echo [WARNING] Fabric cleanup returned an error.
    echo          Continuing with startup...
    echo.
)

echo [OK] Fabric cleanup completed.
echo.

REM ============================================================
REM 3. START FABRIC
REM ============================================================

echo [3/4] Starting Hyperledger Fabric...
echo.
echo        Channel:   foodchainchannel
echo        Chaincode: foodchain
echo        Peer0:     localhost:7051
echo        Peer1:     localhost:9051
echo.
echo        Fabric may take 1-3 minutes.
echo.

start "Hyperledger Fabric" wsl -d Ubuntu bash "%WSL_PROJECT_DIR%/blockchain/start_fabric.sh"

REM Wait for Fabric peer port 7051
echo [FABRIC] Waiting for peer0.org1 on port 7051...

set /a FABRIC_WAIT=0

:FABRIC_CHECK
powershell -NoProfile -Command "$c=Get-NetTCPConnection -LocalPort 7051 -State Listen -ErrorAction SilentlyContinue; if($c){exit 0}else{exit 1}" >nul 2>&1

if not errorlevel 1 (
    echo [FABRIC] Peer0 port 7051 is available.
    goto FABRIC_READY
)

set /a FABRIC_WAIT+=5

if %FABRIC_WAIT% GEQ 180 (
    echo.
    echo [WARNING] Fabric did not become ready within 180 seconds.
    echo          Check the Hyperledger Fabric window.
    echo.
    goto START_BACKEND
)

echo        Waiting... %FABRIC_WAIT% seconds
timeout /t 5 /nobreak >nul
goto FABRIC_CHECK


:FABRIC_READY

echo.
echo [FABRIC] Network is reachable.
echo [FABRIC] Continuing with backend startup...
echo.

REM ============================================================
REM 4. START MQTT + BACKEND
REM ============================================================

:START_BACKEND

echo [4/4] Starting MQTT Broker...
echo.

start "MQTT Broker" cmd /k "color 0B && title MQTT Broker - FoodChain && echo. && echo [MQTT] FoodChain MQTT Broker && echo. && sc query mosquitto && echo. && echo [MQTT] Broker should be available on port 1883"

timeout /t 2 /nobreak >nul

echo.
echo [API] Starting FoodChain FastAPI Backend...
echo.

if not exist "%PYTHON%" (
    echo.
    echo [ERROR] Python virtual environment not found:
    echo        %PYTHON%
    echo.
    pause
    exit /b 1
)

start "FoodChain Backend" cmd /k "color 0E && title FoodChain Backend - Port 8001 && echo. && echo [API] FoodChain FastAPI Backend v2.0 && echo [API] http://127.0.0.1:8001 && echo. && cd /d "%BACKEND_DIR%" && "%PYTHON%" main.py"

REM ============================================================
REM WAIT FOR FASTAPI BEFORE OPENING BROWSER
REM ============================================================

echo.
echo [API] Waiting for FastAPI to become ready...
echo        Browser will NOT open until port 8001 responds.
echo.

set /a API_WAIT=0

:API_CHECK

powershell -NoProfile -Command "$c=Get-NetTCPConnection -LocalPort 8001 -State Listen -ErrorAction SilentlyContinue; if($c){exit 0}else{exit 1}" >nul 2>&1

if not errorlevel 1 (
    echo [API] Port 8001 is listening.
    goto API_READY
)

set /a API_WAIT+=2

if %API_WAIT% GEQ 60 (
    echo.
    echo [WARNING] Backend did not start within 60 seconds.
    echo          Check the FoodChain Backend window.
    echo.
    goto FINISH
)

echo        Waiting... %API_WAIT% seconds
timeout /t 2 /nobreak >nul
goto API_CHECK


:API_READY

REM Give Uvicorn a moment to finish application startup
echo [API] Waiting for application startup...
timeout /t 3 /nobreak >nul

echo.
echo [OK] FastAPI backend is ready.
echo [OK] Opening FoodChain login page...
echo.

start "" "%LOGIN_URL%"

:FINISH

echo.
echo ============================================================
echo   FOODCHAIN SCM SERVICES
echo ============================================================
echo.
echo   Hyperledger Fabric : WSL Ubuntu
echo   Channel            : foodchainchannel
echo   Chaincode          : foodchain
echo   Peer0              : localhost:7051
echo   Peer1              : localhost:9051
echo   Orderer            : localhost:7050
echo.
echo   MQTT               : port 1883
echo   FastAPI            : http://127.0.0.1:8001
echo   Dashboard          : http://127.0.0.1:8001/login.html
echo.
echo   Demo Telemetry     : Dashboard controlled replay
echo.
echo ============================================================
echo.
echo   Login: admin / admin123
echo.
echo   Keep the Fabric / Backend windows open while using demo.
echo ============================================================
echo.

endlocal