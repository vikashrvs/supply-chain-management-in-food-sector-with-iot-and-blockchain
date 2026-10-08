@echo off
title FoodChain Integrity Verifier

cd /d "%~dp0"

echo.
echo ============================================================
echo              FOODCHAIN INTEGRITY VERIFIER
echo ============================================================
echo.

python verify_foodchain.py

echo.
echo ============================================================
echo Verification finished.
echo ============================================================
echo.

pause