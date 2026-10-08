"""
FoodChain API v2.0 — Slim entry point.
Supply Chain Management with IoT & Blockchain.
"""

import sys
import logging
import sqlite3
from pathlib import Path
from contextlib import asynccontextmanager

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from config import FRONTEND_DIR
from database import init_db
from auth import router as auth_router
from routes.sensor import router as sensor_router
from routes.tracking import router as tracking_router
from routes.blockchain import router as blockchain_router
from routes.blockchain_explorer import router as blockchain_explorer_router
from routes.admin import router as admin_router
from routes.producer import router as producer_router
from routes.distributor import router as distributor_router
from routes.consumer import router as consumer_router
from routes.business import router as business_router
from mqtt_handler import setup_mqtt, shutdown_mqtt
from services.fabric_client import check_fabric_connection, start_background_checker
from routes.replay import router as replay_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)-20s │ %(levelname)-7s │ %(message)s",
)
logger = logging.getLogger("foodchain")


@asynccontextmanager
async def lifespan(app):
    """Modern FastAPI lifespan — replaces deprecated @app.on_event decorators."""
    init_db()
    setup_mqtt(app)
    check_fabric_connection()          # Initial check on startup
    start_background_checker(30)       # Re-check every 30s (auto-detects WSL Fabric)
    logger.info("FoodChain API v2.0 started on port 8001")
    yield
    shutdown_mqtt(app)
    logger.info("FoodChain API v2.0 shutting down")


app = FastAPI(
    title="FoodChain API",
    description="Supply Chain Management with IoT & Blockchain",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8001", "http://127.0.0.1:8001"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(producer_router)
app.include_router(distributor_router)
app.include_router(consumer_router)
app.include_router(business_router)

# ----------------------
# KPI endpoint (aggregated dashboard metrics)
# ----------------------
from schemas import KPIs
from utils import safe_query

@app.get("/api/kpis", response_model=KPIs)
def get_kpis():
    """Return aggregated KPI numbers for the dashboard.
    Derives all KPIs from the sensor_readings table.
    """
    try:
        # total batches = real batches registered in the batches table
        total_batches = safe_query("SELECT COUNT(*) FROM batches")[0][0]
        # total sensor records
        total_sensors = safe_query("SELECT COUNT(*) FROM sensor_readings")[0][0]
        # active shipments = batches still created / in transit (same rule as business dashboard)
        active_shipments = safe_query(
            "SELECT COUNT(*) FROM batches WHERE status IN ('created', 'in_transit')"
        )[0][0]
        # blockchain transactions = records with a block hash
        blockchain_tx = safe_query(
            "SELECT COUNT(*) FROM sensor_readings WHERE block_hash IS NOT NULL AND block_hash != ''"
        )[0][0]
        # alerts today = readings with temperature out of broad safe range
        alerts_today = safe_query(
            "SELECT COUNT(*) FROM sensor_readings WHERE date(timestamp) = date('now') AND (temperature > 30 OR temperature < 0 OR humidity > 95 OR humidity < 10)"
        )[0][0]
        # healthy shipments = real batches whose latest reading is in normal range
        healthy_shipments = safe_query(
            """SELECT COUNT(*) FROM (
                SELECT batch_id, temperature, humidity
                FROM sensor_readings
                WHERE id IN (
                    SELECT MAX(id) FROM sensor_readings
                    WHERE COALESCE(NULLIF(batch_id,''), product_id) IN (SELECT batch_id FROM batches)
                    GROUP BY COALESCE(NULLIF(batch_id,''), product_id)
                )
            ) WHERE temperature BETWEEN 0 AND 30 AND humidity BETWEEN 10 AND 95"""
        )[0][0]
        return KPIs(
            total_batches=total_batches,
            total_sensors=total_sensors,
            active_shipments=active_shipments,
            blockchain_transactions=blockchain_tx,
            alerts_today=alerts_today,
            healthy_shipments=healthy_shipments,
        )
    except sqlite3.Error as e:
        logger.error(f'KPI query error: {e}')
        raise HTTPException(status_code=500, detail=f"Database error fetching KPIs: {e}")

app.include_router(sensor_router)
app.include_router(tracking_router)
app.include_router(blockchain_router)
app.include_router(blockchain_explorer_router)
app.include_router(replay_router)


@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse(url="/home.html")


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)
