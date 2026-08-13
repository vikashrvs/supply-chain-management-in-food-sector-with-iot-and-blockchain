"""
FoodChain API v2.0 — Slim entry point.
Supply Chain Management with IoT & Blockchain.
"""

import sys
import logging
from pathlib import Path
from contextlib import asynccontextmanager

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from config import FRONTEND_DIR
from database import init_db
from auth import router as auth_router
from routes.sensor import router as sensor_router
from routes.tracking import router as tracking_router
from routes.blockchain import router as blockchain_router
from routes.replay import router as replay_router
from mqtt_handler import setup_mqtt, shutdown_mqtt
from services.fabric_client import check_fabric_connection, start_background_checker

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

# ----------------------
# KPI endpoint (aggregated dashboard metrics)
# ----------------------
from schemas import KPIs
from utils import safe_query
import sqlite3

@app.get("/api/kpis", response_model=KPIs)
def get_kpis():
    """Return aggregated KPI numbers for the dashboard.
    Derives all KPIs from the sensor_data table.
    """
    try:
        # total unique batches tracked
        total_batches = safe_query(
            "SELECT COUNT(DISTINCT COALESCE(NULLIF(batch_id,''), product_id, 'UNKNOWN')) FROM sensor_data"
        )[0][0]
        # total sensor records
        total_sensors = safe_query("SELECT COUNT(*) FROM sensor_data")[0][0]
        # active shipments = batches not yet at consumer stage
        active_shipments = safe_query(
            "SELECT COUNT(DISTINCT COALESCE(NULLIF(batch_id,''), product_id)) FROM sensor_data WHERE current_stage != 'consumer'"
        )[0][0]
        # blockchain transactions = records with a block hash
        blockchain_tx = safe_query(
            "SELECT COUNT(*) FROM sensor_data WHERE block_hash IS NOT NULL AND block_hash != ''"
        )[0][0]
        # alerts today = readings with temperature out of broad safe range
        alerts_today = safe_query(
            "SELECT COUNT(*) FROM sensor_data WHERE date(timestamp) = date('now') AND (temperature > 30 OR temperature < 0 OR humidity > 95 OR humidity < 10)"
        )[0][0]
        # healthy shipments = latest reading per batch has temp in normal range
        healthy_shipments = safe_query(
            """SELECT COUNT(*) FROM (
                SELECT batch_id, temperature, humidity
                FROM sensor_data
                WHERE id IN (SELECT MAX(id) FROM sensor_data GROUP BY COALESCE(NULLIF(batch_id,''), product_id))
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
        import logging
        logging.getLogger('foodchain').error(f'KPI query error: {e}')
        raise HTTPException(status_code=500, detail=f"Database error fetching KPIs: {e}")
app.include_router(sensor_router)
app.include_router(tracking_router)
app.include_router(blockchain_router)
app.include_router(replay_router)


@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse(url="/login.html")


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)
