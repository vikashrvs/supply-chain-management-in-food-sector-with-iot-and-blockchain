"""
Configuration constants for the FoodChain backend.
Extracted from the monolithic main.py for modular architecture.
"""

import os
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "food_chain.db"
FRONTEND_DIR = BASE_DIR.parent / "frontend"

# ── Supply Chain Constants ───────────────────────────────────────────────────
SUPPLY_CHAIN_STAGES = ["field", "warehouse", "transport", "retailer", "consumer"]
STALE_BATCH_HOURS = 6
RISK_PRIORITY = {"stable": 0, "warning": 1, "critical": 2}

STAGE_THRESHOLDS = {
    "field": {
        "temperature": (18.0, 27.0),
        "humidity": (65.0, 85.0),
        "healthy_note": "Freshly harvested stock is within the expected farm range.",
    },
    "warehouse": {
        "temperature": (4.0, 10.0),
        "humidity": (70.0, 90.0),
        "healthy_note": "Cold storage conditions are stable for warehouse holding.",
    },
    "transport": {
        "temperature": (5.0, 12.0),
        "humidity": (60.0, 80.0),
        "healthy_note": "Transit conditions are stable for refrigerated movement.",
    },
    "retailer": {
        "temperature": (6.0, 14.0),
        "humidity": (55.0, 75.0),
        "healthy_note": "Retail shelf conditions are within the expected range.",
    },
    "consumer": {
        "temperature": (8.0, 16.0),
        "humidity": (50.0, 70.0),
        "healthy_note": "The product has reached the consumer stage with acceptable readings.",
    },
}

# ── JWT Authentication Settings ──────────────────────────────────────────────
SECRET_KEY = os.environ.get("FOODCHAIN_SECRET_KEY", "foodchain-vtu-project-secret-key-2026")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 480

# ── SQL Expression Constants ────────────────────────────────────────────────
BATCH_KEY_EXPR = (
    "COALESCE(pr.batch_id, sd.batch_id, sd.product_id, "
    "printf('LEGACY_BATCH_%03d', sd.id))"
)
UID_KEY_EXPR = (
    "COALESCE(pr.product_uid, NULLIF(sd.product_uid, ''), NULLIF(sd.batch_id, ''), "
    "NULLIF(sd.product_id, ''), printf('UID_%03d', sd.id))"
)
PRODUCT_NAME_EXPR = (
    "COALESCE(pr.product_name, pr.product, sd.product_name, sd.product, "
    "printf('Product %s', "
    "COALESCE(pr.batch_id, sd.batch_id, sd.product_id, printf('LEGACY_BATCH_%03d', sd.id))))"
)
PRODUCT_EXPR = (
    "COALESCE(pr.product, pr.product_name, sd.product, sd.product_name, "
    "printf('Product %s', "
    "COALESCE(pr.batch_id, sd.batch_id, sd.product_id, printf('LEGACY_BATCH_%03d', sd.id))))"
)
CURRENT_STAGE_EXPR = (
    "COALESCE(NULLIF(sd.current_stage, ''), "
    "CASE WHEN LOWER(COALESCE(sd.status, '')) = 'delivered' THEN 'consumer' ELSE 'transport' END)"
)

RECORD_SELECT = f"""
    SELECT
        sd.id,
        sd.timestamp,
        sd.temperature,
        sd.humidity,
        sd.latitude,
        sd.longitude,
        sd.product_id,
        sd.status,
        sd.block_hash,
        sd.fabric_tx_id,
        sd.field_hash,
        {BATCH_KEY_EXPR} AS batch_id,
        {UID_KEY_EXPR} AS product_uid,
        {PRODUCT_EXPR} AS product,
        {PRODUCT_NAME_EXPR} AS product_name,
        COALESCE(sd.sensor_id, 'UNKNOWN_SENSOR') AS sensor_id,
        {CURRENT_STAGE_EXPR} AS current_stage,
        COALESCE(sd.product_ref, pr.id) AS product_ref
    FROM sensor_data sd
    LEFT JOIN product_registry pr ON pr.id = sd.product_ref
"""
