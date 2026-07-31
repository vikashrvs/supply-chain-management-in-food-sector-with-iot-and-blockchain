"""
Shared utility functions for the FoodChain backend.
These are used by both database.py and services/edge_health.py,
extracted here to avoid circular imports.
"""

from config import SUPPLY_CHAIN_STAGES


def normalize_stage(raw_stage, raw_status=None):
    """Normalize stage string to a valid supply chain stage."""
    stage = (raw_stage or "").strip().lower()
    if stage in SUPPLY_CHAIN_STAGES:
        return stage
    if (raw_status or "").strip().lower() == "delivered":
        return "consumer"
    return "transport"


def normalize_status(raw_status, current_stage):
    if normalize_stage(current_stage) == "consumer":
        return "Delivered"
    if (raw_status or "").strip().lower() == "delivered":
        return "Delivered"
    return "In Transit"


def normalize_batch_id(batch_id, product_id=None, fallback=None):
    for value in (batch_id, product_id, fallback):
        text = (value or "").strip()
        if text:
            return text
    return "UNKNOWN_BATCH"


def normalize_product_uid(product_uid, batch_id=None, product_id=None, fallback=None):
    for value in (product_uid, batch_id, product_id, fallback):
        text = (value or "").strip()
        if text:
            return text
    return "UNKNOWN_UID"


def normalize_product(product, product_name, batch_id):
    text = (product or product_name or "").strip()
    if text:
        return text
    return f"Product {batch_id}"


def format_stage_label(stage):
    return normalize_stage(stage).replace("_", " ").title()


def format_range(range_tuple, unit=""):
    """Format a (min, max) threshold tuple as a human-readable string.
    Example: format_range((2, 8), ' C') -> '2–8 C'
    """
    lo, hi = range_tuple
    return f"{lo}\u2013{hi}{unit}"


def safe_query(query, params=()):
    """Execute a SELECT query safely, logging any DB errors.
    Returns a list of rows (list of tuples). If the query fails, logs and raises.
    """
    import sqlite3
    from database import get_connection
    conn = get_connection()
    try:
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        return rows
    except sqlite3.Error as e:
        import logging
        logging.error(f"Database error in safe_query: {e}")
        raise




def parse_timestamp(value):
    from datetime import datetime
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None
