"""
SHA-256 hash chain functions for blockchain integrity verification.
Extracted from main.py.
"""

import hashlib


def get_last_hash(batch_id):
    """Get SHA256 hash of last record for this batch (for chaining)."""
    # Import here to avoid circular dependency (database imports hash_chain)
    from database import get_connection

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT block_hash FROM sensor_readings WHERE batch_id = ? ORDER BY id DESC LIMIT 1",
            (batch_id,),
        )
        row = cursor.fetchone()
        return row["block_hash"] if row and row["block_hash"] else "0" * 64


def compute_block_hash(record, prev_hash):
    data = (
        f"{prev_hash}"
        f"{record['batch_id']}"
        f"{record['timestamp']}"
        f"{record['temperature']}"
        f"{record['humidity']}"
        f"{record['current_stage']}"
    )
    return hashlib.sha256(data.encode()).hexdigest()


def compute_record_hash(record):
    """SHA-256 fingerprint of this record's key field values.
    
    Unlike block_hash (which chains records together), this is an
    independent integrity stamp for each individual row. If anyone
    edits temperature/humidity/batch_id directly in the database,
    this hash will no longer match — proving tampering.
    """
    data = "|".join([
        str(record.get("batch_id", "")),
        str(record.get("product_uid", "")),
        str(record.get("temperature", "")),
        str(record.get("humidity", "")),
        str(record.get("current_stage", "")),
        str(record.get("product_name", "")),
        str(record.get("timestamp", "")),
        str(record.get("sensor_id", "")),
    ])
    return hashlib.sha256(data.encode()).hexdigest()
