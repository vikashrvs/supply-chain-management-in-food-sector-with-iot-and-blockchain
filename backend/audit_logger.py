"""
Structured audit logger for FoodChain backend.
Writes security and operational events to the audit_logs table.
Never logs passwords, tokens, or secrets.
"""

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional
from config import DB_PATH

logger = logging.getLogger("foodchain.audit")


def log_event(
    event_type: str,
    result: str,
    user: Optional[str] = None,
    role: Optional[str] = None,
    batch_id: Optional[str] = None,
    detail: Optional[str] = None,
    ip_address: Optional[str] = None,
    request_id: Optional[str] = None,
):
    """
    Write a structured audit event to the audit_logs table and Python log.

    event_type  : LOGIN | AUTH_FAILURE | BATCH_CREATE | BATCH_TRANSFER |
                  CHECKPOINT_CREATE | ANOMALY_FLAG | AI_ANALYSIS |
                  ADMIN_USER_CHANGE | UNAUTHORIZED | SENSOR_SUBMIT | ...
    result      : SUCCESS | FAILURE | DENIED | ERROR
    user        : username (never password/token)
    role        : user role
    batch_id    : relevant batch ID if applicable
    detail      : short human-readable description
    ip_address  : client IP address
    request_id  : correlation / trace ID
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Log to Python logger at appropriate level
    level = logging.WARNING if result in ("FAILURE", "DENIED", "ERROR") else logging.INFO
    logger.log(
        level,
        "AUDIT | event=%-20s | result=%-8s | user=%-15s | role=%-12s | batch=%-12s | %s",
        event_type,
        result,
        user or "-",
        role or "-",
        batch_id or "-",
        detail or "",
    )

    # Write to DB — best-effort, never crash the main request
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            INSERT INTO audit_logs (
                timestamp, event_type, result, username, role,
                batch_id, detail, ip_address, request_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                event_type,
                result,
                user,
                role,
                batch_id,
                detail,
                ip_address,
                request_id,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.error("audit_logger: DB write failed: %s", exc)
