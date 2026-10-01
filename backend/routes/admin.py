"""
Admin-only API routes for FoodChain.
All endpoints require the 'admin' role.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
import sqlite3

from auth import require_role, get_current_user
from database import get_connection, pwd_context, fetch_latest_sensor_row, enrich_transfer_with_sensor
from schemas import UserCreate, UserUpdate, UserResponse, AuditLogEntry
from audit_logger import log_event

logger = logging.getLogger("foodchain.admin")
router = APIRouter(prefix="/api/admin", tags=["Admin"])


# ── User Management ───────────────────────────────────────────────────────────

@router.get("/users", response_model=List[UserResponse])
def list_users(
    role: Optional[str] = Query(default=None),
    user: dict = Depends(require_role("admin")),
):
    """List all users. Admin only."""
    with get_connection() as conn:
        cursor = conn.cursor()
        if role:
            cursor.execute(
                "SELECT id, username, role, is_active, created_at FROM users WHERE role = ? ORDER BY created_at DESC",
                (role,),
            )
        else:
            cursor.execute(
                "SELECT id, username, role, is_active, created_at FROM users ORDER BY created_at DESC"
            )
        rows = cursor.fetchall()
    return [
        UserResponse(
            id=r["id"],
            username=r["username"],
            role=r["role"],
            is_active=bool(r["is_active"]),
            created_at=r["created_at"] or "",
        )
        for r in rows
    ]


@router.post("/users", response_model=UserResponse, status_code=201)
def create_user(
    payload: UserCreate,
    req: Request,
    user: dict = Depends(require_role("admin")),
):
    """Create a new user. Admin only."""
    password_hash = pwd_context.hash(payload.password)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (username, password_hash, role, is_active, created_at) VALUES (?, ?, ?, 1, ?)",
                (payload.username, password_hash, payload.role, now),
            )
            new_id = cursor.lastrowid
            conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"Username '{payload.username}' already exists.")

    log_event("ADMIN_USER_CHANGE", "SUCCESS", user=user["username"], role=user["role"],
              detail=f"Created user '{payload.username}' with role '{payload.role}'",
              ip_address=req.client.host if req.client else None)
    logger.info("ADMIN | user=%s created by admin=%s", payload.username, user["username"])

    return UserResponse(
        id=new_id,
        username=payload.username,
        role=payload.role,
        is_active=True,
        created_at=now,
    )


@router.put("/users/{user_id}", response_model=UserResponse)
def update_user(
    user_id: int,
    payload: UserUpdate,
    req: Request,
    user: dict = Depends(require_role("admin")),
):
    """Update user role or active status. Admin only."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, role, is_active, created_at FROM users WHERE id = ?", (user_id,))
        target = cursor.fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="User not found.")

        updates = []
        params = []
        if payload.role is not None:
            updates.append("role = ?")
            params.append(payload.role)
        if payload.is_active is not None:
            updates.append("is_active = ?")
            params.append(1 if payload.is_active else 0)

        if updates:
            params.append(user_id)
            cursor.execute(f"UPDATE users SET {', '.join(updates)} WHERE id = ?", params)
            conn.commit()

        cursor.execute("SELECT id, username, role, is_active, created_at FROM users WHERE id = ?", (user_id,))
        updated = cursor.fetchone()

    log_event("ADMIN_USER_CHANGE", "SUCCESS", user=user["username"], role=user["role"],
              detail=f"Updated user '{target['username']}': {payload.model_dump(exclude_none=True)}",
              ip_address=req.client.host if req.client else None)

    return UserResponse(
        id=updated["id"],
        username=updated["username"],
        role=updated["role"],
        is_active=bool(updated["is_active"]),
        created_at=updated["created_at"] or "",
    )


@router.delete("/users/{user_id}", status_code=204)
def deactivate_user(
    user_id: int,
    req: Request,
    user: dict = Depends(require_role("admin")),
):
    """Deactivate a user (soft delete). Admin only."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, username FROM users WHERE id = ?", (user_id,))
        target = cursor.fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="User not found.")
        if target["id"] == user.get("user_id"):
            raise HTTPException(status_code=400, detail="Cannot deactivate your own account.")
        cursor.execute("UPDATE users SET is_active = 0 WHERE id = ?", (user_id,))
        conn.commit()

    log_event("ADMIN_USER_CHANGE", "SUCCESS", user=user["username"], role=user["role"],
              detail=f"Deactivated user '{target['username']}'",
              ip_address=req.client.host if req.client else None)


# ── Audit Logs ────────────────────────────────────────────────────────────────

@router.get("/audit-logs", response_model=List[AuditLogEntry])
def get_audit_logs(
    limit: int = Query(default=100, ge=1, le=500),
    event_type: Optional[str] = Query(default=None),
    result: Optional[str] = Query(default=None),
    user: dict = Depends(require_role("admin")),
):
    """Retrieve audit log entries. Admin only."""
    with get_connection() as conn:
        cursor = conn.cursor()
        where_clauses = []
        params = []
        if event_type:
            where_clauses.append("event_type = ?")
            params.append(event_type)
        if result:
            where_clauses.append("result = ?")
            params.append(result)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        params.append(limit)
        cursor.execute(
            f"SELECT * FROM audit_logs {where_sql} ORDER BY timestamp DESC LIMIT ?",
            params,
        )
        rows = cursor.fetchall()
    return [
        AuditLogEntry(
            id=r["id"],
            timestamp=r["timestamp"],
            event_type=r["event_type"],
            result=r["result"],
            username=r["username"],
            role=r["role"],
            batch_id=r["batch_id"],
            detail=r["detail"],
            ip_address=r["ip_address"],
        )
        for r in rows
    ]


# ── System Stats ──────────────────────────────────────────────────────────────

@router.get("/stats")
def get_admin_stats(user: dict = Depends(require_role("admin"))):
    """System-wide statistics for admin dashboard."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM users")
        total_users = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE is_active = 1")
        active_users = cursor.fetchone()["cnt"]

        cursor.execute("SELECT role, COUNT(*) AS cnt FROM users GROUP BY role")
        role_counts = {r["role"]: r["cnt"] for r in cursor.fetchall()}

        cursor.execute("SELECT COUNT(*) AS cnt FROM batches")
        total_batches = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) AS cnt FROM batch_transfers")
        total_transfers = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) AS cnt FROM sensor_data")
        total_sensor = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) AS cnt FROM audit_logs WHERE result IN ('FAILURE','DENIED')")
        security_events = cursor.fetchone()["cnt"]

        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM audit_logs WHERE timestamp >= date('now','-1 day')"
        )
        recent_events = cursor.fetchone()["cnt"]

    return {
        "users": {
            "total": total_users,
            "active": active_users,
            "by_role": role_counts,
        },
        "batches": {
            "total": total_batches,
            "transfers": total_transfers,
        },
        "iot": {
            "sensor_readings": total_sensor,
        },
        "security": {
            "failed_events_total": security_events,
            "audit_events_last_24h": recent_events,
        },
    }


# ── All Batches (Admin view) ──────────────────────────────────────────────────

@router.get("/batches")
def get_all_batches(
    limit: int = Query(default=50, ge=1, le=200),
    user: dict = Depends(require_role("admin")),
):
    """Get all batches across all producers. Admin only."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM batches ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        rows = cursor.fetchall()
        batches = [
            enrich_transfer_with_sensor(
                dict(r),
                fetch_latest_sensor_row(cursor, r["batch_id"]),
            )
            for r in rows
        ]
    return {"count": len(batches), "batches": batches}


# ── All Transfer Events (Admin view) ─────────────────────────────────────────

@router.get("/transfers")
def get_all_transfers(
    batch_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    user: dict = Depends(require_role("admin")),
):
    """Get all transfer events. Admin only."""
    with get_connection() as conn:
        cursor = conn.cursor()
        if batch_id:
            cursor.execute(
                "SELECT * FROM batch_transfers WHERE batch_id = ? ORDER BY created_at DESC LIMIT ?",
                (batch_id, limit),
            )
        else:
            cursor.execute(
                "SELECT * FROM batch_transfers ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        rows = cursor.fetchall()
        transfers = [
            enrich_transfer_with_sensor(
                dict(r),
                fetch_latest_sensor_row(cursor, r["batch_id"]),
            )
            for r in rows
        ]
    return {"count": len(transfers), "transfers": transfers}
