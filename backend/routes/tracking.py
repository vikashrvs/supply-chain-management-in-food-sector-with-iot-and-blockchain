"""
Tracking routes — batch and UID history/current endpoints.
"""

from fastapi import APIRouter, HTTPException

from database import (
    fetch_batch_history,
    fetch_uid_history,
    fetch_record_rows,
    row_to_dict,
)
from config import SUPPLY_CHAIN_STAGES, BATCH_KEY_EXPR, UID_KEY_EXPR
from services.edge_health import summarize_product_history

router = APIRouter(tags=["Tracking"])


@router.get("/batch/{batch_id}")
def get_batch_history(batch_id: str):
    rows = fetch_batch_history(batch_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history found for batch {batch_id}.")

    history = [row_to_dict(row) for row in rows]
    return {
        "batch_id": batch_id,
        "stages": SUPPLY_CHAIN_STAGES,
        "summary": summarize_product_history(history),
        "history": history,
        "latest": history[-1],
    }


@router.get("/batch/{batch_id}/current")
def get_current_batch(batch_id: str):
    rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id DESC",
        limit=1,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No current status found for batch {batch_id}.")
    return row_to_dict(rows[0])


@router.get("/uid/{product_uid}")
def get_uid_history(product_uid: str):
    rows = fetch_uid_history(product_uid)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history found for product UID {product_uid}.")

    history = [row_to_dict(row) for row in rows]
    return {
        "product_uid": product_uid,
        "stages": SUPPLY_CHAIN_STAGES,
        "summary": summarize_product_history(history),
        "history": history,
        "latest": history[-1],
    }


@router.get("/uid/{product_uid}/current")
def get_current_uid(product_uid: str):
    rows = fetch_record_rows(
        where_clause=f"{UID_KEY_EXPR} = ?",
        params=(product_uid,),
        order_by="sd.id DESC",
        limit=1,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No current status found for product UID {product_uid}.")
    return row_to_dict(rows[0])
