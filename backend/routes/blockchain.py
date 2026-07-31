"""
Blockchain verification routes — hash chain integrity + Fabric status.
"""

from fastapi import APIRouter, HTTPException

from database import fetch_record_rows
from utils import normalize_batch_id, normalize_stage
from config import BATCH_KEY_EXPR
from services.hash_chain import compute_block_hash
from services.fabric_client import get_fabric_status

router = APIRouter(tags=["Blockchain"])


@router.get("/verify/{batch_id}")
def verify_batch_integrity(batch_id: str):
    """Verify SHA256 hash chain integrity for a batch — proves tamper evidence."""
    rows = fetch_record_rows(
        where_clause=f"{BATCH_KEY_EXPR} = ?",
        params=(batch_id,),
        order_by="sd.id ASC",
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No records found for batch {batch_id}.")

    prev_hash = "0" * 64
    chain_intact = True
    chain = []

    for row in rows:
        # Reconstruct the record using the SAME normalization logic as build_record
        # to ensure the recomputed hash matches the one stored at insert time.
        raw_batch_id = normalize_batch_id(row["batch_id"], row["product_id"], f"LEGACY_BATCH_{row['id']:03d}")
        raw_stage = normalize_stage(row["current_stage"], row["status"])
        record = {
            "batch_id": raw_batch_id,
            "timestamp": row["timestamp"],
            "temperature": row["temperature"],
            "humidity": row["humidity"],
            "current_stage": raw_stage,
        }

        expected_hash = compute_block_hash(record, prev_hash)
        actual_hash = row["block_hash"]
        is_valid = actual_hash == expected_hash

        if not is_valid:
            chain_intact = False

        chain.append({
            "id": row["id"],
            "timestamp": row["timestamp"],
            "stage": raw_stage,
            "expected_hash": expected_hash[:16] + "...",
            "actual_hash": (actual_hash[:16] + "...") if actual_hash else None,
            "valid": is_valid,
        })
        prev_hash = expected_hash

    return {
        "batch_id": batch_id,
        "chain_intact": chain_intact,
        "record_count": len(chain),
        "chain": chain,
    }


@router.get("/api/blockchain/status")
def blockchain_status():
    """Get current blockchain layer status (Fabric + hash chain)."""
    return get_fabric_status()
