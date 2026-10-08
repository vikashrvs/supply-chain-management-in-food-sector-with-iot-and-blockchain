"""Real Hyperledger Fabric block explorer endpoint for the Admin dashboard."""
from pathlib import Path
import json
import subprocess

from fastapi import APIRouter, HTTPException, Query

from services.fabric_client import get_fabric_status

router = APIRouter(tags=["Blockchain Explorer"])

_PROJECT_DIR = Path(__file__).resolve().parents[2]
_EXPLORER_SCRIPT = _PROJECT_DIR / "blockchain" / "explorer.js"
_BLOCKCHAIN_DIR = _PROJECT_DIR / "blockchain"


@router.get("/api/blockchain/blocks")
def get_fabric_blocks(limit: int = Query(12, ge=1, le=50)):
    status = get_fabric_status()
    if not status.get("fabric_available"):
        raise HTTPException(status_code=503, detail="Hyperledger Fabric is offline. Start the Fabric network to inspect real blocks.")
    if not _EXPLORER_SCRIPT.exists():
        raise HTTPException(status_code=500, detail="Fabric explorer script is not installed.")

    try:
        result = subprocess.run(
            ["node", str(_EXPLORER_SCRIPT), str(limit)],
            capture_output=True,
            text=True,
            timeout=45,
            cwd=str(_BLOCKCHAIN_DIR),
        )
        if not result.stdout.strip():
            raise RuntimeError(result.stderr.strip() or "Fabric explorer returned no data")
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        if not payload.get("success"):
            raise RuntimeError(payload.get("error", "Fabric explorer failed"))
        return payload
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Timed out while reading Fabric blocks.")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
