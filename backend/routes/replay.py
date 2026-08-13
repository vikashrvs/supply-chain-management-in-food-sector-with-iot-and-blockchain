from fastapi import APIRouter, Query, Request

from database import (
    fetch_batch_history,
    fetch_replay_records,
    get_replay_batch_config,
    get_replay_batch_options,
    replay_row_to_payload,
    row_to_dict,
)
from services.replay_manager import replay_manager

router = APIRouter(prefix="/api/replay", tags=["Demo Replay"])


@router.get("/dataset")
def get_replay_dataset(batch_id: str = "FC-001"):
    batch = get_replay_batch_config(batch_id)
    rows = fetch_replay_records(batch_id)
    return {
        "mode": "Demo Telemetry / Replay Mode",
        "purpose": "Dashboard replay for future Physical ESP32 + DHT11/DHT22 + Gas + GPS integration.",
        "count": len(rows),
        "origin": batch["origin"],
        "destination": batch["destination"],
        "batch_id": batch["batch_id"],
        "device_id": f'{batch["device_id"]} (Demo)',
        "product": batch["product"],
        "available_batches": get_replay_batch_options(),
        "records": [replay_row_to_payload(row) for row in rows],
    }


@router.get("/transportation")
def get_transportation_state(batch_id: str = "FC-001"):
    batch = get_replay_batch_config(batch_id)
    rows = fetch_batch_history(batch_id)
    history = [row_to_dict(row) for row in rows]
    latest = history[-1] if history else None
    total = len(fetch_replay_records(batch_id))
    progress = 0
    if latest and latest.get("replay_record_id"):
        progress = min(100, round((latest["replay_record_id"] / max(total, 1)) * 100))
    return {
        "mode": "Demo Telemetry / Replay Mode",
        "future_source": "Physical ESP32 + DHT11/DHT22 + Gas + GPS",
        "origin": batch["origin"],
        "destination": batch["destination"],
        "batch_id": batch_id,
        "device_id": f'{batch["device_id"]} (Demo)',
        "product": batch["product"],
        "available_batches": get_replay_batch_options(),
        "received_count": len(history),
        "total_records": total,
        "progress_pct": progress,
        "status": latest["transportation_status"] if latest else "READY",
        "latest": latest,
        "history": history,
        "replay": replay_manager.status(),
    }


@router.post("/start")
def start_replay(
    request: Request,
    batch_id: str = "FC-001",
    interval_seconds: float = Query(default=1.5, ge=0.2, le=600),
    reset: bool = True,
):
    mqtt_client = getattr(request.app.state, "mqtt_client", None)
    return replay_manager.start(mqtt_client, batch_id=batch_id, interval_seconds=interval_seconds, reset=reset)


@router.post("/pause")
def pause_replay():
    return replay_manager.pause()


@router.post("/resume")
def resume_replay():
    return replay_manager.resume()


@router.post("/step")
def step_replay(request: Request):
    mqtt_client = getattr(request.app.state, "mqtt_client", None)
    return replay_manager.step_forward(mqtt_client)


@router.post("/reset")
def reset_replay(batch_id: str = "FC-001"):
    return replay_manager.reset(batch_id=batch_id)

