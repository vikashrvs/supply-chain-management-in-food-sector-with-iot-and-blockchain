"""
Edge health evaluation and product history summarization.
Extracted from main.py.
"""

from config import SUPPLY_CHAIN_STAGES, STAGE_THRESHOLDS
from utils import normalize_stage, format_stage_label, format_range


def evaluate_edge_health(current_stage, temperature, humidity):
    stage = normalize_stage(current_stage)
    thresholds = STAGE_THRESHOLDS[stage]
    alerts = []

    temp_range = thresholds["temperature"]
    humidity_range = thresholds["humidity"]

    if temperature is None:
        alerts.append("Temperature reading unavailable.")
    elif temperature < temp_range[0] or temperature > temp_range[1]:
        alerts.append(
            f"Temperature {temperature} C is outside the {format_stage_label(stage)} range "
            f"({format_range(temp_range, ' C')})."
        )

    if humidity is None:
        alerts.append("Humidity reading unavailable.")
    elif humidity < humidity_range[0] or humidity > humidity_range[1]:
        alerts.append(
            f"Humidity {humidity}% is outside the {format_stage_label(stage)} range "
            f"({format_range(humidity_range, '%')})."
        )

    alert_count = len(alerts)
    if alert_count >= 2:
        risk_level = "critical"
        edge_decision = "Hold this product for inspection before the next handoff."
    elif alert_count == 1:
        risk_level = "warning"
        edge_decision = f"Review {format_stage_label(stage)} storage conditions and continue with caution."
    else:
        risk_level = "stable"
        edge_decision = thresholds["healthy_note"]

    return {
        "alerts": alerts,
        "alert_count": alert_count,
        "risk_level": risk_level,
        "health_label": risk_level.title(),
        "is_healthy": alert_count == 0,
        "edge_decision": edge_decision,
        "expected_temperature_range": format_range(temp_range, " C"),
        "expected_humidity_range": format_range(humidity_range, "%"),
        "journey_progress_pct": int(((SUPPLY_CHAIN_STAGES.index(stage) + 1) / len(SUPPLY_CHAIN_STAGES)) * 100),
        "journey_progress_label": f"{SUPPLY_CHAIN_STAGES.index(stage) + 1} of {len(SUPPLY_CHAIN_STAGES)} stages completed",
    }


def summarize_product_history(history):
    visited_stages = []
    incident_count = 0

    for record in history:
        if record["current_stage"] not in visited_stages:
            visited_stages.append(record["current_stage"])
        if record["alert_count"]:
            incident_count += 1

    latest = history[-1]
    return {
        "first_seen": history[0]["timestamp"],
        "last_seen": latest["timestamp"],
        "visited_stages": visited_stages,
        "visited_stage_labels": [format_stage_label(stage) for stage in visited_stages],
        "reading_count": len(history),
        "incident_count": incident_count,
        "latest_risk_level": latest["risk_level"],
        "journey_progress_pct": latest["journey_progress_pct"],
    }
