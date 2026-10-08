"""
Business-facing API routes for FoodChain.
Read-only shipment/performance analytics — no user management, no audit logs.
All endpoints require the 'business' role.
"""

import logging
from fastapi import APIRouter, Depends, Query

from auth import require_role
from database import get_connection, fetch_latest_sensor_row, row_to_dict, enrich_transfer_with_sensor

logger = logging.getLogger("foodchain.business")
router = APIRouter(prefix="/api/business", tags=["Business"])


@router.get("/overview")
def get_business_overview(
    limit: int = Query(default=100, ge=1, le=500),
    user: dict = Depends(require_role("manager", "admin")),
):
    """Return only persisted operational data for the Business dashboard."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM batches")
        total_batches = cursor.fetchone()["cnt"]
        source = "batches"
        if total_batches:
            cursor.execute("SELECT COUNT(DISTINCT COALESCE(NULLIF(product_type, ''), product_name)) AS cnt FROM batches")
            product_types = cursor.fetchone()["cnt"]
            cursor.execute("SELECT COUNT(*) AS cnt FROM batches WHERE status IN ('created', 'in_transit')")
            active_shipments = cursor.fetchone()["cnt"]
            cursor.execute("SELECT COUNT(*) AS cnt FROM batches WHERE status = 'in_transit'")
            in_transit = cursor.fetchone()["cnt"]
            cursor.execute("SELECT COUNT(*) AS cnt FROM batches WHERE status = 'transferred'")
            delivered = cursor.fetchone()["cnt"]
            cursor.execute("SELECT COUNT(*) AS cnt FROM batches WHERE status = 'created'")
            producer_stage = cursor.fetchone()["cnt"]
            cursor.execute(
                """SELECT COUNT(DISTINCT batch_id) AS cnt FROM batch_transfers
                   WHERE event_type = 'anomaly'"""
            )
            delayed = cursor.fetchone()["cnt"]
            cursor.execute(
                """SELECT COUNT(*) AS cnt FROM batches b
                   WHERE b.status = 'transferred'
                   AND NOT EXISTS (
                     SELECT 1 FROM batch_transfers t
                     WHERE t.batch_id = b.batch_id AND t.event_type = 'anomaly'
                   )"""
            )
            clean_deliveries = cursor.fetchone()["cnt"]
            efficiency = round((clean_deliveries / delivered) * 100, 1) if delivered else None
            cursor.execute("SELECT COUNT(DISTINCT sensor_id) AS cnt FROM sensor_readings WHERE sensor_id IS NOT NULL AND TRIM(sensor_id) != ''")
            active_iot_devices = cursor.fetchone()["cnt"]
            cursor.execute(
                """SELECT COALESCE(NULLIF(product_type, ''), NULLIF(product_name, ''), 'Unclassified') AS label,
                          COUNT(*) AS value
                   FROM batches GROUP BY label ORDER BY value DESC"""
            )
            product_distribution = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """SELECT batch_id, product_name, product_type, origin, destination, status,
                          quantity, created_at, updated_at
                   FROM batches ORDER BY updated_at DESC LIMIT ?""",
                (limit,),
            )
            batches = []
            for row in cursor.fetchall():
                batch = dict(row)
                sensor_row = fetch_latest_sensor_row(cursor, batch["batch_id"])
                if sensor_row:
                    sensor = row_to_dict(sensor_row)
                    batch.update({
                        "temperature": sensor["temperature"],
                        "humidity": sensor["humidity"],
                        "device_id": sensor["device_id"],
                        "latest_iot": sensor,
                        "block_hash": sensor["block_hash"],
                        "field_hash": sensor["field_hash"],
                        "fabric_tx_id": sensor["fabric_tx_id"],
                    })
                batches.append(batch)
            cursor.execute(
                """SELECT t.batch_id, t.event_type, t.location_name, t.latitude, t.longitude,
                          t.created_at, b.product_name, b.origin, b.destination, b.status
                   FROM batch_transfers t
                   JOIN batches b ON b.batch_id = t.batch_id
                   WHERE t.latitude IS NOT NULL AND t.longitude IS NOT NULL
                   ORDER BY t.created_at DESC LIMIT ?""",
                (limit,),
            )
            locations = [dict(row) for row in cursor.fetchall()]
        else:
            source = "sensor_readings"
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                   WHERE COALESCE(NULLIF(batch_id, ''), product_id) IS NOT NULL
                 )
                 SELECT COUNT(*) AS cnt FROM latest WHERE row_number = 1"""
            )
            total_batches = cursor.fetchone()["cnt"]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COUNT(DISTINCT COALESCE(NULLIF(product, ''), NULLIF(product_name, ''), product_id))
                 AS cnt FROM latest WHERE row_number = 1"""
            )
            product_types = cursor.fetchone()["cnt"]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COUNT(*) AS cnt FROM latest
                 WHERE row_number = 1 AND LOWER(COALESCE(current_stage, '')) != 'consumer'"""
            )
            active_shipments = cursor.fetchone()["cnt"]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COUNT(*) AS cnt FROM latest
                 WHERE row_number = 1 AND LOWER(COALESCE(current_stage, '')) = 'transport'"""
            )
            in_transit = cursor.fetchone()["cnt"]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COUNT(*) AS cnt FROM latest
                 WHERE row_number = 1 AND (
                   LOWER(COALESCE(current_stage, '')) = 'consumer'
                   OR LOWER(COALESCE(transportation_status, '')) = 'delivered'
                 )"""
            )
            delivered = cursor.fetchone()["cnt"]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COUNT(*) AS cnt FROM latest
                 WHERE row_number = 1 AND LOWER(COALESCE(current_stage, '')) = 'field'"""
            )
            producer_stage = cursor.fetchone()["cnt"]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COUNT(*) AS cnt FROM latest
                 WHERE row_number = 1 AND alert_flag = 1"""
            )
            delayed = cursor.fetchone()["cnt"]
            efficiency = None
            cursor.execute("SELECT COUNT(DISTINCT sensor_id) AS cnt FROM sensor_readings WHERE sensor_id IS NOT NULL AND TRIM(sensor_id) != ''")
            active_iot_devices = cursor.fetchone()["cnt"]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COALESCE(NULLIF(product, ''), NULLIF(product_name, ''), product_id, 'Unclassified') AS label,
                        COUNT(*) AS value
                 FROM latest WHERE row_number = 1 GROUP BY label ORDER BY value DESC"""
            )
            product_distribution = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COALESCE(NULLIF(batch_id, ''), product_id) AS batch_id,
                        COALESCE(NULLIF(product_name, ''), NULLIF(product, ''), product_id) AS product_name,
                        NULL AS product_type, origin_name AS origin, destination_name AS destination,
                        COALESCE(transportation_status, status, current_stage) AS status,
                        NULL AS quantity, timestamp AS created_at, timestamp AS updated_at
                 FROM latest WHERE row_number = 1 ORDER BY id DESC LIMIT ?""",
                (limit,),
            )
            batches = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """WITH latest AS (
                     SELECT sd.*,
                            ROW_NUMBER() OVER (
                              PARTITION BY COALESCE(NULLIF(batch_id, ''), product_id)
                              ORDER BY id DESC
                            ) AS row_number
                   FROM sensor_readings sd
                 )
                 SELECT COALESCE(NULLIF(batch_id, ''), product_id) AS batch_id,
                        latitude, longitude, origin_name AS location_name,
                        current_stage AS status, timestamp AS created_at
                 FROM latest
                 WHERE row_number = 1 AND latitude IS NOT NULL AND longitude IS NOT NULL
                 ORDER BY id DESC LIMIT ?""",
                (limit,),
            )
            locations = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """SELECT event_type, result, username, batch_id, detail, timestamp
               FROM audit_logs ORDER BY timestamp DESC LIMIT ?""",
            (limit,),
        )
        activities = [dict(row) for row in cursor.fetchall()]
        for location in locations:
            sensor_row = fetch_latest_sensor_row(cursor, location.get("batch_id"))
            if sensor_row:
                sensor = row_to_dict(sensor_row)
                location.update({
                    "temperature": sensor["temperature"],
                    "humidity": sensor["humidity"],
                    "device_id": sensor["device_id"],
                    "block_hash": sensor["block_hash"],
                    "field_hash": sensor["field_hash"],
                    "fabric_tx_id": sensor["fabric_tx_id"],
                })
        cursor.execute(
            """SELECT temperature, humidity, sensor_id, timestamp
               FROM sensor_readings
               WHERE temperature IS NOT NULL OR humidity IS NOT NULL
               ORDER BY id DESC LIMIT 1"""
        )
        sensor_row = cursor.fetchone()
        sensor_readings = dict(sensor_row) if sensor_row else None

    return {
        "data_source": source,
        "kpis": {
            "total_batches": total_batches,
            "product_types": product_types,
            "active_shipments": active_shipments,
            "in_transit": in_transit,
            "delivered": delivered,
            "delayed": delayed,
            "efficiency_percent": efficiency,
            "active_iot_devices": active_iot_devices,
        },
        "flow": {
            "producer": producer_stage,
            "processing": None,
            "in_transit": in_transit,
            "distributor": None,
            "delivered": delivered,
        },
        "product_distribution": product_distribution,
        "batches": batches,
        "locations": locations,
        "activities": activities,
        "sensor_readings": sensor_readings,
    }


# ── Shipment / performance overview ─────────────────────────────────────────

@router.get("/stats")
def get_business_stats(user: dict = Depends(require_role("manager", "admin"))):
    """Aggregated shipment metrics for the Business dashboard.

    Status mapping note: the schema only tracks 'created', 'in_transit',
    and 'transferred' on batches.batch — there is no explicit 'delivered'
    state yet, so 'transferred' is used as the delivered proxy here.
    Failures are anomaly events logged in batch_transfers.
    """
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) AS cnt FROM batches")
        total_products = cursor.fetchone()["cnt"]

        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM batches WHERE status IN ('created','in_transit')"
        )
        active_shipments = cursor.fetchone()["cnt"]

        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM batches WHERE status = 'in_transit'"
        )
        in_transit = cursor.fetchone()["cnt"]

        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM batches WHERE status = 'transferred'"
        )
        delivered = cursor.fetchone()["cnt"]

        cursor.execute(
            """SELECT COUNT(DISTINCT batch_id) AS cnt FROM batch_transfers
               WHERE event_type = 'anomaly'"""
        )
        failed_or_flagged = cursor.fetchone()["cnt"]

        # Efficiency = % of delivered batches with zero anomaly events
        cursor.execute(
            """SELECT COUNT(*) AS cnt FROM batches b
               WHERE b.status = 'transferred'
               AND NOT EXISTS (
                   SELECT 1 FROM batch_transfers t
                   WHERE t.batch_id = b.batch_id AND t.event_type = 'anomaly'
               )"""
        )
        clean_deliveries = cursor.fetchone()["cnt"]
        efficiency = round((clean_deliveries / delivered) * 100, 1) if delivered else 0.0

    return {
        "total_products": total_products,
        "active_shipments": active_shipments,
        "in_transit": in_transit,
        "delivered": delivered,
        "failed_or_flagged": failed_or_flagged,
        "efficiency_percent": efficiency,
    }


# ── Recent shipments list (for the dashboard table) ─────────────────────────

@router.get("/shipments")
def get_recent_shipments(
    limit: int = 20,
    user: dict = Depends(require_role("manager", "admin")),
):
    """Recent batches with their latest known location/status, for the
    Business dashboard's shipment table."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT batch_id, product_name, origin, destination, status,
                      created_at, updated_at
               FROM batches
               ORDER BY updated_at DESC
               LIMIT ?""",
            (limit,),
        )
        rows = [
            enrich_transfer_with_sensor(
                dict(r),
                fetch_latest_sensor_row(cursor, r["batch_id"]),
            )
            for r in cursor.fetchall()
        ]
    return {"count": len(rows), "shipments": rows}
