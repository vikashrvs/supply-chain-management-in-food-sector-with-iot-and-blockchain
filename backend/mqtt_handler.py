"""
MQTT handler — subscribes to food/sensor/# and inserts sensor readings.
"""

import json
import logging

import paho.mqtt.client as mqtt

from database import insert_sensor_data
from schemas import SensorReading

logger = logging.getLogger(__name__)


def on_message(client, userdata, message):
    try:
        data = json.loads(message.payload.decode())
    except json.JSONDecodeError as exc:
        logger.warning("MQTT payload decode failed: %s", exc)
        return

    # Validate through Pydantic before inserting — same rules as HTTP POST
    try:
        validated = SensorReading(**data)
        insert_sensor_data(validated.model_dump())
        logger.info("MQTT recorded: batch=%s stage=%s", validated.batch_id, validated.current_stage)
    except Exception as exc:
        logger.warning("MQTT validation/insert failed: %s — raw data: %s", exc, data)


def setup_mqtt(app):
    """Connect to MQTT broker and start listening on food/sensor/#."""
    try:
        try:
            # paho-mqtt >= 2.0 requires CallbackAPIVersion
            mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except (AttributeError, TypeError):
            # paho-mqtt < 2.0 fallback
            mqtt_client = mqtt.Client()
        mqtt_client.on_message = on_message
        mqtt_client.connect("localhost", 1883, 60)
        mqtt_client.subscribe("food/sensor/#")
        mqtt_client.loop_start()
        app.state.mqtt_client = mqtt_client
        logger.info("MQTT connected on food/sensor/#")
    except Exception as exc:
        app.state.mqtt_client = None
        logger.info("MQTT unavailable: %s", exc)


def shutdown_mqtt(app):
    """Gracefully disconnect MQTT client."""
    mqtt_client = getattr(app.state, "mqtt_client", None)
    if mqtt_client is not None:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
