import json
import logging
from datetime import datetime

import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s [EDGE] %(message)s')
logger = logging.getLogger('edge_processor')

BROKER = 'localhost'
PORT = 1883
INPUT_TOPIC = 'food/sensor/#'
OUTPUT_TOPIC = 'food/processed'
ALERT_TOPIC = 'food/alerts'

STAGE_THRESHOLDS = {
    'field': {'temperature': (18.0, 27.0), 'humidity': (65.0, 85.0)},
    'warehouse': {'temperature': (4.0, 10.0), 'humidity': (70.0, 90.0)},
    'transport': {'temperature': (5.0, 12.0), 'humidity': (60.0, 80.0)},
    'retailer': {'temperature': (6.0, 14.0), 'humidity': (55.0, 75.0)},
    'consumer': {'temperature': (8.0, 16.0), 'humidity': (50.0, 70.0)},
}

alert_history = {}

def evaluate_reading(data):
    stage = data.get('current_stage', 'transport')
    temp = data.get('temperature')
    humidity = data.get('humidity')
    thresholds = STAGE_THRESHOLDS.get(stage, STAGE_THRESHOLDS['transport'])
    
    alerts = []
    temp_range = thresholds['temperature']
    hum_range = thresholds['humidity']
    
    if temp is not None and (temp < temp_range[0] or temp > temp_range[1]):
        alerts.append(f'Temperature {temp}°C outside [{temp_range[0]}-{temp_range[1]}]')
    if humidity is not None and (humidity < hum_range[0] or humidity > hum_range[1]):
        alerts.append(f'Humidity {humidity}% outside [{hum_range[0]}-{hum_range[1]}]')
    
    if len(alerts) >= 2:
        risk_level = 'critical'
        edge_action = 'HOLD_FOR_INSPECTION'
    elif len(alerts) == 1:
        risk_level = 'warning'
        edge_action = 'CONTINUE_WITH_CAUTION'
    else:
        risk_level = 'stable'
        edge_action = 'PROCEED'
    
    data['edge_processed'] = True
    data['edge_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['edge_risk_level'] = risk_level
    data['edge_action'] = edge_action
    data['edge_alerts'] = alerts
    
    return data, risk_level, alerts

def on_message(client, userdata, message):
    try:
        raw_data = json.loads(message.payload.decode())
    except json.JSONDecodeError:
        logger.error(f'Invalid JSON on {message.topic}')
        return
    
    processed_data, risk_level, alerts = evaluate_reading(raw_data)
    batch_id = raw_data.get('batch_id', 'UNKNOWN')
    
    # Forward processed data to backend
    client.publish(OUTPUT_TOPIC, json.dumps(processed_data))
    
    # Publish alerts separately
    if alerts:
        alert_msg = {
            'batch_id': batch_id,
            'risk_level': risk_level,
            'alerts': alerts,
            'timestamp': processed_data['edge_timestamp'],
            'stage': raw_data.get('current_stage', 'unknown'),
        }
        client.publish(ALERT_TOPIC, json.dumps(alert_msg))
        
        # Track consecutive alerts
        count = alert_history.get(batch_id, 0) + 1
        alert_history[batch_id] = count
        
        if count >= 3:
            logger.warning(f'🚨 {batch_id}: {count} consecutive alerts — ESCALATING')
    else:
        alert_history[batch_id] = 0
    
    status = '⚠️' if risk_level != 'stable' else '✅'
    logger.info(
        f'{status} {batch_id} | {raw_data.get("current_stage")}'  
        f' | T={raw_data.get("temperature")}°C'
        f' | H={raw_data.get("humidity")}%'
        f' | Edge={risk_level.upper()}'
    )

def main():
    client = mqtt.Client(client_id='edge_processor')
    client.on_message = on_message
    client.connect(BROKER, PORT, 60)
    client.subscribe(INPUT_TOPIC)
    
    logger.info('Edge Processor started — listening on food/sensor/#')
    logger.info('Publishing processed data to food/processed')
    logger.info('Publishing alerts to food/alerts')
    
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info('Edge Processor stopped.')
    finally:
        client.disconnect()

if __name__ == '__main__':
    main()
