"""
Listen on MQTT food/sensor/# and capture the next message.
Prints the raw payload and exits after 1 message (or 60s timeout).
Run from: C:\Users\raj vikash\Desktop\food_chain\backend
"""
import json, sys, time

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt not installed. Run: pip install paho-mqtt")
    sys.exit(1)

BROKER  = "192.168.0.110"
PORT    = 1883
TOPIC   = "food/sensor/#"
TIMEOUT = 60

received = []

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Connected to", BROKER, "port", PORT)
        client.subscribe(TOPIC)
        print("[MQTT] Subscribed to", TOPIC, "-- waiting up to", TIMEOUT, "s ...")
    else:
        print("[MQTT] Connection failed rc=", rc)
        sys.exit(1)

def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="replace")
    print()
    print("[MQTT] Topic  :", msg.topic)
    print("[MQTT] Raw    :", raw)
    try:
        data = json.loads(raw)
        print("[MQTT] Parsed :")
        for k, v in data.items():
            print("         ", k, ":", v)
    except Exception as e:
        print("[MQTT] (JSON parse error:", e, ")")
    received.append(msg)
    client.disconnect()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, keepalive=60)
client.loop_start()

start = time.time()
while not received and (time.time() - start) < TIMEOUT:
    time.sleep(0.5)

client.loop_stop()
if not received:
    print("[MQTT] TIMEOUT -- no message received within", TIMEOUT, "seconds.")
    sys.exit(1)
else:
    print()
    print("[MQTT] Message captured successfully.")
