/*
 * FoodChain IoT Sensor Node
 * Hardware: ESP32 + DHT22 + NEO-6M GPS
 * Protocol: MQTT → Mosquitto Broker → FastAPI Backend
 * 
 * Wiring:
 *   DHT22: VCC→3.3V, GND→GND, DATA→GPIO4
 *   GPS:   VCC→3.3V, GND→GND, TX→GPIO16(RX2), RX→GPIO17(TX2)
 * 
 * Libraries Required:
 *   - PubSubClient (MQTT)
 *   - DHT sensor library
 *   - TinyGPSPlus
 *   - ArduinoJson
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <TinyGPS++.h>
#include <ArduinoJson.h>

// ========== CONFIGURATION ==========
// WiFi
const char* WIFI_SSID     = "YOUR_WIFI_SSID";
const char* WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";

// MQTT Broker (your PC running Mosquitto)
const char* MQTT_SERVER   = "YOUR_PC_IP";  // e.g., "192.168.1.100"
const int   MQTT_PORT     = 1883;

// Sensor Config
const char* BATCH_ID      = "BATCH_001";
const char* PRODUCT_NAME  = "Apples";
const char* SENSOR_ID     = "SENSOR_ESP32_A";
const char* CURRENT_STAGE = "transport";  // field|warehouse|transport|retailer|consumer

// Timing
const unsigned long PUBLISH_INTERVAL = 5000;  // 5 seconds

// ========== PIN DEFINITIONS ==========
#define DHT_PIN     4
#define DHT_TYPE    DHT22
#define GPS_RX_PIN  16
#define GPS_TX_PIN  17
#define LED_PIN     2   // Built-in LED for status
#define BUZZER_PIN  15  // Optional buzzer for alerts

// ========== THRESHOLD CONFIG (Edge Computing) ==========
struct StageThreshold {
    float tempMin, tempMax;
    float humMin, humMax;
};

StageThreshold thresholds[] = {
    {18.0, 27.0, 65.0, 85.0},  // field
    {4.0, 10.0, 70.0, 90.0},   // warehouse
    {5.0, 12.0, 60.0, 80.0},   // transport
    {6.0, 14.0, 55.0, 75.0},   // retailer
    {8.0, 16.0, 50.0, 70.0}    // consumer
};

const char* stageNames[] = {"field", "warehouse", "transport", "retailer", "consumer"};

// ========== OBJECTS ==========
DHT dht(DHT_PIN, DHT_TYPE);
TinyGPSPlus gps;
WiFiClient espClient;
PubSubClient mqtt(espClient);

unsigned long lastPublish = 0;
int stageIndex = 2;  // default: transport

// ========== SETUP ==========
void setup() {
    Serial.begin(115200);
    Serial2.begin(9600, SERIAL_8N1, GPS_RX_PIN, GPS_TX_PIN);
    
    pinMode(LED_PIN, OUTPUT);
    pinMode(BUZZER_PIN, OUTPUT);
    
    dht.begin();
    
    // Determine stage index
    for (int i = 0; i < 5; i++) {
        if (strcmp(stageNames[i], CURRENT_STAGE) == 0) {
            stageIndex = i;
            break;
        }
    }
    
    connectWiFi();
    mqtt.setServer(MQTT_SERVER, MQTT_PORT);
    connectMQTT();
    
    Serial.println("=== FoodChain Sensor Node Started ===");
    Serial.printf("Batch: %s | Stage: %s | Sensor: %s\n", BATCH_ID, CURRENT_STAGE, SENSOR_ID);
}

// ========== MAIN LOOP ==========
void loop() {
    if (!mqtt.connected()) connectMQTT();
    mqtt.loop();
    
    // Read GPS continuously
    while (Serial2.available() > 0) {
        gps.encode(Serial2.read());
    }
    
    if (millis() - lastPublish >= PUBLISH_INTERVAL) {
        lastPublish = millis();
        publishSensorData();
    }
}

// ========== PUBLISH SENSOR DATA ==========
void publishSensorData() {
    float temperature = dht.readTemperature();
    float humidity = dht.readHumidity();
    
    if (isnan(temperature) || isnan(humidity)) {
        Serial.println("ERROR: DHT22 read failed!");
        blinkLED(5, 100);  // Fast blink = sensor error
        return;
    }
    
    // === EDGE COMPUTING: Local threshold check ===
    bool tempAlert = (temperature < thresholds[stageIndex].tempMin || 
                      temperature > thresholds[stageIndex].tempMax);
    bool humAlert  = (humidity < thresholds[stageIndex].humMin || 
                      humidity > thresholds[stageIndex].humMax);
    
    String edgeDecision = "OK";
    if (tempAlert && humAlert) {
        edgeDecision = "CRITICAL";
        triggerAlert();  // Buzzer + LED
    } else if (tempAlert || humAlert) {
        edgeDecision = "WARNING";
        digitalWrite(LED_PIN, HIGH);
    } else {
        digitalWrite(LED_PIN, LOW);
    }
    
    // Build JSON payload
    StaticJsonDocument<512> doc;
    char timestamp[20];
    snprintf(timestamp, sizeof(timestamp), "2026-07-12 %02d:%02d:%02d", 
             (millis()/3600000)%24, (millis()/60000)%60, (millis()/1000)%60);
    
    doc["batch_id"]      = BATCH_ID;
    doc["product"]       = PRODUCT_NAME;
    doc["product_name"]  = PRODUCT_NAME;
    doc["product_id"]    = BATCH_ID;
    doc["sensor_id"]     = SENSOR_ID;
    doc["current_stage"] = CURRENT_STAGE;
    doc["temperature"]   = round(temperature * 100) / 100.0;
    doc["humidity"]       = round(humidity * 100) / 100.0;
    doc["timestamp"]     = timestamp;
    doc["status"]        = (strcmp(CURRENT_STAGE, "consumer") == 0) ? "Delivered" : "In Transit";
    doc["edge_decision"] = edgeDecision;
    
    // GPS location
    JsonObject location = doc.createNestedObject("location");
    if (gps.location.isValid()) {
        location["lat"] = gps.location.lat();
        location["lng"] = gps.location.lng();
    } else {
        location["lat"] = 12.9716;  // Default: Bangalore
        location["lng"] = 77.5946;
    }
    
    // Publish to MQTT
    char payload[512];
    serializeJson(doc, payload);
    
    char topic[64];
    snprintf(topic, sizeof(topic), "food/sensor/%s", BATCH_ID);
    
    if (mqtt.publish(topic, payload)) {
        Serial.printf("[%s] T=%.1f°C H=%.1f%% Edge=%s GPS=%s\n",
                      CURRENT_STAGE, temperature, humidity, edgeDecision.c_str(),
                      gps.location.isValid() ? "FIX" : "NO_FIX");
    } else {
        Serial.println("MQTT publish FAILED");
    }
}

// ========== NETWORK ==========
void connectWiFi() {
    Serial.printf("Connecting to WiFi: %s", WIFI_SSID);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts < 30) {
        delay(500);
        Serial.print(".");
        attempts++;
    }
    if (WiFi.status() == WL_CONNECTED) {
        Serial.printf("\nWiFi connected! IP: %s\n", WiFi.localIP().toString().c_str());
    } else {
        Serial.println("\nWiFi FAILED — restarting...");
        ESP.restart();
    }
}

void connectMQTT() {
    while (!mqtt.connected()) {
        Serial.print("MQTT connecting...");
        if (mqtt.connect(SENSOR_ID)) {
            Serial.println("connected!");
        } else {
            Serial.printf("failed (rc=%d), retrying...\n", mqtt.state());
            delay(2000);
        }
    }
}

void triggerAlert() {
    for (int i = 0; i < 3; i++) {
        digitalWrite(BUZZER_PIN, HIGH);
        digitalWrite(LED_PIN, HIGH);
        delay(200);
        digitalWrite(BUZZER_PIN, LOW);
        digitalWrite(LED_PIN, LOW);
        delay(200);
    }
}

void blinkLED(int times, int ms) {
    for (int i = 0; i < times; i++) {
        digitalWrite(LED_PIN, HIGH);
        delay(ms);
        digitalWrite(LED_PIN, LOW);
        delay(ms);
    }
}
