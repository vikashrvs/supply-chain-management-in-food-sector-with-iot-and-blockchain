/*
 * FoodChain SCM — ESP32 Real Hardware Controller Firmware
 *
 * Hardware Setup:
 * - ESP32 Development Board (NodeMCU ESP32 / WROOM-32)
 * - DHT22 Temperature & Humidity Sensor (Signal -> GPIO 13)
 * - NEO-6M GPS Module
 *      TX -> ESP32 GPIO 25
 *      RX -> ESP32 GPIO 26
 *
 * Required Arduino Libraries:
 * 1. DHT sensor library by Adafruit
 * 2. Adafruit Unified Sensor
 * 3. TinyGPSPlus by Mikal Hart
 * 4. PubSubClient by Nick O'Leary
 * 5. ArduinoJson by Benoit Blanchon
 *
 * MQTT:
 * Topic: food/sensor/ESP32-01
 * Port: 1883
 */

// ============================================================
// CONFIGURATION
// ============================================================

#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <TinyGPSPlus.h>
#include <HardwareSerial.h>
#include <ArduinoJson.h>


const char* WIFI_SSID     = "powerhouse-2G";
const char* WIFI_PASSWORD = "power@829";

const char* MQTT_SERVER   = "192.168.0.108";
const int   MQTT_PORT     = 1883;

const char* MQTT_TOPIC    = "food/sensor/ESP32-01";
const char* DEVICE_ID     = "ESP32-01";

const char* BATCH_ID      = "PROD-3737E1C4";

// Send telemetry every 10 seconds
const unsigned long TRANSMIT_INTERVAL_MS = 10000;


// ============================================================
// PIN DEFINITIONS
// ============================================================

#define DHTPIN       13
#define DHTTYPE      DHT22

#define GPS_RX_PIN   25
#define GPS_TX_PIN   26

#define GPS_BAUD     9600


// ============================================================
// OBJECTS
// ============================================================

DHT dht(DHTPIN, DHTTYPE);

TinyGPSPlus gps;

HardwareSerial SerialGPS(2);

WiFiClient espClient;

PubSubClient mqttClient(espClient);

unsigned long lastTransmitTime = 0;


// ============================================================
// GPS LOCATION
// ============================================================
//
// These are ONLY fallback coordinates for testing when GPS
// has not acquired a satellite fix.
//
// IMPORTANT:
// If GPS has no fix, these values are NOT real GPS data.
//

double currentLat = 12.978220136143342;
double currentLng = 77.4827532709431;

bool gpsHasFix = false;


// ============================================================
// SETUP
// ============================================================

void setup() {

  Serial.begin(115200);

  delay(1000);

  Serial.println();
  Serial.println("==============================================");
  Serial.println("   FOODCHAIN SCM — ESP32 HARDWARE CONTROLLER");
  Serial.println("==============================================");

  // ----------------------------------------------------------
  // Initialize DHT22
  // ----------------------------------------------------------

  dht.begin();

  // ----------------------------------------------------------
  // Initialize GPS
  // ----------------------------------------------------------

  SerialGPS.begin(
    GPS_BAUD,
    SERIAL_8N1,
    GPS_RX_PIN,
    GPS_TX_PIN
  );

  Serial.println(
    "[HARDWARE] DHT22 -> GPIO 13"
  );

  Serial.println(
    "[HARDWARE] NEO-6M TX -> GPIO 25"
  );

  Serial.println(
    "[HARDWARE] NEO-6M RX -> GPIO 26"
  );

  Serial.println(
    "[GPS] Waiting for satellite fix..."
  );

  // ----------------------------------------------------------
  // Connect Wi-Fi
  // ----------------------------------------------------------

  connectWiFi();

  // ----------------------------------------------------------
  // MQTT
  // ----------------------------------------------------------

  mqttClient.setServer(
    MQTT_SERVER,
    MQTT_PORT
  );

  mqttClient.setBufferSize(512);
}


// ============================================================
// MAIN LOOP
// ============================================================

void loop() {

  // ----------------------------------------------------------
  // Maintain Wi-Fi connection
  // ----------------------------------------------------------

  if (WiFi.status() != WL_CONNECTED) {

    connectWiFi();
  }


  // ----------------------------------------------------------
  // Maintain MQTT connection
  // ----------------------------------------------------------

  if (!mqttClient.connected()) {

    reconnectMQTT();
  }

  mqttClient.loop();


  // ----------------------------------------------------------
  // READ GPS CONTINUOUSLY
  // ----------------------------------------------------------

  while (SerialGPS.available() > 0) {

    char gpsChar = SerialGPS.read();

    gps.encode(gpsChar);
  }


  // ----------------------------------------------------------
  // CHECK FOR NEW GPS LOCATION
  // ----------------------------------------------------------

  if (gps.location.isUpdated()) {

    if (gps.location.isValid()) {

      currentLat = gps.location.lat();

      currentLng = gps.location.lng();

      gpsHasFix = true;

      Serial.println();
      Serial.println("[GPS] ===============================");

      Serial.println("[GPS] NEW LOCATION FIX");

      Serial.print("[GPS] Latitude : ");
      Serial.println(currentLat, 6);

      Serial.print("[GPS] Longitude: ");
      Serial.println(currentLng, 6);

      Serial.print("[GPS] Satellites: ");

      if (gps.satellites.isValid()) {
        Serial.println(gps.satellites.value());
      } else {
        Serial.println("Unknown");
      }

      Serial.println("[GPS] ===============================");
    }
  }


  // ----------------------------------------------------------
  // PERIODIC TELEMETRY
  // ----------------------------------------------------------

  unsigned long now = millis();

  if (
    now - lastTransmitTime >=
    TRANSMIT_INTERVAL_MS
  ) {

    lastTransmitTime = now;

    publishTelemetry();
  }
}


// ============================================================
// WIFI CONNECTION
// ============================================================

void connectWiFi() {

  WiFi.disconnect(true);

  delay(500);

  WiFi.mode(WIFI_STA);

  delay(200);

  Serial.println();

  Serial.print("[WIFI] Connecting to SSID: ");

  Serial.println(WIFI_SSID);

  WiFi.begin(
    WIFI_SSID,
    WIFI_PASSWORD
  );

  int attempts = 0;

  while (
    WiFi.status() != WL_CONNECTED &&
    attempts < 30
  ) {

    delay(500);

    Serial.print(".");

    attempts++;
  }

  if (WiFi.status() == WL_CONNECTED) {

    Serial.println();

    Serial.println(
      "[WIFI] Connected successfully!"
    );

    Serial.print(
      "[WIFI] Local IP Address: "
    );

    Serial.println(
      WiFi.localIP()
    );

  } else {

    Serial.println();

    Serial.println(
      "[WIFI] Connection failed."
    );

    Serial.println(
      "[WIFI] Will retry..."
    );
  }
}


// ============================================================
// MQTT CONNECTION
// ============================================================

void reconnectMQTT() {

  while (!mqttClient.connected()) {

    Serial.print(
      "[MQTT] Connecting to broker "
    );

    Serial.print(MQTT_SERVER);

    Serial.print("...");

    String clientId = "ESP32Client-";

    clientId += String(
      random(0xffff),
      HEX
    );

    if (
      mqttClient.connect(
        clientId.c_str()
      )
    ) {

      Serial.println(
        " Connected!"
      );

    } else {

      Serial.print(
        " Failed, rc="
      );

      Serial.print(
        mqttClient.state()
      );

      Serial.println(
        ". Retrying in 5 seconds..."
      );

      delay(5000);
    }
  }
}


// ============================================================
// GPS DIAGNOSTICS
// ============================================================

void printGPSDiagnostics() {

  Serial.println();

  Serial.println(
    "[GPS] -------- GPS DIAGNOSTICS --------"
  );


  // ----------------------------------------------------------
  // Fix status
  // ----------------------------------------------------------

  Serial.print(
    "[GPS] Fix: "
  );

  if (
    gps.location.isValid()
  ) {

    Serial.println("YES");

  } else {

    Serial.println("NO");
  }


  // ----------------------------------------------------------
  // Satellites
  // ----------------------------------------------------------

  Serial.print(
    "[GPS] Satellites: "
  );

  if (
    gps.satellites.isValid()
  ) {

    Serial.println(
      gps.satellites.value()
    );

  } else {

    Serial.println("0");
  }


  // ----------------------------------------------------------
  // HDOP
  // ----------------------------------------------------------

  Serial.print(
    "[GPS] HDOP: "
  );

  if (
    gps.hdop.isValid()
  ) {

    Serial.println(
      gps.hdop.hdop()
    );

  } else {

    Serial.println("Unknown");
  }


  // ----------------------------------------------------------
  // GPS age
  // ----------------------------------------------------------

  Serial.print(
    "[GPS] Location age (ms): "
  );

  if (
    gps.location.isValid()
  ) {

    Serial.println(
      gps.location.age()
    );

  } else {

    Serial.println("N/A");
  }


  // ----------------------------------------------------------
  // Coordinates
  // ----------------------------------------------------------

  Serial.print(
    "[GPS] Latitude: "
  );

  Serial.println(
    currentLat,
    6
  );

  Serial.print(
    "[GPS] Longitude: "
  );

  Serial.println(
    currentLng,
    6
  );


  // ----------------------------------------------------------
  // Explain whether real GPS or fallback
  // ----------------------------------------------------------

  if (
    gps.location.isValid()
  ) {

    Serial.println(
      "[GPS] SOURCE: REAL NEO-6M GPS FIX"
    );

  } else {

    Serial.println(
      "[GPS] SOURCE: FALLBACK COORDINATES"
    );

    Serial.println(
      "[GPS] Waiting for satellite lock..."
    );
  }


  Serial.println(
    "[GPS] --------------------------------"
  );
}


// ============================================================
// PUBLISH TELEMETRY
// ============================================================

void publishTelemetry() {

  // ----------------------------------------------------------
  // Read DHT22
  // ----------------------------------------------------------

  float temp = dht.readTemperature();

  float hum = dht.readHumidity();


  // ----------------------------------------------------------
  // DHT error handling
  // ----------------------------------------------------------

  if (
    isnan(temp) ||
    isnan(hum)
  ) {

    Serial.println(
      "[WARNING] Failed to read from DHT22!"
    );

    // Existing project fallback
    temp = 22.5;

    hum = 55.0;
  }


  // ----------------------------------------------------------
  // GPS diagnostics
  // ----------------------------------------------------------

  printGPSDiagnostics();


  // ----------------------------------------------------------
  // JSON payload
  // ----------------------------------------------------------

  StaticJsonDocument<512> doc;

  doc["device_id"] =
    DEVICE_ID;

  doc["sensor_id"] =
    DEVICE_ID;

  doc["batch_id"] =
    BATCH_ID;

  doc["temperature"] =
    round(temp * 10.0) / 10.0;

  doc["humidity"] =
    round(hum * 10.0) / 10.0;

  doc["gas_value"] =
    0;

  doc["latitude"] =
    currentLat;

  doc["longitude"] =
    currentLng;

  doc["current_stage"] =
    "transport";

  doc["status"] =
    (temp > 25.0)
      ? "ALERT"
      : "IN TRANSIT";

  doc["telemetry_mode"] =
    "Live ESP32 Hardware";


  // ----------------------------------------------------------
  // Serialize JSON
  // ----------------------------------------------------------

  char jsonBuffer[512];

  serializeJson(
    doc,
    jsonBuffer
  );


  // ----------------------------------------------------------
  // Serial output
  // ----------------------------------------------------------

  Serial.println();

  Serial.println(
    "[TELEMETRY] Publishing Payload:"
  );

  Serial.println(
    jsonBuffer
  );


  // ----------------------------------------------------------
  // MQTT publish
  // ----------------------------------------------------------

  if (
    mqttClient.publish(
      MQTT_TOPIC,
      jsonBuffer
    )
  ) {

    Serial.println(
      "[MQTT] Payload published successfully!"
    );

  } else {

    Serial.println(
      "[ERROR] Failed to publish MQTT message."
    );
  }
}