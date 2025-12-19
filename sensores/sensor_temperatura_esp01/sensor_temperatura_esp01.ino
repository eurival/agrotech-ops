#include <ESP8266WiFi.h>
#include <WiFiManager.h>
#include <PubSubClient.h>
#include <EEPROM.h>
#include <OneWire.h>
#include <DallasTemperature.h>

// --- HARDWARE ---
#define ONE_WIRE_BUS 2 
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);

// --- MQTT ---
const char* mqtt_server = "mosquitto.interglobaltecnologia.com.br";
const int mqtt_port = 1883;

struct Settings {
  char clienteId[32]; char localId[32]; bool registrado;
};
Settings configSensor;

WiFiClient espClient;
PubSubClient client(espClient);
long lastMsg = 0;
String myMacAddress = "";

void saveConfig() {
  EEPROM.begin(512); EEPROM.put(0, configSensor); EEPROM.commit(); EEPROM.end();
}

void loadConfig() {
  EEPROM.begin(512); EEPROM.get(0, configSensor); EEPROM.end();
  if (configSensor.registrado > 1) { 
    configSensor.registrado = false;
    strcpy(configSensor.clienteId, "DESCONHECIDO");
  }
}

void callback(char* topic, byte* payload, unsigned int length) {
  // Callback simplificado
}

void reconnect() {
  // Loop até conectar
  while (!client.connected()) {
    Serial.print("Tentando MQTT em ");
    Serial.print(mqtt_server);
    Serial.print("...");
    
    String clientId = "Sensor-" + myMacAddress;
    
    if (client.connect(clientId.c_str())) {
      Serial.println("CONECTADO!");
      client.subscribe(("comandos/" + myMacAddress).c_str());
    } else {
      Serial.print("Falha, rc=");
      Serial.print(client.state());
      Serial.println(" Tenta em 5s");
      delay(5000);
    }
  }
}

void configModeCallback (WiFiManager *myWiFiManager) {}

void setup() {
  Serial.begin(115200);
  delay(1000); // Espera estabilizar energia
  Serial.println("\n--- INICIANDO ---");

  sensors.begin(); 
  loadConfig();
  
  myMacAddress = WiFi.macAddress();
  myMacAddress.replace(":", ""); 

  WiFiManager wifiManager;
  wifiManager.setAPCallback(configModeCallback);
  wifiManager.setConfigPortalTimeout(180);
  
  if (!wifiManager.autoConnect("AGROTECH-ESP01")) {
    Serial.println("Falha WiFi timeout");
    ESP.reset();
  }

  Serial.println("WiFi OK! Configurando MQTT...");
  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(callback);
}

void loop() {
  if (!client.connected()) {
    reconnect();
  }
  client.loop();

  long now = millis();
  if (now - lastMsg > 10000) { 
    lastMsg = now;
    
    Serial.println("Lendo Sensor...");
    sensors.requestTemperatures(); 
    float temp = sensors.getTempCByIndex(0);
    Serial.print("Temp: "); Serial.println(temp);

    // JSON Enxuto
    //String json = "{\"temp\":" + String(temp) + "}";
    // --- JSON PADRÃO AGROTECH (Compatível com Spark) ---
    String json = "{";
    json += "\"mac\": \"" + myMacAddress + "\","; // <--- Essencial para o Spark
    json += "\"temperatura\": " + String(temp) + ",";
    json += "\"umidade\": 50.0,"; // Valor fixo pois ESP01 não tem sensor
    json += "\"latitude\": -16.6869,"; 
    json += "\"longitude\": -49.2648,"; 
    
    json += "\"dadosAdicionais\": {"; 
    json += "\"ip\": \"" + WiFi.localIP().toString() + "\",";
    json += "\"rssi\": " + String(WiFi.RSSI()) + ",";
    json += "\"modelo\": \"ESP01-Mini\""; 
    
    // Adiciona status se registrado
    if (configSensor.registrado) {
      json += ", \"status\": \"OPERACIONAL\",";
      json += "\"cliente\": \"" + String(configSensor.clienteId) + "\",";
      json += "\"local\": \"" + String(configSensor.localId) + "\"";
    } else {
      json += ", \"status\": \"NAO_REGISTRADO\"";
    }
    
    json += "}"; // Fecha dadosAdicionais
    json += "}"; // Fecha JSON principal
    if (configSensor.registrado) {
       client.publish(("sensores/" + String(configSensor.clienteId) + "/telemetria").c_str(), json.c_str());
    } else {
       Serial.println("Enviando Onboarding...");
       client.publish("onboarding/novo", json.c_str());
    }
  }
}