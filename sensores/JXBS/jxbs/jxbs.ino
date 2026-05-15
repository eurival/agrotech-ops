#include <Arduino.h>
#include <WiFi.h>
#include <WiFiManager.h>
#include <PubSubClient.h>

// ==========================================
// CONFIGURAÇÕES DE REDE E MQTT
// ==========================================
const char* mqtt_server = "mosquitto.interglobaltecnologia.com.br";
const int mqtt_port = 1883;

WiFiClient espClient;
PubSubClient client(espClient);
String myMacAddress = "";
String mqttTopic = "";

unsigned long lastMsgTime = 0;
const unsigned long MSG_INTERVAL = 15000; // Envio a cada 15 segundos

// ==========================================
// PINOS ESP32-C3 E RS485 (Seguros para USB)
// ==========================================
#define RX_PIN 4
#define TX_PIN 5
#define DE_RE_PIN 7

HardwareSerial ModbusSerial(1);

// ==========================================
// CÁLCULO DE CRC16 MODBUS
// ==========================================
uint16_t calculateCRC(byte *buf, int len) {
  uint16_t crc = 0xFFFF;
  for (int pos = 0; pos < len; pos++) {
    crc ^= (uint16_t)buf[pos];
    for (int i = 8; i != 0; i--) {
      if ((crc & 0x0001) != 0) { crc >>= 1; crc ^= 0xA001; } 
      else { crc >>= 1; }
    }
  }
  return crc;
}

// ==========================================
// CONEXÃO MQTT (Non-Blocking)
// ==========================================
void reconnectMQTT() {
  if (!client.connected()) {
    Serial.print("Conectando ao Broker AgroTech... ");
    String clientId = "AgroTech-Sensor-" + myMacAddress;
    
    if (client.connect(clientId.c_str())) {
      Serial.println("CONECTADO!");
    } else {
      Serial.print("Falha, rc=");
      Serial.print(client.state());
      Serial.println(" - Tentando novamente...");
    }
  }
}

// ==========================================
// FUNÇÃO DE LEITURA MODBUS (Generic)
// ==========================================
bool lerModbus(uint16_t reg, uint16_t qtd, uint16_t *dest) {
  byte msg[8] = {0x01, 0x03, (byte)(reg >> 8), (byte)(reg & 0xFF), 0x00, (byte)qtd};
  uint16_t crc = calculateCRC(msg, 6);
  msg[6] = crc & 0xFF;
  msg[7] = crc >> 8;

  digitalWrite(DE_RE_PIN, HIGH);
  delay(5);
  ModbusSerial.write(msg, 8);
  ModbusSerial.flush();
  digitalWrite(DE_RE_PIN, LOW);

  unsigned long t = millis();
  int idx = 0;
  byte rb[20];
  while (millis() - t < 500) {
    if (ModbusSerial.available()) rb[idx++] = ModbusSerial.read();
  }

  if (idx >= (5 + 2 * qtd)) {
    for (int i = 0; i < qtd; i++) {
      dest[i] = (rb[3 + (i * 2)] << 8) | rb[4 + (i * 2)];
    }
    return true;
  }
  return false;
}

void setup() {
  Serial.begin(115200);
  pinMode(DE_RE_PIN, OUTPUT);
  digitalWrite(DE_RE_PIN, LOW);
  ModbusSerial.begin(9600, SERIAL_8N1, RX_PIN, TX_PIN);

  WiFi.mode(WIFI_STA);
  myMacAddress = WiFi.macAddress();
  myMacAddress.replace(":", "");
  
  WiFiManager wm;
  String apName = "AGROTECH-ONBOARD-" + myMacAddress.substring(myMacAddress.length() - 4);
  
  if (!wm.autoConnect(apName.c_str())) {
    Serial.println("Falha no Wi-Fi. Reiniciando...");
    ESP.restart();
  }

  Serial.println("Conectado! IP: " + WiFi.localIP().toString());
  client.setServer(mqtt_server, mqtt_port);
  mqttTopic = "sensores/WIFI-" + myMacAddress + "/telemetria";
}

void loop() {
  if (!client.connected()) reconnectMQTT();
  client.loop();

  unsigned long now = millis();
  if (now - lastMsgTime > MSG_INTERVAL) {
    lastMsgTime = now;
    
    uint16_t valHT[2], valPH[1], valNPK[3], valEC[1];
    
    // Leituras baseadas no manual JXBS
    bool ok = true;
    ok &= lerModbus(0x0012, 2, valHT);  // Umidade (0.1) e Temp (0.1)
    ok &= lerModbus(0x0006, 1, valPH);  // pH (0.01)
    ok &= lerModbus(0x0015, 1, valEC);  // EC (us/cm)
    ok &= lerModbus(0x001E, 3, valNPK); // N, P, K

    if (ok) {
      // Mapeamento v1 a v7 para o Spark ETL
      String json = "{";
      json += "\"mac\": \"WIFI-" + myMacAddress + "\",";
      json += "\"leituras\": {";
      json += "\"v1\": " + String(valHT[1] / 10.0) + ","; // Temp
      json += "\"v2\": " + String(valHT[0] / 10.0) + ","; // Umid
      json += "\"v3\": " + String(valPH[0] / 100.0) + ","; // pH
      json += "\"v4\": " + String(valEC[0]) + ",";        // EC
      json += "\"v5\": " + String(valNPK[0]) + ",";       // N
      json += "\"v6\": " + String(valNPK[1]) + ",";       // P
      json += "\"v7\": " + String(valNPK[2]);             // K
      json += "},";
      
      String ads = "{\"ip\":\"" + WiFi.localIP().toString() + "\",\"rssi\":" + String(WiFi.RSSI()) + ",\"status\":\"OPERACIONAL\"}";
      ads.replace("\"", "\\\""); 
      json += "\"dadosAdicionais\": \"" + ads + "\"";
      json += "}";

      if(client.publish(mqttTopic.c_str(), json.c_str())) {
        Serial.println(">>> Enviado para Kafka via MQTT: " + json);
      }
    } else {
      Serial.println("!!! Erro na leitura dos sensores. Verifique fiação.");
    }
  }
}