#include <ESP8266WiFi.h>
#include <DNSServer.h>
#include <ESP8266WebServer.h>
#include <WiFiManager.h> 
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <PubSubClient.h>
#include <EEPROM.h> // <---VOLTOU
#include <OneWire.h>
#include <DallasTemperature.h>

// ============================================================
// --- CONFIGURAÇÕES ---
// ============================================================
const char* mqtt_server = "mosquitto.interglobaltecnologia.com.br";
const int mqtt_port = 1883;
const long interval = 30000; 

// ============================================================
// --- HARDWARE ---
// ============================================================
#define SDA_PIN 12 
#define SCL_PIN 14 
#define ONE_WIRE_BUS 4 

Adafruit_SSD1306 display(128, 64, &Wire, -1);
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);

WiFiClient espClient;
PubSubClient client(espClient);

String myMacAddress = "";
long lastMsg = 0;

// ============================================================
// --- ESTRUTURA DE DADOS (EEPROM) - LEGADO RESTAURADO ---
// ============================================================
struct Settings {
  char empresaId[32]; 
  char unidadeId[32]; 
  byte validacao;     
};
Settings configSensor;

#define MAGIC_NUMBER 0xAA 

// ============================================================
// --- FUNÇÕES DE UI E MEMÓRIA ---
// ============================================================
void drawStatus(String linha1, String linha2, String linha3 = "") {
  display.clearDisplay();
  display.setTextColor(WHITE);
  display.setTextSize(1);
  display.setCursor(0, 0); display.println(linha1);
  display.drawLine(0, 10, 127, 10, WHITE);
  display.setTextSize(2);
  display.setCursor(0, 15); display.println(linha2);
  if(linha3 != "") {
    display.setTextSize(1);
    display.setCursor(0, 50); display.println(linha3);
  }
  display.display();
}

void saveConfig() {
  EEPROM.begin(512);
  EEPROM.put(0, configSensor);
  EEPROM.commit();
  EEPROM.end();
}

void loadConfig() {
  EEPROM.begin(512);
  EEPROM.get(0, configSensor);
  EEPROM.end();
  
  if (configSensor.validacao != MAGIC_NUMBER) { 
    Serial.println("EEPROM Virgem/Corrompida. Resetando...");
    configSensor.validacao = 0; 
    strcpy(configSensor.empresaId, "DESCONHECIDA");
    strcpy(configSensor.unidadeId, "SEM_UNIDADE");
  } else {
    Serial.println("Config Carregada: " + String(configSensor.empresaId));
  }
}

// ============================================================
// --- CALLBACK MQTT (REGISTRAR;) - LEGADO RESTAURADO ---
// ============================================================
void callback(char* topic, byte* payload, unsigned int length) {
  String msg = "";
  for (int i = 0; i < length; i++) msg += (char)payload[i];
  Serial.print("RX CMD: "); Serial.println(msg);

  // Formato esperado: REGISTRAR;NomeEmpresa;NomeUnidade
  if (msg.startsWith("REGISTRAR;")) {
    int primeiraDivisao = msg.indexOf(';');
    int segundaDivisao = msg.indexOf(';', primeiraDivisao + 1);
    
    if (segundaDivisao > 0) {
      String novaEmpresa = msg.substring(primeiraDivisao + 1, segundaDivisao);
      String novaUnidade = msg.substring(segundaDivisao + 1);
      
      novaEmpresa.trim();
      novaUnidade.trim();
      
      novaEmpresa.toCharArray(configSensor.empresaId, 32);
      novaUnidade.toCharArray(configSensor.unidadeId, 32);
      
      configSensor.validacao = MAGIC_NUMBER; 
      saveConfig();
      
      drawStatus("SUCESSO!", "REGISTRADO", novaEmpresa);
      delay(2000);
      
      // Força envio imediato após registro
      lastMsg = 0; 
    }
  }
}

void reconnect() {
  while (!client.connected()) {
    String statusReg = (configSensor.validacao == MAGIC_NUMBER) ? "REG" : "NO-REG";
    drawStatus("CONEXAO", "MQTT...", statusReg);
    
    String clientId = "Sensor-" + myMacAddress;
    
    if (client.connect(clientId.c_str())) {
      Serial.println("MQTT Conectado!");
      
      // Se inscreve no tópico de comandos deste MAC
      String topicCmd = "comandos/" + myMacAddress;
      client.subscribe(topicCmd.c_str());
      Serial.println("Inscrito em: " + topicCmd);
      
      drawStatus("ONLINE", "AGUARDANDO", "WiFi: " + String(WiFi.RSSI()) + "dB");
    } else {
      Serial.print("Falha rc="); Serial.print(client.state());
      drawStatus("ERRO MQTT", "Rc=" + String(client.state()), "Retentando...");
      delay(5000);
    }
  }
}

void configModeCallback (WiFiManager *myWiFiManager) {
  drawStatus("MODO CONFIG", "AGROTECH", "192.168.4.1");
}

// ============================================================
// --- SETUP ---
// ============================================================
void setup() {
  Serial.begin(115200);
  
  Wire.begin(SDA_PIN, SCL_PIN);
  if(!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) { for(;;); }
  drawStatus("BOOT", "INICIANDO", "v4.0 Hybrid");

  sensors.begin(); 
  loadConfig(); // Carrega EEPROM
  
  myMacAddress = WiFi.macAddress();
  myMacAddress.replace(":", ""); 
  
  WiFiManager wifiManager;
  wifiManager.setAPCallback(configModeCallback);
  if (!wifiManager.autoConnect(("AGROTECH-" + myMacAddress).c_str())) {
    ESP.reset();
  }

  client.setServer(mqtt_server, mqtt_port);
  client.setCallback(callback); // Ativa escuta de comandos
  client.setBufferSize(768); // Buffer gigante para JSON
}

// ============================================================
// --- LOOP ---
// ============================================================
void loop() {
  if (!client.connected()) reconnect();
  client.loop();

  long now = millis();
  if (now - lastMsg > interval) { 
    lastMsg = now;
    
    // 1. Leitura
    sensors.requestTemperatures(); 
    float v1 = sensors.getTempCByIndex(0);
    float v2 = 50.0 + (random(30) / 10.0); // Simulação

    if (v1 == -127.00) {
      drawStatus("ERRO", "SENSOR", "Verifique Cabo");
      return; 
    }

    // 2. Montagem do JSON
    String json = "{";
    json += "\"mac\": \"" + myMacAddress + "\",";
    json += "\"leituras\": {";
    json += "\"v1\": " + String(v1) + ","; 
    json += "\"v2\": " + String(v2);
    json += "},";
    
    json += "\"dadosAdicionais\": {"; 
    json += "\"ip\": \"" + WiFi.localIP().toString() + "\",";
    json += "\"rssi\": " + String(WiFi.RSSI()) + ",";
    
    // Envia status e empresa (visualização no log, Spark ignora se quiser)
    if (configSensor.validacao == MAGIC_NUMBER) {
       json += "\"status\": \"OPERACIONAL\",";
       json += "\"empresa\": \"" + String(configSensor.empresaId) + "\"";
    } else {
       json += "\"status\": \"NAO_REGISTRADO\"";
    }
    json += "} }"; 

    // 3. Lógica de Envio (ONBOARDING vs TELEMETRIA)
    String topicoAlvo;
    
    if (configSensor.validacao == MAGIC_NUMBER) {
      // REGISTRADO: Manda para Telemetria
      // O Kafka Connector mapeia "telemetria/raw" -> "telemetria-raw"
      topicoAlvo = "telemetria/raw"; 
    } else {
      // NÃO REGISTRADO: Manda para Onboarding
      // O Kafka Connector mapeia "onboarding/novo" -> "onboarding-raw"
      topicoAlvo = "onboarding/novo";
    }

    Serial.println("--- TX [" + topicoAlvo + "] ---");
    Serial.println(json);

    if (client.publish(topicoAlvo.c_str(), json.c_str())) {
      String linhaStatus = (configSensor.validacao == MAGIC_NUMBER) ? "ENVIADO" : "ONBOARDING";
      drawStatus(linhaStatus, String(v1, 1) + " C", "RSSI: " + String(WiFi.RSSI()));
    } else {
      drawStatus("ERRO", "FALHA ENVIO", "Buffer?");
    }
  }
}