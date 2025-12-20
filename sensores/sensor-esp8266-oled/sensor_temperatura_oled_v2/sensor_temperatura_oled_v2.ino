#include <ESP8266WiFi.h>
#include <DNSServer.h>
#include <ESP8266WebServer.h>
#include <WiFiManager.h> // Instalar: "WiFiManager by tzapu"
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <PubSubClient.h>
#include <OneWire.h>
#include <DallasTemperature.h>

// ============================================================
// --- CONFIGURAÇÕES GERAIS ---
// ============================================================
const char* mqtt_server = "mosquitto.interglobaltecnologia.com.br";
const int mqtt_port = 1883;
const long interval = 30000; // Envia a cada 30s

// ============================================================
// --- HARDWARE ---
// ============================================================
#define SDA_PIN 12 // D6
#define SCL_PIN 14 // D5
#define ONE_WIRE_BUS 4 // D2 (Atenção: No seu codigo antigo estava D2, verifique se é GPIO4 ou pino D2)

Adafruit_SSD1306 display(128, 64, &Wire, -1);
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);

WiFiClient espClient;
PubSubClient client(espClient);

String myMacAddress = "";
long lastMsg = 0;

// ============================================================
// --- TELA (UI) ---
// ============================================================
void drawStatus(String linha1, String linha2, String linha3 = "") {
  display.clearDisplay();
  display.setTextColor(WHITE);
  
  // Cabeçalho
  display.setTextSize(1);
  display.setCursor(0, 0); display.println(linha1);
  display.drawLine(0, 10, 127, 10, WHITE);

  // Corpo
  display.setTextSize(2);
  display.setCursor(0, 15); display.println(linha2);
  
  // Rodapé
  if(linha3 != "") {
    display.setTextSize(1);
    display.setCursor(0, 50); display.println(linha3);
  }
  display.display();
}

// ============================================================
// --- CALLBACK WIFI MANAGER (MODO AP) ---
// ============================================================
void configModeCallback (WiFiManager *myWiFiManager) {
  Serial.println("Entrou no modo de configuração");
  drawStatus("MODO CONFIG", "Conecte no WiFi:", "IP: 192.168.4.1");
}

// ============================================================
// --- SETUP ---
// ============================================================
void setup() {
  Serial.begin(115200);
  
  // 1. Display
  Wire.begin(SDA_PIN, SCL_PIN);
  if(!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) { 
    Serial.println("Falha OLED");
    for(;;); 
  }
  drawStatus("BOOT", "INICIANDO", "Agrotech v3.0");

  // 2. Sensores
  sensors.begin(); 
  
  // 3. Identidade (MAC Limpo)
  myMacAddress = WiFi.macAddress();
  myMacAddress.replace(":", ""); 
  Serial.println("MAC: " + myMacAddress);
  
  // 4. Conexão (WiFiManager)
  WiFiManager wifiManager;
  wifiManager.setAPCallback(configModeCallback);
  
  // Cria rede AGROTECH-AABBCC se não tiver senha salva
  String ssidAP = "AGROTECH-" + myMacAddress;
  
  if (!wifiManager.autoConnect(ssidAP.c_str())) {
    Serial.println("Falha ao conectar e timeout");
    ESP.reset();
    delay(1000);
  }

  // Conectado!
  drawStatus("WIFI OK", "CONECTADO", "IP: " + WiFi.localIP().toString());
  
  // 5. MQTT Config
  client.setServer(mqtt_server, mqtt_port);
  // Não precisa de callback complexo, pois o sensor só ENVIA dados.
  // client.setCallback(callback); 
  
  // AUMENTAR O BUFFER (CRÍTICO PARA O JSON NOVO)
  client.setBufferSize(768); 
}

// ============================================================
// --- RECONEXÃO MQTT ---
// ============================================================
void reconnect() {
  while (!client.connected()) {
    drawStatus("CONEXAO", "MQTT...", "Broker...");
    
    String clientId = "Sensor-" + myMacAddress;
    
    if (client.connect(clientId.c_str())) {
      Serial.println("MQTT Conectado!");
      drawStatus("ONLINE", "AGUARDANDO", "WiFi: " + String(WiFi.RSSI()) + "dB");
    } else {
      Serial.print("Falha MQTT, rc=");
      Serial.print(client.state());
      drawStatus("ERRO MQTT", "Rc=" + String(client.state()), "Retentando 5s...");
      delay(5000);
    }
  }
}

// ============================================================
// --- LOOP PRINCIPAL ---
// ============================================================
void loop() {
  if (!client.connected()) reconnect();
  client.loop();

  long now = millis();
  if (now - lastMsg > interval) { 
    lastMsg = now;
    
    // 1. Leitura Hardware
    sensors.requestTemperatures(); 
    float leituraV1 = sensors.getTempCByIndex(0); // v1 = Temperatura
    float leituraV2 = 50.0 + (random(30) / 10.0); // v2 = Umidade (Simulada)

    // Validação de erro fisico
    if (leituraV1 == -127.00) {
      drawStatus("ERRO", "SENSOR", "Verifique cabo");
      return; 
    }

    // 2. Montagem do JSON (GENÉRICO / GOOGLE STYLE)
    // Não enviamos "temperatura", enviamos "v1". O banco diz o que é v1.
    String json = "{";
    json += "\"mac\": \"" + myMacAddress + "\",";
    
    json += "\"leituras\": {";
    json += "\"v1\": " + String(leituraV1) + ","; 
    json += "\"v2\": " + String(leituraV2);
    json += "},";
    
    json += "\"dadosAdicionais\": {"; 
    json += "\"ip\": \"" + WiFi.localIP().toString() + "\",";
    json += "\"rssi\": " + String(WiFi.RSSI()) + ",";
    json += "\"uptime\": " + String(millis());
    json += "}"; 
    json += "}"; 

    // 3. Envio (DIAGNÓSTICO)
    String topico = "telemetria/raw";
    
    Serial.println("--- TX ---");
    Serial.println(json);
    
    if (client.publish(topico.c_str(), json.c_str())) {
      Serial.println(">>> SUCESSO MQTT");
      drawStatus("ENVIADO", String(leituraV1, 1) + " C", "RSSI: " + String(WiFi.RSSI()));
    } else {
      Serial.println("!!! FALHA MQTT (Buffer?)");
      drawStatus("ERRO", "FALHA ENVIO", "Buffer cheio?");
    }
  }
}