#include <ESP8266WiFi.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <PubSubClient.h>
#include <OneWire.h>
#include <DallasTemperature.h>

// --- CONFIGURAÇÕES DE HARDWARE ---
#define SDA_PIN 12 // D6
#define SCL_PIN 14 // D5
#define ONE_WIRE_BUS 4 // D2 (Pino do Sensor DS18B20)

// --- CONFIGURAÇÕES DE TELA ---
Adafruit_SSD1306 display(128, 64, &Wire, -1);

// --- OBJETOS DO SENSOR ---
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);

// --- CREDENCIAIS DE REDE & BROKER ---
// Em produção, use WiFiManager para não chumbar senha
const char* ssid = "NOME_DA_SUA_REDE";
const char* password = "SENHA_DA_SUA_REDE";

const char* mqtt_server = "mosquitto.interglobaltecnologia.com.br";
const int mqtt_port = 1883;
// const char* mqtt_user = "sensor_user"; // Se tiver autenticação
// const char* mqtt_pass = "sensor_pass";

WiFiClient espClient;
PubSubClient client(espClient);

// --- VARIÁVEIS GLOBAIS ---
String myMacAddress = "";
long lastMsg = 0;
// Enviar a cada 30 segundos (ou 5 min em prod) para não floodar
const long interval = 30000; 

// --- AUXILIAR: DISPLAY ---
void drawStatus(String linha1, String linha2, String linha3) {
  display.clearDisplay();
  display.setTextColor(WHITE);
  display.setTextSize(1);
  display.setCursor(0, 0);  display.println(linha1);
  display.setCursor(0, 20); display.println(linha2);
  display.setTextSize(2);
  display.setCursor(0, 40); display.println(linha3);
  display.display();
}

void setup() {
  Serial.begin(115200);

  // 1. Inicializa Display
  Wire.begin(SDA_PIN, SCL_PIN);
  if(!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) {
    Serial.println(F("Falha no OLED"));
    for(;;);
  }
  drawStatus("BOOT...", "Agrotech v2.0", "Iniciando");

  // 2. Inicializa Sensor
  sensors.begin();
  
  // 3. Obtém MAC Limpo (Identidade Única)
  myMacAddress = WiFi.macAddress();
  myMacAddress.replace(":", ""); // Formato AABBCC112233
  Serial.println("MAC ID: " + myMacAddress);

  // 4. Conecta WiFi
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi Conectado!");

  // 5. Configura MQTT
  client.setServer(mqtt_server, mqtt_port);
  
  // CRÍTICO: Aumenta o buffer para caber o JSON grande
  client.setBufferSize(512); 
}

void reconnect() {
  while (!client.connected()) {
    Serial.print("Tentando MQTT... ");
    String clientId = "Sensor-" + myMacAddress;
    
    if (client.connect(clientId.c_str())) {
      Serial.println("Conectado!");
      drawStatus("WIFI: OK", "MQTT: OK", "Online");
    } else {
      Serial.print("Falha, rc=");
      Serial.print(client.state());
      Serial.println(" tentando em 5s");
      drawStatus("WIFI: OK", "MQTT: ERRO", "Reconn...");
      delay(5000);
    }
  }
}

void loop() {
  if (!client.connected()) reconnect();
  client.loop();

  long now = millis();
  if (now - lastMsg > interval) {
    lastMsg = now;
    
    // 1. Leitura dos Sensores
    sensors.requestTemperatures(); 
    float temp = sensors.getTempCByIndex(0);
    
    // Simulação de umidade (hardware real precisaria de um DHT11/22 ou SHT30)
    float umid = 55.0; 

    // Validação básica de erro do sensor
    if (temp == -127.00) {
      Serial.println("Erro de leitura do sensor!");
      drawStatus("ERRO", "SENSOR", "FALHA");
      return;
    }

    // 2. Montagem do JSON (Schema V2)
    // Atenção: Latitude/Longitude fixas aqui, mas poderiam vir de um módulo GPS
    String json = "{";
    json += "\"mac\": \"" + myMacAddress + "\",";
    json += "\"temperatura\": " + String(temp) + ",";
    json += "\"umidade\": " + String(umid) + ",";
    json += "\"latitude\": -16.6869,"; 
    json += "\"longitude\": -49.2648,"; 
    
    // Objeto aninhado de contexto
    json += "\"dadosAdicionais\": {"; 
    json += "\"ip\": \"" + WiFi.localIP().toString() + "\",";
    json += "\"rssi\": " + String(WiFi.RSSI()) + ",";
    json += "\"modelo\": \"ESP8266-DS18B20-OLED\",";
    json += "\"uptime_ms\": " + String(millis());
    json += "}"; // Fecha dadosAdicionais
    json += "}"; // Fecha JSON principal

    // 3. Publicação (Funil Único)
    // Todo sensor manda para o mesmo tópico. O Spark separa.
    String topico = "telemetria/raw"; 
    
    Serial.print("Enviando pacote (" + String(json.length()) + " bytes)... ");
    if (client.publish(topico.c_str(), json.c_str())) {
      Serial.println("Sucesso!");
      drawStatus("Agrotech", "Temp Atual:", String(temp, 1) + " C");
    } else {
      Serial.println("FALHA no envio. Buffer cheio?");
      drawStatus("ERRO", "ENVIO MQTT", "FALHA");
    }
  }
}