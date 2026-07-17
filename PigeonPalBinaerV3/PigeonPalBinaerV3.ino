// PigeonPalBinaerV2 — basert på «ESP32 for Pigeonpal-binær» (som ligger urørt).
// Endringer 2026-07-15, kun lesing/robusthet:
//   1. 5-byte kortnummer per leverandørens protokoll (02 0A 02 | 5 byte kort-nr |
//      XOR | 03) — gammel kode kastet første byte av kortnummeret. Brikker med
//      førstebyte 0x00 gir identisk chip-id som før.
//   2. Duplikatvakt: samme chip innen 4 s etter forrige aksepterte lesing
//      undertrykkes (mønster fra NyTilSesong; serveren deduper uansett).
//   3. Ved MQTT ack-timeout roteres køfronten bakover så én problem-melding
//      ikke blokkerer nye lesinger (head-of-line).
//   4. Større UART RX-buffer (1024) for burst-toleranse.
//   5. Ringbuffer-overflow rapporteres i statslinjen.
// Alt annet (WiFi-provisjonering, MQTT, tasks/kjerner/prioriteter) er uendret.

#include <WiFi.h>
#include <time.h>
#include <WebServer.h>
#include <DNSServer.h>
#include <Preferences.h>
#include <ctype.h>
#include <esp_mac.h>
#include <esp_event.h>
#include <mqtt_client.h>

// ================== KONFIG ==================
// V3 (presence-runden, 2026-07): V2 + fire tillegg —
//  1) LWT: brokeren melder retained "offline" på statustopicet i det
//     øyeblikket forbindelsen dør (strømkutt trenger ikke si adjø selv).
//  2) Heartbeat: liten JSON hvert minutt på eget topic, UTENOM
//     hendelseskøen — appens Devices-kort får ekte online/offline.
//  3) MQTT-brukernavn/-passord kan settes i oppsettsportalen (lagres i
//     NVS) — mosquitto-auth kan skrus på server-side uten omflashing.
//  4) Lang-trykk (10 s) på BOOT-knappen tømmer lagret WiFi og åpner
//     portalen igjen — eneste utvei før var omflashing hvis ruteren ble
//     byttet. Rask LED-blink kvitterer mens knappen holdes.
const char* FW_VERSION = "3.0.0";
const bool MQTT_TRANSPORT_ENABLED = true; // feature flag for easy rollback
const char* MQTT_BROKER_URI = "mqtt://34.34.165.200:1883";
const char* MQTT_TOPIC = "pigeonpal/ingest/raw/v1";
// Compile-time fallback; portal-lagrede verdier i NVS vinner.
const char* MQTT_USERNAME = "";
const char* MQTT_PASSWORD = "";

// Presence (V3): status = retained online/offline (LWT), hb = heartbeat.
const char* PRESENCE_STATUS_TOPIC_PREFIX = "pigeonpal/presence/v1/";
const unsigned long HEARTBEAT_INTERVAL_MS = 60000UL;

  // ================== PINNER ==================
  #define RFID_RX    16
  #define RFID_TX    17
  #define WIFI_LED   2
  // BOOT-knappen på dev-kortet (GPIO0, aktiv lav) — V3: 10 s hold = WiFi-reset.
  #define RESET_BUTTON 0
  const unsigned long RESET_HOLD_MS = 10000UL;

  HardwareSerial RFID(2);

  // ================== WIFI + PROVISIONING ==================
  static const byte DNS_PORT = 53;
  static const unsigned long WIFI_CONNECT_TIMEOUT_MS = 20000UL;
  const unsigned long FAST_RETRY_WINDOW_MS = 5UL * 60UL * 1000UL;
  const unsigned long FAST_RETRY_MS        = 10000UL;
  const unsigned long SLOW_RETRY_MS        = 60000UL;

  Preferences prefs;
  WebServer server(80);
  DNSServer dnsServer;

  bool provisioningMode = false;
  String savedSsid;
  String savedPass;
  String deviceId;

  // V3: portal-lagrede MQTT-legitimasjoner (tomme = anonym/kompilert fallback) og
  // ferdigbygde presence-topics (bygges når deviceId finnes).
  String savedMqttUser;
  String savedMqttPass;
  String presenceStatusTopic;
  String presenceHeartbeatTopic;
  unsigned long lastHeartbeatMs = 0;
  unsigned long resetButtonHeldSinceMs = 0;

  unsigned long bootMs = 0;
  unsigned long lastWifiAttempt = 0;

  enum LedMode {
    LED_MODE_OFF,
    LED_MODE_SOLID,
    LED_MODE_BLINK_SLOW,
    LED_MODE_BLINK_FAST
  };

  const unsigned long LED_BLINK_SLOW_MS = 700UL;
  const unsigned long LED_BLINK_FAST_MS = 180UL;
  const unsigned long INTERNET_CHECK_INTERVAL_MS = 15000UL;

  LedMode wifiLedMode = LED_MODE_OFF;
  bool wifiLedState = false;
  unsigned long wifiLedLastToggleMs = 0;
  bool internetReachable = false;
  unsigned long lastInternetCheckMs = 0;

  // ================== EVENT + RAM RINGBUFFER ==================
  struct Event {
    char     chipIdDec[24]; // decimal string (uint64)
    uint32_t tMillis;       // tidspunkt ved lesing
  };

  const int EVENT_BUF_SIZE = 1000;
  Event eventBuf[EVENT_BUF_SIZE];

  volatile int head = 0;
  volatile int tail = 0;
  volatile bool overflowed = false;

  SemaphoreHandle_t bufMutex;

  // Retry/backoff state
  uint32_t nextSendAllowedMs = 0;
  uint8_t retryStreak = 0;
  const uint32_t RETRY_BASE_MS   = 600;
  const uint32_t RETRY_MAX_MS    = 12000;
  const uint32_t RETRY_JITTER_MS = 500;

  // MQTT transport state
  esp_mqtt_client_handle_t mqttClient = nullptr;
  volatile bool mqttConnected = false;
  volatile int mqttLastAckMsgId = -1;
  bool mqttPublishInFlight = false;
  int mqttInFlightMsgId = -1;
  uint32_t mqttInFlightSinceMs = 0;

  uint32_t mqttNextReconnectAllowedMs = 0;
  uint8_t mqttReconnectStreak = 0;
  const uint32_t MQTT_RECONNECT_BASE_MS = 1000;
  const uint32_t MQTT_RECONNECT_MAX_MS = 30000;
  const uint32_t MQTT_RECONNECT_JITTER_MS = 700;
  const uint32_t MQTT_ACK_TIMEOUT_MS = 30000;

  uint32_t mqttPublishAttempts = 0;
  uint32_t mqttPublishAccepted = 0;
  uint32_t mqttPublishAcked = 0;
  uint32_t mqttPublishErrors = 0;
  uint32_t mqttAckTimeouts = 0;
  uint32_t mqttReconnectAttempts = 0;

  // ================== PROTOTYPER ==================
  String macToUpperNoColon(const String& mac);
  String getEfuseMacString();
  String makeDeviceId();
  String makeApName();
  String htmlEscape(String s);

  void loadWifiCredentials();
  void saveWifiCredentials(const String& ssid, const String& password);
  void clearWifiCredentials();
  bool startWifi();
  bool connectWifiBlocking(unsigned long timeoutMs);
  void setWifiLedMode(LedMode mode);
  void serviceWifiLed();
  void updateInternetReachability();
  void startProvisioningPortal();
  void handleRoot();
  void handleSave();
  void handleClear();
  void handleCaptiveProbe();
  String buildSsidOptionsHtml();

  void tryInitNtpAnchor();
  uint32_t timestampForEventUnixSeconds(const Event& e);

  bool bufferPeek(Event& out);
  bool bufferCommitPop();
  void bufferPush(const char* chipIdDec, uint32_t tMillis);
  uint32_t bufferOldestAgeMs();
  int bufferCountApprox();

  void scheduleNextRetry();

  String buildEventId(const Event& e);
  int publishEventToMqtt(const Event& e);
  void initMqttClient();
  void scheduleNextMqttReconnect();
  void serviceMqttReconnect();
  static void mqttEventHandler(void* handler_args, esp_event_base_t base, int32_t event_id, void* event_data);
  void senderTask(void* pv);
  void rfidTask(void* pv);

  // ================== DEVICE ID ==================
  String macToUpperNoColon(const String& mac) {
    String out;
    out.reserve(12);
    for (size_t i = 0; i < mac.length(); ++i) {
      char c = mac.charAt(i);
      if (c != ':') out += (char)toupper((unsigned char)c);
    }
    return out;
  }

  String getEfuseMacString() {
    uint8_t mac[6];
    esp_read_mac(mac, ESP_MAC_WIFI_STA);

    char macStr[18];
    snprintf(
      macStr,
      sizeof(macStr),
      "%02X:%02X:%02X:%02X:%02X:%02X",
      mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
    );
    return String(macStr);
  }

  String makeDeviceId() {
    return String("ESP_") + macToUpperNoColon(getEfuseMacString());
  }

  String makeApName() {
    String macNoColon = macToUpperNoColon(getEfuseMacString());
    String suffix = macNoColon.substring(macNoColon.length() - 6);
    return String("PigeonPal-") + suffix;
  }

  String htmlEscape(String s) {
    s.replace("&", "&amp;");
    s.replace("<", "&lt;");
    s.replace(">", "&gt;");
    s.replace("\"", "&quot;");
    s.replace("'", "&#39;");
    return s;
  }

  // ================== WIFI CREDENTIALS ==================
  // V3: MQTT-legitimasjoner i eget NVS-navnerom; portalverdier vinner over
  // kompilerte konstanter, tomt = anonym (dagens broker-oppsett).
  void loadMqttCredentials() {
    prefs.begin("mqtt", true);
    savedMqttUser = prefs.getString("user", "");
    savedMqttPass = prefs.getString("pass", "");
    prefs.end();
    if (savedMqttUser.length() == 0) savedMqttUser = MQTT_USERNAME;
    if (savedMqttPass.length() == 0) savedMqttPass = MQTT_PASSWORD;
  }

  void saveMqttCredentials(const String& user, const String& pass) {
    prefs.begin("mqtt", false);
    prefs.putString("user", user);
    prefs.putString("pass", pass);
    prefs.end();
  }

  void loadWifiCredentials() {
    prefs.begin("wifi", true);
    savedSsid = prefs.getString("ssid", "");
    savedPass = prefs.getString("pass", "");
    prefs.end();
  }

  void saveWifiCredentials(const String& ssid, const String& password) {
    prefs.begin("wifi", false);
    prefs.putString("ssid", ssid);
    prefs.putString("pass", password);
    prefs.end();
  }

  void clearWifiCredentials() {
    prefs.begin("wifi", false);
    prefs.remove("ssid");
    prefs.remove("pass");
    prefs.end();
  }

  bool startWifi() {
    if (savedSsid.length() == 0) return false;
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.begin(savedSsid.c_str(), savedPass.c_str());
    return true;
  }

  bool connectWifiBlocking(unsigned long timeoutMs) {
    if (!startWifi()) return false;

    unsigned long start = millis();
    Serial.print("Connecting to saved WiFi");
    while (WiFi.status() != WL_CONNECTED && millis() - start < timeoutMs) {
      Serial.print(".");
      delay(400);
    }
    Serial.println();

    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("WiFi connected.");
      return true;
    }

    Serial.println("Saved WiFi connect failed.");
    WiFi.disconnect(true);
    return false;
  }

  void setWifiLedMode(LedMode mode) {
    if (wifiLedMode == mode) return;

    wifiLedMode = mode;
    wifiLedLastToggleMs = millis();

    if (mode == LED_MODE_SOLID) {
      wifiLedState = true;
      digitalWrite(WIFI_LED, HIGH);
    } else if (mode == LED_MODE_OFF) {
      wifiLedState = false;
      digitalWrite(WIFI_LED, LOW);
    }
  }

  void serviceWifiLed() {
    unsigned long interval = 0;
    if (wifiLedMode == LED_MODE_BLINK_SLOW) interval = LED_BLINK_SLOW_MS;
    else if (wifiLedMode == LED_MODE_BLINK_FAST) interval = LED_BLINK_FAST_MS;
    else return;

    unsigned long now = millis();
    if (now - wifiLedLastToggleMs >= interval) {
      wifiLedLastToggleMs = now;
      wifiLedState = !wifiLedState;
      digitalWrite(WIFI_LED, wifiLedState ? HIGH : LOW);
    }
  }

  void updateInternetReachability() {
    if (WiFi.status() != WL_CONNECTED) {
      internetReachable = false;
      return;
    }

    unsigned long now = millis();
    if (now - lastInternetCheckMs < INTERNET_CHECK_INTERVAL_MS) return;
    lastInternetCheckMs = now;

    IPAddress ip;
    internetReachable = (WiFi.hostByName("pool.ntp.org", ip) == 1);
  }

  String buildSsidOptionsHtml() {
    int count = WiFi.scanNetworks();
    String html;

    if (count <= 0) {
      html += "<option value=''>No networks found (type manually)</option>";
      return html;
    }

    for (int i = 0; i < count; ++i) {
      String ssid = WiFi.SSID(i);
      if (ssid.length() == 0) continue;
      String esc = htmlEscape(ssid);
      html += "<option value='" + esc + "'>" + esc + " (RSSI " + String(WiFi.RSSI(i)) + ")</option>";
    }
    return html;
  }

  void handleRoot() {
    String options = buildSsidOptionsHtml();
    String apIp = WiFi.softAPIP().toString();

    String html;
    html.reserve(5000);
    html += "<!doctype html><html><head><meta charset='utf-8'>";
    html += "<meta name='viewport' content='width=device-width,initial-scale=1'>";
    html += "<title>PigeonPal WiFi Setup</title>";
    html += "<style>";
    html += "body{font-family:Arial,sans-serif;margin:24px;background:#f7f7f7;color:#111}";
    html += ".card{max-width:560px;background:#fff;padding:20px;border-radius:12px;";
    html += "box-shadow:0 4px 20px rgba(0,0,0,.08)}";
    html += "label{display:block;margin:12px 0 6px;font-weight:600}";
    html += "input,select,button{width:100%;padding:10px;font-size:16px;box-sizing:border-box}";
    html += "button{margin-top:16px;background:#0f766e;color:#fff;border:0;border-radius:8px}";
    html += "small{color:#555}";
    html += "</style></head><body><div class='card'>";
    html += "<h2>PigeonPal WiFi Setup</h2>";
    html += "<p><small>Device ID: " + htmlEscape(deviceId) + "</small></p>";
    html += "<p><small>AP IP: " + htmlEscape(apIp) + "</small></p>";
    html += "<form method='GET' action='http://192.168.4.1/save'>";
    html += "<label>WiFi network</label>";
    html += "<select id='ssidSelect' onchange='document.getElementById(\"ssid\").value=this.value'>";
    html += options;
    html += "</select>";
    html += "<label>SSID (manual/edit)</label>";
    html += "<input id='ssid' name='ssid' placeholder='Network name' required>";
    html += "<label>Password</label>";
    html += "<input name='pass' type='password' placeholder='WiFi password'>";
    html += "<label>MQTT username (optional)</label>";
    html += "<input name='mqttuser' placeholder='Leave empty unless told otherwise' value='" + htmlEscape(savedMqttUser) + "'>";
    html += "<label>MQTT password (optional)</label>";
    html += "<input name='mqttpass' type='password' placeholder='Leave empty unless told otherwise'>";
    html += "<button type='submit'>Save and restart</button></form>";
    html += "<p><small>Etter lagring starter enheten på nytt og kjører uten telefon.</small></p>";
    html += "<p><a href='/clear'>Clear saved WiFi</a></p>";
    html += "</div></body></html>";

    server.send(200, "text/html", html);
  }

  void handleSave() {
    String ssid = server.arg("ssid");
    String pass = server.arg("pass");
    ssid.trim();
    pass.trim();

    if (ssid.length() == 0) {
      server.send(400, "text/plain", "SSID is required.");
      return;
    }

    saveWifiCredentials(ssid, pass);

    // V3: valgfrie MQTT-legitimasjoner; tomt passord med uendret bruker
    // beholder eksisterende passord (så re-provisjonering av WiFi ikke
    // nullstiller broker-tilgangen).
    String mqttUser = server.arg("mqttuser");
    String mqttPass = server.arg("mqttpass");
    mqttUser.trim();
    mqttPass.trim();
    if (mqttPass.length() > 0 || mqttUser != savedMqttUser) {
      saveMqttCredentials(mqttUser, mqttPass);
    }

    String html = "<!doctype html><html><body style='font-family:Arial;padding:24px'>";
    html += "<h3>Saved</h3><p>Restarting and connecting to " + htmlEscape(ssid) + "...</p>";
    html += "</body></html>";

    server.send(200, "text/html", html);
    delay(700);
    ESP.restart();
  }

  void handleClear() {
    clearWifiCredentials();
    server.send(200, "text/plain", "Saved WiFi cleared. Rebooting...");
    delay(500);
    ESP.restart();
  }

  void handleCaptiveProbe() {
    handleRoot();
  }

  void startProvisioningPortal() {
    provisioningMode = true;
    setWifiLedMode(LED_MODE_BLINK_FAST);
    WiFi.mode(WIFI_AP_STA);

    String apName = makeApName();
    bool ok = WiFi.softAP(apName.c_str());
    if (!ok) {
      Serial.println("Failed to start provisioning AP.");
      return;
    }

    dnsServer.start(DNS_PORT, "*", WiFi.softAPIP());
    server.on("/", HTTP_GET, handleRoot);
    server.on("/generate_204", HTTP_GET, handleCaptiveProbe);
    server.on("/hotspot-detect.html", HTTP_GET, handleCaptiveProbe);
    server.on("/ncsi.txt", HTTP_GET, handleCaptiveProbe);
    server.on("/connecttest.txt", HTTP_GET, handleCaptiveProbe);
    server.on("/fwlink", HTTP_GET, handleCaptiveProbe);
    server.on("/save", HTTP_POST, handleSave);
    server.on("/save", HTTP_GET, handleSave);
    server.on("/clear", HTTP_GET, handleClear);
    server.onNotFound(handleRoot);
    server.begin();

    Serial.println("Provisioning mode started.");
    Serial.println("WiFi setup portal ready.");
  }

  // ================== RINGBUFFER FUNKSJONER ==================
  inline bool bufferEmpty_nolock() { return head == tail; }
  inline bool bufferFull_nolock()  { return ((head + 1) % EVENT_BUF_SIZE) == tail; }

  void bufferPush(const char* chipIdDec, uint32_t tMillis) {
    xSemaphoreTake(bufMutex, portMAX_DELAY);

    if (bufferFull_nolock()) {
      overflowed = true;
      xSemaphoreGive(bufMutex);
      return;
    }

    strncpy(eventBuf[head].chipIdDec, chipIdDec, sizeof(eventBuf[head].chipIdDec) - 1);
    eventBuf[head].chipIdDec[sizeof(eventBuf[head].chipIdDec) - 1] = '\0';
    eventBuf[head].tMillis = tMillis;

    head = (head + 1) % EVENT_BUF_SIZE;

    xSemaphoreGive(bufMutex);
  }

  bool bufferPeek(Event& out) {
    xSemaphoreTake(bufMutex, portMAX_DELAY);
    if (bufferEmpty_nolock()) {
      xSemaphoreGive(bufMutex);
      return false;
    }
    out = eventBuf[tail];
    xSemaphoreGive(bufMutex);
    return true;
  }

  bool bufferCommitPop() {
    xSemaphoreTake(bufMutex, portMAX_DELAY);
    if (bufferEmpty_nolock()) {
      xSemaphoreGive(bufMutex);
      return false;
    }
    tail = (tail + 1) % EVENT_BUF_SIZE;
    xSemaphoreGive(bufMutex);
    return true;
  }

  int bufferCountApprox() {
    xSemaphoreTake(bufMutex, portMAX_DELAY);
    int h = head, t = tail;
    int n = (h >= t) ? (h - t) : (EVENT_BUF_SIZE - (t - h));
    xSemaphoreGive(bufMutex);
    return n;
  }

  bool bufferRotateFrontToBack() {
    xSemaphoreTake(bufMutex, portMAX_DELAY);
    if (bufferEmpty_nolock()) {
      xSemaphoreGive(bufMutex);
      return false;
    }

    Event front = eventBuf[tail];
    tail = (tail + 1) % EVENT_BUF_SIZE;
    eventBuf[head] = front;
    head = (head + 1) % EVENT_BUF_SIZE;

    xSemaphoreGive(bufMutex);
    return true;
  }

  uint32_t bufferOldestAgeMs() {
    xSemaphoreTake(bufMutex, portMAX_DELAY);
    uint32_t age = 0;
    if (!bufferEmpty_nolock()) {
      age = (uint32_t)(millis() - eventBuf[tail].tMillis);
    }
    xSemaphoreGive(bufMutex);
    return age;
  }

  // ================== RETRY + MQTT HELPERS ==================
  void scheduleNextRetry() {
    uint32_t backoff = RETRY_BASE_MS;
    for (uint8_t i = 1; i < retryStreak; i++) {
      if (backoff >= (RETRY_MAX_MS / 2)) {
        backoff = RETRY_MAX_MS;
        break;
      }
      backoff <<= 1;
    }
    if (backoff > RETRY_MAX_MS) backoff = RETRY_MAX_MS;

    backoff += (uint32_t)random((long)RETRY_JITTER_MS + 1);
    nextSendAllowedMs = millis() + backoff;
  }

  void scheduleNextMqttReconnect() {
    uint32_t backoff = MQTT_RECONNECT_BASE_MS;
    for (uint8_t i = 1; i < mqttReconnectStreak; i++) {
      if (backoff >= (MQTT_RECONNECT_MAX_MS / 2)) {
        backoff = MQTT_RECONNECT_MAX_MS;
        break;
      }
      backoff <<= 1;
    }
    if (backoff > MQTT_RECONNECT_MAX_MS) backoff = MQTT_RECONNECT_MAX_MS;
    backoff += (uint32_t)random((long)MQTT_RECONNECT_JITTER_MS + 1);
    mqttNextReconnectAllowedMs = millis() + backoff;
  }

  String buildEventId(const Event& e) {
    String id = deviceId;
    id += "-";
    id += e.chipIdDec;
    id += "-";
    id += String((unsigned long)e.tMillis);
    return id;
  }

  static void mqttEventHandler(void* handler_args, esp_event_base_t base, int32_t event_id, void* event_data) {
    (void)handler_args;
    (void)base;
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;

    switch ((esp_mqtt_event_id_t)event_id) {
      case MQTT_EVENT_CONNECTED:
        mqttConnected = true;
        mqttReconnectStreak = 0;
        mqttNextReconnectAllowedMs = 0;
        // V3: retained "online" overskriver LWT-ens "offline"; første
        // heartbeat sendes straks fra senderTask (timer nullstilles).
        esp_mqtt_client_publish(mqttClient, presenceStatusTopic.c_str(), "online", 0, 1, 1);
        lastHeartbeatMs = 0;
        Serial.println("MQTT connected.");
        break;

      case MQTT_EVENT_DISCONNECTED:
        mqttConnected = false;
        mqttPublishInFlight = false;
        mqttInFlightMsgId = -1;
        mqttInFlightSinceMs = 0;
        if (mqttReconnectStreak < 10) mqttReconnectStreak++;
        scheduleNextMqttReconnect();
        Serial.println("MQTT disconnected.");
        Serial.println("MQTT error/retry.");
        break;

      case MQTT_EVENT_PUBLISHED:
        mqttLastAckMsgId = event->msg_id;
        mqttPublishAcked++;
        Serial.println("MQTT publish ok.");
        Serial.println("MQTT ack received.");
        break;

      case MQTT_EVENT_ERROR:
        Serial.println("MQTT error/retry.");
        break;

      default:
        break;
    }
  }

  void initMqttClient() {
    if (!MQTT_TRANSPORT_ENABLED || mqttClient != nullptr) return;

    esp_mqtt_client_config_t cfg = {};
    cfg.broker.address.uri = MQTT_BROKER_URI;
    cfg.credentials.client_id = deviceId.c_str();
    if (savedMqttUser.length() > 0) cfg.credentials.username = savedMqttUser.c_str();
    if (savedMqttPass.length() > 0) cfg.credentials.authentication.password = savedMqttPass.c_str();
    // V3 LWT: brokeren publiserer retained "offline" på statustopicet i det
    // øyeblikket den mister oss (keepalive 60 s setter taket på hvor lenge
    // et stille strømkutt kan se "online" ut).
    cfg.session.last_will.topic = presenceStatusTopic.c_str();
    cfg.session.last_will.msg = "offline";
    cfg.session.last_will.msg_len = 0; // 0 = strlen(msg)
    cfg.session.last_will.qos = 1;
    cfg.session.last_will.retain = true;
    cfg.session.keepalive = 60;
    cfg.session.disable_clean_session = true;
    cfg.network.disable_auto_reconnect = true;
    cfg.network.timeout_ms = 7000;

    mqttClient = esp_mqtt_client_init(&cfg);
    if (!mqttClient) {
      Serial.println("MQTT init failed.");
      return;
    }

    esp_mqtt_client_register_event(
      mqttClient,
      (esp_mqtt_event_id_t)ESP_EVENT_ANY_ID,
      mqttEventHandler,
      nullptr
    );

    esp_err_t err = esp_mqtt_client_start(mqttClient);
    if (err != ESP_OK) {
      Serial.println("MQTT error/retry.");
      Serial.println("MQTT start failed.");
      if (mqttReconnectStreak < 10) mqttReconnectStreak++;
      scheduleNextMqttReconnect();
      return;
    }

    Serial.println("MQTT client started.");
  }

  void serviceMqttReconnect() {
    if (!MQTT_TRANSPORT_ENABLED || mqttClient == nullptr || mqttConnected) return;
    if ((int32_t)(millis() - mqttNextReconnectAllowedMs) < 0) return;

    mqttReconnectAttempts++;
    Serial.println("MQTT error/retry.");
    Serial.println("MQTT reconnect attempt.");

    esp_err_t err = esp_mqtt_client_reconnect(mqttClient);
    if (err != ESP_OK) {
      Serial.println("MQTT error/retry.");
      Serial.println("MQTT reconnect call failed.");
      if (mqttReconnectStreak < 10) mqttReconnectStreak++;
      scheduleNextMqttReconnect();
      return;
    }

    // Avoid tight reconnect calls before the DISCONNECTED event reschedules.
    mqttNextReconnectAllowedMs = millis() + 2000;
  }

  int publishEventToMqtt(const Event& e) {
    if (!MQTT_TRANSPORT_ENABLED || !mqttConnected || mqttClient == nullptr) return -1;

    uint32_t ts = timestampForEventUnixSeconds(e);

    String eventId = buildEventId(e);
    String payload = "{";
    payload += "\"deviceId\":\"" + deviceId + "\",";
    payload += "\"chipId\":\"" + String(e.chipIdDec) + "\",";
    payload += "\"timestamp\":" + String((unsigned long)ts) + ",";
    payload += "\"eventId\":\"" + eventId + "\"";
    payload += "}";

    Serial.println("MQTT publish event.");

    int msgId = esp_mqtt_client_publish(
      mqttClient,
      MQTT_TOPIC,
      payload.c_str(),
      payload.length(),
      1,   // QoS1
      0    // no retain
    );

    Serial.println("MQTT publish queued.");
    Serial.println("MQTT publish status recorded.");

    if (msgId > 0) {
      internetReachable = true;
      lastInternetCheckMs = millis();
    }

    return msgId;
  }

  // ================== RFID PARSING ==================
  const uint8_t RFID_STX = 0x02;
  const uint8_t RFID_ETX = 0x03;
  const size_t RFID_EXPECTED_FRAME_LEN = 10;
  const size_t RFID_MAX_FRAME_LEN = 16;
  // Leverandørens ramme: 02 | 0A len | 02 korttype | 5 byte kortnummer | XOR | 03.
  // Kortnummeret starter rett etter korttypen — index 3, IKKE 4.
  const size_t RFID_CHIP_OFFSET = 3;
  const size_t RFID_CHIP_LEN = 5;
  const int RFID_MIN_DEC_DIGITS = 10;
  const unsigned long RFID_BYTE_GAP_TIMEOUT_MS = 50UL;
  const unsigned long RFID_DUPLICATE_BLOCK_MS = 4000UL;

  uint8_t rfidFrameBuf[RFID_MAX_FRAME_LEN] = {0};
  size_t rfidFrameLen = 0;
  size_t rfidExpectedLen = 0;
  bool rfidInFrame = false;
  unsigned long rfidLastByteMs = 0;

  uint32_t rfidBytesRead = 0;
  uint32_t rfidFramesOk = 0;
  uint32_t rfidFramesBadLen = 0;
  uint32_t rfidFramesBadEnd = 0;
  uint32_t rfidFramesChecksumFail = 0;
  uint32_t rfidFramesOverflow = 0;
  uint32_t rfidFrameTimeouts = 0;
  uint32_t rfidDuplicatesBlocked = 0;
  unsigned long lastRfidStatsMs = 0;
  char lastAcceptedChipId[24] = {0};
  unsigned long lastAcceptedChipMs = 0;

  void resetRfidParser() {
    rfidInFrame = false;
    rfidFrameLen = 0;
    rfidExpectedLen = 0;
  }

  bool validRfidChecksum(const uint8_t* frame, size_t len) {
    if (len != RFID_EXPECTED_FRAME_LEN) return false;

    size_t checksumIndex = len - 2;
    uint8_t checksum = 0;

    // Protocol checksum: XOR of length/card-type/card-number bytes, indexes 1..7.
    for (size_t i = 1; i < checksumIndex; i++) {
      checksum ^= frame[i];
    }

    return checksum == frame[checksumIndex];
  }

  bool readRFIDBinaryFrame(uint8_t* out, size_t outLen, size_t* frameLenOut) {
    while (RFID.available()) {
      uint8_t b = RFID.read();
      rfidBytesRead++;
      unsigned long now = millis();

      if (rfidInFrame && (now - rfidLastByteMs) > RFID_BYTE_GAP_TIMEOUT_MS) {
        resetRfidParser();
        rfidFrameTimeouts++;
      }
      rfidLastByteMs = now;

      if (!rfidInFrame) {
        if (b == RFID_STX) {
          resetRfidParser();
          rfidInFrame = true;
          rfidFrameBuf[rfidFrameLen++] = b;
        }
        continue;
      }

      if (rfidFrameLen >= RFID_MAX_FRAME_LEN) {
        resetRfidParser();
        rfidFramesOverflow++;
        continue;
      }

      rfidFrameBuf[rfidFrameLen++] = b;

      if (rfidFrameLen == 2) {
        rfidExpectedLen = b;

        if (rfidExpectedLen != RFID_EXPECTED_FRAME_LEN) {
          resetRfidParser();
          rfidFramesBadLen++;
          continue;
        }
      }

      if (rfidExpectedLen == 0 || rfidFrameLen < rfidExpectedLen) {
        continue;
      }

      if (rfidFrameLen > rfidExpectedLen) {
        resetRfidParser();
        rfidFramesOverflow++;
        continue;
      }

      // 10-byte frame:
      // 02 0A 02 card0 card1 card2 card3 card4 xor 03  (5-byte card number)
      // Do not treat 0x02 or 0x03 inside the frame as delimiters.
      if (rfidFrameBuf[rfidExpectedLen - 1] != RFID_ETX) {
        resetRfidParser();
        rfidFramesBadEnd++;
        continue;
      }

      if (!validRfidChecksum(rfidFrameBuf, rfidExpectedLen)) {
        resetRfidParser();
        rfidFramesChecksumFail++;
        continue;
      }

      if (outLen >= rfidExpectedLen) {
        memcpy(out, rfidFrameBuf, rfidExpectedLen);
        if (frameLenOut) *frameLenOut = rfidExpectedLen;
      }

      resetRfidParser();
      rfidFramesOk++;
      return true;
    }

    return false;
  }

  uint64_t chipIdFromBinaryFrame(const uint8_t* frame) {
    uint64_t id = 0;
    for (size_t i = 0; i < RFID_CHIP_LEN; i++) {
      id = (id << 8) | frame[RFID_CHIP_OFFSET + i];
    }
    return id;
  }

  void chipIdHexCStr(const uint8_t* frame, char* out, size_t outLen) {
    snprintf(
      out,
      outLen,
      "%02X%02X%02X%02X%02X",
      frame[RFID_CHIP_OFFSET],
      frame[RFID_CHIP_OFFSET + 1],
      frame[RFID_CHIP_OFFSET + 2],
      frame[RFID_CHIP_OFFSET + 3],
      frame[RFID_CHIP_OFFSET + 4]
    );
  }

  // Minst 10 sifre (som før); brikker med kortnummer > 32 bit får flere sifre.
  void chipIdDecCStr(uint64_t chipId, char* out, size_t outLen) {
    snprintf(out, outLen, "%0*llu", RFID_MIN_DEC_DIGITS, (unsigned long long)chipId);
  }

  bool shouldBlockDuplicate(const char* chipId, unsigned long nowMs) {
    if (lastAcceptedChipId[0] == '\0') return false;

    bool sameChip = strcmp(chipId, lastAcceptedChipId) == 0;
    bool withinWindow = (unsigned long)(nowMs - lastAcceptedChipMs) < RFID_DUPLICATE_BLOCK_MS;

    return sameChip && withinWindow;
  }

  void rememberAcceptedChip(const char* chipId, unsigned long nowMs) {
    strncpy(lastAcceptedChipId, chipId, sizeof(lastAcceptedChipId) - 1);
    lastAcceptedChipId[sizeof(lastAcceptedChipId) - 1] = '\0';
    lastAcceptedChipMs = nowMs;
  }

  void logRfidRead(const char* chipHex, const char* chipDec) {
    Serial.print("RFID frame ok chipHex=");
    Serial.print(chipHex);
    Serial.print(" chipId=");
    Serial.println(chipDec);
  }

  void maybeLogRfidStats() {
    unsigned long now = millis();
    if (now - lastRfidStatsMs < 5000UL) return;
    lastRfidStatsMs = now;

    Serial.print("RFID stats: framesOk=");
    Serial.print(rfidFramesOk);
    Serial.print(" badLen=");
    Serial.print(rfidFramesBadLen);
    Serial.print(" badEnd=");
    Serial.print(rfidFramesBadEnd);
    Serial.print(" checksumFail=");
    Serial.print(rfidFramesChecksumFail);
    Serial.print(" overflow=");
    Serial.print(rfidFramesOverflow);
    Serial.print(" timeout=");
    Serial.print(rfidFrameTimeouts);
    Serial.print(" dupBlocked=");
    Serial.print(rfidDuplicatesBlocked);
    Serial.print(" buffered=");
    Serial.print(bufferCountApprox());
    Serial.print(" bufOverflow=");
    Serial.print(overflowed ? 1 : 0);
    Serial.print(" mqttConn=");
    Serial.println(mqttConnected ? 1 : 0);
    overflowed = false;
  }

  void rfidTask(void* pv) {
    (void)pv;

    for (;;) {
      while (true) {
        uint8_t frame[RFID_EXPECTED_FRAME_LEN];
        size_t frameLen = 0;
        if (!readRFIDBinaryFrame(frame, sizeof(frame), &frameLen)) break;
        if (frameLen != RFID_EXPECTED_FRAME_LEN) continue;

        unsigned long readNow = millis();
        uint64_t chipId = chipIdFromBinaryFrame(frame);
        char chipHex[2 * RFID_CHIP_LEN + 1];
        char chipDec[24];

        chipIdHexCStr(frame, chipHex, sizeof(chipHex));
        chipIdDecCStr(chipId, chipDec, sizeof(chipDec));

        // Cheap duplicate guard: only block the same chip read directly after
        // itself; a bird staying on the coil re-queues every 4 s.
        if (shouldBlockDuplicate(chipDec, readNow)) {
          rfidDuplicatesBlocked++;
          continue;
        }

        rememberAcceptedChip(chipDec, readNow);
        bufferPush(chipDec, readNow);
        logRfidRead(chipHex, chipDec);
      }

      maybeLogRfidStats();
      vTaskDelay(pdMS_TO_TICKS(1));
    }
  }

  // ================== NTP ANKER ==================
  volatile bool ntpReady = false;
  volatile time_t ntpEpochAtAnchor = 0;
  volatile uint32_t millisAtAnchor = 0;

  void tryInitNtpAnchor() {
    if (ntpReady) return;
    if (WiFi.status() != WL_CONNECTED) return;

    static bool ntpStarted = false;
    if (!ntpStarted) {
      configTime(0, 0, "pool.ntp.org", "time.nist.gov");
      ntpStarted = true;
    }

    time_t now = time(nullptr);
    if (now > 1700000000) {
      ntpEpochAtAnchor = now;
      millisAtAnchor = millis();
      ntpReady = true;
    }
  }

  uint32_t timestampForEventUnixSeconds(const Event& e) {
    if (!ntpReady) return 0;

    int32_t deltaMs = (int32_t)(e.tMillis - millisAtAnchor);
    time_t eventEpoch = ntpEpochAtAnchor + (deltaMs / 1000);
    if (eventEpoch <= 0) return 0;
    return (uint32_t)eventEpoch;
  }

  // ================== MQTT SENDER TASK ==================
  void senderTask(void* pv) {
    (void)pv;

    for (;;) {
      if (!MQTT_TRANSPORT_ENABLED) {
        vTaskDelay(pdMS_TO_TICKS(1000));
        continue;
      }

      if (WiFi.status() == WL_CONNECTED) {
        tryInitNtpAnchor();
        initMqttClient();
        serviceMqttReconnect();

        if (mqttPublishInFlight) {
          if (mqttLastAckMsgId == mqttInFlightMsgId) {
            mqttLastAckMsgId = -1;
            bufferCommitPop();
            mqttPublishInFlight = false;
            mqttInFlightMsgId = -1;
            retryStreak = 0;
          } else if ((uint32_t)(millis() - mqttInFlightSinceMs) > MQTT_ACK_TIMEOUT_MS) {
            mqttAckTimeouts++;
            mqttPublishInFlight = false;
            mqttInFlightMsgId = -1;
            // Head-of-line: send the timed-out event to the back of the queue
            // so one problematic message never blocks newer reads.
            bufferRotateFrontToBack();
            if (retryStreak < 10) retryStreak++;
            scheduleNextRetry();
            Serial.println("MQTT error/retry.");
          }
        }

        if (!mqttPublishInFlight &&
            mqttConnected &&
            (int32_t)(millis() - nextSendAllowedMs) >= 0) {
          int n = bufferCountApprox();
          uint32_t oldest = bufferOldestAgeMs();

          int toSend = 1;
          if (n > 10 || oldest > 5000)  toSend = 3;
          if (n > 40 || oldest > 15000) toSend = 5;

          for (int i = 0; i < toSend; i++) {
            if ((int32_t)(millis() - nextSendAllowedMs) < 0) break;
            if (mqttPublishInFlight) break;

            Event e;
            if (!bufferPeek(e)) break;

            mqttPublishAttempts++;
            int msgId = publishEventToMqtt(e);

            if (msgId > 0) {
              mqttPublishAccepted++;
              mqttPublishInFlight = true;
              mqttInFlightMsgId = msgId;
              mqttInFlightSinceMs = millis();
              retryStreak = 0;
              break;
            }

            mqttPublishErrors++;            if (retryStreak < 10) retryStreak++;
            scheduleNextRetry();
            break;
          }
        }

        // V3 heartbeat: eget topic, QoS 0, fyres og glemmes — bufres ALDRI
        // og konkurrerer aldri med hendelseskøen. lastHeartbeatMs=0 etter
        // (re)connect gir umiddelbart første slag.
        if (mqttConnected &&
            (lastHeartbeatMs == 0 || millis() - lastHeartbeatMs >= HEARTBEAT_INTERVAL_MS)) {
          char hb[224];
          snprintf(hb, sizeof(hb),
            "{\"deviceId\":\"%s\",\"fw\":\"%s\",\"uptimeS\":%lu,\"rssi\":%d,\"queued\":%d,\"heapFree\":%u}",
            deviceId.c_str(), FW_VERSION,
            (unsigned long)(millis() / 1000UL),
            (int)WiFi.RSSI(),
            bufferCountApprox(),
            (unsigned)ESP.getFreeHeap());
          esp_mqtt_client_publish(mqttClient, presenceHeartbeatTopic.c_str(), hb, 0, 0, 0);
          lastHeartbeatMs = millis();
        }
      } else {
        mqttConnected = false;
      }

      vTaskDelay(pdMS_TO_TICKS(10));
    }
  }

  // ================== SETUP / LOOP ==================
  void setup() {
    Serial.begin(115200);
    randomSeed((uint32_t)micros());
    bootMs = millis();

    pinMode(WIFI_LED, OUTPUT);
    digitalWrite(WIFI_LED, LOW);
    setWifiLedMode(LED_MODE_BLINK_SLOW);

    // Burst tolerance: default RX buffer (256 B ≈ 25 frames) → 1024 B.
    RFID.setRxBufferSize(1024);
    RFID.begin(9600, SERIAL_8N1, RFID_RX, -1);

    bufMutex = xSemaphoreCreateMutex();
    if (!bufMutex) {
      Serial.println("FATAL: could not create mutex");
      while (true) delay(1000);
    }

    xTaskCreatePinnedToCore(
      rfidTask,
      "rfidTask",
      4096,
      nullptr,
      3,
      nullptr,
      1
    );

    pinMode(RESET_BUTTON, INPUT_PULLUP);

    deviceId = makeDeviceId();
    presenceStatusTopic = String(PRESENCE_STATUS_TOPIC_PREFIX) + deviceId + "/status";
    presenceHeartbeatTopic = String(PRESENCE_STATUS_TOPIC_PREFIX) + deviceId + "/hb";
    Serial.println("Device initialized.");
    Serial.print("FW: ");
    Serial.println(FW_VERSION);
    Serial.println("RFID monitor active on USB serial at 115200 baud.");
    Serial.println("Reader input: GPIO16 (Serial2 RX).");

    loadWifiCredentials();
    loadMqttCredentials();

    if (!connectWifiBlocking(WIFI_CONNECT_TIMEOUT_MS)) {
      startProvisioningPortal();
      return;
    }

    lastWifiAttempt = millis();

    xTaskCreatePinnedToCore(
      senderTask,
      "senderTask",
      8192,
      nullptr,
      1,
      nullptr,
      0
    );

    Serial.println("Ultra-streng async: task=RFID, task=MQTT.");
  }

  // V3: 10 s hold på BOOT → tøm WiFi og restart inn i portalen. Eneste
  // utvei før var omflashing hvis lagret nett ble ugyldig (byttet ruter).
  // Rask LED-blink kvitterer mens knappen holdes; slipp før 10 s = angre.
  void serviceResetButton() {
    if (digitalRead(RESET_BUTTON) == LOW) {
      if (resetButtonHeldSinceMs == 0) {
        resetButtonHeldSinceMs = millis();
      } else if (millis() - resetButtonHeldSinceMs >= RESET_HOLD_MS) {
        Serial.println("Reset button held 10s: clearing WiFi, reopening portal.");
        clearWifiCredentials();
        delay(200);
        ESP.restart();
      } else if (millis() - resetButtonHeldSinceMs >= 1500) {
        setWifiLedMode(LED_MODE_BLINK_FAST);
      }
    } else if (resetButtonHeldSinceMs != 0) {
      resetButtonHeldSinceMs = 0;
      setWifiLedMode(provisioningMode
        ? LED_MODE_BLINK_FAST
        : (WiFi.status() == WL_CONNECTED ? LED_MODE_SOLID : LED_MODE_BLINK_SLOW));
    }
  }

  void loop() {
    if (provisioningMode) {
      dnsServer.processNextRequest();
      server.handleClient();
      serviceWifiLed();
      serviceResetButton();
      delay(2);
      return;
    }

    unsigned long now = millis();

    // 1) WiFi/internett-status + reconnect
    if (WiFi.status() == WL_CONNECTED) {
      setWifiLedMode(LED_MODE_SOLID);
    } else {
      internetReachable = false;
      setWifiLedMode(LED_MODE_BLINK_SLOW);

      unsigned long retryMs =
        (now - bootMs <= FAST_RETRY_WINDOW_MS) ? FAST_RETRY_MS : SLOW_RETRY_MS;

      if (now - lastWifiAttempt >= retryMs) {
        if (!startWifi()) {
          startProvisioningPortal();
          return;
        }
        lastWifiAttempt = now;
      }
    }

    // 2) Service (reset-knappen etter WiFi-blokken så holde-blinken
    //    ikke overstyres av statusen samme runde)
    serviceResetButton();
    serviceWifiLed();
  }

