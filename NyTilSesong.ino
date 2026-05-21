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
const bool MQTT_TRANSPORT_ENABLED = true; // feature flag for easy rollback
const char* MQTT_BROKER_URI = "mqtt://34.34.165.200:1883";
const char* MQTT_TOPIC = "pigeonpal/ingest/raw/v1";
const char* MQTT_USERNAME = "";
const char* MQTT_PASSWORD = "";

  // ================== PINNER ==================
  #define RFID_RX    16
  #define RFID_TX    17
  #define WIFI_LED   2
  #define BUZZER_PIN 27

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
  bool isMainLoopContext();
  void timestampForEventIso(const Event& e, char* out, size_t outLen);

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

  // ================== BUZZER ==================
  const unsigned long BUZZER_MIN_GAP_MS = 250;
  const unsigned long BUZZER_PULSE_MS   = 40;

  unsigned long lastBeepMs = 0;
  bool buzzerOn = false;
  unsigned long buzzerOffAt = 0;

  void beepImmediateRateLimited() {
    unsigned long now = millis();
    if (now - lastBeepMs < BUZZER_MIN_GAP_MS) return;
    lastBeepMs = now;

    digitalWrite(BUZZER_PIN, HIGH);
    buzzerOn = true;
    buzzerOffAt = now + BUZZER_PULSE_MS;
  }

  void serviceBuzzer() {
    if (!buzzerOn) return;
    if (millis() >= buzzerOffAt) {
      digitalWrite(BUZZER_PIN, LOW);
      buzzerOn = false;
    }
  }

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
    if (strlen(MQTT_USERNAME) > 0) cfg.credentials.username = MQTT_USERNAME;
    if (strlen(MQTT_PASSWORD) > 0) cfg.credentials.authentication.password = MQTT_PASSWORD;
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

    char ts[32];
    timestampForEventIso(e, ts, sizeof(ts));

    String eventId = buildEventId(e);
    String payload = "{";
    payload += "\"deviceId\":\"" + deviceId + "\",";
    payload += "\"chipId\":\"" + String(e.chipIdDec) + "\",";
    payload += "\"timestamp\":\"" + String(ts) + "\",";
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
  const size_t RFID_MAX_HEX_LEN = 24;
  const unsigned long RFID_BYTE_GAP_TIMEOUT_MS = 50UL;
  char rfidBuf[RFID_MAX_HEX_LEN + 1] = {0};
  size_t rfidBufLen = 0;
  bool rfidInFrame = false;
  unsigned long rfidLastByteMs = 0;
  const unsigned long RFID_DUPLICATE_DEBOUNCE_MS = 2000UL;
  const int RFID_MIN_DEC_DIGITS = 10;
  uint32_t rfidBytesRead = 0;
  uint32_t rfidFramesOk = 0;
  uint32_t rfidFramesTooShort = 0;
  uint32_t rfidFramesOverflow = 0;
  uint32_t rfidTagsBuffered = 0;
  uint32_t rfidDuplicatesSuppressed = 0;
  unsigned long lastRfidStatsMs = 0;
  char lastAcceptedRfidHex[RFID_MAX_HEX_LEN + 1] = {0};
  unsigned long lastAcceptedRfidMs = 0;

  void resetRfidParser() {
    rfidInFrame = false;
    rfidBufLen = 0;
    rfidBuf[0] = '\0';
  }

  bool readRFIDHexFrame(char* out, size_t outLen) {
    while (RFID.available()) {
      uint8_t b = RFID.read();
      rfidBytesRead++;
      unsigned long now = millis();

      if (rfidInFrame && (now - rfidLastByteMs) > RFID_BYTE_GAP_TIMEOUT_MS) {
        resetRfidParser();
      }
      rfidLastByteMs = now;

      if (b == 0x02) {
        resetRfidParser();
        rfidInFrame = true;
        continue;
      }

      if (!rfidInFrame) continue;

      if (b == 0x03) {
        if (rfidBufLen >= 4) {
          if (outLen > 0) {
            size_t copyLen = rfidBufLen < (outLen - 1) ? rfidBufLen : (outLen - 1);
            memcpy(out, rfidBuf, copyLen);
            out[copyLen] = '\0';
          }
          resetRfidParser();
          rfidFramesOk++;
          return true;
        } else {
          resetRfidParser();
          rfidFramesTooShort++;
        }
      } else {
        char c = (char)b;
        if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')) {
          if (rfidBufLen < RFID_MAX_HEX_LEN) {
            rfidBuf[rfidBufLen++] = c;
            rfidBuf[rfidBufLen] = '\0';
          } else {
            resetRfidParser();
            rfidFramesOverflow++;
          }
        } else {
          resetRfidParser();
        }
      }
    }
    return false;
  }

  void hexToDecCStr(const char* hex, char* out, size_t outLen) {
    uint64_t v = strtoull(hex, nullptr, 16);
    snprintf(out, outLen, "%0*llu", RFID_MIN_DEC_DIGITS, (unsigned long long)v);
  }

  void logRfidRead(const char* hex, const char* chipDec) {
    Serial.println("RFID event queued.");
  }

  bool shouldBufferRfidTag(const char* hex, unsigned long now) {
    if (strcmp(hex, lastAcceptedRfidHex) == 0 &&
        (now - lastAcceptedRfidMs) < RFID_DUPLICATE_DEBOUNCE_MS) {
      rfidDuplicatesSuppressed++;
      return false;
    }

    strncpy(lastAcceptedRfidHex, hex, sizeof(lastAcceptedRfidHex) - 1);
    lastAcceptedRfidHex[sizeof(lastAcceptedRfidHex) - 1] = '\0';
    lastAcceptedRfidMs = now;
    return true;
  }

  void maybeLogRfidStats() {
    unsigned long now = millis();
    if (now - lastRfidStatsMs < 5000UL) return;
    lastRfidStatsMs = now;

    Serial.print("RFID stats: framesOk=");
    Serial.print(rfidFramesOk);
    Serial.print(" buffered=");
    Serial.print(rfidTagsBuffered);
    Serial.print(" mqttConn=");
    Serial.println(mqttConnected ? 1 : 0);
  }

  void rfidTask(void* pv) {
    (void)pv;

    for (;;) {
      while (true) {
        char hex[RFID_MAX_HEX_LEN + 1];
        if (!readRFIDHexFrame(hex, sizeof(hex))) break;

        unsigned long readNow = millis();
        if (!shouldBufferRfidTag(hex, readNow)) continue;

        char chipDec[24];
        hexToDecCStr(hex, chipDec, sizeof(chipDec));
        bufferPush(chipDec, readNow);
        rfidTagsBuffered++;
      }

      vTaskDelay(pdMS_TO_TICKS(1));
    }
  }

  // ================== NTP ANKER ==================
  volatile bool ntpReady = false;
  volatile time_t ntpEpochAtAnchor = 0;
  volatile uint32_t millisAtAnchor = 0;

  bool isMainLoopContext() {
    return xPortGetCoreID() == 1;
  }

  void tryInitNtpAnchor() {
    if (ntpReady) return;
    if (WiFi.status() != WL_CONNECTED) return;

    static bool ntpStarted = false;
    if (!ntpStarted) {
      if (!isMainLoopContext()) return;
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

  void epochToIsoZ(time_t epoch, char* out, size_t outLen) {
    struct tm tm;
    gmtime_r(&epoch, &tm);
    strftime(out, outLen, "%Y-%m-%dT%H:%M:%SZ", &tm);
  }

  void timestampForEventIso(const Event& e, char* out, size_t outLen) {
    if (!ntpReady) {
      snprintf(out, outLen, "%lu", (unsigned long)e.tMillis);
      return;
    }
    int32_t deltaMs = (int32_t)(e.tMillis - millisAtAnchor);
    time_t eventEpoch = ntpEpochAtAnchor + (deltaMs / 1000);
    epochToIsoZ(eventEpoch, out, outLen);
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

    pinMode(BUZZER_PIN, OUTPUT);
    digitalWrite(BUZZER_PIN, LOW);

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

    deviceId = makeDeviceId();
    Serial.println("Device initialized.");
    Serial.println("RFID monitor active on USB serial at 115200 baud.");
    Serial.println("Reader input: GPIO16 (Serial2 RX).");

    loadWifiCredentials();

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

  void loop() {
    if (provisioningMode) {
      setWifiLedMode(LED_MODE_BLINK_FAST);
      dnsServer.processNextRequest();
      server.handleClient();
      serviceWifiLed();
      serviceBuzzer();
      maybeLogRfidStats();
      delay(2);
      return;
    }

    unsigned long now = millis();

    // 1) WiFi/internett-status + reconnect
    if (WiFi.status() == WL_CONNECTED) {
      tryInitNtpAnchor();
      updateInternetReachability();

      if (internetReachable) setWifiLedMode(LED_MODE_SOLID);
      else setWifiLedMode(LED_MODE_BLINK_SLOW);
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

    // 2) Service
    serviceWifiLed();
    serviceBuzzer();
    maybeLogRfidStats();
  }

