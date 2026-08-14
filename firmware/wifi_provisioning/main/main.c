/* WiFi provisioning: SoftAP + captive portal for first-boot setup, NVS
 * storage of credentials, and a station-mode retry on every subsequent boot.
 * This is the "essential" WiFi requirement from requirements.md/§1 & §4C
 * and architecture-plan.md Phase 2 - the ESP32's only route to the home
 * LAN/MQTT broker, since Phase 0 confirmed the BACnet Ethernet segment has
 * no default gateway.
 *
 * Flow:
 *   - Boot with saved credentials in NVS -> try connecting as a station.
 *     On success, stay connected (this milestone stops there; merging this
 *     with the BACnet client's Ethernet interface, and keeping the HTTP
 *     server up post-connect for ongoing BACnet/room config per
 *     requirements.md, is the next integration step - see the component's
 *     README).
 *   - No saved credentials, or the station connection fails/times out ->
 *     fall back to SoftAP + captive portal (based on ESP-IDF's
 *     examples/protocols/http_server/captive_portal) so the user can enter
 *     their WiFi details from a phone/laptop without installing anything.
 *   - Submitting the form saves credentials to NVS and reboots into the
 *     station-mode path above.
 */
#include <string.h>
#include <sys/param.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_mac.h"
#include "esp_system.h"

#include "nvs_flash.h"
#include "nvs.h"
#include "esp_wifi.h"
#include "esp_netif.h"
#include "lwip/inet.h"

#include "esp_http_server.h"
#include "dns_server.h"

#define EXAMPLE_ESP_WIFI_SSID CONFIG_ESP_WIFI_SSID
#define EXAMPLE_ESP_WIFI_PASS CONFIG_ESP_WIFI_PASSWORD
#define EXAMPLE_MAX_STA_CONN CONFIG_ESP_MAX_STA_CONN

#define NVS_NAMESPACE "wifi_cfg"
#define NVS_KEY_SSID "ssid"
#define NVS_KEY_PASS "pass"
#define STA_CONNECT_TIMEOUT_MS 25000

extern const char root_start[] asm("_binary_root_html_start");
extern const char root_end[] asm("_binary_root_html_end");

static const char *TAG = "wifi_prov";
static EventGroupHandle_t WifiEventGroup;
#define STA_CONNECTED_BIT BIT0
#define STA_FAILED_BIT BIT1

/* --- NVS credential storage --- */

static bool load_wifi_credentials(char *ssid, size_t ssid_len, char *pass, size_t pass_len)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_NAMESPACE, NVS_READONLY, &handle) != ESP_OK) {
        return false; /* namespace doesn't exist yet - never provisioned */
    }

    esp_err_t err_ssid = nvs_get_str(handle, NVS_KEY_SSID, ssid, &ssid_len);
    esp_err_t err_pass = nvs_get_str(handle, NVS_KEY_PASS, pass, &pass_len);
    nvs_close(handle);

    if (err_ssid != ESP_OK) {
        return false;
    }
    if (err_pass == ESP_ERR_NVS_NOT_FOUND) {
        pass[0] = '\0'; /* open network: password key legitimately absent */
    } else if (err_pass != ESP_OK) {
        return false;
    }
    return strlen(ssid) > 0;
}

static esp_err_t save_wifi_credentials(const char *ssid, const char *pass)
{
    nvs_handle_t handle;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &handle);
    if (err != ESP_OK) {
        return err;
    }
    err = nvs_set_str(handle, NVS_KEY_SSID, ssid);
    if (err == ESP_OK) {
        err = nvs_set_str(handle, NVS_KEY_PASS, pass);
    }
    if (err == ESP_OK) {
        err = nvs_commit(handle);
    }
    nvs_close(handle);
    return err;
}

/* --- Station mode: try connecting with saved credentials --- */

#define STA_MAX_RETRIES 5
static int StaRetryCount = 0;

static void sta_event_handler(
    void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        wifi_event_sta_disconnected_t *event = (wifi_event_sta_disconnected_t *)event_data;
        ESP_LOGW(
            TAG, "STA disconnected: reason=%d (attempt %d/%d)", event->reason,
            StaRetryCount + 1, STA_MAX_RETRIES);
        if (StaRetryCount < STA_MAX_RETRIES) {
            StaRetryCount++;
            vTaskDelay(pdMS_TO_TICKS(500)); /* brief backoff before retry */
            esp_wifi_connect();
        } else {
            xEventGroupSetBits(WifiEventGroup, STA_FAILED_BIT);
        }
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "STA got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        xEventGroupSetBits(WifiEventGroup, STA_CONNECTED_BIT);
    }
}

/* Returns true if it connected successfully. */
static bool try_connect_sta(const char *ssid, const char *pass)
{
    StaRetryCount = 0;
    WifiEventGroup = xEventGroupCreate();
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &sta_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &sta_event_handler, NULL));

    wifi_config_t wifi_config = {0};
    strlcpy((char *)wifi_config.sta.ssid, ssid, sizeof(wifi_config.sta.ssid));
    strlcpy((char *)wifi_config.sta.password, pass, sizeof(wifi_config.sta.password));

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "Connecting to saved SSID '%s'...", ssid);
    EventBits_t bits = xEventGroupWaitBits(
        WifiEventGroup, STA_CONNECTED_BIT | STA_FAILED_BIT, pdFALSE, pdFALSE,
        pdMS_TO_TICKS(STA_CONNECT_TIMEOUT_MS));

    if (bits & STA_CONNECTED_BIT) {
        return true;
    }

    ESP_LOGW(TAG, "STA connect failed/timed out - falling back to provisioning AP");
    ESP_ERROR_CHECK(esp_wifi_stop());
    ESP_ERROR_CHECK(esp_wifi_deinit());
    esp_event_handler_unregister(WIFI_EVENT, ESP_EVENT_ANY_ID, &sta_event_handler);
    esp_event_handler_unregister(IP_EVENT, IP_EVENT_STA_GOT_IP, &sta_event_handler);
    vEventGroupDelete(WifiEventGroup);
    return false;
}

/* --- SoftAP + captive portal fallback --- */

static void ap_event_handler(
    void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    if (event_id == WIFI_EVENT_AP_STACONNECTED) {
        wifi_event_ap_staconnected_t *event = (wifi_event_ap_staconnected_t *)event_data;
        ESP_LOGI(TAG, "station " MACSTR " join, AID=%d", MAC2STR(event->mac), event->aid);
    } else if (event_id == WIFI_EVENT_AP_STADISCONNECTED) {
        wifi_event_ap_stadisconnected_t *event = (wifi_event_ap_stadisconnected_t *)event_data;
        ESP_LOGI(
            TAG, "station " MACSTR " leave, AID=%d, reason=%d", MAC2STR(event->mac),
            event->aid, event->reason);
    }
}

static void wifi_init_softap(void)
{
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &ap_event_handler, NULL));

    /* Also bring up a (disconnected) STA netif alongside the AP - APSTA mode
       is what lets the device scan for nearby networks (esp_wifi_scan_start)
       while the setup AP stays up for the portal, so the setup page can
       offer a list of real, in-range networks instead of a blind text
       field. Users can't tell in advance if their router only broadcasts
       5GHz (which this ESP32 can't join) or an unsupported WiFi standard -
       a scanned list only shows what's actually joinable. */
    esp_netif_create_default_wifi_sta();

    wifi_config_t wifi_config = {
        .ap = {
            .ssid = EXAMPLE_ESP_WIFI_SSID,
            .ssid_len = strlen(EXAMPLE_ESP_WIFI_SSID),
            .password = EXAMPLE_ESP_WIFI_PASS,
            .max_connection = EXAMPLE_MAX_STA_CONN,
            .authmode = WIFI_AUTH_WPA_WPA2_PSK,
        },
    };
    if (strlen(EXAMPLE_ESP_WIFI_PASS) == 0) {
        wifi_config.ap.authmode = WIFI_AUTH_OPEN;
    }

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_APSTA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    esp_netif_ip_info_t ip_info;
    esp_netif_get_ip_info(esp_netif_get_handle_from_ifkey("WIFI_AP_DEF"), &ip_info);
    char ip_addr[16];
    inet_ntoa_r(ip_info.ip.addr, ip_addr, 16);
    ESP_LOGI(TAG, "Set up softAP with IP: %s", ip_addr);
    ESP_LOGI(TAG, "wifi_init_softap finished. SSID:'%s' password:'%s'", EXAMPLE_ESP_WIFI_SSID, EXAMPLE_ESP_WIFI_PASS);
}

static esp_err_t root_get_handler(httpd_req_t *req)
{
    const uint32_t root_len = root_end - root_start;
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, root_start, root_len);
    return ESP_OK;
}

static const httpd_uri_t root_uri = {
    .uri = "/",
    .method = HTTP_GET,
    .handler = root_get_handler,
};

/* JSON string escaping - only SSIDs (untrusted, attacker-adjacent input
   from the RF environment) pass through here, so escape defensively. */
static void json_escape(char *dst, const char *src, size_t dst_len)
{
    size_t di = 0;
    while (*src && di + 2 < dst_len) {
        if (*src == '"' || *src == '\\') {
            dst[di++] = '\\';
        }
        if ((unsigned char)*src < 0x20) {
            src++;
            continue; /* drop control chars rather than emit invalid JSON */
        }
        dst[di++] = *src++;
    }
    dst[di] = '\0';
}

static esp_err_t scan_get_handler(httpd_req_t *req)
{
    uint16_t max_aps = 30;
    wifi_ap_record_t ap_records[30];

    wifi_scan_config_t scan_config = {0};
    esp_err_t err = esp_wifi_scan_start(&scan_config, true /* block until done */);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Scan failed: %s", esp_err_to_name(err));
        httpd_resp_set_type(req, "application/json");
        httpd_resp_send(req, "[]", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    esp_wifi_scan_get_ap_records(&max_aps, ap_records);

    char *buf = malloc(4096);
    if (!buf) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    size_t off = 0;
    off += snprintf(buf + off, 4096 - off, "[");

    /* De-dupe by SSID (multiple APs/bands can share one network name),
       keeping the strongest signal seen for each. */
    char seen_ssid[30][33];
    int seen_count = 0;
    bool first = true;
    for (int i = 0; i < max_aps && off < 4000; i++) {
        if (ap_records[i].ssid[0] == '\0') {
            continue; /* hidden network - nothing to show/select */
        }
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp((char *)ap_records[i].ssid, seen_ssid[j]) == 0) {
                dup = true;
                break;
            }
        }
        if (dup) {
            continue;
        }
        if (seen_count < 30) {
            strlcpy(seen_ssid[seen_count], (char *)ap_records[i].ssid, sizeof(seen_ssid[0]));
            seen_count++;
        }

        char escaped[96];
        json_escape(escaped, (char *)ap_records[i].ssid, sizeof(escaped));
        bool secure = ap_records[i].authmode != WIFI_AUTH_OPEN;
        off += snprintf(
            buf + off, 4096 - off, "%s{\"ssid\":\"%s\",\"rssi\":%d,\"secure\":%s}",
            first ? "" : ",", escaped, ap_records[i].rssi, secure ? "true" : "false");
        first = false;
    }
    off += snprintf(buf + off, 4096 - off, "]");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, off);
    free(buf);
    return ESP_OK;
}

static const httpd_uri_t scan_uri = {
    .uri = "/scan",
    .method = HTTP_GET,
    .handler = scan_get_handler,
};

/* application/x-www-form-urlencoded decode - just enough for our two fields,
   no external dependency needed for a two-field form. */
static void url_decode(char *dst, const char *src, size_t dst_len)
{
    size_t di = 0;
    while (*src && di + 1 < dst_len) {
        if (*src == '+') {
            dst[di++] = ' ';
            src++;
        } else if (*src == '%' && src[1] && src[2]) {
            char hex[3] = {src[1], src[2], 0};
            dst[di++] = (char)strtol(hex, NULL, 16);
            src += 3;
        } else {
            dst[di++] = *src++;
        }
    }
    dst[di] = '\0';
}

static bool form_extract(const char *body, const char *key, char *out, size_t out_len)
{
    char needle[16];
    snprintf(needle, sizeof(needle), "%s=", key);
    const char *start = strstr(body, needle);
    if (!start) {
        return false;
    }
    start += strlen(needle);
    const char *end = strchr(start, '&');
    size_t raw_len = end ? (size_t)(end - start) : strlen(start);
    if (raw_len >= out_len * 3) { /* generous bound before url-decoding shrinks it */
        raw_len = out_len * 3 - 1;
    }
    char raw[196];
    size_t copy_len = raw_len < sizeof(raw) - 1 ? raw_len : sizeof(raw) - 1;
    memcpy(raw, start, copy_len);
    raw[copy_len] = '\0';
    url_decode(out, raw, out_len);
    return true;
}

static const char *confirm_page =
    "<!DOCTYPE html><html><body style='font-family:sans-serif;max-width:420px;margin:2em auto;'>"
    "<h1>Saved</h1><p>Restarting and connecting to your network. If it connects successfully "
    "this access point will disappear; if not, it will reappear so you can try again.</p>"
    "</body></html>";

static esp_err_t connect_post_handler(httpd_req_t *req)
{
    char body[256] = {0};
    int total = req->content_len < (int)sizeof(body) - 1 ? req->content_len : (int)sizeof(body) - 1;
    int received = httpd_req_recv(req, body, total);
    if (received <= 0) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    body[received] = '\0';

    char ssid[33] = {0};
    char pass[65] = {0};
    if (!form_extract(body, "ssid", ssid, sizeof(ssid)) || strlen(ssid) == 0) {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(req, "Missing SSID", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    form_extract(body, "password", pass, sizeof(pass));

    esp_err_t err = save_wifi_credentials(ssid, pass);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to save credentials: %s", esp_err_to_name(err));
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    ESP_LOGI(TAG, "Saved credentials for SSID '%s', restarting in 3s...", ssid);
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, confirm_page, HTTPD_RESP_USE_STRLEN);

    vTaskDelay(pdMS_TO_TICKS(3000));
    esp_restart();
    return ESP_OK; /* unreachable */
}

static const httpd_uri_t connect_uri = {
    .uri = "/connect",
    .method = HTTP_POST,
    .handler = connect_post_handler,
};

esp_err_t http_404_error_handler(httpd_req_t *req, httpd_err_code_t err)
{
    httpd_resp_set_status(req, "302 Temporary Redirect");
    httpd_resp_set_hdr(req, "Location", "/");
    httpd_resp_send(req, "Redirect to the captive portal", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static httpd_handle_t start_webserver(void)
{
    httpd_handle_t server = NULL;
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.max_open_sockets = 13;
    config.lru_purge_enable = true;

    ESP_LOGI(TAG, "Starting server on port: '%d'", config.server_port);
    if (httpd_start(&server, &config) == ESP_OK) {
        httpd_register_uri_handler(server, &root_uri);
        httpd_register_uri_handler(server, &scan_uri);
        httpd_register_uri_handler(server, &connect_uri);
        httpd_register_err_handler(server, HTTPD_404_NOT_FOUND, http_404_error_handler);
    }
    return server;
}

static void start_provisioning_ap(void)
{
    esp_log_level_set("httpd_uri", ESP_LOG_ERROR);
    esp_log_level_set("httpd_txrx", ESP_LOG_ERROR);
    esp_log_level_set("httpd_parse", ESP_LOG_ERROR);

    esp_netif_create_default_wifi_ap();
    wifi_init_softap();
    start_webserver();

    dns_server_config_t config =
        DNS_SERVER_CONFIG_SINGLE("*" /* all A queries */, "WIFI_AP_DEF" /* softAP netif ID */);
    start_dns_server(&config);

    ESP_LOGI(TAG, "Provisioning AP ready - connect to '%s' and open any website", EXAMPLE_ESP_WIFI_SSID);
}

void app_main(void)
{
    ESP_ERROR_CHECK(nvs_flash_init());
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    char ssid[33] = {0};
    char pass[65] = {0};
    if (load_wifi_credentials(ssid, sizeof(ssid), pass, sizeof(pass))) {
        if (try_connect_sta(ssid, pass)) {
            ESP_LOGI(TAG, "Connected to '%s' - provisioning not needed this boot", ssid);
            return; /* station mode stays up; see file header for next steps */
        }
    } else {
        ESP_LOGI(TAG, "No saved WiFi credentials found");
    }

    start_provisioning_ap();
}
