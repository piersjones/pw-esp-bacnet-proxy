/* Merged firmware: dual-interface bring-up per requirements.md's network
 * model (S1) - Ethernet/W5500 for BACnet/IP only, WiFi for everything else
 * (config portal, and eventually MQTT/HA). Combines two previously separate,
 * independently-tested milestones:
 *   - firmware/bacnet_bridge (this project, pre-merge): BACnet/IP client
 *     proven against the real Delta panel - ReadProperty and WriteProperty
 *     both confirmed working.
 *   - firmware/wifi_provisioning: SoftAP + captive portal WiFi setup, NVS
 *     credential storage, station-mode retry, network scan - proven against
 *     a real home WiFi network.
 *
 * Both interfaces come up concurrently: Ethernet bring-up + the BACnet test
 * sequence run in their own task, while app_main() handles WiFi (bounded
 * duration either way - connects within ~25s or falls back to SoftAP
 * immediately) without waiting on Ethernet first.
 *
 * Once WiFi connects (or reconnects on a saved-credentials boot), the HTTP
 * server serves four post-connect pages instead of nothing: manage.html
 * (system power/Boost + per-room setpoint/power control), status.html
 * (WiFi/Ethernet/BACnet connection status, read-only, no BACnet transaction
 * needed), health.html (FCU performance/diagnostics - see below), and
 * reset.html (the "Reconfigure WiFi" action: clears NVS, reboots into the
 * SoftAP portal). Confirmed on real hardware: boots straight through
 * Ethernet+BACnet bring-up and WiFi STA reconnect (retried past the usual
 * transient WPA timeouts) to "Starting connected-mode dashboard server" with
 * no crash. A BacnetMutex serializes BACnet/IP transactions between the
 * boot-time milestone test and on-demand reads triggered by any of the
 * API handlers below, all of which run directly on the httpd task (given
 * a 24KB stack for exactly this reason - see start_connected_webserver()).
 *
 * health.html's point map (System-level cooling output/flow/valve-signal/
 * air-ΔT, per-room supply-air/required-vs-current thermal output, and the
 * alarm/valve-status pills) was fact-checked object-by-object against the
 * real 430-object EPICS dump (discovery/device_753016_epics.txt) before
 * being wired in - every object referenced is confirmed to exist with
 * matching name/units; none of it has been individually live-tested via
 * ReadProperty yet, and the alarm polarity (0=healthy) is inferred from the
 * dump's own active-text/inactive-text fields, not yet confirmed against a
 * real triggered fault.
 *
 * Still open (per requirements.md S4C / architecture-plan.md Phase 2 item
 * 6): BACnet target selection/discovery and a room-mapping editor -
 * deliberately deferred since rooms/points are still fixed in firmware
 * (Rooms[] in this file), not user-editable from the UI.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"
#include "esp_netif.h"
#include "esp_eth.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_mac.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "ethernet_init.h"
#include "lwip/ip4_addr.h"
#include "lwip/inet.h"
#include "lwip/sockets.h"
#include "lwip/netdb.h"
#include "ping/ping_sock.h"

#include "nvs_flash.h"
#include "nvs.h"
#include "esp_wifi.h"
#include "esp_http_server.h"
#include "dns_server.h"
#include "mqtt_client.h"
#include "mdns.h"
#include "esp_ota_ops.h"
#include "mbedtls/base64.h"

#include "bacnet/bacdef.h"
#include "bacnet/bacaddr.h"
#include "bacnet/bacapp.h"
#include "bacnet/bacenum.h"
#include "bacnet/npdu.h"
#include "bacnet/apdu.h"
#include "bacnet/rp.h"
#include "bacnet/wp.h"
#include "bacnet/bactext.h"
#include "bacnet/basic/binding/address.h"
#include "bacnet/basic/tsm/tsm.h"
#include "bacnet/basic/service/s_rp.h"
#include "bacnet/basic/service/s_wp.h"
#include "bacnet/basic/service/h_apdu.h"
#include "bacnet/basic/npdu/h_npdu.h"
#include "bacnet/datalink/bip.h"
#include "bacnet/iam.h"
#include "bacnet/whois.h"
#include "bacnet/basic/service/s_whois.h"

/* ===================== In-Memory Web Log Buffer ===================== */
#define LOG_BUFFER_SIZE 16384
static char LogBuffer[LOG_BUFFER_SIZE];
static size_t LogHead = 0;
static size_t LogCount = 0;
static portMUX_TYPE LogMux = portMUX_INITIALIZER_UNLOCKED;

static int web_log_vprintf(const char *fmt, va_list args)
{
    char temp[256];
    va_list copy;
    va_copy(copy, args);
    int len = vsnprintf(temp, sizeof(temp), fmt, copy);
    va_end(copy);

    if (len > 0) {
        taskENTER_CRITICAL(&LogMux);
        for (int i = 0; i < len; i++) {
            LogBuffer[LogHead] = temp[i];
            LogHead = (LogHead + 1) % LOG_BUFFER_SIZE;
            if (LogCount < LOG_BUFFER_SIZE) {
                LogCount++;
            }
        }
        taskEXIT_CRITICAL(&LogMux);
    }

    return vprintf(fmt, args);
}

/* ===================== Task spawning ===================== */

/* xTaskCreate returning pdFAIL is otherwise completely silent, and the stacks
   here are large enough (24KB) that "there is free heap, just not 24KB of it in
   one contiguous internal block" is a real outcome once WiFi, httpd, the MQTT
   client, and mqtt_state have each taken their share. That is
   exactly how the BACnet client task once vanished at boot: nothing was
   created, nothing was logged, BacnetReady stayed false forever, and every
   reading on the dashboard went blank with no clue as to why. Route every
   xTaskCreate through here so a failed spawn is loud and carries the heap
   numbers needed to size the stack down. */
static bool spawn_task(
    TaskFunction_t fn, const char *name, uint32_t stack_bytes, void *arg,
    UBaseType_t prio, TaskHandle_t *out_handle)
{
    if (xTaskCreate(fn, name, stack_bytes, arg, prio, out_handle) == pdPASS) {
        return true;
    }
    ESP_LOGE(
        "MAIN",
        "xTaskCreate(\"%s\", %u bytes) FAILED - free internal heap %u, "
        "largest free block %u",
        name, (unsigned)stack_bytes,
        (unsigned)heap_caps_get_free_size(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL),
        (unsigned)heap_caps_get_largest_free_block(
            MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL));
    return false;
}

/* Bytes of stack that named task has NEVER touched, at any point since it
   started - i.e. (requested stack size - this) is the true worst-case peak
   usage observed so far. Every large stack size in this file (24576, chosen
   for bacnet_client_run() back when 8KB genuinely overflowed) was inherited by
   several other tasks that never had their own usage measured, which is
   exactly how a later blind guess at a smaller number (8192, for the esp-mqtt
   client's own task) silently corrupted memory in the field instead of failing
   cleanly - this build's CONFIG_COMPILER_STACK_CHECK_MODE_NONE and
   canary-only FreeRTOS overflow checking do not reliably catch that. Returns
   -1 if no task by that name is currently running. */
static int task_stack_headroom_bytes(const char *name)
{
    TaskHandle_t h = xTaskGetHandle(name);
    if (!h) {
        return -1;
    }
    return (int)(uxTaskGetStackHighWaterMark(h) * sizeof(StackType_t));
}

/* ===================== BACnet/Ethernet side ===================== */

#define DEFAULT_TARGET_DEVICE_INSTANCE 753016
#define DEFAULT_TARGET_IP "10.0.3.16"
#define DEFAULT_TARGET_PORT 47808

static uint32_t TargetDeviceInstance = DEFAULT_TARGET_DEVICE_INSTANCE;
static char TargetIp[16] = DEFAULT_TARGET_IP;
static uint16_t TargetPort = DEFAULT_TARGET_PORT;

/* This device's static IP on the isolated BACnet segment (Phase 1). */
#define LOCAL_STATIC_IP "10.0.3.99"
#define LOCAL_NETMASK "255.255.0.0"
#define LOCAL_GATEWAY "0.0.0.0"

#define SYS_POWER_WRITE_INSTANCE 13 /* `BMS Run Signal` (binary-value) - confirmed write target */
#define SYS_POWER_READBACK_INSTANCE 1 /* `FCU Run Status` (binary-value) - read-only, can diverge from BMS Run Signal */
#define BOOST_INSTANCE 1 /* `FCU Operating Mode` (multi-state-value) - confirmed via Phase 0.5 diff */
#define BOOST_MODE_AUTO 1
#define BOOST_MODE_FULL_HEATING 4
#define BOOST_MODE_FULL_COOLING 5

#define BOOST_TIMEOUT_DEFAULT_MIN 60
#define BOOST_TIMEOUT_MAX_MIN 240

static uint16_t BoostTimeoutMinutes = BOOST_TIMEOUT_DEFAULT_MIN;
static bool BoostRevertExternal = false;
static bool BoostSelfInitiated = false;
static int64_t BoostDeadlineUs = 0;

static bool boost_apply(unsigned mode, bool self_initiated);
static void app_config_save(void);

static int boost_remaining_minutes(void)
{
    if (BoostDeadlineUs == 0) {
        return 0;
    }
    int64_t remaining_us = BoostDeadlineUs - esp_timer_get_time();
    if (remaining_us <= 0) {
        return 0;
    }
    return (int)((remaining_us + 59999999) / 60000000);
}

#define MIN_SETPOINT_C 18.0f

typedef struct {
    char name[32];
    bool active;
    uint32_t setpoint_instance;    /* analog-value, `Room_x Setpoint` */
    uint32_t temperature_instance; /* analog-value, `Room_x Temperature`, read-only */
    uint32_t power_instance;       /* binary-value, `Room_x Run Status` */
    uint32_t supply_air_instance;    /* analog-value, `Room_x Supply Air Temperature`, read-only */
    uint32_t required_output_instance; /* analog-value, `Room_x Required Thermal Output` (kW), read-only */
    uint32_t current_output_instance;  /* analog-value, `Room_x Current Thermal Output` (kW), read-only */
} room_config_t;

#define MAX_ROOMS 8
static room_config_t Rooms[MAX_ROOMS] = {
    { "Room A", true, 1100, 1101, 1101, 1102, 1105, 1106 },
    { "Room B", true, 1200, 1201, 1201, 1202, 1205, 1206 },
    { "Room C", false, 1300, 1301, 1301, 1302, 1305, 1306 },
    { "Room D", false, 1400, 1401, 1401, 1402, 1405, 1406 },
    { "Room E", false, 1500, 1501, 1501, 1502, 1505, 1506 },
};
static size_t RoomCount = 5;

#define NVS_TARGET_NAMESPACE "nvs_target"
#define NVS_ROOMS_NAMESPACE "nvs_rooms"

static void target_config_load(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_TARGET_NAMESPACE, NVS_READONLY, &handle) != ESP_OK) {
        return;
    }
    size_t ip_len = sizeof(TargetIp);
    nvs_get_str(handle, "ip", TargetIp, &ip_len);
    nvs_get_u16(handle, "port", &TargetPort);
    nvs_get_u32(handle, "dev_id", &TargetDeviceInstance);
    nvs_close(handle);
}

static void target_config_save(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_TARGET_NAMESPACE, NVS_READWRITE, &handle) != ESP_OK) {
        return;
    }
    nvs_set_str(handle, "ip", TargetIp);
    nvs_set_u16(handle, "port", TargetPort);
    nvs_set_u32(handle, "dev_id", TargetDeviceInstance);
    nvs_commit(handle);
    nvs_close(handle);
}

static const room_config_t DefaultRooms[MAX_ROOMS] = {
    { "Room A", true, 1100, 1101, 1101, 1102, 1105, 1106 },
    { "Room B", true, 1200, 1201, 1201, 1202, 1205, 1206 },
    { "Room C", false, 1300, 1301, 1301, 1302, 1305, 1306 },
    { "Room D", false, 1400, 1401, 1401, 1402, 1405, 1406 },
    { "Room E", false, 1500, 1501, 1501, 1502, 1505, 1506 },
};

static void rooms_config_load(void)
{
    /* Initialize with known working defaults first */
    memcpy(Rooms, DefaultRooms, sizeof(DefaultRooms));
    RoomCount = 5;

    nvs_handle_t handle;
    if (nvs_open(NVS_ROOMS_NAMESPACE, NVS_READONLY, &handle) != ESP_OK) {
        return;
    }
    uint8_t count = 0;
    if (nvs_get_u8(handle, "count", &count) == ESP_OK && count > 0 && count <= MAX_ROOMS) {
        RoomCount = count;
        for (size_t i = 0; i < RoomCount; i++) {
            char key[32];
            snprintf(key, sizeof(key), "r%u_name", (unsigned)i);
            size_t nlen = sizeof(Rooms[i].name);
            nvs_get_str(handle, key, Rooms[i].name, &nlen);

            snprintf(key, sizeof(key), "r%u_act", (unsigned)i);
            uint8_t act = 0;
            if (nvs_get_u8(handle, key, &act) == ESP_OK) {
                Rooms[i].active = (act != 0);
            }

            snprintf(key, sizeof(key), "r%u_sp", (unsigned)i);
            uint32_t sp = 0;
            if (nvs_get_u32(handle, key, &sp) == ESP_OK && sp > 0) Rooms[i].setpoint_instance = sp;

            snprintf(key, sizeof(key), "r%u_temp", (unsigned)i);
            uint32_t temp = 0;
            if (nvs_get_u32(handle, key, &temp) == ESP_OK && temp > 0) Rooms[i].temperature_instance = temp;

            snprintf(key, sizeof(key), "r%u_pwr", (unsigned)i);
            uint32_t pwr = 0;
            if (nvs_get_u32(handle, key, &pwr) == ESP_OK && pwr > 0) Rooms[i].power_instance = pwr;

            snprintf(key, sizeof(key), "r%u_sa", (unsigned)i);
            uint32_t sa = 0;
            if (nvs_get_u32(handle, key, &sa) == ESP_OK && sa > 0) Rooms[i].supply_air_instance = sa;

            snprintf(key, sizeof(key), "r%u_req", (unsigned)i);
            uint32_t req = 0;
            if (nvs_get_u32(handle, key, &req) == ESP_OK && req > 0) Rooms[i].required_output_instance = req;

            snprintf(key, sizeof(key), "r%u_cur", (unsigned)i);
            uint32_t cur = 0;
            if (nvs_get_u32(handle, key, &cur) == ESP_OK && cur > 0) Rooms[i].current_output_instance = cur;
        }
    }
    nvs_close(handle);

    /* Sanity check defaults for any missing instances/names */
    for (size_t i = 0; i < RoomCount; i++) {
        if (Rooms[i].setpoint_instance == 0) Rooms[i].setpoint_instance = DefaultRooms[i < 5 ? i : 0].setpoint_instance;
        if (Rooms[i].temperature_instance == 0) Rooms[i].temperature_instance = DefaultRooms[i < 5 ? i : 0].temperature_instance;
        if (Rooms[i].power_instance == 0) Rooms[i].power_instance = DefaultRooms[i < 5 ? i : 0].power_instance;
        if (Rooms[i].supply_air_instance == 0) Rooms[i].supply_air_instance = DefaultRooms[i < 5 ? i : 0].supply_air_instance;
        if (Rooms[i].required_output_instance == 0) Rooms[i].required_output_instance = DefaultRooms[i < 5 ? i : 0].required_output_instance;
        if (Rooms[i].current_output_instance == 0) Rooms[i].current_output_instance = DefaultRooms[i < 5 ? i : 0].current_output_instance;
        if (Rooms[i].name[0] == '\0') {
            strlcpy(Rooms[i].name, DefaultRooms[i < 5 ? i : 0].name, sizeof(Rooms[i].name));
        }
    }
}

static void rooms_config_save(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_ROOMS_NAMESPACE, NVS_READWRITE, &handle) != ESP_OK) {
        return;
    }
    nvs_set_u8(handle, "count", (uint8_t)RoomCount);
    for (size_t i = 0; i < RoomCount; i++) {
        char key[32];
        snprintf(key, sizeof(key), "r%u_name", (unsigned)i);
        nvs_set_str(handle, key, Rooms[i].name);

        snprintf(key, sizeof(key), "r%u_act", (unsigned)i);
        nvs_set_u8(handle, key, Rooms[i].active ? 1 : 0);

        snprintf(key, sizeof(key), "r%u_sp", (unsigned)i);
        nvs_set_u32(handle, key, Rooms[i].setpoint_instance);
        snprintf(key, sizeof(key), "r%u_temp", (unsigned)i);
        nvs_set_u32(handle, key, Rooms[i].temperature_instance);
        snprintf(key, sizeof(key), "r%u_pwr", (unsigned)i);
        nvs_set_u32(handle, key, Rooms[i].power_instance);
        snprintf(key, sizeof(key), "r%u_sa", (unsigned)i);
        nvs_set_u32(handle, key, Rooms[i].supply_air_instance);
        snprintf(key, sizeof(key), "r%u_req", (unsigned)i);
        nvs_set_u32(handle, key, Rooms[i].required_output_instance);
        snprintf(key, sizeof(key), "r%u_cur", (unsigned)i);
        nvs_set_u32(handle, key, Rooms[i].current_output_instance);
    }
    nvs_commit(handle);
    nvs_close(handle);
}

/* Whole-unit health/performance objects, confirmed to exist with matching
   name/units against discovery/device_753016_epics.txt (not yet individually
   live-tested via ReadProperty - see architecture-plan.md). Naming keeps the
   "Cooling Valve"/"Fan N" qualifiers from the real object names rather than
   the generic "Flow"/"Supply Air" labels an earlier draft of this page used,
   since this unit also has separate heating-valve and per-fan objects that
   those generic names would collide with. */
/* Design (nameplate) max, NOT a live demand - useful as the reference the
   "required" figure gets pinned to when the unit is forced to full output.
   Confirmed on real hardware: reads 4.40 kW, identical to what
   `Overall Required Cooling Output` reports while Boost is in Full Cooling. */
#define HEALTH_DESIGN_COOLING_DUTY_INSTANCE 25 /* AV: "FCU Design Cooling Duty (Sensible)" (kW) */
#define HEALTH_COOLING_OUTPUT_INSTANCE 26   /* AV: "Overall Current Cooling Output" (kW) */
#define HEALTH_REQUIRED_OUTPUT_INSTANCE 27  /* AV: "Overall Required Cooling Output" (kW) */
#define HEALTH_COOLING_FLOW_INSTANCE 42     /* AV: "Cooling Valve Current Flow Rate" (L/s) */
#define HEALTH_REQUIRED_FLOW_INSTANCE 43    /* AV: "Cooling Valve Required Flow Rate" (L/s) */
#define HEALTH_FLOW_DESIGN_PCT_INSTANCE 45  /* AV: "Cooling Valve Percentage of Design Flow" (%), controller-computed */
#define HEALTH_VALVE_SIGNAL_INSTANCE 8      /* AO: "Cooling Valve Control Signal" (%) */

#define HEALTH_DESIGN_HEATING_DUTY_INSTANCE 20 /* AV: "FCU Design Heating Duty" (kW) */
#define HEALTH_HEATING_OUTPUT_INSTANCE 21      /* AV: "Overall Current Heating Output" (kW) */
#define HEALTH_REQUIRED_HEATING_OUTPUT_INSTANCE 22 /* AV: "Overall Required Heating Output" (kW) */
#define HEALTH_HEATING_FLOW_INSTANCE 32        /* AV: "Heating Valve Current Flow Rate" (L/s) */
#define HEALTH_REQUIRED_HEATING_FLOW_INSTANCE 33 /* AV: "Heating Valve Required Flow Rate" (L/s) */
#define HEALTH_HEATING_FLOW_DESIGN_PCT_INSTANCE 35 /* AV: "Heating Valve Percentage of Design Flow" (%) */
#define HEALTH_HEATING_VALVE_SIGNAL_INSTANCE 7 /* AO: "Heating Valve / Element 1 Control Signal" (%) */

#define HEALTH_RETURN_AIR_INSTANCE 9        /* AI: "Return Air Temperature Sensor" */
/* AI:1-5 are "Fan N Supply Air Temperature Sensor" - not every unit has all
   5 fans wired/enabled, so the health handler reads all 5 and averages
   whichever ones succeed rather than assuming a fixed count. */
#define HEALTH_FAN_SUPPLY_AIR_COUNT 5
static const uint32_t HealthFanSupplyAirInstances[HEALTH_FAN_SUPPLY_AIR_COUNT] = {1, 2, 3, 4, 5};

#define HEALTH_FAN_SPEED_COUNT 4
static const uint32_t HealthFanSpeedInstances[HEALTH_FAN_SPEED_COUNT] = {1, 2, 3, 4}; /* AO: 1..4 "Fan N Speed Signal" */

/* Fans are wired one-per-room in configuration order - fan N (1-based, matches
   the instance numbers above) serves the Nth *active* entry in Rooms[]. There
   is no BACnet point that states this mapping; it is the installer's own
   confirmed assumption, asserted once here and reused by every fan
   display/discovery site instead of each guessing independently. */
static int fan_room_count(void)
{
    int n = 0;
    for (size_t i = 0; i < RoomCount && n < HEALTH_FAN_SUPPLY_AIR_COUNT; i++) {
        if (Rooms[i].active) {
            n++;
        }
    }
    return n;
}

/* fan_index is 1-based. Returns false (leaving out untouched) if there is no
   room at that position - callers should only be asking for
   fan_index <= fan_room_count(). */
static bool fan_room_name(int fan_index, char *out, size_t out_size)
{
    int seen = 0;
    for (size_t i = 0; i < RoomCount; i++) {
        if (!Rooms[i].active) {
            continue;
        }
        seen++;
        if (seen == fan_index) {
            strlcpy(out, Rooms[i].name, out_size);
            return true;
        }
    }
    return false;
}

/* How many of the 5 fan channels are actually fitted. The controller knows:
   `Number of FCU Fans` reads 2.0 on this unit (one fan per room), and its
   own description is "Selects number of fan outputs to use...". Always read
   it rather than assuming 5 - fan count is exactly the kind of thing that
   differs between installations.

   This matters because a BACnet read SUCCEEDING says nothing about whether
   the sensor is physically fitted: this unit reports all 5 Fan Supply Air
   Temperature Sensors as readable with out-of-service FALSE and no
   reliability fault. Channels 4 and 5 sit at -40C (open-circuit
   thermistor), but channel 3 reads a perfectly plausible ~20.5C despite
   there being no third fan - so a plausibility range alone would NOT have
   caught it. The fan count is the authoritative filter; the range check
   below is only a secondary signal for a fitted-but-faulty sensor. */
#define HEALTH_FAN_COUNT_INSTANCE 50 /* AV: "Number of FCU Fans" */
#define PLAUSIBLE_TEMP_MIN_C (-10.0f)
#define PLAUSIBLE_TEMP_MAX_C 60.0f
static inline bool temp_is_plausible(float c)
{
    return c >= PLAUSIBLE_TEMP_MIN_C && c <= PLAUSIBLE_TEMP_MAX_C;
}

#define HEALTH_COOLING_VALVE_STATUS_INSTANCE 4 /* MSV: "Cooling Valve Status" - 1=CLOSED 2=CONTROLLING 3=RESYNCING */
#define HEALTH_HEATING_VALVE_STATUS_INSTANCE 3 /* MSV: "Heating Valve Status" - same state text */
static const char *HealthValveStatusNames[] = {"Unknown", "Closed", "Controlling", "Resyncing"};

/* Alarm binary-values: active-text/inactive-text confirmed "ON"/"OFF" in the
   EPICS dump, i.e. ACTIVE(1)=fault, INACTIVE(0)=healthy - not live-tripped
   yet (see architecture-plan.md), so treat with normal caution until a real
   fault has been observed and confirmed to actually set one of these. */
typedef struct {
    const char *label;
    uint32_t instance;
} health_alarm_t;
static const health_alarm_t HealthAlarms[] = {
    {"Master Alarm", 20},
    {"Filter Alarm", 22},
    {"Replace Filter Alarm", 30},
    {"Condensate Alarm", 23},
    {"Fan Failure Alarm", 24},
    {"Sensor Failure Alarm", 25},
    {"Element Cutout Alarm", 28},
};
#define HEALTH_ALARM_COUNT (sizeof(HealthAlarms) / sizeof(HealthAlarms[0]))

static const char *TAG_BAC = "bacnet_client";

static volatile bool EthConnected = false;
static esp_netif_t *EthNetif = NULL;

static BACNET_ADDRESS Target_Address;
static uint8_t Request_Invoke_ID = 0;
static volatile bool Reply_Received = false;
static volatile bool Reply_Errored = false;
static BACNET_APPLICATION_DATA_VALUE Last_Read_Value;
/* Raw (still-encoded) copy of the same ack's application data, captured
   alongside Last_Read_Value. Needed by the /api/bacnet/explore endpoint to
   decode array properties (e.g. Priority_Array's 16 back-to-back
   application-tagged values) that bacapp_decode_application_data alone only
   ever gives the first element of - every other reader in this file only
   ever wants that first element, so they're untouched by this. */
static uint8_t Last_Read_Raw[MAX_APDU];
static int Last_Read_Raw_Len = 0;

/* Guards the shared transaction state above (Target_Address, Request_Invoke_ID,
   Reply_Received/Reply_Errored, Last_Read_Value) and the BACnet/IP socket, so the boot-time milestone
   test and later on-demand reads triggered by HTTP status requests (a
   different task) never interleave transactions. Created in app_main() before
   any task that touches BACnet starts. */
static SemaphoreHandle_t BacnetMutex;
static volatile bool BacnetReady = false;
static volatile bool DiscoveryNeedsBacnetRefresh = true;
/* Cleared on every MQTT (re)connect - see mqtt_state_task for why the refresh
   flag alone is not enough to rate-limit discovery publishes. */
static volatile bool DiscoveryPublishedOnce = false;
static char TargetDeviceName[MAX_CHARACTER_STRING_BYTES + 1] = {0};
static volatile bool TargetDeviceNameValid = false;

static void eth_event_handler(
    void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    switch (event_id) {
        case ETHERNET_EVENT_CONNECTED:
            ESP_LOGI(TAG_BAC, "Ethernet Link Up");
            EthConnected = true;
            break;
        case ETHERNET_EVENT_DISCONNECTED:
            ESP_LOGI(TAG_BAC, "Ethernet Link Down");
            EthConnected = false;
            break;
        default:
            break;
    }
}

static void my_error_handler(
    BACNET_ADDRESS *src,
    uint8_t invoke_id,
    BACNET_ERROR_CLASS error_class,
    BACNET_ERROR_CODE error_code)
{
    if (address_match(&Target_Address, src) && (invoke_id == Request_Invoke_ID)) {
        ESP_LOGE(TAG_BAC, "BACnet Error: class=%d code=%d", error_class, error_code);
        Reply_Errored = true;
    }
}

static void my_abort_handler(
    BACNET_ADDRESS *src, uint8_t invoke_id, uint8_t abort_reason, bool server)
{
    (void)server;
    if (address_match(&Target_Address, src) && (invoke_id == Request_Invoke_ID)) {
        ESP_LOGE(TAG_BAC, "BACnet Abort: reason=%d", abort_reason);
        Reply_Errored = true;
    }
}

static void my_reject_handler(
    BACNET_ADDRESS *src, uint8_t invoke_id, uint8_t reject_reason)
{
    if (address_match(&Target_Address, src) && (invoke_id == Request_Invoke_ID)) {
        ESP_LOGE(TAG_BAC, "BACnet Reject: reason=%d", reject_reason);
        Reply_Errored = true;
    }
}

static void my_read_property_ack_handler(
    uint8_t *service_request,
    uint16_t service_len,
    BACNET_ADDRESS *src,
    BACNET_CONFIRMED_SERVICE_ACK_DATA *service_data)
{
    BACNET_READ_PROPERTY_DATA data;
    int len;

    if (!(address_match(&Target_Address, src) &&
          (service_data->invoke_id == Request_Invoke_ID))) {
        return;
    }

    len = rp_ack_decode_service_request(service_request, service_len, &data);
    if (len < 0) {
        ESP_LOGE(TAG_BAC, "ReadProperty ack: decode failed");
        Reply_Errored = true;
        return;
    }

    Last_Read_Raw_Len = data.application_data_len < (int)sizeof(Last_Read_Raw)
        ? data.application_data_len : (int)sizeof(Last_Read_Raw);
    memcpy(Last_Read_Raw, data.application_data, Last_Read_Raw_Len);

    if (bacapp_decode_application_data(
            data.application_data, (uint32_t)data.application_data_len,
            &Last_Read_Value) < 0) {
        ESP_LOGE(TAG_BAC, "ReadProperty ack: value decode failed");
        Reply_Errored = true;
        return;
    }
    Reply_Received = true;
}

static void my_write_property_simple_ack_handler(
    BACNET_ADDRESS *src, uint8_t invoke_id)
{
    if (address_match(&Target_Address, src) && (invoke_id == Request_Invoke_ID)) {
        ESP_LOGI(TAG_BAC, "WriteProperty ack: success (Simple-ACK)");
        Reply_Received = true;
    }
}

typedef struct {
    uint32_t device_id;
    char ip[16];
    uint16_t port;
    uint16_t vendor_id;
    char name[48];
    char vendor_name[48];
    char model_name[48];
} discovered_bacnet_dev_t;

#define MAX_DISCOVERED_DEVICES 8
static discovered_bacnet_dev_t DiscoveredDevices[MAX_DISCOVERED_DEVICES];
static size_t DiscoveredDeviceCount = 0;

static void my_i_am_handler(
    uint8_t *service_request, uint16_t len, BACNET_ADDRESS *src)
{
    (void)len;
    uint32_t device_id = 0;
    unsigned max_apdu = 0;
    int segmentation = 0;
    uint16_t vendor_id = 0;

    int decoded = iam_decode_service_request(
        service_request, &device_id, &max_apdu, &segmentation, &vendor_id);
    if (decoded <= 0 || !src) {
        return;
    }

    char ip_str[16] = {0};
    snprintf(ip_str, sizeof(ip_str), "%u.%u.%u.%u",
             src->mac[0], src->mac[1], src->mac[2], src->mac[3]);
    uint16_t port = 0;
    memcpy(&port, &src->mac[4], 2);
    if (port == 0) port = 47808;

    ESP_LOGI(TAG_BAC, "I-Am received: Device %u from %s:%u (Vendor %u)",
             (unsigned)device_id, ip_str, (unsigned)port, (unsigned)vendor_id);

    /* Bind in address cache */
    address_add(device_id, max_apdu, src);

    /* Deduplicate */
    for (size_t i = 0; i < DiscoveredDeviceCount; i++) {
        if (DiscoveredDevices[i].device_id == device_id &&
            strcmp(DiscoveredDevices[i].ip, ip_str) == 0) {
            return;
        }
    }

    if (DiscoveredDeviceCount < MAX_DISCOVERED_DEVICES) {
        size_t idx = DiscoveredDeviceCount++;
        DiscoveredDevices[idx].device_id = device_id;
        strlcpy(DiscoveredDevices[idx].ip, ip_str, sizeof(DiscoveredDevices[idx].ip));
        DiscoveredDevices[idx].port = port;
        DiscoveredDevices[idx].vendor_id = vendor_id;
        snprintf(DiscoveredDevices[idx].vendor_name, sizeof(DiscoveredDevices[idx].vendor_name),
                 vendor_id == 8 ? "Delta Controls" : "Vendor %u", (unsigned)vendor_id);
        strlcpy(DiscoveredDevices[idx].model_name, "DAC Controller", sizeof(DiscoveredDevices[idx].model_name));
        snprintf(DiscoveredDevices[idx].name, sizeof(DiscoveredDevices[idx].name),
                 "Device %u", (unsigned)device_id);
    }
}

static void init_bacnet_handlers(void)
{
    apdu_set_confirmed_ack_handler(
        SERVICE_CONFIRMED_READ_PROPERTY, my_read_property_ack_handler);
    apdu_set_confirmed_simple_ack_handler(
        SERVICE_CONFIRMED_WRITE_PROPERTY, my_write_property_simple_ack_handler);
    apdu_set_unconfirmed_handler(
        SERVICE_UNCONFIRMED_I_AM, my_i_am_handler);
    apdu_set_error_handler(SERVICE_CONFIRMED_READ_PROPERTY, my_error_handler);
    apdu_set_error_handler(SERVICE_CONFIRMED_WRITE_PROPERTY, my_error_handler);
    apdu_set_abort_handler(my_abort_handler);
    apdu_set_reject_handler(my_reject_handler);
}

/* Mirrors `bacrp --mac <ip>:<port> --dnet 0` - pre-seed the address cache
   directly instead of Who-Is/I-Am discovery (this panel doesn't answer
   broadcast Who-Is). See bacnet_client's README for the mac[4:6] port
   byte-order note - this deliberately does NOT use
   bacnet_address_mac_from_ascii(). */
static void bind_target_device(void)
{
    BACNET_ADDRESS dest = {0};
    uint16_t native_port = TargetPort;
    const char *ip_octets = TargetIp;
    unsigned a, b, c, d;

    sscanf(ip_octets, "%u.%u.%u.%u", &a, &b, &c, &d);
    dest.mac[0] = (uint8_t)a;
    dest.mac[1] = (uint8_t)b;
    dest.mac[2] = (uint8_t)c;
    dest.mac[3] = (uint8_t)d;
    memcpy(&dest.mac[4], &native_port, 2);
    dest.mac_len = 6;
    dest.net = 0;
    dest.len = 0;

    address_add(TargetDeviceInstance, MAX_APDU, &dest);
}

static bool wait_for_transaction(void)
{
    if (Request_Invoke_ID == 0) {
        return false;
    }

    uint32_t last_tick = xTaskGetTickCount();

    /* 20 iterations of 25ms = 500ms max timeout. BACnet over local IP responds in < 50ms. */
    for (int i = 0; i < 20; i++) {
        uint8_t pdu[BIP_MPDU_MAX] = {0};
        BACNET_ADDRESS src = {0};
        uint16_t pdu_len = bip_receive(&src, pdu, sizeof(pdu), 0);
        if (pdu_len) {
            npdu_handler(&src, pdu, pdu_len);
        }

        uint32_t now = xTaskGetTickCount();
        uint32_t elapsed_ms = (now - last_tick) * portTICK_PERIOD_MS;
        if (elapsed_ms >= 100) {
            tsm_timer_milliseconds(elapsed_ms);
            last_tick = now;
        }

        if (Reply_Received || Reply_Errored) {
            return Reply_Received;
        }
        if (tsm_invoke_id_free(Request_Invoke_ID)) {
            return false;
        }
        if (tsm_invoke_id_failed(Request_Invoke_ID)) {
            tsm_free_invoke_id(Request_Invoke_ID);
            return false;
        }

        vTaskDelay(pdMS_TO_TICKS(25));
    }

    if (Request_Invoke_ID != 0) {
        tsm_free_invoke_id(Request_Invoke_ID);
    }
    return false;
}

/* Raw transaction primitives - caller must already hold BacnetMutex. Every
   typed helper below funnels through these two so there's exactly one place
   that touches Reply_Received/Reply_Errored/Request_Invoke_ID/Last_Read_Value
   and the bip socket. */
static bool bacnet_read_locked_idx(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, uint32_t array_index,
    BACNET_APPLICATION_DATA_VALUE *out_value)
{
    Reply_Received = false;
    Reply_Errored = false;
    Request_Invoke_ID = Send_Read_Property_Request(
        TargetDeviceInstance, object_type, object_instance, property,
        array_index);
    if (!wait_for_transaction()) {
        return false;
    }
    *out_value = Last_Read_Value;
    return true;
}

static bool bacnet_read_locked(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, BACNET_APPLICATION_DATA_VALUE *out_value)
{
    return bacnet_read_locked_idx(
        object_type, object_instance, property, BACNET_ARRAY_ALL, out_value);
}

static bool bacnet_write_locked_idx(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, BACNET_APPLICATION_DATA_VALUE *value,
    uint8_t priority, uint32_t array_index)
{
    Reply_Received = false;
    Reply_Errored = false;
    Request_Invoke_ID = Send_Write_Property_Request(
        TargetDeviceInstance, object_type, object_instance, property,
        value, priority, array_index);
    return wait_for_transaction();
}

static bool bacnet_write_locked(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, BACNET_APPLICATION_DATA_VALUE *value)
{
    return bacnet_write_locked_idx(
        object_type, object_instance, property, value, BACNET_NO_PRIORITY,
        BACNET_ARRAY_ALL);
}

/* xSemaphoreTake with a bounded wait so a slow/unreachable target can't hang
   the caller's task indefinitely (matters for HTTP handlers, which run on
   the httpd task). Returns false (without logging its own message - callers
   log with context) on mutex timeout, transaction failure, or a
   property/tag mismatch. */
static bool read_real_property(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, float *out_value)
{
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        ESP_LOGW(TAG_BAC, "read_real_property: BACnet busy, timed out waiting for mutex");
        return false;
    }
    BACNET_APPLICATION_DATA_VALUE v;
    bool ok = bacnet_read_locked(object_type, object_instance, property, &v);
    xSemaphoreGive(BacnetMutex);
    if (ok && v.tag != BACNET_APPLICATION_TAG_REAL) {
        ESP_LOGE(TAG_BAC, "Expected REAL value, got tag=%d", v.tag);
        return false;
    }
    if (ok) {
        *out_value = v.type.Real;
    }
    return ok;
}

static bool write_real_property(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, float value)
{
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        ESP_LOGW(TAG_BAC, "write_real_property: BACnet busy, timed out waiting for mutex");
        return false;
    }
    BACNET_APPLICATION_DATA_VALUE v = {0};
    v.tag = BACNET_APPLICATION_TAG_REAL;
    v.type.Real = value;
    bool ok = bacnet_write_locked(object_type, object_instance, property, &v);
    xSemaphoreGive(BacnetMutex);
    return ok;
}

/* binary-value present-value: BACNET_APPLICATION_TAG_ENUMERATED, 0=inactive/off, 1=active/on. */
static bool read_bool_property(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, bool *out_value)
{
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        ESP_LOGW(TAG_BAC, "read_bool_property: BACnet busy, timed out waiting for mutex");
        return false;
    }
    BACNET_APPLICATION_DATA_VALUE v;
    bool ok = bacnet_read_locked(object_type, object_instance, property, &v);
    xSemaphoreGive(BacnetMutex);
    if (ok && v.tag != BACNET_APPLICATION_TAG_ENUMERATED) {
        ESP_LOGE(TAG_BAC, "Expected ENUMERATED value, got tag=%d", v.tag);
        return false;
    }
    if (ok) {
        *out_value = v.type.Enumerated != 0;
    }
    return ok;
}

static bool write_bool_property(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, bool value)
{
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        ESP_LOGW(TAG_BAC, "write_bool_property: BACnet busy, timed out waiting for mutex");
        return false;
    }
    BACNET_APPLICATION_DATA_VALUE v = {0};
    v.tag = BACNET_APPLICATION_TAG_ENUMERATED;
    v.type.Enumerated = value ? 1 : 0;
    bool ok = bacnet_write_locked(object_type, object_instance, property, &v);
    xSemaphoreGive(BacnetMutex);
    return ok;
}

/* multi-state-value present-value: BACNET_APPLICATION_TAG_UNSIGNED_INT, 1-based state index. */
static bool read_msv_property(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, unsigned *out_value)
{
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        ESP_LOGW(TAG_BAC, "read_msv_property: BACnet busy, timed out waiting for mutex");
        return false;
    }
    BACNET_APPLICATION_DATA_VALUE v;
    bool ok = bacnet_read_locked(object_type, object_instance, property, &v);
    xSemaphoreGive(BacnetMutex);
    if (ok && v.tag != BACNET_APPLICATION_TAG_UNSIGNED_INT) {
        ESP_LOGE(TAG_BAC, "Expected UNSIGNED_INT value, got tag=%d", v.tag);
        return false;
    }
    if (ok) {
        *out_value = (unsigned)v.type.Unsigned_Int;
    }
    return ok;
}

static bool write_msv_property(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, unsigned value)
{
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        ESP_LOGW(TAG_BAC, "write_msv_property: BACnet busy, timed out waiting for mutex");
        return false;
    }
    BACNET_APPLICATION_DATA_VALUE v = {0};
    v.tag = BACNET_APPLICATION_TAG_UNSIGNED_INT;
    v.type.Unsigned_Int = value;
    bool ok = bacnet_write_locked(object_type, object_instance, property, &v);
    xSemaphoreGive(BacnetMutex);
    return ok;
}

/* ---- Generic BACnet explorer (read/write any object/property) ----
   Backs /api/bacnet/read and /api/bacnet/write. Unlike the typed helpers
   above (one object/property pair each, wired into MQTT/HA control), this
   takes object type/instance/property as request parameters so ad hoc
   discovery/diagnosis can be done straight against this device over HTTP,
   replacing the old workflow of SSHing into a separate box to run a desktop
   BACnet CLI. Accepts both bactext names ("binary-value",
   "present-value", ...) and raw numeric IDs for type/property, matching
   what the bacnet-stack CLI tools already accept. */

static void json_escape(char *dst, const char *src, size_t dst_len)
{
    if (!dst || dst_len == 0) return;
    if (!src) {
        dst[0] = '\0';
        return;
    }
    size_t di = 0;
    while (*src && di + 2 < dst_len) {
        if (*src == '"' || *src == '\\') {
            dst[di++] = '\\';
        }
        if ((unsigned char)*src < 0x20) {
            src++;
            continue;
        }
        dst[di++] = *src++;
    }
    dst[di] = '\0';
}

typedef struct {
    float delta_t;
    bool delta_t_valid;
    char performance[16];       /* "Cooling", "Heating", "Neutral", "Unavailable" */
    char performance_level[16]; /* "High", "Medium", "Low", "None", "Unavailable" */
    char system_health[16];     /* "Good", "Fair", "Poor", "Unavailable" */
    char diag_status[64];       /* Concise status */
    char diag_detail[256];      /* Detailed note */
} health_diagnostics_t;

static void compute_health_diagnostics(
    bool is_heating,
    bool cur_out_valid, float cur_out,
    bool cur_req_valid, float cur_req,
    bool flow_pct_valid, float flow_pct,
    bool valve_sig_valid, float valve_sig,
    bool ret_valid, float ret_air,
    bool sa_valid, float sa_air,
    bool any_alarm_active, const char *alarm_label,
    health_diagnostics_t *diag)
{
    if (!diag) return;
    memset(diag, 0, sizeof(*diag));

    /* 1. Delta T: Supply Air - Return Air */
    /* Net cooling is negative (sa < ret), net heating is positive (sa > ret) */
    if (ret_valid && temp_is_plausible(ret_air) && sa_valid && temp_is_plausible(sa_air)) {
        diag->delta_t_valid = true;
        diag->delta_t = sa_air - ret_air;
    } else {
        diag->delta_t_valid = false;
        diag->delta_t = 0.0f;
    }

    /* 2. Performance: "Cooling", "Heating", "Neutral" */
    if (diag->delta_t_valid) {
        if (diag->delta_t <= -0.3f) {
            snprintf(diag->performance, sizeof(diag->performance), "Cooling");
        } else if (diag->delta_t >= 0.3f) {
            snprintf(diag->performance, sizeof(diag->performance), "Heating");
        } else {
            snprintf(diag->performance, sizeof(diag->performance), "Neutral");
        }
    } else {
        snprintf(diag->performance, sizeof(diag->performance), "Unavailable");
    }

    /* 3. Performance Level: "Low", "Medium", "High", "None" */
    if (diag->delta_t_valid) {
        if (strcmp(diag->performance, "Cooling") == 0) {
            float abs_dt = -diag->delta_t;
            if (abs_dt >= 6.0f) {
                snprintf(diag->performance_level, sizeof(diag->performance_level), "High");
            } else if (abs_dt >= 3.0f) {
                snprintf(diag->performance_level, sizeof(diag->performance_level), "Medium");
            } else {
                snprintf(diag->performance_level, sizeof(diag->performance_level), "Low");
            }
        } else if (strcmp(diag->performance, "Heating") == 0) {
            float dt = diag->delta_t;
            if (dt >= 8.0f) {
                snprintf(diag->performance_level, sizeof(diag->performance_level), "High");
            } else if (dt >= 4.0f) {
                snprintf(diag->performance_level, sizeof(diag->performance_level), "Medium");
            } else {
                snprintf(diag->performance_level, sizeof(diag->performance_level), "Low");
            }
        } else {
            snprintf(diag->performance_level, sizeof(diag->performance_level), "None");
        }
    } else {
        snprintf(diag->performance_level, sizeof(diag->performance_level), "Unavailable");
    }

    /* 4. Health & Diagnostics Evaluation */
    float deliv_pct = 0.0f;
    bool has_demand = cur_req_valid && (cur_req >= 0.10f);
    if (has_demand && cur_out_valid) {
        deliv_pct = (cur_out / cur_req) * 100.0f;
    }

    /* Inadvertent heating or cooling reversal */
    bool cooling_reversal = (!is_heating && has_demand && diag->delta_t_valid && diag->delta_t >= 0.3f);
    bool heating_reversal = (is_heating && has_demand && diag->delta_t_valid && diag->delta_t <= -0.3f);

    bool under_delivering = (has_demand && cur_out_valid && deliv_pct < 40.0f);
    bool moderate_delivering = (has_demand && cur_out_valid && deliv_pct < 75.0f);
    bool flow_starved = (flow_pct_valid && flow_pct < 40.0f && valve_sig_valid && valve_sig > 30.0f);
    bool flow_low_valve_low = (flow_pct_valid && flow_pct < 50.0f && valve_sig_valid && valve_sig <= 30.0f);
    bool flow_ok_temp_bad = (has_demand && flow_pct_valid && flow_pct >= 60.0f && diag->delta_t_valid && fabsf(diag->delta_t) < 2.5f);

    /* Determine System Health */
    if (any_alarm_active || cooling_reversal || heating_reversal || (has_demand && under_delivering && flow_starved)) {
        snprintf(diag->system_health, sizeof(diag->system_health), "Poor");
    } else if (has_demand && (under_delivering || moderate_delivering || flow_starved || flow_ok_temp_bad)) {
        snprintf(diag->system_health, sizeof(diag->system_health), "Fair");
    } else {
        snprintf(diag->system_health, sizeof(diag->system_health), "Good");
    }

    /* Determine Diagnostic Status & Detail */
    if (any_alarm_active) {
        snprintf(diag->diag_status, sizeof(diag->diag_status), "Active Alarm: %s", alarm_label ? alarm_label : "Fault");
        snprintf(diag->diag_detail, sizeof(diag->diag_detail), "A BACnet alarm object is active. Inspect alarm points.");
    } else if (cooling_reversal) {
        snprintf(diag->diag_status, sizeof(diag->diag_status), "Inadvertent Heating (Motor Heat)");
        snprintf(diag->diag_detail, sizeof(diag->diag_detail),
                 "Cooling requested but supply air is +%.1f°C warmer than return. Motor heat is dominating due to insufficient chilled water cooling.",
                 diag->delta_t);
    } else if (heating_reversal) {
        snprintf(diag->diag_status, sizeof(diag->diag_status), "Inadvertent Cooling");
        snprintf(diag->diag_detail, sizeof(diag->diag_detail),
                 "Heating requested but supply air is %.1f°C colder than return.",
                 diag->delta_t);
    } else if (flow_starved) {
        snprintf(diag->diag_status, sizeof(diag->diag_status), "Water Flow Starved");
        snprintf(diag->diag_detail, sizeof(diag->diag_detail),
                 "Valve is open (%.0f%%) but coil flow is only %.0f%% of design. Check isolation valve, strainer, or loop pressure.",
                 valve_sig_valid ? valve_sig : 0.0f, flow_pct_valid ? flow_pct : 0.0f);
    } else if (flow_ok_temp_bad) {
        snprintf(diag->diag_status, sizeof(diag->diag_status), "Water Temperature Inadequate");
        snprintf(diag->diag_detail, sizeof(diag->diag_detail),
                 "Flow is %.0f%% of design but air ΔT is only %.1f°C. Water supply temperature is likely out of range.",
                 flow_pct_valid ? flow_pct : 0.0f, diag->delta_t);
    } else if (!has_demand) {
        if (flow_low_valve_low) {
            snprintf(diag->diag_status, sizeof(diag->diag_status), "Idle (Flow Throttled)");
            snprintf(diag->diag_detail, sizeof(diag->diag_detail), "No active thermal demand. Valve and flow are appropriately throttled.");
        } else {
            snprintf(diag->diag_status, sizeof(diag->diag_status), "Idle");
            snprintf(diag->diag_detail, sizeof(diag->diag_detail), "No active thermal demand. System resting normally.");
        }
    } else if (under_delivering || moderate_delivering) {
        snprintf(diag->diag_status, sizeof(diag->diag_status), "Under-Delivering Capacity");
        snprintf(diag->diag_detail, sizeof(diag->diag_detail),
                 "Delivering %.0f%% of requested output (%.2f kW / %.2f kW demanded).",
                 deliv_pct, cur_out_valid ? cur_out : 0.0f, cur_req_valid ? cur_req : 0.0f);
    } else {
        snprintf(diag->diag_status, sizeof(diag->diag_status), "Normal Operation");
        snprintf(diag->diag_detail, sizeof(diag->diag_detail),
                 "Delivering %.0f%% of demand with healthy flow (%.0f%%) and temperature delta (%.1f°C).",
                 deliv_pct, flow_pct_valid ? flow_pct : 0.0f, diag->delta_t);
    }
}

static inline void copy_character_string(char *dst, size_t dst_size, const BACNET_CHARACTER_STRING *src)
{
    if (!dst || dst_size == 0) return;
    dst[0] = '\0';
    if (!src) return;
    size_t len = characterstring_length(src);
    if (len >= dst_size) len = dst_size - 1;
    const char *s = characterstring_value_const(src);
    if (s && len > 0) {
        memcpy(dst, s, len);
    }
    dst[len] = '\0';
}

static size_t append_value_json(char *buf, size_t buf_len, const BACNET_APPLICATION_DATA_VALUE *v)
{
    if (!buf || buf_len == 0 || !v) return 0;
    switch (v->tag) {
        case BACNET_APPLICATION_TAG_NULL:
            return (size_t)snprintf(buf, buf_len, "null");
        case BACNET_APPLICATION_TAG_BOOLEAN:
            return (size_t)snprintf(buf, buf_len, v->type.Boolean ? "true" : "false");
        case BACNET_APPLICATION_TAG_UNSIGNED_INT:
            return (size_t)snprintf(buf, buf_len, "%lu", (unsigned long)v->type.Unsigned_Int);
        case BACNET_APPLICATION_TAG_SIGNED_INT:
            return (size_t)snprintf(buf, buf_len, "%ld", (long)v->type.Signed_Int);
        case BACNET_APPLICATION_TAG_REAL:
            return (size_t)snprintf(buf, buf_len, "%.3f", (double)v->type.Real);
        case BACNET_APPLICATION_TAG_DOUBLE:
            return (size_t)snprintf(buf, buf_len, "%.3f", v->type.Double);
        case BACNET_APPLICATION_TAG_ENUMERATED:
            return (size_t)snprintf(buf, buf_len, "%lu", (unsigned long)v->type.Enumerated);
        case BACNET_APPLICATION_TAG_OBJECT_ID:
            return (size_t)snprintf(
                buf, buf_len, "\"%s:%u\"",
                bactext_object_type_name_default(v->type.Object_Id.type, "?"),
                (unsigned)v->type.Object_Id.instance);
        case BACNET_APPLICATION_TAG_CHARACTER_STRING: {
            char tmp[64] = {0};
            copy_character_string(tmp, sizeof(tmp), (const BACNET_CHARACTER_STRING *)&v->type.Character_String);
            char escaped[80] = {0};
            json_escape(escaped, tmp, sizeof(escaped));
            return (size_t)snprintf(buf, buf_len, "\"%s\"", escaped);
        }
        default:
            return (size_t)snprintf(buf, buf_len, "null");
    }
}

/* Fills out/out_len with a JSON response either way (success or a
   {"ok":false,"error":...} body) - callers just send it back verbatim. */
static void explorer_read(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, uint32_t array_index, char *out, size_t out_len)
{
    if (!out || out_len == 0) return;
    if (!BacnetReady) {
        snprintf(out, out_len, "{\"ok\":false,\"error\":\"bacnet not ready\"}");
        return;
    }
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        snprintf(out, out_len, "{\"ok\":false,\"error\":\"bacnet busy, timed out waiting for mutex\"}");
        return;
    }
    BACNET_APPLICATION_DATA_VALUE v;
    bool ok = bacnet_read_locked_idx(object_type, object_instance, property, array_index, &v);
    static uint8_t raw_copy[MAX_APDU];
    int raw_len = 0;
    if (ok) {
        raw_len = Last_Read_Raw_Len;
        if (raw_len > 0 && raw_len <= (int)sizeof(raw_copy)) {
            memcpy(raw_copy, Last_Read_Raw, raw_len);
        }
    }
    xSemaphoreGive(BacnetMutex);

    if (!ok) {
        snprintf(out, out_len, "{\"ok\":false,\"error\":\"read failed (timeout or BACnet error - see device log)\"}");
        return;
    }

    if (property == PROP_PRIORITY_ARRAY && array_index == BACNET_ARRAY_ALL) {
        size_t pos = (size_t)snprintf(out, out_len, "{\"ok\":true,\"tag\":\"priority-array\",\"values\":[");
        int offset = 0;
        int slot = 0;
        while (offset < raw_len && slot < 16 && pos < out_len - 32) {
            BACNET_APPLICATION_DATA_VALUE elem = {0};
            int consumed = bacapp_decode_application_data(
                raw_copy + offset, (uint32_t)(raw_len - offset), &elem);
            if (consumed <= 0) {
                break;
            }
            offset += consumed;
            if (slot > 0 && pos < out_len - 32) {
                pos += (size_t)snprintf(out + pos, out_len - pos, ",");
            }
            if (pos < out_len - 32) {
                pos += append_value_json(out + pos, out_len - pos, &elem);
            }
            slot++;
        }
        if (pos < out_len - 4) {
            snprintf(out + pos, out_len - pos, "]}");
        }
        return;
    }

    size_t pos = (size_t)snprintf(
        out, out_len, "{\"ok\":true,\"tag\":\"%s\",\"value\":",
        bactext_application_tag_name_default(v.tag, "unknown"));
    if (pos < out_len - 32) {
        pos += append_value_json(out + pos, out_len - pos, &v);
    }
    if (pos < out_len - 2) {
        snprintf(out + pos, out_len - pos, "}");
    }
}

/* value_tag selects how value_str is parsed/encoded: "real", "enum" (or
   "bool" - same tag, BACnet's ENUMERATED), "uint", or "null" (writes an
   actual BACNET_APPLICATION_TAG_NULL, e.g. to relinquish a priority-array
   slot by array index rather than change Present_Value). */
static void explorer_write(
    BACNET_OBJECT_TYPE object_type, uint32_t object_instance,
    BACNET_PROPERTY_ID property, uint32_t array_index, uint8_t priority,
    const char *value_tag, const char *value_str, char *out, size_t out_len)
{
    if (!BacnetReady) {
        snprintf(out, out_len, "{\"ok\":false,\"error\":\"bacnet not ready\"}");
        return;
    }
    BACNET_APPLICATION_DATA_VALUE v = {0};
    if (strcmp(value_tag, "real") == 0) {
        v.tag = BACNET_APPLICATION_TAG_REAL;
        v.type.Real = strtof(value_str, NULL);
    } else if (strcmp(value_tag, "enum") == 0 || strcmp(value_tag, "bool") == 0) {
        v.tag = BACNET_APPLICATION_TAG_ENUMERATED;
        v.type.Enumerated = (uint32_t)strtoul(value_str, NULL, 10);
    } else if (strcmp(value_tag, "uint") == 0) {
        v.tag = BACNET_APPLICATION_TAG_UNSIGNED_INT;
        v.type.Unsigned_Int = (uint32_t)strtoul(value_str, NULL, 10);
    } else if (strcmp(value_tag, "null") == 0) {
        v.tag = BACNET_APPLICATION_TAG_NULL;
    } else {
        snprintf(out, out_len, "{\"ok\":false,\"error\":\"valuetag must be real|enum|bool|uint|null\"}");
        return;
    }

    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        snprintf(out, out_len, "{\"ok\":false,\"error\":\"bacnet busy, timed out waiting for mutex\"}");
        return;
    }
    bool ok = bacnet_write_locked_idx(
        object_type, object_instance, property, &v, priority, array_index);
    xSemaphoreGive(BacnetMutex);

    if (!ok) {
        snprintf(out, out_len, "{\"ok\":false,\"error\":\"write failed (timeout or BACnet error - see device log)\"}");
        return;
    }
    snprintf(out, out_len, "{\"ok\":true}");
}

static bool parse_object_type(const char *s, BACNET_OBJECT_TYPE *out)
{
    uint32_t idx;
    if (bactext_object_type_strtol(s, &idx)) {
        *out = (BACNET_OBJECT_TYPE)idx;
        return true;
    }
    char *end;
    unsigned long v = strtoul(s, &end, 10);
    if (end == s || *end != '\0') {
        return false;
    }
    *out = (BACNET_OBJECT_TYPE)v;
    return true;
}

static bool parse_property_id(const char *s, BACNET_PROPERTY_ID *out)
{
    uint32_t idx;
    if (bactext_property_strtol(s, &idx)) {
        *out = (BACNET_PROPERTY_ID)idx;
        return true;
    }
    char *end;
    unsigned long v = strtoul(s, &end, 10);
    if (end == s || *end != '\0') {
        return false;
    }
    *out = (BACNET_PROPERTY_ID)v;
    return true;
}

/* Reads the target's Device object-name into TargetDeviceName. This is a
   labelling step only - it is what the dashboard shows as "Target device" -
   and deliberately has no bearing on BacnetReady. A controller that refuses
   or mangles this one property must not be able to black out every other
   point on the device. */
static bool confirm_target_device_name(void)
{
    char name[MAX_CHARACTER_STRING_BYTES + 1] = {0};

    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        ESP_LOGW(TAG_BAC, "Device object-name read: BacnetMutex busy");
        return false;
    }
    Reply_Received = false;
    Reply_Errored = false;
    Request_Invoke_ID = Send_Read_Property_Request(
        TargetDeviceInstance, OBJECT_DEVICE, TargetDeviceInstance,
        PROP_OBJECT_NAME, BACNET_ARRAY_ALL);
    bool name_ok = wait_for_transaction() &&
        Last_Read_Value.tag == BACNET_APPLICATION_TAG_CHARACTER_STRING;
    if (name_ok) {
        copy_character_string(
            name, sizeof(name),
            (const BACNET_CHARACTER_STRING *)&Last_Read_Value.type.Character_String);
    }
    xSemaphoreGive(BacnetMutex);

    if (!name_ok) {
        return false;
    }
    strlcpy(TargetDeviceName, name, sizeof(TargetDeviceName));
    TargetDeviceNameValid = true;
    ESP_LOGI(TAG_BAC, "Target device confirmed: object-name = \"%s\"", name);
    return true;
}

/* Brings the BACnet/IP datalink up and then supervises it for the lifetime of
   the device. Runs inline on eth_bringup_task rather than in a task of its own
   - see the stack note at that task's xTaskCreate for why.

   Never returns while the datalink is alive. */
static void bacnet_client_run(void)
{
    unsigned max_apdu = 0;

    ESP_LOGI(TAG_BAC, "Starting BACnet/IP datalink on port %u", TargetPort);
    bip_socket_esp_idf_set_netif(EthNetif);
    if (!bip_init(TargetPort)) {
        ESP_LOGE(TAG_BAC, "bip_init() failed - BACnet unavailable this boot");
        return;
    }

    address_init();
    init_bacnet_handlers();
    bind_target_device();

    if (!address_bind_request(TargetDeviceInstance, &max_apdu, &Target_Address)) {
        ESP_LOGE(TAG_BAC, "address_bind_request failed even after pre-seeding - bug");
        return;
    }

    /* The datalink is up and the target is bound, so it is safe for other tasks
       (the HTTP handlers, mqtt_state_task) to issue transactions, serialized
       through BacnetMutex. That - and only that - is what BacnetReady means.

       It must not additionally wait on the object-name handshake below: doing
       so made one optional read the single point of failure for the whole
       bridge, and when that read never happened the dashboard reported
       "Not confirmed yet" with every value blank. */
    BacnetReady = true;
    DiscoveryNeedsBacnetRefresh = true;
    ESP_LOGI(TAG_BAC, "BACnet datalink ready (BacnetReady = true)");

    /* Confirm the device name in the background. Ethernet link-up does not mean
       traffic can flow yet (switch STP learning can block UDP for 30+ seconds),
       so this retries indefinitely - but data flows the whole time it does. */
    int attempt = 0;
    for (;;) {
        if (!TargetDeviceNameValid) {
            attempt++;
            if (confirm_target_device_name()) {
                /* Republish discovery now that the device has a real name. */
                DiscoveryNeedsBacnetRefresh = true;
            } else {
                ESP_LOGW(
                    TAG_BAC, "Device object-name read attempt %d failed, retrying",
                    attempt);
            }
        }
        vTaskDelay(pdMS_TO_TICKS(TargetDeviceNameValid ? 60000 : 5000));
    }
}

static volatile int PingSuccessCount = 0;
static volatile int PingTimeoutCount = 0;
static volatile bool PingDone = false;

static void ping_success_cb(esp_ping_handle_t hdl, void *args)
{
    uint32_t elapsed_time;
    esp_ping_get_profile(hdl, ESP_PING_PROF_TIMEGAP, &elapsed_time, sizeof(elapsed_time));
    ESP_LOGI(TAG_BAC, "PING reply from %s: time=%ums", TargetIp, (unsigned)elapsed_time);
    PingSuccessCount++;
}

static void ping_timeout_cb(esp_ping_handle_t hdl, void *args)
{
    ESP_LOGW(TAG_BAC, "PING timeout");
    PingTimeoutCount++;
}

static void ping_end_cb(esp_ping_handle_t hdl, void *args)
{
    PingDone = true;
    esp_ping_delete_session(hdl);
}

static void ping_test(void)
{
    ip_addr_t target;
    ip4addr_aton(TargetIp, (ip4_addr_t *)&target.u_addr.ip4);
    target.type = IPADDR_TYPE_V4;

    esp_ping_config_t config = ESP_PING_DEFAULT_CONFIG();
    config.target_addr = target;
    config.count = 4;
    config.interval_ms = 500;
    config.timeout_ms = 2000;

    esp_ping_callbacks_t cbs = {
        .on_ping_success = ping_success_cb,
        .on_ping_timeout = ping_timeout_cb,
        .on_ping_end = ping_end_cb,
        .cb_args = NULL,
    };

    esp_ping_handle_t ping;
    ESP_ERROR_CHECK(esp_ping_new_session(&config, &cbs, &ping));
    ESP_ERROR_CHECK(esp_ping_start(ping));

    ESP_LOGI(TAG_BAC, "Pinging %s to check IP-layer connectivity...", TargetIp);
    while (!PingDone) {
        vTaskDelay(pdMS_TO_TICKS(200));
    }
    ESP_LOGI(TAG_BAC, "Ping result: %d success, %d timeout", PingSuccessCount, PingTimeoutCount);
}

/* Ethernet bring-up + BACnet test, in its own task so it doesn't block
   app_main() from getting WiFi up concurrently (see file header). */
static void eth_bringup_task(void *arg)
{
    uint8_t eth_port_cnt = 0;
    esp_eth_handle_t *eth_handles;
    ESP_ERROR_CHECK(example_eth_init(&eth_handles, &eth_port_cnt));

    esp_netif_config_t cfg = ESP_NETIF_DEFAULT_ETH();
    EthNetif = esp_netif_new(&cfg);
    esp_eth_netif_glue_handle_t glue = esp_eth_new_netif_glue(eth_handles[0]);
    ESP_ERROR_CHECK(esp_netif_attach(EthNetif, glue));

    ESP_ERROR_CHECK(esp_netif_dhcpc_stop(EthNetif));
    esp_netif_ip_info_t ip_info = {0};
    ip4addr_aton(LOCAL_STATIC_IP, (ip4_addr_t *)&ip_info.ip);
    ip4addr_aton(LOCAL_NETMASK, (ip4_addr_t *)&ip_info.netmask);
    ip4addr_aton(LOCAL_GATEWAY, (ip4_addr_t *)&ip_info.gw);
    ESP_ERROR_CHECK(esp_netif_set_ip_info(EthNetif, &ip_info));
    ESP_LOGI(TAG_BAC, "Static IP configured: %s", LOCAL_STATIC_IP);

    ESP_ERROR_CHECK(
        esp_event_handler_register(ETH_EVENT, ESP_EVENT_ANY_ID, &eth_event_handler, NULL));
    ESP_ERROR_CHECK(esp_eth_start(eth_handles[0]));

    ESP_LOGI(TAG_BAC, "Waiting for Ethernet link...");
    while (!EthConnected) {
        vTaskDelay(pdMS_TO_TICKS(200));
    }

    ping_test();

    /* Runs the BACnet client inline instead of spawning a second big-stack task
       here. This task is already carrying the 24KB stack those code paths need
       (nested MAX_APDU=1476 buffers across bip_receive/rp_ack_decode/
       bacapp_decode overflow anything smaller - see bacnet_client's README),
       and it was allocated in app_main while the heap was still empty.

       Spawning a fresh 24KB task at this point could not be relied on: by the
       time Ethernet has come up and ping_test has run (~5s in), httpd, the MQTT
       client, and mqtt_state have each taken a 24KB stack and there is no
       longer a contiguous 24KB internal block to hand out. The xTaskCreate
       then failed silently and BACnet never started at all. */
    bacnet_client_run();
    vTaskDelete(NULL);
}

/* ===================== WiFi provisioning side ===================== */

#define EXAMPLE_ESP_WIFI_SSID CONFIG_ESP_WIFI_SSID
#define EXAMPLE_ESP_WIFI_PASS CONFIG_ESP_WIFI_PASSWORD
#define EXAMPLE_MAX_STA_CONN CONFIG_ESP_MAX_STA_CONN

#define NVS_NAMESPACE "wifi_cfg"
#define NVS_KEY_SSID "ssid"
#define NVS_KEY_PASS "pass"
#define STA_CONNECT_TIMEOUT_MS 25000
#define STA_MAX_RETRIES 5

extern const char root_start[] asm("_binary_root_html_start");
extern const char root_end[] asm("_binary_root_html_end");
extern const char manage_start[] asm("_binary_manage_html_start");
extern const char manage_end[] asm("_binary_manage_html_end");
extern const char status_page_start[] asm("_binary_status_html_start");
extern const char status_page_end[] asm("_binary_status_html_end");
extern const char health_page_start[] asm("_binary_health_html_start");
extern const char health_page_end[] asm("_binary_health_html_end");
extern const char reset_page_start[] asm("_binary_reset_html_start");
extern const char reset_page_end[] asm("_binary_reset_html_end");
extern const char update_page_start[] asm("_binary_update_html_start");
extern const char update_page_end[] asm("_binary_update_html_end");
extern const char objects_page_start[] asm("_binary_objects_html_start");
extern const char objects_page_end[] asm("_binary_objects_html_end");
extern const char wizard_page_start[] asm("_binary_wizard_html_start");
extern const char wizard_page_end[] asm("_binary_wizard_html_end");
extern const char mqtt_page_start[] asm("_binary_mqtt_html_start");
extern const char mqtt_page_end[] asm("_binary_mqtt_html_end");

static const char *TAG_WIFI = "wifi_prov";
static EventGroupHandle_t WifiEventGroup;
#define STA_CONNECTED_BIT BIT0
#define STA_FAILED_BIT BIT1
static int StaRetryCount = 0;

/* Set once STA connects; read by the post-connect dashboard's /api/status. */
static char StaSsid[33] = {0};
static char StaIp[16] = {0};

/* Whether /api/wizard/finish has ever been called. Until it has, "/" redirects
 * to "/wizard" instead of the normal dashboard, so a freshly-provisioned
 * device lands on setup rather than looking like nothing happened. */
#define NVS_KEY_WIZARD_DONE "wizard_done"
static bool WizardCompleted = false;

static void wizard_completed_load(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_NAMESPACE, NVS_READONLY, &handle) != ESP_OK) {
        return;
    }
    uint8_t done = 0;
    if (nvs_get_u8(handle, NVS_KEY_WIZARD_DONE, &done) == ESP_OK) {
        WizardCompleted = done != 0;
    }
    nvs_close(handle);
}

static void wizard_completed_save(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_NAMESPACE, NVS_READWRITE, &handle) != ESP_OK) {
        return;
    }
    nvs_set_u8(handle, NVS_KEY_WIZARD_DONE, 1);
    nvs_commit(handle);
    nvs_close(handle);
    WizardCompleted = true;
}

static bool load_wifi_credentials(char *ssid, size_t ssid_len, char *pass, size_t pass_len)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_NAMESPACE, NVS_READONLY, &handle) != ESP_OK) {
        return false;
    }
    esp_err_t err_ssid = nvs_get_str(handle, NVS_KEY_SSID, ssid, &ssid_len);
    esp_err_t err_pass = nvs_get_str(handle, NVS_KEY_PASS, pass, &pass_len);
    nvs_close(handle);

    if (err_ssid != ESP_OK) {
        return false;
    }
    if (err_pass == ESP_ERR_NVS_NOT_FOUND) {
        pass[0] = '\0';
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

static void sta_event_handler(
    void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        wifi_event_sta_disconnected_t *event = (wifi_event_sta_disconnected_t *)event_data;
        ESP_LOGW(
            TAG_WIFI, "STA disconnected: reason=%d (retry count %d)", event->reason,
            StaRetryCount + 1);
        if (StaRetryCount < STA_MAX_RETRIES) {
            StaRetryCount++;
            vTaskDelay(pdMS_TO_TICKS(500));
            esp_wifi_connect();
        } else if (WifiEventGroup) {
            /* During initial boot provisioning check */
            xEventGroupSetBits(WifiEventGroup, STA_FAILED_BIT);
        } else {
            /* Already in normal operation: keep retrying indefinitely */
            vTaskDelay(pdMS_TO_TICKS(2000));
            esp_wifi_connect();
        }
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG_WIFI, "STA got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        snprintf(StaIp, sizeof(StaIp), IPSTR, IP2STR(&event->ip_info.ip));
        StaRetryCount = 0;
        if (WifiEventGroup) {
            xEventGroupSetBits(WifiEventGroup, STA_CONNECTED_BIT);
        }
    }
}

static bool try_connect_sta(const char *ssid, const char *pass)
{
    StaRetryCount = 0;
    strlcpy(StaSsid, ssid, sizeof(StaSsid));
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

    ESP_LOGI(TAG_WIFI, "Connecting to saved SSID '%s'...", ssid);
    EventBits_t bits = xEventGroupWaitBits(
        WifiEventGroup, STA_CONNECTED_BIT | STA_FAILED_BIT, pdFALSE, pdFALSE,
        pdMS_TO_TICKS(STA_CONNECT_TIMEOUT_MS));

    if (bits & STA_CONNECTED_BIT) {
        vEventGroupDelete(WifiEventGroup);
        WifiEventGroup = NULL;
        return true;
    }

    ESP_LOGW(TAG_WIFI, "STA connect failed/timed out - falling back to provisioning AP");
    ESP_ERROR_CHECK(esp_wifi_stop());
    ESP_ERROR_CHECK(esp_wifi_deinit());
    esp_event_handler_unregister(WIFI_EVENT, ESP_EVENT_ANY_ID, &sta_event_handler);
    esp_event_handler_unregister(IP_EVENT, IP_EVENT_STA_GOT_IP, &sta_event_handler);
    vEventGroupDelete(WifiEventGroup);
    WifiEventGroup = NULL;
    return false;
}

static void ap_event_handler(
    void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    if (event_id == WIFI_EVENT_AP_STACONNECTED) {
        wifi_event_ap_staconnected_t *event = (wifi_event_ap_staconnected_t *)event_data;
        ESP_LOGI(TAG_WIFI, "station " MACSTR " join, AID=%d", MAC2STR(event->mac), event->aid);
    } else if (event_id == WIFI_EVENT_AP_STADISCONNECTED) {
        wifi_event_ap_stadisconnected_t *event = (wifi_event_ap_stadisconnected_t *)event_data;
        ESP_LOGI(
            TAG_WIFI, "station " MACSTR " leave, AID=%d, reason=%d", MAC2STR(event->mac),
            event->aid, event->reason);
    }
}

static void wifi_init_softap(void)
{
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &ap_event_handler, NULL));
    esp_netif_create_default_wifi_sta(); /* APSTA - lets the setup page scan for real networks */

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
    ESP_LOGI(TAG_WIFI, "Set up softAP with IP: %s", ip_addr);
    ESP_LOGI(
        TAG_WIFI, "wifi_init_softap finished. SSID:'%s' password:'%s'", EXAMPLE_ESP_WIFI_SSID,
        EXAMPLE_ESP_WIFI_PASS);
}

static esp_err_t root_get_handler(httpd_req_t *req)
{
    const uint32_t root_len = root_end - root_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, root_start, root_len);
    return ESP_OK;
}

static const httpd_uri_t root_uri = {.uri = "/", .method = HTTP_GET, .handler = root_get_handler};

static esp_err_t scan_get_handler(httpd_req_t *req)
{
    uint16_t max_aps = 30;
    wifi_ap_record_t ap_records[30];

    wifi_scan_config_t scan_config = {0};
    esp_err_t err = esp_wifi_scan_start(&scan_config, true);
    if (err != ESP_OK) {
        ESP_LOGE(TAG_WIFI, "Scan failed: %s", esp_err_to_name(err));
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

    char seen_ssid[30][33];
    int seen_count = 0;
    bool first = true;
    for (int i = 0; i < max_aps && off < 4000; i++) {
        if (ap_records[i].ssid[0] == '\0') {
            continue;
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

static const httpd_uri_t scan_uri = {.uri = "/scan", .method = HTTP_GET, .handler = scan_get_handler};

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
    if (raw_len >= out_len * 3) {
        raw_len = out_len * 3 - 1;
    }
    /* Percent-encoded input can be up to 3x the decoded output length. This
     * buffer must cover the largest caller's out_len (api_objects_batch_handler's
     * points_str[1024]) or the copy below silently truncates before url_decode
     * ever runs - confirmed live: the default 17-point pin set encodes past the
     * old 196-byte cap and only the first 10 points were ever read. */
    char raw[3200];
    size_t copy_len = raw_len < sizeof(raw) - 1 ? raw_len : sizeof(raw) - 1;
    memcpy(raw, start, copy_len);
    raw[copy_len] = '\0';
    url_decode(out, raw, out_len);
    return true;
}

static const char *confirm_page =
    "<!DOCTYPE html><html><body style='font-family:sans-serif;max-width:420px;margin:2em auto;'>"
    "<h1>Saved</h1>"
    "<p>Restarting and connecting to your network. This access point (ESP-BACnet-Setup) will "
    "disappear once it connects; if it can't connect within about 25 seconds, the access point "
    "will reappear so you can try again.</p>"
    "<p><strong>Once connected</strong>, on the same WiFi network open:</p>"
    "<p style='font-size:1.1em'><strong>http://esp-bacnet-bridge.local</strong></p>"
    "<p>in a browser. It will take you straight to the setup wizard to finish configuring "
    "BACnet and MQTT. If that address doesn't resolve (some routers/phones don't support "
    ".local names), check your router's connected-devices list for an IP address instead.</p>"
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
        ESP_LOGE(TAG_WIFI, "Failed to save credentials: %s", esp_err_to_name(err));
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    ESP_LOGI(TAG_WIFI, "Saved credentials for SSID '%s', restarting in 3s...", ssid);
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, confirm_page, HTTPD_RESP_USE_STRLEN);

    vTaskDelay(pdMS_TO_TICKS(3000));
    esp_restart();
    return ESP_OK;
}

static const httpd_uri_t connect_uri = {
    .uri = "/connect", .method = HTTP_POST, .handler = connect_post_handler};

esp_err_t http_404_error_handler(httpd_req_t *req, httpd_err_code_t err)
{
    httpd_resp_set_status(req, "302 Temporary Redirect");
    httpd_resp_set_hdr(req, "Location", "/");
    httpd_resp_send(req, "Redirect to the captive portal", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

/* ===================== Post-connect pages ===================== */

static esp_err_t manage_get_handler(httpd_req_t *req)
{
    if (!WizardCompleted) {
        httpd_resp_set_status(req, "302 Found");
        httpd_resp_set_hdr(req, "Location", "/wizard");
        httpd_resp_send(req, NULL, 0);
        return ESP_OK;
    }
    const uint32_t len = manage_end - manage_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, manage_start, len);
    return ESP_OK;
}

static const httpd_uri_t manage_uri = {
    .uri = "/", .method = HTTP_GET, .handler = manage_get_handler};

static esp_err_t status_page_get_handler(httpd_req_t *req)
{
    const uint32_t len = status_page_end - status_page_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, status_page_start, len);
    return ESP_OK;
}

static const httpd_uri_t status_page_uri = {
    .uri = "/status", .method = HTTP_GET, .handler = status_page_get_handler};

static esp_err_t health_page_get_handler(httpd_req_t *req)
{
    const uint32_t len = health_page_end - health_page_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, health_page_start, len);
    return ESP_OK;
}

static const httpd_uri_t health_page_uri = {
    .uri = "/health", .method = HTTP_GET, .handler = health_page_get_handler};

static esp_err_t reset_page_get_handler(httpd_req_t *req)
{
    const uint32_t len = reset_page_end - reset_page_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, reset_page_start, len);
    return ESP_OK;
}

static const httpd_uri_t reset_page_uri = {
    .uri = "/reset", .method = HTTP_GET, .handler = reset_page_get_handler};

/* Original ESP32 (unlike S2/S3/C3) has no ADC channel wired to its own
   supply rail, so there's no real voltage reading available without adding
   an external divider into a spare GPIO - a hardware change, not a
   firmware one. The brownout detector is the one power-health signal
   available for free: it resets the chip below ~2.43V, and the reason for
   the MOST RECENT boot (not a live reading) is queryable via
   esp_reset_reason(). A device that keeps coming back with "Brownout" or
   "Power glitch" here has a supply problem worth chasing down. */
static const char *reset_reason_name(esp_reset_reason_t reason)
{
    switch (reason) {
        case ESP_RST_POWERON:    return "Power-on";
        case ESP_RST_SW:         return "Software restart";
        case ESP_RST_PANIC:      return "Crash (panic)";
        case ESP_RST_INT_WDT:    return "Interrupt watchdog";
        case ESP_RST_TASK_WDT:   return "Task watchdog";
        case ESP_RST_WDT:        return "Other watchdog";
        case ESP_RST_DEEPSLEEP:  return "Woke from deep sleep";
        case ESP_RST_BROWNOUT:   return "Brownout (supply sagged)";
        case ESP_RST_SDIO:       return "SDIO";
        case ESP_RST_USB:        return "USB";
        case ESP_RST_JTAG:       return "JTAG";
        case ESP_RST_EFUSE:      return "eFuse error";
        case ESP_RST_PWR_GLITCH: return "Power glitch";
        case ESP_RST_CPU_LOCKUP: return "CPU lockup";
        default:                 return "Unknown";
    }
}

/* Forward-declared: defined alongside the rest of the OTA-password logic
   further down, but api_network_get_handler (used by the setup wizard's
   Step 1 to show whether a password still needs to be chosen) runs first
   in the file. */
static bool ota_password_is_set(void);

/* Fast/cheap - no BACnet transaction, just cached connection state, so this
   stays responsive even if the BACnet link/target is unreachable. */
static esp_err_t api_network_get_handler(httpd_req_t *req)
{
    char target_name[MAX_CHARACTER_STRING_BYTES + 1] = {0};
    bool target_name_valid = TargetDeviceNameValid;
    if (target_name_valid) {
        strlcpy(target_name, TargetDeviceName, sizeof(target_name));
    }
    char name_escaped[96] = {0};
    char name_json[100] = "null";
    if (target_name_valid) {
        json_escape(name_escaped, target_name, sizeof(name_escaped));
        snprintf(name_json, sizeof(name_json), "\"%s\"", name_escaped);
    }

    int64_t uptime_s = esp_timer_get_time() / 1000000LL;
    const char *reset_reason = reset_reason_name(esp_reset_reason());
    bool reset_is_power_issue =
        esp_reset_reason() == ESP_RST_BROWNOUT || esp_reset_reason() == ESP_RST_PWR_GLITCH;

    char rssi_json[16] = "null";
    wifi_ap_record_t ap_info;
    if (esp_wifi_sta_get_ap_info(&ap_info) == ESP_OK) {
        snprintf(rssi_json, sizeof(rssi_json), "%d", ap_info.rssi);
    }

    /* bacnet_ready and the heap figures are here because their absence is what
       made a silent task-spawn failure so hard to find: the dashboard could
       only say "Not confirmed yet" with every value blank, and nothing exposed
       whether the datalink was up or whether the heap could still satisfy a
       24KB stack. largest_free_block is the number that matters for that. */
    char buf[768];
    int off = snprintf(
        buf, sizeof(buf),
        "{\"wifi_ssid\":\"%s\",\"wifi_ip\":\"%s\",\"wifi_rssi\":%s,\"eth_connected\":%s,"
        "\"bacnet_target_name\":%s,\"bacnet_target_ip\":\"%s\",\"bacnet_ready\":%s,"
        "\"uptime_seconds\":%lld,\"last_reset_reason\":\"%s\",\"last_reset_is_power_issue\":%s,"
        "\"free_heap\":%u,\"min_free_heap\":%u,\"largest_free_block\":%u,"
        "\"ota_password_set\":%s}",
        StaSsid, StaIp, rssi_json, EthConnected ? "true" : "false", name_json, TargetIp,
        BacnetReady ? "true" : "false",
        (long long)uptime_s, reset_reason, reset_is_power_issue ? "true" : "false",
        (unsigned)heap_caps_get_free_size(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL),
        (unsigned)heap_caps_get_minimum_free_size(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL),
        (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL),
        ota_password_is_set() ? "true" : "false");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, off);
    return ESP_OK;
}

static const httpd_uri_t api_network_uri = {
    .uri = "/api/network", .method = HTTP_GET, .handler = api_network_get_handler};

static esp_err_t api_status_get_handler(httpd_req_t *req)
{
    bool sys_power = false;
    bool sys_power_valid = BacnetReady && read_bool_property(
        OBJECT_BINARY_VALUE, SYS_POWER_READBACK_INSTANCE, PROP_PRESENT_VALUE, &sys_power);

    unsigned boost_mode = 0;
    bool boost_valid = BacnetReady &&
        read_msv_property(OBJECT_MULTI_STATE_VALUE, BOOST_INSTANCE, PROP_PRESENT_VALUE, &boost_mode);

    char buf[1536];
    size_t off = 0;
    off += snprintf(
        buf + off, sizeof(buf) - off,
        "{\"sys_power_valid\":%s,\"sys_power\":%s,"
        "\"boost_valid\":%s,\"boost_mode\":%u,"
        "\"boost_timeout_minutes\":%u,\"boost_remaining_minutes\":%d,"
        "\"rooms\":[",
        sys_power_valid ? "true" : "false", sys_power ? "true" : "false",
        boost_valid ? "true" : "false", boost_mode,
        (unsigned)BoostTimeoutMinutes, boost_remaining_minutes());

    bool first_room = true;
    for (size_t i = 0; i < RoomCount && off < sizeof(buf); i++) {
        const room_config_t *room = &Rooms[i];
        if (!room->active) {
            continue;
        }
        float setpoint = 0.0f;
        float temperature = 0.0f;
        bool power = false;
        bool setpoint_valid = BacnetReady && read_real_property(
            OBJECT_ANALOG_VALUE, room->setpoint_instance, PROP_PRESENT_VALUE, &setpoint);
        bool temperature_valid = BacnetReady && read_real_property(
            OBJECT_ANALOG_VALUE, room->temperature_instance, PROP_PRESENT_VALUE, &temperature);
        bool power_valid = BacnetReady && read_bool_property(
            OBJECT_BINARY_VALUE, room->power_instance, PROP_PRESENT_VALUE, &power);

        off += snprintf(
            buf + off, sizeof(buf) - off,
            "%s{\"id\":%u,\"name\":\"%s\",\"setpoint_valid\":%s,\"setpoint\":%.1f,"
            "\"temperature_valid\":%s,\"temperature\":%.1f,"
            "\"power_valid\":%s,\"power\":%s}",
            first_room ? "" : ",", (unsigned)i, room->name, setpoint_valid ? "true" : "false",
            setpoint_valid ? setpoint : 0.0f, temperature_valid ? "true" : "false",
            temperature_valid ? temperature : 0.0f, power_valid ? "true" : "false",
            power ? "true" : "false");
        first_room = false;
    }
    off += snprintf(buf + off, sizeof(buf) - off, "]}");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, off);
    return ESP_OK;
}

static const httpd_uri_t api_status_uri = {
    .uri = "/api/status", .method = HTTP_GET, .handler = api_status_get_handler};

static esp_err_t api_health_get_handler(httpd_req_t *req)
{
    /* Cooling metrics */
    float cooling_output = 0.0f, required_cooling_output = 0.0f;
    bool cooling_output_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_COOLING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &cooling_output);
    bool required_cooling_output_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_REQUIRED_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &required_cooling_output);

    float design_cooling_duty = 0.0f;
    bool design_cooling_duty_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_DESIGN_COOLING_DUTY_INSTANCE, PROP_PRESENT_VALUE,
        &design_cooling_duty);

    float cooling_flow = 0.0f, required_cooling_flow = 0.0f, cooling_flow_design_pct = 0.0f;
    bool cooling_flow_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_COOLING_FLOW_INSTANCE, PROP_PRESENT_VALUE, &cooling_flow);
    bool required_cooling_flow_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_REQUIRED_FLOW_INSTANCE, PROP_PRESENT_VALUE, &required_cooling_flow);
    bool cooling_flow_design_pct_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_FLOW_DESIGN_PCT_INSTANCE, PROP_PRESENT_VALUE, &cooling_flow_design_pct);

    float cooling_valve_signal = 0.0f;
    bool cooling_valve_signal_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_OUTPUT, HEALTH_VALVE_SIGNAL_INSTANCE, PROP_PRESENT_VALUE, &cooling_valve_signal);

    /* Heating metrics */
    float heating_output = 0.0f, required_heating_output = 0.0f;
    bool heating_output_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_HEATING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &heating_output);
    bool required_heating_output_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_REQUIRED_HEATING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &required_heating_output);

    float design_heating_duty = 0.0f;
    bool design_heating_duty_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_DESIGN_HEATING_DUTY_INSTANCE, PROP_PRESENT_VALUE,
        &design_heating_duty);

    float heating_flow = 0.0f, required_heating_flow = 0.0f, heating_flow_design_pct = 0.0f;
    bool heating_flow_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_HEATING_FLOW_INSTANCE, PROP_PRESENT_VALUE, &heating_flow);
    bool required_heating_flow_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_REQUIRED_HEATING_FLOW_INSTANCE, PROP_PRESENT_VALUE, &required_heating_flow);
    bool heating_flow_design_pct_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_HEATING_FLOW_DESIGN_PCT_INSTANCE, PROP_PRESENT_VALUE, &heating_flow_design_pct);

    float heating_valve_signal = 0.0f;
    bool heating_valve_signal_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_OUTPUT, HEALTH_HEATING_VALVE_SIGNAL_INSTANCE, PROP_PRESENT_VALUE, &heating_valve_signal);

    float return_air = 0.0f;
    bool return_air_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_INPUT, HEALTH_RETURN_AIR_INSTANCE, PROP_PRESENT_VALUE, &return_air);

    float fan_count_raw = 0.0f;
    bool fan_count_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_FAN_COUNT_INSTANCE, PROP_PRESENT_VALUE, &fan_count_raw);
    int fan_count = 0;
    if (fan_count_valid) {
        fan_count = (int)(fan_count_raw + 0.5f);
        if (fan_count < 0) {
            fan_count = 0;
        }
        if (fan_count > HEALTH_FAN_SUPPLY_AIR_COUNT) {
            fan_count = HEALTH_FAN_SUPPLY_AIR_COUNT;
        }
    }

    int fans_to_read = fan_count_valid ? fan_count : 2;
    /* Also read every fan that has a room to name it after, even if the
       controller's own fan-count point disagrees - otherwise a room-named fan
       beyond that count would be shown but permanently "Unreadable". */
    if (fan_room_count() > fans_to_read) fans_to_read = fan_room_count();
    if (fans_to_read > HEALTH_FAN_SUPPLY_AIR_COUNT) fans_to_read = HEALTH_FAN_SUPPLY_AIR_COUNT;
    if (fans_to_read < 1) fans_to_read = 1;

    float fan_supply[HEALTH_FAN_SUPPLY_AIR_COUNT];
    bool fan_supply_valid[HEALTH_FAN_SUPPLY_AIR_COUNT];
    for (int i = 0; i < HEALTH_FAN_SUPPLY_AIR_COUNT; i++) {
        fan_supply[i] = 0.0f;
        fan_supply_valid[i] = (i < fans_to_read) && BacnetReady && read_real_property(
            OBJECT_ANALOG_INPUT, HealthFanSupplyAirInstances[i], PROP_PRESENT_VALUE, &fan_supply[i]);
    }

    float fan_speeds[HEALTH_FAN_SPEED_COUNT];
    bool fan_speeds_valid[HEALTH_FAN_SPEED_COUNT];
    for (int i = 0; i < HEALTH_FAN_SPEED_COUNT; i++) {
        fan_speeds[i] = 0.0f;
        fan_speeds_valid[i] = (i < fans_to_read) && BacnetReady && read_real_property(
            OBJECT_ANALOG_OUTPUT, HealthFanSpeedInstances[i], PROP_PRESENT_VALUE, &fan_speeds[i]);
    }

    float room_supply_air[MAX_ROOMS];
    bool room_supply_air_valid[MAX_ROOMS];
    float supply_air_sum = 0.0f;
    int supply_air_count = 0;
    for (size_t i = 0; i < RoomCount; i++) {
        room_supply_air_valid[i] = BacnetReady && read_real_property(
            OBJECT_ANALOG_VALUE, Rooms[i].supply_air_instance, PROP_PRESENT_VALUE, &room_supply_air[i]);
        if (Rooms[i].active && room_supply_air_valid[i] && temp_is_plausible(room_supply_air[i])) {
            supply_air_sum += room_supply_air[i];
            supply_air_count++;
        }
    }

    /* Determine active mode */
    const char *active_mode = "idle";
    float cool_dem = (required_cooling_output_valid && required_cooling_output > 0.05f) ? required_cooling_output : (cooling_output_valid ? cooling_output : 0.0f);
    float heat_dem = (required_heating_output_valid && required_heating_output > 0.05f) ? required_heating_output : (heating_output_valid ? heating_output : 0.0f);
    if (heat_dem > cool_dem && heat_dem > 0.05f) {
        active_mode = "heating";
    } else if (cool_dem > heat_dem && cool_dem > 0.05f) {
        active_mode = "cooling";
    } else if (heating_valve_signal_valid && heating_valve_signal > 5.0f && (!cooling_valve_signal_valid || heating_valve_signal > cooling_valve_signal)) {
        active_mode = "heating";
    } else if (cooling_valve_signal_valid && cooling_valve_signal > 5.0f) {
        active_mode = "cooling";
    }

    bool any_alarm_active = false;
    char active_alarm_label[64] = "";
    bool alarm_active[HEALTH_ALARM_COUNT];
    bool alarm_valid[HEALTH_ALARM_COUNT];
    for (size_t i = 0; i < HEALTH_ALARM_COUNT; i++) {
        alarm_active[i] = false;
        alarm_valid[i] = false;
    }
    /* Every alarm point must be read unconditionally - a prior version only read
       alarms[1..] when alarms[0] (Master Alarm) was active, which is backwards:
       Master Alarm is false the overwhelming majority of the time, so that left
       6 of 7 alarms permanently "valid":false and rendering as "?" on the health
       page regardless of their real state. */
    for (size_t i = 0; i < HEALTH_ALARM_COUNT; i++) {
        alarm_valid[i] = BacnetReady && read_bool_property(
            OBJECT_BINARY_VALUE, HealthAlarms[i].instance, PROP_PRESENT_VALUE, &alarm_active[i]);
        if (alarm_valid[i] && alarm_active[i] && !any_alarm_active) {
            any_alarm_active = true;
            snprintf(active_alarm_label, sizeof(active_alarm_label), "%s", HealthAlarms[i].label);
        }
    }

    bool is_heating = (strcmp(active_mode, "heating") == 0);
    float cur_out = is_heating ? (heating_output_valid ? heating_output : 0.0f) : (cooling_output_valid ? cooling_output : 0.0f);
    bool cur_out_valid = is_heating ? heating_output_valid : cooling_output_valid;
    float cur_req = is_heating ? (required_heating_output_valid ? required_heating_output : 0.0f) : (required_cooling_output_valid ? required_cooling_output : 0.0f);
    bool cur_req_valid = is_heating ? required_heating_output_valid : required_cooling_output_valid;
    float cur_fp = is_heating ? (heating_flow_design_pct_valid ? heating_flow_design_pct : 0.0f) : (cooling_flow_design_pct_valid ? cooling_flow_design_pct : 0.0f);
    bool cur_fp_valid = is_heating ? heating_flow_design_pct_valid : cooling_flow_design_pct_valid;
    float cur_vsig = is_heating ? (heating_valve_signal_valid ? heating_valve_signal : 0.0f) : (cooling_valve_signal_valid ? cooling_valve_signal : 0.0f);
    bool cur_vsig_valid = is_heating ? heating_valve_signal_valid : cooling_valve_signal_valid;

    float avg_sa = (supply_air_count > 0) ? (supply_air_sum / supply_air_count) : 0.0f;
    health_diagnostics_t diag;
    compute_health_diagnostics(
        is_heating,
        cur_out_valid, cur_out,
        cur_req_valid, cur_req,
        cur_fp_valid, cur_fp,
        cur_vsig_valid, cur_vsig,
        return_air_valid, return_air,
        supply_air_count > 0, avg_sa,
        any_alarm_active, active_alarm_label,
        &diag);

    unsigned cooling_valve_status = 0, heating_valve_status = 0;
    bool cooling_valve_status_valid = BacnetReady && read_msv_property(
        OBJECT_MULTI_STATE_VALUE, HEALTH_COOLING_VALVE_STATUS_INSTANCE, PROP_PRESENT_VALUE,
        &cooling_valve_status);
    bool heating_valve_status_valid = BacnetReady && read_msv_property(
        OBJECT_MULTI_STATE_VALUE, HEALTH_HEATING_VALVE_STATUS_INSTANCE, PROP_PRESENT_VALUE,
        &heating_valve_status);

    char diag_stat_esc[128], diag_det_esc[384];
    json_escape(diag_stat_esc, diag.diag_status, sizeof(diag_stat_esc));
    json_escape(diag_det_esc, diag.diag_detail, sizeof(diag_det_esc));

    char *buf = malloc(6144);
    if (!buf) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    size_t off = 0;
    off += snprintf(
        buf + off, 6144 - off,
        "{\"active_mode\":\"%s\","
        "\"cooling_output_valid\":%s,\"cooling_output\":%.2f,"
        "\"required_output_valid\":%s,\"required_output\":%.2f,"
        "\"cooling_flow_valid\":%s,\"cooling_flow\":%.4f,"
        "\"required_flow_valid\":%s,\"required_flow\":%.4f,"
        "\"flow_design_pct_valid\":%s,\"flow_design_pct\":%.1f,"
        "\"valve_signal_valid\":%s,\"valve_signal\":%.1f,"
        "\"design_cooling_duty_valid\":%s,\"design_cooling_duty\":%.2f,"
        "\"heating_output_valid\":%s,\"heating_output\":%.2f,"
        "\"required_heating_output_valid\":%s,\"required_heating_output\":%.2f,"
        "\"heating_flow_valid\":%s,\"heating_flow\":%.4f,"
        "\"required_heating_flow_valid\":%s,\"required_heating_flow\":%.4f,"
        "\"heating_flow_design_pct_valid\":%s,\"heating_flow_design_pct\":%.1f,"
        "\"heating_valve_signal_valid\":%s,\"heating_valve_signal\":%.1f,"
        "\"design_heating_duty_valid\":%s,\"design_heating_duty\":%.2f,"
        "\"delta_t_valid\":%s,\"delta_t\":%.2f,"
        "\"performance\":\"%s\",\"performance_level\":\"%s\","
        "\"system_health\":\"%s\",\"diag_status\":\"%s\",\"diag_detail\":\"%s\","
        "\"return_air_valid\":%s,\"return_air\":%.1f,"
        "\"cooling_valve_status\":\"%s\",\"heating_valve_status\":\"%s\",",
        active_mode,
        cooling_output_valid ? "true" : "false", cooling_output_valid ? cooling_output : 0.0f,
        required_cooling_output_valid ? "true" : "false", required_cooling_output_valid ? required_cooling_output : 0.0f,
        cooling_flow_valid ? "true" : "false", cooling_flow_valid ? cooling_flow : 0.0f,
        required_cooling_flow_valid ? "true" : "false", required_cooling_flow_valid ? required_cooling_flow : 0.0f,
        cooling_flow_design_pct_valid ? "true" : "false", cooling_flow_design_pct_valid ? cooling_flow_design_pct : 0.0f,
        cooling_valve_signal_valid ? "true" : "false", cooling_valve_signal_valid ? cooling_valve_signal : 0.0f,
        design_cooling_duty_valid ? "true" : "false", design_cooling_duty_valid ? design_cooling_duty : 0.0f,
        heating_output_valid ? "true" : "false", heating_output_valid ? heating_output : 0.0f,
        required_heating_output_valid ? "true" : "false", required_heating_output_valid ? required_heating_output : 0.0f,
        heating_flow_valid ? "true" : "false", heating_flow_valid ? heating_flow : 0.0f,
        required_heating_flow_valid ? "true" : "false", required_heating_flow_valid ? required_heating_flow : 0.0f,
        heating_flow_design_pct_valid ? "true" : "false", heating_flow_design_pct_valid ? heating_flow_design_pct : 0.0f,
        heating_valve_signal_valid ? "true" : "false", heating_valve_signal_valid ? heating_valve_signal : 0.0f,
        design_heating_duty_valid ? "true" : "false", design_heating_duty_valid ? design_heating_duty : 0.0f,
        diag.delta_t_valid ? "true" : "false", diag.delta_t_valid ? diag.delta_t : 0.0f,
        diag.performance, diag.performance_level,
        diag.system_health, diag_stat_esc, diag_det_esc,
        return_air_valid ? "true" : "false", return_air_valid ? return_air : 0.0f,
        (cooling_valve_status_valid && cooling_valve_status < 4) ?
            HealthValveStatusNames[cooling_valve_status] : "Unavailable",
        (heating_valve_status_valid && heating_valve_status < 4) ?
            HealthValveStatusNames[heating_valve_status] : "Unavailable");

    /* Only the fan channels with an associated room are emitted here - the rest
       are hidden rather than shown as unlabelled "not in fan count" rows. See
       fan_room_count()/fan_room_name() for the room<->fan assumption. */
    int used_fans = fan_room_count();
    off += snprintf(
        buf + off, 6144 - off,
        "\"fan_count_valid\":%s,\"fan_count\":%d,\"fan_channels_total\":%d,\"fan_supply_air\":[",
        fan_count_valid ? "true" : "false", fan_count, HEALTH_FAN_SUPPLY_AIR_COUNT);
    for (int i = 0; i < used_fans && off < 6144; i++) {
        bool spd_valid = (i < HEALTH_FAN_SPEED_COUNT) && fan_speeds_valid[i];
        float spd_val = spd_valid ? fan_speeds[i] : 0.0f;
        char room_name[32] = "";
        fan_room_name(i + 1, room_name, sizeof(room_name));
        char room_esc[64];
        json_escape(room_esc, room_name, sizeof(room_esc));
        off += snprintf(
            buf + off, 6144 - off,
            "%s{\"fan\":%u,\"room\":\"%s\",\"valid\":%s,\"value\":%.1f,\"speed_valid\":%s,\"speed\":%.1f,\"plausible\":%s,\"configured\":%s}",
            i == 0 ? "" : ",", (unsigned)HealthFanSupplyAirInstances[i], room_esc,
            fan_supply_valid[i] ? "true" : "false", fan_supply_valid[i] ? fan_supply[i] : 0.0f,
            spd_valid ? "true" : "false", spd_val,
            (fan_supply_valid[i] && temp_is_plausible(fan_supply[i])) ? "true" : "false",
            (fan_count_valid && i < fan_count) ? "true" : "false");
    }
    off += snprintf(buf + off, 6144 - off, "],\"rooms\":[");

    bool first_h_room = true;
    for (size_t i = 0; i < RoomCount && off < 6144; i++) {
        const room_config_t *room = &Rooms[i];
        if (!room->active) {
            continue;
        }
        float required = 0.0f, current = 0.0f;
        bool required_valid = BacnetReady && read_real_property(
            OBJECT_ANALOG_VALUE, room->required_output_instance, PROP_PRESENT_VALUE, &required);
        bool current_valid = BacnetReady && read_real_property(
            OBJECT_ANALOG_VALUE, room->current_output_instance, PROP_PRESENT_VALUE, &current);

        off += snprintf(
            buf + off, 6144 - off,
            "%s{\"name\":\"%s\",\"supply_air_valid\":%s,\"supply_air\":%.1f,"
            "\"required_output_valid\":%s,\"required_output\":%.2f,"
            "\"current_output_valid\":%s,\"current_output\":%.2f}",
            first_h_room ? "" : ",", room->name, room_supply_air_valid[i] ? "true" : "false",
            room_supply_air_valid[i] ? room_supply_air[i] : 0.0f, required_valid ? "true" : "false",
            required_valid ? required : 0.0f, current_valid ? "true" : "false",
            current_valid ? current : 0.0f);
        first_h_room = false;
    }
    off += snprintf(buf + off, 6144 - off, "],\"alarms\":[");

    for (size_t i = 0; i < HEALTH_ALARM_COUNT && off < 6144; i++) {
        off += snprintf(
            buf + off, 6144 - off, "%s{\"label\":\"%s\",\"valid\":%s,\"active\":%s}",
            i == 0 ? "" : ",", HealthAlarms[i].label,
            alarm_valid[i] ? "true" : "false",
            alarm_active[i] ? "true" : "false");
    }
    off += snprintf(buf + off, 6144 - off, "]}");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, off);
    free(buf);
    return ESP_OK;
}

static const httpd_uri_t api_health_uri = {
    .uri = "/api/health", .method = HTTP_GET, .handler = api_health_get_handler};

static bool recv_body(httpd_req_t *req, char *body, size_t body_len)
{
    int total = req->content_len < (int)body_len - 1 ? req->content_len : (int)body_len - 1;
    int received = httpd_req_recv(req, body, total);
    if (received <= 0) {
        return false;
    }
    body[received] = '\0';
    return true;
}

static void send_write_result(httpd_req_t *req, bool ok, const char *extra_json)
{
    httpd_resp_set_type(req, "application/json");
    if (!ok) {
        httpd_resp_send(req, "{\"ok\":false,\"error\":\"write failed\"}", HTTPD_RESP_USE_STRLEN);
        return;
    }
    if (extra_json) {
        httpd_resp_send(req, extra_json, HTTPD_RESP_USE_STRLEN);
    } else {
        httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);
    }
}

static void send_bad_request(httpd_req_t *req, const char *error_json)
{
    httpd_resp_set_status(req, "400 Bad Request");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, error_json, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t api_bacnet_read_get_handler(httpd_req_t *req)
{
    char query[256] = {0};
    if (httpd_req_get_url_query_str(req, query, sizeof(query)) != ESP_OK) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"need type, instance, property query params\"}");
        return ESP_OK;
    }
    char type_str[24] = {0}, instance_str[12] = {0}, prop_str[32] = {0}, index_str[12] = {0};
    if (!form_extract(query, "type", type_str, sizeof(type_str)) ||
        !form_extract(query, "instance", instance_str, sizeof(instance_str)) ||
        !form_extract(query, "property", prop_str, sizeof(prop_str))) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"need type, instance, property query params\"}");
        return ESP_OK;
    }
    BACNET_OBJECT_TYPE object_type;
    BACNET_PROPERTY_ID property;
    if (!parse_object_type(type_str, &object_type) || !parse_property_id(prop_str, &property)) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"unrecognized type or property name\"}");
        return ESP_OK;
    }
    uint32_t instance = (uint32_t)strtoul(instance_str, NULL, 10);
    uint32_t array_index = BACNET_ARRAY_ALL;
    if (form_extract(query, "index", index_str, sizeof(index_str)) && strlen(index_str) > 0) {
        array_index = (uint32_t)strtoul(index_str, NULL, 10);
    }

    char resp[1024];
    explorer_read(object_type, instance, property, array_index, resp, sizeof(resp));
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, resp, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static const httpd_uri_t api_bacnet_read_uri = {
    .uri = "/api/bacnet/read", .method = HTTP_GET, .handler = api_bacnet_read_get_handler};

static esp_err_t api_bacnet_write_post_handler(httpd_req_t *req)
{
    char body[256] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    char type_str[24] = {0}, instance_str[12] = {0}, prop_str[32] = {0};
    char index_str[12] = {0}, priority_str[8] = {0};
    char valuetag_str[8] = {0}, value_str[32] = {0};
    if (!form_extract(body, "type", type_str, sizeof(type_str)) ||
        !form_extract(body, "instance", instance_str, sizeof(instance_str)) ||
        !form_extract(body, "property", prop_str, sizeof(prop_str)) ||
        !form_extract(body, "valuetag", valuetag_str, sizeof(valuetag_str)) ||
        !form_extract(body, "value", value_str, sizeof(value_str))) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"need type, instance, property, valuetag, value\"}");
        return ESP_OK;
    }
    BACNET_OBJECT_TYPE object_type;
    BACNET_PROPERTY_ID property;
    if (!parse_object_type(type_str, &object_type) || !parse_property_id(prop_str, &property)) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"unrecognized type or property name\"}");
        return ESP_OK;
    }
    uint32_t instance = (uint32_t)strtoul(instance_str, NULL, 10);
    uint32_t array_index = BACNET_ARRAY_ALL;
    if (form_extract(body, "index", index_str, sizeof(index_str)) && strlen(index_str) > 0) {
        array_index = (uint32_t)strtoul(index_str, NULL, 10);
    }
    uint8_t priority = BACNET_NO_PRIORITY;
    if (form_extract(body, "priority", priority_str, sizeof(priority_str)) && strlen(priority_str) > 0) {
        unsigned long p = strtoul(priority_str, NULL, 10);
        if (p < BACNET_MIN_PRIORITY || p > BACNET_MAX_PRIORITY) {
            send_bad_request(req, "{\"ok\":false,\"error\":\"priority must be 1-16\"}");
            return ESP_OK;
        }
        priority = (uint8_t)p;
    }

    char resp[128];
    explorer_write(
        object_type, instance, property, array_index, priority, valuetag_str, value_str, resp,
        sizeof(resp));
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, resp, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static const httpd_uri_t api_bacnet_write_uri = {
    .uri = "/api/bacnet/write", .method = HTTP_POST, .handler = api_bacnet_write_post_handler};

/* ===================== JSON & Helper Primitives ===================== */

static bool json_get_str(const char *json, const char *key, char *out, size_t out_len)
{
    if (!json || !key || !out || out_len == 0) return false;
    char search[64];
    snprintf(search, sizeof(search), "\"%s\"", key);
    const char *p = strstr(json, search);
    if (!p) return false;
    p += strlen(search);
    while (*p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n' || *p == ':')) p++;
    if (*p == '\"') {
        p++;
        size_t idx = 0;
        while (*p && *p != '\"' && idx + 1 < out_len) {
            if (*p == '\\' && *(p + 1)) {
                p++;
            }
            out[idx++] = *p++;
        }
        out[idx] = '\0';
        return true;
    }
    return false;
}

static bool json_get_int(const char *json, const char *key, int *out)
{
    if (!json || !key || !out) return false;
    char search[64];
    snprintf(search, sizeof(search), "\"%s\"", key);
    const char *p = strstr(json, search);
    if (!p) return false;
    p += strlen(search);
    while (*p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n' || *p == ':')) p++;
    char *end = NULL;
    long val = strtol(p, &end, 10);
    if (end != p) {
        *out = (int)val;
        return true;
    }
    return false;
}

static bool json_get_float(const char *json, const char *key, float *out)
{
    if (!json || !key || !out) return false;
    char search[64];
    snprintf(search, sizeof(search), "\"%s\"", key);
    const char *p = strstr(json, search);
    if (!p) return false;
    p += strlen(search);
    while (*p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n' || *p == ':')) p++;
    if (*p == '\"') p++;
    char *end = NULL;
    float val = strtof(p, &end);
    if (end != p) {
        *out = val;
        return true;
    }
    return false;
}

static bool json_get_bool(const char *json, const char *key, bool *out)
{
    if (!json || !key || !out) return false;
    char search[64];
    snprintf(search, sizeof(search), "\"%s\"", key);
    const char *p = strstr(json, search);
    if (!p) return false;
    p += strlen(search);
    while (*p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n' || *p == ':')) p++;
    if (strncmp(p, "true", 4) == 0) {
        *out = true;
        return true;
    } else if (strncmp(p, "false", 5) == 0) {
        *out = false;
        return true;
    }
    return false;
}

static bool json_extract_obj(const char *json, const char *key, char *out, size_t out_len)
{
    if (!json || !key || !out || out_len == 0) return false;
    char search[64];
    snprintf(search, sizeof(search), "\"%s\"", key);
    const char *p = strstr(json, search);
    if (!p) return false;
    p += strlen(search);
    while (*p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n' || *p == ':')) p++;
    if (*p == '{') {
        const char *start = p;
        int depth = 0;
        while (*p) {
            if (*p == '{') depth++;
            else if (*p == '}') {
                depth--;
                if (depth == 0) {
                    p++;
                    break;
                }
            }
            p++;
        }
        size_t len = (size_t)(p - start);
        if (len >= out_len) len = out_len - 1;
        memcpy(out, start, len);
        out[len] = '\0';
        return true;
    }
    return false;
}

static bool recv_full_body(httpd_req_t *req, char **out_body, size_t *out_len)
{
    int total_len = req->content_len;
    if (total_len <= 0 || total_len > 16384) {
        return false;
    }
    char *buf = malloc(total_len + 1);
    if (!buf) {
        return false;
    }
    int cur = 0;
    while (cur < total_len) {
        int received = httpd_req_recv(req, buf + cur, total_len - cur);
        if (received <= 0) {
            if (received == HTTPD_SOCK_ERR_TIMEOUT) {
                continue;
            }
            free(buf);
            return false;
        }
        cur += received;
    }
    buf[cur] = '\0';
    *out_body = buf;
    if (out_len) *out_len = (size_t)cur;
    return true;
}

/* Parses room index and value from either JSON body ({"room":0, "value":"..."})
   or form body ("room=0&value=..."). Validates against RoomCount. */
static bool parse_room_and_value(
    httpd_req_t *req, const char *body, int *out_room_idx, char *value_out, size_t value_out_len)
{
    int room_idx = -1;
    if (body && body[0] == '{') {
        int r = -1;
        if (json_get_int(body, "room", &r) || json_get_int(body, "id", &r)) {
            room_idx = r;
        }
        if (!json_get_str(body, "value", value_out, value_out_len)) {
            float fval = 0.0f;
            if (json_get_float(body, "value", &fval) || json_get_float(body, "setpoint", &fval)) {
                snprintf(value_out, value_out_len, "%.1f", fval);
            }
        }
    } else if (body) {
        char room_str[16] = {0};
        if (form_extract(body, "room", room_str, sizeof(room_str)) ||
            form_extract(body, "id", room_str, sizeof(room_str))) {
            room_idx = atoi(room_str);
        }
        form_extract(body, "value", value_out, value_out_len);
    }
    if (room_idx < 0 || room_idx >= (int)RoomCount || strlen(value_out) == 0) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"missing or invalid room/value\"}");
        return false;
    }
    *out_room_idx = room_idx;
    return true;
}

static esp_err_t api_room_setpoint_post_handler(httpd_req_t *req)
{
    char body[128] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    int room_idx;
    char value_str[16] = {0};
    if (!parse_room_and_value(req, body, &room_idx, value_str, sizeof(value_str))) {
        return ESP_OK;
    }

    float value = strtof(value_str, NULL);
    if (value < MIN_SETPOINT_C) {
        /* Touchscreen-enforced floor - BACnet itself doesn't stop a lower
           write (confirmed in Phase 0.5 testing), so clamp here instead. */
        value = MIN_SETPOINT_C;
    }

    bool ok = BacnetReady && write_real_property(
        OBJECT_ANALOG_VALUE, Rooms[room_idx].setpoint_instance, PROP_PRESENT_VALUE, value);
    char resp[64];
    snprintf(resp, sizeof(resp), "{\"ok\":true,\"setpoint\":%.1f}", value);
    send_write_result(req, ok, resp);
    return ESP_OK;
}

static const httpd_uri_t api_room_setpoint_uri = {
    .uri = "/api/room-setpoint", .method = HTTP_POST, .handler = api_room_setpoint_post_handler};

static esp_err_t api_room_power_post_handler(httpd_req_t *req)
{
    char body[64] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    int room_idx;
    char value_str[8] = {0};
    if (!parse_room_and_value(req, body, &room_idx, value_str, sizeof(value_str))) {
        return ESP_OK;
    }
    if (strcmp(value_str, "on") != 0 && strcmp(value_str, "off") != 0) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"value must be on/off\"}");
        return ESP_OK;
    }

    bool on = strcmp(value_str, "on") == 0;
    bool ok = BacnetReady && write_bool_property(
        OBJECT_BINARY_VALUE, Rooms[room_idx].power_instance, PROP_PRESENT_VALUE, on);
    send_write_result(req, ok, NULL);
    return ESP_OK;
}

static const httpd_uri_t api_room_power_uri = {
    .uri = "/api/room-power", .method = HTTP_POST, .handler = api_room_power_post_handler};

static esp_err_t api_system_power_post_handler(httpd_req_t *req)
{
    char body[32] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    char value_str[8] = {0};
    form_extract(body, "value", value_str, sizeof(value_str));
    if (strcmp(value_str, "on") != 0 && strcmp(value_str, "off") != 0) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"value must be on/off\"}");
        return ESP_OK;
    }

    bool on = strcmp(value_str, "on") == 0;
    bool ok = BacnetReady && write_bool_property(
        OBJECT_BINARY_VALUE, SYS_POWER_WRITE_INSTANCE, PROP_PRESENT_VALUE, on);
    send_write_result(req, ok, NULL);
    return ESP_OK;
}

static const httpd_uri_t api_system_power_uri = {
    .uri = "/api/system-power", .method = HTTP_POST, .handler = api_system_power_post_handler};

static esp_err_t api_boost_post_handler(httpd_req_t *req)
{
    char body[32] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    char value_str[8] = {0};
    form_extract(body, "value", value_str, sizeof(value_str));

    unsigned mode;
    if (strcmp(value_str, "auto") == 0) {
        mode = BOOST_MODE_AUTO;
    } else if (strcmp(value_str, "heat") == 0) {
        mode = BOOST_MODE_FULL_HEATING;
    } else if (strcmp(value_str, "cool") == 0) {
        mode = BOOST_MODE_FULL_COOLING;
    } else {
        send_bad_request(req, "{\"ok\":false,\"error\":\"value must be auto/heat/cool\"}");
        return ESP_OK;
    }

    bool ok = boost_apply(mode, true);
    send_write_result(req, ok, NULL);
    return ESP_OK;
}

static const httpd_uri_t api_boost_uri = {
    .uri = "/api/boost", .method = HTTP_POST, .handler = api_boost_post_handler};

static esp_err_t api_boost_timeout_post_handler(httpd_req_t *req)
{
    char body[32] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    char value_str[8] = {0};
    form_extract(body, "value", value_str, sizeof(value_str));

    char *endptr = NULL;
    long minutes = strtol(value_str, &endptr, 10);
    if (endptr == value_str || minutes < 0 || minutes > BOOST_TIMEOUT_MAX_MIN) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"value must be 0-240\"}");
        return ESP_OK;
    }

    BoostTimeoutMinutes = (uint16_t)minutes;
    app_config_save();
    if (BoostSelfInitiated && BoostDeadlineUs != 0) {
        BoostDeadlineUs = BoostTimeoutMinutes > 0
            ? esp_timer_get_time() + (int64_t)BoostTimeoutMinutes * 60000000LL
            : 0;
    }
    send_write_result(req, true, NULL);
    return ESP_OK;
}

static const httpd_uri_t api_boost_timeout_uri = {
    .uri = "/api/boost-timeout", .method = HTTP_POST, .handler = api_boost_timeout_post_handler};

static esp_err_t api_wifi_reset_post_handler(httpd_req_t *req)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_NAMESPACE, NVS_READWRITE, &handle) == ESP_OK) {
        nvs_erase_key(handle, NVS_KEY_SSID);
        nvs_erase_key(handle, NVS_KEY_PASS);
        nvs_commit(handle);
        nvs_close(handle);
    }

    ESP_LOGI(TAG_WIFI, "WiFi credentials cleared via dashboard, restarting in 2s...");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);

    vTaskDelay(pdMS_TO_TICKS(2000));
    esp_restart();
    return ESP_OK;
}

static const httpd_uri_t api_wifi_reset_uri = {
    .uri = "/api/wifi-reset", .method = HTTP_POST, .handler = api_wifi_reset_post_handler};

/* ===================== LAN firmware update (OTA) ===================== */
/* User-chosen admin password (not auto-generated - a device you can't ever
   ask "what did I set this to" over USB once it's in the wall is worse
   than one you set on purpose and are responsible for remembering).
   Stashed in NVS, never in source/git. Set from the setup wizard's Step 1
   (api_setup_password_post_handler below) - see the "before then, no auth"
   logic in check_ota_auth(): with no password chosen yet, /api/ota is wide
   open, exactly like a factory-reset router before you set its admin
   password. Losing/forgetting it isn't a USB-recovery problem anymore
   either: a factory reset (already wireless-reachable) wipes
   OTA_NVS_NAMESPACE, which drops this straight back to the open/unset
   state, ready for Step 1 to set a new one - no secret ever needs to
   round-trip back out over the network to recover from that. */
#define OTA_NVS_NAMESPACE "ota_cfg"
#define OTA_NVS_KEY_PASSWORD "password"
#define OTA_PASSWORD_MAX_LEN 64
#define OTA_PASSWORD_MIN_LEN 8
static char OtaPassword[OTA_PASSWORD_MAX_LEN + 1] = {0};

static bool ota_password_is_set(void)
{
    return OtaPassword[0] != '\0';
}

static void ota_password_load(void)
{
    nvs_handle_t handle;
    if (nvs_open(OTA_NVS_NAMESPACE, NVS_READONLY, &handle) == ESP_OK) {
        size_t len = sizeof(OtaPassword);
        nvs_get_str(handle, OTA_NVS_KEY_PASSWORD, OtaPassword, &len);
        nvs_close(handle);
    }
    if (ota_password_is_set()) {
        ESP_LOGI(TAG_WIFI, "OTA update (/update) password: set (user: admin)");
    } else {
        ESP_LOGW(
            TAG_WIFI,
            "OTA update (/update) password: NOT SET - /api/ota is OPEN to anyone on the LAN "
            "until one is chosen in the setup wizard's Step 1");
    }
}

static void ota_password_store(const char *pw)
{
    nvs_handle_t handle;
    if (nvs_open(OTA_NVS_NAMESPACE, NVS_READWRITE, &handle) == ESP_OK) {
        nvs_set_str(handle, OTA_NVS_KEY_PASSWORD, pw);
        nvs_commit(handle);
        nvs_close(handle);
    }
    strlcpy(OtaPassword, pw, sizeof(OtaPassword));
}

/* Constant-time-ish compare - avoids an early-exit strcmp() timing signal on
   an endpoint that flashes arbitrary firmware. Not that this device faces
   anything more sophisticated than curious housemates, but it's free. */
static bool secure_streq(const char *a, const char *b)
{
    size_t la = strlen(a), lb = strlen(b);
    if (la != lb) {
        return false;
    }
    uint8_t diff = 0;
    for (size_t i = 0; i < la; i++) {
        diff |= (uint8_t)(a[i] ^ b[i]);
    }
    return diff == 0;
}

static void send_401(httpd_req_t *req)
{
    httpd_resp_set_status(req, "401 Unauthorized");
    httpd_resp_set_hdr(req, "WWW-Authenticate", "Basic realm=\"OTA Update\"");
    httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
}

/* Expects "Authorization: Basic base64(admin:<OtaPassword>)". Returns false
   (and has already sent a 401) on any missing/malformed/wrong credential.
   No password chosen yet (fresh device, or just after a factory reset) ->
   open, so Step 1 of the setup wizard can set the first one without a
   chicken-and-egg "prove you know the password you're about to set". */
static bool check_ota_auth(httpd_req_t *req)
{
    if (!ota_password_is_set()) {
        return true;
    }

    size_t hdr_len = httpd_req_get_hdr_value_len(req, "Authorization");
    if (hdr_len == 0) {
        send_401(req);
        return false;
    }
    char *hdr = malloc(hdr_len + 1);
    if (!hdr) {
        httpd_resp_send_500(req);
        return false;
    }
    esp_err_t err = httpd_req_get_hdr_value_str(req, "Authorization", hdr, hdr_len + 1);
    if (err != ESP_OK || strncmp(hdr, "Basic ", 6) != 0) {
        free(hdr);
        send_401(req);
        return false;
    }

    unsigned char decoded[96] = {0};
    size_t decoded_len = 0;
    int rc = mbedtls_base64_decode(
        decoded, sizeof(decoded) - 1, &decoded_len, (unsigned char *)hdr + 6, strlen(hdr + 6));
    free(hdr);
    if (rc != 0) {
        send_401(req);
        return false;
    }
    decoded[decoded_len] = '\0';

    char expected[16 + OTA_PASSWORD_MAX_LEN + 2];
    snprintf(expected, sizeof(expected), "admin:%s", OtaPassword);
    if (!secure_streq((char *)decoded, expected)) {
        send_401(req);
        return false;
    }
    return true;
}

static esp_err_t api_ota_post_handler(httpd_req_t *req)
{
    if (!check_ota_auth(req)) {
        return ESP_OK;
    }
    if (req->content_len <= 0) {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(req, "{\"ok\":false,\"error\":\"empty body\"}", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    const esp_partition_t *update_partition = esp_ota_get_next_update_partition(NULL);
    if (!update_partition) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    ESP_LOGI(
        TAG_WIFI, "OTA: writing %lu bytes to partition '%s' at 0x%lx",
        (unsigned long)req->content_len, update_partition->label,
        (unsigned long)update_partition->address);

    /* Held for the whole transfer so a firmware flash and a live BACnet
       transaction (which shares the same 6s-timeout convention as every
       other handler here) can never interleave. */
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) != pdTRUE) {
        httpd_resp_set_status(req, "503 Service Unavailable");
        httpd_resp_send(req, "{\"ok\":false,\"error\":\"BACnet busy, try again\"}", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    esp_ota_handle_t ota_handle;
    esp_err_t err = esp_ota_begin(update_partition, req->content_len, &ota_handle);
    if (err != ESP_OK) {
        xSemaphoreGive(BacnetMutex);
        ESP_LOGE(TAG_WIFI, "esp_ota_begin failed: %s", esp_err_to_name(err));
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    char *buf = malloc(2048);
    if (!buf) {
        esp_ota_abort(ota_handle);
        xSemaphoreGive(BacnetMutex);
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    size_t remaining = req->content_len;
    bool write_failed = false;
    while (remaining > 0) {
        int to_recv = remaining < 2048 ? (int)remaining : 2048;
        int recv_len = httpd_req_recv(req, buf, to_recv);
        if (recv_len == HTTPD_SOCK_ERR_TIMEOUT) {
            continue;
        }
        if (recv_len <= 0) {
            write_failed = true;
            break;
        }
        if (esp_ota_write(ota_handle, buf, recv_len) != ESP_OK) {
            write_failed = true;
            break;
        }
        remaining -= (size_t)recv_len;
    }
    free(buf);

    if (write_failed) {
        esp_ota_abort(ota_handle);
        xSemaphoreGive(BacnetMutex);
        ESP_LOGE(TAG_WIFI, "OTA: transfer failed with %lu bytes remaining", (unsigned long)remaining);
        httpd_resp_set_status(req, "500 Internal Server Error");
        httpd_resp_send(req, "{\"ok\":false,\"error\":\"transfer failed\"}", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    err = esp_ota_end(ota_handle);
    if (err != ESP_OK) {
        xSemaphoreGive(BacnetMutex);
        ESP_LOGE(TAG_WIFI, "esp_ota_end failed (bad image?): %s", esp_err_to_name(err));
        httpd_resp_set_status(req, "422 Unprocessable Entity");
        httpd_resp_send(
            req, "{\"ok\":false,\"error\":\"image validation failed - not flashed\"}",
            HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    err = esp_ota_set_boot_partition(update_partition);
    xSemaphoreGive(BacnetMutex);
    if (err != ESP_OK) {
        ESP_LOGE(TAG_WIFI, "esp_ota_set_boot_partition failed: %s", esp_err_to_name(err));
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    ESP_LOGW(TAG_WIFI, "OTA: image written and validated, rebooting into it in 2s...");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);

    vTaskDelay(pdMS_TO_TICKS(2000));
    esp_restart();
    return ESP_OK;
}

static const httpd_uri_t api_ota_uri = {
    .uri = "/api/ota", .method = HTTP_POST, .handler = api_ota_post_handler};

static esp_err_t update_page_get_handler(httpd_req_t *req)
{
    const uint32_t len = update_page_end - update_page_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, update_page_start, len);
    return ESP_OK;
}
static const httpd_uri_t update_page_uri = {
    .uri = "/update", .method = HTTP_GET, .handler = update_page_get_handler};

static esp_err_t objects_page_get_handler(httpd_req_t *req)
{
    const uint32_t len = objects_page_end - objects_page_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, objects_page_start, len);
    return ESP_OK;
}
static const httpd_uri_t objects_page_uri = {
    .uri = "/objects", .method = HTTP_GET, .handler = objects_page_get_handler};

static esp_err_t wizard_page_get_handler(httpd_req_t *req)
{
    const uint32_t len = wizard_page_end - wizard_page_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, wizard_page_start, len);
    return ESP_OK;
}
static const httpd_uri_t wizard_page_uri = {
    .uri = "/wizard", .method = HTTP_GET, .handler = wizard_page_get_handler};

static esp_err_t mqtt_page_get_handler(httpd_req_t *req)
{
    const uint32_t len = mqtt_page_end - mqtt_page_start;
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, mqtt_page_start, len);
    return ESP_OK;
}
static const httpd_uri_t mqtt_page_uri = {
    .uri = "/mqtt", .method = HTTP_GET, .handler = mqtt_page_get_handler};



/* Step 1 of the setup wizard. Free-to-set while no password exists yet
   (check_ota_auth's open-until-set rule); once one's chosen, changing it
   needs the current one, same as /api/ota itself. Registered on both
   webservers since the wizard runs in both provisioning-AP and
   already-connected mode (re-running it to rotate the password later). */
static esp_err_t api_setup_password_post_handler(httpd_req_t *req)
{
    char body[128] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    char pw[OTA_PASSWORD_MAX_LEN + 1] = {0};
    if (!json_get_str(body, "password", pw, sizeof(pw)) || strlen(pw) < OTA_PASSWORD_MIN_LEN) {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(
            req, "{\"ok\":false,\"error\":\"password must be at least 8 characters\"}",
            HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    if (ota_password_is_set() && !check_ota_auth(req)) {
        return ESP_OK; /* check_ota_auth already sent the 401 */
    }

    ota_password_store(pw);
    ESP_LOGI(TAG_WIFI, "OTA update password set via setup wizard Step 1");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static const httpd_uri_t api_setup_password_uri = {
    .uri = "/api/setup-password", .method = HTTP_POST, .handler = api_setup_password_post_handler};

/* Forward declarations */
static void mqtt_publish_discovery(void);
static void mqtt_unpublish_discovery(void);
static void mqtt_app_restart(void);

/* ===================== Object Scanner & Browser APIs ===================== */

typedef struct {
    uint16_t type;
    uint32_t instance;
    char name[64]; /* was 32 - confirmed live truncating "Fan 1 Supply Air Temperature Se..." */
} scanned_object_t;

#define MAX_SCANNED_OBJECTS 480
static scanned_object_t ScannedObjects[MAX_SCANNED_OBJECTS];
static size_t ScannedObjectCount = 0;

typedef enum {
    SCAN_STATE_IDLE = 0,
    SCAN_STATE_RUNNING,
    SCAN_STATE_COMPLETE,
    SCAN_STATE_ERROR
} scan_state_t;

static scan_state_t ScanState = SCAN_STATE_IDLE;
static uint32_t ScanCurrent = 0;
static uint32_t ScanTotal = 0;
static uint8_t ScanPercent = 0;
static char ScanErrorMsg[64] = {0};
static TaskHandle_t ScanTaskHandle = NULL;

static void object_scan_task(void *arg)
{
    (void)arg;
    ScanState = SCAN_STATE_RUNNING;
    ScanCurrent = 0;
    ScanPercent = 0;
    ScanTotal = 0;
    ScanErrorMsg[0] = '\0';

    if (!BacnetReady) {
        ScanState = SCAN_STATE_ERROR;
        snprintf(ScanErrorMsg, sizeof(ScanErrorMsg), "BACnet datalink not ready");
        ScanTaskHandle = NULL;
        vTaskDelete(NULL);
        return;
    }

    uint32_t total = 0;
    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) == pdTRUE) {
        BACNET_APPLICATION_DATA_VALUE v = {0};
        bool ok = bacnet_read_locked_idx(OBJECT_DEVICE, TargetDeviceInstance, PROP_OBJECT_LIST, 0, &v);
        if (ok && v.tag == BACNET_APPLICATION_TAG_UNSIGNED_INT) {
            total = v.type.Unsigned_Int;
        }
        xSemaphoreGive(BacnetMutex);
    }

    if (total == 0 || total > 2000) {
        total = 430;
    }
    ScanTotal = total;

    size_t found = 0;
    for (uint32_t idx = 1; idx <= total && found < MAX_SCANNED_OBJECTS; idx++) {
        uint16_t obj_type = 0;
        uint32_t obj_inst = 0;
        bool got_id = false;

        if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(5000)) == pdTRUE) {
            BACNET_APPLICATION_DATA_VALUE v = {0};
            if (bacnet_read_locked_idx(OBJECT_DEVICE, TargetDeviceInstance, PROP_OBJECT_LIST, idx, &v)) {
                if (v.tag == BACNET_APPLICATION_TAG_OBJECT_ID) {
                    obj_type = (uint16_t)v.type.Object_Id.type;
                    obj_inst = v.type.Object_Id.instance;
                    got_id = true;
                }
            }
            xSemaphoreGive(BacnetMutex);
        }

        if (got_id) {
            char name_buf[64] = {0};
            if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(5000)) == pdTRUE) {
                BACNET_APPLICATION_DATA_VALUE vname = {0};
                if (bacnet_read_locked((BACNET_OBJECT_TYPE)obj_type, obj_inst, PROP_OBJECT_NAME, &vname)) {
                    if (vname.tag == BACNET_APPLICATION_TAG_CHARACTER_STRING) {
                        copy_character_string(name_buf, sizeof(name_buf), (const BACNET_CHARACTER_STRING *)&vname.type.Character_String);
                    }
                }
                xSemaphoreGive(BacnetMutex);
            }

            ScannedObjects[found].type = obj_type;
            ScannedObjects[found].instance = obj_inst;
            if (strlen(name_buf) > 0) {
                strlcpy(ScannedObjects[found].name, name_buf, sizeof(ScannedObjects[found].name));
            } else {
                snprintf(ScannedObjects[found].name, sizeof(ScannedObjects[found].name), "%s #%u",
                         bactext_object_type_name_default(obj_type, "obj"), (unsigned)obj_inst);
            }
            found++;
            ScannedObjectCount = found;
        }

        ScanCurrent = idx;
        ScanPercent = (uint8_t)((idx * 100) / total);
        vTaskDelay(pdMS_TO_TICKS(50));
    }

    ScanState = SCAN_STATE_COMPLETE;
    ScanTaskHandle = NULL;
    vTaskDelete(NULL);
}

static esp_err_t api_objects_scan_start_handler(httpd_req_t *req)
{
    if (ScanState == SCAN_STATE_RUNNING) {
        httpd_resp_set_type(req, "application/json");
        httpd_resp_send(req, "{\"ok\":true,\"status\":\"already_running\"}", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    ScanState = SCAN_STATE_RUNNING;
    ScanCurrent = 0;
    ScanPercent = 0;
    ScannedObjectCount = 0;
    spawn_task(object_scan_task, "obj_scan", 8192, NULL, 5, &ScanTaskHandle);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true,\"status\":\"started\"}", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_objects_scan_start_uri = {
    .uri = "/api/objects/scan-start", .method = HTTP_POST, .handler = api_objects_scan_start_handler};

static esp_err_t api_objects_scan_status_handler(httpd_req_t *req)
{
    const char *st_str = "idle";
    if (ScanState == SCAN_STATE_RUNNING) st_str = "scanning";
    else if (ScanState == SCAN_STATE_COMPLETE) st_str = "complete";
    else if (ScanState == SCAN_STATE_ERROR) st_str = "error";

    char buf[256];
    snprintf(buf, sizeof(buf),
             "{\"ok\":true,\"state\":\"%s\",\"current\":%u,\"total\":%u,\"percent\":%u,\"count\":%u,\"error\":\"%s\"}",
             st_str, (unsigned)ScanCurrent, (unsigned)ScanTotal, (unsigned)ScanPercent,
             (unsigned)ScannedObjectCount, ScanErrorMsg);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_objects_scan_status_uri = {
    .uri = "/api/objects/scan-status", .method = HTTP_GET, .handler = api_objects_scan_status_handler};

static esp_err_t api_objects_get_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "application/json");
    char chunk[512];
    int len = snprintf(chunk, sizeof(chunk), "{\"ok\":true,\"count\":%u,\"objects\":[", (unsigned)ScannedObjectCount);
    httpd_resp_send_chunk(req, chunk, len);

    /* 64-char name * worst-case 2x escaping (every char is '"' or '\\') + slack */
    char name_escaped[129];
    for (size_t i = 0; i < ScannedObjectCount; i++) {
        json_escape(name_escaped, ScannedObjects[i].name, sizeof(name_escaped));
        const char *tname = bactext_object_type_name_default(ScannedObjects[i].type, "unknown");
        bool writable = (ScannedObjects[i].type == OBJECT_ANALOG_VALUE ||
                          ScannedObjects[i].type == OBJECT_BINARY_VALUE ||
                          ScannedObjects[i].type == OBJECT_MULTI_STATE_VALUE ||
                          ScannedObjects[i].type == OBJECT_ANALOG_OUTPUT ||
                          ScannedObjects[i].type == OBJECT_BINARY_OUTPUT ||
                          ScannedObjects[i].type == OBJECT_MULTI_STATE_OUTPUT);
        len = snprintf(chunk, sizeof(chunk), "%s{\"type\":\"%s\",\"instance\":%u,\"name\":\"%s\",\"writable\":%s}",
                       i == 0 ? "" : ",", tname, (unsigned)ScannedObjects[i].instance, name_escaped,
                       writable ? "true" : "false");
        httpd_resp_send_chunk(req, chunk, len);
    }
    httpd_resp_send_chunk(req, "]}", 2);
    httpd_resp_send_chunk(req, NULL, 0);
    return ESP_OK;
}
static const httpd_uri_t api_objects_uri = {
    .uri = "/api/objects", .method = HTTP_GET, .handler = api_objects_get_handler};

static inline bool is_commandable_type(BACNET_OBJECT_TYPE type)
{
    /* On Delta DAC controllers, only Analog Output, Binary Output, and Multi-State Output support Priority_Array */
    return (type == OBJECT_ANALOG_OUTPUT ||
            type == OBJECT_BINARY_OUTPUT ||
            type == OBJECT_MULTI_STATE_OUTPUT);
}

static esp_err_t api_objects_inspect_handler(httpd_req_t *req)
{
    char query[128] = {0};
    if (httpd_req_get_url_query_str(req, query, sizeof(query)) != ESP_OK) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"Missing query\"}");
        return ESP_OK;
    }
    char type_str[32] = {0}, inst_str[16] = {0};
    httpd_query_key_value(query, "type", type_str, sizeof(type_str));
    httpd_query_key_value(query, "instance", inst_str, sizeof(inst_str));

    BACNET_OBJECT_TYPE otype;
    if (!parse_object_type(type_str, &otype)) {
        send_bad_request(req, "{\"ok\":false,\"error\":\"Invalid type\"}");
        return ESP_OK;
    }
    uint32_t oinst = (uint32_t)strtoul(inst_str, NULL, 10);

    char pv_json[64] = "null";
    char desc_json[128] = "null";
    char units_json[64] = "null";
    char flags_json[64] = "null";
    char pri_json[512] = "null";

    if (!BacnetReady) {
        httpd_resp_set_type(req, "application/json");
        httpd_resp_send(req, "{\"ok\":false,\"error\":\"BACnet not ready\"}", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(4000)) != pdTRUE) {
        httpd_resp_set_type(req, "application/json");
        httpd_resp_send(req, "{\"ok\":false,\"error\":\"BACnet mutex busy\"}", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    /* 1. Present_Value */
    BACNET_APPLICATION_DATA_VALUE v = {0};
    if (bacnet_read_locked(otype, oinst, PROP_PRESENT_VALUE, &v)) {
        append_value_json(pv_json, sizeof(pv_json), &v);
    }

    /* 2. Units - only for Analog objects */
    if (otype == OBJECT_ANALOG_INPUT || otype == OBJECT_ANALOG_OUTPUT || otype == OBJECT_ANALOG_VALUE) {
        vTaskDelay(pdMS_TO_TICKS(10));
        BACNET_APPLICATION_DATA_VALUE vunits = {0};
        if (bacnet_read_locked(otype, oinst, PROP_UNITS, &vunits) &&
            vunits.tag == BACNET_APPLICATION_TAG_ENUMERATED) {
            const char *uname = bactext_engineering_unit_name_default(vunits.type.Enumerated, "");
            snprintf(units_json, sizeof(units_json), "\"%s\"", uname);
        }
    }

    /* 3. Priority Array - only for actual commandable outputs */
    if (is_commandable_type(otype)) {
        vTaskDelay(pdMS_TO_TICKS(10));
        static uint8_t raw_copy[MAX_APDU];
        int raw_len = 0;
        if (bacnet_read_locked_idx(otype, oinst, PROP_PRIORITY_ARRAY, BACNET_ARRAY_ALL, &v)) {
            raw_len = Last_Read_Raw_Len;
            if (raw_len > 0 && raw_len <= (int)sizeof(raw_copy)) {
                memcpy(raw_copy, Last_Read_Raw, raw_len);
                pri_json[0] = '[';
                pri_json[1] = '\0';
                size_t pos = 1;
                int offset = 0;
                int slot = 0;
                while (offset < raw_len && slot < 16 && pos < sizeof(pri_json) - 32) {
                    BACNET_APPLICATION_DATA_VALUE elem = {0};
                    int consumed = bacapp_decode_application_data(
                        raw_copy + offset, (uint32_t)(raw_len - offset), &elem);
                    if (consumed <= 0) break;
                    offset += consumed;
                    if (slot > 0 && pos < sizeof(pri_json) - 32) {
                        pri_json[pos++] = ',';
                        pri_json[pos] = '\0';
                    }
                    if (pos < sizeof(pri_json) - 32) {
                        pos += append_value_json(pri_json + pos, sizeof(pri_json) - pos, &elem);
                    }
                    slot++;
                }
                if (pos < sizeof(pri_json) - 2) {
                    pri_json[pos++] = ']';
                    pri_json[pos] = '\0';
                }
            }
        }
    }

    xSemaphoreGive(BacnetMutex);

    char buf[1536];
    snprintf(buf, sizeof(buf),
             "{\"ok\":true,\"type\":\"%s\",\"instance\":%u,\"present_value\":%s,\"description\":%s,\"units\":%s,\"status_flags\":%s,\"priority_array\":%s}",
             type_str, (unsigned)oinst, pv_json, desc_json, units_json, flags_json, pri_json);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_objects_inspect_uri = {
    .uri = "/api/objects/inspect", .method = HTTP_GET, .handler = api_objects_inspect_handler};

/* Sized to comfortably batch-read a whole object-browser section (~50 points)
 * in one request rather than the old 32-point cap. points_str must be large
 * enough to hold the decoded points list before it's tokenized - the encoded
 * form body passes through form_extract()'s 3200-byte staging buffer, which
 * bounds how large this can safely go. */
#define BATCH_MAX_POINTS 80
#define BATCH_POINTS_BUF 2048
#define BATCH_RESP_BUF 8192

static esp_err_t api_objects_batch_handler(httpd_req_t *req)
{
    char body[BATCH_POINTS_BUF] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    if (!BacnetReady) {
        httpd_resp_set_type(req, "application/json");
        httpd_resp_send(req, "{\"ok\":false,\"error\":\"BACnet not ready\"}", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    char *buf = malloc(BATCH_RESP_BUF);
    if (!buf) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    size_t pos = snprintf(buf, BATCH_RESP_BUF, "{\"ok\":true,\"values\":[");
    const size_t safety_margin = 200; /* room for one more entry + "],\"truncated\":true}" */

    char points_str[BATCH_POINTS_BUF] = {0};
    if (!json_get_str(body, "points", points_str, sizeof(points_str))) {
        form_extract(body, "points", points_str, sizeof(points_str));
    }

    char *saveptr = NULL;
    char *token = strtok_r(points_str, ",", &saveptr);
    int count = 0;
    bool truncated = false;

    if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(6000)) == pdTRUE) {
        while (token) {
            if (count >= BATCH_MAX_POINTS || pos >= BATCH_RESP_BUF - safety_margin) {
                truncated = true;
                break;
            }
            while (*token == ' ' || *token == '\"' || *token == '[' || *token == ']') token++;
            char *colon = strchr(token, ':');
            if (colon) {
                *colon = '\0';
                char *tstr = token;
                char *istr = colon + 1;
                char *endquote = strpbrk(istr, " \"]");
                if (endquote) *endquote = '\0';

                BACNET_OBJECT_TYPE otype;
                if (parse_object_type(tstr, &otype)) {
                    uint32_t oinst = (uint32_t)strtoul(istr, NULL, 10);
                    BACNET_APPLICATION_DATA_VALUE v = {0};
                    bool ok = bacnet_read_locked(otype, oinst, PROP_PRESENT_VALUE, &v);

                    char val_json[48] = "null";
                    if (ok) {
                        append_value_json(val_json, sizeof(val_json), &v);
                    }

                    if (count > 0) {
                        buf[pos++] = ',';
                        buf[pos] = '\0';
                    }
                    pos += snprintf(buf + pos, BATCH_RESP_BUF - pos,
                                    "{\"type\":\"%s\",\"instance\":%u,\"valid\":%s,\"value\":%s}",
                                    tstr, (unsigned)oinst, ok ? "true" : "false", val_json);
                    count++;
                }
            }
            token = strtok_r(NULL, ",", &saveptr);
            vTaskDelay(pdMS_TO_TICKS(10));
        }
        xSemaphoreGive(BacnetMutex);
    }

    if (pos < BATCH_RESP_BUF - 40) {
        pos += snprintf(buf + pos, BATCH_RESP_BUF - pos, "],\"truncated\":%s}", truncated ? "true" : "false");
    } else {
        snprintf(buf + pos, BATCH_RESP_BUF - pos, "]}");
    }

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    free(buf);
    return ESP_OK;
}
static const httpd_uri_t api_objects_batch_uri = {
    .uri = "/api/objects/batch", .method = HTTP_POST, .handler = api_objects_batch_handler};

/* ===================== Custom MQTT Configuration ===================== */

#define NVS_MQTT_NAMESPACE "nvs_mqtt"
#define NVS_CUSTOM_MQTT_NAMESPACE "nvs_custmq"
#define NVS_APP_NAMESPACE "app_cfg"
#define MAX_CUSTOM_MQTT 32

static esp_mqtt_client_handle_t MqttClient = NULL;
static volatile bool MqttConnected = false;

static char MqttBrokerHost[64] = "";
static uint16_t MqttBrokerPort = 1883;
static char MqttBrokerUser[32] = "";
static char MqttBrokerPass[64] = "";
static char HaDeviceId[32] = "";
static char HaDeviceName[48] = "";
static char MqttTopicBase[64] = "";
static bool HaDiscoveryEnabled = true;
static bool HaHealthDiscoveryEnabled = true;
/* mqtt_publish_discovery() runs at MQTT_EVENT_CONNECTED, which typically
   fires before the BACnet task finishes binding - at that point fan-count
   discovery falls back to publishing every possible fan channel. Once
   BacnetReady flips true, mqtt_state_task republishes discovery so the fan
   list gets trimmed to the real channel count. */

typedef struct {
    char type_str[24];
    uint16_t obj_type;
    uint32_t instance;
    char name[32];
    char component[16];
    bool enabled;
} custom_mqtt_point_t;

static custom_mqtt_point_t CustomMqttPoints[MAX_CUSTOM_MQTT];
static size_t CustomMqttCount = 0;

static void mqtt_config_load(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_MQTT_NAMESPACE, NVS_READONLY, &handle) == ESP_OK) {
        size_t host_len = sizeof(MqttBrokerHost);
        nvs_get_str(handle, "host", MqttBrokerHost, &host_len);
        nvs_get_u16(handle, "port", &MqttBrokerPort);
        size_t user_len = sizeof(MqttBrokerUser);
        nvs_get_str(handle, "user", MqttBrokerUser, &user_len);
        size_t pass_len = sizeof(MqttBrokerPass);
        nvs_get_str(handle, "pass", MqttBrokerPass, &pass_len);
        size_t base_len = sizeof(MqttTopicBase);
        nvs_get_str(handle, "base_top", MqttTopicBase, &base_len);
        size_t devn_len = sizeof(HaDeviceName);
        nvs_get_str(handle, "dev_name", HaDeviceName, &devn_len);
        size_t devid_len = sizeof(HaDeviceId);
        nvs_get_str(handle, "dev_id", HaDeviceId, &devid_len);
        uint8_t disc = 1;
        if (nvs_get_u8(handle, "disc_en", &disc) == ESP_OK) {
            HaDiscoveryEnabled = (disc != 0);
        }
        uint8_t hdisc = 1;
        if (nvs_get_u8(handle, "hlth_en", &hdisc) == ESP_OK) {
            HaHealthDiscoveryEnabled = (hdisc != 0);
        }
        nvs_close(handle);
    }

    if (HaDeviceName[0] == '\0') {
        if (TargetDeviceNameValid && strlen(TargetDeviceName) > 0) {
            snprintf(HaDeviceName, sizeof(HaDeviceName), "BACnet Bridge (%.31s)", TargetDeviceName);
        } else {
            strlcpy(HaDeviceName, "BACnet Bridge", sizeof(HaDeviceName));
        }
    }
    if (HaDeviceId[0] == '\0') {
        if (TargetDeviceInstance > 0) {
            snprintf(HaDeviceId, sizeof(HaDeviceId), "bacnet_%u", (unsigned)TargetDeviceInstance);
        } else {
            strlcpy(HaDeviceId, "bacnet_bridge", sizeof(HaDeviceId));
        }
    }
    if (MqttTopicBase[0] == '\0') {
        if (TargetDeviceInstance > 0) {
            snprintf(MqttTopicBase, sizeof(MqttTopicBase), "esp32bacnet/%u", (unsigned)TargetDeviceInstance);
        } else {
            strlcpy(MqttTopicBase, "esp32bacnet", sizeof(MqttTopicBase));
        }
    }
}

static void mqtt_config_save(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_MQTT_NAMESPACE, NVS_READWRITE, &handle) != ESP_OK) {
        return;
    }
    nvs_set_str(handle, "host", MqttBrokerHost);
    nvs_set_u16(handle, "port", MqttBrokerPort);
    nvs_set_str(handle, "user", MqttBrokerUser);
    nvs_set_str(handle, "pass", MqttBrokerPass);
    nvs_set_str(handle, "base_top", MqttTopicBase);
    nvs_set_str(handle, "dev_name", HaDeviceName);
    nvs_set_str(handle, "dev_id", HaDeviceId);
    nvs_set_u8(handle, "disc_en", HaDiscoveryEnabled ? 1 : 0);
    nvs_set_u8(handle, "hlth_en", HaHealthDiscoveryEnabled ? 1 : 0);
    nvs_commit(handle);
    nvs_close(handle);
}

static void custom_mqtt_load(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_CUSTOM_MQTT_NAMESPACE, NVS_READONLY, &handle) != ESP_OK) {
        return;
    }
    uint8_t count = 0;
    if (nvs_get_u8(handle, "count", &count) == ESP_OK && count <= MAX_CUSTOM_MQTT) {
        CustomMqttCount = count;
        for (size_t i = 0; i < CustomMqttCount; i++) {
            char key[32];
            snprintf(key, sizeof(key), "p%u_tstr", (unsigned)i);
            size_t slen = sizeof(CustomMqttPoints[i].type_str);
            nvs_get_str(handle, key, CustomMqttPoints[i].type_str, &slen);

            snprintf(key, sizeof(key), "p%u_type", (unsigned)i);
            nvs_get_u16(handle, key, &CustomMqttPoints[i].obj_type);

            snprintf(key, sizeof(key), "p%u_inst", (unsigned)i);
            nvs_get_u32(handle, key, &CustomMqttPoints[i].instance);

            snprintf(key, sizeof(key), "p%u_name", (unsigned)i);
            size_t nlen = sizeof(CustomMqttPoints[i].name);
            nvs_get_str(handle, key, CustomMqttPoints[i].name, &nlen);

            snprintf(key, sizeof(key), "p%u_comp", (unsigned)i);
            size_t clen = sizeof(CustomMqttPoints[i].component);
            nvs_get_str(handle, key, CustomMqttPoints[i].component, &clen);

            snprintf(key, sizeof(key), "p%u_en", (unsigned)i);
            uint8_t en = 0;
            nvs_get_u8(handle, key, &en);
            CustomMqttPoints[i].enabled = (en != 0);
        }
    }
    nvs_close(handle);
}

static void custom_mqtt_save(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_CUSTOM_MQTT_NAMESPACE, NVS_READWRITE, &handle) != ESP_OK) {
        return;
    }
    nvs_set_u8(handle, "count", (uint8_t)CustomMqttCount);
    for (size_t i = 0; i < CustomMqttCount; i++) {
        char key[32];
        snprintf(key, sizeof(key), "p%u_tstr", (unsigned)i);
        nvs_set_str(handle, key, CustomMqttPoints[i].type_str);

        snprintf(key, sizeof(key), "p%u_type", (unsigned)i);
        nvs_set_u16(handle, key, CustomMqttPoints[i].obj_type);

        snprintf(key, sizeof(key), "p%u_inst", (unsigned)i);
        nvs_set_u32(handle, key, CustomMqttPoints[i].instance);

        snprintf(key, sizeof(key), "p%u_name", (unsigned)i);
        nvs_set_str(handle, key, CustomMqttPoints[i].name);

        snprintf(key, sizeof(key), "p%u_comp", (unsigned)i);
        nvs_set_str(handle, key, CustomMqttPoints[i].component);

        snprintf(key, sizeof(key), "p%u_en", (unsigned)i);
        nvs_set_u8(handle, key, CustomMqttPoints[i].enabled ? 1 : 0);
    }
    nvs_commit(handle);
    nvs_close(handle);
}

static void parse_custom_mqtt_from_json(const char *body)
{
    const char *p = strstr(body, "\"custom_mqtt\"");
    if (!p) p = strstr(body, "\"points\"");
    if (p) {
        const char *arr = strchr(p, '[');
        if (arr) {
            const char *obj = arr;
            size_t idx = 0;
            while ((obj = strchr(obj, '{')) != NULL && idx < MAX_CUSTOM_MQTT) {
                const char *end_obj = strchr(obj, '}');
                if (!end_obj) break;
                char item[256];
                size_t len = (size_t)(end_obj - obj + 1);
                if (len >= sizeof(item)) len = sizeof(item) - 1;
                memcpy(item, obj, len);
                item[len] = '\0';

                char tstr[24] = {0}, name[32] = {0}, comp[16] = {0};
                int inst = -1;
                bool en = true;

                json_get_str(item, "type", tstr, sizeof(tstr));
                json_get_int(item, "instance", &inst);
                json_get_str(item, "name", name, sizeof(name));
                json_get_str(item, "component", comp, sizeof(comp));
                json_get_bool(item, "enabled", &en);

                if (strlen(tstr) > 0 && inst >= 0 && strlen(name) > 0) {
                    strlcpy(CustomMqttPoints[idx].type_str, tstr, sizeof(CustomMqttPoints[idx].type_str));
                    BACNET_OBJECT_TYPE obj_type;
                    if (parse_object_type(tstr, &obj_type)) {
                        CustomMqttPoints[idx].obj_type = (uint16_t)obj_type;
                    }
                    CustomMqttPoints[idx].instance = (uint32_t)inst;
                    strlcpy(CustomMqttPoints[idx].name, name, sizeof(CustomMqttPoints[idx].name));
                    strlcpy(CustomMqttPoints[idx].component, comp[0] ? comp : "sensor", sizeof(CustomMqttPoints[idx].component));
                    CustomMqttPoints[idx].enabled = en;
                    idx++;
                }
                obj = end_obj + 1;
            }
            CustomMqttCount = idx;
        }
    }
}

#define NVS_KEY_BOOST_TIMEOUT "boost_min"
#define NVS_KEY_BOOST_EXTERNAL "boost_ext"

static void app_config_load(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_APP_NAMESPACE, NVS_READONLY, &handle) != ESP_OK) {
        return;
    }
    uint16_t minutes;
    if (nvs_get_u16(handle, NVS_KEY_BOOST_TIMEOUT, &minutes) == ESP_OK &&
        minutes <= BOOST_TIMEOUT_MAX_MIN) {
        BoostTimeoutMinutes = minutes;
    }
    uint8_t external;
    if (nvs_get_u8(handle, NVS_KEY_BOOST_EXTERNAL, &external) == ESP_OK) {
        BoostRevertExternal = external != 0;
    }
    nvs_close(handle);
}

static void app_config_save(void)
{
    nvs_handle_t handle;
    if (nvs_open(NVS_APP_NAMESPACE, NVS_READWRITE, &handle) != ESP_OK) {
        return;
    }
    nvs_set_u16(handle, NVS_KEY_BOOST_TIMEOUT, BoostTimeoutMinutes);
    nvs_set_u8(handle, NVS_KEY_BOOST_EXTERNAL, BoostRevertExternal ? 1 : 0);
    nvs_commit(handle);
    nvs_close(handle);
}

static esp_err_t api_objects_custom_mqtt_get_handler(httpd_req_t *req)
{
    char buf[2048];
    size_t off = snprintf(buf, sizeof(buf), "{\"ok\":true,\"points\":[");
    for (size_t i = 0; i < CustomMqttCount && off < sizeof(buf) - 64; i++) {
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%s{\"type\":\"%s\",\"instance\":%u,\"name\":\"%s\",\"component\":\"%s\",\"enabled\":%s}",
                        i == 0 ? "" : ",", CustomMqttPoints[i].type_str,
                        (unsigned)CustomMqttPoints[i].instance, CustomMqttPoints[i].name,
                        CustomMqttPoints[i].component, CustomMqttPoints[i].enabled ? "true" : "false");
    }
    snprintf(buf + off, sizeof(buf) - off, "]}");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_objects_custom_mqtt_get_uri = {
    .uri = "/api/objects/custom-mqtt", .method = HTTP_GET, .handler = api_objects_custom_mqtt_get_handler};

static esp_err_t api_objects_custom_mqtt_post_handler(httpd_req_t *req)
{
    char body[512] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    char action[16] = {0}, tstr[24] = {0}, name[32] = {0}, comp[16] = {0};
    int inst = 0;
    bool enabled = true;

    if (body[0] == '{') {
        json_get_str(body, "action", action, sizeof(action));
        json_get_str(body, "type", tstr, sizeof(tstr));
        json_get_int(body, "instance", &inst);
        json_get_str(body, "name", name, sizeof(name));
        json_get_str(body, "component", comp, sizeof(comp));
        json_get_bool(body, "enabled", &enabled);
    } else {
        httpd_query_key_value(body, "action", action, sizeof(action));
        httpd_query_key_value(body, "type", tstr, sizeof(tstr));
        char inst_str[16] = {0};
        httpd_query_key_value(body, "instance", inst_str, sizeof(inst_str));
        if (inst_str[0]) inst = (int)strtol(inst_str, NULL, 10);
        httpd_query_key_value(body, "name", name, sizeof(name));
        httpd_query_key_value(body, "component", comp, sizeof(comp));
    }

    if (comp[0] == '\0') {
        strlcpy(comp, "sensor", sizeof(comp));
    }

    BACNET_OBJECT_TYPE otype;
    parse_object_type(tstr, &otype);

    if (strcmp(action, "remove") == 0 || !enabled) {
        for (size_t i = 0; i < CustomMqttCount; i++) {
            if (CustomMqttPoints[i].obj_type == (uint16_t)otype && CustomMqttPoints[i].instance == (uint32_t)inst) {
                if (MqttClient && MqttConnected) {
                    char unique_id[64];
                    snprintf(unique_id, sizeof(unique_id), "%s_cust_%u_%u",
                             HaDeviceId, CustomMqttPoints[i].obj_type, (unsigned)CustomMqttPoints[i].instance);
                    char disc_top[128];
                    snprintf(disc_top, sizeof(disc_top), "homeassistant/%s/%s/config",
                             CustomMqttPoints[i].component, unique_id);
                    esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
                }
                for (size_t j = i; j + 1 < CustomMqttCount; j++) {
                    CustomMqttPoints[j] = CustomMqttPoints[j + 1];
                }
                CustomMqttCount--;
                break;
            }
        }
    } else {
        bool found = false;
        for (size_t i = 0; i < CustomMqttCount; i++) {
            if (CustomMqttPoints[i].obj_type == (uint16_t)otype && CustomMqttPoints[i].instance == (uint32_t)inst) {
                if (strlen(name) > 0) strlcpy(CustomMqttPoints[i].name, name, sizeof(CustomMqttPoints[i].name));
                strlcpy(CustomMqttPoints[i].component, comp, sizeof(CustomMqttPoints[i].component));
                CustomMqttPoints[i].enabled = true;
                found = true;
                break;
            }
        }
        if (!found && CustomMqttCount < MAX_CUSTOM_MQTT) {
            strlcpy(CustomMqttPoints[CustomMqttCount].type_str, tstr, sizeof(CustomMqttPoints[CustomMqttCount].type_str));
            CustomMqttPoints[CustomMqttCount].obj_type = (uint16_t)otype;
            CustomMqttPoints[CustomMqttCount].instance = (uint32_t)inst;
            if (strlen(name) > 0) {
                strlcpy(CustomMqttPoints[CustomMqttCount].name, name, sizeof(CustomMqttPoints[CustomMqttCount].name));
            } else {
                snprintf(CustomMqttPoints[CustomMqttCount].name, sizeof(CustomMqttPoints[CustomMqttCount].name),
                         "%s #%u", tstr, (unsigned)inst);
            }
            strlcpy(CustomMqttPoints[CustomMqttCount].component, comp, sizeof(CustomMqttPoints[CustomMqttCount].component));
            CustomMqttPoints[CustomMqttCount].enabled = true;
            CustomMqttCount++;
        }
    }

    custom_mqtt_save();
    mqtt_publish_discovery();

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_objects_custom_mqtt_post_uri = {
    .uri = "/api/objects/custom-mqtt", .method = HTTP_POST, .handler = api_objects_custom_mqtt_post_handler};

/* ===================== BACnet Discovery & Strategy Analysis APIs ===================== */

static esp_err_t api_bacnet_discover_handler(httpd_req_t *req)
{
    char body[256] = {0};
    recv_body(req, body, sizeof(body));

    char req_ip[16] = {0};
    int req_port = 0;
    int req_dev_id = 0;
    json_get_str(body, "ip", req_ip, sizeof(req_ip));
    json_get_int(body, "port", &req_port);
    json_get_int(body, "device_id", &req_dev_id);

    if (EthConnected && xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(8000)) == pdTRUE) {
        DiscoveredDeviceCount = 0;

        if (strlen(req_ip) > 0) {
            /* Targeted unicast probe */
            BACNET_ADDRESS dest = {0};
            uint16_t nport = req_port > 0 ? (uint16_t)req_port : (TargetPort ? TargetPort : 47808);
            unsigned a, b, c, d;
            if (sscanf(req_ip, "%u.%u.%u.%u", &a, &b, &c, &d) == 4) {
                dest.mac[0] = (uint8_t)a;
                dest.mac[1] = (uint8_t)b;
                dest.mac[2] = (uint8_t)c;
                dest.mac[3] = (uint8_t)d;
                memcpy(&dest.mac[4], &nport, 2);
                dest.mac_len = 6;
                Send_WhoIs_To_Network(&dest, -1, -1);
            }
        } else {
            /* Full network discovery:
               1. Global broadcast (255.255.255.255)
               2. Subnet broadcast (10.0.255.255 & 10.0.3.255)
               3. Fast unicast sweep across 10.0.3.1 - 10.0.3.32 (and current TargetIp)
            */
            Send_WhoIs_Global(-1, -1);

            uint16_t nport = TargetPort ? TargetPort : 47808;
            BACNET_ADDRESS bcast = {0};
            bcast.mac[0] = 10; bcast.mac[1] = 0; bcast.mac[2] = 255; bcast.mac[3] = 255;
            memcpy(&bcast.mac[4], &nport, 2);
            bcast.mac_len = 6;
            Send_WhoIs_To_Network(&bcast, -1, -1);

            bcast.mac[2] = 3;
            Send_WhoIs_To_Network(&bcast, -1, -1);

            /* Sweep 10.0.3.1 to 10.0.3.32 with 15ms pacing between probes */
            for (unsigned oct = 1; oct <= 32; oct++) {
                BACNET_ADDRESS udest = {0};
                udest.mac[0] = 10; udest.mac[1] = 0; udest.mac[2] = 3; udest.mac[3] = (uint8_t)oct;
                memcpy(&udest.mac[4], &nport, 2);
                udest.mac_len = 6;
                Send_WhoIs_To_Network(&udest, -1, -1);
                vTaskDelay(pdMS_TO_TICKS(15));
            }
            /* Also probe current TargetIp if not in 10.0.3.1-32 */
            unsigned a, b, c, d;
            if (sscanf(TargetIp, "%u.%u.%u.%u", &a, &b, &c, &d) == 4 && (c != 3 || d > 32)) {
                BACNET_ADDRESS udest = {0};
                udest.mac[0] = (uint8_t)a; udest.mac[1] = (uint8_t)b; udest.mac[2] = (uint8_t)c; udest.mac[3] = (uint8_t)d;
                memcpy(&udest.mac[4], &nport, 2);
                udest.mac_len = 6;
                Send_WhoIs_To_Network(&udest, -1, -1);
            }
        }

        /* Pump receive loop for 2.0 seconds to capture I-Am replies */
        int64_t deadline = esp_timer_get_time() + 2000000;
        while (esp_timer_get_time() < deadline) {
            uint8_t pdu[BIP_MPDU_MAX] = {0};
            BACNET_ADDRESS src = {0};
            uint16_t pdu_len = bip_receive(&src, pdu, sizeof(pdu), 20);
            if (pdu_len > 0) {
                npdu_handler(&src, pdu, pdu_len);
            }
            vTaskDelay(pdMS_TO_TICKS(15));
        }

        /* For each discovered device, query device name, vendor name, model name.
           bacnet_read_locked() assumes the caller already holds BacnetMutex - true
           here, it was taken above - so these must NOT go through read_real_property()
           or any other helper that re-takes it; this is a non-recursive mutex and
           doing so previously stalled 6s per device for a value that was discarded
           anyway. */
        for (size_t i = 0; i < DiscoveredDeviceCount; i++) {
            BACNET_APPLICATION_DATA_VALUE val = {0};
            if (bacnet_read_locked(OBJECT_DEVICE, DiscoveredDevices[i].device_id, PROP_OBJECT_NAME, &val) &&
                val.tag == BACNET_APPLICATION_TAG_CHARACTER_STRING) {
                copy_character_string(DiscoveredDevices[i].name, sizeof(DiscoveredDevices[i].name),
                                      (const BACNET_CHARACTER_STRING *)&val.type.Character_String);
            }
            if (bacnet_read_locked(OBJECT_DEVICE, DiscoveredDevices[i].device_id, PROP_VENDOR_NAME, &val) &&
                val.tag == BACNET_APPLICATION_TAG_CHARACTER_STRING) {
                copy_character_string(DiscoveredDevices[i].vendor_name, sizeof(DiscoveredDevices[i].vendor_name),
                                      (const BACNET_CHARACTER_STRING *)&val.type.Character_String);
            }
            if (bacnet_read_locked(OBJECT_DEVICE, DiscoveredDevices[i].device_id, PROP_MODEL_NAME, &val) &&
                val.tag == BACNET_APPLICATION_TAG_CHARACTER_STRING) {
                copy_character_string(DiscoveredDevices[i].model_name, sizeof(DiscoveredDevices[i].model_name),
                                      (const BACNET_CHARACTER_STRING *)&val.type.Character_String);
            }
        }

        /* Fallback: If no device responded to Who-Is, attempt direct ReadProperty on TargetDeviceInstance */
        if (DiscoveredDeviceCount == 0) {
            bind_target_device();
            BACNET_APPLICATION_DATA_VALUE v = {0};
            char dname[48] = {0}, vname[48] = {0}, mname[48] = {0};
            bool direct_ok = false;
            if (bacnet_read_locked(OBJECT_DEVICE, TargetDeviceInstance, PROP_OBJECT_NAME, &v) &&
                v.tag == BACNET_APPLICATION_TAG_CHARACTER_STRING) {
                copy_character_string(dname, sizeof(dname), (const BACNET_CHARACTER_STRING *)&v.type.Character_String);
                direct_ok = true;
            }
            if (bacnet_read_locked(OBJECT_DEVICE, TargetDeviceInstance, PROP_VENDOR_NAME, &v) &&
                v.tag == BACNET_APPLICATION_TAG_CHARACTER_STRING) {
                copy_character_string(vname, sizeof(vname), (const BACNET_CHARACTER_STRING *)&v.type.Character_String);
            }
            if (bacnet_read_locked(OBJECT_DEVICE, TargetDeviceInstance, PROP_MODEL_NAME, &v) &&
                v.tag == BACNET_APPLICATION_TAG_CHARACTER_STRING) {
                copy_character_string(mname, sizeof(mname), (const BACNET_CHARACTER_STRING *)&v.type.Character_String);
            }
            if (direct_ok) {
                DiscoveredDevices[0].device_id = TargetDeviceInstance;
                strlcpy(DiscoveredDevices[0].ip, TargetIp, sizeof(DiscoveredDevices[0].ip));
                DiscoveredDevices[0].port = TargetPort;
                DiscoveredDevices[0].vendor_id = 8;
                strlcpy(DiscoveredDevices[0].name, dname, sizeof(DiscoveredDevices[0].name));
                strlcpy(DiscoveredDevices[0].vendor_name, vname[0] ? vname : "Delta Controls", sizeof(DiscoveredDevices[0].vendor_name));
                strlcpy(DiscoveredDevices[0].model_name, mname[0] ? mname : "DAC Controller", sizeof(DiscoveredDevices[0].model_name));
                DiscoveredDeviceCount = 1;
            }
        }

        /* If at least 1 device discovered, bind the first one as active target */
        if (DiscoveredDeviceCount > 0) {
            TargetDeviceInstance = DiscoveredDevices[0].device_id;
            strlcpy(TargetIp, DiscoveredDevices[0].ip, sizeof(TargetIp));
            TargetPort = DiscoveredDevices[0].port;
            strlcpy(TargetDeviceName, DiscoveredDevices[0].name, sizeof(TargetDeviceName));
            TargetDeviceNameValid = true;
            BacnetReady = true;
            target_config_save();
            bind_target_device();
        }

        xSemaphoreGive(BacnetMutex);
    }

    char *buf = malloc(4096);
    if (!buf) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    size_t off = snprintf(buf, 4096, "{\"ok\":true,\"count\":%u,\"devices\":[", (unsigned)DiscoveredDeviceCount);
    for (size_t i = 0; i < DiscoveredDeviceCount; i++) {
        char name_esc[64] = "", vend_esc[64] = "", mod_esc[64] = "";
        json_escape(name_esc, DiscoveredDevices[i].name, sizeof(name_esc));
        json_escape(vend_esc, DiscoveredDevices[i].vendor_name, sizeof(vend_esc));
        json_escape(mod_esc, DiscoveredDevices[i].model_name, sizeof(mod_esc));

        off += snprintf(buf + off, 4096 - off,
                        "%s{\"device_id\":%u,\"instance\":%u,\"name\":\"%s\",\"ip\":\"%s\",\"port\":%u,\"vendor\":\"%s\",\"model\":\"%s\"}",
                        i == 0 ? "" : ",",
                        (unsigned)DiscoveredDevices[i].device_id,
                        (unsigned)DiscoveredDevices[i].device_id,
                        name_esc, DiscoveredDevices[i].ip,
                        (unsigned)DiscoveredDevices[i].port,
                        vend_esc, mod_esc);
    }
    snprintf(buf + off, 4096 - off, "]}");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    free(buf);
    return ESP_OK;
}
static const httpd_uri_t api_bacnet_discover_uri = {
    .uri = "/api/bacnet/discover", .method = HTTP_POST, .handler = api_bacnet_discover_handler};

static esp_err_t api_strategy_inspect_handler(httpd_req_t *req)
{
    float fan_count_raw = 0.0f;
    bool fan_count_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_FAN_COUNT_INSTANCE, PROP_PRESENT_VALUE, &fan_count_raw);
    int fan_count = fan_count_valid ? (int)(fan_count_raw + 0.5f) : 0;

    int plausible_fans = 0;
    for (int i = 0; i < HEALTH_FAN_SUPPLY_AIR_COUNT; i++) {
        float fval = 0.0f;
        if (BacnetReady && read_real_property(OBJECT_ANALOG_INPUT, HealthFanSupplyAirInstances[i], PROP_PRESENT_VALUE, &fval)) {
            if (temp_is_plausible(fval)) {
                plausible_fans++;
            }
        }
    }
    if (!fan_count_valid) {
        fan_count = plausible_fans > 0 ? plausible_fans : 1;
    }

    float design_duty = 0.0f;
    bool design_duty_valid = BacnetReady && read_real_property(
        OBJECT_ANALOG_VALUE, HEALTH_DESIGN_COOLING_DUTY_INSTANCE, PROP_PRESENT_VALUE, &design_duty);

    char *buf = malloc(3072);
    if (!buf) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    size_t off = snprintf(
        buf, 3072,
        "{\"ok\":true,\"device_name\":\"%s\",\"device_id\":%u,\"vendor\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\","
        "\"fan_count\":%d,\"fan_count_valid\":%s,\"plausible_fans\":%d,\"design_duty\":%.2f,\"design_duty_valid\":%s,\"fans\":[",
        TargetDeviceNameValid ? TargetDeviceName : "Delta Controller",
        (unsigned)TargetDeviceInstance,
        fan_count, fan_count_valid ? "true" : "false", plausible_fans,
        design_duty_valid ? design_duty : 0.0f,
        design_duty_valid ? "true" : "false");

    for (int i = 0; i < 5; i++) {
        float fval = 0.0f;
        bool fvalid = BacnetReady && read_real_property(
            OBJECT_ANALOG_INPUT, HealthFanSupplyAirInstances[i], PROP_PRESENT_VALUE, &fval);
        off += snprintf(buf + off, 3072 - off,
                        "%s{\"index\":%d,\"sensor\":%u,\"valid\":%s,\"temp\":%.1f,\"plausible\":%s,\"active\":%s}",
                        i == 0 ? "" : ",", i + 1, (unsigned)HealthFanSupplyAirInstances[i],
                        fvalid ? "true" : "false", fvalid ? fval : 0.0f,
                        (fvalid && temp_is_plausible(fval)) ? "true" : "false",
                        (i < fan_count) ? "true" : "false");
    }

    off += snprintf(buf + off, 3072 - off, "],\"rooms\":[");
    for (size_t i = 0; i < RoomCount; i++) {
        float sp = 0.0f, temp = 0.0f, sa = 0.0f;
        bool pwr = false;
        bool sp_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, Rooms[i].setpoint_instance, PROP_PRESENT_VALUE, &sp);
        bool temp_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, Rooms[i].temperature_instance, PROP_PRESENT_VALUE, &temp);
        bool pwr_ok = BacnetReady && read_bool_property(OBJECT_BINARY_VALUE, Rooms[i].power_instance, PROP_PRESENT_VALUE, &pwr);
        bool sa_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, Rooms[i].supply_air_instance, PROP_PRESENT_VALUE, &sa);

        char prefix[16];
        snprintf(prefix, sizeof(prefix), "Room_%c", (char)('A' + i));
        char def_name[16];
        snprintf(def_name, sizeof(def_name), "Room %c", (char)('A' + i));

        bool temp_plausible = temp_ok && temp_is_plausible(temp);
        /* Room is detected as active if it's within configured fan count OR has plausible active temperature */
        bool detected_active = (i < (size_t)fan_count) || (temp_plausible && temp > 16.0f && temp < 35.0f && (sp_ok || sa_ok));

        off += snprintf(buf + off, 3072 - off,
                        "%s{\"id\":%u,\"name\":\"%s\",\"default_name\":\"%s\",\"prefix\":\"%s\","
                        "\"active\":%s,\"detected_active\":%s,\"instance_base\":%u,"
                        "\"setpoint\":%.1f,\"setpoint_valid\":%s,"
                        "\"temp\":%.1f,\"temp_valid\":%s,\"temp_plausible\":%s,"
                        "\"power\":%s,\"power_valid\":%s,"
                        "\"supply_air\":%.1f,\"supply_air_valid\":%s}",
                        i == 0 ? "" : ",", (unsigned)i, Rooms[i].name, def_name, prefix,
                        Rooms[i].active ? "true" : "false", detected_active ? "true" : "false",
                        (unsigned)(1100 + i * 100),
                        sp_ok ? sp : 0.0f, sp_ok ? "true" : "false",
                        temp_ok ? temp : 0.0f, temp_ok ? "true" : "false", temp_plausible ? "true" : "false",
                        pwr_ok && pwr ? "true" : "false", pwr_ok ? "true" : "false",
                        sa_ok ? sa : 0.0f, sa_ok ? "true" : "false");
    }
    snprintf(buf + off, 3072 - off, "]}");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    free(buf);
    return ESP_OK;
}
static const httpd_uri_t api_strategy_inspect_uri = {
    .uri = "/api/strategy/inspect", .method = HTTP_GET, .handler = api_strategy_inspect_handler};

/* ===================== Rooms & MQTT Management APIs ===================== */

static void parse_rooms_from_json(const char *body)
{
    const char *p = strstr(body, "\"rooms\"");
    if (p) {
        const char *arr = strchr(p, '[');
        if (arr) {
            const char *obj = arr;
            size_t idx = 0;
            while ((obj = strchr(obj, '{')) != NULL && idx < MAX_ROOMS) {
                const char *end_obj = strchr(obj, '}');
                if (!end_obj) break;
                char item[256];
                size_t len = (size_t)(end_obj - obj + 1);
                if (len >= sizeof(item)) len = sizeof(item) - 1;
                memcpy(item, obj, len);
                item[len] = '\0';

                char rname[32] = {0};
                if (json_get_str(item, "name", rname, sizeof(rname)) && strlen(rname) > 0) {
                    strlcpy(Rooms[idx].name, rname, sizeof(Rooms[idx].name));
                }
                bool act = false;
                if (json_get_bool(item, "active", &act)) {
                    Rooms[idx].active = act;
                }
                int sp = 0, temp = 0, pwr = 0, sa = 0, req = 0, cur = 0;
                if (json_get_int(item, "setpoint_instance", &sp) && sp > 0) Rooms[idx].setpoint_instance = (uint32_t)sp;
                if (json_get_int(item, "temperature_instance", &temp) && temp > 0) Rooms[idx].temperature_instance = (uint32_t)temp;
                if (json_get_int(item, "power_instance", &pwr) && pwr > 0) Rooms[idx].power_instance = (uint32_t)pwr;
                if (json_get_int(item, "supply_air_instance", &sa) && sa > 0) Rooms[idx].supply_air_instance = (uint32_t)sa;
                if (json_get_int(item, "required_output_instance", &req) && req > 0) Rooms[idx].required_output_instance = (uint32_t)req;
                if (json_get_int(item, "current_output_instance", &cur) && cur > 0) Rooms[idx].current_output_instance = (uint32_t)cur;

                idx++;
                obj = end_obj + 1;
            }
            if (idx > 0) {
                RoomCount = idx;
            }
        }
    }
    for (size_t i = 0; i < MAX_ROOMS; i++) {
        char key[32];
        snprintf(key, sizeof(key), "r%u_name", (unsigned)i);
        char rname[32] = {0};
        if (json_get_str(body, key, rname, sizeof(rname)) && strlen(rname) > 0) {
            strlcpy(Rooms[i].name, rname, sizeof(Rooms[i].name));
        }
        snprintf(key, sizeof(key), "r%u_active", (unsigned)i);
        bool act = false;
        if (json_get_bool(body, key, &act)) {
            Rooms[i].active = act;
        }
    }
}

static esp_err_t api_rooms_get_handler(httpd_req_t *req)
{
    char buf[1536];
    size_t off = snprintf(buf, sizeof(buf), "{\"ok\":true,\"rooms\":[");
    for (size_t i = 0; i < RoomCount && off < sizeof(buf) - 64; i++) {
        off += snprintf(
            buf + off, sizeof(buf) - off,
            "%s{\"id\":%u,\"name\":\"%s\",\"active\":%s,\"setpoint_instance\":%u,\"temperature_instance\":%u,"
            "\"power_instance\":%u,\"supply_air_instance\":%u,\"required_output_instance\":%u,\"current_output_instance\":%u}",
            i == 0 ? "" : ",", (unsigned)i, Rooms[i].name, Rooms[i].active ? "true" : "false",
            (unsigned)Rooms[i].setpoint_instance, (unsigned)Rooms[i].temperature_instance,
            (unsigned)Rooms[i].power_instance, (unsigned)Rooms[i].supply_air_instance,
            (unsigned)Rooms[i].required_output_instance, (unsigned)Rooms[i].current_output_instance);
    }
    snprintf(buf + off, sizeof(buf) - off, "]}");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_rooms_get_uri = {
    .uri = "/api/rooms", .method = HTTP_GET, .handler = api_rooms_get_handler};

static esp_err_t api_rooms_post_handler(httpd_req_t *req)
{
    char body[1024] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    parse_rooms_from_json(body);
    rooms_config_save();
    mqtt_publish_discovery();

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_rooms_post_uri = {
    .uri = "/api/rooms", .method = HTTP_POST, .handler = api_rooms_post_handler};

static esp_err_t api_mqtt_config_get_handler(httpd_req_t *req)
{
    char buf[512];
    snprintf(buf, sizeof(buf),
             "{\"ok\":true,\"host\":\"%s\",\"port\":%u,\"user\":\"%s\",\"pass_set\":%s,"
             "\"base_topic\":\"%s\",\"ha_device_name\":\"%s\",\"ha_device_id\":\"%s\","
             "\"ha_discovery\":%s,\"ha_health_discovery\":%s,\"connected\":%s}",
             MqttBrokerHost, (unsigned)MqttBrokerPort, MqttBrokerUser,
             strlen(MqttBrokerPass) > 0 ? "true" : "false", MqttTopicBase,
             HaDeviceName, HaDeviceId, HaDiscoveryEnabled ? "true" : "false",
             HaHealthDiscoveryEnabled ? "true" : "false",
             MqttConnected ? "true" : "false");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_mqtt_config_get_uri = {
    .uri = "/api/mqtt/config", .method = HTTP_GET, .handler = api_mqtt_config_get_handler};

static esp_err_t api_mqtt_config_post_handler(httpd_req_t *req)
{
    char body[512] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    char host[64] = {0}, user[32] = {0}, pass[64] = {0};
    char base_top[64] = {0}, dev_name[48] = {0}, dev_id[32] = {0};
    int port = 0;
    bool disc = true, hdisc = true;

    if (json_get_str(body, "host", host, sizeof(host)) && strlen(host) > 0) {
        strlcpy(MqttBrokerHost, host, sizeof(MqttBrokerHost));
    }
    if (json_get_int(body, "port", &port) && port > 0) {
        MqttBrokerPort = (uint16_t)port;
    }
    if (json_get_str(body, "user", user, sizeof(user))) {
        strlcpy(MqttBrokerUser, user, sizeof(MqttBrokerUser));
    }
    if (json_get_str(body, "pass", pass, sizeof(pass))) {
        strlcpy(MqttBrokerPass, pass, sizeof(MqttBrokerPass));
    }
    if (json_get_str(body, "base_topic", base_top, sizeof(base_top)) && strlen(base_top) > 0) {
        strlcpy(MqttTopicBase, base_top, sizeof(MqttTopicBase));
    }
    if (json_get_str(body, "ha_device_name", dev_name, sizeof(dev_name)) && strlen(dev_name) > 0) {
        strlcpy(HaDeviceName, dev_name, sizeof(HaDeviceName));
    }
    if (json_get_str(body, "ha_device_id", dev_id, sizeof(dev_id)) && strlen(dev_id) > 0) {
        strlcpy(HaDeviceId, dev_id, sizeof(HaDeviceId));
    }
    if (json_get_bool(body, "ha_discovery", &disc)) {
        HaDiscoveryEnabled = disc;
    }
    if (json_get_bool(body, "ha_health_discovery", &hdisc)) {
        HaHealthDiscoveryEnabled = hdisc;
    }

    mqtt_config_save();
    mqtt_app_restart();

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_mqtt_config_post_uri = {
    .uri = "/api/mqtt/config", .method = HTTP_POST, .handler = api_mqtt_config_post_handler};

static esp_err_t api_mqtt_entities_get_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "application/json");
    char chunk[512];
    int len = snprintf(chunk, sizeof(chunk),
                       "{\"ok\":true,\"ha_discovery\":%s,\"ha_health_discovery\":%s,\"connected\":%s,\"base_topic\":\"%s\",\"entities\":[",
                       HaDiscoveryEnabled ? "true" : "false",
                       HaHealthDiscoveryEnabled ? "true" : "false",
                       MqttConnected ? "true" : "false",
                       MqttTopicBase);
    httpd_resp_send_chunk(req, chunk, len);

    bool first = true;

    /* Core 1: System Power */
    bool sys_pwr = false;
    bool sys_pwr_ok = BacnetReady && read_bool_property(OBJECT_BINARY_VALUE, SYS_POWER_READBACK_INSTANCE, PROP_PRESENT_VALUE, &sys_pwr);
    len = snprintf(chunk, sizeof(chunk),
                   "%s{\"name\":\"System Power\",\"group\":\"core\",\"type\":\"switch\","
                   "\"unique_id\":\"%s_system_power\",\"topic\":\"%s/system_power/state\","
                   "\"value\":\"%s\",\"removable\":false,\"enabled\":true}",
                   first ? "" : ",", HaDeviceId, MqttTopicBase,
                   sys_pwr_ok ? (sys_pwr ? "ON" : "OFF") : "Unavailable");
    httpd_resp_send_chunk(req, chunk, len);
    first = false;

    /* Core 2: Boost Mode */
    unsigned bmode = 0;
    bool bmode_ok = BacnetReady && read_msv_property(OBJECT_MULTI_STATE_VALUE, BOOST_INSTANCE, PROP_PRESENT_VALUE, &bmode);
    const char *bmode_str = "Unavailable";
    if (bmode_ok) {
        if (bmode == BOOST_MODE_AUTO) bmode_str = "auto";
        else if (bmode == BOOST_MODE_FULL_HEATING) bmode_str = "heat";
        else if (bmode == BOOST_MODE_FULL_COOLING) bmode_str = "cool";
        else bmode_str = "other";
    }
    len = snprintf(chunk, sizeof(chunk),
                   ",{\"name\":\"Boost Mode\",\"group\":\"core\",\"type\":\"select\","
                   "\"unique_id\":\"%s_boost\",\"topic\":\"%s/boost/state\","
                   "\"value\":\"%s\",\"removable\":false,\"enabled\":true}",
                   HaDeviceId, MqttTopicBase, bmode_str);
    httpd_resp_send_chunk(req, chunk, len);

    /* Core 3: Boost Timeout */
    char btout_str[32];
    snprintf(btout_str, sizeof(btout_str), "%u min", (unsigned)BoostTimeoutMinutes);
    len = snprintf(chunk, sizeof(chunk),
                   ",{\"name\":\"Boost Timeout\",\"group\":\"core\",\"type\":\"number\","
                   "\"unique_id\":\"%s_boost_timeout\",\"topic\":\"%s/boost_timeout/state\","
                   "\"value\":\"%s\",\"removable\":false,\"enabled\":true}",
                   HaDeviceId, MqttTopicBase, btout_str);
    httpd_resp_send_chunk(req, chunk, len);

    /* Core 4: Boost Remaining */
    char brem_str[32];
    snprintf(brem_str, sizeof(brem_str), "%d min", boost_remaining_minutes());
    len = snprintf(chunk, sizeof(chunk),
                   ",{\"name\":\"Boost Remaining\",\"group\":\"core\",\"type\":\"sensor\","
                   "\"unique_id\":\"%s_boost_remaining\",\"topic\":\"%s/boost_remaining/state\","
                   "\"value\":\"%s\",\"removable\":false,\"enabled\":true}",
                   HaDeviceId, MqttTopicBase, brem_str);
    httpd_resp_send_chunk(req, chunk, len);

    /* Core 5: Rooms */
    for (size_t i = 0; i < RoomCount; i++) {
        if (!Rooms[i].active) continue;
        float temp = 0.0f, sp = 0.0f;
        bool pwr = false;
        bool t_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, Rooms[i].temperature_instance, PROP_PRESENT_VALUE, &temp);
        bool sp_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, Rooms[i].setpoint_instance, PROP_PRESENT_VALUE, &sp);
        bool p_ok = BacnetReady && read_bool_property(OBJECT_BINARY_VALUE, Rooms[i].power_instance, PROP_PRESENT_VALUE, &pwr);

        char rval[64];
        if (t_ok && sp_ok) {
            snprintf(rval, sizeof(rval), "%.1f°C (Set: %.1f°C)%s", temp, sp, p_ok ? (pwr ? " [On]" : " [Off]") : "");
        } else {
            snprintf(rval, sizeof(rval), "%s%s",
                     t_ok ? "Temp OK" : "Temp unavail",
                     p_ok ? (pwr ? " [On]" : " [Off]") : "");
        }
        char name_esc[64];
        json_escape(name_esc, Rooms[i].name, sizeof(name_esc));
        len = snprintf(chunk, sizeof(chunk),
                       ",{\"name\":\"%s (Climate)\",\"group\":\"core\",\"type\":\"climate\","
                       "\"unique_id\":\"%s_room%u\",\"topic\":\"%s/room%u/mode/state\","
                       "\"value\":\"%s\",\"removable\":false,\"enabled\":true}",
                       name_esc, HaDeviceId, (unsigned)i, MqttTopicBase, (unsigned)i, rval);
        httpd_resp_send_chunk(req, chunk, len);
    }

    /* Health Group */
    float h_out = 0.0f, h_req = 0.0f, h_flow_pct = 0.0f, h_ret = 0.0f;
    bool h_out_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, HEALTH_COOLING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &h_out);
    bool h_req_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, HEALTH_REQUIRED_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &h_req);
    bool h_fp_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, HEALTH_FLOW_DESIGN_PCT_INSTANCE, PROP_PRESENT_VALUE, &h_flow_pct);
    bool h_ret_ok = BacnetReady && read_real_property(OBJECT_ANALOG_INPUT, HEALTH_RETURN_AIR_INSTANCE, PROP_PRESENT_VALUE, &h_ret);

    float h_heat_out = 0.0f, h_heat_req = 0.0f;
    bool h_hout_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, HEALTH_HEATING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &h_heat_out);
    bool h_hreq_ok = BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, HEALTH_REQUIRED_HEATING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &h_heat_req);

    const char *hmode = "cooling";
    if (h_hout_ok && h_hreq_ok && (h_heat_req > h_req || h_heat_out > h_out) && h_heat_req > 0.05f) {
        hmode = "heating";
    }

    char v_out[32], v_req[32], v_pct[32], v_flow[32], v_ret[32], v_delta[32];
    if (strcmp(hmode, "heating") == 0) {
        snprintf(v_out, sizeof(v_out), "%s", h_hout_ok ? "" : "Unavailable");
        if (h_hout_ok) snprintf(v_out, sizeof(v_out), "%.2f kW", h_heat_out);
        snprintf(v_req, sizeof(v_req), "%s", h_hreq_ok ? "" : "Unavailable");
        if (h_hreq_ok) snprintf(v_req, sizeof(v_req), "%.2f kW", h_heat_req);
        if (h_hout_ok && h_hreq_ok && h_heat_req > 0.1f) snprintf(v_pct, sizeof(v_pct), "%.0f%%", (h_heat_out / h_heat_req) * 100.0f);
        else snprintf(v_pct, sizeof(v_pct), "Unavailable");
    } else {
        snprintf(v_out, sizeof(v_out), "%s", h_out_ok ? "" : "Unavailable");
        if (h_out_ok) snprintf(v_out, sizeof(v_out), "%.2f kW", h_out);
        snprintf(v_req, sizeof(v_req), "%s", h_req_ok ? "" : "Unavailable");
        if (h_req_ok) snprintf(v_req, sizeof(v_req), "%.2f kW", h_req);
        if (h_out_ok && h_req_ok && h_req > 0.1f) snprintf(v_pct, sizeof(v_pct), "%.0f%%", (h_out / h_req) * 100.0f);
        else snprintf(v_pct, sizeof(v_pct), "Unavailable");
    }

    snprintf(v_flow, sizeof(v_flow), "%s", h_fp_ok ? "" : "Unavailable");
    if (h_fp_ok) snprintf(v_flow, sizeof(v_flow), "%.1f%%", h_flow_pct);

    snprintf(v_ret, sizeof(v_ret), "%s", h_ret_ok ? "" : "Unavailable");
    if (h_ret_ok) snprintf(v_ret, sizeof(v_ret), "%.1f°C", h_ret);

    /* Delta T calculation */
    float sa_sum = 0.0f;
    int sa_cnt = 0;
    for (size_t i = 0; i < RoomCount; i++) {
        if (!Rooms[i].active) continue;
        float r_sa = 0.0f;
        if (BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, Rooms[i].supply_air_instance, PROP_PRESENT_VALUE, &r_sa) && temp_is_plausible(r_sa)) {
            sa_sum += r_sa;
            sa_cnt++;
        }
    }

    float valve_sig = 0.0f;
    bool valve_sig_ok = (strcmp(hmode, "heating") == 0)
        ? (BacnetReady && read_real_property(OBJECT_ANALOG_OUTPUT, HEALTH_HEATING_VALVE_SIGNAL_INSTANCE, PROP_PRESENT_VALUE, &valve_sig))
        : (BacnetReady && read_real_property(OBJECT_ANALOG_OUTPUT, HEALTH_VALVE_SIGNAL_INSTANCE, PROP_PRESENT_VALUE, &valve_sig));

    bool is_heating = (strcmp(hmode, "heating") == 0);
    float cur_out = is_heating ? (h_hout_ok ? h_heat_out : 0.0f) : (h_out_ok ? h_out : 0.0f);
    bool cur_out_ok = is_heating ? h_hout_ok : h_out_ok;
    float cur_req = is_heating ? (h_hreq_ok ? h_heat_req : 0.0f) : (h_req_ok ? h_req : 0.0f);
    bool cur_req_ok = is_heating ? h_hreq_ok : h_req_ok;

    bool any_alarm_active = false;
    char alarm_label[64] = "";
    bool master_alarm = false;
    if (BacnetReady && read_bool_property(OBJECT_BINARY_VALUE, HealthAlarms[0].instance, PROP_PRESENT_VALUE, &master_alarm) && master_alarm) {
        any_alarm_active = true;
        snprintf(alarm_label, sizeof(alarm_label), "%s", HealthAlarms[0].label);
    }

    health_diagnostics_t diag;
    compute_health_diagnostics(
        is_heating,
        cur_out_ok, cur_out,
        cur_req_ok, cur_req,
        h_fp_ok, h_flow_pct,
        valve_sig_ok, valve_sig,
        h_ret_ok, h_ret,
        sa_cnt > 0, (sa_cnt > 0) ? (sa_sum / sa_cnt) : 0.0f,
        any_alarm_active, alarm_label,
        &diag);

    if (diag.delta_t_valid) {
        snprintf(v_delta, sizeof(v_delta), "%.1f°C", diag.delta_t);
    } else {
        snprintf(v_delta, sizeof(v_delta), "Unavailable");
    }

    /* Output Health Entities */
    struct { const char *name; const char *subtop; const char *val; const char *unit; } h_items[] = {
        {"Delivered Thermal Output", "health/output/state", v_out, "kW"},
        {"Thermal Output Demand", "health/required_output/state", v_req, "kW"},
        {"Delivery Percentage", "health/output_pct/state", v_pct, "%"},
        {"Coil Flow % of Design", "health/flow_pct/state", v_flow, "%"},
        {"Return Air Temperature", "health/return_air/state", v_ret, "°C"},
        {"Air Delta Across Coil", "health/air_delta/state", v_delta, "°C"},
        {"FCU Performance Mode", "health/performance/state", diag.performance, ""},
        {"FCU Performance Level", "health/performance_level/state", diag.performance_level, ""},
        {"FCU System Health", "health/system_health/state", diag.system_health, ""},
        {"FCU Diagnostic Status", "health/diag_status/state", diag.diag_status, ""},
        {"FCU Diagnostic Detail", "health/diag_detail/state", diag.diag_detail, ""},
    };
    for (size_t i = 0; i < sizeof(h_items)/sizeof(h_items[0]); i++) {
        char typ_str[32];
        if (strlen(h_items[i].unit) > 0) {
            snprintf(typ_str, sizeof(typ_str), "sensor (%s)", h_items[i].unit);
        } else {
            snprintf(typ_str, sizeof(typ_str), "sensor");
        }
        char val_esc[280];
        json_escape(val_esc, h_items[i].val, sizeof(val_esc));
        len = snprintf(chunk, sizeof(chunk),
                       ",{\"name\":\"%s\",\"group\":\"health\",\"type\":\"%s\","
                       "\"unique_id\":\"%s_%s\",\"topic\":\"%s/%s\","
                       "\"value\":\"%s\",\"removable\":false,\"enabled\":%s}",
                       h_items[i].name, typ_str,
                       HaDeviceId, h_items[i].subtop,
                       MqttTopicBase, h_items[i].subtop,
                       val_esc,
                       HaHealthDiscoveryEnabled ? "true" : "false");
        httpd_resp_send_chunk(req, chunk, len);
    }

    /* Fan Health Entities - only those with a room to name them after; see
       fan_room_count(). */
    int fcnt = fan_room_count();
    for (int f = 1; f <= fcnt; f++) {
        float ftemp = 0.0f, fspd = 0.0f;
        bool ft_ok = BacnetReady && read_real_property(OBJECT_ANALOG_INPUT, f, PROP_PRESENT_VALUE, &ftemp);
        bool fs_ok = (f <= HEALTH_FAN_SPEED_COUNT) && BacnetReady && read_real_property(OBJECT_ANALOG_OUTPUT, f, PROP_PRESENT_VALUE, &fspd);

        char ft_val[32], fs_val[32];
        if (ft_ok && temp_is_plausible(ftemp)) snprintf(ft_val, sizeof(ft_val), "%.1f°C", ftemp);
        else snprintf(ft_val, sizeof(ft_val), "Unavailable");

        if (fs_ok) snprintf(fs_val, sizeof(fs_val), "%.1f%%", fspd);
        else snprintf(fs_val, sizeof(fs_val), "Unavailable");

        char room_name[32] = "", room_esc[64];
        fan_room_name(f, room_name, sizeof(room_name));
        json_escape(room_esc, room_name, sizeof(room_esc));

        char fname_t[96], fname_s[96], fsub_t[48], fsub_s[48];
        snprintf(fname_t, sizeof(fname_t), "%s Supply Air Temp", room_esc);
        snprintf(fname_s, sizeof(fname_s), "%s Fan Speed", room_esc);
        snprintf(fsub_t, sizeof(fsub_t), "health/fan%d/supply_air/state", f);
        snprintf(fsub_s, sizeof(fsub_s), "health/fan%d/speed/state", f);

        len = snprintf(chunk, sizeof(chunk),
                       ",{\"name\":\"%s\",\"group\":\"health\",\"type\":\"sensor (°C)\","
                       "\"unique_id\":\"%s_health_fan%d_sa\",\"topic\":\"%s/%s\","
                       "\"value\":\"%s\",\"removable\":false,\"enabled\":%s}",
                       fname_t, HaDeviceId, f, MqttTopicBase, fsub_t, ft_val,
                       HaHealthDiscoveryEnabled ? "true" : "false");
        httpd_resp_send_chunk(req, chunk, len);

        len = snprintf(chunk, sizeof(chunk),
                       ",{\"name\":\"%s\",\"group\":\"health\",\"type\":\"sensor (%%)\","
                       "\"unique_id\":\"%s_health_fan%d_spd\",\"topic\":\"%s/%s\","
                       "\"value\":\"%s\",\"removable\":false,\"enabled\":%s}",
                       fname_s, HaDeviceId, f, MqttTopicBase, fsub_s, fs_val,
                       HaHealthDiscoveryEnabled ? "true" : "false");
        httpd_resp_send_chunk(req, chunk, len);
    }

    /* Custom Group */
    for (size_t i = 0; i < CustomMqttCount; i++) {
        char val_str[32] = "Unavailable";
        if (CustomMqttPoints[i].obj_type == OBJECT_ANALOG_VALUE ||
            CustomMqttPoints[i].obj_type == OBJECT_ANALOG_INPUT ||
            CustomMqttPoints[i].obj_type == OBJECT_ANALOG_OUTPUT) {
            float val = 0.0f;
            if (BacnetReady && read_real_property((BACNET_OBJECT_TYPE)CustomMqttPoints[i].obj_type, CustomMqttPoints[i].instance, PROP_PRESENT_VALUE, &val)) {
                snprintf(val_str, sizeof(val_str), "%.2f", val);
            }
        } else if (CustomMqttPoints[i].obj_type == OBJECT_BINARY_VALUE ||
                   CustomMqttPoints[i].obj_type == OBJECT_BINARY_INPUT ||
                   CustomMqttPoints[i].obj_type == OBJECT_BINARY_OUTPUT) {
            bool bval = false;
            if (BacnetReady && read_bool_property((BACNET_OBJECT_TYPE)CustomMqttPoints[i].obj_type, CustomMqttPoints[i].instance, PROP_PRESENT_VALUE, &bval)) {
                snprintf(val_str, sizeof(val_str), "%s", bval ? "ON" : "OFF");
            }
        }
        char name_esc[64];
        json_escape(name_esc, CustomMqttPoints[i].name, sizeof(name_esc));
        char subtop[64];
        snprintf(subtop, sizeof(subtop), "custom/%u_%u/state", CustomMqttPoints[i].obj_type, (unsigned)CustomMqttPoints[i].instance);
        len = snprintf(chunk, sizeof(chunk),
                       ",{\"name\":\"%s\",\"group\":\"custom\",\"type\":\"%s (%s #%u)\","
                       "\"unique_id\":\"%s_cust_%u_%u\",\"topic\":\"%s/%s\","
                       "\"value\":\"%s\",\"removable\":true,\"enabled\":%s,\"obj_type\":%u,\"instance\":%u,\"component\":\"%s\"}",
                       name_esc, CustomMqttPoints[i].component, CustomMqttPoints[i].type_str,
                       (unsigned)CustomMqttPoints[i].instance,
                       HaDeviceId, CustomMqttPoints[i].obj_type, (unsigned)CustomMqttPoints[i].instance,
                       MqttTopicBase, subtop, val_str,
                       CustomMqttPoints[i].enabled ? "true" : "false",
                       (unsigned)CustomMqttPoints[i].obj_type, (unsigned)CustomMqttPoints[i].instance,
                       CustomMqttPoints[i].component);
        httpd_resp_send_chunk(req, chunk, len);
    }

    httpd_resp_send_chunk(req, "]}", 2);
    httpd_resp_send_chunk(req, NULL, 0);
    return ESP_OK;
}
static const httpd_uri_t api_mqtt_entities_uri = {
    .uri = "/api/mqtt/entities", .method = HTTP_GET, .handler = api_mqtt_entities_get_handler};

static bool test_tcp_connection(const char *host, uint16_t port, uint32_t timeout_ms)
{
    struct addrinfo hints = {0};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = NULL;
    char port_str[8];
    snprintf(port_str, sizeof(port_str), "%u", port);

    if (getaddrinfo(host, port_str, &hints, &res) != 0 || !res) {
        return false;
    }

    int sock = socket(res->ai_family, res->ai_socktype, 0);
    if (sock < 0) {
        freeaddrinfo(res);
        return false;
    }

    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    int ret = connect(sock, res->ai_addr, res->ai_addrlen);
    close(sock);
    freeaddrinfo(res);
    return ret == 0;
}

static esp_err_t api_mqtt_test_handler(httpd_req_t *req)
{
    char body[256] = {0};
    recv_body(req, body, sizeof(body));
    char host[64] = {0};
    int port = 0;
    if (!json_get_str(body, "host", host, sizeof(host)) || strlen(host) == 0) {
        strlcpy(host, MqttBrokerHost, sizeof(host));
    }
    if (!json_get_int(body, "port", &port) || port <= 0) {
        port = MqttBrokerPort;
    }

    bool ok = test_tcp_connection(host, (uint16_t)port, 3000);
    char buf[128];
    snprintf(buf, sizeof(buf), "{\"ok\":%s,\"host\":\"%s\",\"port\":%u}", ok ? "true" : "false", host, (unsigned)port);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_mqtt_test_uri = {
    .uri = "/api/mqtt/test", .method = HTTP_POST, .handler = api_mqtt_test_handler};

/* Best-effort broker discovery via mDNS service browsing (_mqtt._tcp) -
   only finds brokers that actively announce themselves (e.g. Home
   Assistant's Mosquitto add-on does this by default; a bare `mosquitto`
   install, including the one this firmware talks to on the Pi, does NOT
   unless an Avahi service file is added for it). Absence of results here
   doesn't mean no broker exists on the LAN, just that none advertised -
   the MQTT step's manual host field is still the fallback either way.
   Connected-mode only (mdns_start_service() never runs in provisioning-AP
   mode - see app_main), so this is only registered on that server. */
static esp_err_t api_mqtt_scan_handler(httpd_req_t *req)
{
    mdns_result_t *results = NULL;
    esp_err_t err = mdns_query_ptr("_mqtt", "_tcp", 3000, 10, &results);

    char *buf = malloc(1024);
    if (!buf) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    size_t off = (size_t)snprintf(buf, 1024, "{\"ok\":%s,\"brokers\":[", err == ESP_OK ? "true" : "false");

    bool first = true;
    for (mdns_result_t *r = results; r && off < 900; r = r->next) {
        char ip[48] = "";
        if (r->addr && r->addr->addr.type == ESP_IPADDR_TYPE_V4) {
            esp_ip4addr_ntoa(&r->addr->addr.u_addr.ip4, ip, sizeof(ip));
        } else if (r->hostname) {
            snprintf(ip, sizeof(ip), "%s.local", r->hostname);
        }
        if (ip[0] == '\0') {
            continue;
        }
        char name_escaped[64] = "";
        if (r->instance_name) {
            json_escape(name_escaped, r->instance_name, sizeof(name_escaped));
        }
        off += (size_t)snprintf(
            buf + off, 1024 - off, "%s{\"host\":\"%s\",\"port\":%u,\"name\":\"%s\"}",
            first ? "" : ",", ip, (unsigned)r->port, name_escaped);
        first = false;
    }
    off += (size_t)snprintf(buf + off, 1024 - off, "]}");

    if (results) {
        mdns_query_results_free(results);
    }
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, off);
    free(buf);
    return ESP_OK;
}
static const httpd_uri_t api_mqtt_scan_uri = {
    .uri = "/api/mqtt/scan", .method = HTTP_GET, .handler = api_mqtt_scan_handler};

static esp_err_t api_mqtt_republish_handler(httpd_req_t *req)
{
    mqtt_publish_discovery();
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_mqtt_republish_uri = {
    .uri = "/api/mqtt/republish", .method = HTTP_POST, .handler = api_mqtt_republish_handler};

static esp_err_t api_wizard_finish_handler(httpd_req_t *req)
{
    char body[1536] = {0};
    if (!recv_body(req, body, sizeof(body))) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    char tip[16] = {0};
    int tport = 0, tdev = 0;
    if (json_get_str(body, "target_ip", tip, sizeof(tip)) && strlen(tip) > 0) {
        strlcpy(TargetIp, tip, sizeof(TargetIp));
    }
    if (json_get_int(body, "target_port", &tport) && tport > 0) {
        TargetPort = (uint16_t)tport;
    }
    if (json_get_int(body, "target_device_id", &tdev) && tdev > 0) {
        TargetDeviceInstance = (uint32_t)tdev;
    }
    target_config_save();
    bind_target_device();

    parse_rooms_from_json(body);
    rooms_config_save();

    char mhost[64] = {0}, muser[32] = {0}, mpass[64] = {0};
    char mtop[64] = {0}, mname[48] = {0}, mid[32] = {0};
    int mport = 0;
    bool mdisc = true;

    if (json_get_str(body, "mqtt_host", mhost, sizeof(mhost)) && strlen(mhost) > 0) {
        strlcpy(MqttBrokerHost, mhost, sizeof(MqttBrokerHost));
    }
    if (json_get_int(body, "mqtt_port", &mport) && mport > 0) {
        MqttBrokerPort = (uint16_t)mport;
    }
    if (json_get_str(body, "mqtt_user", muser, sizeof(muser))) {
        strlcpy(MqttBrokerUser, muser, sizeof(MqttBrokerUser));
    }
    if (json_get_str(body, "mqtt_pass", mpass, sizeof(mpass))) {
        strlcpy(MqttBrokerPass, mpass, sizeof(MqttBrokerPass));
    }
    if (json_get_str(body, "mqtt_base_topic", mtop, sizeof(mtop)) && strlen(mtop) > 0) {
        strlcpy(MqttTopicBase, mtop, sizeof(MqttTopicBase));
    }
    if (json_get_str(body, "ha_device_name", mname, sizeof(mname)) && strlen(mname) > 0) {
        strlcpy(HaDeviceName, mname, sizeof(HaDeviceName));
    }
    if (json_get_str(body, "ha_device_id", mid, sizeof(mid)) && strlen(mid) > 0) {
        strlcpy(HaDeviceId, mid, sizeof(HaDeviceId));
    }
    if (json_get_bool(body, "ha_discovery", &mdisc)) {
        HaDiscoveryEnabled = mdisc;
    }
    mqtt_config_save();
    mqtt_app_restart();
    wizard_completed_save();

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_wizard_finish_uri = {
    .uri = "/api/wizard/finish", .method = HTTP_POST, .handler = api_wizard_finish_handler};

static void delayed_reboot_task(void *arg)
{
    (void)arg;
    vTaskDelay(pdMS_TO_TICKS(1500));
    esp_restart();
    vTaskDelete(NULL);
}

static esp_err_t api_config_export_handler(httpd_req_t *req)
{
    char wifi_ssid[33] = "";
    char wifi_pass[65] = "";
    nvs_handle_t whandle;
    if (nvs_open(NVS_NAMESPACE, NVS_READONLY, &whandle) == ESP_OK) {
        size_t len = sizeof(wifi_ssid);
        nvs_get_str(whandle, NVS_KEY_SSID, wifi_ssid, &len);
        len = sizeof(wifi_pass);
        nvs_get_str(whandle, NVS_KEY_PASS, wifi_pass, &len);
        nvs_close(whandle);
    }

    char *buf = malloc(6144);
    if (!buf) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    char wifi_ssid_esc[64] = "", wifi_pass_esc[128] = "";
    char ota_pw_esc[128] = "";
    char tip_esc[32] = "", mhost_esc[128] = "", muser_esc[64] = "", mpass_esc[128] = "";
    char mtop_esc[128] = "", mdev_esc[96] = "", mid_esc[64] = "";

    json_escape(wifi_ssid_esc, wifi_ssid, sizeof(wifi_ssid_esc));
    json_escape(wifi_pass_esc, wifi_pass, sizeof(wifi_pass_esc));
    json_escape(ota_pw_esc, OtaPassword, sizeof(ota_pw_esc));
    json_escape(tip_esc, TargetIp, sizeof(tip_esc));
    json_escape(mhost_esc, MqttBrokerHost, sizeof(mhost_esc));
    json_escape(muser_esc, MqttBrokerUser, sizeof(muser_esc));
    json_escape(mpass_esc, MqttBrokerPass, sizeof(mpass_esc));
    json_escape(mtop_esc, MqttTopicBase, sizeof(mtop_esc));
    json_escape(mdev_esc, HaDeviceName, sizeof(mdev_esc));
    json_escape(mid_esc, HaDeviceId, sizeof(mid_esc));

    size_t off = snprintf(
        buf, 6144,
        "{\n"
        "  \"version\": 1,\n"
        "  \"wizard_done\": %s,\n"
        "  \"target\": {\n"
        "    \"ip\": \"%s\",\n"
        "    \"port\": %u,\n"
        "    \"dev_id\": %u\n"
        "  },\n"
        "  \"wifi\": {\n"
        "    \"ssid\": \"%s\",\n"
        "    \"pass\": \"%s\"\n"
        "  },\n"
        "  \"ota\": {\n"
        "    \"password\": \"%s\"\n"
        "  },\n"
        "  \"rooms\": [\n",
        WizardCompleted ? "true" : "false",
        tip_esc, (unsigned)TargetPort, (unsigned)TargetDeviceInstance,
        wifi_ssid_esc, wifi_pass_esc,
        ota_pw_esc);

    for (size_t i = 0; i < RoomCount; i++) {
        char rname_esc[64] = "";
        json_escape(rname_esc, Rooms[i].name, sizeof(rname_esc));
        off += snprintf(
            buf + off, 6144 - off,
            "    {\"id\": %u, \"name\": \"%s\", \"active\": %s, "
            "\"setpoint_instance\": %u, \"temperature_instance\": %u, \"power_instance\": %u, "
            "\"supply_air_instance\": %u, \"required_output_instance\": %u, \"current_output_instance\": %u}%s\n",
            (unsigned)i, rname_esc, Rooms[i].active ? "true" : "false",
            (unsigned)Rooms[i].setpoint_instance, (unsigned)Rooms[i].temperature_instance,
            (unsigned)Rooms[i].power_instance, (unsigned)Rooms[i].supply_air_instance,
            (unsigned)Rooms[i].required_output_instance, (unsigned)Rooms[i].current_output_instance,
            (i + 1 < RoomCount) ? "," : "");
    }

    off += snprintf(
        buf + off, 6144 - off,
        "  ],\n"
        "  \"mqtt\": {\n"
        "    \"host\": \"%s\",\n"
        "    \"port\": %u,\n"
        "    \"user\": \"%s\",\n"
        "    \"pass\": \"%s\",\n"
        "    \"base_topic\": \"%s\",\n"
        "    \"ha_device_name\": \"%s\",\n"
        "    \"ha_device_id\": \"%s\",\n"
        "    \"ha_discovery\": %s,\n"
        "    \"ha_health_discovery\": %s\n"
        "  },\n"
        "  \"custom_mqtt\": [\n",
        mhost_esc, (unsigned)MqttBrokerPort, muser_esc, mpass_esc,
        mtop_esc, mdev_esc, mid_esc,
        HaDiscoveryEnabled ? "true" : "false",
        HaHealthDiscoveryEnabled ? "true" : "false");

    for (size_t i = 0; i < CustomMqttCount; i++) {
        char pname_esc[64] = "", pcomp_esc[32] = "", ptstr_esc[32] = "";
        json_escape(pname_esc, CustomMqttPoints[i].name, sizeof(pname_esc));
        json_escape(pcomp_esc, CustomMqttPoints[i].component, sizeof(pcomp_esc));
        json_escape(ptstr_esc, CustomMqttPoints[i].type_str, sizeof(ptstr_esc));
        off += snprintf(
            buf + off, 6144 - off,
            "    {\"type\": \"%s\", \"instance\": %u, \"name\": \"%s\", \"component\": \"%s\", \"enabled\": %s}%s\n",
            ptstr_esc, (unsigned)CustomMqttPoints[i].instance, pname_esc, pcomp_esc,
            CustomMqttPoints[i].enabled ? "true" : "false",
            (i + 1 < CustomMqttCount) ? "," : "");
    }

    off += snprintf(
        buf + off, 6144 - off,
        "  ],\n"
        "  \"app\": {\n"
        "    \"boost_timeout_min\": %u,\n"
        "    \"boost_revert_external\": %s\n"
        "  }\n"
        "}\n",
        (unsigned)BoostTimeoutMinutes,
        BoostRevertExternal ? "true" : "false");

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Content-Disposition", "attachment; filename=\"esp32_bacnet_config.json\"");
    httpd_resp_send(req, buf, off);
    free(buf);
    return ESP_OK;
}
static const httpd_uri_t api_config_export_uri = {
    .uri = "/api/config/export", .method = HTTP_GET, .handler = api_config_export_handler};

static esp_err_t api_config_import_handler(httpd_req_t *req)
{
    char *body = NULL;
    size_t body_len = 0;
    if (!recv_full_body(req, &body, &body_len) || !body || body_len == 0) {
        if (body) free(body);
        send_bad_request(req, "{\"ok\":false,\"error\":\"empty or invalid payload\"}");
        return ESP_OK;
    }

    bool has_wifi_update = false;
    bool reboot_required = false;

    /* 1. Target settings */
    char tgt_obj[256] = {0};
    char tip[32] = {0};
    int tport = 0, tdev = 0;
    if (json_extract_obj(body, "target", tgt_obj, sizeof(tgt_obj))) {
        json_get_str(tgt_obj, "ip", tip, sizeof(tip));
        json_get_int(tgt_obj, "port", &tport);
        json_get_int(tgt_obj, "dev_id", &tdev);
    }
    if (tip[0] == '\0') json_get_str(body, "target_ip", tip, sizeof(tip));
    if (tport <= 0) json_get_int(body, "target_port", &tport);
    if (tdev <= 0) json_get_int(body, "target_device_id", &tdev);

    if (strlen(tip) > 0) strlcpy(TargetIp, tip, sizeof(TargetIp));
    if (tport > 0) TargetPort = (uint16_t)tport;
    if (tdev > 0) TargetDeviceInstance = (uint32_t)tdev;
    target_config_save();
    bind_target_device();

    /* 2. WiFi settings */
    char wifi_obj[256] = {0};
    char w_ssid[33] = {0}, w_pass[65] = {0};
    if (json_extract_obj(body, "wifi", wifi_obj, sizeof(wifi_obj))) {
        json_get_str(wifi_obj, "ssid", w_ssid, sizeof(w_ssid));
        json_get_str(wifi_obj, "pass", w_pass, sizeof(w_pass));
    }
    if (w_ssid[0] == '\0') {
        json_get_str(body, "ssid", w_ssid, sizeof(w_ssid));
        json_get_str(body, "password", w_pass, sizeof(w_pass));
    }
    if (strlen(w_ssid) > 0) {
        nvs_handle_t whandle;
        if (nvs_open(NVS_NAMESPACE, NVS_READWRITE, &whandle) == ESP_OK) {
            nvs_set_str(whandle, NVS_KEY_SSID, w_ssid);
            nvs_set_str(whandle, NVS_KEY_PASS, w_pass);
            nvs_set_u8(whandle, NVS_KEY_WIZARD_DONE, 1);
            nvs_commit(whandle);
            nvs_close(whandle);
            has_wifi_update = true;
        }
    }

    /* 3. OTA password */
    char ota_obj[128] = {0};
    char ota_pw[OTA_PASSWORD_MAX_LEN + 1] = {0};
    if (json_extract_obj(body, "ota", ota_obj, sizeof(ota_obj))) {
        json_get_str(ota_obj, "password", ota_pw, sizeof(ota_pw));
    }
    if (strlen(ota_pw) >= OTA_PASSWORD_MIN_LEN) {
        ota_password_store(ota_pw);
    }

    /* 4. Rooms */
    parse_rooms_from_json(body);
    rooms_config_save();

    /* 5. MQTT settings */
    char mqtt_obj[512] = {0};
    char mhost[64] = {0}, muser[32] = {0}, mpass[64] = {0};
    char mtop[64] = {0}, mname[48] = {0}, mid[32] = {0};
    int mport = 0;
    bool mdisc = true, mhdisc = true;
    const char *mq_src = json_extract_obj(body, "mqtt", mqtt_obj, sizeof(mqtt_obj)) ? mqtt_obj : body;

    if (json_get_str(mq_src, "host", mhost, sizeof(mhost)) || json_get_str(body, "mqtt_host", mhost, sizeof(mhost))) {
        strlcpy(MqttBrokerHost, mhost, sizeof(MqttBrokerHost));
    }
    if (json_get_int(mq_src, "port", &mport) || json_get_int(body, "mqtt_port", &mport)) {
        if (mport > 0) MqttBrokerPort = (uint16_t)mport;
    }
    if (json_get_str(mq_src, "user", muser, sizeof(muser)) || json_get_str(body, "mqtt_user", muser, sizeof(muser))) {
        strlcpy(MqttBrokerUser, muser, sizeof(MqttBrokerUser));
    }
    if (json_get_str(mq_src, "pass", mpass, sizeof(mpass)) || json_get_str(body, "mqtt_pass", mpass, sizeof(mpass))) {
        strlcpy(MqttBrokerPass, mpass, sizeof(MqttBrokerPass));
    }
    if (json_get_str(mq_src, "base_topic", mtop, sizeof(mtop)) || json_get_str(body, "mqtt_base_topic", mtop, sizeof(mtop)) || json_get_str(mq_src, "base_top", mtop, sizeof(mtop))) {
        strlcpy(MqttTopicBase, mtop, sizeof(MqttTopicBase));
    }
    if (json_get_str(mq_src, "ha_device_name", mname, sizeof(mname)) || json_get_str(body, "ha_device_name", mname, sizeof(mname))) {
        strlcpy(HaDeviceName, mname, sizeof(HaDeviceName));
    }
    if (json_get_str(mq_src, "ha_device_id", mid, sizeof(mid)) || json_get_str(body, "ha_device_id", mid, sizeof(mid))) {
        strlcpy(HaDeviceId, mid, sizeof(HaDeviceId));
    }
    if (json_get_bool(mq_src, "ha_discovery", &mdisc) || json_get_bool(body, "ha_discovery", &mdisc)) {
        HaDiscoveryEnabled = mdisc;
    }
    if (json_get_bool(mq_src, "ha_health_discovery", &mhdisc)) {
        HaHealthDiscoveryEnabled = mhdisc;
    }
    mqtt_config_save();
    mqtt_app_restart();
    mqtt_publish_discovery();

    /* 6. Custom MQTT points */
    parse_custom_mqtt_from_json(body);
    custom_mqtt_save();

    /* 7. App / Boost settings */
    char app_obj[128] = {0};
    int b_min = 0;
    bool b_ext = false;
    if (json_extract_obj(body, "app", app_obj, sizeof(app_obj))) {
        if (json_get_int(app_obj, "boost_timeout_min", &b_min)) {
            BoostTimeoutMinutes = (uint32_t)b_min;
        }
        if (json_get_bool(app_obj, "boost_revert_external", &b_ext)) {
            BoostRevertExternal = b_ext;
        }
        app_config_save();
    }

    /* 8. Wizard completion */
    bool w_done = false;
    if (json_get_bool(body, "wizard_done", &w_done) && w_done) {
        wizard_completed_save();
    } else {
        wizard_completed_save();
    }

    free(body);

    /* If we were in SoftAP mode or WiFi credentials changed, a reboot joins the restored network */
    if (has_wifi_update || StaIp[0] == '\0') {
        reboot_required = true;
        spawn_task(delayed_reboot_task, "delay_reboot", 2048, NULL, 5, NULL);
    }

    char resp[128];
    snprintf(resp, sizeof(resp), "{\"ok\":true,\"reboot_required\":%s}", reboot_required ? "true" : "false");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, resp, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}
static const httpd_uri_t api_config_import_uri = {
    .uri = "/api/config/import", .method = HTTP_POST, .handler = api_config_import_handler};

static void erase_namespace(const char *ns)
{
    nvs_handle_t handle;
    if (nvs_open(ns, NVS_READWRITE, &handle) == ESP_OK) {
        nvs_erase_all(handle);
        nvs_commit(handle);
        nvs_close(handle);
    }
}

static esp_err_t api_factory_reset_handler(httpd_req_t *req)
{
    mqtt_unpublish_discovery();

    erase_namespace(NVS_NAMESPACE);
    erase_namespace(NVS_TARGET_NAMESPACE);
    erase_namespace(NVS_ROOMS_NAMESPACE);
    erase_namespace(NVS_MQTT_NAMESPACE);
    erase_namespace(NVS_CUSTOM_MQTT_NAMESPACE);
    erase_namespace(NVS_APP_NAMESPACE);
    erase_namespace(OTA_NVS_NAMESPACE);

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"ok\":true}", HTTPD_RESP_USE_STRLEN);

    vTaskDelay(pdMS_TO_TICKS(1000));
    esp_restart();
    return ESP_OK;
}
static const httpd_uri_t api_factory_reset_uri = {
    .uri = "/api/factory-reset", .method = HTTP_POST, .handler = api_factory_reset_handler};

/* ===================== Web Server Life Cycle ===================== */

static esp_err_t api_logs_get_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/plain; charset=utf-8");
    char chunk[512];
    taskENTER_CRITICAL(&LogMux);
    size_t count = LogCount;
    size_t start = (LogHead + LOG_BUFFER_SIZE - count) % LOG_BUFFER_SIZE;
    taskEXIT_CRITICAL(&LogMux);

    size_t sent = 0;
    while (sent < count) {
        size_t chunk_len = count - sent;
        if (chunk_len > sizeof(chunk) - 1) chunk_len = sizeof(chunk) - 1;
        taskENTER_CRITICAL(&LogMux);
        for (size_t i = 0; i < chunk_len; i++) {
            chunk[i] = LogBuffer[(start + sent + i) % LOG_BUFFER_SIZE];
        }
        taskEXIT_CRITICAL(&LogMux);
        chunk[chunk_len] = '\0';
        if (httpd_resp_send_chunk(req, chunk, chunk_len) != ESP_OK) {
            httpd_resp_send_chunk(req, NULL, 0);
            return ESP_FAIL;
        }
        sent += chunk_len;
    }
    httpd_resp_send_chunk(req, NULL, 0);
    return ESP_OK;
}
static const httpd_uri_t api_logs_uri = {
    .uri = "/api/logs", .method = HTTP_GET, .handler = api_logs_get_handler};

/* Temporary diagnostic: minimum-ever-free stack for each of this file's large
   (24KB-requested) tasks, so their real worst-case usage can be measured
   instead of guessed - see task_stack_headroom_bytes(). Exercise the app
   (health page, room changes, MQTT command if it manages to connect) for a
   while before reading this, so the numbers reflect worst case, not idle.
   mqtt_state now also drains MqttCommandQueue (see that task), so its number
   covers what used to be mqtt_command's workload too. */
static esp_err_t api_debug_stacks_get_handler(httpd_req_t *req)
{
    char buf[512];
    int off = snprintf(
        buf, sizeof(buf),
        "{\"note\":\"headroom_bytes = requested_stack - worst_ever_used; negative task name = not currently running\","
        "\"free_heap\":%u,\"largest_free_block\":%u,"
        "\"eth_bringup\":{\"requested\":24576,\"headroom_bytes\":%d},"
        "\"mqtt_state\":{\"requested\":24576,\"headroom_bytes\":%d},"
        "\"httpd\":{\"requested\":24576,\"headroom_bytes\":%d}}",
        (unsigned)heap_caps_get_free_size(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL),
        (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL),
        task_stack_headroom_bytes("eth_bringup"),
        task_stack_headroom_bytes("mqtt_state"),
        task_stack_headroom_bytes("httpd"));
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, buf, off);
    return ESP_OK;
}
static const httpd_uri_t api_debug_stacks_uri = {
    .uri = "/api/debug/stacks", .method = HTTP_GET, .handler = api_debug_stacks_get_handler};

static httpd_handle_t start_connected_webserver(void)
{
    httpd_handle_t server = NULL;
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.max_open_sockets = 13;
    config.lru_purge_enable = true;
    config.stack_size = 24576;
    config.max_uri_handlers = 55;

    ESP_LOGI(TAG_WIFI, "Starting connected-mode dashboard server on port: '%d'", config.server_port);
    if (httpd_start(&server, &config) == ESP_OK) {
        httpd_register_uri_handler(server, &manage_uri);
        httpd_register_uri_handler(server, &status_page_uri);
        httpd_register_uri_handler(server, &health_page_uri);
        httpd_register_uri_handler(server, &reset_page_uri);
        httpd_register_uri_handler(server, &update_page_uri);
        httpd_register_uri_handler(server, &objects_page_uri);
        httpd_register_uri_handler(server, &wizard_page_uri);
        httpd_register_uri_handler(server, &mqtt_page_uri);
        httpd_register_uri_handler(server, &api_network_uri);
        httpd_register_uri_handler(server, &api_status_uri);
        httpd_register_uri_handler(server, &api_health_uri);
        httpd_register_uri_handler(server, &api_room_setpoint_uri);
        httpd_register_uri_handler(server, &api_room_power_uri);
        httpd_register_uri_handler(server, &api_system_power_uri);
        httpd_register_uri_handler(server, &api_boost_uri);
        httpd_register_uri_handler(server, &api_boost_timeout_uri);
        httpd_register_uri_handler(server, &api_bacnet_read_uri);
        httpd_register_uri_handler(server, &api_bacnet_write_uri);
        httpd_register_uri_handler(server, &api_wifi_reset_uri);
        httpd_register_uri_handler(server, &api_ota_uri);
        httpd_register_uri_handler(server, &api_setup_password_uri);
        httpd_register_uri_handler(server, &api_objects_scan_start_uri);
        httpd_register_uri_handler(server, &api_objects_scan_status_uri);
        httpd_register_uri_handler(server, &api_objects_uri);
        httpd_register_uri_handler(server, &api_objects_inspect_uri);
        httpd_register_uri_handler(server, &api_objects_batch_uri);
        httpd_register_uri_handler(server, &api_objects_custom_mqtt_get_uri);
        httpd_register_uri_handler(server, &api_objects_custom_mqtt_post_uri);
        httpd_register_uri_handler(server, &api_bacnet_discover_uri);
        httpd_register_uri_handler(server, &api_strategy_inspect_uri);
        httpd_register_uri_handler(server, &api_rooms_get_uri);
        httpd_register_uri_handler(server, &api_rooms_post_uri);
        httpd_register_uri_handler(server, &api_mqtt_config_get_uri);
        httpd_register_uri_handler(server, &api_mqtt_config_post_uri);
        httpd_register_uri_handler(server, &api_mqtt_entities_uri);
        httpd_register_uri_handler(server, &api_mqtt_test_uri);
        httpd_register_uri_handler(server, &api_mqtt_scan_uri);
        httpd_register_uri_handler(server, &api_mqtt_republish_uri);
        httpd_register_uri_handler(server, &api_wizard_finish_uri);
        httpd_register_uri_handler(server, &api_factory_reset_uri);
        httpd_register_uri_handler(server, &api_config_export_uri);
        httpd_register_uri_handler(server, &api_config_import_uri);
        httpd_register_uri_handler(server, &api_logs_uri);
        httpd_register_uri_handler(server, &api_debug_stacks_uri);
    }
    return server;
}

static httpd_handle_t start_webserver(void)
{
    httpd_handle_t server = NULL;
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.max_open_sockets = 13;
    config.lru_purge_enable = true;
    config.stack_size = 24576;
    config.max_uri_handlers = 25;

    ESP_LOGI(TAG_WIFI, "Starting server on port: '%d'", config.server_port);
    if (httpd_start(&server, &config) == ESP_OK) {
        httpd_register_uri_handler(server, &root_uri);
        httpd_register_uri_handler(server, &scan_uri);
        httpd_register_uri_handler(server, &connect_uri);
        httpd_register_uri_handler(server, &wizard_page_uri);
        httpd_register_uri_handler(server, &api_network_uri);
        httpd_register_uri_handler(server, &api_bacnet_discover_uri);
        httpd_register_uri_handler(server, &api_strategy_inspect_uri);
        httpd_register_uri_handler(server, &api_rooms_get_uri);
        httpd_register_uri_handler(server, &api_rooms_post_uri);
        httpd_register_uri_handler(server, &api_mqtt_config_get_uri);
        httpd_register_uri_handler(server, &api_mqtt_config_post_uri);
        httpd_register_uri_handler(server, &api_mqtt_test_uri);
        httpd_register_uri_handler(server, &api_wizard_finish_uri);
        httpd_register_uri_handler(server, &api_setup_password_uri);
        httpd_register_uri_handler(server, &api_config_export_uri);
        httpd_register_uri_handler(server, &api_config_import_uri);
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
        DNS_SERVER_CONFIG_SINGLE("*", "WIFI_AP_DEF");
    start_dns_server(&config);

    ESP_LOGI(
        TAG_WIFI, "Provisioning AP ready - connect to '%s' and open any website",
        EXAMPLE_ESP_WIFI_SSID);
}

/* ===================== MQTT / Home Assistant ===================== */

typedef struct {
    char topic[128];
    char data[64];
} mqtt_command_t;
static QueueHandle_t MqttCommandQueue;

static void mqtt_publish_discovery(void)
{
    if (!MqttClient || !MqttConnected || !HaDiscoveryEnabled) {
        return;
    }
    char *buf = malloc(2048);
    if (!buf) {
        return;
    }
    char avail_topic[96];
    snprintf(avail_topic, sizeof(avail_topic), "%s/status", MqttTopicBase);

    /* System Power switch */
    snprintf(
        buf, 2048,
        "{\"name\":\"System Power\",\"unique_id\":\"%s_system_power\","
        "\"availability_topic\":\"%s\",\"state_topic\":\"%s/system_power/state\","
        "\"command_topic\":\"%s/system_power/set\","
        "\"payload_on\":\"ON\",\"payload_off\":\"OFF\",\"device_class\":\"switch\","
        "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
        "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
        HaDeviceId, avail_topic, MqttTopicBase, MqttTopicBase,
        HaDeviceId, HaDeviceName);
    char disc_top[160];
    snprintf(disc_top, sizeof(disc_top), "homeassistant/switch/%s_system_power/config", HaDeviceId);
    esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);

    /* Boost mode select */
    snprintf(
        buf, 2048,
        "{\"name\":\"Boost Mode\",\"unique_id\":\"%s_boost\","
        "\"availability_topic\":\"%s\",\"state_topic\":\"%s/boost/state\","
        "\"command_topic\":\"%s/boost/set\","
        "\"options\":[\"auto\",\"heat\",\"cool\"],"
        "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
        "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
        HaDeviceId, avail_topic, MqttTopicBase, MqttTopicBase,
        HaDeviceId, HaDeviceName);
    snprintf(disc_top, sizeof(disc_top), "homeassistant/select/%s_boost/config", HaDeviceId);
    esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);

    /* Boost auto-revert timeout */
    snprintf(
        buf, 2048,
        "{\"name\":\"Boost Timeout\",\"unique_id\":\"%s_boost_timeout\","
        "\"availability_topic\":\"%s\",\"state_topic\":\"%s/boost_timeout/state\","
        "\"command_topic\":\"%s/boost_timeout/set\","
        "\"min\":0,\"max\":%d,\"step\":5,\"unit_of_measurement\":\"min\","
        "\"entity_category\":\"config\","
        "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
        "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
        HaDeviceId, avail_topic, MqttTopicBase, MqttTopicBase,
        BOOST_TIMEOUT_MAX_MIN, HaDeviceId, HaDeviceName);
    snprintf(disc_top, sizeof(disc_top), "homeassistant/number/%s_boost_timeout/config", HaDeviceId);
    esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);

    snprintf(
        buf, 2048,
        "{\"name\":\"Boost Remaining\",\"unique_id\":\"%s_boost_remaining\","
        "\"availability_topic\":\"%s\",\"state_topic\":\"%s/boost_remaining/state\","
        "\"unit_of_measurement\":\"min\",\"entity_category\":\"diagnostic\","
        "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
        "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
        HaDeviceId, avail_topic, MqttTopicBase, HaDeviceId, HaDeviceName);
    snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_boost_remaining/config", HaDeviceId);
    esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);

    /* Per-room native climate entities */
    for (size_t i = 0; i < RoomCount; i++) {
        if (!Rooms[i].active) continue;

        /* Clean up any old loose entity discovery topics from broker */
        snprintf(disc_top, sizeof(disc_top), "homeassistant/switch/%s_room%u_power/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/number/%s_room%u_setpoint/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_room%u_temp/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_room%u_sa/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_room%u_out/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_room%u_req/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);

        /* Publish the single native Climate entity */
        snprintf(
            buf, 2048,
            "{\"name\":\"%s\",\"unique_id\":\"%s_room%u\","
            "\"availability_topic\":\"%s\","
            "\"current_temperature_topic\":\"%s/room%u/temperature/state\","
            "\"temperature_state_topic\":\"%s/room%u/setpoint/state\","
            "\"temperature_command_topic\":\"%s/room%u/setpoint/set\","
            "\"mode_state_topic\":\"%s/room%u/mode/state\","
            "\"mode_command_topic\":\"%s/room%u/mode/set\","
            "\"action_topic\":\"%s/room%u/action/state\","
            "\"modes\":[\"off\",\"heat_cool\"],"
            "\"temperature_unit\":\"C\",\"min_temp\":18,\"max_temp\":30,\"temp_step\":0.5,\"precision\":0.1,"
            "\"preset_mode_state_topic\":\"%s/room%u/preset/state\","
            "\"preset_mode_command_topic\":\"%s/room%u/preset/set\","
            "\"preset_modes\":[\"boost_heat\",\"boost_cool\"],"
            "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
            "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
            Rooms[i].name, HaDeviceId, (unsigned)i,
            avail_topic,
            MqttTopicBase, (unsigned)i,
            MqttTopicBase, (unsigned)i,
            MqttTopicBase, (unsigned)i,
            MqttTopicBase, (unsigned)i,
            MqttTopicBase, (unsigned)i,
            MqttTopicBase, (unsigned)i,
            MqttTopicBase, (unsigned)i,
            MqttTopicBase, (unsigned)i,
            HaDeviceId, HaDeviceName);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/climate/%s_room%u/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);
    }

    /* Custom MQTT Points */
    for (size_t i = 0; i < CustomMqttCount; i++) {
        if (!CustomMqttPoints[i].enabled) continue;
        char unique_id[64];
        snprintf(unique_id, sizeof(unique_id), "%s_cust_%u_%u",
                 HaDeviceId, CustomMqttPoints[i].obj_type, (unsigned)CustomMqttPoints[i].instance);
        char state_top[96];
        snprintf(state_top, sizeof(state_top), "%s/custom/%u_%u/state",
                 MqttTopicBase, CustomMqttPoints[i].obj_type, (unsigned)CustomMqttPoints[i].instance);

        snprintf(disc_top, sizeof(disc_top), "homeassistant/%s/%s/config",
                 CustomMqttPoints[i].component, unique_id);

        snprintf(
            buf, 2048,
            "{\"name\":\"%s\",\"unique_id\":\"%s\","
            "\"availability_topic\":\"%s\",\"state_topic\":\"%s\","
            "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
            "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
            CustomMqttPoints[i].name, unique_id, avail_topic, state_top,
            HaDeviceId, HaDeviceName);
        esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);
    }

    /* Health Group Discovery */
    int used_fans = fan_room_count();

    if (HaHealthDiscoveryEnabled) {
        struct { const char *name; const char *key; const char *subtop; const char *unit; const char *dev_cla; } h_discs[] = {
            {"Delivered Thermal Output", "output", "health/output/state", "kW", "power"},
            {"Thermal Output Demand", "required_output", "health/required_output/state", "kW", "power"},
            {"Thermal Output Delivery %", "output_pct", "health/output_pct/state", "%", ""},
            {"Coil Flow % of Design", "flow_pct", "health/flow_pct/state", "%", ""},
            {"Return Air Temperature", "return_air", "health/return_air/state", "°C", "temperature"},
            {"Air Delta Across Coil", "air_delta", "health/air_delta/state", "°C", "temperature_delta"},
            {"FCU Performance Mode", "performance", "health/performance/state", "", ""},
            {"FCU Performance Level", "performance_level", "health/performance_level/state", "", ""},
            {"FCU System Health", "system_health", "health/system_health/state", "", ""},
            {"FCU Diagnostic Status", "diag_status", "health/diag_status/state", "", ""},
            {"FCU Diagnostic Detail", "diag_detail", "health/diag_detail/state", "", ""},
        };
        for (size_t i = 0; i < sizeof(h_discs)/sizeof(h_discs[0]); i++) {
            snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_%s/config", HaDeviceId, h_discs[i].key);
            char dev_cla_json[48] = "";
            if (strlen(h_discs[i].dev_cla) > 0) {
                snprintf(dev_cla_json, sizeof(dev_cla_json), ",\"device_class\":\"%s\"", h_discs[i].dev_cla);
            }
            char unit_json[48] = "";
            if (strlen(h_discs[i].unit) > 0) {
                snprintf(unit_json, sizeof(unit_json), ",\"unit_of_measurement\":\"%s\"", h_discs[i].unit);
            }
            snprintf(
                buf, 2048,
                "{\"name\":\"%s\",\"unique_id\":\"%s_health_%s\","
                "\"availability_topic\":\"%s\",\"state_topic\":\"%s/%s\"%s%s,"
                "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
                "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
                h_discs[i].name, HaDeviceId, h_discs[i].key,
                avail_topic, MqttTopicBase, h_discs[i].subtop,
                unit_json, dev_cla_json,
                HaDeviceId, HaDeviceName);
            esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);
        }

        /* Topics and unique_ids stay numeric (fan%d) so they are stable even if
           a room gets renamed later - only the human-facing "name" changes. sa
           and spd are looped separately because there are more supply-air
           sensors fitted than speed outputs (see HEALTH_FAN_SUPPLY_AIR_COUNT vs
           HEALTH_FAN_SPEED_COUNT above) - a prior version capped both loops at
           the smaller of the two, so a 5th room's supply-air sensor could never
           be discovered in HA regardless of used_fans. */
        for (int f = 1; f <= HEALTH_FAN_SUPPLY_AIR_COUNT; f++) {
            snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_fan%d_sa/config", HaDeviceId, f);
            if (f > used_fans) {
                esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
            } else {
                char room_name[32] = "";
                fan_room_name(f, room_name, sizeof(room_name));
                char room_esc[64];
                json_escape(room_esc, room_name, sizeof(room_esc));
                snprintf(
                    buf, 2048,
                    "{\"name\":\"%s Supply Air Temp\",\"unique_id\":\"%s_health_fan%d_sa\","
                    "\"availability_topic\":\"%s\",\"state_topic\":\"%s/health/fan%d/supply_air/state\","
                    "\"unit_of_measurement\":\"°C\",\"device_class\":\"temperature\","
                    "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
                    "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
                    room_esc, HaDeviceId, f,
                    avail_topic, MqttTopicBase, f,
                    HaDeviceId, HaDeviceName);
                esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);
            }
        }

        for (int f = 1; f <= HEALTH_FAN_SPEED_COUNT; f++) {
            snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_fan%d_spd/config", HaDeviceId, f);
            if (f > used_fans) {
                esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
            } else {
                char room_name[32] = "";
                fan_room_name(f, room_name, sizeof(room_name));
                char room_esc[64];
                json_escape(room_esc, room_name, sizeof(room_esc));
                snprintf(
                    buf, 2048,
                    "{\"name\":\"%s Fan Speed\",\"unique_id\":\"%s_health_fan%d_spd\","
                    "\"availability_topic\":\"%s\",\"state_topic\":\"%s/health/fan%d/speed/state\","
                    "\"unit_of_measurement\":\"%%\","
                    "\"device\":{\"identifiers\":[\"%s\"],\"name\":\"%s\","
                    "\"manufacturer\":\"Delta Controls\",\"model\":\"DAC_1180E-MB\"}}",
                    room_esc, HaDeviceId, f,
                    avail_topic, MqttTopicBase, f,
                    HaDeviceId, HaDeviceName);
                esp_mqtt_client_publish(MqttClient, disc_top, buf, 0, 1, true);
            }
        }
    } else {
        const char *hlth_keys[] = {
            "output", "required_output", "output_pct", "flow_pct", "return_air", "air_delta",
            "performance", "performance_level", "system_health", "diag_status", "diag_detail"
        };
        for (size_t i = 0; i < sizeof(hlth_keys)/sizeof(hlth_keys[0]); i++) {
            snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_%s/config", HaDeviceId, hlth_keys[i]);
            esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        }
        for (int f = 1; f <= HEALTH_FAN_SUPPLY_AIR_COUNT; f++) {
            snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_fan%d_sa/config", HaDeviceId, f);
            esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
            snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_fan%d_spd/config", HaDeviceId, f);
            esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        }
    }

    free(buf);
}

static void mqtt_unpublish_discovery(void)
{
    if (!MqttClient || !MqttConnected) {
        return;
    }
    char disc_top[160];

    snprintf(disc_top, sizeof(disc_top), "homeassistant/switch/%s_system_power/config", HaDeviceId);
    esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
    snprintf(disc_top, sizeof(disc_top), "homeassistant/select/%s_boost/config", HaDeviceId);
    esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
    snprintf(disc_top, sizeof(disc_top), "homeassistant/number/%s_boost_timeout/config", HaDeviceId);
    esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
    snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_boost_remaining/config", HaDeviceId);
    esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);

    for (size_t i = 0; i < MAX_ROOMS; i++) {
        snprintf(disc_top, sizeof(disc_top), "homeassistant/climate/%s_room%u/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/switch/%s_room%u_power/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/number/%s_room%u_setpoint/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_room%u_temp/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_room%u_sa/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_room%u_out/config", HaDeviceId, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
    }

    for (size_t i = 0; i < CustomMqttCount; i++) {
        char unique_id[64];
        snprintf(unique_id, sizeof(unique_id), "%s_cust_%u_%u",
                 HaDeviceId, CustomMqttPoints[i].obj_type, (unsigned)CustomMqttPoints[i].instance);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/%s/%s/config",
                 CustomMqttPoints[i].component, unique_id);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
    }

    const char *hlth_keys[] = {
        "output", "required_output", "output_pct", "flow_pct", "return_air", "air_delta",
        "performance", "performance_level", "system_health", "diag_status", "diag_detail"
    };
    for (size_t i = 0; i < sizeof(hlth_keys)/sizeof(hlth_keys[0]); i++) {
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_%s/config", HaDeviceId, hlth_keys[i]);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
    }
    for (int f = 1; f <= HEALTH_FAN_SUPPLY_AIR_COUNT; f++) {
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_fan%d_sa/config", HaDeviceId, f);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
        snprintf(disc_top, sizeof(disc_top), "homeassistant/sensor/%s_health_fan%d_spd/config", HaDeviceId, f);
        esp_mqtt_client_publish(MqttClient, disc_top, "", 0, 1, true);
    }

    /* Give the client a moment to flush the publishes over the wire before
     * we tear down MQTT config and reboot. */
    vTaskDelay(pdMS_TO_TICKS(300));
}

static void mqtt_subscribe_commands(void)
{
    char topic[96];
    snprintf(topic, sizeof(topic), "%s/system_power/set", MqttTopicBase);
    esp_mqtt_client_subscribe(MqttClient, topic, 1);
    snprintf(topic, sizeof(topic), "%s/boost/set", MqttTopicBase);
    esp_mqtt_client_subscribe(MqttClient, topic, 1);
    snprintf(topic, sizeof(topic), "%s/boost_timeout/set", MqttTopicBase);
    esp_mqtt_client_subscribe(MqttClient, topic, 1);
    for (size_t i = 0; i < RoomCount; i++) {
        if (!Rooms[i].active) continue;
        snprintf(topic, sizeof(topic), "%s/room%u/setpoint/set", MqttTopicBase, (unsigned)i);
        esp_mqtt_client_subscribe(MqttClient, topic, 1);
        snprintf(topic, sizeof(topic), "%s/room%u/mode/set", MqttTopicBase, (unsigned)i);
        esp_mqtt_client_subscribe(MqttClient, topic, 1);
        snprintf(topic, sizeof(topic), "%s/room%u/preset/set", MqttTopicBase, (unsigned)i);
        esp_mqtt_client_subscribe(MqttClient, topic, 1);
        snprintf(topic, sizeof(topic), "%s/room%u/power/set", MqttTopicBase, (unsigned)i);
        esp_mqtt_client_subscribe(MqttClient, topic, 1);
    }
}

static const char *boost_preset_name(unsigned mode)
{
    if (mode == BOOST_MODE_FULL_HEATING) {
        return "boost_heat";
    }
    if (mode == BOOST_MODE_FULL_COOLING) {
        return "boost_cool";
    }
    return "none";
}

static const char *boost_select_name(unsigned mode)
{
    if (mode == BOOST_MODE_FULL_HEATING) {
        return "heat";
    }
    if (mode == BOOST_MODE_FULL_COOLING) {
        return "cool";
    }
    return "auto";
}

static void mqtt_publish_boost_state(unsigned mode)
{
    if (!MqttClient || !MqttConnected) return;
    char topic[96];
    snprintf(topic, sizeof(topic), "%s/boost/state", MqttTopicBase);
    esp_mqtt_client_publish(MqttClient, topic, boost_select_name(mode), 0, 1, true);

    const char *preset = boost_preset_name(mode);
    for (size_t i = 0; i < RoomCount; i++) {
        if (!Rooms[i].active) continue;
        snprintf(topic, sizeof(topic), "%s/room%u/preset/state", MqttTopicBase, (unsigned)i);
        esp_mqtt_client_publish(MqttClient, topic, preset, 0, 1, true);
    }
    char remaining[8];
    snprintf(remaining, sizeof(remaining), "%d", boost_remaining_minutes());
    snprintf(topic, sizeof(topic), "%s/boost_remaining/state", MqttTopicBase);
    esp_mqtt_client_publish(MqttClient, topic, remaining, 0, 1, true);

    char timeout[8];
    snprintf(timeout, sizeof(timeout), "%u", (unsigned)BoostTimeoutMinutes);
    snprintf(topic, sizeof(topic), "%s/boost_timeout/state", MqttTopicBase);
    esp_mqtt_client_publish(MqttClient, topic, timeout, 0, 1, true);
}

static bool boost_apply(unsigned mode, bool self_initiated)
{
    if (!BacnetReady) {
        return false;
    }
    bool ok = write_msv_property(OBJECT_MULTI_STATE_VALUE, BOOST_INSTANCE, PROP_PRESENT_VALUE, mode);

    unsigned actual = mode;
    if (!read_msv_property(OBJECT_MULTI_STATE_VALUE, BOOST_INSTANCE, PROP_PRESENT_VALUE, &actual)) {
        actual = mode;
    }
    if (actual == BOOST_MODE_AUTO) {
        BoostSelfInitiated = false;
        BoostDeadlineUs = 0;
    } else if (self_initiated) {
        BoostSelfInitiated = true;
        BoostDeadlineUs = BoostTimeoutMinutes > 0
            ? esp_timer_get_time() + (int64_t)BoostTimeoutMinutes * 60000000LL
            : 0;
    }
    mqtt_publish_boost_state(actual);
    return ok;
}

static void mqtt_republish_bool(uint32_t instance, const char *state_topic, const char *on_val, const char *off_val)
{
    bool value;
    if (BacnetReady && read_bool_property(OBJECT_BINARY_VALUE, instance, PROP_PRESENT_VALUE, &value)) {
        if (MqttClient && MqttConnected) {
            esp_mqtt_client_publish(MqttClient, state_topic, value ? on_val : off_val, 0, 1, true);
        }
    }
}

#define THERMAL_OUTPUT_DEADBAND_KW 0.05f

static const char *hvac_action_for(float current_output)
{
    if (current_output < -THERMAL_OUTPUT_DEADBAND_KW) {
        return "cooling";
    }
    if (current_output > THERMAL_OUTPUT_DEADBAND_KW) {
        return "heating";
    }
    return "idle";
}

static void mqtt_publish_room_action(size_t room_idx)
{
    char topic[96];
    snprintf(topic, sizeof(topic), "%s/room%u/action/state", MqttTopicBase, (unsigned)room_idx);

    bool power;
    if (!BacnetReady ||
        !read_bool_property(
            OBJECT_BINARY_VALUE, Rooms[room_idx].power_instance, PROP_PRESENT_VALUE, &power)) {
        return;
    }
    if (!power) {
        if (MqttClient && MqttConnected) {
            esp_mqtt_client_publish(MqttClient, topic, "off", 0, 1, true);
        }
        return;
    }
    float current_output;
    if (read_real_property(
            OBJECT_ANALOG_VALUE, Rooms[room_idx].current_output_instance, PROP_PRESENT_VALUE,
            &current_output)) {
        if (MqttClient && MqttConnected) {
            esp_mqtt_client_publish(MqttClient, topic, hvac_action_for(current_output), 0, 1, true);
        }
    }
}

static void mqtt_republish_real(uint32_t instance, const char *state_topic)
{
    float value;
    if (BacnetReady && read_real_property(OBJECT_ANALOG_VALUE, instance, PROP_PRESENT_VALUE, &value)) {
        if (MqttClient && MqttConnected) {
            char payload[16];
            snprintf(payload, sizeof(payload), "%.1f", value);
            esp_mqtt_client_publish(MqttClient, state_topic, payload, 0, 1, true);
        }
    }
}

static void mqtt_handle_command(const char *topic, const char *data)
{
    char match_topic[96];
    snprintf(match_topic, sizeof(match_topic), "%s/system_power/set", MqttTopicBase);
    if (strcmp(topic, match_topic) == 0) {
        bool on = strcasecmp(data, "ON") == 0 || strcasecmp(data, "1") == 0 || strcasecmp(data, "true") == 0;
        if (BacnetReady) {
            write_bool_property(OBJECT_BINARY_VALUE, SYS_POWER_WRITE_INSTANCE, PROP_PRESENT_VALUE, on);
        }
        snprintf(match_topic, sizeof(match_topic), "%s/system_power/state", MqttTopicBase);
        mqtt_republish_bool(SYS_POWER_READBACK_INSTANCE, match_topic, "ON", "OFF");
        return;
    }
    snprintf(match_topic, sizeof(match_topic), "%s/boost/set", MqttTopicBase);
    if (strcmp(topic, match_topic) == 0) {
        unsigned mode;
        if (strcmp(data, "auto") == 0) {
            mode = BOOST_MODE_AUTO;
        } else if (strcmp(data, "heat") == 0) {
            mode = BOOST_MODE_FULL_HEATING;
        } else if (strcmp(data, "cool") == 0) {
            mode = BOOST_MODE_FULL_COOLING;
        } else {
            return;
        }
        boost_apply(mode, true);
        return;
    }
    snprintf(match_topic, sizeof(match_topic), "%s/boost_timeout/set", MqttTopicBase);
    if (strcmp(topic, match_topic) == 0) {
        int minutes = atoi(data);
        if (minutes < 0 || minutes > BOOST_TIMEOUT_MAX_MIN) {
            return;
        }
        BoostTimeoutMinutes = (uint16_t)minutes;
        app_config_save();
        if (BoostSelfInitiated && BoostDeadlineUs != 0) {
            BoostDeadlineUs = BoostTimeoutMinutes > 0
                ? esp_timer_get_time() + (int64_t)BoostTimeoutMinutes * 60000000LL
                : 0;
        }
        unsigned actual;
        if (BacnetReady &&
            read_msv_property(OBJECT_MULTI_STATE_VALUE, BOOST_INSTANCE, PROP_PRESENT_VALUE, &actual)) {
            mqtt_publish_boost_state(actual);
        }
        return;
    }
    for (size_t i = 0; i < RoomCount; i++) {
        if (!Rooms[i].active) continue;
        char setpoint_topic[96];
        char mode_topic[96];
        char power_topic[96];
        char preset_topic[96];
        char setpoint_state_topic[96];
        char mode_state_topic[96];
        char power_state_topic[96];

        snprintf(setpoint_topic, sizeof(setpoint_topic), "%s/room%u/setpoint/set", MqttTopicBase, (unsigned)i);
        snprintf(mode_topic, sizeof(mode_topic), "%s/room%u/mode/set", MqttTopicBase, (unsigned)i);
        snprintf(power_topic, sizeof(power_topic), "%s/room%u/power/set", MqttTopicBase, (unsigned)i);
        snprintf(preset_topic, sizeof(preset_topic), "%s/room%u/preset/set", MqttTopicBase, (unsigned)i);

        if (strcmp(topic, preset_topic) == 0) {
            unsigned mode;
            if (strcmp(data, "boost_heat") == 0) {
                mode = BOOST_MODE_FULL_HEATING;
            } else if (strcmp(data, "boost_cool") == 0) {
                mode = BOOST_MODE_FULL_COOLING;
            } else {
                mode = BOOST_MODE_AUTO;
            }
            boost_apply(mode, true);
            return;
        }
        snprintf(setpoint_state_topic, sizeof(setpoint_state_topic), "%s/room%u/setpoint/state", MqttTopicBase, (unsigned)i);
        snprintf(mode_state_topic, sizeof(mode_state_topic), "%s/room%u/mode/state", MqttTopicBase, (unsigned)i);
        snprintf(power_state_topic, sizeof(power_state_topic), "%s/room%u/power/state", MqttTopicBase, (unsigned)i);

        if (strcmp(topic, setpoint_topic) == 0) {
            float value = strtof(data, NULL);
            if (value < MIN_SETPOINT_C) {
                value = MIN_SETPOINT_C;
            }
            if (value > 30.0f) {
                value = 30.0f;
            }
            if (BacnetReady) {
                write_real_property(
                    OBJECT_ANALOG_VALUE, Rooms[i].setpoint_instance, PROP_PRESENT_VALUE, value);
            }
            mqtt_republish_real(Rooms[i].setpoint_instance, setpoint_state_topic);
            return;
        }
        if (strcmp(topic, mode_topic) == 0) {
            bool on = (strcasecmp(data, "off") != 0 && strcasecmp(data, "0") != 0 && strcasecmp(data, "false") != 0);
            if (BacnetReady) {
                write_bool_property(OBJECT_BINARY_VALUE, Rooms[i].power_instance, PROP_PRESENT_VALUE, on);
            }
            mqtt_republish_bool(Rooms[i].power_instance, mode_state_topic, "heat_cool", "off");
            mqtt_republish_bool(Rooms[i].power_instance, power_state_topic, "ON", "OFF");
            mqtt_publish_room_action(i);
            return;
        }
        if (strcmp(topic, power_topic) == 0) {
            bool on = (strcasecmp(data, "ON") == 0 || strcasecmp(data, "1") == 0 || strcasecmp(data, "true") == 0 || strcasecmp(data, "on") == 0);
            if (BacnetReady) {
                write_bool_property(OBJECT_BINARY_VALUE, Rooms[i].power_instance, PROP_PRESENT_VALUE, on);
            }
            mqtt_republish_bool(Rooms[i].power_instance, power_state_topic, "ON", "OFF");
            mqtt_republish_bool(Rooms[i].power_instance, mode_state_topic, "heat_cool", "off");
            mqtt_publish_room_action(i);
            return;
        }
    }
}

static void mqtt_event_handler(
    void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
    (void)handler_args;
    (void)base;
    esp_mqtt_event_handle_t event = event_data;

    switch (event_id) {
        case MQTT_EVENT_CONNECTED:
            ESP_LOGI(TAG_WIFI, "MQTT connected to broker");
            MqttConnected = true;
            {
                char st_top[96];
                snprintf(st_top, sizeof(st_top), "%s/status", MqttTopicBase);
                esp_mqtt_client_publish(MqttClient, st_top, "online", 0, 1, true);
            }
            DiscoveryNeedsBacnetRefresh = true;
            DiscoveryPublishedOnce = false;
            mqtt_subscribe_commands();
            break;
        case MQTT_EVENT_DISCONNECTED:
            ESP_LOGW(TAG_WIFI, "MQTT disconnected");
            MqttConnected = false;
            break;
        case MQTT_EVENT_DATA: {
            mqtt_command_t cmd = {0};
            size_t topic_len = (size_t)event->topic_len < sizeof(cmd.topic) - 1
                ? (size_t)event->topic_len : sizeof(cmd.topic) - 1;
            memcpy(cmd.topic, event->topic, topic_len);
            size_t data_len = (size_t)event->data_len < sizeof(cmd.data) - 1
                ? (size_t)event->data_len : sizeof(cmd.data) - 1;
            memcpy(cmd.data, event->data, data_len);
            xQueueSend(MqttCommandQueue, &cmd, 0);
            break;
        }
        default:
            break;
    }
}

static void mqtt_app_restart(void)
{
    if (MqttClient) {
        esp_mqtt_client_stop(MqttClient);
        esp_mqtt_client_destroy(MqttClient);
        MqttClient = NULL;
        MqttConnected = false;
    }
    if (strlen(MqttBrokerHost) > 0) {
        char uri[128];
        snprintf(uri, sizeof(uri), "mqtt://%s:%u", MqttBrokerHost, (unsigned)MqttBrokerPort);
        char will_topic[96];
        snprintf(will_topic, sizeof(will_topic), "%s/status", MqttTopicBase);

        /* Reverted to 24576 - a shrink to 8192 here caused a hard, unrecoverable
           crash in the field (device stopped responding entirely, needed a
           power cycle, then failed again repeatedly). This build has
           CONFIG_COMPILER_STACK_CHECK_MODE_NONE and only FreeRTOS's weaker
           canary-based overflow check (not the pointer/watchpoint check), so
           an actual overflow of a too-small task stack can corrupt adjacent
           memory rather than fail cleanly - unlike a plain failed allocation
           (see spawn_task()), which is safe precisely because nothing runs on
           a stack that was never handed out. Without serial access to this
           device there is no way to confirm a smaller number is actually safe,
           so this stays at the one value already known not to corrupt
           anything - it only ever failed to allocate, gracefully (logged
           below), which is a real but non-fatal gap versus a bricked device. */
        esp_mqtt_client_config_t mqtt_cfg = {
            .broker.address.uri = uri,
            .session.last_will.topic = will_topic,
            .session.last_will.msg = "offline",
            .session.last_will.msg_len = 0,
            .session.last_will.qos = 1,
            .session.last_will.retain = true,
            .task.stack_size = 24576,
        };
        if (strlen(MqttBrokerUser) > 0) {
            mqtt_cfg.credentials.username = MqttBrokerUser;
            mqtt_cfg.credentials.authentication.password = MqttBrokerPass;
        }
        MqttClient = esp_mqtt_client_init(&mqtt_cfg);
        if (!MqttClient) {
            ESP_LOGE(
                TAG_WIFI,
                "esp_mqtt_client_init FAILED - free internal heap %u, largest free block %u",
                (unsigned)heap_caps_get_free_size(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL),
                (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL));
            return;
        }
        esp_mqtt_client_register_event(MqttClient, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
        if (esp_mqtt_client_start(MqttClient) != ESP_OK) {
            ESP_LOGE(
                TAG_WIFI,
                "esp_mqtt_client_start FAILED - free internal heap %u, largest free block %u",
                (unsigned)heap_caps_get_free_size(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL),
                (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT | MALLOC_CAP_INTERNAL));
        }
    }
}

/* mqtt_worker_task is spawned from app_main(), early and unconditionally -
   see the comment there for why. It sits idle until MqttConnected/BacnetReady
   go true, so starting it before MQTT or even WiFi exists is safe; the queue
   just needs to exist by the time this runs. */
static void mqtt_app_start(void)
{
    mqtt_app_restart();
}

/* Does two jobs that used to be two separate 24KB-stack tasks
   (mqtt_command_task and mqtt_state_task): draining MqttCommandQueue (BACnet
   writes triggered from Home Assistant) and the 20-second periodic state/
   discovery publish below. Both were idle almost all the time - one blocked
   on an empty queue, the other slept 20s at a stretch - so merging them frees
   a whole 24KB stack's worth of contention for the boot-time heap, which is
   what let esp-mqtt's own client task fail to start in the first place (see
   the stack-size comment in mqtt_app_restart). The 200ms queue-receive
   timeout keeps command latency low without a dedicated task for it. */
static void mqtt_state_task(void *arg)
{
    (void)arg;
    TickType_t last_publish_tick = 0;
    for (;;) {
        mqtt_command_t cmd;
        while (MqttCommandQueue && xQueueReceive(MqttCommandQueue, &cmd, pdMS_TO_TICKS(200)) == pdTRUE) {
            mqtt_handle_command(cmd.topic, cmd.data);
        }
        TickType_t now = xTaskGetTickCount();
        if ((now - last_publish_tick) < pdMS_TO_TICKS(20000)) {
            continue;
        }
        last_publish_tick = now;

        if (MqttConnected) {
            /* Publish once on connect, then once more when BACnet comes up so
               the fan list gets trimmed to the real channel count. Do NOT keep
               republishing the whole retained discovery set every 20s while
               BACnet is down: that pushes dozens of retained QoS-1 messages
               into the MQTT outbox on every pass, for no benefit, and the heap
               it churns is the same heap the big task stacks are drawn from. */
            if (DiscoveryNeedsBacnetRefresh &&
                (!DiscoveryPublishedOnce || BacnetReady)) {
                mqtt_publish_discovery();
                DiscoveryPublishedOnce = true;
                if (BacnetReady) {
                    DiscoveryNeedsBacnetRefresh = false;
                }
            }
        }
        if (MqttConnected && BacnetReady) {
            bool sys_power;
            if (read_bool_property(
                    OBJECT_BINARY_VALUE, SYS_POWER_READBACK_INSTANCE, PROP_PRESENT_VALUE, &sys_power)) {
                char st_top[96];
                snprintf(st_top, sizeof(st_top), "%s/system_power/state", MqttTopicBase);
                esp_mqtt_client_publish(
                    MqttClient, st_top, sys_power ? "ON" : "OFF", 0, 1,
                    true);
            }

            unsigned boost_mode;
            if (read_msv_property(
                    OBJECT_MULTI_STATE_VALUE, BOOST_INSTANCE, PROP_PRESENT_VALUE, &boost_mode)) {
                if (boost_mode == BOOST_MODE_AUTO) {
                    BoostSelfInitiated = false;
                    BoostDeadlineUs = 0;
                } else if (BoostTimeoutMinutes > 0 &&
                           (BoostSelfInitiated || BoostRevertExternal)) {
                    if (BoostDeadlineUs == 0) {
                        BoostDeadlineUs =
                            esp_timer_get_time() + (int64_t)BoostTimeoutMinutes * 60000000LL;
                    } else if (esp_timer_get_time() >= BoostDeadlineUs) {
                        ESP_LOGI(
                            TAG_WIFI, "Boost timeout (%u min) elapsed - reverting to Auto",
                            (unsigned)BoostTimeoutMinutes);
                        boost_apply(BOOST_MODE_AUTO, false);
                        boost_mode = BOOST_MODE_AUTO;
                    }
                }
                mqtt_publish_boost_state(boost_mode);
            }

            for (size_t i = 0; i < RoomCount; i++) {
                if (!Rooms[i].active) continue;
                char topic[96];
                float temperature;
                if (read_real_property(
                        OBJECT_ANALOG_VALUE, Rooms[i].temperature_instance, PROP_PRESENT_VALUE,
                        &temperature)) {
                    char payload[16];
                    snprintf(payload, sizeof(payload), "%.1f", temperature);
                    snprintf(topic, sizeof(topic), "%s/room%u/temperature/state", MqttTopicBase, (unsigned)i);
                    esp_mqtt_client_publish(MqttClient, topic, payload, 0, 1, true);
                }
                float setpoint;
                if (read_real_property(
                        OBJECT_ANALOG_VALUE, Rooms[i].setpoint_instance, PROP_PRESENT_VALUE, &setpoint)) {
                    char payload[16];
                    snprintf(payload, sizeof(payload), "%.1f", setpoint);
                    snprintf(topic, sizeof(topic), "%s/room%u/setpoint/state", MqttTopicBase, (unsigned)i);
                    esp_mqtt_client_publish(MqttClient, topic, payload, 0, 1, true);
                }
                bool power;
                if (read_bool_property(
                        OBJECT_BINARY_VALUE, Rooms[i].power_instance, PROP_PRESENT_VALUE, &power)) {
                    snprintf(topic, sizeof(topic), "%s/room%u/mode/state", MqttTopicBase, (unsigned)i);
                    esp_mqtt_client_publish(
                        MqttClient, topic, power ? "heat_cool" : "off", 0, 1, true);
                    snprintf(topic, sizeof(topic), "%s/room%u/power/state", MqttTopicBase, (unsigned)i);
                    esp_mqtt_client_publish(
                        MqttClient, topic, power ? "ON" : "OFF", 0, 1, true);
                }
                float sa;
                if (read_real_property(
                        OBJECT_ANALOG_VALUE, Rooms[i].supply_air_instance, PROP_PRESENT_VALUE, &sa)) {
                    char payload[16];
                    snprintf(payload, sizeof(payload), "%.1f", sa);
                    snprintf(topic, sizeof(topic), "%s/room%u/supply_air/state", MqttTopicBase, (unsigned)i);
                    esp_mqtt_client_publish(MqttClient, topic, payload, 0, 1, true);
                }
                float cur_out;
                if (read_real_property(
                        OBJECT_ANALOG_VALUE, Rooms[i].current_output_instance, PROP_PRESENT_VALUE, &cur_out)) {
                    char payload[16];
                    snprintf(payload, sizeof(payload), "%.2f", cur_out);
                    snprintf(topic, sizeof(topic), "%s/room%u/current_output/state", MqttTopicBase, (unsigned)i);
                    esp_mqtt_client_publish(MqttClient, topic, payload, 0, 1, true);
                }
                float req_out;
                if (read_real_property(
                        OBJECT_ANALOG_VALUE, Rooms[i].required_output_instance, PROP_PRESENT_VALUE, &req_out)) {
                    char payload[16];
                    snprintf(payload, sizeof(payload), "%.2f", req_out);
                    snprintf(topic, sizeof(topic), "%s/room%u/required_output/state", MqttTopicBase, (unsigned)i);
                    esp_mqtt_client_publish(MqttClient, topic, payload, 0, 1, true);
                }
                mqtt_publish_room_action(i);
            }

            for (size_t i = 0; i < CustomMqttCount; i++) {
                if (!CustomMqttPoints[i].enabled) continue;
                char state_top[96];
                snprintf(state_top, sizeof(state_top), "%s/custom/%u_%u/state",
                         MqttTopicBase, CustomMqttPoints[i].obj_type, (unsigned)CustomMqttPoints[i].instance);
                if (CustomMqttPoints[i].obj_type == OBJECT_ANALOG_VALUE ||
                    CustomMqttPoints[i].obj_type == OBJECT_ANALOG_INPUT ||
                    CustomMqttPoints[i].obj_type == OBJECT_ANALOG_OUTPUT) {
                    float val = 0.0f;
                    if (read_real_property((BACNET_OBJECT_TYPE)CustomMqttPoints[i].obj_type,
                                           CustomMqttPoints[i].instance, PROP_PRESENT_VALUE, &val)) {
                        char payload[16];
                        snprintf(payload, sizeof(payload), "%.2f", val);
                        esp_mqtt_client_publish(MqttClient, state_top, payload, 0, 1, true);
                    }
                } else if (CustomMqttPoints[i].obj_type == OBJECT_BINARY_VALUE ||
                           CustomMqttPoints[i].obj_type == OBJECT_BINARY_INPUT ||
                           CustomMqttPoints[i].obj_type == OBJECT_BINARY_OUTPUT) {
                    bool val = false;
                    if (read_bool_property((BACNET_OBJECT_TYPE)CustomMqttPoints[i].obj_type,
                                           CustomMqttPoints[i].instance, PROP_PRESENT_VALUE, &val)) {
                        esp_mqtt_client_publish(MqttClient, state_top, val ? "ON" : "OFF", 0, 1, true);
                    }
                }
            }

            if (HaHealthDiscoveryEnabled) {
                float h_out = 0.0f, h_req = 0.0f, h_fp = 0.0f, h_ret = 0.0f;
                bool h_out_ok = read_real_property(OBJECT_ANALOG_VALUE, HEALTH_COOLING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &h_out);
                bool h_req_ok = read_real_property(OBJECT_ANALOG_VALUE, HEALTH_REQUIRED_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &h_req);
                bool h_fp_ok = read_real_property(OBJECT_ANALOG_VALUE, HEALTH_FLOW_DESIGN_PCT_INSTANCE, PROP_PRESENT_VALUE, &h_fp);
                bool h_ret_ok = read_real_property(OBJECT_ANALOG_INPUT, HEALTH_RETURN_AIR_INSTANCE, PROP_PRESENT_VALUE, &h_ret);

                float h_hout = 0.0f, h_hreq = 0.0f;
                bool h_hout_ok = read_real_property(OBJECT_ANALOG_VALUE, HEALTH_HEATING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &h_hout);
                bool h_hreq_ok = read_real_property(OBJECT_ANALOG_VALUE, HEALTH_REQUIRED_HEATING_OUTPUT_INSTANCE, PROP_PRESENT_VALUE, &h_hreq);

                bool is_heating = (h_hout_ok && h_hreq_ok && (h_hreq > h_req || h_hout > h_out) && h_hreq > 0.05f);
                float cur_out = is_heating ? (h_hout_ok ? h_hout : 0.0f) : (h_out_ok ? h_out : 0.0f);
                float cur_req = is_heating ? (h_hreq_ok ? h_hreq : 0.0f) : (h_req_ok ? h_req : 0.0f);

                char htop[128], hpayload[320];
                if ((is_heating && h_hout_ok) || (!is_heating && h_out_ok)) {
                    snprintf(hpayload, sizeof(hpayload), "%.2f", cur_out);
                    snprintf(htop, sizeof(htop), "%s/health/output/state", MqttTopicBase);
                    esp_mqtt_client_publish(MqttClient, htop, hpayload, 0, 1, true);
                }
                if ((is_heating && h_hreq_ok) || (!is_heating && h_req_ok)) {
                    snprintf(hpayload, sizeof(hpayload), "%.2f", cur_req);
                    snprintf(htop, sizeof(htop), "%s/health/required_output/state", MqttTopicBase);
                    esp_mqtt_client_publish(MqttClient, htop, hpayload, 0, 1, true);
                }
                if (cur_req > 0.1f) {
                    snprintf(hpayload, sizeof(hpayload), "%.1f", (cur_out / cur_req) * 100.0f);
                    snprintf(htop, sizeof(htop), "%s/health/output_pct/state", MqttTopicBase);
                    esp_mqtt_client_publish(MqttClient, htop, hpayload, 0, 1, true);
                }
                if (h_fp_ok) {
                    snprintf(hpayload, sizeof(hpayload), "%.1f", h_fp);
                    snprintf(htop, sizeof(htop), "%s/health/flow_pct/state", MqttTopicBase);
                    esp_mqtt_client_publish(MqttClient, htop, hpayload, 0, 1, true);
                }
                if (h_ret_ok && temp_is_plausible(h_ret)) {
                    snprintf(hpayload, sizeof(hpayload), "%.1f", h_ret);
                    snprintf(htop, sizeof(htop), "%s/health/return_air/state", MqttTopicBase);
                    esp_mqtt_client_publish(MqttClient, htop, hpayload, 0, 1, true);
                }

                /* Delta T & Diagnostics */
                float sa_sum = 0.0f;
                int sa_cnt = 0;
                for (size_t i = 0; i < RoomCount; i++) {
                    if (!Rooms[i].active) continue;
                    float r_sa = 0.0f;
                    if (read_real_property(OBJECT_ANALOG_VALUE, Rooms[i].supply_air_instance, PROP_PRESENT_VALUE, &r_sa) && temp_is_plausible(r_sa)) {
                        sa_sum += r_sa;
                        sa_cnt++;
                    }
                }

                float valve_sig = 0.0f;
                bool valve_sig_ok = is_heating
                    ? read_real_property(OBJECT_ANALOG_OUTPUT, HEALTH_HEATING_VALVE_SIGNAL_INSTANCE, PROP_PRESENT_VALUE, &valve_sig)
                    : read_real_property(OBJECT_ANALOG_OUTPUT, HEALTH_VALVE_SIGNAL_INSTANCE, PROP_PRESENT_VALUE, &valve_sig);

                bool any_alarm_active = false;
                char alarm_label[64] = "";
                bool master_alarm = false;
                if (read_bool_property(OBJECT_BINARY_VALUE, HealthAlarms[0].instance, PROP_PRESENT_VALUE, &master_alarm) && master_alarm) {
                    any_alarm_active = true;
                    snprintf(alarm_label, sizeof(alarm_label), "%s", HealthAlarms[0].label);
                    for (size_t i = 1; i < HEALTH_ALARM_COUNT; i++) {
                        bool sub_act = false;
                        if (read_bool_property(OBJECT_BINARY_VALUE, HealthAlarms[i].instance, PROP_PRESENT_VALUE, &sub_act) && sub_act) {
                            snprintf(alarm_label, sizeof(alarm_label), "%s", HealthAlarms[i].label);
                            break;
                        }
                    }
                }

                health_diagnostics_t diag;
                compute_health_diagnostics(
                    is_heating,
                    is_heating ? h_hout_ok : h_out_ok, cur_out,
                    is_heating ? h_hreq_ok : h_req_ok, cur_req,
                    h_fp_ok, h_fp,
                    valve_sig_ok, valve_sig,
                    h_ret_ok, h_ret,
                    sa_cnt > 0, (sa_cnt > 0) ? (sa_sum / sa_cnt) : 0.0f,
                    any_alarm_active, alarm_label,
                    &diag);

                if (diag.delta_t_valid) {
                    snprintf(hpayload, sizeof(hpayload), "%.1f", diag.delta_t);
                    snprintf(htop, sizeof(htop), "%s/health/air_delta/state", MqttTopicBase);
                    esp_mqtt_client_publish(MqttClient, htop, hpayload, 0, 1, true);
                }

                snprintf(htop, sizeof(htop), "%s/health/performance/state", MqttTopicBase);
                esp_mqtt_client_publish(MqttClient, htop, diag.performance, 0, 1, true);

                snprintf(htop, sizeof(htop), "%s/health/performance_level/state", MqttTopicBase);
                esp_mqtt_client_publish(MqttClient, htop, diag.performance_level, 0, 1, true);

                snprintf(htop, sizeof(htop), "%s/health/system_health/state", MqttTopicBase);
                esp_mqtt_client_publish(MqttClient, htop, diag.system_health, 0, 1, true);

                snprintf(htop, sizeof(htop), "%s/health/diag_status/state", MqttTopicBase);
                esp_mqtt_client_publish(MqttClient, htop, diag.diag_status, 0, 1, true);

                snprintf(htop, sizeof(htop), "%s/health/diag_detail/state", MqttTopicBase);
                esp_mqtt_client_publish(MqttClient, htop, diag.diag_detail, 0, 1, true);

                /* Fan health states - only the room-backed channels; see
                   fan_room_count(). Publishing state for a channel HA was never
                   told about via discovery is harmless but wasted traffic. */
                int fcnt = fan_room_count();
                for (int f = 1; f <= fcnt; f++) {
                    float ftemp = 0.0f, fspd = 0.0f;
                    if (read_real_property(OBJECT_ANALOG_INPUT, f, PROP_PRESENT_VALUE, &ftemp) && temp_is_plausible(ftemp)) {
                        snprintf(hpayload, sizeof(hpayload), "%.1f", ftemp);
                        snprintf(htop, sizeof(htop), "%s/health/fan%d/supply_air/state", MqttTopicBase, f);
                        esp_mqtt_client_publish(MqttClient, htop, hpayload, 0, 1, true);
                    }
                    if (f <= HEALTH_FAN_SPEED_COUNT && read_real_property(OBJECT_ANALOG_OUTPUT, f, PROP_PRESENT_VALUE, &fspd)) {
                        snprintf(hpayload, sizeof(hpayload), "%.1f", fspd);
                        snprintf(htop, sizeof(htop), "%s/health/fan%d/speed/state", MqttTopicBase, f);
                        esp_mqtt_client_publish(MqttClient, htop, hpayload, 0, 1, true);
                    }
                }
            }
        }
    }
}

/* ===================== app_main ===================== */

#define MDNS_HOSTNAME "esp-bacnet-bridge"

static void mdns_start_service(void)
{
    if (mdns_init() != ESP_OK) {
        return;
    }
    mdns_hostname_set(MDNS_HOSTNAME);
    mdns_instance_name_set("ESP32 BACnet Bridge");
    mdns_service_add(NULL, "_http", "_tcp", 80, NULL, 0);
    ESP_LOGI(TAG_WIFI, "mDNS ready - reachable at http://%s.local", MDNS_HOSTNAME);
}

void app_main(void)
{
    esp_log_set_vprintf(web_log_vprintf);

    ESP_LOGI("MAIN", "=======================================================");
    ESP_LOGI("MAIN", "  ESP32 BACnet Bridge for Delta DAC-1180E Controllers");
    ESP_LOGI("MAIN", "  Created by Piers Wingfield <piers@wingfield.tech>");
    ESP_LOGI("MAIN", "  Open Source under MIT License · Free for Community Use");
    ESP_LOGI("MAIN", "=======================================================");

    ESP_ERROR_CHECK(nvs_flash_init());

    esp_ota_mark_app_valid_cancel_rollback();

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    target_config_load();
    rooms_config_load();
    mqtt_config_load();
    custom_mqtt_load();
    app_config_load();
    wizard_completed_load();
    ota_password_load();

    BacnetMutex = xSemaphoreCreateMutex();

    /* 24KB, and created first: this task runs bacnet_client_run() inline, so it
       needs the BACnet stack budget, and taking it here - before httpd and the
       MQTT tasks claim theirs - is what guarantees BACnet gets its memory. */
    spawn_task(eth_bringup_task, "eth_bringup", 24576, NULL, 5, NULL);

    /* Same reasoning, same place: mqtt_state_task also needs a 24KB stack, and
       is built to sit idle until MqttConnected/BacnetReady go true, so there is
       no reason to wait for WiFi or MQTT to exist before claiming its memory.
       Waiting is exactly what let it lose the race to esp_http_server + WiFi's
       own allocations once already: this task's xTaskCreate failed with 17KB
       free but the largest contiguous block only 16KB - a silent failure
       before spawn_task() made failures loud.

       This used to be two separate 24KB tasks (mqtt_command_task handled
       MqttCommandQueue on its own). They were merged into one - see the
       comment on mqtt_state_task - specifically because even after moving
       both early, esp-mqtt's own internal client task (spawned later, in
       mqtt_app_restart(), which this file does not control the size of
       reducing without risk - see that comment) was still losing ITS 24KB
       allocation race by ~10KB. Removing one whole 24KB task from the
       boot-time total is what actually closes that gap, rather than moving it
       to yet another task. */
    MqttCommandQueue = xQueueCreate(8, sizeof(mqtt_command_t));
    spawn_task(mqtt_state_task, "mqtt_state", 24576, NULL, 5, NULL);

    char ssid[33] = {0};
    char pass[65] = {0};
    if (load_wifi_credentials(ssid, sizeof(ssid), pass, sizeof(pass))) {
        if (try_connect_sta(ssid, pass)) {
            ESP_LOGI(TAG_WIFI, "Connected to '%s' - provisioning not needed this boot", ssid);
            mdns_start_service();
            start_connected_webserver();
            mqtt_app_start();
            return;
        }
    } else {
        ESP_LOGI(TAG_WIFI, "No saved WiFi credentials found");
    }

    start_provisioning_ap();
}
