# ESP32 BACnet Bridge - Architecture, Technical Intelligence & API Reference

This document serves as the technical reference manual for developers and AI agents working on or extending the `esp-bacnet` firmware. It consolidates all system requirements, hardware specifications, firmware design patterns, discovery lessons learned, and the complete REST API reference.

---

## 1. System Requirements & Design Goals

The primary goal of this project is to provide a robust, 100% local, zero-cloud bridge between residential BACnet/IP HVAC controllers (specifically the Delta Controls DAC-1180E) and modern home automation systems (Home Assistant via MQTT).

### Key Architectural Constraints
1. **Network Isolation**: The building's HVAC controller network (`10.0.3.x`) must remain physically and logically isolated from the resident's home WiFi network (`192.168.x.x`). The bridge achieves this by operating two completely independent `esp_netif` network interfaces.
2. **Zero External Cloud**: No reliance on third-party cloud services, external accounts, or remote servers.
3. **Resilience & Self-Healing**: Hardware brownout monitoring, automatic WiFi auto-reconnect with exponential backoff, watchdog timers, and bootloader app rollback for OTA updates.
4. **App-Free Provisioning**: First-boot captive portal (`192.168.4.1`) and step-by-step Setup Wizard (`/wizard`).

---

## 2. Hardware & Wiring Specifications

### Components
- **Microcontroller**: ESP32-WROOM-32D Development Board with **USB-C port** and 4MB Flash.
- **Ethernet Module**: WIZnet W5500 SPI Ethernet module.
- **Power Supply**: 5V 1A power delivered via USB-C from inside the FCU controller wall enclosure.

### SPI Ethernet Pinout (ESP32 to W5500)
The W5500 module communicates over ESP32 SPI Host 1 (VSPI):

| W5500 Pin | ESP32 GPIO | Description |
|---|---|---|
| **VCC** | 3.3V / 5V | Power input (depends on breakout board regulator) |
| **GND** | GND | Ground |
| **MOSI** | GPIO 23 | SPI Master Out Slave In |
| **MISO** | GPIO 19 | SPI Master In Slave Out |
| **SCLK** | GPIO 18 | SPI Clock (12 MHz) |
| **CS** | GPIO 5 | SPI Chip Select |
| **INT** | GPIO 4 | Interrupt Pin |
| **RST** | GPIO 16 | Dedicated Hardware Reset |

> **Hardware Reset Line**: GPIO 16 is explicitly driven low for 10ms during startup before initializing the SPI driver. Tying W5500 RST passively to 3.3V can cause intermittent SPI init failures during soft reboots.

### Ethernet Topology Note
The Delta DAC-1180E controller features **dual Ethernet ports** (daisy-chain ports), allowing the W5500 patch cable to plug directly into an available port on the controller. Single-port controllers requiring an external Ethernet hub are untested in this setup.

---

## 3. Firmware Architecture & Components

```
                                 ┌─────────────────────────────────┐
                                 │   ESP32 Dual-Interface Core     │
┌──────────────────────────┐     │                                 │     ┌─────────────────────────┐
│ Building FCU Network     │     │   ┌─────────────────────────┐   │     │ Resident Home Network   │
│ Delta DAC-1180E          │<───>│   │  SPI W5500 Ethernet     │   │<───>│ WiFi STA (192.168.x.x) │
│ (10.0.3.16:47808)        │     │   │  (10.0.3.x / static)    │   │     │ & SoftAP (192.168.4.1)  │
└──────────────────────────┘     │   └────────────┬────────────┘   │     └────────────┬────────────┘
                                 │                │                │                  │
                                 │   ┌────────────▼────────────┐   │                  │
                                 │   │ BACnet Client Task      │   │                  │
                                 │   │ (Unicast WhoIs/RP/WP)   │   │                  │
                                 │   └────────────┬────────────┘   │                  │
                                 │                │                │                  │
                                 │   ┌────────────▼────────────┐   │     ┌────────────▼────────────┐
                                 │   │ Shared State & Mutex    │<──┼────>│ HTTP Server (Port 80)   │
                                 │   │ (Rooms / System Power)  │   │     │ & MQTT Client           │
                                 │   └─────────────────────────┘   │     └─────────────────────────┘
                                 └─────────────────────────────────┘
```

### Key Modules
- **`main.c`**: Core orchestrator, HTTP REST handlers, WiFi manager, NVS persistence, MQTT publisher, and HTML embedding.
- **`components/bacnet_client`**: Custom ESP-IDF port of Steve Karg's `bacnet-stack`. Implements `bip_socket_esp_idf.c` using raw lwIP sockets over the W5500 netif interface.
- **`components/ethernet_init`**: Driver initialization for the W5500 SPI interface.
- **`components/dns_server`**: Captive portal DNS redirect server for initial WiFi provisioning.

### Non-Volatile Storage (NVS) Schema
State is saved across reboots using ESP-IDF NVS namespaces:
- `nvs_target`: `ip` (string), `port` (uint16), `dev_id` (uint32).
- `nvs_rooms`: `count` (uint8), `r{i}_name`, `r{i}_en`, `r{i}_sp`, `r{i}_temp`, `r{i}_pwr`, `r{i}_sup`, `r{i}_req`, `r{i}_cur`.
- `nvs_mqtt`: `host`, `port`, `user`, `pass`, `prefix`.
- `nvs_ota`: `password` (string).
- `nvs_wifi`: `ssid`, `password`.

### Dual App Partitions & OTA Rollback
The firmware uses custom partitions (`partitions.csv`):
```csv
# Name,   Type, SubType, Offset,  Size, Flags
nvs,      data, nvs,     0x9000,  0x4000,
otadata,  data, ota,     0xd000,  0x2000,
phy_init, data, phy,     0xf000,  0x1000,
factory,  app,  factory, 0x10000, 0x1D0000,
ota_0,    app,  ota_0,   0x1E0000,0x1D0000,
ota_1,    app,  ota_1,   0x3B0000,0x1D0000,
```
With `CONFIG_BOOTLOADER_APP_ROLLBACK_ENABLE=y`, a newly flashed OTA image must successfully boot and call `esp_ota_mark_app_valid_cancel_rollback()`. If the new image crashes during boot, the bootloader automatically reverts to the previously working image.

---

## 4. Discovery Lessons Learned Along the Way

1. **BACnet Broadcast vs Unicast**:
   - The Delta DAC-1180E controller on this building network **does not answer broadcast `Who-Is` requests** (`255.255.255.255`).
   - Resolution: The client executes a targeted **unicast `Who-Is` sweep** across `10.0.3.1` through `10.0.3.32`.
2. **BACnet Write Target Selection**:
   - Writing to `Room_x Occupancy Status` (`binary-value, 1102`) does *not* switch room power.
   - Differential snapshot diffing proved that writing directly to `Room_x Run Status` (`binary-value, 1101/1201/1301/1401/1501`) successfully turns rooms on/off at priority 16 without needing to alter source-selector objects (`Room_x Run Status Control`).
3. **Port Byte-Order in `bip.c`**:
   - Upstream `bip.c` stores UDP ports in `BACNET_ADDRESS.mac[4:6]` in native host byte order (little-endian on ESP32), while standard helper `bacnet_address_mac_from_ascii()` outputs big-endian.
   - Mixing the two sent requests to port `49338` instead of `47808`. Fixed in `main.c`'s `bind_target_device()`.
4. **Browser Memory & Object Cataloging**:
   - Fetching 430+ BACnet objects in a single JSON payload exceeded micro-controller RAM and caused web browser lockups.
   - Resolution: Implemented chunked paginated scanning (`/api/scan/objects?offset=...`) and on-device catalog pinning (`/api/catalog/pin`).

---

## 5. Developer & AI Agent Guidelines

If you (or an AI coding agent) are extending this codebase:

1. **Modifying HTML Templates**:
   - HTML files in `firmware/bacnet_bridge/main/*.html` are compiled directly into the binary using ESP-IDF CMake `EMBED_FILES`.
   - After editing an HTML file, re-run `idf.py build`.
2. **BACnet Mutex Usage**:
   - All BACnet transactions must acquire `BacnetMutex` before interacting with the stack:
     ```c
     if (xSemaphoreTake(BacnetMutex, pdMS_TO_TICKS(1000))) {
         // Perform BACnet Read/Write
         xSemaphoreGive(BacnetMutex);
     }
     ```
3. **Testing API Changes**:
   - Test endpoints using `curl` or Postman.
   - Use the embedded circular log console at `/health` to observe real-time log messages.

---

## 6. Complete REST API Reference

### System & Health Endpoints

#### `GET /api/status`
Returns general climate system state, boost timers, and per-room statuses.
- **Response**:
  ```json
  {
    "sys_power": true,
    "sys_power_valid": true,
    "boost_mode": 0,
    "boost_valid": true,
    "boost_timeout_minutes": 60,
    "boost_remaining_minutes": 0,
    "rooms": [
      {
        "index": 0,
        "name": "Living Room",
        "enabled": true,
        "setpoint": 21.0,
        "setpoint_valid": true,
        "temperature": 22.5,
        "temperature_valid": true,
        "power": true,
        "power_valid": true,
        "supply_air": 18.0,
        "required_output": 45.0,
        "current_output": 45.0
      }
    ]
  }
  ```

#### `GET /api/health`
Returns live FCU health, thermal duty delivery, coil flow, signed Air Delta across coil, performance ratings, and deterministic system diagnostics:
- **`delta_t`**: Supply air minus return air in °C (negative = cooling drop, positive = heating rise).
- **`performance`**: `"Cooling"`, `"Heating"`, or `"Neutral"`.
- **`performance_level`**: `"Low"`, `"Medium"`, `"High"`, or `"None"`.
- **`system_health`**: `"Good"`, `"Fair"`, or `"Poor"`.
- **`diag_status`**: Concise status description (e.g. `"Water Flow Starved"`, `"Inadvertent Heating (Motor Heat)"`).
- **`diag_detail`**: Contextual explanation and troubleshooting suggestions.

#### `GET /api/network`
Returns WiFi, Ethernet link, BACnet target connection status, uptime, and reset reasons.

#### `GET /api/system/health`
Returns micro-controller system telemetry (free heap, minimum free heap, free 8-bit heap, task list, project metadata).

#### `GET /api/logs`
Returns the recent in-memory circular log buffer.

---

### Control Endpoints

#### `POST /api/system-power`
Turns the master FCU system on or off.
- **Payload**: `{"value": "on"}` or `{"value": "off"}`

#### `POST /api/boost`
Sets the system boost mode.
- **Payload**: `{"value": "auto"}`, `{"value": "heat"}`, or `{"value": "cool"}`

#### `POST /api/boost-timeout`
Configures the boost auto-revert timeout in minutes (0 = disabled).
- **Payload**: `{"value": 60}`

#### `POST /api/room/{idx}/setpoint`
Sets a room's target temperature setpoint in °C.
- **Payload**: `{"value": 21.5}`

#### `POST /api/room/{idx}/power`
Turns a specific room zone on or off.
- **Payload**: `{"value": "on"}` or `{"value": "off"}`

---

### BACnet Object Discovery Endpoints

#### `POST /api/bacnet/discover`
Triggers a BACnet controller discovery scan.
- **Payload for full network sweep**: `{}` (sweeps `10.0.3.1`–`10.0.3.32` over unicast `Who-Is`).
- **Payload for targeted IP probe**: `{"ip": "10.0.3.16"}` (probes a specific IP).
- **Response**:
  ```json
  {
    "ok": true,
    "count": 1,
    "devices": [
      {
        "device_id": 753016,
        "instance": 753016,
        "name": "C305",
        "ip": "10.0.3.16",
        "port": 47808,
        "vendor": "Delta Controls",
        "model": "DAC_1180E-MB"
      }
    ]
  }
  ```

#### `GET /api/scan/objects?offset=0&limit=50`
Scans BACnet objects on the target controller in paginated chunks.

#### `GET /api/catalog`
Returns the cached BACnet object catalog and pinned points.

#### `POST /api/catalog/pin`
Pins or unpins an object in the browser catalog.

---

### Configuration & OTA Endpoints

#### `GET /api/config/export`
Exports complete NVS configuration (WiFi, Target IP, Rooms, MQTT) as a downloadable JSON object.

#### `POST /api/config/import`
Restores device configuration from an uploaded JSON payload.

#### `POST /api/ota`
Accepts binary firmware images for OTA update. Requires HTTP Basic Authentication (`admin:<OtaPassword>`).

---

## Future Architecture Roadmap

1. **Native Apple HomeKit Integration**: Embedded HAP (HomeKit Accessory Protocol) daemon running directly on the ESP32 to allow native pairing with Apple Home without needing Home Assistant or Homebridge.
2. **Native Google Home Integration**: Local Matter / Google Home Local SDK support for direct voice and app control via Google Assistant ecosystem.
3. **Native Amazon Alexa Integration**: Direct local Alexa Smart Home Skill API integration for direct voice control.
