# ESP32 BACnet Bridge for controlling domestic climate systems

Control your home cooling/heating from HomeAssistant via MQTT. Developed for West Hampstead Square Cooling/Heating (Delta DAC-1180E HVAC Controllers)

An open-source, local-only bridge running on an ESP32 micro-controller that connects residential fan coil heating and cooling systems (Delta Controls BACnet/IP) to **Home Assistant (via MQTT)** and a standalone web dashboard.

Originally developed and customized for the ducted heating/cooling systems in the **West Hampstead Square** development in London, but designed to be modular and easily customizable for anyone with similar Delta DAC-1180E HVAC controllers.

---

## System Requirements & Prerequisites

To use this bridge in your home automation setup, you will need:
1. **MQTT Broker**: A running MQTT broker (such as [Eclipse Mosquitto](https://mosquitto.org/) or the Home Assistant Mosquitto add-on) on your local home network.
2. **Control & Automation Platform**: A smart home controller (such as [Home Assistant](https://www.home-assistant.io/)) to control cooling and heating. It should receive auto-discovered MQTT climate entities, display room dashboards, and run automations.

---

## Hardware & Wiring

| Component | Specification | Notes |
|---|---|---|
| **Microcontroller** | ESP32-WROOM-32D Development Board | **USB-C port** model recommended |
| **Ethernet Controller** | WIZnet W5500 SPI Module | Connected directly to the FCU controller |
| **Power Supply** | 5V 1A via USB-C | Can be powered directly inside the wall controller enclosure |
| **Network Connection** | Standard RJ45 Patch Cable | Plugs into the Delta FCU controller's RJ45 Ethernet port |

### Hardware Wiring (ESP32 to W5500)

| W5500 Pin | ESP32 GPIO | Function |
|---|---|---|
| **VCC** | 3V3 / 5V | Power supply (match module rating) |
| **GND** | GND | Ground |
| **MOSI** | GPIO 23 | SPI Master Output / Slave Input |
| **MISO** | GPIO 19 | SPI Master Input / Slave Output |
| **SCLK** | GPIO 18 | SPI Clock |
| **CS** | GPIO 5 | SPI Chip Select |
| **INT** | GPIO 4 | Hardware Interrupt |
| **RST** | GPIO 16 | Dedicated Hardware Reset |

> **Note on Ethernet Topologies**: This bridge has been tested directly connected to a Delta DAC-1180E controller featuring **dual network ports** (daisy-chain ports). If your controller model has only a single network port, an external Ethernet switch/hub may be required; this single-port hub configuration has not been explicitly tested in this installation.

---

## System Discovery Facts & System Architecture

This project was built from deep on-wire discovery and protocol analysis of the physical Delta Controls DAC-1180E system. Key discovery findings that informed the firmware architecture include:

1. **Strategy Architecture**:
   - Controller Model: Delta Controls `DAC_1180E-MB`.
   - Firmware Strategy: `"Multiroom for DAC1180E V1.0 (West Hampstead Square)"`.
   - Manages up to **5 distinct room zones** (`Room_A` through `Room_E`), each containing setpoints, ambient temperatures, supply air temperatures, fan heating/cooling duty, and occupancy signals.
2. **BACnet/IP Unicast Requirement**:
   - The panel does **not respond to broadcast `Who-Is` requests** (`255.255.255.255`).
   - The ESP32 firmware implements a fast **unicast `Who-Is` sweep** across `10.0.3.1` to `10.0.3.32` on the controller subnet, complemented by a direct IP probe option in the web Setup Wizard.
3. **Safe Control Write Targets & Priority array**:
   - **System Power**: Controlled via `BMS Run Signal` (`binary-value, 13`). Readback uses `FCU Run Status` (`binary-value, 1`).
   - **Room Power**: Controlled via `Room_x Run Status` (`binary-value, 1101/1201/1301/1401/1501`).
   - **Room Setpoints**: Written via `Room_x Setpoint` (`analog-value, 1100/1200/1300/1400/1500`).
   - Differential snapshot diffing confirmed that room writes apply directly at priority 16 without needing to fight internal controller programs or override selector objects.
4. **Boost Semantics**:
   - The native strategy lacks a built-in "Boost" object. The bridge synthesizes a **Boost lifecycle state machine** in firmware (Auto, Heat Boost, Cool Boost) with configurable auto-revert timers (default 60 minutes).

---

## Features Built

- **Dual-Interface Isolation**: Hardware SPI Ethernet (`10.0.3.x`) stays completely isolated from your home WiFi network (`192.168.x.x`).
- **Captive Portal First-Boot**: Zero-app setup. On first boot, connects to `ESP-BACnet-Setup` SoftAP to configure home WiFi.
- **On-Device Web Setup Wizard**: Guides target discovery, room selection, and MQTT broker setup.
- **Home Assistant Auto-Discovery**: Provisions full `climate` entities for every configured room zone, system power toggles, boost mode controls, and individual temperature sensors over MQTT.
- **Dynamic Object Browser & Scanner**: Live on-wire BACnet object browser capable of scanning 400+ objects with paginated UX and pin/unpin controls.
- **Circular In-Memory Logs Console**: Real-time diagnostic console accessible from the Web UI (`/health`) showing live BACnet transactions and network events.
- **Rollback-Protected LAN OTA Updates**: Password-protected web OTA update with dual 1900KB app partitions and automatic bootloader rollback safety.
- **Configuration Backup & Restore**: Export and import complete device configuration as a single JSON file.

---

## Planned Future Roadmap

- [ ] **Native Apple HomeKit Integration**: Embedded HAP (HomeKit Accessory Protocol) server on the ESP32 for direct pairing with Apple Home without external middleware.
- [ ] **Native Google Home Integration**: Local Matter / Google Home Local SDK support for direct voice and app control via Google Assistant.
- [ ] **Native Amazon Alexa Integration**: Direct local Alexa Smart Home Skill integration for native voice control.

---

## How to Use

### 1. Build & Flash
Build using standard ESP-IDF v5.x:
```bash
cd firmware/bacnet_bridge
idf.py set-target esp32
idf.py build
idf.py -p /dev/ttyUSB0 flash monitor
```

### 2. Physical Installation
1. Mount the ESP32 board and W5500 module inside or near the FCU controller enclosure.
2. Connect power via USB-C.
3. Plug an RJ45 Ethernet cable from the W5500 module into one of the RJ45 network ports on the Delta DAC-1180E controller.

### 3. First-Boot WiFi Setup
1. When unconfigured, the device creates an open WiFi network: `ESP-BACnet-Setup`.
2. Connect your phone or laptop. The captive portal will open automatically (or navigate to `http://192.168.4.1`).
3. Select your home 2.4GHz WiFi network, enter your password, and save.

### 4. Setup Wizard & Home Assistant Integration
1. Open `http://esp-bacnet.local` (or the IP assigned by your router).
2. The **Setup Wizard** will launch automatically.
3. Verify or discover your Delta controller's BACnet target IP (default `10.0.3.16`) and Device Instance ID (e.g. `753016`).
4. Enter your MQTT broker details (`host`, `port`, `user`, `password`).
5. Once saved, Home Assistant will automatically discover the climate entities and sensors.

---

## Customizing for Your Apartment Layout

- **Device Instance ID**: Different apartments have different BACnet device IDs (e.g., `753016`, `753017`). Change this in the Setup Wizard or Settings without recompiling.
- **Room Mapping**: 1-bed, 2-bed, or 3-bed apartments can enable, disable, or rename rooms directly from the **Objects** tab in the web UI.

---

## Credits & Acknowledgements

* **Creator & Author**: [Piers Wingfield](https://github.com/pierswingfield) (`piers@wingfield.tech`)
* **BACnet Protocol Stack**: Built using the open-source [bacnet-stack](https://github.com/bacnet-stack/bacnet-stack) library by Steve Karg and contributors, with a custom ESP-IDF raw socket datalink implementation.
* **mDNS Component**: Espressif `espressif/mdns` library.

---

## Disclaimer & License

### Disclaimer
> **NO WARRANTIES AND NO LIABILITIES ACCEPTED.**
> This is an independent personal project shared for community use. It is **not** affiliated with, sponsored by, or endorsed by Delta Controls Inc., building management, or building freeholders. Interfacing with building HVAC controllers carries inherent risks. You use this software and hardware design entirely at your own risk. The author accepts no liability for any damage to HVAC equipment, property, heating/cooling failures, or warranty invalidations.

### License
Licensed under the **MIT License**. Free for anyone to use, modify, and distribute with attribution. See [LICENSE](LICENSE) for details.
