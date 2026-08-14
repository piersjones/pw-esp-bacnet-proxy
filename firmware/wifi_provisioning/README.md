# wifi_provisioning

WiFi provisioning firmware for the ESP32 BACnet bridge — the "essential" WiFi requirement from
[requirements.md](../../requirements.md) §1/§4C and [architecture-plan.md](../../architecture-plan.md)
Phase 2. The BACnet Ethernet segment has no default gateway (confirmed in Phase 0), so this is
the device's only route to the home LAN, MQTT broker, and (eventually) OTA updates.

## Provenance

Built on ESP-IDF's `examples/protocols/http_server/captive_portal` (SoftAP + DNS-hijack redirect
server + HTTP server, unmodified `components/dns_server`), extended with:

- A real setup form (`main/root.html`) instead of the example's static "Hello World" page.
- NVS storage of WiFi credentials (`main.c`'s `save_wifi_credentials`/`load_wifi_credentials`).
- Station-mode connection with retry on every boot, falling back to the SoftAP portal only if
  saved credentials don't work.
- A `GET /scan` endpoint returning nearby networks as JSON, so the setup page offers a clickable
  list of real, in-range, joinable networks instead of a blind text field.

## Flow

1. **Boot with saved credentials** → try connecting as a station (5 retries with backoff — WPA
   handshake timeouts are common and transient, not indicative of a wrong password; see the bug
   note below). On success, stays connected.
2. **No saved credentials, or connection fails after retries** → falls back to SoftAP mode
   (`ESP-BACnet-Setup`, open network) with the captive portal. Most phones/laptops auto-open the
   setup page on connect.
3. **Setup page**: scans for nearby networks on load (`GET /scan`), shows them as a clickable
   list (signal strength, lock icon for secured networks). "Enter it manually" is available as a
   fallback for hidden networks.
4. **Submitting the form** (`POST /connect`) saves credentials to NVS and reboots into step 1.

## Confirmed on real hardware (2026-08-13)

Full round trip: joined the `ESP-BACnet-Setup` AP from a phone, the setup page loaded, submitted
real home WiFi credentials, device saved them and rebooted, connected as a station, obtained a
real IP from the home router.

**Bug found and fixed**: the first version gave up after a single `WIFI_EVENT_STA_DISCONNECTED`
with no visibility into why, failing almost instantly (~40ms) even against correct credentials.
Adding the disconnect reason code to the log revealed transient WPA handshake timeouts (reason
203/205) — normal noise a single-attempt handler shouldn't treat as fatal. A 5-attempt retry
with a short backoff fixed it; it now connects cleanly within 1-2 retries.

The `/scan` feature (added after the connectivity fix, in response to user feedback) has been
built and flashed successfully but not yet clicked through on real hardware.

## Still open

- Persistent HTTP config UI reachable *after* connecting as a station, for BACnet target
  selection/discovery and room mapping (requirements.md §4C's room-configurability requirement) —
  currently the HTTP server + portal only run in SoftAP mode.
- A "reconfigure WiFi" path (clear NVS, return to SoftAP) without needing to re-flash.
- Merging this project with `firmware/bacnet_bridge`'s Ethernet/BACnet client into one firmware —
  they're deliberately separate, independently-tested projects right now (different subsystems,
  built in parallel per the Phase 2/3 plan), not yet combined.
- Visual branding/design polish on the setup form — deferred on purpose; functional correctness
  came first.
