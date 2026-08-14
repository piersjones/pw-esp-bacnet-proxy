# bacnet_client component

Vendored BACnet client code, sourced from two places:

1. **[bacnet-stack](https://github.com/bacnet-stack/bacnet-stack)** (Steve Karg and contributors) —
   the core protocol encode/decode and client service files (`bacnet/*.c`, `bacnet/basic/**/*.c`
   listed in this component's `CMakeLists.txt`), taken from the `master` branch as of 2026-08-13.
   Per-file license varies (mostly MIT or GPL-2.0-or-later WITH GCC-exception-2.0 — see each
   file's header and `license/` in the upstream repo); the GCC-exception variant is specifically
   designed to permit linking into non-GPL embedded firmware like this project's.
2. **`bacnet-stack`'s `ports/esp32`** (Kato Gangstad, 2026-04-04) — the BACnet/IP datalink layer
   (`bacnet/datalink/bip.c`, `bip_init.c`, `bvlc.c`) originally written for a PlatformIO/Arduino
   ESP32 target. Reused as-is; only the socket layer needed replacing (see below), since the
   BVLC/BACnet-IP framing logic itself is framework-agnostic.

## What's genuinely new here

`bacnet/datalink/bip_socket_esp_idf.c` — an ESP-IDF/raw-lwIP-BSD-socket implementation of the
5-function `bip_socket_*` contract the upstream port defines in `bip.h`, replacing its
Arduino-`WiFiUDP`-based `bip_socket.cpp`. This is what lets the same validated `bip.c`/`bvlc.c`
logic run under ESP-IDF instead of Arduino.

## Deliberate deviations from a straight copy

- **No BBMD support.** `bacnet/datalink/datalink.h` (upstream) unconditionally pulls in
  `bacnet/basic/bbmd/h_bbmd.h` when `BACDL_BIP` is defined, but that header expects upstream's
  full `bvlc.h` (with `BACNET_IP_ADDRESS` etc.), which conflicts with the simpler ESP32-port
  `bvlc.c`/`bvlc.h` pair used here. Since this client only ever talks unicast to one known
  device — no broadcast, no foreign device registration — the `h_bbmd.h` include is commented
  out in this vendored `datalink.h`, and `h_bbmd.c` isn't vendored at all.
- **`bvlc.h` split into two files.** The upstream port's `bvlc.h` did `#include
  "bacnet/datalink/bvlc.h"` expecting that path to resolve to the *real* upstream header (only
  true in the original PlatformIO project's include-path layout, where the two were genuinely
  separate files). Here, `bacnet/datalink/bvlc.h` is upstream's real header (needed for
  `BACNET_IP_ADDRESS`, referenced by `bip.h`), and the ESP32 port's original simplified
  declarations (`bvlc_for_non_bbmd`, `pico_bvlc_get_function_code`, etc.) live in
  `bacnet/datalink/bvlc_esp_port.h` instead.
- **No `h_rp.c`/`h_whois.c`/`h_iam.c`/`h_dcc.c`/`h_noserv.c`/`device.c`.** These handle
  *incoming* requests to this device (acting as a BACnet server) and require `Device_Init()` and
  an object table this project has no use for — it's a pure client reading/writing the Delta
  panel's objects, never responding to requests from other devices.
- **Port byte-order quirk, fixed at the call site, not in `bip.c`.** `bip.c` reads/writes the
  UDP port embedded in `BACNET_ADDRESS.mac[4:6]` via a raw `memcpy` into/out of a `uint16_t` —
  i.e. *native* (little-endian, on ESP32) byte order — consistently across all its own call
  sites (`bip_get_my_address`, `bip_decode_bip_address`, the receive path). This differs from
  upstream `bacaddr.c`'s `bacnet_address_mac_from_ascii()`, which encodes big-endian per the
  BACnet spec's normal convention. Mixing the two silently sent every request to the wrong UDP
  port (47808 → 49338, byte-swapped) with no error anywhere in the stack — see
  `main.c`'s `bind_target_device()`, which builds the target address manually to match `bip.c`'s
  actual convention instead of using the upstream helper.

## Milestone status

As of 2026-08-13: confirmed working end-to-end against the real Delta panel over the physical
W5500 Ethernet link — `ReadProperty` of the Device object's `object-name` returns `"C305"`,
matching the desktop CLI (`bacrp --mac 10.0.3.16:47808 --dnet 0 753016 device 753016
object-name`) exactly. See `main/main.c` for the test harness.
