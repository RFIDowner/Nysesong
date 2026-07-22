# PigeonPalBinaerV31 — frame-stats-runden (V3.1)

**Firmware for enheter med binær-antennen** (leverandørramme
`02 0A 02 | 5 byte | XOR | 03`). Søsterfirmware: `PigeonPalHexV31` for
hex/ASCII-antennen. **Flash riktig variant til riktig antennetype** — feil
variant gir null lesinger (18.07-lærdommen).

Eneste endring fra `PigeonPalBinaerV3`: **RFID-tellerne sendes med i
heartbeatet** hvert 60. s:

- `rfFramesOk` — gyldige rammer fra reader-modulen
- `rfBadLen` / `rfBadEnd` — rammer med feil lengde/sluttbyte
- `rfChecksumFail` — rammer med ødelagt XOR-sjekksum. **Kollisjons-indikator:**
  to chipper i feltet samtidig gir gjerne ødelagte sjekksummer her.
- `rfOverflow` — ringbuffer-overflow
- `rfDupBlocked` — samme-chip-lesinger blokkert av 4 s-vinduet
- `rfBytes` — råbytes lest fra seriell-linjen

Alle tellerne er **kumulative siden boot** — `uptimeS` gir referansen.
Reader-modulen er urørt; ESP32 lytter bare. Serverside: heartbeat-whitelisten
i `deviceHeartbeat` slipper feltene inn i `device_presence` (deployet 22.07).

Kompilert 2026-07-22 med esp32:esp32@3.3.11: 81 % flash, ingen advarsler.
