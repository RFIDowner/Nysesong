# PigeonPalHexV31 — frame-stats-runden (V3.1)

**Firmware for Stigs inngangs-antenne** (hex/ASCII-protokoll). Eneste endring
fra `PigeonPalHexV3`: **RFID-tellerne sendes med i heartbeatet** hvert 60. s:

- `rfFramesOk` — gyldige rammer hørt fra reader-modulen
- `rfTooShort` — rammer forkastet som for korte (< 4 hex-tegn)
- `rfOverflow` — rammer forkastet som for lange (> 24 hex-tegn)
- `rfDupBlocked` — samme-chip-lesinger blokkert av 4 s-vinduet
- `rfBytes` — råbytes lest fra seriell-linjen

Alle tellerne er **kumulative siden boot** — `uptimeS` gir referansen (faller
`uptimeS`, har enheten restartet og tellerne begynner på null igjen).

**Formålet er lesetap-diagnose fra skyen** (22.07-funnene: 7 av 23 duer ulest,
enkeltringer som «ikke vil skannes»): sveip uten app-respons **med**
`rfFramesOk`-økning = tapet ligger oppstrøms (MQTT/bro/ingest); **uten**
økning = modulen hørte aldri chippen (geometri/orientering/svak chip).
Reader-modulen er urørt — ESP32 lytter bare, som før.

Alt annet (parser, chip-id-format, LWT, MQTT-auth i portal, BOOT-reset) er
identisk med V3 — se `../PigeonPalHexV3/README.md`. Serverside: heartbeat-
handleren må slippe `rf*`-feltene gjennom til `device_presence` (PigeonPal-
branch `feat/heartbeat-frame-stats`); eldre firmware uten feltene påvirkes
ikke.

Kompilert 2026-07-22 med esp32:esp32@3.3.11: 81 % flash, ingen advarsler.
Flash som vanlig fra Arduino IDE — velg denne mappen, ikke V3.
