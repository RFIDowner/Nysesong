# PigeonPalHexV3 — presence-runden for HEX/ASCII-antennen

**Dette er firmwaren for Stigs device** (antennen som sender hex/ASCII-rammer:
STX 0x02 + hex-tegn + ETX 0x03). Base = `NyTilSesong.ino` (urørt) —
hex-parseren og chip-id-formatet er beholdt 1:1, så enheten sender
**identiske chip-id-er som den alltid har gjort**. Ingen re-registrering i
duedatabasen trengs.

`PigeonPalBinaerV3` er søsterfirmwaren for enheter med binær-antennen
(leverandørramme `02 0A 02 | 5 byte | XOR | 03`). **Flash riktig variant til
riktig antennetype** — feil variant gir null lesinger (18.07-lærdommen).

V3-tilleggene (identiske med binær-V3, samme server-kontrakt):

1. **LWT (Last Will):** brokeren publiserer retained `offline` på
   `pigeonpal/presence/v1/<deviceId>/status` i det øyeblikket forbindelsen
   dør. Ved tilkobling publiserer enheten retained `online` på samme topic.
   Keepalive 60 s = maks forsinkelse før et stille strømkutt synes.
2. **Heartbeat:** JSON hvert 60. sekund på
   `pigeonpal/presence/v1/<deviceId>/hb` (QoS 0, aldri bufret, utenom
   hendelseskøen): `deviceId`, `fw` (= `3.0.0-hex`), `uptimeS`, `rssi`,
   `queued`, `heapFree`.
3. **MQTT-legitimasjoner i portalen:** to valgfrie felt, lagret i NVS
   (namespace `mqtt`). Tomt = anonym (dagens broker). Mosquitto-auth kan
   dermed skrus på server-side uten omflashing. Re-lagring av WiFi med
   uendret bruker og tomt passordfelt beholder eksisterende MQTT-passord.
4. **BOOT-knapp lang-trykk (10 s):** tømmer lagret WiFi og restarter inn i
   oppsettsportalen. Rask LED-blink fra 1,5 s hold kvitterer; slipp før
   10 s = angre.

I tillegg (generiske robusthetsgrep fra V2-runden, ikke antennespesifikke):
UART RX-buffer 1024, head-of-line-rotasjon ved MQTT ack-timeout, og
**serial-logging koblet inn** — hver OK-lesing printer
`RFID frame ok chipHex=… chipId=…`, statslinje hvert 5. sekund
(NyTilSesong printet ingenting per lesing).

## Flash-sjekkliste
- Arduino IDE, samme board-oppsett som før (esp32).
- `MQTT_USERNAME`/`MQTT_PASSWORD` i toppen kan stå tomme (portal-verdier
  vinner uansett).
- Etter flash, verifiser i seriemonitor (115200 baud):
  1. `FW: 3.0.0-hex` logges ved boot.
  2. Skann en ring → `RFID frame ok chipHex=… chipId=…` med KJENT chip-id
     (samme som i duedatabasen).
  3. `MQTT connected.` → lesingen dukker opp i appens Loft activity.
  4. Devices-kortet på dashen viser grønn prikk, `fw`-felt kommer i
     heartbeat-dataene.

## Server-side motstykker (LIVE i prod 18.07)
- Broen abonnerer på presence-topicene og videresender til `deviceHeartbeat`;
  appens Devices-kort viser ekte online/offline. Ingen server-endring trengs
  for denne firmwaren.
- Mosquitto-auth + lukking av port 1883 gjøres server-side når flåten kjører
  V3-varianter (auth først parallelt med anonym, 1883 lukkes sist).
