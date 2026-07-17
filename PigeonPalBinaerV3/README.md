# PigeonPalBinaerV3 — presence-runden

V3 = V2 (5-byte chip-fiks, duplikatvakt, HOL-rotasjon) + fire tillegg:

1. **LWT (Last Will):** brokeren publiserer retained `offline` på
   `pigeonpal/presence/v1/<deviceId>/status` i det øyeblikket forbindelsen
   dør. Ved tilkobling publiserer enheten retained `online` på samme topic.
   Keepalive 60 s = maks forsinkelse før et stille strømkutt synes.
2. **Heartbeat:** JSON hvert 60. sekund på
   `pigeonpal/presence/v1/<deviceId>/hb` (QoS 0, aldri bufret, utenom
   hendelseskøen): `deviceId`, `fw`, `uptimeS`, `rssi`, `queued`, `heapFree`.
3. **MQTT-legitimasjoner i portalen:** to valgfrie felt i oppsettsportalen,
   lagret i NVS (namespace `mqtt`). Tomt = anonym (dagens broker). Når
   mosquitto-auth skrus på server-side trengs altså INGEN omflashing —
   legitimasjonene kan settes via portalen (BOOT-hold, se pkt. 4).
   Re-lagring av WiFi med uendret bruker og tomt passordfelt beholder
   eksisterende MQTT-passord.
4. **BOOT-knapp lang-trykk (10 s):** tømmer lagret WiFi og restarter inn i
   oppsettsportalen. Rask LED-blink fra 1,5 s hold kvitterer; slipp før
   10 s = angre. (Før: omflashing var eneste utvei ved byttet ruter.)

## Flash-sjekkliste
- Arduino IDE, samme board-oppsett som V2.
- `MQTT_USERNAME`/`MQTT_PASSWORD` i toppen kan stå tomme (portal-verdier
  vinner uansett).
- Etter flash: verifiser i seriemonitor at `FW: 3.0.0` logges, og at
  `MQTT connected.` følges av retained `online` (sjekk med
  `mosquitto_sub -t 'pigeonpal/presence/v1/#' -v`).

## Server-side motstykker (egen runde, ikke i denne mappa)
- Broen/appen abonnerer ikke på presence-topicene ennå — dash-kortet får
  ekte online/offline i device-presence-runden i app-repoet.
- Mosquitto-auth + lukking av port 1883 (HMAC B) gjøres server-side når
  V3 er flashet på enhetene.
