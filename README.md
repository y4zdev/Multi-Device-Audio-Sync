# Multi-Device Audio Sync

A LAN-based **network sound control system** written in Rust (Axum + WebRTC).  
Capture audio on one device and route it to one or many speakers over your local network — all from a browser, with no plugins.

---

## Architecture

Three device roles operate over a self-hosted HTTPS server:

| Role | Page | What it does |
|---|---|---|
| **Mic / Source** | `/sender` | Captures system, browser mic, or display audio and streams it via WebRTC |
| **Speaker** | `/receiver` | Subscribes to one or more streams, applies local gain and jitter buffering |
| **Manager** | `/manager` | Lists all devices, routes streams to speakers, controls per-device volume, shows a live routing diagram |

The server maintains:
- A **stream registry** (`DashMap<String, TrackLocalStaticSample>`) — one entry per live WebRTC track
- A **device registry** (`DashMap<String, DeviceInfo>`) — one entry per registered client
- A **WebSocket broadcast bus** (`/ws`) — pushes `snapshot`, `device_joined`, `device_left`, `stream_added`, `stream_removed`, `volume_changed`, `route_changed`, and `set_volume` events to all connected clients in real time

---

## Quickstart

### 1. Build and run

```bash
cargo build --release
./target/release/multi-device-audio-sync
```

The server binds to `0.0.0.0:8443` (HTTPS).  
On startup it prints your LAN IP and the trust URL:

```
Detected LAN IP : 192.168.1.42
─────────────────────────────────────────────
 Trust cert first : https://192.168.1.42:8443/cert
 Receiver         : https://192.168.1.42:8443
 Sender           : https://192.168.1.42:8443/sender
 Manager          : https://192.168.1.42:8443/manager
─────────────────────────────────────────────
```

### 2. Trust the certificate (every new device)

The server generates a self-signed TLS certificate on each startup. Every device that connects must trust it once.

Open `https://<LAN-IP>:8443/cert` on the device and follow the instructions:

- **Desktop Chrome / Firefox** — click *Advanced → Proceed* in the browser warning. No install needed.
- **Android** — download `cert.pem`, then Settings → Security → Install certificate → CA certificate.
- **iOS** — open the `.pem` file to install a profile, then Settings → General → VPN & Device Management → trust it, then Settings → About → Certificate Trust Settings → enable it.

> The certificate is regenerated on every server restart. You must re-trust it after restarting.

---

## Using the system

### Sender (Mic / Source) — `/sender`

1. Enter a **Device Name** (e.g. `Game PC`, `Desktop Mic`) and click **Register Device**.
2. Enter a **Stream Name** (e.g. `game`, `music`, `mic`).
3. Choose a **Source**:
   - `System Audio` — captures the default PulseAudio input on the server host (Linux only)
   - `Browser Mic` — captures the browser's microphone via `getUserMedia`
   - `Display Audio` — captures tab/screen audio via `getDisplayMedia`
4. Click **Start Stream**. The orange visualizer confirms the stream is live.

The sender registers itself as a `mic` device, sends a heartbeat every 10 s, and reconnects automatically with exponential back-off on link failure.

### Receiver (Speaker) — `/receiver`

1. Enter a **Speaker Name** (e.g. `Living Room`, `Headphones`) and click **Register Speaker**.
2. Available streams appear in the **[02] Available Streams** panel.
3. Click **▶ SUB** on any stream to subscribe. Adjust gain and jitter buffer per stream.
4. The manager can push stream assignments and volume changes remotely — these apply automatically.

### Manager — `/manager`

- **[01] Devices** — lists all online mics and speakers with their status and assigned stream count.
- **[02] Streams** — lists all live streams.
- **Diagram** (center) — visual routing map:
  - Click a **MIC node**, then a **SPEAKER node** to assign all current streams to that speaker.
  - Click a bright **route line** (server → speaker) to clear that speaker's assignments.
  - Ghost lines show unconnected paths.
- **[03] Device Controls** (right) — select any device to:
  - Set master volume (calls `PATCH /device/{id}/volume`; the target device applies it immediately)
  - Assign individual streams via checkboxes (calls `PATCH /device/{id}/streams`)
  - Remove the device

---

## API reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/` or `/receiver` | Receiver page |
| `GET` | `/sender` | Sender page |
| `GET` | `/manager` | Manager page |
| `GET` | `/cert` | Certificate trust instructions |
| `GET` | `/cert.pem` | Download self-signed cert |
| `GET` | `/streams` | List active stream names |
| `POST` | `/sender/offer` | WebRTC offer from sender |
| `POST` | `/receiver/offer` | WebRTC offer from receiver |
| `DELETE` | `/stream/{name}` | Remove a stream |
| `POST` | `/device/register` | Register a device (`{id?, name, role}`) |
| `GET` | `/devices` | List all devices |
| `DELETE` | `/device/{id}` | Remove a device |
| `PATCH` | `/device/{id}/volume` | Set master volume (`{volume: 0.0–2.0}`) |
| `PATCH` | `/device/{id}/streams` | Set assigned streams (`{streams: [...]}`) |
| `GET` | `/ws` | WebSocket control bus |

---

## Known limitations

- **LAN only.** WebRTC uses a single STUN server (`stun.l.google.com`). Connections across NAT boundaries or over the internet are not supported without a TURN relay.
- **Certificate regenerates on restart.** Every server restart generates a new self-signed cert. All devices must re-trust after a restart.
- **System audio source is Linux-only.** The `cpal` system capture path targets PulseAudio. On Windows or macOS, use the `Browser Mic` or `Display Audio` sources instead.
- **Browser autoplay policy.** Browsers block audio playback until a user gesture occurs. The receiver page handles this by resuming the `AudioContext` on the first subscribe click. If audio is silent, tap anywhere on the page first.
- **No persistent state.** Device registrations and stream assignments are held in memory only. Everything resets on server restart.
- **No authentication.** The manager page has no login. Anyone on the LAN who trusts the cert can access it.

---

## Project structure

```
multi-device-audio-sync/
├── src/
│   └── main.rs          # Axum server, WebRTC, device registry, WebSocket bus
├── static/
│   ├── sender.html      # Mic / source client
│   ├── index.html       # Speaker / receiver client
│   └── manager.html     # Manager control surface
├── Cargo.toml
└── README.md
```

---

## Milestone status

| Milestone | Scope | Status |
|---|---|---|
| **A** | Device registry, `/devices` API, Manager devices table | ✅ Complete |
| **B** | WebSocket bus, remote volume, stream assignment | ✅ Complete |
| **C** | SVG diagram routing view, polished docs | ✅ Complete |
