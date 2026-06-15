# Multi-Device-Audio-Sync

> **Stream desktop / system audio to any device on your LAN — in real-time, over WebRTC.**  
> Built with Rust · Axum · WebRTC · Opus · cpal · PipeWire/PulseAudio  
> by [Y4ZDEV](https://y4z.dev)

---

## What it does

Captures system audio (or any input device) on your Arch Linux machine and streams it live to phones, tablets, or other computers on the same network — with sub-100 ms latency, using browser-native WebRTC. No app install needed on receivers. Just open a URL.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│  Arch Linux Host (Rust server · port 8443)  │
│                                             │
│  cpal → PipeWire/PulseAudio monitor source  │
│       → Opus encoder (192 kbps, 20 ms)      │
│       → TrackLocalStaticSample              │
│       → WebRTC peer connections             │
└──────────┬──────────────────────────────────┘
           │  HTTPS (self-signed TLS)
           │  WebRTC / ICE / UDP  50000–50050
           ▼
   ┌──────────────┐    ┌──────────────┐
   │  Phone / Tab │    │  Desktop     │
   │  /receiver   │    │  /receiver   │
   └──────────────┘    └──────────────┘
```

A **sender** registers a named audio stream. Any number of **receivers** can subscribe to one or more streams simultaneously.

---

## Routes

| Route | Purpose |
|---|---|
| `https://<IP>:8443/` | Receiver (default) |
| `https://<IP>:8443/receiver` | Receiver page |
| `https://<IP>:8443/sender` | Sender page |
| `https://<IP>:8443/cert` | Certificate trust guide |
| `https://<IP>:8443/cert.pem` | Download self-signed cert |
| `GET /streams` | List active stream names |
| `POST /sender/offer` | Register stream + WebRTC SDP offer |
| `POST /receiver/offer` | Subscribe to streams + WebRTC SDP offer |
| `DELETE /stream/:name` | Remove a stream |

---

## Stack

| Layer | Crate |
|---|---|
| HTTP server | `axum` + `axum-server` (TLS via `rustls`) |
| TLS cert | `rcgen` (self-signed, per-run) |
| WebRTC | `webrtc` |
| Audio codec | `opus` |
| Audio capture | `cpal` |
| Concurrency map | `dashmap` |
| Async runtime | `tokio` |

---

## Requirements

- Arch Linux (or any Linux with PipeWire/PulseAudio)
- Rust stable (`rustup update`)
- `libopus` system library
- `libasound2` / ALSA headers

```bash
sudo pacman -S opus alsa-lib
```

---

## Build & Run

```bash
git clone https://github.com/y4zdev/Multi-Device-Audio-Sync
cd Multi-Device-Audio-Sync
cargo build --release
cargo run --release
```

On startup you will see:

```
Detected LAN IP : 192.168.x.x
cert.pem written (share with devices to trust)
─────────────────────────────────────────────
 Trust cert first : https://192.168.x.x:8443/cert
 Receiver         : https://192.168.x.x:8443
 Receiver (url)   : https://192.168.x.x:8443/receiver
 Sender           : https://192.168.x.x:8443/sender
─────────────────────────────────────────────
```

---

## First Use — Trust the Certificate

Because the TLS certificate is self-signed (regenerated on each run), browsers and phones require a one-time trust step.

### Android
1. Open `https://<IP>:8443/cert` on the phone
2. Download `cert.pem`
3. Settings → Security → Install certificate → CA certificate → pick the file

### iOS
1. Open `https://<IP>:8443/cert.pem` in Safari
2. A profile is installed — go to Settings → General → VPN & Device Management → trust it
3. Then Settings → About → Certificate Trust Settings → enable it

### Desktop Chrome / Firefox
Navigate to `https://<IP>:8443` → click **Advanced → Proceed**. No install needed.

> **Note:** The certificate is regenerated every server restart. Re-trust if you restart.

---

## Stream Desktop Audio (PipeWire / PulseAudio)

The server auto-selects the best input source. To force desktop audio output:

```bash
# PulseAudio / PipeWire-pulse
pactl set-default-source <monitor-source-name>

# Find monitor source name
pactl list short sources | grep monitor

# Example
pactl set-default-source alsa_output.pci-0000_00_1f.3.analog-stereo.monitor
```

Or use `pavucontrol` → Recording tab → set the server stream to your output monitor.

---

## Firewall

```bash
# ufw
sudo ufw allow 8443/tcp
sudo ufw allow 50000:50050/udp

# iptables
sudo iptables -A INPUT -p tcp --dport 8443 -j ACCEPT
sudo iptables -A INPUT -p udp --match multiport --dports 50000:50050 -j ACCEPT
```

---

## Audio Settings

| Parameter | Value |
|---|---|
| Codec | Opus |
| Bitrate | 192 kbps |
| Frame size | 20 ms (960 samples @ 48 kHz) |
| Sample rate | 48000 Hz |
| Channels | Stereo |
| FEC | Enabled |
| VBR | Enabled |
| Packet loss hint | 5% |
| ICE UDP range | 50000–50790 |

---

## License

MIT — do whatever you want.

---

<p align="right">made with ♥ by <a href="https://y4z.dev">Y4ZDEV</a></p>
