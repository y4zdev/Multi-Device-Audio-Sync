# Y4ZDEV · AudioSyncWEB2ANY

A lightweight Rust + WebRTC desktop-audio streamer that captures Linux desktop output and plays it in a phone browser over the local network.

## Features

- Rust backend with Axum for the signaling endpoint and static page serving.
- WebRTC audio streaming to a mobile browser using Opus.
- Linux desktop-audio capture through PipeWire/Pulse monitor sources.
- Simple mobile-friendly receiver page with connection status and logs.
- Tuned for local-network listening on Arch Linux.

## Stack

- Rust
- Axum
- Tokio
- webrtc-rs
- CPAL
- Opus
- PipeWire / PulseAudio monitor source

## Project layout

```text
.
├── src/
│   └── main.rs
├── static/
│   └── index.html
├── Cargo.toml
└── README.md
```

## How it works

1. The Rust app starts an HTTP server on port `8080`.
2. The browser opens the receiver page from the same server.
3. The page creates a WebRTC offer and sends it to `/offer`.
4. The Rust backend creates a peer connection, attaches an Opus audio track, and returns the answer.
5. Desktop audio is captured from the selected Linux input source and encoded to Opus for playback in the phone browser.

## Requirements

- Linux desktop, preferably Arch Linux.
- PipeWire or PulseAudio compatible monitor source.
- Rust toolchain.
- A phone and computer on the same network.
- Open firewall ports for HTTP and WebRTC UDP traffic.

## Run

```bash
cargo run
```

Then open on the phone:

```text
http://YOUR_PC_IP:8080
```

## Default source

If desktop audio is not selected automatically, set the monitor source manually:

```bash
pactl set-default-source alsa_output.pci-0000_00_1f.3.analog-stereo.monitor
```

To check the active default source:

```bash
pactl get-default-source
```

## Firewall

Allow the HTTP port and UDP range used by ICE:

```bash
sudo ufw allow 8080/tcp
sudo ufw allow 50000:50050/udp
```

## Notes

- This project is aimed at low-latency listening on a local network.
- Mobile browsers may block autoplay, so playback may need a manual tap.
- If the monitor source sleeps, the first seconds of audio can feel delayed.
- For best results, keep PipeWire monitor suspension disabled.

## Future improvements

- Automatic monitor-source selection by exact PipeWire node name.
- Better resampling and buffering control.
- Live bitrate and source display in the UI.
- HTTPS support for wider browser compatibility.
- Persistent audio device selection.

## Author

Built and maintained by **Y4ZDEV**.
