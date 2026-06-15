use anyhow::{anyhow, Result};
use axum::{
    extract::State,
    response::{Html, Redirect},
    routing::{get, post, delete},
    Json, Router,
};
use axum_server::tls_rustls::RustlsConfig;
use bytes::Bytes;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use dashmap::DashMap;
use rcgen::{CertificateParams, DistinguishedName, SanType};
use serde::{Deserialize, Serialize};
use std::net::UdpSocket;
use std::process::Command;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors,
        media_engine::{MediaEngine, MIME_TYPE_OPUS},
        setting_engine::SettingEngine,
        APIBuilder,
    },
    ice_transport::{
        ice_gatherer_state::RTCIceGathererState,
        ice_server::RTCIceServer,
    },
    interceptor::registry::Registry,
    media::Sample,
    peer_connection::{
        configuration::RTCConfiguration,
        peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
    track::track_local::{
        track_local_static_sample::TrackLocalStaticSample, TrackLocal,
    },
};
use webrtc_ice::udp_network::{EphemeralUDP, UDPNetwork};

// ── constants ─────────────────────────────────────────────────────────────────
const OPUS_SR: usize    = 48_000;
const OPUS_CH: usize    = 2;
const FRAME_SIZE: usize = 960;   // 20 ms @ 48 kHz
const DURATION_MS: u64  = 20;
const OPUS_BITRATE: i32 = 192_000;

// ── Request / Response types ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct SenderOfferRequest {
    name:   String,
    source: Option<String>,
    sdp:    RTCSessionDescription,
}

#[derive(Deserialize)]
struct ReceiverOfferRequest {
    streams: Vec<String>,
    sdp:     RTCSessionDescription,
}

#[derive(Serialize)]
struct StreamsResponse {
    streams: Vec<String>,
}

// ── App state ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    api:      Arc<webrtc::api::API>,
    streams:  Arc<DashMap<String, Arc<TrackLocalStaticSample>>>,
    cert_pem: Arc<String>,
}

// ── LAN IP ────────────────────────────────────────────────────────────────────

fn local_lan_ip() -> String {
    UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| { s.connect("8.8.8.8:80")?; s.local_addr() })
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string())
}

// ── Self-signed TLS cert ──────────────────────────────────────────────────────

fn make_self_signed_cert(lan_ip: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut params = CertificateParams::new(vec![
        "localhost".to_string(),
        lan_ip.to_string(),
    ])
    .map_err(|e| anyhow!("CertificateParams: {e}"))?;

    params.distinguished_name = DistinguishedName::new();

    if let Ok(ip) = lan_ip.parse::<std::net::IpAddr>() {
        params.subject_alt_names.push(SanType::IpAddress(ip));
    }
    params.subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));

    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| anyhow!("KeyPair::generate: {e}"))?;
    let cert = params.self_signed(&key_pair)
        .map_err(|e| anyhow!("self_signed: {e}"))?;

    Ok((cert.pem().into_bytes(), key_pair.serialize_pem().into_bytes()))
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("Failed to install rustls ring CryptoProvider"))?;

    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut media_engine)?;

    let ephemeral_udp = EphemeralUDP::new(30690, 30790)?;
    let mut setting_engine = SettingEngine::default();
    setting_engine.set_udp_network(UDPNetwork::Ephemeral(ephemeral_udp));

    let api = Arc::new(
        APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .with_setting_engine(setting_engine)
            .build(),
    );

    let lan_ip = local_lan_ip();
    println!("Detected LAN IP : {lan_ip}");

    let (cert_pem_bytes, key_pem_bytes) = make_self_signed_cert(&lan_ip)?;
    let cert_pem_str = String::from_utf8(cert_pem_bytes.clone()).unwrap_or_default();

    match std::fs::write("cert.pem", &cert_pem_bytes) {
        Ok(_)  => println!("cert.pem written (share with devices to trust)"),
        Err(e) => eprintln!("Warning: could not write cert.pem: {e}"),
    }

    let tls_config = RustlsConfig::from_pem(cert_pem_bytes, key_pem_bytes)
        .await
        .map_err(|e| anyhow!("TLS config error: {e}"))?;

    let state = AppState {
        api,
        streams:  Arc::new(DashMap::new()),
        cert_pem: Arc::new(cert_pem_str),
    };

    let app = Router::new()
        // Pages
        .route("/",         get(receiver_handler))
        .route("/sender",   get(sender_handler))
        .route("/receiver", get(receiver_handler))
        .route("/cert",     get(cert_page_handler))
        .route("/cert.pem", get(cert_download_handler))
        // API
        .route("/streams",        get(list_streams))
        .route("/sender/offer",   post(handle_sender_offer))
        .route("/receiver/offer", post(handle_receiver_offer))
        .route("/stream/{name}",  delete(remove_stream))
        .with_state(state);

    let bind_addr: std::net::SocketAddr = "0.0.0.0:8443".parse()?;
    println!("─────────────────────────────────────────────");
    println!(" Trust cert first : https://{lan_ip}:8443/cert");
    println!(" Receiver         : https://{lan_ip}:8443");
    println!(" Receiver (url)   : https://{lan_ip}:8443/receiver");
    println!(" Sender           : https://{lan_ip}:8443/sender");
    println!("─────────────────────────────────────────────");

    axum_server::bind_rustls(bind_addr, tls_config)
        .serve(app.into_make_service())
        .await
        .map_err(|e| anyhow!("server error: {e}"))
}

// ── Page handlers ─────────────────────────────────────────────────────────────

async fn receiver_handler() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn sender_handler() -> Html<&'static str> {
    Html(include_str!("../static/sender.html"))
}

async fn cert_page_handler(
    State(state): State<AppState>,
) -> Html<String> {
    let html = format!(r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Y4Z // Trust Certificate</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#050608;color:#d1d5db;font-family:'Share Tech Mono',monospace;
          display:flex;flex-direction:column;align-items:center;justify-content:center;
          min-height:100dvh;padding:24px;gap:20px;}}
    h1{{color:#00f0ff;font-size:1.4rem;letter-spacing:3px;text-transform:uppercase;text-align:center;}}
    p{{color:#6b7280;font-size:0.85rem;max-width:500px;text-align:center;line-height:1.6;}}
    .card{{background:rgba(10,12,16,0.92);border:1px solid rgba(0,240,255,0.3);
           padding:20px;max-width:500px;width:100%;display:flex;flex-direction:column;gap:12px;}}
    .step{{display:flex;gap:10px;align-items:flex-start;}}
    .num{{color:#00f0ff;font-size:1rem;min-width:24px;}}
    .desc{{color:#d1d5db;font-size:0.82rem;line-height:1.5;}}
    .desc b{{color:#fcee0a;}}
    a.btn{{display:block;text-align:center;padding:12px;background:rgba(0,240,255,0.08);
            border:1px solid #00f0ff;color:#00f0ff;text-decoration:none;
            font-size:0.9rem;letter-spacing:2px;transition:background 0.2s;}}
    a.btn:hover{{background:#00f0ff;color:#000;}}
    .warn{{color:#ff8800;font-size:0.78rem;text-align:center;}}
  </style>
</head>
<body>
  <h1>// Trust Certificate</h1>
  <p>To use Audio_Sync_LINK on this device, you need to trust the self-signed certificate once.</p>
  <div class="card">
    <div class="step"><span class="num">1.</span><span class="desc">Tap the button below to <b>download cert.pem</b> to this device.</span></div>
    <a class="btn" href="/cert.pem" download="y4z-cert.pem">⬇ Download cert.pem</a>
    <div class="step"><span class="num">2.</span><span class="desc"><b>Android:</b> Settings → Security → Install certificate → CA certificate → pick the file.</span></div>
    <div class="step"><span class="num">2.</span><span class="desc"><b>iOS:</b> Open the .pem file → installs a profile. Then Settings → General → VPN &amp; Device Management → trust it. Then Settings → About → Certificate Trust Settings → enable it.</span></div>
    <div class="step"><span class="num">3.</span><span class="desc"><b>Desktop Chrome/Firefox:</b> Navigate to the URL below and click <b>Advanced → Proceed</b>. No install needed.</span></div>
    <div class="step"><span class="num">4.</span><span class="desc">Come back here and open the app links below.</span></div>
  </div>
  <div class="card">
    <a class="btn" href="/receiver">▶ Open Receiver</a>
    <a class="btn" href="/sender">▶ Open Sender</a>
  </div>
  <div class="warn">Certificate is re-generated on each server restart. Re-trust if you restart the server.</div>
</body>
</html>
"#);
    Html(html)
}

async fn cert_download_handler(
    State(state): State<AppState>,
) -> impl axum::response::IntoResponse {
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/x-pem-file"),
            (axum::http::header::CONTENT_DISPOSITION, "attachment; filename=\"y4z-cert.pem\""),
        ],
        state.cert_pem.as_bytes().to_vec(),
    )
}

// ── API handlers ───────────────────────────────────────────────────────────────

async fn list_streams(
    State(state): State<AppState>,
) -> Json<StreamsResponse> {
    let streams: Vec<String> = state.streams.iter().map(|e| e.key().clone()).collect();
    Json(StreamsResponse { streams })
}

async fn remove_stream(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> axum::http::StatusCode {
    state.streams.remove(&name);
    println!("[stream] removed: {name}");
    axum::http::StatusCode::NO_CONTENT
}

async fn handle_sender_offer(
    State(state): State<AppState>,
    Json(req): Json<SenderOfferRequest>,
) -> Json<RTCSessionDescription> {
    let name   = req.name.clone();
    let source = req.source.as_deref().unwrap_or("system").to_string();

    let track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type:     MIME_TYPE_OPUS.to_owned(),
            clock_rate:    OPUS_SR as u32,
            channels:      OPUS_CH as u16,
            sdp_fmtp_line: "minptime=20;useinbandfec=1;stereo=1;sprop-stereo=1".to_string(),
            ..Default::default()
        },
        name.clone(),
        "mypro".to_owned(),
    ));
    state.streams.insert(name.clone(), Arc::clone(&track));
    println!("[stream] registered: {name} (source={source})");

    if source == "system" {
        let rt_handle = Handle::current();
        let name2 = name.clone();
        std::thread::spawn(move || {
            if let Err(e) = start_audio_capture(track, rt_handle) {
                eprintln!("[{name2}] audio capture error: {e:?}");
            }
        });

        let pc = build_peer_connection(&state).await;
        attach_state_cleanup(pc.clone(), name.clone(), Arc::clone(&state.streams), "system");
        return negotiate_and_answer(pc, req.sdp).await;
    }

    // Browser / display audio: relay inbound WebRTC track
    let pc = build_peer_connection(&state).await;

    pc.on_track(Box::new({
        let relay_track = Arc::clone(&track);
        let name2 = name.clone();
        move |remote_track, _, _| {
            let relay = Arc::clone(&relay_track);
            let name3 = name2.clone();
            println!("[sender/browser] [{name3}] inbound track");
            tokio::spawn(async move {
                while let Ok((rtp_pkt, _)) = remote_track.read_rtp().await {
                    let payload = Bytes::copy_from_slice(&rtp_pkt.payload);
                    let _ = relay.write_sample(&Sample {
                        data:     payload,
                        duration: std::time::Duration::from_millis(DURATION_MS),
                        ..Default::default()
                    }).await;
                }
                println!("[sender/browser] [{name3}] track ended");
            });
            Box::pin(async {})
        }
    }));

    attach_state_cleanup(pc.clone(), name.clone(), Arc::clone(&state.streams), "browser");
    negotiate_and_answer(pc, req.sdp).await
}

async fn handle_receiver_offer(
    State(state): State<AppState>,
    Json(req): Json<ReceiverOfferRequest>,
) -> Json<RTCSessionDescription> {
    let pc = build_peer_connection(&state).await;

    for stream_name in &req.streams {
        if let Some(track) = state.streams.get(stream_name) {
            let rtp_sender = pc
                .add_track(Arc::clone(&*track) as Arc<dyn TrackLocal + Send + Sync>)
                .await
                .expect("failed to add track");
            tokio::spawn(async move {
                let mut buf = vec![0u8; 1500];
                while rtp_sender.read(&mut buf).await.is_ok() {}
            });
            println!("[receiver] subscribed to: {stream_name}");
        } else {
            println!("[receiver] stream not found (skipped): {stream_name}");
        }
    }

    pc.on_peer_connection_state_change(Box::new(|s: RTCPeerConnectionState| {
        println!("[receiver] PC state: {s}");
        Box::pin(async {})
    }));

    negotiate_and_answer(pc, req.sdp).await
}

// ── WebRTC helpers ─────────────────────────────────────────────────────────────

fn attach_state_cleanup(
    pc:      Arc<webrtc::peer_connection::RTCPeerConnection>,
    name:    String,
    streams: Arc<DashMap<String, Arc<TrackLocalStaticSample>>>,
    tag:     &'static str,
) {
    pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        let name    = name.clone();
        let streams = Arc::clone(&streams);
        println!("[sender/{tag}] [{name}] PC state: {s}");
        if matches!(s, RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) {
            streams.remove(&name);
            println!("[stream] unregistered: {name}");
        }
        Box::pin(async {})
    }));
}

async fn build_peer_connection(
    state: &AppState,
) -> Arc<webrtc::peer_connection::RTCPeerConnection> {
    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        }],
        ..Default::default()
    };
    Arc::new(
        state.api.new_peer_connection(config).await
            .expect("failed to create peer connection"),
    )
}

async fn negotiate_and_answer(
    pc:    Arc<webrtc::peer_connection::RTCPeerConnection>,
    offer: RTCSessionDescription,
) -> Json<RTCSessionDescription> {
    pc.set_remote_description(offer).await
        .expect("set_remote_description failed");

    let answer = pc.create_answer(None).await
        .expect("create_answer failed");

    let (tx, rx) = oneshot::channel::<()>();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    pc.on_ice_gathering_state_change(Box::new(move |s: RTCIceGathererState| {
        let tx = Arc::clone(&tx);
        Box::pin(async move {
            if s == RTCIceGathererState::Complete {
                if let Some(t) = tx.lock().await.take() {
                    let _ = t.send(());
                }
            }
        })
    }));

    pc.set_local_description(answer).await
        .expect("set_local_description failed");

    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;

    let final_answer = pc.local_description().await
        .expect("no local description");

    Json(final_answer)
}

// ── Audio capture (system source) ─────────────────────────────────────────────

fn pulse_default_source_name() -> Option<String> {
    let out = Command::new("pactl").arg("get-default-source").output().ok()?;
    if !out.status.success() { return None; }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

fn choose_input_device() -> Result<cpal::Device> {
    let host    = cpal::default_host();
    let devices: Vec<cpal::Device> = host.input_devices()?.collect();
    if devices.is_empty() { return Err(anyhow!("no input devices found")); }

    let default_src = pulse_default_source_name();
    println!("[cpal] PulseAudio default source: {}",
        default_src.as_deref().unwrap_or("<none>"));

    if let Some(ref src) = default_src {
        let src_l = src.to_ascii_lowercase();
        for d in &devices {
            let name = d.name().unwrap_or_default().to_ascii_lowercase();
            if name.contains(&src_l) {
                println!("[cpal] selected (pulse match): {}", d.name().unwrap_or_default());
                return Ok(d.clone());
            }
        }
    }
    for d in &devices {
        let name = d.name().unwrap_or_default().to_ascii_lowercase();
        if name.contains("monitor") {
            println!("[cpal] selected (monitor fallback): {}", d.name().unwrap_or_default());
            return Ok(d.clone());
        }
    }
    let d = host.default_input_device()
        .ok_or_else(|| anyhow!("no default input device"))?;
    println!("[cpal] selected (default): {}", d.name().unwrap_or_default());
    Ok(d)
}

async fn write_encoded_frame(
    track:       Arc<TrackLocalStaticSample>,
    payload:     Vec<u8>,
    duration_ms: u64,
) {
    let _ = track.write_sample(&Sample {
        data:     Bytes::from(payload),
        duration: std::time::Duration::from_millis(duration_ms),
        ..Default::default()
    }).await;
}

fn start_audio_capture(
    track:     Arc<TrackLocalStaticSample>,
    rt_handle: Handle,
) -> Result<()> {
    let device  = choose_input_device()?;
    let sup_cfg = device.default_input_config()?;
    let input_sr  = sup_cfg.sample_rate().0 as usize;
    let input_ch  = sup_cfg.channels() as usize;
    let stream_cfg: cpal::StreamConfig = sup_cfg.clone().into();

    println!("[cpal] device : {}", device.name().unwrap_or_default());
    println!("[cpal] native : {}Hz {}ch", input_sr, input_ch);

    let mut encoder = opus::Encoder::new(
        OPUS_SR as u32,
        opus::Channels::Stereo,
        opus::Application::Audio,
    )?;
    encoder.set_bitrate(opus::Bitrate::Bits(OPUS_BITRATE))?;
    encoder.set_vbr(true)?;
    encoder.set_inband_fec(true)?;
    encoder.set_packet_loss_perc(5)?;

    // Shared ring buffer: native f32 samples waiting to be resampled + encoded
    let ring: Arc<std::sync::Mutex<Vec<f32>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let ring_enc = Arc::clone(&ring);
    let track_enc = Arc::clone(&track);
    let rt_enc = rt_handle.clone();

    // Encoder thread: drains ring buffer, resamples to 48kHz stereo, encodes Opus
    std::thread::spawn(move || {
        let ratio = OPUS_SR as f64 / input_sr as f64;
        let min_input_needed = ((FRAME_SIZE as f64 / ratio).ceil() as usize + 1) * input_ch;
        let mut opus_pcm: Vec<i16> = Vec::with_capacity(FRAME_SIZE * OPUS_CH * 4);

        loop {
            // Wait until we have enough native samples to produce at least one Opus frame
            loop {
                let len = ring_enc.lock().unwrap().len();
                if len >= min_input_needed { break; }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }

            let chunk: Vec<f32> = {
                let mut buf = ring_enc.lock().unwrap();
                let take = buf.len(); // process all available
                buf.drain(..take).collect()
            };

            // Resample + convert to stereo i16 at 48kHz
            let native_frames = chunk.len() / input_ch;
            let target_frames = (native_frames as f64 * ratio) as usize;

            for i in 0..target_frames {
                let src_f = i as f64 / ratio;
                let src_i = src_f as usize;
                let frac  = (src_f - src_i as f64) as f32;

                let i0 = (src_i * input_ch).min(chunk.len().saturating_sub(input_ch));
                let i1 = ((src_i + 1) * input_ch).min(chunk.len().saturating_sub(input_ch));

                let l0 = chunk[i0];
                let r0 = if input_ch >= 2 { chunk[i0 + 1] } else { l0 };
                let l1 = chunk[i1];
                let r1 = if input_ch >= 2 { chunk[i1 + 1] } else { l1 };

                let l = l0 + (l1 - l0) * frac;
                let r = r0 + (r1 - r0) * frac;

                opus_pcm.push((l.clamp(-1.0, 1.0) * i16::MAX as f32) as i16);
                opus_pcm.push((r.clamp(-1.0, 1.0) * i16::MAX as f32) as i16);
            }

            // Encode complete 20ms Opus frames
            while opus_pcm.len() >= FRAME_SIZE * OPUS_CH {
                let frame: Vec<i16> = opus_pcm.drain(..FRAME_SIZE * OPUS_CH).collect();
                let mut out = vec![0u8; 4000];
                if let Ok(n) = encoder.encode(&frame, &mut out) {
                    out.truncate(n);
                    rt_enc.spawn(write_encoded_frame(
                        Arc::clone(&track_enc), out, DURATION_MS,
                    ));
                }
            }
        }
    });

    // cpal callback: push raw samples into ring buffer
    let stream = match sup_cfg.sample_format() {
        cpal::SampleFormat::F32 => {
            let ring2 = Arc::clone(&ring);
            device.build_input_stream(
                &stream_cfg,
                move |data: &[f32], _| {
                    ring2.lock().unwrap().extend_from_slice(data);
                },
                |e| eprintln!("[cpal] {e}"),
                None,
            )?
        }
        cpal::SampleFormat::I16 => {
            let ring2 = Arc::clone(&ring);
            device.build_input_stream(
                &stream_cfg,
                move |data: &[i16], _| {
                    let mut buf = ring2.lock().unwrap();
                    buf.extend(data.iter().map(|&s| s as f32 / i16::MAX as f32));
                },
                |e| eprintln!("[cpal] {e}"),
                None,
            )?
        }
        cpal::SampleFormat::U16 => {
            let ring2 = Arc::clone(&ring);
            device.build_input_stream(
                &stream_cfg,
                move |data: &[u16], _| {
                    let mut buf = ring2.lock().unwrap();
                    buf.extend(data.iter().map(|&s| (s as f32 - 32768.0) / 32768.0));
                },
                |e| eprintln!("[cpal] {e}"),
                None,
            )?
        }
        other => return Err(anyhow!("unsupported sample format: {other:?}")),
    };

    stream.play()?;
    println!("[cpal] audio capture started ({DURATION_MS}ms frames / {OPUS_BITRATE}bps)");
    loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
}
