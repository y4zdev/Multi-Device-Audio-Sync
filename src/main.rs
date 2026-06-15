use anyhow::{anyhow, Result};
use axum::{
    extract::State,
    response::Html,
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
    api:     Arc<webrtc::api::API>,
    streams: Arc<DashMap<String, Arc<TrackLocalStaticSample>>>,
}

// ── LAN IP detection ──────────────────────────────────────────────────────────

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
    params.subject_alt_names.push(SanType::IpAddress("127.0.0.1".parse().unwrap()));

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
            .build()
    );

    let state = AppState {
        api,
        streams: Arc::new(DashMap::new()),
    };

    let app = Router::new()
        .route("/",               get(index_handler))
        .route("/streams",        get(list_streams))
        .route("/sender/offer",   post(handle_sender_offer))
        .route("/receiver/offer", post(handle_receiver_offer))
        .route("/stream/{name}",  delete(remove_stream))
        .with_state(state);

    let lan_ip = local_lan_ip();
    println!("Detected LAN IP : {lan_ip}");

    let (cert_pem, key_pem) = make_self_signed_cert(&lan_ip)?;
    match std::fs::write("cert.pem", &cert_pem) {
        Ok(_)  => println!("cert.pem written"),
        Err(e) => eprintln!("Warning: could not write cert.pem: {e}"),
    }

    let tls_config = RustlsConfig::from_pem(cert_pem, key_pem)
        .await
        .map_err(|e| anyhow!("TLS config error: {e}"))?;

    let bind_addr: std::net::SocketAddr = "0.0.0.0:8443".parse()?;
    println!("Listening on  https://{lan_ip}:8443");
    println!("Sender   →  https://{lan_ip}:8443/?role=sender");
    println!("Receiver →  https://{lan_ip}:8443");

    axum_server::bind_rustls(bind_addr, tls_config)
        .serve(app.into_make_service())
        .await
        .map_err(|e| anyhow!("server error: {e}"))
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

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
            clock_rate:    48_000,
            channels:      2,
            sdp_fmtp_line: "minptime=10;useinbandfec=1;stereo=1".to_string(),
            ..Default::default()
        },
        name.clone(),
        "mypro".to_owned(),
    ));
    state.streams.insert(name.clone(), Arc::clone(&track));
    println!("[stream] registered: {name} (source={source})");

    // ── System audio: server-side cpal capture ────────────────────────────────
    if source == "system" {
        let rt_handle = Handle::current();
        let name2 = name.clone();
        std::thread::spawn(move || {
            if let Err(e) = start_audio_capture(track, rt_handle) {
                eprintln!("[{name2}] audio capture error: {e:?}");
            }
        });

        let pc = build_peer_connection(&state, false).await;
        pc.on_peer_connection_state_change(Box::new({
            let name3   = name.clone();
            let streams = Arc::clone(&state.streams);
            move |s: RTCPeerConnectionState| {
                let name3   = name3.clone();
                let streams = Arc::clone(&streams);
                println!("[sender/system] [{name3}] PC state: {s}");
                if matches!(s, RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) {
                    streams.remove(&name3);
                    println!("[stream] unregistered: {name3}");
                }
                Box::pin(async {})
            }
        }));
        return negotiate_and_answer(pc, req.sdp).await;
    }

    // ── Browser / display audio: inbound WebRTC from browser ──────────────────
    let pc = build_peer_connection(&state, true).await;

    pc.on_track(Box::new({
        let relay_track = Arc::clone(&track);
        let name2 = name.clone();
        move |remote_track, _, _| {
            let relay = Arc::clone(&relay_track);
            let name3 = name2.clone();
            println!("[sender/browser] [{name3}] inbound track: {} {}",
                remote_track.kind(), remote_track.id());
            tokio::spawn(async move {
                // read_rtp() returns Result<(rtp::packet::Packet, Attributes)>
                // Forward only the Opus payload bytes from each RTP packet.
                while let Ok((rtp_pkt, _)) = remote_track.read_rtp().await {
                    let payload = Bytes::copy_from_slice(&rtp_pkt.payload);
                    let _ = relay.write_sample(&Sample {
                        data:     payload,
                        duration: std::time::Duration::from_millis(10),
                        ..Default::default()
                    }).await;
                }
                println!("[sender/browser] [{name3}] inbound track ended");
            });
            Box::pin(async {})
        }
    }));

    pc.on_peer_connection_state_change(Box::new({
        let name2   = name.clone();
        let streams = Arc::clone(&state.streams);
        move |s: RTCPeerConnectionState| {
            let name2   = name2.clone();
            let streams = Arc::clone(&streams);
            println!("[sender/browser] [{name2}] PC state: {s}");
            if matches!(s, RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) {
                streams.remove(&name2);
                println!("[stream] unregistered: {name2}");
            }
            Box::pin(async {})
        }
    }));

    negotiate_and_answer(pc, req.sdp).await
}

async fn handle_receiver_offer(
    State(state): State<AppState>,
    Json(req): Json<ReceiverOfferRequest>,
) -> Json<RTCSessionDescription> {
    let pc = build_peer_connection(&state, false).await;

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

// ── WebRTC helpers ────────────────────────────────────────────────────────────

async fn build_peer_connection(
    state: &AppState,
    _is_sender: bool,
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
            .expect("failed to create peer connection")
    )
}

async fn negotiate_and_answer(
    pc: Arc<webrtc::peer_connection::RTCPeerConnection>,
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

// ── Audio capture (system source) ────────────────────────────────────────────

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
    track: Arc<TrackLocalStaticSample>,
    payload: Vec<u8>,
    duration_ms: u64,
) {
    let _ = track.write_sample(&Sample {
        data:     Bytes::from(payload),
        duration: std::time::Duration::from_millis(duration_ms),
        ..Default::default()
    }).await;
}

fn start_audio_capture(
    track: Arc<TrackLocalStaticSample>,
    rt_handle: Handle,
) -> Result<()> {
    let device  = choose_input_device()?;
    let sup_cfg = device.default_input_config()?;
    let input_sr  = sup_cfg.sample_rate().0 as usize;
    let input_ch  = sup_cfg.channels() as usize;
    let stream_cfg: cpal::StreamConfig = sup_cfg.clone().into();

    println!("[cpal] device : {}", device.name().unwrap_or_default());
    println!("[cpal] config : {:?}", sup_cfg);

    const OPUS_SR:    usize = 48_000;
    const OPUS_CH:    usize = 2;
    const FRAME_SIZE: usize = 480;

    let mut encoder = opus::Encoder::new(
        OPUS_SR as u32,
        opus::Channels::Stereo,
        opus::Application::Audio,
    )?;
    encoder.set_bitrate(opus::Bitrate::Bits(128_000))?;
    encoder.set_vbr(true)?;
    encoder.set_inband_fec(true)?;

    fn make_callback(
        track:       Arc<TrackLocalStaticSample>,
        rt_handle:   Handle,
        mut encoder: opus::Encoder,
        input_sr:    usize,
        input_ch:    usize,
    ) -> impl FnMut(Vec<f32>) + Send + 'static {
        let mut input_pcm: Vec<f32> = Vec::with_capacity(8192);
        let mut opus_pcm:  Vec<i16> = Vec::with_capacity(FRAME_SIZE * OPUS_CH * 4);

        move |samples: Vec<f32>| {
            input_pcm.extend_from_slice(&samples);
            let frames_in = input_pcm.len() / input_ch;
            if frames_in == 0 { return; }

            let ratio      = OPUS_SR as f64 / input_sr as f64;
            let frames_out = ((frames_in as f64) * ratio) as usize;
            if frames_out == 0 { return; }

            let mut resampled = Vec::with_capacity(frames_out * OPUS_CH);
            for i in 0..frames_out {
                let src = ((i as f64 / ratio) as usize).min(frames_in - 1);
                let l = input_pcm[src * input_ch];
                let r = if input_ch >= 2 { input_pcm[src * input_ch + 1] } else { l };
                resampled.push((l * i16::MAX as f32).clamp(i16::MIN as f32, i16::MAX as f32) as i16);
                resampled.push((r * i16::MAX as f32).clamp(i16::MIN as f32, i16::MAX as f32) as i16);
            }
            input_pcm.clear();

            opus_pcm.extend_from_slice(&resampled);
            while opus_pcm.len() >= FRAME_SIZE * OPUS_CH {
                let frame: Vec<i16> = opus_pcm.drain(..FRAME_SIZE * OPUS_CH).collect();
                let mut out = vec![0u8; 4000];
                if let Ok(n) = encoder.encode(&frame, &mut out) {
                    out.truncate(n);
                    rt_handle.spawn(write_encoded_frame(
                        Arc::clone(&track), out, 10,
                    ));
                }
            }
        }
    }

    let cb = make_callback(
        Arc::clone(&track), rt_handle.clone(), encoder, input_sr, input_ch,
    );
    let cb = Arc::new(std::sync::Mutex::new(cb));

    let stream = match sup_cfg.sample_format() {
        cpal::SampleFormat::F32 => {
            let cb = Arc::clone(&cb);
            device.build_input_stream(
                &stream_cfg,
                move |data: &[f32], _| { cb.lock().unwrap()(data.to_vec()); },
                |e| eprintln!("[cpal] stream error: {e}"),
                None,
            )?
        }
        cpal::SampleFormat::I16 => {
            let cb = Arc::clone(&cb);
            device.build_input_stream(
                &stream_cfg,
                move |data: &[i16], _| {
                    let f: Vec<f32> = data.iter()
                        .map(|&s| s as f32 / i16::MAX as f32)
                        .collect();
                    cb.lock().unwrap()(f);
                },
                |e| eprintln!("[cpal] stream error: {e}"),
                None,
            )?
        }
        cpal::SampleFormat::U16 => {
            let cb = Arc::clone(&cb);
            device.build_input_stream(
                &stream_cfg,
                move |data: &[u16], _| {
                    let f: Vec<f32> = data.iter()
                        .map(|&s| (s as f32 - 32768.0) / 32768.0)
                        .collect();
                    cb.lock().unwrap()(f);
                },
                |e| eprintln!("[cpal] stream error: {e}"),
                None,
            )?
        }
        other => return Err(anyhow!("unsupported sample format: {other:?}")),
    };

    stream.play()?;
    println!("[cpal] audio capture started");
    loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
}
