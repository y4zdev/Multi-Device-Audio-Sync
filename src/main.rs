use anyhow::{anyhow, Result};
use axum::{
    extract::State,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use axum_server::tls_rustls::RustlsConfig;
use bytes::Bytes;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use rcgen::{CertificateParams, DistinguishedName, SanType};
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

// ── App state ────────────────────────────────────────────────────────────────
struct AppState {
    api:   webrtc::api::API,
    track: Arc<TrackLocalStaticSample>,
}

// ── Detect LAN IP (no packet sent) ───────────────────────────────────────────
fn local_lan_ip() -> String {
    UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| { s.connect("8.8.8.8:80")?; s.local_addr() })
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string())
}

// ── Generate self-signed cert with proper SANs ───────────────────────────────
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
    params.subject_alt_names.push(SanType::IpAddress(
        "127.0.0.1".parse().unwrap(),
    ));

    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| anyhow!("KeyPair::generate: {e}"))?;

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| anyhow!("self_signed: {e}"))?;

    Ok((cert.pem().into_bytes(), key_pair.serialize_pem().into_bytes()))
}

// ── Entry point ───────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    // Install ring as the rustls crypto provider (must be first)
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("Failed to install rustls ring CryptoProvider"))?;

    // WebRTC media engine
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut media_engine)?;

    // Pin ICE UDP to port range 30690-30710
    let ephemeral_udp = EphemeralUDP::new(30690, 30710)?;
    let mut setting_engine = SettingEngine::default();
    setting_engine.set_udp_network(UDPNetwork::Ephemeral(ephemeral_udp));

    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_setting_engine(setting_engine)
        .build();

    // Shared Opus track
    let track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type:      MIME_TYPE_OPUS.to_owned(),
            clock_rate:     48_000,
            channels:       2,
            sdp_fmtp_line:  "minptime=10;useinbandfec=1;stereo=1".to_string(),
            ..Default::default()
        },
        "desktop_audio".to_owned(),
        "mypro".to_owned(),
    ));

    // Audio capture on a dedicated OS thread
    let audio_track = Arc::clone(&track);
    let rt_handle   = Handle::current();
    std::thread::spawn(move || {
        if let Err(e) = start_audio_capture(audio_track, rt_handle) {
            eprintln!("audio capture failed: {e:?}");
        }
    });

    // Axum router
    let state = Arc::new(AppState { api, track });
    let app = Router::new()
        .route("/",      get(index_handler))
        .route("/offer", post(handle_offer))
        .with_state(state);

    // Self-signed TLS
    let lan_ip = local_lan_ip();
    println!("Detected LAN IP : {lan_ip}");

    let (cert_pem, key_pem) = make_self_signed_cert(&lan_ip)?;

    match std::fs::write("cert.pem", &cert_pem) {
        Ok(_)  => println!("cert.pem written  (install as CA on Android to skip browser warning)"),
        Err(e) => eprintln!("Warning: could not write cert.pem: {e}"),
    }

    let tls_config = RustlsConfig::from_pem(cert_pem, key_pem)
        .await
        .map_err(|e| anyhow!("TLS config error: {e}"))?;

    let bind_addr: std::net::SocketAddr = "0.0.0.0:8443".parse()?;
    println!("Listening on  https://{lan_ip}:8443");
    println!("Open on phone https://{lan_ip}:8443");

    axum_server::bind_rustls(bind_addr, tls_config)
        .serve(app.into_make_service())
        .await
        .map_err(|e| anyhow!("server error: {e}"))
}

// ── Static page ───────────────────────────────────────────────────────────────
async fn index_handler() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

// ── WebRTC offer handler ──────────────────────────────────────────────────────
async fn handle_offer(
    State(state): State<Arc<AppState>>,
    Json(offer):  Json<RTCSessionDescription>,
) -> Json<RTCSessionDescription> {
    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        }],
        ..Default::default()
    };

    let pc = Arc::new(
        state.api.new_peer_connection(config).await
            .expect("failed to create peer connection"),
    );

    // Drain RTCP
    let rtp_sender = pc
        .add_track(Arc::clone(&state.track) as Arc<dyn TrackLocal + Send + Sync>)
        .await
        .expect("failed to add track");
    tokio::spawn(async move {
        let mut buf = vec![0u8; 1500];
        while rtp_sender.read(&mut buf).await.is_ok() {}
    });

    pc.on_peer_connection_state_change(Box::new(|s: RTCPeerConnectionState| {
        println!("PC state: {s}");
        Box::pin(async {})
    }));

    pc.set_remote_description(offer).await
        .expect("set_remote_description failed");

    let answer = pc.create_answer(None).await
        .expect("create_answer failed");

    // Wait for ICE gathering to complete
    let (tx, rx) = oneshot::channel::<()>();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    pc.on_ice_gathering_state_change(Box::new(move |state: RTCIceGathererState| {
        let tx = Arc::clone(&tx);
        Box::pin(async move {
            if state == RTCIceGathererState::Complete {
                if let Some(t) = tx.lock().await.take() {
                    let _ = t.send(());
                }
            }
        })
    }));

    pc.set_local_description(answer).await
        .expect("set_local_description failed");

    let _ = tokio::time::timeout(std::time::Duration::from_secs(4), rx).await;

    let final_answer = pc.local_description().await
        .expect("no local description");

    Json(final_answer)
}

// ── PulseAudio default source helper ─────────────────────────────────────────
fn pulse_default_source_name() -> Option<String> {
    let out = Command::new("pactl").arg("get-default-source").output().ok()?;
    if !out.status.success() { return None; }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

// ── Pick best input device ────────────────────────────────────────────────────
fn choose_input_device() -> Result<cpal::Device> {
    let host    = cpal::default_host();
    let devices: Vec<cpal::Device> = host.input_devices()?.collect();
    if devices.is_empty() { return Err(anyhow!("no input devices")); }

    let default_src = pulse_default_source_name();
    println!("Pulse default source: {}", default_src.as_deref().unwrap_or("<none>"));

    // 1. Match Pulse default source by name
    if let Some(ref src) = default_src {
        let src_l = src.to_ascii_lowercase();
        for d in &devices {
            let name = d.name().unwrap_or_default().to_ascii_lowercase();
            if name.contains(&src_l) {
                println!("Selected (pulse match): {}", d.name().unwrap_or_default());
                return Ok(d.clone());
            }
        }
    }

    // 2. Prefer monitor (loopback) device
    for d in &devices {
        let name = d.name().unwrap_or_default().to_ascii_lowercase();
        if name.contains("monitor") {
            println!("Selected (monitor): {}", d.name().unwrap_or_default());
            return Ok(d.clone());
        }
    }

    // 3. Fall back to host default
    host.default_input_device()
        .ok_or_else(|| anyhow!("no default input device"))
}

// ── Write one Opus frame to the WebRTC track ──────────────────────────────────
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

// ── Audio capture + encode ────────────────────────────────────────────────────
fn start_audio_capture(
    track:     Arc<TrackLocalStaticSample>,
    rt_handle: Handle,
) -> Result<()> {
    let device         = choose_input_device()?;
    let sup_cfg        = device.default_input_config()?;
    let input_sr       = sup_cfg.sample_rate().0 as usize;
    let input_ch       = sup_cfg.channels()     as usize;
    let stream_config: cpal::StreamConfig = sup_cfg.clone().into();

    println!("Input device : {}",  device.name().unwrap_or_default());
    println!("Input config : {:?}", sup_cfg);

    const OPUS_SR:    usize = 48_000;
    const OPUS_CH:    usize = 2;
    const FRAME_SIZE: usize = 480;   // 10 ms at 48 kHz

    let mut encoder = opus::Encoder::new(
        OPUS_SR as u32,
        opus::Channels::Stereo,
        opus::Application::Audio,
    )?;
    encoder.set_bitrate(opus::Bitrate::Bits(128_000))?;
    encoder.set_vbr(true)?;
    encoder.set_inband_fec(true)?;

    fn make_callback(
        track:        Arc<TrackLocalStaticSample>,
        rt_handle:    Handle,
        mut encoder:  opus::Encoder,
        input_sr:     usize,
        input_ch:     usize,
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
                let l   = input_pcm[src * input_ch];
                let r   = if input_ch >= 2 { input_pcm[src * input_ch + 1] } else { l };
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
                    rt_handle.spawn(write_encoded_frame(Arc::clone(&track), out, 10));
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
                &stream_config,
                move |data: &[f32], _| {
                    cb.lock().unwrap()(data.to_vec());
                },
                |e| eprintln!("stream error: {e}"),
                None,
            )?
        }
        cpal::SampleFormat::I16 => {
            let cb = Arc::clone(&cb);
            device.build_input_stream(
                &stream_config,
                move |data: &[i16], _| {
                    let f: Vec<f32> = data.iter().map(|&s| s as f32 / i16::MAX as f32).collect();
                    cb.lock().unwrap()(f);
                },
                |e| eprintln!("stream error: {e}"),
                None,
            )?
        }
        cpal::SampleFormat::U16 => {
            let cb = Arc::clone(&cb);
            device.build_input_stream(
                &stream_config,
                move |data: &[u16], _| {
                    let f: Vec<f32> = data.iter().map(|&s| (s as f32 - 32768.0) / 32768.0).collect();
                    cb.lock().unwrap()(f);
                },
                |e| eprintln!("stream error: {e}"),
                None,
            )?
        }
        other => return Err(anyhow!("unsupported sample format: {other:?}")),
    };

    stream.play()?;
    println!("Audio capture started");
    loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
}
