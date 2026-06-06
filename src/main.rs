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
use rcgen::generate_simple_self_signed;
use std::net::UdpSocket;
use std::process::Command;
use std::sync::Arc;
use tokio::runtime::Handle;
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors,
        media_engine::{MediaEngine, MIME_TYPE_OPUS},
        setting_engine::SettingEngine,
        APIBuilder,
    },
    ice::udp_network::{EphemeralUDP, UDPNetwork},
    ice_transport::ice_server::RTCIceServer,
    interceptor::registry::Registry,
    media::Sample,
    peer_connection::{
        configuration::RTCConfiguration,
        peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
    track::track_local::{track_local_static_sample::TrackLocalStaticSample, TrackLocal},
};

struct AppState {
    api: webrtc::api::API,
    track: Arc<TrackLocalStaticSample>,
}

/// Detect the local WiFi/LAN IP by routing toward 8.8.8.8 (no packet sent).
fn local_lan_ip() -> String {
    let s = UdpSocket::bind("0.0.0.0:0").unwrap_or_else(|_| panic!("bind failed"));
    s.connect("8.8.8.8:80").unwrap_or_else(|_| panic!("connect probe failed"));
    s.local_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    // ── WebRTC setup ──────────────────────────────────────────────────────────
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut media_engine)?;

    let mut ephemeral_udp = EphemeralUDP::default();
    ephemeral_udp.set_ports(50000, 50050)?;

    let mut setting_engine = SettingEngine::default();
    setting_engine.set_udp_network(UDPNetwork::Ephemeral(ephemeral_udp));

    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_setting_engine(setting_engine)
        .build();

    let track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            clock_rate: 48_000,
            channels: 2,
            sdp_fmtp_line: "minptime=10;useinbandfec=1;stereo=1".to_string(),
            ..Default::default()
        },
        "desktop_audio".to_owned(),
        "mypro".to_owned(),
    ));

    // ── Audio capture thread ──────────────────────────────────────────────────
    let audio_track = Arc::clone(&track);
    let rt_handle = Handle::current();

    std::thread::spawn(move || {
        if let Err(err) = start_audio_capture(audio_track, rt_handle) {
            eprintln!("audio capture failed: {err:?}");
        }
    });

    // ── Axum router ───────────────────────────────────────────────────────────
    let app_state = Arc::new(AppState { api, track });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/offer", post(handle_offer))
        .with_state(app_state);

    // ── Self-signed TLS cert (generated at runtime, no OpenSSL needed) ────────
    let lan_ip = local_lan_ip();
    println!("Detected LAN IP: {lan_ip}");

    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        lan_ip.clone(),
    ];

    let cert = generate_simple_self_signed(subject_alt_names)
        .map_err(|e| anyhow!("cert generation failed: {e}"))?;

    let cert_pem = cert.cert.pem();
    let key_pem  = cert.key_pair.serialize_pem();

    // Persist cert.pem so you can install it as a trusted CA on Android.
    // Settings → Security → Install a certificate → CA certificate → pick cert.pem
    if let Err(e) = std::fs::write("cert.pem", &cert_pem) {
        eprintln!("Warning: could not write cert.pem: {e}");
    } else {
        println!("cert.pem written — install on Android to remove the browser warning");
    }

    let tls_config = RustlsConfig::from_pem(
        cert_pem.into_bytes(),
        key_pem.into_bytes(),
    )
    .await
    .map_err(|e| anyhow!("TLS config failed: {e}"))?;

    // ── Bind HTTPS ────────────────────────────────────────────────────────────
    let bind_addr: std::net::SocketAddr = "0.0.0.0:8443".parse()?;
    println!("Listening on https://{lan_ip}:8443");
    println!("Open on your phone: https://{lan_ip}:8443");

    axum_server::bind_rustls(bind_addr, tls_config)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn handle_offer(
    State(state): State<Arc<AppState>>,
    Json(offer): Json<RTCSessionDescription>,
) -> Json<RTCSessionDescription> {
    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        }],
        ..Default::default()
    };

    let peer_connection = Arc::new(
        state
            .api
            .new_peer_connection(config)
            .await
            .expect("failed to create peer connection"),
    );

    let rtp_sender = peer_connection
        .add_track(Arc::clone(&state.track) as Arc<dyn TrackLocal + Send + Sync>)
        .await
        .expect("failed to add audio track");

    tokio::spawn(async move {
        let mut rtcp_buf = vec![0u8; 1500];
        while rtp_sender.read(&mut rtcp_buf).await.is_ok() {}
    });

    peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        println!("Peer connection state: {s}");
        Box::pin(async {})
    }));

    peer_connection
        .set_remote_description(offer)
        .await
        .expect("failed to set remote description");

    let answer = peer_connection
        .create_answer(None)
        .await
        .expect("failed to create answer");

    peer_connection
        .set_local_description(answer)
        .await
        .expect("failed to set local description");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let final_answer = peer_connection
        .local_description()
        .await
        .expect("missing local description after ICE gathering");

    Json(final_answer)
}

fn pulse_default_source_name() -> Option<String> {
    let output = Command::new("pactl")
        .arg("get-default-source")
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let s = String::from_utf8(output.stdout).ok()?;
    let s = s.trim().to_string();

    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn choose_input_device() -> Result<cpal::Device> {
    let host = cpal::default_host();
    let devices: Vec<cpal::Device> = host.input_devices()?.collect();

    if devices.is_empty() {
        return Err(anyhow!("no input devices found"));
    }

    let default_source = pulse_default_source_name();
    if let Some(ref src) = default_source {
        println!("Pulse default source: {src}");
    } else {
        println!("Pulse default source: <not found>");
    }

    for device in &devices {
        if let Ok(desc) = device.description() {
            let name = desc.name().to_ascii_lowercase();
            let ext = desc.extended().join(" ").to_ascii_lowercase();

            if let Some(ref src) = default_source {
                let src_l = src.to_ascii_lowercase();
                if name.contains(&src_l) || ext.contains(&src_l) {
                    println!("Selected device from Pulse default source: {:?}", desc);
                    return Ok(device.clone());
                }
            }
        }
    }

    for device in &devices {
        if let Ok(desc) = device.description() {
            let name = desc.name().to_ascii_lowercase();
            let ext = desc.extended().join(" ").to_ascii_lowercase();

            if name.contains("monitor") || ext.contains("monitor") {
                println!("Selected monitor device fallback: {:?}", desc);
                return Ok(device.clone());
            }
        }
    }

    if let Some(device) = host.default_input_device() {
        if let Ok(desc) = device.description() {
            println!("Selected host default input device fallback: {:?}", desc);
        }
        return Ok(device);
    }

    Err(anyhow!("no suitable input device found"))
}

async fn write_encoded_frame(
    track: Arc<TrackLocalStaticSample>,
    payload: Vec<u8>,
    duration_ms: u64,
) {
    let _ = track
        .write_sample(&Sample {
            data: Bytes::from(payload),
            duration: std::time::Duration::from_millis(duration_ms),
            ..Default::default()
        })
        .await;
}

fn start_audio_capture(track: Arc<TrackLocalStaticSample>, rt_handle: Handle) -> Result<()> {
    let device = choose_input_device()?;
    let supported_config = device.default_input_config()?;

    println!("Using input device: {:?}", device.description());
    println!("Input config: {:?}", supported_config);

    let input_sample_rate = supported_config.sample_rate().0 as usize;
    let input_channels    = supported_config.channels() as usize;

    let opus_sample_rate = 48_000usize;
    let opus_channels    = 2usize;
    let frame_size       = 480usize;

    let mut encoder = opus::Encoder::new(
        opus_sample_rate as u32,
        opus::Channels::Stereo,
        opus::Application::Audio,
    )?;
    encoder.set_bitrate(opus::Bitrate::Bits(128_000))?;
    encoder.set_vbr(true)?;
    encoder.set_inband_fec(true)?;

    let mut input_pcm: Vec<f32> = Vec::with_capacity(8192);
    let mut opus_pcm: Vec<i16>  = Vec::with_capacity(frame_size * opus_channels * 4);

    let stream_config: cpal::StreamConfig = supported_config.clone().into();

    macro_rules! build_stream {
        ($data:expr, $convert:expr) => {{
            input_pcm.extend($data.iter().map($convert));

            let frames_available = input_pcm.len() / input_channels;
            if frames_available == 0 { return; }

            let ratio     = opus_sample_rate as f64 / input_sample_rate as f64;
            let out_frames = ((frames_available as f64) * ratio) as usize;
            if out_frames == 0 { return; }

            let mut resampled: Vec<i16> = Vec::with_capacity(out_frames * opus_channels);

            for out_idx in 0..out_frames {
                let src_frame = ((out_idx as f64 / ratio) as usize)
                    .min(frames_available.saturating_sub(1));

                let left  = input_pcm[src_frame * input_channels];
                let right = if input_channels >= 2 {
                    input_pcm[src_frame * input_channels + 1]
                } else {
                    left
                };

                resampled.push((left  * i16::MAX as f32).clamp(i16::MIN as f32, i16::MAX as f32) as i16);
                resampled.push((right * i16::MAX as f32).clamp(i16::MIN as f32, i16::MAX as f32) as i16);
            }

            input_pcm.clear();
            opus_pcm.extend_from_slice(&resampled);

            while opus_pcm.len() >= frame_size * opus_channels {
                let frame: Vec<i16> = opus_pcm.drain(..frame_size * opus_channels).collect();
                let mut opus_out = vec![0u8; 4000];

                if let Ok(len) = encoder.encode(&frame, &mut opus_out) {
                    opus_out.truncate(len);
                    let track = Arc::clone(&track);
                    rt_handle.spawn(write_encoded_frame(track, opus_out, 10));
                }
            }
        }};
    }

    let stream = match supported_config.sample_format() {
        cpal::SampleFormat::F32 => device.build_input_stream(
            &stream_config,
            move |data: &[f32], _| { build_stream!(data, |&s| s); },
            move |err| eprintln!("cpal stream error: {err}"),
            None,
        )?,

        cpal::SampleFormat::I16 => device.build_input_stream(
            &stream_config,
            move |data: &[i16], _| {
                build_stream!(data, |&s| s as f32 / i16::MAX as f32);
            },
            move |err| eprintln!("cpal stream error: {err}"),
            None,
        )?,

        cpal::SampleFormat::U16 => device.build_input_stream(
            &stream_config,
            move |data: &[u16], _| {
                build_stream!(data, |&s| (s as f32 - 32768.0) / 32768.0);
            },
            move |err| eprintln!("cpal stream error: {err}"),
            None,
        )?,

        other => return Err(anyhow!("unsupported sample format: {other:?}")),
    };

    stream.play()?;
    println!("Audio capture started");

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
