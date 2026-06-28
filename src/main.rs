use anyhow::{anyhow, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State, DefaultBodyLimit,
    },
    response::Html,
    routing::{delete, get, patch, post},
    Json, Router,
};
use bytes::Bytes;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::net::UdpSocket;
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, oneshot};

mod db;
mod auth;
pub mod player;

// Removed WebRTC
// ── constants ─────────────────────────────────────────────────────────────────
const OPUS_SR: usize    = 48_000;
const OPUS_CH: usize    = 2;
const FRAME_SIZE: usize = 960;   // 20 ms @ 48 kHz
const DURATION_MS: u64  = 20;
const OPUS_BITRATE: i32 = 192_000;

// ── Device model ──────────────────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceRole {
    Mic,
    Speaker,
    Manager,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceStatus {
    Online,
    Offline,
    Live,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub id:               String,
    pub name:             String,
    pub role:             DeviceRole,
    pub status:           DeviceStatus,
    pub assigned_streams: Vec<String>,
    pub volume:           f32,
    pub last_seen:        u64,
}

// ── Control events (WebSocket broadcast) ──────────────────────────────────────

#[derive(Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlEvent {
    DeviceJoined   { device: DeviceInfo },
    DeviceUpdated  { device: DeviceInfo },
    DeviceLeft     { id: String },
    StreamAdded    { name: String },
    StreamRemoved  { name: String },
    VolumeChanged  { device_id: String, volume: f32 },
    RouteChanged   { source: String, speaker_id: String, connected: bool },
    SetVolume      { device_id: String, stream: Option<String>, volume: f32 },
}

// ── Request / Response types ──────────────────────────────────────────────────

// Removed WebRTC structs

#[derive(Serialize)]
struct StreamsResponse {
    streams: Vec<String>,
}

#[derive(Deserialize)]
struct RegisterDeviceRequest {
    id:   Option<String>,
    name: String,
    role: DeviceRole,
}

#[derive(Deserialize)]
struct PatchVolumeRequest {
    volume: f32,
}

#[derive(Deserialize)]
struct PatchStreamsRequest {
    streams: Vec<String>,
}

// ── App state ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    streams:  Arc<DashMap<String, broadcast::Sender<Bytes>>>,
    devices:  Arc<DashMap<String, DeviceInfo>>,
    event_tx: broadcast::Sender<ControlEvent>,
    db:       Arc<db::Db>,
    player_state: Arc<player::PlayerState>,
}

// ── LAN IP ────────────────────────────────────────────────────────────────────

fn local_lan_ip() -> String {
    UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| { s.connect("8.8.8.8:80")?; s.local_addr() })
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_string())
}


// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let db = Arc::new(db::Db::new("airlink.db").unwrap());
    db.init_default_admin();

    let (event_tx, _) = broadcast::channel::<ControlEvent>(256);

    let player_state = player::PlayerState::new();

    let state = Arc::new(AppState {
        streams:  Arc::new(DashMap::new()),
        devices:  Arc::new(DashMap::new()),
        event_tx,
        db,
        player_state: player_state.clone(),
    });
    
    tokio::spawn(player::start_player_loop(state.clone()));

    let state_clone = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let mut to_remove = Vec::new();
            let mut to_offline = Vec::new();

            for mut entry in state_clone.devices.iter_mut() {
                let diff = now.saturating_sub(entry.last_seen);
                if diff > 300_000 {
                    to_remove.push(entry.key().clone());
                } else if diff > 15_000 && matches!(entry.status, DeviceStatus::Online) {
                    entry.value_mut().status = DeviceStatus::Offline;
                    to_offline.push(entry.value().clone());
                }
            }

            for id in to_remove {
                state_clone.devices.remove(&id);
                let _ = state_clone.event_tx.send(ControlEvent::DeviceLeft { id: id.clone() });
                println!("[device] swept: {id}");
            }
            for dev in to_offline {
                let _ = state_clone.event_tx.send(ControlEvent::DeviceUpdated { device: dev });
            }
        }
    });

    use axum::middleware;
    
    let pages = Router::new()
        .nest("/api/player", player::player_routes().layer(DefaultBodyLimit::disable()))
        .with_state(state.clone())
        .route("/sender",   get(sender_handler))
        .route("/receiver", get(receiver_handler))
        .route("/controller",get(controller_handler))
        .route("/admin",    get(admin_handler))
        .route("/admin/api/users", get(auth::list_users).post(auth::create_user))
        .route("/admin/api/settings", get(auth::get_settings).post(auth::update_settings))
        .route("/",         get(receiver_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth::require_role));

    let app = Router::new()
        .merge(pages)
        .route("/login",    get(auth::login_page).post(auth::login_post))
        .route("/logout",   get(auth::logout))
        // Stream API
        .route("/streams",        get(list_streams))
        .route("/start_system_capture", post(start_system_capture_handler))
        .route("/ws/sender/{name}", get(ws_sender_handler))
        .route("/ws/receiver/{name}", get(ws_receiver_handler))
        .route("/stream/{name}",  delete(remove_stream))
        // Device API
        .route("/device/register",     post(register_device))
        .route("/devices",             get(list_devices))
        .route("/device/{id}",         delete(remove_device))
        .route("/device/{id}/volume",  patch(patch_device_volume))
        .route("/device/{id}/streams", patch(patch_device_streams))
        // WebSocket control bus
        .route("/ws", get(ws_handler))
        .with_state(state);

    let bind_addr: std::net::SocketAddr = "0.0.0.0:443".parse()?;
    let acme_domain = "airlink.y4z.dev".to_string(); // Or from db

    println!("─────────────────────────────────────────────");
    println!(" Receiver         : https://{acme_domain}");
    println!(" Sender           : https://{acme_domain}/sender");
    println!(" Manager          : https://{acme_domain}/admin");
    println!("─────────────────────────────────────────────");

    use rustls_acme::{caches::DirCache, AcmeConfig};
    use tokio_stream::StreamExt;

    // We use a second domain (nip.io) to bypass Let's Encrypt's 5 duplicate certs per week limit
    println!(" ACME Directory   : Let's Encrypt Production");

    let mut state = AcmeConfig::new(vec![acme_domain, "173.234.15.93.nip.io".to_string()])
        .contact(vec!["mailto:admin@y4z.dev".to_string()])
        .cache(DirCache::new(".cert_cache_prod")) // Use a brand new cache dir
        .directory_lets_encrypt(true)
        .state();

    let rustls_config = state.default_rustls_config();
    let acceptor = state.axum_acceptor(rustls_config);

    tokio::spawn(async move {
        loop {
            match state.next().await {
                Some(Ok(ok)) => println!("[ACME] event: {:?}", ok),
                Some(Err(err)) => eprintln!("[ACME] error: {:?}", err),
                None => break,
            }
        }
    });

    // Spawn HTTP to HTTPS redirect server
    let http_bind_addr: std::net::SocketAddr = "0.0.0.0:80".parse()?;
    tokio::spawn(async move {
        let app = axum::Router::new().fallback(|req: axum::extract::Request| async move {
            let host = req.headers().get(axum::http::header::HOST)
                .and_then(|h| h.to_str().ok())
                .unwrap_or("airlink.y4z.dev");
            let path = req.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
            let https_url = format!("https://{}{}", host, path);
            axum::response::Redirect::permanent(&https_url)
        });
        
        if let Ok(listener) = tokio::net::TcpListener::bind(http_bind_addr).await {
            println!(" Redirect server  : http://0.0.0.0:80 -> https");
            let _ = axum::serve(listener, app).await;
        } else {
            eprintln!("Warning: Could not bind to port 80 for HTTP redirect");
        }
    });

    axum_server::bind(bind_addr)
        .acceptor(acceptor)
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

async fn controller_handler() -> Html<&'static str> {
    Html(include_str!("../static/controller.html"))
}

async fn admin_handler() -> Html<&'static str> {
    Html(include_str!("../static/admin.html"))
}


// ── Stream API handlers ────────────────────────────────────────────────────────

async fn list_streams(
    State(state): State<Arc<AppState>>,
) -> Json<StreamsResponse> {
    let streams: Vec<String> = state.streams.iter().map(|e| e.key().clone()).collect();
    Json(StreamsResponse { streams })
}

async fn remove_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> axum::http::StatusCode {
    state.streams.remove(&name);
    let _ = state.event_tx.send(ControlEvent::StreamRemoved { name: name.clone() });
    println!("[stream] removed: {name}");
    axum::http::StatusCode::NO_CONTENT
}

// ── Device API handlers ────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn register_device(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterDeviceRequest>,
) -> (axum::http::StatusCode, Json<DeviceInfo>) {
    let mut existing = None;
    for entry in state.devices.iter() {
        if entry.value().name == req.name && entry.value().role == req.role {
            if matches!(entry.value().status, DeviceStatus::Offline) {
                existing = Some(entry.value().clone());
                break;
            }
        }
    }

    let device = if let Some(mut dev) = existing {
        dev.status = DeviceStatus::Online;
        dev.last_seen = now_ms();
        dev
    } else {
        let id = req.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        DeviceInfo {
            id,
            name:             req.name,
            role:             req.role,
            status:           DeviceStatus::Online,
            assigned_streams: vec![],
            volume:           1.0,
            last_seen:        now_ms(),
        }
    };

    state.devices.insert(device.id.clone(), device.clone());
    let _ = state.event_tx.send(ControlEvent::DeviceJoined { device: device.clone() });
    println!("[device] registered: {} ({})", device.name, device.id);
    (axum::http::StatusCode::CREATED, Json(device))
}

async fn list_devices(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<DeviceInfo>> {
    let devices: Vec<DeviceInfo> = state.devices.iter().map(|e| e.value().clone()).collect();
    Json(devices)
}

async fn remove_device(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> axum::http::StatusCode {
    state.devices.remove(&id);
    let _ = state.event_tx.send(ControlEvent::DeviceLeft { id: id.clone() });
    println!("[device] removed: {id}");
    axum::http::StatusCode::NO_CONTENT
}

async fn patch_device_volume(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<PatchVolumeRequest>,
) -> axum::http::StatusCode {
    if let Some(mut dev) = state.devices.get_mut(&id) {
        dev.volume = req.volume.clamp(0.0, 2.0);
        dev.last_seen = now_ms();
        let _ = state.event_tx.send(ControlEvent::VolumeChanged {
            device_id: id.clone(),
            volume: dev.volume,
        });
        let _ = state.event_tx.send(ControlEvent::SetVolume {
            device_id: id.clone(),
            stream:    None,
            volume:    dev.volume,
        });
        axum::http::StatusCode::NO_CONTENT
    } else if state.streams.contains_key(&id) {
        // Broadcast SetVolume for the stream sender
        let _ = state.event_tx.send(ControlEvent::SetVolume {
            device_id: id.clone(),
            stream:    None,
            volume:    req.volume.clamp(0.0, 2.0),
        });
        axum::http::StatusCode::NO_CONTENT
    } else {
        axum::http::StatusCode::NOT_FOUND
    }
}

async fn patch_device_streams(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<PatchStreamsRequest>,
) -> axum::http::StatusCode {
    if let Some(mut dev) = state.devices.get_mut(&id) {
        let old = dev.assigned_streams.clone();
        dev.assigned_streams = req.streams.clone();
        dev.last_seen = now_ms();
        for s in &req.streams {
            if !old.contains(s) {
                let _ = state.event_tx.send(ControlEvent::RouteChanged {
                    source:     s.clone(),
                    speaker_id: id.clone(),
                    connected:  true,
                });
            }
        }
        for s in &old {
            if !req.streams.contains(s) {
                let _ = state.event_tx.send(ControlEvent::RouteChanged {
                    source:     s.clone(),
                    speaker_id: id.clone(),
                    connected:  false,
                });
            }
        }
        axum::http::StatusCode::NO_CONTENT
    } else {
        axum::http::StatusCode::NOT_FOUND
    }
}

// ── WebSocket control bus ─────────────────────────────────────────────────────

async fn ws_handler(
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.event_tx.subscribe();

    let snapshot_streams: Vec<String> = state.streams.iter().map(|e| e.key().clone()).collect();
    let snapshot_devices: Vec<DeviceInfo> = state.devices.iter().map(|e| e.value().clone()).collect();
    let snap = serde_json::json!({
        "type": "snapshot",
        "streams": snapshot_streams,
        "devices": snapshot_devices,
    });
    if socket.send(Message::Text(snap.to_string().into())).await.is_err() {
        return;
    }

    let mut current_device_id: Option<String> = None;
    let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
    ping_interval.tick().await; // consume the immediate first tick
    let mut pong_deadline: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(ev) => {
                        let text = match serde_json::to_string(&ev) {
                            Ok(t) => t,
                            Err(_) => continue,
                        };
                        if socket.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
            _ = ping_interval.tick() => {
                if let Some(dl) = pong_deadline {
                    if tokio::time::Instant::now() > dl {
                        // Pong never arrived within 8s - connection is dead
                        println!("[ws] pong timeout - connection dead");
                        break;
                    }
                }
                if socket.send(Message::Ping(vec![].into())).await.is_err() {
                    break;
                }
                // Set pong deadline 8s from now
                pong_deadline = Some(tokio::time::Instant::now() + tokio::time::Duration::from_secs(8));
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&t) {
                            if val.get("type").and_then(|v| v.as_str()) == Some("heartbeat") {
                                if let Some(id) = val.get("device_id").and_then(|v| v.as_str()) {
                                    if current_device_id.is_none() {
                                        current_device_id = Some(id.to_string());
                                    }
                                    let mut updated = None;
                                    if let Some(mut dev) = state.devices.get_mut(id) {
                                        dev.last_seen = now_ms();
                                        if matches!(dev.status, DeviceStatus::Offline) {
                                            dev.status = DeviceStatus::Online;
                                            updated = Some(dev.clone());
                                        } else {
                                            dev.status = DeviceStatus::Online;
                                        }
                                    }
                                    if let Some(dev) = updated {
                                        let _ = state.event_tx.send(ControlEvent::DeviceUpdated { device: dev });
                                    }
                                }
                            } else if val.get("type").and_then(|v| v.as_str()) == Some("ping") {
                                if let Some(ts) = val.get("ts").and_then(|v| v.as_u64()) {
                                    let pong = serde_json::json!({ "type": "pong", "ts": ts });
                                    let _ = socket.send(Message::Text(pong.to_string().into())).await;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Pong(_))) => { pong_deadline = None; }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }

    if let Some(id) = current_device_id {
        let mut updated = None;
        if let Some(mut dev) = state.devices.get_mut(&id) {
            dev.status = DeviceStatus::Offline;
            updated = Some(dev.clone());
        }
        if let Some(dev) = updated {
            println!("[device] OFFLINE (realtime): {}", dev.name);
            let _ = state.event_tx.send(ControlEvent::DeviceUpdated { device: dev });
        }
    }
}

// ── WebSocket streaming handlers ─────────────────────────────────────────────

async fn ws_sender_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_ws_sender(socket, state, name))
}

async fn handle_ws_sender(mut socket: WebSocket, state: Arc<AppState>, name: String) {
    let (tx, _) = broadcast::channel(100);
    state.streams.insert(name.clone(), tx.clone());
    let _ = state.event_tx.send(ControlEvent::StreamAdded { name: name.clone() });
    println!("[stream] registered: {name} (source=browser)");

    if name == "system" {
        // Automatically start system capture if the "system" sender connects via WS?
        // Actually, system capture runs on the server directly, not via WS!
        // Wait, for system capture we'll spawn a thread locally in a different endpoint or at startup.
    }

    let mut frame_count: u64 = 0;
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Binary(data)) => {
                let _ = tx.send(Bytes::from(data));
                frame_count += 1;
                if frame_count % 500 == 1 {
                    println!("[stream] {name}: frame #{frame_count}, subscribers={}", tx.receiver_count());
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = socket.send(Message::Pong(data)).await;
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Close(_)) => break,
            Ok(_) => {} // Text or other
            Err(e) => {
                println!("[stream] {name}: recv error: {e}");
                break;
            }
        }
    }

    state.streams.remove(&name);
    let _ = state.event_tx.send(ControlEvent::StreamRemoved { name: name.clone() });
    println!("[stream] unregistered: {name}");
}

async fn ws_receiver_handler(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_ws_receiver(socket, state, name))
}

async fn handle_ws_receiver(mut socket: WebSocket, state: Arc<AppState>, name: String) {
    let mut rx = {
        if let Some(tx) = state.streams.get(&name) {
            tx.subscribe()
        } else {
            return; // Stream not found
        }
    };

    println!("[receiver] connected to stream: {name}");

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(data) => {
                        if socket.send(Message::Binary(data)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            client_msg = socket.recv() => {
                if let Some(Ok(Message::Close(_))) | None = client_msg {
                    break;
                }
            }
        }
    }
    println!("[receiver] disconnected from stream: {name}");
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
    tx:          broadcast::Sender<Bytes>,
    payload:     Vec<u8>,
) {
    let _ = tx.send(Bytes::from(payload));
}

// Added an endpoint to explicitly start the system capture.
async fn start_system_capture_handler(State(state): State<Arc<AppState>>) -> Html<&'static str> {
    let name = "system".to_string();
    if state.streams.contains_key(&name) {
        return Html("System stream already running.");
    }
    
    let (tx, _) = broadcast::channel(100);
    state.streams.insert(name.clone(), tx.clone());
    let _ = state.event_tx.send(ControlEvent::StreamAdded { name: name.clone() });
    println!("[stream] registered: {name} (source=system)");

    let rt_handle = Handle::current();
    std::thread::spawn(move || {
        if let Err(e) = start_audio_capture(tx, rt_handle) {
            eprintln!("[system] audio capture error: {e:?}");
        }
    });
    
    Html("System stream started.")
}

fn start_audio_capture(
    tx:        broadcast::Sender<Bytes>,
    rt_handle: Handle,
) -> Result<()> {
    let device  = choose_input_device()?;
    let sup_cfg = device.default_input_config()?;
    let input_sr  = sup_cfg.sample_rate().0 as usize;
    let input_ch  = sup_cfg.channels() as usize;
    let stream_cfg: cpal::StreamConfig = sup_cfg.clone().into();

    println!("[cpal] device : {}", device.name().unwrap_or_default());
    println!("[cpal] native : {}Hz {}ch", input_sr, input_ch);

    // Just send raw PCM chunks directly without opus encoding
    let stream = match sup_cfg.sample_format() {
        cpal::SampleFormat::F32 => {
            let tx = tx.clone();
            device.build_input_stream(
                &stream_cfg,
                move |data: &[f32], _| {
                    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
                    let _ = tx.send(Bytes::from(bytes.to_vec()));
                },
                |e| eprintln!("[cpal] {e}"),
                None,
            )?
        }
        cpal::SampleFormat::I16 => {
            let tx = tx.clone();
            device.build_input_stream(
                &stream_cfg,
                move |data: &[i16], _| {
                    let f32_data: Vec<f32> = data.iter().map(|&s| s as f32 / i16::MAX as f32).collect();
                    let bytes = unsafe { std::slice::from_raw_parts(f32_data.as_ptr() as *const u8, f32_data.len() * 4) };
                    let _ = tx.send(Bytes::from(bytes.to_vec()));
                },
                |e| eprintln!("[cpal] {e}"),
                None,
            )?
        }
        cpal::SampleFormat::U16 => {
            let tx = tx.clone();
            device.build_input_stream(
                &stream_cfg,
                move |data: &[u16], _| {
                    let f32_data: Vec<f32> = data.iter().map(|&s| (s as f32 - 32768.0) / 32768.0).collect();
                    let bytes = unsafe { std::slice::from_raw_parts(f32_data.as_ptr() as *const u8, f32_data.len() * 4) };
                    let _ = tx.send(Bytes::from(bytes.to_vec()));
                },
                |e| eprintln!("[cpal] {e}"),
                None,
            )?
        }
        other => return Err(anyhow!("unsupported sample format: {other:?}")),
    };

    stream.play()?;
    println!("[cpal] audio capture started (Raw PCM float32)");
    loop { std::thread::sleep(std::time::Duration::from_secs(60)); }
}
