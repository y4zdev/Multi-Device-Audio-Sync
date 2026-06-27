import re

with open("src/main.rs", "r") as f:
    code = f.read()

# 1. Remove webrtc imports
code = re.sub(r'use webrtc::\{[\s\S]*?use webrtc_ice::udp_mux::.*?;\n', '', code)
# add axum ws
code = code.replace("use anyhow::{anyhow, Result};", "use anyhow::{anyhow, Result};\nuse axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};")

# 2. Update AppState
code = code.replace("api:      Arc<webrtc::api::API>,", "")
code = code.replace("streams:  Arc<DashMap<String, Arc<TrackLocalStaticSample>>>,", "streams:  Arc<DashMap<String, broadcast::Sender<Bytes>>>,")

# 3. Update main init
code = re.sub(r'    let mut media_engine = MediaEngine::default\(\);[\s\S]*?setting_engine\.set_udp_network\(UDPNetwork::Muxed\(udp_mux\)\);', '', code)
code = re.sub(r'                setting_engine\.set_nat_1to1_ips\([\s\S]*?\);', '', code)
code = re.sub(r'    let api = Arc::new\([\s\S]*?\.build\(\),\n    \);', '', code)
code = code.replace("        api,\n", "")

# 4. Update routes
code = code.replace(".route(\"/sender/offer\",   post(handle_sender_offer))", ".route(\"/ws/sender/:name\", get(ws_sender_handler))")
code = code.replace(".route(\"/receiver/offer\", post(handle_receiver_offer))", ".route(\"/ws/receiver/:name\", get(ws_receiver_handler))")
code = code.replace("    let bind_addr: std::net::SocketAddr = \"0.0.0.0:443\".parse()?;", "    let bind_addr: std::net::SocketAddr = \"0.0.0.0:443\".parse()?;") # anchor

# 5. Replace WebRTC logic with WS logic
webrtc_start = code.find("// ── WebRTC offer handlers")
if webrtc_start != -1:
    webrtc_end = code.find("// ── Audio capture (system source)", webrtc_start)
    
    ws_code = """// ── WebSocket Streaming ─────────────────────────────────────────────────────

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

    while let Some(msg) = socket.recv().await {
        if let Ok(Message::Binary(data)) = msg {
            let _ = tx.send(Bytes::from(data));
        } else if let Ok(Message::Close(_)) = msg {
            break;
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
            return;
        }
    };

    println!("[receiver] connected to stream: {name}");

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(data) => {
                        if socket.send(Message::Binary(data.to_vec())).await.is_err() {
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

"""
    code = code[:webrtc_start] + ws_code + code[webrtc_end:]

# 6. Update system audio capture
sys_start = code.find("fn start_audio_capture(")
if sys_start != -1:
    sys_end = code.find("}\n\n// ── App routing", sys_start)
    if sys_end == -1: sys_end = code.find("}\n\n// ── HTTP", sys_start)
    if sys_end == -1: sys_end = code.find("}\n\n// ── HTML", sys_start)
    if sys_end != -1:
        # replace the audio capture function
        new_audio_cap = """fn start_audio_capture(
    tx: broadcast::Sender<Bytes>,
    rt: Handle,
) -> Result<()> {
    let host = cpal::default_host();
    let device_name = pulse_default_source_name();
    let device = if let Some(name) = device_name {
        host.input_devices()?.find(|d| d.name().unwrap_or_default() == name)
            .or_else(|| host.default_input_device())
    } else {
        host.default_input_device()
    }.ok_or_else(|| anyhow!("No input device"))?;

    let config = cpal::StreamConfig {
        channels: OPUS_CH as u16,
        sample_rate: cpal::SampleRate(OPUS_SR as u32),
        buffer_size: cpal::BufferSize::Default,
    };

    let stream = device.build_input_stream(
        &config,
        move |data: &[f32], _: &_| {
            // send f32 raw bytes
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    data.as_ptr() as *const u8,
                    data.len() * 4,
                )
            };
            let _ = tx.send(Bytes::from(bytes.to_vec()));
        },
        |err| eprintln!("audio error: {err}"),
        None,
    )?;

    stream.play()?;
    std::thread::park(); // run forever
    Ok(())
}
"""
        code = code[:sys_start] + new_audio_cap + code[sys_end+1:]

with open("src/main.rs", "w") as f:
    f.write(code)

