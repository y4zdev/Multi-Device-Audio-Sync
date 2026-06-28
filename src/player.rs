use axum::{
    extract::{State, Multipart, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use std::path::PathBuf;
use std::io::Write;
use tokio::process::Command;
use tokio::io::AsyncReadExt;
use bytes::Bytes;
use crate::{AppState, DeviceInfo, DeviceStatus, ControlEvent, now_ms};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Track {
    pub id: String,
    pub name: String,
    pub path: String,
    pub duration_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerStateData {
    pub playlist: Vec<Track>,
    pub current_idx: usize,
    pub playing: bool,
    pub position_sec: f64,
    #[serde(default = "default_true")]
    pub loop_queue: bool,
    #[serde(default)]
    pub shuffle: bool,
}

fn default_true() -> bool { true }

pub struct PlayerState {
    pub data: Mutex<PlayerStateData>,
    pub command_tx: broadcast::Sender<PlayerCommand>,
}

#[derive(Clone, Debug)]
pub enum PlayerCommand {
    Play,
    Pause,
    Next,
    Prev,
    Seek(usize), // Seek track
    SeekTime(f64), // Seek time inside track
    RemoveTrack(usize),
    MoveTrack(usize, usize),
    ToggleLoop,
    ToggleShuffle,
}

impl PlayerState {
    pub fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(10);
        
        let data = std::fs::read_to_string("player_state.json")
            .ok()
            .and_then(|s| serde_json::from_str::<PlayerStateData>(&s).ok())
            .map(|mut d| {
                d.playing = false; // Start paused on restart
                d
            })
            .unwrap_or_else(|| PlayerStateData {
                playlist: vec![],
                current_idx: 0,
                playing: false,
                position_sec: 0.0,
                loop_queue: true,
                shuffle: false,
            });

        let state = Arc::new(Self {
            data: Mutex::new(data),
            command_tx: tx,
        });
        
        let state_clone = state.clone();
        tokio::spawn(async move {
            let mut last_json = String::new();
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                let data = state_clone.data.lock().await.clone();
                if let Ok(json) = serde_json::to_string(&data) {
                    if json != last_json {
                        let _ = std::fs::write("player_state.json", &json);
                        last_json = json;
                    }
                }
            }
        });

        state
    }
}

pub fn player_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/upload", post(upload_track))
        .route("/state", get(get_state))
        .route("/control", post(control_player))
}

pub async fn start_player_loop(app_state: Arc<AppState>) {
    let name = "BGM Player".to_string();
    
    // Register device in state
    let device_id = "bgm-server-player".to_string();
    let device = DeviceInfo {
        id: device_id.clone(),
        name: name.clone(),
        role: crate::DeviceRole::Mic,
        status: DeviceStatus::Online,
        assigned_streams: vec![],
        volume: 1.0,
        last_seen: now_ms(),
    };
    app_state.devices.insert(device.id.clone(), device.clone());
    let _ = app_state.event_tx.send(ControlEvent::DeviceJoined { device: device.clone() });

    // Register stream
    let (tx, _) = broadcast::channel(100);
    app_state.streams.insert(name.clone(), tx.clone());
    let _ = app_state.event_tx.send(ControlEvent::StreamAdded { name: name.clone() });
    
    let player_state = &app_state.player_state;
    let mut cmd_rx = player_state.command_tx.subscribe();
    
    // Create uploads dir
    std::fs::create_dir_all("uploads").unwrap();
    
    loop {
        let mut track_path = None;
        let mut seek_time = 0.0;
        {
            let mut data = player_state.data.lock().await;
            if data.playing && !data.playlist.is_empty() {
                track_path = Some(data.playlist[data.current_idx].path.clone());
                seek_time = data.position_sec;
            } else {
                data.playing = false;
            }
        }
        
        if let Some(path) = track_path {
            let mut child = Command::new("ffmpeg")
                .args(&[
                    "-ss", &format!("{:.2}", seek_time),
                    "-i", &path, 
                    "-f", "f32le", 
                    "-ar", "48000", 
                    "-ac", "2", 
                    "-"
                ])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("Failed to spawn ffmpeg");
                
            let mut stdout = child.stdout.take().unwrap();
            let mut buf = vec![0u8; 4096 * 2 * 4]; // 32KB
            
            let mut volume: f32 = 0.0;
            let mut fading_out = false;
            
            let play_start_time = tokio::time::Instant::now();
            let mut track_position = 0.0;
            
            loop {
                tokio::select! {
                    res = stdout.read_exact(&mut buf) => {
                        match res {
                            Ok(n) => {
                                let chunk_duration = 32768.0 / 384_000.0;
                                track_position += chunk_duration;
                                
                                let elapsed = play_start_time.elapsed().as_secs_f64();
                                if track_position > elapsed {
                                    tokio::time::sleep(std::time::Duration::from_secs_f64(track_position - elapsed)).await;
                                }
                                if fading_out {
                                    volume = (volume - 0.02).max(0.0);
                                    if volume <= 0.0 { break; }
                                } else {
                                    volume = (volume + 0.02).min(1.0);
                                }
                                
                                let mut processed = buf.clone();
                                let floats = bytemuck::cast_slice_mut::<u8, f32>(&mut processed);
                                
                                let dev_volume = app_state.devices.get("bgm-server-player")
                                    .map(|d| d.volume)
                                    .unwrap_or(1.0);
                                let total_vol = volume * dev_volume;
                                
                                for f in floats.iter_mut() {
                                    *f *= total_vol;
                                }
                                let _ = tx.send(Bytes::from(processed));
                                
                                let mut data = player_state.data.lock().await;
                                data.position_sec += 32768.0 / 384_000.0;
                            }
                            Err(_) => {
                                let mut data = player_state.data.lock().await;
                                if !data.playlist.is_empty() {
                                    if data.loop_queue {
                                        let len = data.playlist.len();
                                        data.current_idx = if data.shuffle && len > 1 {
                                            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as usize;
                                            let mut n = now % len;
                                            if n == data.current_idx { n = (n + 1) % len; }
                                            n
                                        } else {
                                            (data.current_idx + 1) % len
                                        };
                                    } else {
                                        if data.current_idx + 1 < data.playlist.len() {
                                            data.current_idx += 1;
                                        } else {
                                            data.playing = false;
                                            data.position_sec = 0.0;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                    Ok(cmd) = cmd_rx.recv() => {
                        match cmd {
                            PlayerCommand::Play => { fading_out = false; }
                            PlayerCommand::Pause => {
                                let mut data = player_state.data.lock().await;
                                data.playing = false;
                                fading_out = true;
                            }
                            PlayerCommand::Next => {
                                let mut data = player_state.data.lock().await;
                                if !data.playlist.is_empty() {
                                    let len = data.playlist.len();
                                    data.current_idx = if data.shuffle && len > 1 {
                                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as usize;
                                        let mut n = now % len;
                                        if n == data.current_idx { n = (n + 1) % len; }
                                        n
                                    } else {
                                        (data.current_idx + 1) % len
                                    };
                                    data.position_sec = 0.0;
                                }
                                fading_out = true;
                            }
                            PlayerCommand::Prev => {
                                let mut data = player_state.data.lock().await;
                                if !data.playlist.is_empty() {
                                    if data.current_idx == 0 {
                                        data.current_idx = data.playlist.len().saturating_sub(1);
                                    } else {
                                        data.current_idx -= 1;
                                    }
                                    data.position_sec = 0.0;
                                }
                                fading_out = true;
                            }
                            PlayerCommand::Seek(idx) => {
                                let mut data = player_state.data.lock().await;
                                if !data.playlist.is_empty() {
                                    data.current_idx = idx % data.playlist.len();
                                    data.position_sec = 0.0;
                                }
                                fading_out = true;
                            }
                            PlayerCommand::SeekTime(t) => {
                                let mut data = player_state.data.lock().await;
                                data.position_sec = t.max(0.0);
                                fading_out = true;
                            }
                            PlayerCommand::RemoveTrack(idx) => {
                                let mut data = player_state.data.lock().await;
                                if idx < data.playlist.len() {
                                    let removed_track = data.playlist.remove(idx);
                                    let _ = std::fs::remove_file(&removed_track.path);
                                    if data.current_idx == idx {
                                        if !data.playlist.is_empty() {
                                            data.current_idx = data.current_idx % data.playlist.len();
                                        } else {
                                            data.playing = false;
                                        }
                                        fading_out = true;
                                    } else if data.current_idx > idx {
                                        data.current_idx -= 1;
                                    }
                                }
                            }
                            PlayerCommand::ToggleLoop => {
                                let mut data = player_state.data.lock().await;
                                data.loop_queue = !data.loop_queue;
                            }
                            PlayerCommand::ToggleShuffle => {
                                let mut data = player_state.data.lock().await;
                                data.shuffle = !data.shuffle;
                            }
                            PlayerCommand::MoveTrack(from, to) => {
                                let mut data = player_state.data.lock().await;
                                if from < data.playlist.len() && to < data.playlist.len() {
                                    let track = data.playlist.remove(from);
                                    data.playlist.insert(to, track);
                                    
                                    if data.current_idx == from {
                                        data.current_idx = to;
                                    } else if from < data.current_idx && to >= data.current_idx {
                                        data.current_idx -= 1;
                                    } else if from > data.current_idx && to <= data.current_idx {
                                        data.current_idx += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            let _ = child.kill().await;
        } else {
            if let Ok(cmd) = cmd_rx.recv().await {
                match cmd {
                    PlayerCommand::Play => {
                        let mut data = player_state.data.lock().await;
                        data.playing = true;
                    }
                    PlayerCommand::Seek(idx) => {
                        let mut data = player_state.data.lock().await;
                        if !data.playlist.is_empty() {
                            data.current_idx = idx % data.playlist.len();
                        } else {
                            data.current_idx = 0;
                        }
                        data.position_sec = 0.0;
                        data.playing = true;
                    }
                    PlayerCommand::RemoveTrack(idx) => {
                        let mut data = player_state.data.lock().await;
                        if idx < data.playlist.len() {
                            let removed_track = data.playlist.remove(idx);
                            let _ = std::fs::remove_file(&removed_track.path);
                            if data.current_idx == idx {
                                if !data.playlist.is_empty() {
                                    data.current_idx = data.current_idx % data.playlist.len();
                                }
                            } else if data.current_idx > idx {
                                data.current_idx -= 1;
                            }
                        }
                    }
                    PlayerCommand::ToggleLoop => {
                        let mut data = player_state.data.lock().await;
                        data.loop_queue = !data.loop_queue;
                    }
                    PlayerCommand::ToggleShuffle => {
                        let mut data = player_state.data.lock().await;
                        data.shuffle = !data.shuffle;
                    }
                    PlayerCommand::MoveTrack(from, to) => {
                        let mut data = player_state.data.lock().await;
                        if from < data.playlist.len() && to < data.playlist.len() {
                            let track = data.playlist.remove(from);
                            data.playlist.insert(to, track);
                            
                            if data.current_idx == from {
                                data.current_idx = to;
                            } else if from < data.current_idx && to >= data.current_idx {
                                data.current_idx -= 1;
                            } else if from > data.current_idx && to <= data.current_idx {
                                data.current_idx += 1;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn upload_track(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> axum::http::StatusCode {
    let mut uploaded = false;
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.file_name().unwrap_or("track").to_string();
        if let Ok(data) = field.bytes().await {
            let id = uuid::Uuid::new_v4().to_string();
            let path = format!("uploads/{}_{}", id, name);
            
            if let Ok(mut file) = std::fs::File::create(&path) {
                if file.write_all(&data).is_ok() {
                    
                    let out = std::process::Command::new("ffprobe")
                        .args(&["-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", &path])
                        .output();
                        
                    let duration_sec = if let Ok(o) = out {
                        String::from_utf8_lossy(&o.stdout).trim().parse::<f64>().unwrap_or(0.0)
                    } else { 0.0 };
                    
                    let mut pdata = state.player_state.data.lock().await;
                    pdata.playlist.push(Track {
                        id,
                        name,
                        path,
                        duration_sec,
                    });
                    uploaded = true;
                }
            }
        }
    }
    
    if uploaded { axum::http::StatusCode::OK } else { axum::http::StatusCode::BAD_REQUEST }
}

async fn get_state(State(state): State<Arc<AppState>>) -> Json<PlayerStateData> {
    let data = state.player_state.data.lock().await.clone();
    Json(data)
}

#[derive(Deserialize)]
pub struct ControlReq {
    pub action: String,
    pub index: Option<usize>,
    pub to_index: Option<usize>,
    pub time: Option<f64>,
}

async fn control_player(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ControlReq>,
) -> axum::http::StatusCode {
    let cmd = match req.action.as_str() {
        "play" => PlayerCommand::Play,
        "pause" => PlayerCommand::Pause,
        "next" => PlayerCommand::Next,
        "prev" => PlayerCommand::Prev,
        "seek" => if let Some(t) = req.time { PlayerCommand::SeekTime(t) } else { return axum::http::StatusCode::BAD_REQUEST },
        "seek_track" => if let Some(i) = req.index { PlayerCommand::Seek(i) } else { return axum::http::StatusCode::BAD_REQUEST },
        "remove_track" => if let Some(i) = req.index { PlayerCommand::RemoveTrack(i) } else { return axum::http::StatusCode::BAD_REQUEST },
        "move_track" => if let (Some(from), Some(to)) = (req.index, req.to_index) { PlayerCommand::MoveTrack(from, to) } else { return axum::http::StatusCode::BAD_REQUEST },
        "toggle_loop" => PlayerCommand::ToggleLoop,
        "toggle_shuffle" => PlayerCommand::ToggleShuffle,
        _ => return axum::http::StatusCode::BAD_REQUEST,
    };
    
    let _ = state.player_state.command_tx.send(cmd);
    
    if req.action == "play" || req.action == "seek" {
        let mut pdata = state.player_state.data.lock().await;
        if !pdata.playlist.is_empty() {
            pdata.playing = true;
        }
    } else if req.action == "pause" {
        state.player_state.data.lock().await.playing = false;
    }
    
    axum::http::StatusCode::OK
}
