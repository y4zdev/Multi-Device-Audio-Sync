use axum::{
    extract::{State, Request},
    http::{StatusCode, header, Response},
    middleware::Next,
    response::{IntoResponse, Redirect, Html},
    routing::{get, post},
    Form, Json, Router,
};
use axum_extra::extract::CookieJar;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{db::Db, AppState};

#[derive(Deserialize)]
pub struct LoginPayload {
    pub username: String,
    pub password: String,
}

pub async fn login_page() -> Html<&'static str> {
    Html(include_str!("../static/login.html"))
}

pub async fn login_post(
    State(state): State<Arc<AppState>>,
    mut jar: CookieJar,
    Form(payload): Form<LoginPayload>,
) -> Result<(CookieJar, Redirect), (StatusCode, &'static str)> {
    let db = &state.db;
    
    let user = db.get_user(&payload.username)
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "DB Error"))?
        .ok_or((StatusCode::UNAUTHORIZED, "Invalid username or password"))?;
        
    let valid = bcrypt::verify(&payload.password, &user.password_hash)
        .unwrap_or(false);
        
    if !valid {
        return Err((StatusCode::UNAUTHORIZED, "Invalid username or password"));
    }
    
    let session_id = db.create_session(&user.id)
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Session creation failed"))?;
        
    let cookie = axum_extra::extract::cookie::Cookie::build(("session_id", session_id))
        .path("/")
        .http_only(true)
        .build();
        
    jar = jar.add(cookie);
    
    let redirect = match user.role.as_str() {
        "speaker" => "/receiver",
        "mic" => "/sender",
        "controller" => "/controller",
        "admin" => "/admin",
        _ => "/",
    };

    Ok((jar, Redirect::to(redirect)))
}

pub async fn logout(mut jar: CookieJar) -> (CookieJar, Redirect) {
    jar = jar.remove(axum_extra::extract::cookie::Cookie::from("session_id"));
    (jar, Redirect::to("/login"))
}

#[derive(Serialize)]
pub struct UserResponse {
    pub id: String,
    pub username: String,
    pub role: String,
}

pub async fn list_users(State(state): State<Arc<AppState>>) -> Json<Vec<UserResponse>> {
    let db = &state.db;
    let users = db.get_all_users().unwrap_or_default();
    let resp: Vec<UserResponse> = users.into_iter().map(|u| UserResponse {
        id: u.id,
        username: u.username,
        role: u.role,
    }).collect();
    Json(resp)
}

#[derive(Deserialize)]
pub struct CreateUserPayload {
    pub username: String,
    pub password: String,
    pub role: String,
}

pub async fn create_user(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateUserPayload>,
) -> Result<Json<UserResponse>, (StatusCode, &'static str)> {
    let db = &state.db;
    if db.get_user(&payload.username).unwrap_or(None).is_some() {
        return Err((StatusCode::BAD_REQUEST, "Username already exists"));
    }
    
    let hash = bcrypt::hash(&payload.password, bcrypt::DEFAULT_COST).unwrap();
    db.create_user(&payload.username, &hash, &payload.role).map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create user"))?;
    
    let user = db.get_user(&payload.username).unwrap().unwrap();
    Ok(Json(UserResponse {
        id: user.id,
        username: user.username,
        role: user.role,
    }))
}

#[derive(Serialize, Deserialize)]
pub struct SettingsPayload {
    pub mode: String,
    pub server_ip: String,
}

pub async fn get_settings(State(state): State<Arc<AppState>>) -> Json<SettingsPayload> {
    let mode = state.db.get_setting("streaming_mode").unwrap_or(None).unwrap_or_else(|| "lan".to_string());
    let ip = state.db.get_setting("server_ip").unwrap_or(None).unwrap_or_else(|| "".to_string());
    Json(SettingsPayload { mode, server_ip: ip })
}

pub async fn update_settings(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SettingsPayload>,
) -> Result<Json<SettingsPayload>, (StatusCode, &'static str)> {
    state.db.set_setting("streaming_mode", &payload.mode).map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Failed to save settings"))?;
    state.db.set_setting("server_ip", &payload.server_ip).map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Failed to save settings"))?;
    Ok(Json(payload))
}

// Middleware to enforce roles
pub async fn require_role(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    mut req: Request,
    next: Next,
) -> Result<Response<axum::body::Body>, StatusCode> {
    let path = req.uri().path();
    
    let session_id = jar.get("session_id").map(|c| c.value().to_string());
    let user = if let Some(sid) = session_id {
        state.db.get_user_by_session(&sid).unwrap_or(None)
    } else {
        None
    };
    
    let user = match user {
        Some(u) => u,
        None => {
            // Redirect to login if unauthenticated and requesting an HTML page
            return Ok(Redirect::to("/login").into_response());
        }
    };
    
    // Authorization logic
    let mut allowed = false;
    if user.role == "admin" {
        allowed = true;
    } else if path.starts_with("/receiver") && user.role == "speaker" {
        allowed = true;
    } else if path.starts_with("/sender") && user.role == "mic" {
        allowed = true;
    } else if path.starts_with("/controller") && user.role == "controller" {
        allowed = true;
    } else if path == "/" {
        allowed = true;
    }

    if !allowed {
        return Ok(Html("<h1>403 Forbidden</h1><p>You do not have the required role to access this page.</p>").into_response());
    }
    
    // Pass user ID/Role via extensions
    req.extensions_mut().insert(user.clone());
    
    Ok(next.run(req).await)
}
