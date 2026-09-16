use axum::{
    Json,
    body::Bytes,
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::atomic::Ordering};
use uuid::Uuid;

use crate::{
    admission::{AdmissionError, Budget},
    db,
    passwords::PasswordError,
    security::client_ip,
    state::AppState,
};

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct CreateRoomRequest {
    password: Option<String>,
}

#[derive(Serialize)]
pub struct CreateRoomResponse {
    pub room_id: String,
    pub password_required: bool,
}

#[derive(Serialize)]
pub struct RoomStatusResponse {
    pub exists: bool,
    pub room_id: String,
    pub password_required: bool,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub fn error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
        .into_response()
}

pub fn admission_error(err: AdmissionError) -> Response {
    match err {
        AdmissionError::Limited(retry) => {
            let mut response = error(
                StatusCode::TOO_MANY_REQUESTS,
                "Too many attempts; retry later",
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, retry.to_string().parse().unwrap());
            response
        }
        AdmissionError::Unavailable => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Admission temporarily unavailable",
        ),
    }
}

pub async fn create_room(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let ip = match client_ip(peer.ip(), &headers, &state.config.trusted_proxies) {
        Ok(ip) => ip,
        Err(status) => return error(status, "Invalid client address"),
    };
    if let Err(err) = state.admission.check(Budget::Creation(ip)) {
        return admission_error(err);
    }
    let request = if body.is_empty() {
        CreateRoomRequest::default()
    } else {
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.split(';').next())
            .map(str::trim);
        if content_type != Some("application/json") {
            return error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Expected application/json",
            );
        }
        match serde_json::from_slice::<CreateRoomRequest>(&body) {
            Ok(request) => request,
            Err(_) => return error(StatusCode::BAD_REQUEST, "Invalid room request"),
        }
    };
    let hash = match request.password {
        None => None,
        Some(password) => match state.passwords.hash(password).await {
            Ok(hash) => Some(hash),
            Err(PasswordError::Invalid) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "Password must contain 15–256 characters and at most 1024 UTF-8 bytes",
                );
            }
            Err(PasswordError::Common) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "This password is commonly used; choose a different passphrase",
                );
            }
            Err(_) => {
                state.admission.unavailable.fetch_add(1, Ordering::Relaxed);
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Password protection temporarily unavailable",
                );
            }
        },
    };
    let room_id = Uuid::new_v4().to_string();
    if db::create_room_with_password(&state.db, &room_id, hash.as_deref())
        .await
        .is_err()
    {
        tracing::error!("Failed to create room");
        return error(StatusCode::INTERNAL_SERVER_ERROR, "Failed to create room");
    }
    Json(CreateRoomResponse {
        room_id,
        password_required: hash.is_some(),
    })
    .into_response()
}

pub async fn get_room(State(state): State<AppState>, Path(room_id): Path<String>) -> Response {
    match db::room_password(&state.db, &room_id).await {
        Ok(Some(hash)) => Json(RoomStatusResponse {
            exists: true,
            room_id,
            password_required: hash.is_some(),
        })
        .into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "Room not found"),
        Err(_) => {
            tracing::error!("Failed to check room");
            error(StatusCode::INTERNAL_SERVER_ERROR, "Failed to check room")
        }
    }
}
