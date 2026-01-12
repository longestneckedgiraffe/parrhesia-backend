use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use uuid::Uuid;

use crate::db;
use crate::state::AppState;

#[derive(Serialize)]
pub struct CreateRoomResponse {
    pub room_id: String,
}

#[derive(Serialize)]
pub struct RoomStatusResponse {
    pub exists: bool,
    pub room_id: String,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn create_room(
    State(state): State<AppState>,
) -> Result<Json<CreateRoomResponse>, (StatusCode, Json<ErrorResponse>)> {
    let room_id = Uuid::new_v4().to_string();

    db::create_room(&state.db, &room_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create room: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to create room".to_string(),
                }),
            )
        })?;

    tracing::info!("Created room: {}", room_id);
    Ok(Json(CreateRoomResponse { room_id }))
}

pub async fn get_room(
    State(state): State<AppState>,
    Path(room_id): Path<String>,
) -> Result<Json<RoomStatusResponse>, (StatusCode, Json<ErrorResponse>)> {
    let exists = db::room_exists(&state.db, &room_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to check room: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to check room".to_string(),
                }),
            )
        })?;

    if !exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Room not found".to_string(),
            }),
        ));
    }

    Ok(Json(RoomStatusResponse {
        exists: true,
        room_id,
    }))
}
