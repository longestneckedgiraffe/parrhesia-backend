use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, State, WebSocketUpgrade,
    },
    response::Response,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tokio::time::interval;
use uuid::Uuid;

use crate::db;
use crate::state::{AppState, MessageType, RoomMessage};

fn is_valid_p256_public_key(base64_key: &str) -> bool {
    match STANDARD.decode(base64_key) {
        Ok(bytes) => bytes.len() == 65 && bytes[0] == 0x04,
        Err(_) => false,
    }
}

const MAX_PARTICIPANTS: i64 = 16;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
struct IncomingMessage {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(default)]
    public_key: Option<String>,
    #[serde(default)]
    payload: Option<String>,
    #[serde(default)]
    target_peer_id: Option<String>,
}

#[derive(Serialize)]
struct OutgoingMessage {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    public_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_creator: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    creator_id: Option<String>,
}

impl OutgoingMessage {
    fn welcome(peer_id: &str, is_creator: bool, creator_id: Option<&str>) -> Self {
        Self {
            msg_type: "welcome".to_string(),
            peer_id: Some(peer_id.to_string()),
            public_key: None,
            payload: None,
            is_creator: Some(is_creator),
            creator_id: creator_id.map(|s| s.to_string()),
        }
    }

    fn peer_key(peer_id: &str, public_key: &str) -> Self {
        Self {
            msg_type: "peer_key".to_string(),
            peer_id: Some(peer_id.to_string()),
            public_key: Some(public_key.to_string()),
            payload: None,
            is_creator: None,
            creator_id: None,
        }
    }

    fn peer_joined(peer_id: &str, public_key: &str) -> Self {
        Self {
            msg_type: "peer_joined".to_string(),
            peer_id: Some(peer_id.to_string()),
            public_key: Some(public_key.to_string()),
            payload: None,
            is_creator: None,
            creator_id: None,
        }
    }

    fn peer_left(peer_id: &str) -> Self {
        Self {
            msg_type: "peer_left".to_string(),
            peer_id: Some(peer_id.to_string()),
            public_key: None,
            payload: None,
            is_creator: None,
            creator_id: None,
        }
    }

    fn chat(peer_id: &str, payload: &str) -> Self {
        Self {
            msg_type: "message".to_string(),
            peer_id: Some(peer_id.to_string()),
            public_key: None,
            payload: Some(payload.to_string()),
            is_creator: None,
            creator_id: None,
        }
    }

    fn key_share(from_peer_id: &str, payload: &str) -> Self {
        Self {
            msg_type: "key_share".to_string(),
            peer_id: Some(from_peer_id.to_string()),
            public_key: None,
            payload: Some(payload.to_string()),
            is_creator: None,
            creator_id: None,
        }
    }

    fn room_expired() -> Self {
        Self {
            msg_type: "room_expired".to_string(),
            peer_id: None,
            public_key: None,
            payload: None,
            is_creator: None,
            creator_id: None,
        }
    }

    fn room_full() -> Self {
        Self {
            msg_type: "room_full".to_string(),
            peer_id: None,
            public_key: None,
            payload: None,
            is_creator: None,
            creator_id: None,
        }
    }
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(room_id): Path<String>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state, room_id))
}

async fn send_json<T: Serialize>(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: &T,
) -> bool {
    match serde_json::to_string(msg) {
        Ok(json) => sender.send(Message::Text(json.into())).await.is_ok(),
        Err(_) => false,
    }
}

async fn handle_socket(socket: WebSocket, state: AppState, room_id: String) {
    let exists = match db::room_exists(&state.db, &room_id).await {
        Ok(exists) => exists,
        Err(e) => {
            tracing::error!("Failed to check room existence: {}", e);
            return;
        }
    };

    if !exists {
        tracing::warn!("WebSocket connection to non-existent room: {}", room_id);
        return;
    }

    let conn_id = Uuid::new_v4().to_string();
    tracing::info!("New WebSocket connection: {} to room {}", conn_id, room_id);

    let is_creator = match db::set_creator(&state.db, &room_id, &conn_id).await {
        Ok(became_creator) => became_creator,
        Err(e) => {
            tracing::error!("Failed to set creator: {}", e);
            false
        }
    };

    let creator_id = match db::get_creator(&state.db, &room_id).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("Failed to get creator: {}", e);
            None
        }
    };

    if is_creator {
        tracing::info!("Connection {} is the room creator", conn_id);
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

    if !send_json(
        &mut ws_sender,
        &OutgoingMessage::welcome(&conn_id, is_creator, creator_id.as_deref()),
    )
    .await
    {
        tracing::error!("Failed to send welcome message");
        return;
    }

    let public_key = loop {
        match ws_receiver.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(msg) = serde_json::from_str::<IncomingMessage>(&text) {
                    if msg.msg_type == "key_announce" {
                        if let Some(key) = msg.public_key {
                            if !is_valid_p256_public_key(&key) {
                                tracing::warn!("Invalid public key format from {}", conn_id);
                                return;
                            }
                            break key;
                        }
                    }
                }
                tracing::warn!("Expected key_announce, got: {}", text);
            }
            Some(Ok(Message::Close(_))) | None => {
                tracing::info!("Client disconnected before key exchange: {}", conn_id);
                return;
            }
            Some(Err(e)) => {
                tracing::error!("WebSocket error during key exchange: {}", e);
                return;
            }
            _ => continue,
        }
    };

    tracing::info!("Received public key from {}", conn_id);

    let added = match db::try_add_participant(
        &state.db,
        &conn_id,
        &room_id,
        &public_key,
        MAX_PARTICIPANTS,
    )
    .await
    {
        Ok(added) => added,
        Err(e) => {
            tracing::error!("Failed to add participant: {}", e);
            return;
        }
    };

    if !added {
        tracing::warn!("Room {} is full", room_id);
        let _ = send_json(&mut ws_sender, &OutgoingMessage::room_full()).await;
        return;
    }

    match db::get_other_public_keys(&state.db, &room_id, &conn_id).await {
        Ok(peers) => {
            for (peer_id, peer_key) in peers {
                if !send_json(&mut ws_sender, &OutgoingMessage::peer_key(&peer_id, &peer_key)).await
                {
                    tracing::error!("Failed to send peer key");
                    let _ = db::remove_participant(&state.db, &conn_id).await;
                    return;
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to get peer keys: {}", e);
            let _ = db::remove_participant(&state.db, &conn_id).await;
            return;
        }
    }

    let tx = state.get_or_create_channel(&room_id).await;
    let mut rx = tx.subscribe();

    let _ = tx.send(RoomMessage {
        from_conn_id: conn_id.clone(),
        target_conn_id: None,
        payload: public_key.clone(),
        msg_type: MessageType::PeerJoined,
    });

    let mut ping_interval = interval(PING_INTERVAL);
    let mut last_pong = Instant::now();
    let mut waiting_for_pong = false;

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if waiting_for_pong && last_pong.elapsed() > PONG_TIMEOUT {
                    tracing::warn!("Connection {} timed out (no pong received)", conn_id);
                    break;
                }

                if ws_sender.send(Message::Ping(vec![].into())).await.is_err() {
                    tracing::error!("Failed to send ping to {}", conn_id);
                    break;
                }
                waiting_for_pong = true;
            }

            msg = rx.recv() => {
                match msg {
                    Ok(room_msg) => {
                        if room_msg.from_conn_id == conn_id {
                            continue;
                        }

                        if let Some(ref target) = room_msg.target_conn_id {
                            if target != &conn_id {
                                continue;
                            }
                        }

                        let outgoing = match room_msg.msg_type {
                            MessageType::Chat => OutgoingMessage::chat(&room_msg.from_conn_id, &room_msg.payload),
                            MessageType::KeyShare => OutgoingMessage::key_share(&room_msg.from_conn_id, &room_msg.payload),
                            MessageType::PeerJoined => OutgoingMessage::peer_joined(&room_msg.from_conn_id, &room_msg.payload),
                            MessageType::PeerLeft => OutgoingMessage::peer_left(&room_msg.from_conn_id),
                            MessageType::RoomExpired => OutgoingMessage::room_expired(),
                        };

                        if let Ok(json) = serde_json::to_string(&outgoing) {
                            if ws_sender.send(Message::Text(json.into())).await.is_err() {
                                break;
                            }
                        }

                        if matches!(room_msg.msg_type, MessageType::RoomExpired) {
                            let _ = ws_sender.close().await;
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let incoming: IncomingMessage = match serde_json::from_str(&text) {
                            Ok(m) => m,
                            Err(_) => {
                                tracing::warn!("Invalid message format from {}", conn_id);
                                continue;
                            }
                        };

                        match incoming.msg_type.as_str() {
                            "message" => {
                                if let Some(payload) = incoming.payload {
                                    if let Err(e) = db::update_activity(&state.db, &room_id).await {
                                        tracing::error!("Failed to update activity: {}", e);
                                    }

                                    let _ = tx.send(RoomMessage {
                                        from_conn_id: conn_id.clone(),
                                        target_conn_id: None,
                                        payload,
                                        msg_type: MessageType::Chat,
                                    });
                                }
                            }
                            "key_share" => {
                                if let (Some(payload), Some(target_peer_id)) =
                                    (incoming.payload, incoming.target_peer_id)
                                {
                                    tracing::info!("Key share from {} to {}", conn_id, target_peer_id);
                                    let _ = tx.send(RoomMessage {
                                        from_conn_id: conn_id.clone(),
                                        target_conn_id: Some(target_peer_id),
                                        payload,
                                        msg_type: MessageType::KeyShare,
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_pong = Instant::now();
                        waiting_for_pong = false;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if ws_sender.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::info!("WebSocket closed by client: {}", conn_id);
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::error!("WebSocket error for {}: {}", conn_id, e);
                        break;
                    }
                    None => break,
                    _ => {}
                }
            }
        }
    }

    if let Err(e) = db::remove_participant(&state.db, &conn_id).await {
        tracing::error!("Failed to remove participant: {}", e);
    }

    match db::reset_creator_if_empty(&state.db, &room_id).await {
        Ok(true) => tracing::info!("Room {} is now empty, creator reset", room_id),
        Ok(false) => {}
        Err(e) => tracing::error!("Failed to reset creator: {}", e),
    }

    let _ = tx.send(RoomMessage {
        from_conn_id: conn_id.clone(),
        target_conn_id: None,
        payload: String::new(),
        msg_type: MessageType::PeerLeft,
    });

    tracing::info!("Connection {} disconnected from room {}", conn_id, room_id);
}
