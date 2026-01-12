use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, State, WebSocketUpgrade,
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db;
use crate::state::{AppState, MessageType, RoomMessage};

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

    if let Err(e) = db::store_public_key(&state.db, &conn_id, &room_id, &public_key).await {
        tracing::error!("Failed to store public key: {}", e);
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

    let conn_id_clone = conn_id.clone();
    let room_id_clone = room_id.clone();
    let forward_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if msg.from_conn_id == conn_id_clone {
                continue;
            }

            if let Some(ref target) = msg.target_conn_id {
                if target != &conn_id_clone {
                    continue;
                }
            }

            let outgoing = match msg.msg_type {
                MessageType::Chat => OutgoingMessage::chat(&msg.from_conn_id, &msg.payload),
                MessageType::KeyShare => {
                    OutgoingMessage::key_share(&msg.from_conn_id, &msg.payload)
                }
                MessageType::PeerJoined => {
                    OutgoingMessage::peer_joined(&msg.from_conn_id, &msg.payload)
                }
                MessageType::PeerLeft => OutgoingMessage::peer_left(&msg.from_conn_id),
                MessageType::RoomExpired => OutgoingMessage::room_expired(),
            };

            let json = match serde_json::to_string(&outgoing) {
                Ok(j) => j,
                Err(_) => continue,
            };

            if ws_sender.send(Message::Text(json.into())).await.is_err() {
                break;
            }

            if matches!(msg.msg_type, MessageType::RoomExpired) {
                let _ = ws_sender.close().await;
                break;
            }
        }
        tracing::info!(
            "Forward task ended for {} in room {}",
            conn_id_clone,
            room_id_clone
        );
    });

    let state_clone = state.clone();
    let room_id_clone = room_id.clone();
    let conn_id_clone = conn_id.clone();
    while let Some(result) = ws_receiver.next().await {
        match result {
            Ok(Message::Text(text)) => {
                let incoming: IncomingMessage = match serde_json::from_str(&text) {
                    Ok(m) => m,
                    Err(_) => {
                        tracing::warn!("Invalid message format from {}", conn_id_clone);
                        continue;
                    }
                };

                match incoming.msg_type.as_str() {
                    "message" => {
                        if let Some(payload) = incoming.payload {
                            if let Err(e) =
                                db::update_activity(&state_clone.db, &room_id_clone).await
                            {
                                tracing::error!("Failed to update activity: {}", e);
                            }

                            let _ = tx.send(RoomMessage {
                                from_conn_id: conn_id_clone.clone(),
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
                            tracing::info!(
                                "Key share from {} to {}",
                                conn_id_clone,
                                target_peer_id
                            );
                            let _ = tx.send(RoomMessage {
                                from_conn_id: conn_id_clone.clone(),
                                target_conn_id: Some(target_peer_id),
                                payload,
                                msg_type: MessageType::KeyShare,
                            });
                        }
                    }
                    _ => {}
                }
            }
            Ok(Message::Close(_)) => {
                tracing::info!("WebSocket closed by client: {}", conn_id_clone);
                break;
            }
            Err(e) => {
                tracing::error!("WebSocket error for {}: {}", conn_id_clone, e);
                break;
            }
            _ => {}
        }
    }

    forward_task.abort();

    if let Err(e) = db::remove_participant(&state.db, &conn_id).await {
        tracing::error!("Failed to remove participant: {}", e);
    }

    let _ = tx.send(RoomMessage {
        from_conn_id: conn_id.clone(),
        target_conn_id: None,
        payload: String::new(),
        msg_type: MessageType::PeerLeft,
    });

    tracing::info!("Connection {} disconnected from room {}", conn_id, room_id);
}
