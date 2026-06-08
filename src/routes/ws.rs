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
use tokio::time::{interval, timeout};
use uuid::Uuid;

use crate::db;
use crate::state::{AppState, RoomEvent};

fn is_valid_mldsa65_public_key(base64_key: &str) -> bool {
    match STANDARD.decode(base64_key) {
        Ok(bytes) => bytes.len() == 1952,
        Err(_) => false,
    }
}

fn is_valid_mlkem768_public_key(base64_key: &str) -> bool {
    match STANDARD.decode(base64_key) {
        Ok(bytes) => bytes.len() == 1184,
        Err(_) => false,
    }
}

const MAX_PARTICIPANTS: i64 = 16;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);

// A client must complete the key-exchange handshake within this window, or the
// socket is dropped. The heartbeat only starts afterwards, so without this a
// connection could sit idle in the handshake forever (Cloudflare's WS proxy
// sees a healthy connection and won't reap it).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// Largest inbound WebSocket message we accept. Parrhesia relays text only, so
// this is generous; it exists to close the 16-way broadcast amplifier (the
// library default is 64 MiB). Oversized frames cause the socket to error+close.
const MAX_MESSAGE_BYTES: usize = 64 * 1024;

// Per-connection inbound budget: 30 messages/sec sustained, bursts up to 60.
// No human (even keystroke-rate typing indicators) approaches this; it caps a
// flooding peer. Cloudflare handles volumetric/edge DoS — this is the per-frame
// budget it has no visibility into.
const MSG_RATE_CAPACITY: f64 = 60.0;
const MSG_RATE_REFILL_PER_SEC: f64 = 30.0;

/// Per-connection token bucket guarding how many inbound frames a single socket
/// can have relayed. Refills continuously at `refill_per_sec`, capped at
/// `capacity`. `check` is the only mutator; time is injected so it stays unit-testable.
struct RateLimiter {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self {
            tokens: capacity,
            capacity,
            refill_per_sec,
            last_refill: Instant::now(),
        }
    }

    /// Refills based on elapsed time, then tries to spend one token. Returns
    /// false (caller should drop the message) when the bucket is empty.
    fn check(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum IncomingMessage {
    KeyAnnounce {
        #[serde(default)]
        public_key: Option<String>,
        #[serde(default)]
        pq_public_key: Option<String>,
        #[serde(default)]
        sig: Option<String>,
    },
    Message {
        #[serde(default)]
        payload: Option<String>,
        #[serde(default)]
        epoch: Option<u64>,
        #[serde(default)]
        counter: Option<u64>,
    },
    TreeCommit {
        #[serde(default)]
        tree_commit: Option<String>,
    },
    TreeWelcome {
        #[serde(default)]
        tree_welcome: Option<String>,
        #[serde(default)]
        target_peer_id: Option<String>,
    },
    Typing,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OutgoingMessage {
    Welcome {
        peer_id: String,
        is_creator: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        creator_id: Option<String>,
    },
    PeerKey {
        peer_id: String,
        public_key: String,
        pq_public_key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        sig: Option<String>,
    },
    PeerJoined {
        peer_id: String,
        public_key: String,
        pq_public_key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        sig: Option<String>,
    },
    PeerLeft {
        peer_id: String,
    },
    Message {
        peer_id: String,
        payload: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        epoch: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        counter: Option<u64>,
    },
    TreeCommit {
        peer_id: String,
        tree_commit: String,
    },
    Typing {
        peer_id: String,
    },
    RoomExpired,
    TreeWelcome {
        tree_welcome: String,
    },
    RoomFull,
}

impl From<RoomEvent> for OutgoingMessage {
    fn from(event: RoomEvent) -> Self {
        match event {
            RoomEvent::Chat {
                from,
                payload,
                epoch,
                counter,
            } => OutgoingMessage::Message {
                peer_id: from,
                payload,
                epoch,
                counter,
            },
            RoomEvent::TreeCommit { from, tree_data } => OutgoingMessage::TreeCommit {
                peer_id: from,
                tree_commit: tree_data,
            },
            RoomEvent::TreeWelcome { tree_data, .. } => OutgoingMessage::TreeWelcome {
                tree_welcome: tree_data,
            },
            RoomEvent::PeerJoined {
                from,
                public_key,
                pq_public_key,
                sig,
            } => OutgoingMessage::PeerJoined {
                peer_id: from,
                public_key,
                pq_public_key,
                sig,
            },
            RoomEvent::PeerLeft { from } => OutgoingMessage::PeerLeft { peer_id: from },
            RoomEvent::Typing { from } => OutgoingMessage::Typing { peer_id: from },
            RoomEvent::RoomExpired => OutgoingMessage::RoomExpired,
        }
    }
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(room_id): Path<String>,
) -> Response {
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .max_frame_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, state, room_id))
}

async fn send_json<T: Serialize>(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: &T,
) -> bool {
    match serde_json::to_string(msg) {
        Ok(json) => sender.send(Message::Text(json)).await.is_ok(),
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
        &OutgoingMessage::Welcome {
            peer_id: conn_id.clone(),
            is_creator,
            creator_id: creator_id.clone(),
        },
    )
    .await
    {
        tracing::error!("Failed to send welcome message");
        return;
    }

    let handshake = timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            match ws_receiver.next().await {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(IncomingMessage::KeyAnnounce {
                        public_key: Some(key),
                        pq_public_key,
                        sig,
                    }) = serde_json::from_str::<IncomingMessage>(&text)
                    {
                        if !is_valid_mldsa65_public_key(&key) {
                            tracing::warn!("Invalid public key format from {}", conn_id);
                            return None;
                        }
                        match pq_public_key {
                            Some(pq_key) if is_valid_mlkem768_public_key(&pq_key) => {
                                return Some((key, pq_key, sig));
                            }
                            _ => {
                                tracing::warn!("Missing or invalid ML-KEM public key from {}", conn_id);
                                return None;
                            }
                        }
                    }
                    tracing::warn!("Expected key_announce, got: {}", text);
                }
                Some(Ok(Message::Close(_))) | None => {
                    tracing::info!("Client disconnected before key exchange: {}", conn_id);
                    return None;
                }
                Some(Err(e)) => {
                    tracing::error!("WebSocket error during key exchange: {}", e);
                    return None;
                }
                _ => continue,
            }
        }
    })
    .await;

    let (public_key, pq_public_key, announce_sig) = match handshake {
        Ok(Some(keys)) => keys,
        Ok(None) => return,
        Err(_) => {
            tracing::warn!("Key-exchange handshake timed out for {}", conn_id);
            return;
        }
    };

    tracing::info!("Received public key from {}", conn_id);

    let added = match db::try_add_participant(
        &state.db,
        &conn_id,
        &room_id,
        &public_key,
        &pq_public_key,
        announce_sig.as_deref(),
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
        let _ = send_json(&mut ws_sender, &OutgoingMessage::RoomFull).await;
        return;
    }

    match db::get_other_public_keys(&state.db, &room_id, &conn_id).await {
        Ok(peers) => {
            for (peer_id, peer_key, peer_pq_key, peer_sig) in peers {
                if !send_json(
                    &mut ws_sender,
                    &OutgoingMessage::PeerKey {
                        peer_id,
                        public_key: peer_key,
                        pq_public_key: peer_pq_key,
                        sig: peer_sig,
                    },
                )
                .await
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

    let _ = tx.send(RoomEvent::PeerJoined {
        from: conn_id.clone(),
        public_key: public_key.clone(),
        pq_public_key: pq_public_key.clone(),
        sig: announce_sig,
    });

    let mut ping_interval = interval(PING_INTERVAL);
    let mut last_pong = Instant::now();
    let mut waiting_for_pong = false;
    let mut rate_limiter = RateLimiter::new(MSG_RATE_CAPACITY, MSG_RATE_REFILL_PER_SEC);

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if waiting_for_pong && last_pong.elapsed() > PONG_TIMEOUT {
                    tracing::warn!("Connection {} timed out (no pong received)", conn_id);
                    break;
                }

                if ws_sender.send(Message::Ping(vec![])).await.is_err() {
                    tracing::error!("Failed to send ping to {}", conn_id);
                    break;
                }
                waiting_for_pong = true;
            }

            msg = rx.recv() => {
                match msg {
                    Ok(event) => {
                        if event.sender() == Some(conn_id.as_str()) {
                            continue;
                        }

                        if let Some(target) = event.target()
                            && target != conn_id.as_str()
                        {
                            continue;
                        }

                        let is_expiry = matches!(event, RoomEvent::RoomExpired);

                        if let Ok(json) = serde_json::to_string(&OutgoingMessage::from(event))
                            && ws_sender.send(Message::Text(json)).await.is_err()
                        {
                            break;
                        }

                        if is_expiry {
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
                        if !rate_limiter.check(Instant::now()) {
                            tracing::debug!("Rate limit exceeded for {}, dropping message", conn_id);
                            continue;
                        }

                        let incoming: IncomingMessage = match serde_json::from_str(&text) {
                            Ok(m) => m,
                            Err(_) => {
                                tracing::warn!("Invalid message format from {}", conn_id);
                                continue;
                            }
                        };

                        match incoming {
                            IncomingMessage::Message {
                                payload: Some(payload),
                                epoch,
                                counter,
                            } => {
                                if let Err(e) = db::update_activity(&state.db, &room_id).await {
                                    tracing::error!("Failed to update activity: {}", e);
                                }

                                let _ = tx.send(RoomEvent::Chat {
                                    from: conn_id.clone(),
                                    payload,
                                    epoch,
                                    counter,
                                });
                            }
                            IncomingMessage::TreeCommit {
                                tree_commit: Some(tree_commit),
                            } => {
                                tracing::info!("Tree commit from {}", conn_id);
                                let _ = tx.send(RoomEvent::TreeCommit {
                                    from: conn_id.clone(),
                                    tree_data: tree_commit,
                                });
                            }
                            IncomingMessage::TreeWelcome {
                                tree_welcome: Some(tree_welcome),
                                target_peer_id: Some(target_peer_id),
                            } => {
                                tracing::info!("Tree welcome from {} to {}", conn_id, target_peer_id);
                                let _ = tx.send(RoomEvent::TreeWelcome {
                                    from: conn_id.clone(),
                                    target: target_peer_id,
                                    tree_data: tree_welcome,
                                });
                            }
                            IncomingMessage::Typing => {
                                let _ = tx.send(RoomEvent::Typing {
                                    from: conn_id.clone(),
                                });
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

    let _ = tx.send(RoomEvent::PeerLeft {
        from: conn_id.clone(),
    });

    tracing::info!("Connection {} disconnected from room {}", conn_id, room_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    // ML-DSA-65 public keys are 1952 bytes; ML-KEM-768 are 1184 bytes.
    fn key_of_len(len: usize) -> String {
        STANDARD.encode(vec![0u8; len])
    }

    #[test]
    fn accepts_correctly_sized_mldsa65_key() {
        assert!(is_valid_mldsa65_public_key(&key_of_len(1952)));
    }

    #[test]
    fn rejects_wrong_length_mldsa65_key() {
        assert!(!is_valid_mldsa65_public_key(&key_of_len(1951)));
        assert!(!is_valid_mldsa65_public_key(&key_of_len(1953)));
        assert!(!is_valid_mldsa65_public_key(&key_of_len(0)));
    }

    #[test]
    fn rejects_non_base64_mldsa65_key() {
        assert!(!is_valid_mldsa65_public_key("not valid base64!!!"));
    }

    #[test]
    fn accepts_correctly_sized_mlkem768_key() {
        assert!(is_valid_mlkem768_public_key(&key_of_len(1184)));
    }

    #[test]
    fn rejects_wrong_length_mlkem768_key() {
        assert!(!is_valid_mlkem768_public_key(&key_of_len(1183)));
        assert!(!is_valid_mlkem768_public_key(&key_of_len(1185)));
    }

    #[test]
    fn rejects_non_base64_mlkem768_key() {
        assert!(!is_valid_mlkem768_public_key("@@@not-base64@@@"));
    }

    // A key sized for one algorithm must never be accepted as the other.
    #[test]
    fn does_not_confuse_the_two_key_types() {
        assert!(!is_valid_mlkem768_public_key(&key_of_len(1952)));
        assert!(!is_valid_mldsa65_public_key(&key_of_len(1184)));
    }

    #[test]
    fn rate_limiter_allows_burst_up_to_capacity() {
        let t0 = Instant::now();
        let mut rl = RateLimiter::new(3.0, 1.0);
        // The full capacity is available immediately, with no time elapsed.
        assert!(rl.check(t0));
        assert!(rl.check(t0));
        assert!(rl.check(t0));
        // Bucket now empty -> the next frame is dropped.
        assert!(!rl.check(t0));
    }

    #[test]
    fn rate_limiter_refills_over_time() {
        let t0 = Instant::now();
        let mut rl = RateLimiter::new(2.0, 1.0);
        assert!(rl.check(t0));
        assert!(rl.check(t0));
        assert!(!rl.check(t0));
        // One second later, exactly one token has refilled.
        assert!(rl.check(t0 + Duration::from_secs(1)));
        assert!(!rl.check(t0 + Duration::from_secs(1)));
    }

    #[test]
    fn rate_limiter_caps_at_capacity() {
        let t0 = Instant::now();
        let mut rl = RateLimiter::new(2.0, 100.0);
        // After a long idle, tokens must saturate at capacity, not accumulate.
        let later = t0 + Duration::from_secs(10);
        assert!(rl.check(later));
        assert!(rl.check(later));
        assert!(!rl.check(later));
    }

    #[test]
    fn welcome_serializes_with_creator() {
        let json = serde_json::to_string(&OutgoingMessage::Welcome {
            peer_id: "p1".to_string(),
            is_creator: true,
            creator_id: Some("c1".to_string()),
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"welcome","peer_id":"p1","is_creator":true,"creator_id":"c1"}"#
        );
    }

    #[test]
    fn welcome_omits_creator_when_absent() {
        let json = serde_json::to_string(&OutgoingMessage::Welcome {
            peer_id: "p1".to_string(),
            is_creator: false,
            creator_id: None,
        })
        .unwrap();
        assert_eq!(json, r#"{"type":"welcome","peer_id":"p1","is_creator":false}"#);
    }

    #[test]
    fn peer_key_omits_sig_when_absent() {
        let json = serde_json::to_string(&OutgoingMessage::PeerKey {
            peer_id: "p1".to_string(),
            public_key: "PK".to_string(),
            pq_public_key: "PQ".to_string(),
            sig: None,
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"peer_key","peer_id":"p1","public_key":"PK","pq_public_key":"PQ"}"#
        );
    }

    #[test]
    fn peer_key_includes_sig_when_present() {
        let json = serde_json::to_string(&OutgoingMessage::PeerKey {
            peer_id: "p1".to_string(),
            public_key: "PK".to_string(),
            pq_public_key: "PQ".to_string(),
            sig: Some("S".to_string()),
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"peer_key","peer_id":"p1","public_key":"PK","pq_public_key":"PQ","sig":"S"}"#
        );
    }

    #[test]
    fn chat_event_maps_to_message_frame() {
        let json = serde_json::to_string(&OutgoingMessage::from(RoomEvent::Chat {
            from: "p1".to_string(),
            payload: "X".to_string(),
            epoch: Some(7),
            counter: Some(42),
        }))
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"message","peer_id":"p1","payload":"X","epoch":7,"counter":42}"#
        );
    }

    #[test]
    fn chat_event_omits_epoch_and_counter_when_absent() {
        let json = serde_json::to_string(&OutgoingMessage::from(RoomEvent::Chat {
            from: "p1".to_string(),
            payload: "X".to_string(),
            epoch: None,
            counter: None,
        }))
        .unwrap();
        assert_eq!(json, r#"{"type":"message","peer_id":"p1","payload":"X"}"#);
    }

    #[test]
    fn peer_joined_event_maps_to_peer_joined_frame() {
        let json = serde_json::to_string(&OutgoingMessage::from(RoomEvent::PeerJoined {
            from: "p1".to_string(),
            public_key: "PK".to_string(),
            pq_public_key: "PQ".to_string(),
            sig: None,
        }))
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"peer_joined","peer_id":"p1","public_key":"PK","pq_public_key":"PQ"}"#
        );
    }

    #[test]
    fn tree_commit_event_maps_to_tree_commit_frame() {
        let json = serde_json::to_string(&OutgoingMessage::from(RoomEvent::TreeCommit {
            from: "p1".to_string(),
            tree_data: "TC".to_string(),
        }))
        .unwrap();
        assert_eq!(
            json,
            r#"{"type":"tree_commit","peer_id":"p1","tree_commit":"TC"}"#
        );
    }

    #[test]
    fn tree_welcome_event_maps_to_tree_welcome_frame() {
        let json = serde_json::to_string(&OutgoingMessage::from(RoomEvent::TreeWelcome {
            from: "p1".to_string(),
            target: "p2".to_string(),
            tree_data: "TW".to_string(),
        }))
        .unwrap();
        assert_eq!(json, r#"{"type":"tree_welcome","tree_welcome":"TW"}"#);
    }

    #[test]
    fn typing_event_maps_to_typing_frame() {
        let json = serde_json::to_string(&OutgoingMessage::from(RoomEvent::Typing {
            from: "p1".to_string(),
        }))
        .unwrap();
        assert_eq!(json, r#"{"type":"typing","peer_id":"p1"}"#);
    }

    #[test]
    fn peer_left_event_maps_to_peer_left_frame() {
        let json = serde_json::to_string(&OutgoingMessage::from(RoomEvent::PeerLeft {
            from: "p1".to_string(),
        }))
        .unwrap();
        assert_eq!(json, r#"{"type":"peer_left","peer_id":"p1"}"#);
    }

    #[test]
    fn room_expired_event_maps_to_room_expired_frame() {
        let json = serde_json::to_string(&OutgoingMessage::from(RoomEvent::RoomExpired)).unwrap();
        assert_eq!(json, r#"{"type":"room_expired"}"#);
    }

    #[test]
    fn room_full_serializes_as_bare_type() {
        let json = serde_json::to_string(&OutgoingMessage::RoomFull).unwrap();
        assert_eq!(json, r#"{"type":"room_full"}"#);
    }

    #[test]
    fn parses_key_announce_with_all_fields() {
        let msg: IncomingMessage = serde_json::from_str(
            r#"{"type":"key_announce","public_key":"PK","pq_public_key":"PQ","sig":"S"}"#,
        )
        .unwrap();
        match msg {
            IncomingMessage::KeyAnnounce {
                public_key,
                pq_public_key,
                sig,
            } => {
                assert_eq!(public_key.as_deref(), Some("PK"));
                assert_eq!(pq_public_key.as_deref(), Some("PQ"));
                assert_eq!(sig.as_deref(), Some("S"));
            }
            _ => panic!("expected KeyAnnounce"),
        }
    }

    #[test]
    fn parses_message_and_defaults_missing_fields() {
        let msg: IncomingMessage =
            serde_json::from_str(r#"{"type":"message","payload":"X"}"#).unwrap();
        match msg {
            IncomingMessage::Message {
                payload,
                epoch,
                counter,
            } => {
                assert_eq!(payload.as_deref(), Some("X"));
                assert_eq!(epoch, None);
                assert_eq!(counter, None);
            }
            _ => panic!("expected Message"),
        }
    }

    #[test]
    fn unknown_type_deserializes_to_unknown_not_error() {
        let msg: IncomingMessage =
            serde_json::from_str(r#"{"type":"definitely_not_a_real_type","foo":1}"#).unwrap();
        assert!(matches!(msg, IncomingMessage::Unknown));
    }

    #[test]
    fn legacy_and_unknown_fields_are_ignored() {
        let msg: IncomingMessage = serde_json::from_str(
            r#"{"type":"message","payload":"X","pq_ciphertext":"legacy","extra":true}"#,
        )
        .unwrap();
        assert!(matches!(msg, IncomingMessage::Message { .. }));
    }
}
