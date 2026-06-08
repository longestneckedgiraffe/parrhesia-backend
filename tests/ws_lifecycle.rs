//! End-to-end WebSocket lifecycle tests.
//!
//! These spin up the real application (`parrhesia::app`) on an ephemeral port
//! backed by a fresh in-memory database, then drive it with genuine WebSocket
//! clients over the loopback interface. They characterize the full relay
//! handshake — connect -> welcome -> key_announce -> peer_key/peer_joined ->
//! message relay -> peer_left — which `handle_socket` had no coverage for.

use std::net::SocketAddr;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

use parrhesia::config::Config;
use parrhesia::db;
use parrhesia::state::AppState;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ML-DSA-65 public keys are 1952 bytes; ML-KEM-768 are 1184 bytes. The server
// validates only length + base64, so zero-filled keys of the right size pass.
const MLDSA_LEN: usize = 1952;
const MLKEM_LEN: usize = 1184;
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

fn b64_zeros(n: usize) -> String {
    STANDARD.encode(vec![0u8; n])
}

fn test_config() -> Config {
    Config {
        database_url: String::new(),
        port: 0,
        inactivity_expiry_hours: 24,
        cleanup_interval_mins: 5,
    }
}

/// Starts the real app on an ephemeral loopback port, backed by a fresh
/// in-memory DB. Returns the bound address and the pool (so a test can seed a
/// room directly without going through the REST endpoint).
async fn spawn_app() -> (SocketAddr, SqlitePool) {
    // A single shared connection keeps the `:memory:` database alive for the
    // pool's lifetime and serializes access deterministically.
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("open in-memory db");
    db::run_migrations(&pool).await.expect("run migrations");

    let state = AppState::new(pool.clone(), test_config());
    let app = parrhesia::app(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    (addr, pool)
}

async fn create_room(pool: &SqlitePool) -> String {
    let id = Uuid::new_v4().to_string();
    db::create_room(pool, &id).await.expect("create room");
    id
}

async fn open(addr: SocketAddr, room: &str) -> Ws {
    let (ws, _resp) = connect_async(format!("ws://{addr}/ws/{room}"))
        .await
        .expect("ws connect");
    ws
}

/// Reads the next JSON text frame, skipping protocol Ping/Pong frames. The
/// server fires an immediate keepalive ping when its relay loop starts, so any
/// data-frame assertion has to look past it.
async fn next_json(ws: &mut Ws) -> Value {
    loop {
        let frame = timeout(RECV_TIMEOUT, ws.next())
            .await
            .expect("timed out waiting for a ws frame")
            .expect("ws stream ended unexpectedly")
            .expect("ws transport error");
        match frame {
            Message::Text(text) => {
                return serde_json::from_str(&text).expect("frame should be valid JSON");
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("expected a text frame, got: {other:?}"),
        }
    }
}

/// Connects, consumes the `welcome`, sends a valid `key_announce`, and returns
/// the socket together with the welcome payload.
async fn join(addr: SocketAddr, room: &str) -> (Ws, Value) {
    let mut ws = open(addr, room).await;
    let welcome = next_json(&mut ws).await;
    assert_eq!(welcome["type"], "welcome", "first frame should be welcome");

    let announce = json!({
        "type": "key_announce",
        "public_key": b64_zeros(MLDSA_LEN),
        "pq_public_key": b64_zeros(MLKEM_LEN),
    });
    ws.send(Message::Text(announce.to_string()))
        .await
        .expect("send key_announce");

    (ws, welcome)
}

#[tokio::test]
async fn welcome_is_sent_to_the_first_connection() {
    let (addr, pool) = spawn_app().await;
    let room = create_room(&pool).await;

    let (_ws, welcome) = join(addr, &room).await;

    assert_eq!(welcome["type"], "welcome");
    assert_eq!(
        welcome["protocol_version"], 1,
        "welcome announces the protocol version"
    );
    assert!(
        welcome["peer_id"].as_str().is_some_and(|s| !s.is_empty()),
        "welcome should carry a non-empty peer_id"
    );
    assert_eq!(welcome["is_creator"], true, "first joiner is the creator");
}

#[tokio::test]
async fn second_peer_learns_first_key_and_first_is_notified() {
    let (addr, pool) = spawn_app().await;
    let room = create_room(&pool).await;

    let (mut a, welcome_a) = join(addr, &room).await;
    let a_id = welcome_a["peer_id"].as_str().unwrap().to_string();

    let (mut b, welcome_b) = join(addr, &room).await;
    let b_id = welcome_b["peer_id"].as_str().unwrap().to_string();

    // The newcomer is handed each existing peer's key via `peer_key`.
    let peer_key = next_json(&mut b).await;
    assert_eq!(peer_key["type"], "peer_key");
    assert_eq!(peer_key["peer_id"], a_id);
    assert_eq!(peer_key["public_key"], b64_zeros(MLDSA_LEN));
    assert_eq!(peer_key["pq_public_key"], b64_zeros(MLKEM_LEN));

    // Existing peers are told about the newcomer via a broadcast `peer_joined`.
    let joined = next_json(&mut a).await;
    assert_eq!(joined["type"], "peer_joined");
    assert_eq!(joined["peer_id"], b_id);
}

#[tokio::test]
async fn a_message_is_relayed_to_the_other_peer() {
    let (addr, pool) = spawn_app().await;
    let room = create_room(&pool).await;

    let (mut a, welcome_a) = join(addr, &room).await;
    let a_id = welcome_a["peer_id"].as_str().unwrap().to_string();
    let (mut b, _welcome_b) = join(addr, &room).await;

    // Settle the join handshake on both sides.
    let _peer_key = next_json(&mut b).await; // peer_key(A)
    let _joined = next_json(&mut a).await; // peer_joined(B)

    let chat = json!({
        "type": "message",
        "payload": "opaque-ciphertext",
        "epoch": 7,
        "counter": 42,
    });
    a.send(Message::Text(chat.to_string())).await.unwrap();

    let relayed = next_json(&mut b).await;
    assert_eq!(relayed["type"], "message");
    assert_eq!(relayed["peer_id"], a_id, "relayed message keeps the sender id");
    assert_eq!(relayed["payload"], "opaque-ciphertext");
    assert_eq!(relayed["epoch"], 7, "epoch is passed through unchanged");
    assert_eq!(relayed["counter"], 42, "counter is passed through unchanged");
}

#[tokio::test]
async fn disconnect_broadcasts_peer_left() {
    let (addr, pool) = spawn_app().await;
    let room = create_room(&pool).await;

    let (mut a, welcome_a) = join(addr, &room).await;
    let a_id = welcome_a["peer_id"].as_str().unwrap().to_string();
    let (mut b, _welcome_b) = join(addr, &room).await;

    let _peer_key = next_json(&mut b).await; // peer_key(A)
    let _joined = next_json(&mut a).await; // peer_joined(B)

    // A leaves abruptly.
    drop(a);

    let left = next_json(&mut b).await;
    assert_eq!(left["type"], "peer_left");
    assert_eq!(left["peer_id"], a_id);
}

#[tokio::test]
async fn connecting_to_an_unknown_room_is_closed_without_welcome() {
    let (addr, _pool) = spawn_app().await;

    // No room was created, so the relay should drop the socket immediately.
    let mut ws = open(addr, &Uuid::new_v4().to_string()).await;

    let frame = timeout(RECV_TIMEOUT, ws.next())
        .await
        .expect("server should act promptly on an unknown room");
    match frame {
        None | Some(Ok(Message::Close(_))) => {}
        Some(Err(_)) => {} // an abrupt transport-level close is acceptable too
        Some(Ok(other)) => panic!("expected close/none for unknown room, got: {other:?}"),
    }
}
