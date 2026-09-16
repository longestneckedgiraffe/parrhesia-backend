//! HTTP and real-socket regression tests for the admission boundary.
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use parrhesia::{
    config::Config,
    db,
    passwords::PasswordService,
    state::{AppState, Participant},
};
use serde_json::{Value, json};
use sqlx::sqlite::SqlitePoolOptions;
use std::{
    net::SocketAddr,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{OnceCell, Semaphore},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tower::ServiceExt;
use uuid::Uuid;

const PASSWORD: &str = "café telescopes drift slowly";
const WAIT: Duration = Duration::from_secs(20);
static HASH: OnceCell<String> = OnceCell::const_new();
// Keep production-cost KDF tests within this small CI host's memory budget.
static KDF_TESTS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(2));
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn hash() -> String {
    HASH.get_or_init(|| async {
        PasswordService::default()
            .hash(PASSWORD.into())
            .await
            .unwrap()
    })
    .await
    .clone()
}

struct TestApp {
    router: Router,
    state: AppState,
    addr: SocketAddr,
    task: JoinHandle<()>,
}
impl Drop for TestApp {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl TestApp {
    async fn new() -> Self {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        db::run_migrations(&pool).await.unwrap();
        let config = Config {
            database_url: String::new(),
            port: 0,
            bind_address: "127.0.0.1".parse().unwrap(),
            allowed_origins: vec!["https://parrhesia.chat".into()],
            trusted_proxies: vec![],
            inactivity_expiry_hours: 24,
            cleanup_interval_mins: 5,
        };
        let state = AppState::new(pool, config);
        let router = parrhesia::app(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = router
            .clone()
            .into_make_service_with_connect_info::<SocketAddr>();
        let task = tokio::spawn(async move {
            axum::serve(listener, service).await.unwrap();
        });
        Self {
            router,
            state,
            addr,
            task,
        }
    }
    async fn seed(&self, hash: Option<&str>) -> String {
        let room = Uuid::new_v4().to_string();
        db::create_room_with_password(&self.state.db, &room, hash)
            .await
            .unwrap();
        room
    }
    async fn http(
        &self,
        method: &str,
        path: &str,
        body: String,
        content_type: Option<&str>,
        ip: u8,
    ) -> (StatusCode, axum::http::HeaderMap, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .extension(ConnectInfo(SocketAddr::from(([192, 0, 2, ip], 5000))));
        if let Some(value) = content_type {
            request = request.header("Content-Type", value);
        }
        let response = self
            .router
            .clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = to_bytes(body, 16384).await.unwrap();
        let value =
            serde_json::from_slice(&body).unwrap_or_else(|_| json!(String::from_utf8_lossy(&body)));
        (parts.status, parts.headers, value)
    }
    async fn open(&self, room: &str) -> Ws {
        let mut request = format!("ws://{}/ws/{room}", self.addr)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("Origin", "https://parrhesia.chat".parse().unwrap());
        connect_async(request).await.unwrap().0
    }
    async fn authenticate(&self, room: &str, password: &str) -> (Ws, Value) {
        let mut ws = self.open(room).await;
        assert_eq!(
            next(&mut ws).await,
            json!({"type":"auth_required", "protocol_version":2})
        );
        send(&mut ws, json!({"type":"authenticate", "password":password})).await;
        let result = next(&mut ws).await;
        (ws, result)
    }
}
async fn send(ws: &mut Ws, value: Value) {
    ws.send(Message::Text(value.to_string())).await.unwrap();
}
async fn next(ws: &mut Ws) -> Value {
    loop {
        match timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap() {
            Message::Text(s) => return serde_json::from_str(&s).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            frame => panic!("expected JSON, got {frame:?}"),
        }
    }
}
async fn closed(ws: &mut Ws) {
    loop {
        match timeout(WAIT, ws.next()).await.unwrap() {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            frame => panic!("unexpected data after rejection: {frame:?}"),
        }
    }
}
fn announce() -> Value {
    json!({"type":"key_announce", "public_key":STANDARD.encode(vec![0;1952]), "pq_public_key":STANDARD.encode(vec![0;1184])})
}

#[tokio::test]
async fn legacy_migration_is_idempotent_and_preserves_rooms() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query("CREATE TABLE rooms (id TEXT PRIMARY KEY, created_at INTEGER NOT NULL, last_activity INTEGER NOT NULL)").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO rooms VALUES ('old', 123, 456)")
        .execute(&pool)
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    assert_eq!(db::room_password(&pool, "old").await.unwrap(), Some(None));
    let timestamps: (i64, i64) =
        sqlx::query_as("SELECT created_at, last_activity FROM rooms WHERE id='old'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(timestamps, (123, 456));
}

#[tokio::test]
async fn http_supports_legacy_and_protected_creation_without_exposing_verifier() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    for (body, protected) in [
        (String::new(), false),
        ("{}".into(), false),
        ("{\"password\":null}".into(), false),
        (json!({"password":PASSWORD}).to_string(), true),
    ] {
        let (status, headers, value) = app
            .http("POST", "/api/rooms", body, Some("application/json"), 1)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(value["password_required"], protected);
        assert_eq!(value.as_object().unwrap().len(), 2);
        let id = value["room_id"].as_str().unwrap();
        let stored = db::room_password(&app.state.db, id).await.unwrap().unwrap();
        assert_eq!(stored.is_some(), protected);
        if let Some(hash) = stored {
            assert!(
                app.state
                    .passwords
                    .verify(PASSWORD.into(), hash)
                    .await
                    .unwrap()
            );
        }
        let (status, headers, value) = app
            .http("GET", &format!("/api/rooms/{id}"), String::new(), None, 1)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(value["password_required"], protected);
        assert_eq!(value.as_object().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn invalid_requests_never_create_open_rooms() {
    let app = TestApp::new().await;
    let common = include_str!("../data/common-passwords.txt")
        .lines()
        .find(|s| s.len() >= 15)
        .unwrap();
    let cases = [
        (
            "{".into(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "null".into(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"password":""}).to_string(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"password":"short"}).to_string(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"password":common}).to_string(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"password":42}).to_string(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"passwrod":PASSWORD}).to_string(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"password":PASSWORD}).to_string(),
            Some("text/plain"),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            json!({"password":"x".repeat(5000)}).to_string(),
            Some("application/json"),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            json!({"password":"🦒".repeat(257)}).to_string(),
            Some("application/json"),
            StatusCode::BAD_REQUEST,
        ),
    ];
    for (i, (body, content_type, expected)) in cases.into_iter().enumerate() {
        let (status, headers, value) = app
            .http("POST", "/api/rooms", body, content_type, i as u8)
            .await;
        assert_eq!(status, expected, "{value}");
        assert_eq!(headers["cache-control"], "no-store");
    }
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM rooms")
        .fetch_one(&app.state.db)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn failed_database_insert_does_not_fall_back_to_unprotected_room() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    sqlx::query("CREATE TRIGGER reject_protected BEFORE INSERT ON rooms WHEN NEW.password_hash IS NOT NULL BEGIN SELECT RAISE(ABORT, 'injected failure'); END").execute(&app.state.db).await.unwrap();
    let (status, _, _) = app
        .http(
            "POST",
            "/api/rooms",
            json!({"password":PASSWORD}).to_string(),
            Some("application/json"),
            1,
        )
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM rooms")
        .fetch_one(&app.state.db)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn creation_rate_limit_has_retry_after() {
    let app = TestApp::new().await;
    for _ in 0..5 {
        assert_eq!(
            app.http("POST", "/api/rooms", String::new(), None, 1)
                .await
                .0,
            StatusCode::OK
        );
    }
    let (status, headers, _) = app.http("POST", "/api/rooms", String::new(), None, 1).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(
        headers["retry-after"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
}

#[tokio::test]
async fn protected_peers_authenticate_before_keys_and_relay() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    let room = app.seed(Some(&hash().await)).await;
    let mut waiting = app.open(&room).await;
    assert_eq!(next(&mut waiting).await["type"], "auth_required");
    assert!(app.state.rooms.read().await.is_empty());
    drop(waiting);
    let (mut a, welcome) = app.authenticate(&room, PASSWORD).await;
    assert_eq!(welcome["type"], "welcome");
    assert_eq!(welcome["protocol_version"], 2);
    assert_eq!(welcome["is_creator"], true);
    send(&mut a, announce()).await;
    assert_eq!(next(&mut a).await["type"], "joined");
    let (mut b, welcome_b) = app.authenticate(&room, PASSWORD).await;
    assert_eq!(welcome_b["is_creator"], false);
    send(&mut b, announce()).await;
    assert_eq!(next(&mut b).await["type"], "joined");
    assert_eq!(next(&mut b).await["type"], "peer_key");
    assert_eq!(next(&mut a).await["type"], "peer_joined");
    // Start this deadline after the deliberately slow debug-build KDF work.
    let mut waiting = app.open(&room).await;
    assert_eq!(next(&mut waiting).await["type"], "auth_required");
    send(
        &mut a,
        json!({"type":"message","payload":"opaque-ciphertext","epoch":7,"counter":42}),
    )
    .await;
    let message = next(&mut b).await;
    assert_eq!(message["payload"], "opaque-ciphertext");
    assert_eq!(message["epoch"], 7);
    assert_eq!(message["counter"], 42);
    assert!(
        timeout(Duration::from_millis(100), waiting.next())
            .await
            .is_err(),
        "unauthenticated socket received room data"
    );
    drop(a);
    assert_eq!(next(&mut b).await["type"], "peer_left");
    drop(waiting);
}

#[tokio::test]
async fn wrong_password_cannot_attach_or_refresh_activity() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    let room = app.seed(Some(&hash().await)).await;
    sqlx::query("UPDATE rooms SET last_activity=123 WHERE id=?")
        .bind(&room)
        .execute(&app.state.db)
        .await
        .unwrap();
    let (mut ws, result) = app
        .authenticate(&room, "different correct length secret")
        .await;
    assert_eq!(result["type"], "auth_failed");
    closed(&mut ws).await;
    assert!(app.state.rooms.read().await.is_empty());
    let (activity,): (i64,) = sqlx::query_as("SELECT last_activity FROM rooms WHERE id=?")
        .bind(&room)
        .fetch_one(&app.state.db)
        .await
        .unwrap();
    assert_eq!(activity, 123);
}

#[tokio::test]
async fn malformed_authentication_and_corrupt_verifiers_fail_closed() {
    let app = TestApp::new().await;
    let room = app.seed(Some("corrupt-hash")).await;
    for frame in [
        json!({"type":"key_announce"}).to_string(),
        "{".into(),
        json!({"type":"authenticate"}).to_string(),
        json!({"type":"authenticate","password":PASSWORD,"extra":true}).to_string(),
        "x".repeat(4097),
    ] {
        let mut ws = app.open(&room).await;
        assert_eq!(next(&mut ws).await["type"], "auth_required");
        ws.send(Message::Text(frame)).await.unwrap();
        assert_eq!(next(&mut ws).await["type"], "auth_failed");
        closed(&mut ws).await;
    }
    let (mut ws, result) = app.authenticate(&room, PASSWORD).await;
    assert_eq!(result["type"], "auth_unavailable");
    closed(&mut ws).await;
    assert!(app.state.rooms.read().await.is_empty());
}

#[tokio::test]
async fn room_limit_survives_socket_reconnects() {
    let app = TestApp::new().await;
    let room = app.seed(Some("corrupt-hash")).await;
    for _ in 0..20 {
        let (mut ws, result) = app.authenticate(&room, "short").await;
        assert_eq!(result["type"], "auth_failed");
        closed(&mut ws).await;
    }
    let (mut ws, result) = app.authenticate(&room, PASSWORD).await;
    assert_eq!(result["type"], "auth_rate_limited");
    assert!(result["retry_after_secs"].as_u64().unwrap() > 0);
    closed(&mut ws).await;
}

#[tokio::test]
async fn origins_are_checked_and_untrusted_forwarded_headers_cannot_evade_limits() {
    let app = TestApp::new().await;
    let room = app.seed(None).await;
    for origin in [
        None,
        Some("null"),
        Some("https://evil.example"),
        Some("https://parrhesia.chat.evil.example"),
    ] {
        let mut request = format!("ws://{}/ws/{room}", app.addr)
            .into_client_request()
            .unwrap();
        if let Some(origin) = origin {
            request
                .headers_mut()
                .insert("Origin", origin.parse().unwrap());
        }
        match connect_async(request).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::FORBIDDEN)
            }
            _ => panic!("invalid origin was not rejected"),
        }
    }
    for i in 0..31 {
        let mut request = format!("ws://{}/ws/{room}", app.addr)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("Origin", "https://parrhesia.chat".parse().unwrap());
        request.headers_mut().insert(
            "x-parrhesia-client-ip",
            format!("192.0.2.{i}").parse().unwrap(),
        );
        let result = connect_async(request).await;
        if i < 30 {
            drop(result.unwrap());
        } else {
            match result {
                Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
                    assert!(response.headers().contains_key("retry-after"));
                }
                _ => panic!("IP budget bypassed"),
            }
        }
    }
}

#[tokio::test]
async fn pending_limit_is_released_on_disconnect() {
    let app = TestApp::new().await;
    let room = app.seed(Some("corrupt-hash")).await;
    let mut waiting = Vec::new();
    for _ in 0..10 {
        let mut ws = app.open(&room).await;
        assert_eq!(next(&mut ws).await["type"], "auth_required");
        waiting.push(ws);
    }
    let mut request = format!("ws://{}/ws/{room}", app.addr)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Origin", "https://parrhesia.chat".parse().unwrap());
    match connect_async(request).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE)
        }
        _ => panic!("pending limit bypassed"),
    }
    // Explicit rejection consumes and releases one pending slot.
    send(
        &mut waiting[0],
        json!({"type":"authenticate","password":"short"}),
    )
    .await;
    assert_eq!(next(&mut waiting[0]).await["type"], "auth_failed");
    closed(&mut waiting[0]).await;
    let mut another = app.open(&room).await;
    assert_eq!(next(&mut another).await["type"], "auth_required");
    assert!(app.state.rooms.read().await.is_empty());
}

#[tokio::test]
async fn expiration_during_authentication_never_recreates_room() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    let room = app.seed(Some(&hash().await)).await;
    let mut ws = app.open(&room).await;
    assert_eq!(next(&mut ws).await["type"], "auth_required");
    // Hold the lifecycle lock while verification runs, then expire before attach.
    let rooms = app.state.rooms.write().await;
    send(&mut ws, json!({"type":"authenticate","password":PASSWORD})).await;
    sqlx::query("UPDATE rooms SET last_activity=0 WHERE id=?")
        .bind(&room)
        .execute(&app.state.db)
        .await
        .unwrap();
    let deleted = db::delete_inactive_rooms(&app.state.db, 1).await.unwrap();
    assert_eq!(deleted, vec![room.clone()]);
    drop(rooms);
    assert_eq!(next(&mut ws).await["type"], "auth_unavailable");
    closed(&mut ws).await;
    assert!(app.state.rooms.read().await.is_empty());
    assert_eq!(db::room_password(&app.state.db, &room).await.unwrap(), None);
}

#[tokio::test]
async fn protected_rooms_still_enforce_key_validation_and_capacity() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    let room = app.seed(Some(&hash().await)).await;
    let (mut invalid, welcome) = app.authenticate(&room, PASSWORD).await;
    assert_eq!(welcome["type"], "welcome");
    send(
        &mut invalid,
        json!({"type":"key_announce","public_key":"invalid"}),
    )
    .await;
    closed(&mut invalid).await;
    assert!(app.state.rooms.read().await[&room].participants.is_empty());
    let (mut full, welcome) = app.authenticate(&room, PASSWORD).await;
    assert_eq!(welcome["type"], "welcome");
    {
        let mut rooms = app.state.rooms.write().await;
        for i in 0..16 {
            rooms.get_mut(&room).unwrap().participants.insert(
                i.to_string(),
                Participant {
                    public_key: "unused".into(),
                    pq_public_key: "unused".into(),
                    sig: None,
                },
            );
        }
    }
    send(&mut full, announce()).await;
    assert_eq!(next(&mut full).await["type"], "room_full");
    closed(&mut full).await;
    assert_eq!(app.state.rooms.read().await[&room].participants.len(), 16);
}

#[tokio::test]
async fn missing_authentication_times_out_without_creating_state() {
    let app = TestApp::new().await;
    let room = app.seed(Some("corrupt-hash")).await;
    let mut ws = app.open(&room).await;
    assert_eq!(next(&mut ws).await["type"], "auth_required");
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::time::resume();
    assert_eq!(next(&mut ws).await["type"], "auth_failed");
    closed(&mut ws).await;
    assert!(app.state.rooms.read().await.is_empty());
}

#[derive(Clone)]
struct LogWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn unexpected_handshake_frames_do_not_leak_credentials_to_logs() {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = LogWriter(bytes.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let app = TestApp::new().await;
    let room = app.seed(None).await;
    let mut ws = app.open(&room).await;
    assert_eq!(next(&mut ws).await["type"], "welcome");
    send(
        &mut ws,
        json!({"type":"authenticate","password":"NEVER_LOG_THIS_SECRET_4379"}),
    )
    .await;
    closed(&mut ws).await;
    let logs = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("Expected key_announce"));
    assert!(!logs.contains("NEVER_LOG_THIS_SECRET_4379"));
}

#[tokio::test]
async fn simultaneous_protected_joiners_and_expiration_follow_existing_lifecycle() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    let room = app.seed(Some(&hash().await)).await;
    let ((mut a, wa), (mut b, wb)) = tokio::join!(
        app.authenticate(&room, PASSWORD),
        app.authenticate(&room, PASSWORD)
    );
    assert_eq!(wa["type"], "welcome");
    assert_eq!(wb["type"], "welcome");
    assert_ne!(wa["is_creator"], wb["is_creator"]);
    send(&mut a, announce()).await;
    send(&mut b, announce()).await;
    assert_eq!(next(&mut a).await["type"], "joined");
    assert_eq!(next(&mut b).await["type"], "joined");
    assert_eq!(next(&mut a).await["peer_id"], wb["peer_id"]);
    assert_eq!(next(&mut b).await["peer_id"], wa["peer_id"]);
    sqlx::query("UPDATE rooms SET last_activity=0 WHERE id=?")
        .bind(&room)
        .execute(&app.state.db)
        .await
        .unwrap();
    assert_eq!(
        app.state.expire_inactive_rooms(1).await.unwrap(),
        vec![room.clone()]
    );
    assert_eq!(next(&mut a).await["type"], "room_expired");
    assert_eq!(next(&mut b).await["type"], "room_expired");
    closed(&mut a).await;
    closed(&mut b).await;
    assert_eq!(db::room_password(&app.state.db, &room).await.unwrap(), None);
    assert!(app.state.rooms.read().await.is_empty());
}

#[tokio::test]
async fn binary_authentication_and_pipelined_frames_are_rejected() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    let room = app.seed(Some(&hash().await)).await;
    let mut binary = app.open(&room).await;
    assert_eq!(next(&mut binary).await["type"], "auth_required");
    binary.send(Message::Binary(vec![0, 1, 2])).await.unwrap();
    assert_eq!(next(&mut binary).await["type"], "auth_failed");
    closed(&mut binary).await;
    let mut pipeline = app.open(&room).await;
    assert_eq!(next(&mut pipeline).await["type"], "auth_required");
    send(
        &mut pipeline,
        json!({"type":"authenticate","password":PASSWORD}),
    )
    .await;
    send(&mut pipeline, announce()).await;
    assert_eq!(next(&mut pipeline).await["type"], "auth_failed");
    closed(&mut pipeline).await;
    assert!(app.state.rooms.read().await.is_empty());
}

#[tokio::test]
async fn successful_password_does_not_remove_key_announcement_deadline() {
    let _guard = KDF_TESTS.acquire().await.unwrap();
    let app = TestApp::new().await;
    let room = app.seed(Some(&hash().await)).await;
    let (mut ws, welcome) = app.authenticate(&room, PASSWORD).await;
    assert_eq!(welcome["type"], "welcome");
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::time::resume();
    closed(&mut ws).await;
    let rooms = app.state.rooms.read().await;
    assert!(rooms[&room].participants.is_empty());
    assert_eq!(rooms[&room].creator_id, None);
}
