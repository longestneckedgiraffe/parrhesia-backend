mod cleanup;
mod config;
mod db;
mod routes;
mod security;
mod state;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use http::{header, HeaderValue, Method};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::Config;
use crate::state::AppState;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "parrhesia=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::from_env();
    let port = config.port;

    let pool = db::init_pool(&config.database_url)
        .await
        .expect("Failed to initialize database");

    tracing::info!("Database initialized at {}", config.database_url);

    let state = AppState::new(pool, config);

    cleanup::spawn_cleanup_task(state.clone());

    let cors = CorsLayer::new()
        .allow_origin("https://parrhesia.chat".parse::<HeaderValue>().unwrap())
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::CONTENT_TYPE]);

    let app = Router::new()
        .route("/api/rooms", post(routes::rooms::create_room))
        .route("/api/rooms/:id", get(routes::rooms::get_room))
        .route("/ws/:room_id", get(routes::ws::ws_handler))
        .route("/health", get(|| async { "OK" }))
        .layer(middleware::from_fn(security::security_headers))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Starting server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind to address");

    axum::serve(listener, app)
        .await
        .expect("Server failed");
}
