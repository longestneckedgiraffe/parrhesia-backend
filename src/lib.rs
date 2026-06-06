pub mod cleanup;
pub mod config;
pub mod db;
pub mod state;

mod routes;
mod security;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use http::{header, HeaderValue, Method};
use tower_http::cors::CorsLayer;

use crate::state::AppState;

/// Builds the application router: routes, CORS, and security-header middleware.
///
/// Shared by `main` (production startup) and the integration tests so both
/// exercise the exact same wiring — the tests run against the real stack, not a
/// hand-rolled subset of it.
pub fn app(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin("https://parrhesia.chat".parse::<HeaderValue>().unwrap())
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::CONTENT_TYPE]);

    Router::new()
        .route("/api/rooms", post(routes::rooms::create_room))
        .route("/api/rooms/:id", get(routes::rooms::get_room))
        .route("/ws/:room_id", get(routes::ws::ws_handler))
        .route("/health", get(|| async { "OK" }))
        .layer(middleware::from_fn(security::security_headers))
        .layer(cors)
        .with_state(state)
}
