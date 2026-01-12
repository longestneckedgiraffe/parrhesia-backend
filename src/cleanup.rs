use std::time::Duration;
use tokio::time::interval;

use crate::db;
use crate::state::AppState;

pub fn spawn_cleanup_task(state: AppState) {
    let interval_mins = state.config.cleanup_interval_mins;
    let expiry_secs = state.config.inactivity_expiry_secs();

    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_mins * 60));

        loop {
            ticker.tick().await;

            tracing::debug!("Running cleanup task...");

            match db::delete_inactive_rooms(&state.db, expiry_secs).await {
                Ok(deleted_rooms) => {
                    if !deleted_rooms.is_empty() {
                        tracing::info!("Deleted {} inactive rooms", deleted_rooms.len());
                        state.broadcast_room_expired(&deleted_rooms).await;
                    }
                }
                Err(e) => {
                    tracing::error!("Cleanup task failed: {}", e);
                }
            }
        }
    });

    tracing::info!(
        "Cleanup task started: checking every {} mins, expiring after {} hours",
        interval_mins,
        expiry_secs / 3600
    );
}
