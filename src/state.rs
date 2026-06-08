use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use crate::config::Config;

pub type RoomChannels = Arc<RwLock<HashMap<String, broadcast::Sender<RoomEvent>>>>;

#[derive(Clone, Debug)]
pub enum RoomEvent {
    PeerJoined {
        from: String,
        public_key: String,
        pq_public_key: String,
        sig: Option<String>,
    },
    Chat {
        from: String,
        payload: String,
        epoch: Option<u64>,
        counter: Option<u64>,
    },
    TreeCommit {
        from: String,
        tree_data: String,
    },
    TreeWelcome {
        from: String,
        target: String,
        tree_data: String,
    },
    Typing {
        from: String,
    },
    PeerLeft {
        from: String,
    },
    RoomExpired,
}

impl RoomEvent {
    pub fn sender(&self) -> Option<&str> {
        match self {
            RoomEvent::PeerJoined { from, .. }
            | RoomEvent::Chat { from, .. }
            | RoomEvent::TreeCommit { from, .. }
            | RoomEvent::TreeWelcome { from, .. }
            | RoomEvent::Typing { from }
            | RoomEvent::PeerLeft { from } => Some(from),
            RoomEvent::RoomExpired => None,
        }
    }

    pub fn target(&self) -> Option<&str> {
        match self {
            RoomEvent::TreeWelcome { target, .. } => Some(target),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub db: SqlitePool,
    pub config: Arc<Config>,
    pub rooms: RoomChannels,
}

impl AppState {
    pub fn new(db: SqlitePool, config: Config) -> Self {
        Self {
            db,
            config: Arc::new(config),
            rooms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_or_create_channel(
        &self,
        room_id: &str,
    ) -> broadcast::Sender<RoomEvent> {
        let mut rooms = self.rooms.write().await;
        rooms
            .entry(room_id.to_string())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(100);
                tx
            })
            .clone()
    }

    pub async fn remove_channel(&self, room_id: &str) {
        let mut rooms = self.rooms.write().await;
        rooms.remove(room_id);
    }

    pub async fn broadcast_room_expired(&self, room_ids: &[String]) {
        let rooms = self.rooms.read().await;
        for room_id in room_ids {
            if let Some(tx) = rooms.get(room_id) {
                let _ = tx.send(RoomEvent::RoomExpired);
            }
        }
        drop(rooms);

        let mut rooms = self.rooms.write().await;
        for room_id in room_ids {
            rooms.remove(room_id);
        }
    }
}
