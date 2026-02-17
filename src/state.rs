use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use crate::config::Config;

pub type RoomChannels = Arc<RwLock<HashMap<String, broadcast::Sender<RoomMessage>>>>;

#[derive(Clone, Debug)]
pub struct RoomMessage {
    pub from_conn_id: String,
    pub target_conn_id: Option<String>,
    pub payload: String,
    pub msg_type: MessageType,
    pub pq_public_key: Option<String>,
    pub pq_ciphertext: Option<String>,
    pub sig: Option<String>,
    pub epoch: Option<u64>,
    pub counter: Option<u64>,
    pub tree_data: Option<String>,
}

#[derive(Clone, Debug)]
pub enum MessageType {
    Chat,
    TreeCommit,
    TreeWelcome,
    PeerJoined,
    PeerLeft,
    RoomExpired,
    Typing,
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
    ) -> broadcast::Sender<RoomMessage> {
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
                let _ = tx.send(RoomMessage {
                    from_conn_id: "system".to_string(),
                    target_conn_id: None,
                    payload: String::new(),
                    msg_type: MessageType::RoomExpired,
                    pq_public_key: None,
                    pq_ciphertext: None,
                    sig: None,
                    epoch: None,
                    counter: None,
                    tree_data: None,
                });
            }
        }
        drop(rooms);

        let mut rooms = self.rooms.write().await;
        for room_id in room_ids {
            rooms.remove(room_id);
        }
    }
}
