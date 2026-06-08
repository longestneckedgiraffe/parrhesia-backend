use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use crate::config::Config;

pub type RoomMap = Arc<RwLock<HashMap<String, Room>>>;

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

#[derive(Clone, Debug)]
pub struct Participant {
    pub public_key: String,
    pub pq_public_key: String,
    pub sig: Option<String>,
}

pub struct Room {
    pub tx: broadcast::Sender<RoomEvent>,
    pub creator_id: Option<String>,
    pub participants: HashMap<String, Participant>,
}

impl Room {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(100);
        Self {
            tx,
            creator_id: None,
            participants: HashMap::new(),
        }
    }
}

pub type PeerSnapshot = Vec<(String, String, String, Option<String>)>;

pub enum JoinOutcome {
    Joined {
        receiver: broadcast::Receiver<RoomEvent>,
        peers: PeerSnapshot,
    },
    Full,
    RoomGone,
}

#[derive(Clone)]
pub struct AppState {
    pub db: SqlitePool,
    pub config: Arc<Config>,
    pub rooms: RoomMap,
}

impl AppState {
    pub fn new(db: SqlitePool, config: Config) -> Self {
        Self {
            db,
            config: Arc::new(config),
            rooms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn attach(
        &self,
        room_id: &str,
        conn_id: &str,
    ) -> (broadcast::Sender<RoomEvent>, bool, Option<String>) {
        let mut rooms = self.rooms.write().await;
        let room = rooms.entry(room_id.to_string()).or_insert_with(Room::new);
        let is_creator = if room.creator_id.is_none() {
            room.creator_id = Some(conn_id.to_string());
            true
        } else {
            false
        };
        (room.tx.clone(), is_creator, room.creator_id.clone())
    }

    pub async fn join(
        &self,
        room_id: &str,
        conn_id: &str,
        participant: Participant,
        max_participants: usize,
    ) -> JoinOutcome {
        let mut rooms = self.rooms.write().await;
        let Some(room) = rooms.get_mut(room_id) else {
            return JoinOutcome::RoomGone;
        };
        if room.participants.len() >= max_participants {
            return JoinOutcome::Full;
        }
        let peers: PeerSnapshot = room
            .participants
            .iter()
            .map(|(id, p)| {
                (
                    id.clone(),
                    p.public_key.clone(),
                    p.pq_public_key.clone(),
                    p.sig.clone(),
                )
            })
            .collect();
        let receiver = room.tx.subscribe();
        room.participants.insert(conn_id.to_string(), participant);
        JoinOutcome::Joined { receiver, peers }
    }

    pub async fn detach(&self, room_id: &str, conn_id: &str) {
        let mut rooms = self.rooms.write().await;
        if let Some(room) = rooms.get_mut(room_id) {
            room.participants.remove(conn_id);
            if room.participants.is_empty() {
                room.creator_id = None;
            }
        }
    }

    pub async fn broadcast_room_expired(&self, room_ids: &[String]) {
        let rooms = self.rooms.read().await;
        for room_id in room_ids {
            if let Some(room) = rooms.get(room_id) {
                let _ = room.tx.send(RoomEvent::RoomExpired);
            }
        }
        drop(rooms);

        let mut rooms = self.rooms.write().await;
        for room_id in room_ids {
            rooms.remove(room_id);
        }
    }
}
