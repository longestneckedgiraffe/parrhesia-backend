use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn init_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    run_migrations(&pool).await?;

    Ok(pool)
}

async fn run_migrations(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS rooms (
            id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            last_activity INTEGER NOT NULL,
            creator_id TEXT
        )
        "#,
    )
    .execute(pool)
    .await?;

    let _ = sqlx::query("ALTER TABLE rooms ADD COLUMN creator_id TEXT")
        .execute(pool)
        .await;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS participants (
            id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            public_key TEXT NOT NULL,
            pq_public_key TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        )
        "#,
    )
    .execute(pool)
    .await?;

    let _ = sqlx::query("ALTER TABLE participants ADD COLUMN sig TEXT")
        .execute(pool)
        .await;

    Ok(())
}

fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

pub async fn create_room(pool: &SqlitePool, room_id: &str) -> Result<(), sqlx::Error> {
    let now = now_timestamp();
    sqlx::query("INSERT INTO rooms (id, created_at, last_activity) VALUES (?, ?, ?)")
        .bind(room_id)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn room_exists(pool: &SqlitePool, room_id: &str) -> Result<bool, sqlx::Error> {
    let result: Option<(String,)> =
        sqlx::query_as("SELECT id FROM rooms WHERE id = ?")
            .bind(room_id)
            .fetch_optional(pool)
            .await?;
    Ok(result.is_some())
}

pub async fn update_activity(pool: &SqlitePool, room_id: &str) -> Result<(), sqlx::Error> {
    let now = now_timestamp();
    sqlx::query("UPDATE rooms SET last_activity = ? WHERE id = ?")
        .bind(now)
        .bind(room_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_inactive_rooms(
    pool: &SqlitePool,
    inactivity_threshold_secs: i64,
) -> Result<Vec<String>, sqlx::Error> {
    let cutoff = now_timestamp() - inactivity_threshold_secs;

    let rooms: Vec<(String,)> =
        sqlx::query_as("SELECT id FROM rooms WHERE last_activity < ?")
            .bind(cutoff)
            .fetch_all(pool)
            .await?;

    let room_ids: Vec<String> = rooms.into_iter().map(|(id,)| id).collect();

    sqlx::query("DELETE FROM rooms WHERE last_activity < ?")
        .bind(cutoff)
        .execute(pool)
        .await?;

    Ok(room_ids)
}

pub async fn store_public_key(
    pool: &SqlitePool,
    participant_id: &str,
    room_id: &str,
    public_key: &str,
) -> Result<(), sqlx::Error> {
    let now = now_timestamp();
    sqlx::query(
        "INSERT OR REPLACE INTO participants (id, room_id, public_key, created_at) VALUES (?, ?, ?, ?)",
    )
    .bind(participant_id)
    .bind(room_id)
    .bind(public_key)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_other_public_keys(
    pool: &SqlitePool,
    room_id: &str,
    exclude_participant_id: &str,
) -> Result<Vec<(String, String, String, Option<String>)>, sqlx::Error> {
    let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT id, public_key, pq_public_key, sig FROM participants WHERE room_id = ? AND id != ?",
    )
    .bind(room_id)
    .bind(exclude_participant_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn remove_participant(
    pool: &SqlitePool,
    participant_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM participants WHERE id = ?")
        .bind(participant_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn remove_room_participants(
    pool: &SqlitePool,
    room_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM participants WHERE room_id = ?")
        .bind(room_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_creator(
    pool: &SqlitePool,
    room_id: &str,
    creator_id: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE rooms SET creator_id = ? WHERE id = ? AND creator_id IS NULL",
    )
    .bind(creator_id)
    .bind(room_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn get_creator(
    pool: &SqlitePool,
    room_id: &str,
) -> Result<Option<String>, sqlx::Error> {
    let result: Option<(Option<String>,)> =
        sqlx::query_as("SELECT creator_id FROM rooms WHERE id = ?")
            .bind(room_id)
            .fetch_optional(pool)
            .await?;
    Ok(result.and_then(|(id,)| id))
}

pub async fn count_participants(pool: &SqlitePool, room_id: &str) -> Result<i64, sqlx::Error> {
    let result: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM participants WHERE room_id = ?")
            .bind(room_id)
            .fetch_one(pool)
            .await?;
    Ok(result.0)
}

pub async fn try_add_participant(
    pool: &SqlitePool,
    participant_id: &str,
    room_id: &str,
    public_key: &str,
    pq_public_key: &str,
    sig: Option<&str>,
    max_participants: i64,
) -> Result<bool, sqlx::Error> {
    let now = now_timestamp();

    let result = sqlx::query(
        r#"
        INSERT INTO participants (id, room_id, public_key, pq_public_key, sig, created_at)
        SELECT ?, ?, ?, ?, ?, ?
        WHERE (SELECT COUNT(*) FROM participants WHERE room_id = ?) < ?
        "#,
    )
    .bind(participant_id)
    .bind(room_id)
    .bind(public_key)
    .bind(pq_public_key)
    .bind(sig)
    .bind(now)
    .bind(room_id)
    .bind(max_participants)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn reset_creator_if_empty(pool: &SqlitePool, room_id: &str) -> Result<bool, sqlx::Error> {
    let count = count_participants(pool, room_id).await?;
    if count == 0 {
        sqlx::query("UPDATE rooms SET creator_id = NULL WHERE id = ?")
            .bind(room_id)
            .execute(pool)
            .await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A fresh, isolated in-memory database per test. `max_connections(1)` keeps
    // every query on the same connection, so the `:memory:` database persists
    // for the lifetime of the pool instead of being recreated per acquire.
    async fn test_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("failed to open in-memory database");
        run_migrations(&pool)
            .await
            .expect("failed to run migrations");
        pool
    }

    #[tokio::test]
    async fn create_and_check_room() {
        let pool = test_pool().await;

        assert!(!room_exists(&pool, "room1").await.unwrap());
        create_room(&pool, "room1").await.unwrap();
        assert!(room_exists(&pool, "room1").await.unwrap());
        assert!(!room_exists(&pool, "missing").await.unwrap());
    }

    #[tokio::test]
    async fn participant_cap_is_enforced_per_room() {
        let pool = test_pool().await;
        create_room(&pool, "a").await.unwrap();
        create_room(&pool, "b").await.unwrap();

        // Cap of 2 in room "a": the first two succeed, the third is rejected.
        assert!(try_add_participant(&pool, "p1", "a", "k1", "pq1", None, 2).await.unwrap());
        assert!(try_add_participant(&pool, "p2", "a", "k2", "pq2", None, 2).await.unwrap());
        assert!(!try_add_participant(&pool, "p3", "a", "k3", "pq3", None, 2).await.unwrap());
        assert_eq!(count_participants(&pool, "a").await.unwrap(), 2);

        // The cap is per-room: a full room "a" must not block room "b".
        assert!(try_add_participant(&pool, "p4", "b", "k4", "pq4", None, 2).await.unwrap());
        assert_eq!(count_participants(&pool, "b").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn first_connection_becomes_creator() {
        let pool = test_pool().await;
        create_room(&pool, "r").await.unwrap();

        assert!(set_creator(&pool, "r", "first").await.unwrap());
        // A later connection must not overwrite the existing creator.
        assert!(!set_creator(&pool, "r", "second").await.unwrap());
        assert_eq!(
            get_creator(&pool, "r").await.unwrap(),
            Some("first".to_string())
        );
    }

    #[tokio::test]
    async fn reset_creator_only_when_room_is_empty() {
        let pool = test_pool().await;
        create_room(&pool, "r").await.unwrap();
        set_creator(&pool, "r", "owner").await.unwrap();
        try_add_participant(&pool, "owner", "r", "k", "pq", None, 16).await.unwrap();

        // Still occupied -> no reset.
        assert!(!reset_creator_if_empty(&pool, "r").await.unwrap());
        assert_eq!(
            get_creator(&pool, "r").await.unwrap(),
            Some("owner".to_string())
        );

        // Now empty -> creator is cleared so the next joiner can claim it.
        remove_participant(&pool, "owner").await.unwrap();
        assert!(reset_creator_if_empty(&pool, "r").await.unwrap());
        assert_eq!(get_creator(&pool, "r").await.unwrap(), None);
    }

    #[tokio::test]
    async fn get_other_public_keys_excludes_self() {
        let pool = test_pool().await;
        create_room(&pool, "r").await.unwrap();
        try_add_participant(&pool, "me", "r", "mykey", "mypq", None, 16).await.unwrap();
        try_add_participant(&pool, "peer", "r", "peerkey", "peerpq", Some("peersig"), 16)
            .await
            .unwrap();

        let others = get_other_public_keys(&pool, "r", "me").await.unwrap();
        assert_eq!(others.len(), 1);
        let (id, key, pq, sig) = &others[0];
        assert_eq!(id, "peer");
        assert_eq!(key, "peerkey");
        assert_eq!(pq, "peerpq");
        assert_eq!(sig.as_deref(), Some("peersig"));
    }

    #[tokio::test]
    async fn remove_room_participants_clears_only_that_room() {
        let pool = test_pool().await;
        create_room(&pool, "r").await.unwrap();
        create_room(&pool, "other").await.unwrap();
        try_add_participant(&pool, "p1", "r", "k1", "pq1", None, 16).await.unwrap();
        try_add_participant(&pool, "p2", "r", "k2", "pq2", None, 16).await.unwrap();
        try_add_participant(&pool, "p3", "other", "k3", "pq3", None, 16).await.unwrap();

        remove_room_participants(&pool, "r").await.unwrap();
        assert_eq!(count_participants(&pool, "r").await.unwrap(), 0);
        assert_eq!(count_participants(&pool, "other").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn delete_inactive_rooms_targets_only_stale_rooms() {
        let pool = test_pool().await;
        create_room(&pool, "stale").await.unwrap();
        create_room(&pool, "fresh").await.unwrap();

        // Backdate the stale room well beyond any threshold.
        sqlx::query("UPDATE rooms SET last_activity = 0 WHERE id = ?")
            .bind("stale")
            .execute(&pool)
            .await
            .unwrap();

        let deleted = delete_inactive_rooms(&pool, 3600).await.unwrap();
        assert_eq!(deleted, vec!["stale".to_string()]);
        assert!(!room_exists(&pool, "stale").await.unwrap());
        assert!(room_exists(&pool, "fresh").await.unwrap());
    }

    #[tokio::test]
    async fn update_activity_refreshes_timestamp() {
        let pool = test_pool().await;
        create_room(&pool, "r").await.unwrap();
        sqlx::query("UPDATE rooms SET last_activity = 0 WHERE id = ?")
            .bind("r")
            .execute(&pool)
            .await
            .unwrap();

        update_activity(&pool, "r").await.unwrap();

        let (last_activity,): (i64,) =
            sqlx::query_as("SELECT last_activity FROM rooms WHERE id = ?")
                .bind("r")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(last_activity > 0);
    }

    #[tokio::test]
    async fn store_public_key_replaces_existing_row() {
        let pool = test_pool().await;
        create_room(&pool, "r").await.unwrap();
        store_public_key(&pool, "p", "r", "first").await.unwrap();
        store_public_key(&pool, "p", "r", "second").await.unwrap();

        // INSERT OR REPLACE must keep a single row holding the latest key.
        assert_eq!(count_participants(&pool, "r").await.unwrap(), 1);
        let others = get_other_public_keys(&pool, "r", "someone-else").await.unwrap();
        assert_eq!(others[0].1, "second");
    }
}
