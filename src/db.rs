use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn init_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

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
    .execute(&pool)
    .await?;

    let _ = sqlx::query("ALTER TABLE rooms ADD COLUMN creator_id TEXT")
        .execute(&pool)
        .await;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS participants (
            id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            public_key TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY (room_id) REFERENCES rooms(id) ON DELETE CASCADE
        )
        "#,
    )
    .execute(&pool)
    .await?;

    Ok(pool)
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
) -> Result<Vec<(String, String)>, sqlx::Error> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, public_key FROM participants WHERE room_id = ? AND id != ?",
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
