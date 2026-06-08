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

pub async fn run_migrations(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS rooms (
            id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            last_activity INTEGER NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

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
    let result: Option<(String,)> = sqlx::query_as("SELECT id FROM rooms WHERE id = ?")
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

    let rooms: Vec<(String,)> = sqlx::query_as("SELECT id FROM rooms WHERE last_activity < ?")
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("failed to open in-memory database");
        run_migrations(&pool).await.expect("failed to run migrations");
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
    async fn delete_inactive_rooms_targets_only_stale_rooms() {
        let pool = test_pool().await;
        create_room(&pool, "stale").await.unwrap();
        create_room(&pool, "fresh").await.unwrap();

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

        let (last_activity,): (i64,) = sqlx::query_as("SELECT last_activity FROM rooms WHERE id = ?")
            .bind("r")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(last_activity > 0);
    }
}
