use std::env;

pub struct Config {
    pub database_url: String,
    pub port: u16,
    pub inactivity_expiry_hours: u64,
    pub cleanup_interval_mins: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "sqlite:./parrhesia.db?mode=rwc".to_string()),
            port: env::var("PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3000),
            inactivity_expiry_hours: env::var("INACTIVITY_EXPIRY_HOURS")
                .ok()
                .and_then(|h| h.parse().ok())
                .unwrap_or(24),
            cleanup_interval_mins: env::var("CLEANUP_INTERVAL_MINS")
                .ok()
                .and_then(|m| m.parse().ok())
                .unwrap_or(5),
        }
    }

    pub fn inactivity_expiry_secs(&self) -> i64 {
        (self.inactivity_expiry_hours * 3600) as i64
    }
}
