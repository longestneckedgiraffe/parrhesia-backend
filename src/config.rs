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

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_expiry(hours: u64) -> Config {
        Config {
            database_url: String::new(),
            port: 0,
            inactivity_expiry_hours: hours,
            cleanup_interval_mins: 5,
        }
    }

    #[test]
    fn expiry_hours_convert_to_seconds() {
        assert_eq!(config_with_expiry(24).inactivity_expiry_secs(), 86_400);
        assert_eq!(config_with_expiry(1).inactivity_expiry_secs(), 3_600);
    }

    #[test]
    fn zero_hours_is_zero_seconds() {
        assert_eq!(config_with_expiry(0).inactivity_expiry_secs(), 0);
    }
}
