use std::{env, net::IpAddr};

pub struct Config {
    pub database_url: String,
    pub port: u16,
    pub bind_address: IpAddr,
    pub allowed_origins: Vec<String>,
    pub trusted_proxies: Vec<IpAddr>,
    pub inactivity_expiry_hours: u64,
    pub cleanup_interval_mins: u64,
}

impl Config {
    pub fn from_env() -> Self {
        let allowed_origins: Vec<String> = env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "https://parrhesia.chat".into())
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();
        assert!(
            !allowed_origins.is_empty()
                && allowed_origins.iter().all(|s| {
                    let Ok(uri) = s.parse::<http::Uri>() else {
                        return false;
                    };
                    matches!(uri.scheme_str(), Some("https" | "http"))
                        && uri.authority().is_some_and(|a| !a.as_str().contains('@'))
                        && uri.path() == "/"
                        && uri.query().is_none()
                        && !s.ends_with('/')
                        && s.parse::<http::HeaderValue>().is_ok()
                }),
            "ALLOWED_ORIGINS must contain exact http(s) origins without trailing slashes"
        );
        Self {
            bind_address: env::var("BIND_ADDRESS")
                .unwrap_or_else(|_| "127.0.0.1".into())
                .parse()
                .expect("BIND_ADDRESS must be an IP address"),
            allowed_origins,
            trusted_proxies: env::var("TRUSTED_PROXY_IPS")
                .unwrap_or_default()
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| {
                    s.trim()
                        .parse()
                        .expect("TRUSTED_PROXY_IPS must contain IP addresses")
                })
                .collect(),
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
            bind_address: "127.0.0.1".parse().unwrap(),
            allowed_origins: vec!["https://parrhesia.chat".into()],
            trusted_proxies: vec![],
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
