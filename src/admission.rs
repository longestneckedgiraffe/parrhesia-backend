//! Bounded, process-local abuse controls; no persistent client address records.
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

const MAX_BUCKETS: usize = 10_000;
const IDLE_TTL: Duration = Duration::from_secs(600);

#[derive(Hash, PartialEq, Eq)]
pub enum Budget {
    Connection(IpAddr),
    Creation(IpAddr),
    Room(String),
}

struct Bucket {
    tokens: f64,
    updated: Instant,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AdmissionError {
    Limited(u64),
    Unavailable,
}

#[derive(Default)]
struct Limits {
    buckets: HashMap<Budget, Bucket>,
    pending: HashMap<IpAddr, usize>,
    pending_total: usize,
}

#[derive(Default)]
pub struct Admission {
    limits: Mutex<Limits>,
    pub rejected: AtomicU64,
    pub throttled: AtomicU64,
    pub verification_failed: AtomicU64,
    pub unavailable: AtomicU64,
}

impl Admission {
    pub fn check(&self, key: Budget) -> Result<(), AdmissionError> {
        self.check_at(key, Instant::now())
    }

    fn check_at(&self, key: Budget, now: Instant) -> Result<(), AdmissionError> {
        let capacity = match key {
            Budget::Connection(_) => 30.0,
            Budget::Creation(_) => 5.0,
            Budget::Room(_) => 20.0,
        };
        let mut limits = self.limits.lock().unwrap();
        if !limits.buckets.contains_key(&key) && limits.buckets.len() >= MAX_BUCKETS {
            limits
                .buckets
                .retain(|_, bucket| now.saturating_duration_since(bucket.updated) < IDLE_TTL);
            if limits.buckets.len() >= MAX_BUCKETS {
                self.unavailable.fetch_add(1, Ordering::Relaxed);
                return Err(AdmissionError::Unavailable);
            }
        }
        let bucket = limits.buckets.entry(key).or_insert(Bucket {
            tokens: capacity,
            updated: now,
        });
        let refill = capacity / 60.0;
        bucket.tokens = (bucket.tokens
            + now.saturating_duration_since(bucket.updated).as_secs_f64() * refill)
            .min(capacity);
        bucket.updated = now;
        if bucket.tokens < 1.0 {
            self.throttled.fetch_add(1, Ordering::Relaxed);
            return Err(AdmissionError::Limited(
                ((1.0 - bucket.tokens) / refill).ceil() as u64,
            ));
        }
        bucket.tokens -= 1.0;
        Ok(())
    }

    pub fn pending(self: &Arc<Self>, ip: IpAddr) -> Result<PendingAuth, AdmissionError> {
        let mut limits = self.limits.lock().unwrap();
        if limits.pending_total >= 100 || limits.pending.get(&ip).copied().unwrap_or(0) >= 10 {
            self.unavailable.fetch_add(1, Ordering::Relaxed);
            return Err(AdmissionError::Unavailable);
        }
        *limits.pending.entry(ip).or_default() += 1;
        limits.pending_total += 1;
        Ok(PendingAuth {
            admission: self.clone(),
            ip,
        })
    }

    pub fn maintain(&self) {
        let now = Instant::now();
        self.limits
            .lock()
            .unwrap()
            .buckets
            .retain(|_, bucket| now.saturating_duration_since(bucket.updated) < IDLE_TTL);
        let rejected = self.rejected.swap(0, Ordering::Relaxed);
        let throttled = self.throttled.swap(0, Ordering::Relaxed);
        let verification_failed = self.verification_failed.swap(0, Ordering::Relaxed);
        let unavailable = self.unavailable.swap(0, Ordering::Relaxed);
        if rejected + throttled + verification_failed + unavailable > 0 {
            tracing::info!(
                rejected,
                throttled,
                verification_failed,
                unavailable,
                "Admission counters since last maintenance"
            );
        }
    }
}

pub struct PendingAuth {
    admission: Arc<Admission>,
    ip: IpAddr,
}

impl Drop for PendingAuth {
    fn drop(&mut self) {
        let mut limits = self.admission.limits.lock().unwrap();
        let count = limits
            .pending
            .get_mut(&self.ip)
            .expect("pending guard owns a slot");
        *count -= 1;
        if *count == 0 {
            limits.pending.remove(&self.ip);
        }
        limits.pending_total -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budgets_refill_without_resetting_on_reconnection() {
        let admission = Admission::default();
        let ip = "192.0.2.1".parse().unwrap();
        let now = Instant::now();
        for _ in 0..5 {
            assert!(admission.check_at(Budget::Creation(ip), now).is_ok());
        }
        assert_eq!(
            admission.check_at(Budget::Creation(ip), now),
            Err(AdmissionError::Limited(12))
        );
        assert!(
            admission
                .check_at(Budget::Creation(ip), now + Duration::from_secs(12))
                .is_ok()
        );
        for _ in 0..30 {
            assert!(admission.check_at(Budget::Connection(ip), now).is_ok());
        }
        assert_eq!(
            admission.check_at(Budget::Connection(ip), now),
            Err(AdmissionError::Limited(2))
        );
        for _ in 0..20 {
            assert!(admission.check_at(Budget::Room("room".into()), now).is_ok());
        }
        assert_eq!(
            admission.check_at(Budget::Room("room".into()), now),
            Err(AdmissionError::Limited(3))
        );
    }

    #[test]
    fn full_limiter_rejects_new_entries_and_expires_idle_entries() {
        let admission = Admission::default();
        let now = Instant::now();
        for i in 0..MAX_BUCKETS {
            admission
                .check_at(Budget::Room(i.to_string()), now)
                .unwrap();
        }
        assert_eq!(
            admission.check_at(Budget::Room("new".into()), now),
            Err(AdmissionError::Unavailable)
        );
        assert!(
            admission
                .check_at(Budget::Room("new".into()), now + IDLE_TTL)
                .is_ok()
        );
        assert_eq!(admission.limits.lock().unwrap().buckets.len(), 1);
    }

    #[test]
    fn pending_guards_bound_global_and_per_ip_counts_and_release_on_drop() {
        let admission = Arc::new(Admission::default());
        let mut guards = vec![];
        for i in 1..=10 {
            let ip = IpAddr::from([192, 0, 2, i]);
            for _ in 0..10 {
                guards.push(admission.pending(ip).unwrap());
            }
            assert!(matches!(
                admission.pending(ip),
                Err(AdmissionError::Unavailable)
            ));
        }
        assert!(matches!(
            admission.pending(IpAddr::from([192, 0, 2, 11])),
            Err(AdmissionError::Unavailable)
        ));
        guards.clear();
        assert_eq!(admission.limits.lock().unwrap().pending_total, 0);
        assert!(admission.limits.lock().unwrap().pending.is_empty());
    }
}
