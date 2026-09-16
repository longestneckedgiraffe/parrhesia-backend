//! Room admission secrets are independent of the client-side encryption keys.
use argon2::{Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version};
use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
};
use tokio::sync::Semaphore;
use unicode_normalization::UnicodeNormalization;
use zeroize::Zeroizing;

const MEMORY_KIB: u32 = 64 * 1024;
const ITERATIONS: u32 = 3;
const LANES: u32 = 1;
const OUTPUT_BYTES: usize = 32;

static BLOCKLIST: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    include_str!("../data/common-passwords.txt")
        .lines()
        .collect()
});

#[derive(Debug, PartialEq, Eq)]
pub enum PasswordError {
    Invalid,
    Common,
    Busy,
    Internal,
}

pub fn normalize(password: String) -> Result<Zeroizing<String>, PasswordError> {
    let password = Zeroizing::new(password);
    if password.len() > 1024 {
        return Err(PasswordError::Invalid);
    }
    let normalized = Zeroizing::new(password.nfc().collect::<String>());
    if normalized.len() > 1024 || !(15..=256).contains(&normalized.chars().count()) {
        return Err(PasswordError::Invalid);
    }
    Ok(normalized)
}

fn argon2() -> Argon2<'static> {
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(MEMORY_KIB, ITERATIONS, LANES, Some(OUTPUT_BYTES))
            .expect("fixed Argon2 parameters"),
    )
}

#[derive(Clone)]
pub struct PasswordService {
    slots: Arc<Semaphore>,
}

impl Default for PasswordService {
    fn default() -> Self {
        // Initialize at startup, never on a request's async executor thread.
        LazyLock::force(&BLOCKLIST);
        Self {
            slots: Arc::new(Semaphore::new(2)),
        }
    }
}

impl PasswordService {
    async fn compute<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> Result<T, PasswordError> + Send + 'static,
    ) -> Result<T, PasswordError> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| PasswordError::Busy)?;
        tokio::task::spawn_blocking(move || {
            // The blocking job owns its permit, even if the awaiting task is cancelled.
            let _permit = permit;
            job()
        })
        .await
        .map_err(|_| PasswordError::Internal)?
    }

    pub async fn hash(&self, password: String) -> Result<String, PasswordError> {
        let password = normalize(password)?;
        if BLOCKLIST.contains(password.as_str()) {
            return Err(PasswordError::Common);
        }
        self.compute(move || {
            // PasswordHasher generates a 16-byte salt using the OS CSPRNG.
            argon2()
                .hash_password(password.as_bytes())
                .map(|hash| hash.to_string())
                .map_err(|_| PasswordError::Internal)
        })
        .await
    }

    pub async fn verify(&self, password: String, hash: String) -> Result<bool, PasswordError> {
        let password = normalize(password)?;
        self.compute(move || {
            let hash = PasswordHash::new(&hash).map_err(|_| PasswordError::Internal)?;
            // Never accept corrupt, downgraded, or unbounded stored parameters.
            let params = Params::try_from(&hash).map_err(|_| PasswordError::Internal)?;
            if hash.algorithm.as_str() != "argon2id"
                || hash.version != Some(19)
                || params.m_cost() != MEMORY_KIB
                || params.t_cost() != ITERATIONS
                || params.p_cost() != LANES
                || hash.salt.as_ref().map(|s| s.len()) != Some(16)
                || hash.hash.as_ref().map(|h| h.len()) != Some(OUTPUT_BYTES)
            {
                return Err(PasswordError::Internal);
            }
            match argon2().verify_password(password.as_bytes(), &hash) {
                Ok(()) => Ok(true),
                Err(argon2::password_hash::Error::PasswordInvalid) => Ok(false),
                Err(_) => Err(PasswordError::Internal),
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_and_limits_preserve_password_semantics() {
        assert_eq!(
            &*normalize(" café and telescope ".into()).unwrap(),
            " café and telescope "
        );
        assert_eq!(
            normalize("cafe\u{301} and telescope".into()).unwrap(),
            normalize("café and telescope".into()).unwrap()
        );
        for password in ["".into(), "a".repeat(14), "a".repeat(257), "🦒".repeat(257)] {
            assert_eq!(normalize(password), Err(PasswordError::Invalid));
        }
        assert!(normalize("🦒".repeat(256)).is_ok());
        assert!(normalize("a".repeat(15)).is_ok());
    }

    #[tokio::test]
    async fn salted_hashes_verify_and_corrupt_or_unbounded_hashes_fail_closed() {
        let service = PasswordService::default();
        let password = "café telescopes drift slowly";
        let hash = service.hash(password.into()).await.unwrap();
        let other = service.hash(password.into()).await.unwrap();
        assert_ne!(hash, other);
        assert!(hash.starts_with("$argon2id$v=19$m=65536,t=3,p=1$"));
        assert!(
            service
                .verify("cafe\u{301} telescopes drift slowly".into(), hash.clone())
                .await
                .unwrap()
        );
        assert!(
            !service
                .verify("different correct length secret".into(), hash.clone())
                .await
                .unwrap()
        );
        for corrupt in [
            "".into(),
            "broken".into(),
            hash.replace("m=65536", "m=4294967295"),
            hash.replace("argon2id", "argon2i"),
        ] {
            assert_eq!(
                service.verify(password.into(), corrupt).await,
                Err(PasswordError::Internal)
            );
        }
    }

    #[tokio::test]
    async fn common_passwords_are_rejected_before_hashing() {
        let service = PasswordService::default();
        let common = BLOCKLIST.iter().find(|s| s.chars().count() >= 15).unwrap();
        assert_eq!(
            service.hash((*common).into()).await,
            Err(PasswordError::Common)
        );
    }

    #[tokio::test]
    async fn cancellation_keeps_blocking_permit_until_job_finishes() {
        let service = PasswordService::default();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let worker = service.clone();
        let task = tokio::spawn(async move {
            worker
                .compute(move || {
                    started_tx.send(()).unwrap();
                    finish_rx.recv().unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(service.slots.available_permits(), 1);
        let other_permit = service.slots.clone().try_acquire_owned().unwrap();
        assert_eq!(service.compute(|| Ok(())).await, Err(PasswordError::Busy));
        finish_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if service.slots.available_permits() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(other_permit);
        assert_eq!(service.slots.available_permits(), 2);
    }

    #[tokio::test]
    #[ignore = "run with --release --ignored --nocapture for deployment timing"]
    async fn benchmark_two_concurrent_verifications() {
        let service = PasswordService::default();
        let hash = service
            .hash("café telescopes drift slowly".into())
            .await
            .unwrap();
        let start = std::time::Instant::now();
        let (a, b) = tokio::join!(
            service.verify("café telescopes drift slowly".into(), hash.clone()),
            service.verify("café telescopes drift slowly".into(), hash)
        );
        assert!(a.unwrap() && b.unwrap());
        println!(
            "Two concurrent 64 MiB Argon2id verifications: {:?}",
            start.elapsed()
        );
    }
}
