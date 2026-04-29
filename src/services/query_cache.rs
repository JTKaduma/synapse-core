use redis::{AsyncCommands, Client};
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;
use std::time::Duration;

/// Fraction of TTL remaining that triggers an early refresh (10% before expiry).
const EARLY_REFRESH_FRACTION: f64 = 0.10;

/// How long the refresh lock is held (prevents multiple refreshers).
const LOCK_TTL_SECS: i64 = 30;

#[derive(Clone)]
pub struct QueryCache {
    client: Client,
}

impl QueryCache {
    pub fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = Client::open(redis_url)?;
        Ok(Self { client })
    }

    /// Get a cached value or compute it, with stampede prevention.
    ///
    /// Strategy:
    /// 1. Fresh value in cache → return it.
    /// 2. Value in early-refresh window (last 10% of TTL) → serve stale value,
    ///    spawn background refresh (only one worker acquires the lock).
    /// 3. Cache miss → acquire lock, compute once; other concurrent callers
    ///    spin-wait then re-check the cache.
    pub async fn get_or_compute<T, F, Fut>(
        &self,
        key: &str,
        ttl: Duration,
        loader: F,
    ) -> Result<T, anyhow::Error>
    where
        T: Serialize + DeserializeOwned + Clone + Send + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, anyhow::Error>> + Send + 'static,
    {
        let mut conn = self.client.get_async_connection().await?;
        let lock_key = format!("{}:refresh_lock", key);

        // --- 1. Check cache and remaining TTL ---
        let cached_bytes: Option<Vec<u8>> = conn.get(key).await?;
        let remaining_ttl_ms: i64 = conn.pttl(key).await?;

        let ttl_ms = ttl.as_millis() as i64;
        let early_threshold_ms = (ttl_ms as f64 * EARLY_REFRESH_FRACTION) as i64;

        if let Some(bytes) = cached_bytes {
            let value: T = serde_json::from_slice(&bytes)?;

            // --- 2. Early-refresh window: serve stale, refresh in background ---
            if remaining_ttl_ms > 0 && remaining_ttl_ms <= early_threshold_ms {
                let acquired: bool = conn
                    .set_nx::<_, _, bool>(&lock_key, "1")
                    .await
                    .unwrap_or(false);

                if acquired {
                    let _: Result<bool, _> = conn.expire(&lock_key, LOCK_TTL_SECS).await;

                    let client = self.client.clone();
                    let key_owned = key.to_owned();
                    let lock_key_owned = lock_key.clone();
                    tokio::spawn(async move {
                        match loader().await {
                            Ok(fresh) => {
                                if let Ok(serialized) = serde_json::to_vec(&fresh) {
                                    if let Ok(mut bg_conn) = client.get_async_connection().await {
                                        let _: Result<(), _> = bg_conn
                                            .set_ex(&key_owned, serialized, ttl.as_secs())
                                            .await;
                                        tracing::debug!(
                                            key = %key_owned,
                                            "cache refreshed (early expiry)"
                                        );
                                        increment_stampede_prevented(&mut bg_conn, &key_owned)
                                            .await;
                                        let _: Result<(), _> = bg_conn.del(&lock_key_owned).await;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    key = %key_owned,
                                    error = %e,
                                    "background cache refresh failed"
                                );
                                if let Ok(mut bg_conn) = client.get_async_connection().await {
                                    let _: Result<(), _> = bg_conn.del(&lock_key_owned).await;
                                }
                            }
                        }
                    });
                }
                // Serve stale value regardless of whether we got the lock.
            }

            return Ok(value);
        }

        // --- 3. Cache miss: acquire lock, compute, store ---
        let acquired: bool = conn
            .set_nx::<_, _, bool>(&lock_key, "1")
            .await
            .unwrap_or(false);

        if acquired {
            let _: Result<bool, _> = conn.expire(&lock_key, LOCK_TTL_SECS).await;

            let result = loader().await;
            let _: Result<(), _> = conn.del(&lock_key).await;

            match result {
                Ok(value) => {
                    let serialized = serde_json::to_vec(&value)?;
                    let _: Result<(), _> = conn.set_ex(key, serialized, ttl.as_secs()).await;
                    Ok(value)
                }
                Err(e) => Err(e),
            }
        } else {
            // Another worker holds the lock; wait briefly and retry from cache.
            for _ in 0..10u8 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let cached: Option<Vec<u8>> = conn.get(key).await.unwrap_or(None);
                if let Some(bytes) = cached {
                    let value: T = serde_json::from_slice(&bytes)?;
                    return Ok(value);
                }
            }
            // Fallback: compute directly if cache still empty after waiting.
            loader().await
        }
    }
}

async fn increment_stampede_prevented(conn: &mut redis::aio::Connection, key: &str) {
    let metric_key = format!("metrics:cache_stampede_prevented:{}", key);
    let _: Result<i64, _> = conn.incr(&metric_key, 1i64).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn make_cache() -> Option<QueryCache> {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        QueryCache::new(&url).ok()
    }

    /// 100 concurrent requests for an expired key must result in exactly 1 DB query.
    #[tokio::test]
    async fn test_100_concurrent_requests_single_db_query() {
        let Some(cache) = make_cache() else {
            eprintln!("Skipping: no Redis available");
            return;
        };

        let key = format!("test:stampede:{}", uuid::Uuid::new_v4());
        let db_calls = Arc::new(AtomicUsize::new(0));
        let ttl = Duration::from_secs(60);

        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            let cache = cache.clone();
            let key = key.clone();
            let db_calls = db_calls.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_compute(&key, ttl, move || {
                        let db_calls = db_calls.clone();
                        async move {
                            db_calls.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Ok::<String, anyhow::Error>("result".to_string())
                        }
                    })
                    .await
            }));
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        let calls = db_calls.load(Ordering::SeqCst);
        assert_eq!(calls, 1, "expected 1 DB call, got {calls}");
    }

    /// Stale value is served immediately during early-refresh window.
    #[tokio::test]
    async fn test_stale_value_served_during_early_refresh() {
        let Some(cache) = make_cache() else {
            eprintln!("Skipping: no Redis available");
            return;
        };

        let key = format!("test:early_refresh:{}", uuid::Uuid::new_v4());
        let ttl = Duration::from_secs(10);

        // Seed with a value that has only 1 s left (within 10% of 10 s = 1 s threshold).
        {
            let mut conn = cache.client.get_async_connection().await.unwrap();
            let bytes = serde_json::to_vec("stale").unwrap();
            let _: () = conn.set_ex(&key, bytes, 1u64).await.unwrap();
        }

        // Should return stale immediately.
        let val: String = cache
            .get_or_compute(&key, ttl, || async { Ok("fresh".to_string()) })
            .await
            .unwrap();
        assert_eq!(val, "stale");

        // Allow background refresh to complete.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Now should return the refreshed value.
        let val2: String = cache
            .get_or_compute(&key, ttl, || async { Ok("fresh".to_string()) })
            .await
            .unwrap();
        assert_eq!(val2, "fresh");
    }
}
