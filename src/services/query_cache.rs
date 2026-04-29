use redis::{AsyncCommands, Client};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;
use std::future::Future;
use std::time::Duration;

/// Fraction of TTL remaining that triggers an early refresh (10% before expiry).
const EARLY_REFRESH_FRACTION: f64 = 0.10;

/// How long the refresh lock is held (prevents multiple refreshers).
const LOCK_TTL_SECS: i64 = 30;

/// TTL for warmed cache entries (5 minutes).
const WARM_TTL: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub struct QueryCache {
    client: Client,
}

/// Build a namespaced cache key.
///
/// - With tenant: `tenant:{tenant_id}:{base_key}`
/// - Without tenant: `{base_key}` (backward-compatible)
pub fn make_key(base_key: &str, tenant_id: Option<&str>) -> String {
    match tenant_id {
        Some(id) => format!("tenant:{}:{}", id, base_key),
        None => base_key.to_owned(),
    }
}

impl QueryCache {
    pub fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = Client::open(redis_url)?;
        Ok(Self { client })
    }

    /// Get a cached value or compute it, with stampede prevention.
    ///
    /// Pass `tenant_id = Some("t1")` for tenant-scoped caching, or `None` for
    /// global (single-tenant / backward-compatible) mode.
    pub async fn get_or_compute<T, F, Fut>(
        &self,
        base_key: &str,
        tenant_id: Option<&str>,
        ttl: Duration,
        loader: F,
    ) -> Result<T, anyhow::Error>
    where
        T: Serialize + DeserializeOwned + Clone + Send + 'static,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, anyhow::Error>> + Send + 'static,
    {
        let key = make_key(base_key, tenant_id);
        self.get_or_compute_raw(&key, ttl, loader).await
    }

    /// Invalidate a cache entry for both the tenant-specific and global keys.
    pub async fn invalidate(
        &self,
        base_key: &str,
        tenant_id: Option<&str>,
    ) -> Result<(), redis::RedisError> {
        let mut conn = self.client.get_async_connection().await?;
        let global_key = make_key(base_key, None);
        let _: Result<(), _> = conn.del(&global_key).await;
        if let Some(id) = tenant_id {
            let tenant_key = make_key(base_key, Some(id));
            let _: Result<(), _> = conn.del(&tenant_key).await;
        }
        Ok(())
    }

    /// Pre-warm the cache after a partition rotation.
    ///
    /// Populates:
    /// - `query:status_counts`  — count of transactions per status
    /// - `query:daily_totals`   — transaction counts for the last 7 days
    /// - `query:asset_stats`    — distinct asset codes with transaction counts
    pub async fn warm_cache(
        &self,
        pool: &PgPool,
        tenant_id: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        // status counts
        self.get_or_compute("query:status_counts", tenant_id, WARM_TTL, {
            let pool = pool.clone();
            move || {
                let pool = pool.clone();
                async move {
                    let rows = sqlx::query_as::<_, (String, i64)>(
                        "SELECT status, COUNT(*) FROM transactions GROUP BY status",
                    )
                    .fetch_all(&pool)
                    .await?;
                    Ok::<Vec<(String, i64)>, anyhow::Error>(rows)
                }
            }
        })
        .await?;

        // daily totals — last 7 days
        self.get_or_compute("query:daily_totals", tenant_id, WARM_TTL, {
            let pool = pool.clone();
            move || {
                let pool = pool.clone();
                async move {
                    let rows = sqlx::query_as::<_, (chrono::NaiveDate, i64)>(
                        "SELECT created_at::date AS day, COUNT(*) \
                             FROM transactions \
                             WHERE created_at >= NOW() - INTERVAL '7 days' \
                             GROUP BY day ORDER BY day",
                    )
                    .fetch_all(&pool)
                    .await?;
                    Ok::<Vec<(chrono::NaiveDate, i64)>, anyhow::Error>(rows)
                }
            }
        })
        .await?;

        // asset stats
        self.get_or_compute("query:asset_stats", tenant_id, WARM_TTL, {
            let pool = pool.clone();
            move || {
                let pool = pool.clone();
                async move {
                    let rows = sqlx::query_as::<_, (String, i64)>(
                        "SELECT asset_code, COUNT(*) FROM transactions GROUP BY asset_code",
                    )
                    .fetch_all(&pool)
                    .await?;
                    Ok::<Vec<(String, i64)>, anyhow::Error>(rows)
                }
            }
        })
        .await?;

        tracing::info!(tenant_id = ?tenant_id, "cache warmed after partition rotation");
        Ok(())
    }

    /// Internal: stampede-safe fetch/compute on a fully-resolved key.
    async fn get_or_compute_raw<T, F, Fut>(
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

        let cached_bytes: Option<Vec<u8>> = conn.get(key).await?;
        let remaining_ttl_ms: i64 = conn.pttl(key).await?;

        let ttl_ms = ttl.as_millis() as i64;
        let early_threshold_ms = (ttl_ms as f64 * EARLY_REFRESH_FRACTION) as i64;

        if let Some(bytes) = cached_bytes {
            let value: T = serde_json::from_slice(&bytes)?;

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
            }

            return Ok(value);
        }

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
            for _ in 0..10u8 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let cached: Option<Vec<u8>> = conn.get(key).await.unwrap_or(None);
                if let Some(bytes) = cached {
                    let value: T = serde_json::from_slice(&bytes)?;
                    return Ok(value);
                }
            }
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

    // ── key generation ────────────────────────────────────────────────────────

    #[test]
    fn test_make_key_no_tenant() {
        assert_eq!(make_key("query:status_counts", None), "query:status_counts");
    }

    #[test]
    fn test_make_key_with_tenant() {
        assert_eq!(
            make_key("query:status_counts", Some("acme")),
            "tenant:acme:query:status_counts"
        );
    }

    // ── multi-tenant isolation ────────────────────────────────────────────────

    /// Two tenants must receive independent cached results.
    #[tokio::test]
    async fn test_two_tenants_get_independent_results() {
        let Some(cache) = make_cache() else {
            eprintln!("Skipping: no Redis available");
            return;
        };

        let base = format!("test:mt:{}", uuid::Uuid::new_v4());
        let ttl = Duration::from_secs(60);

        let v1: String = cache
            .get_or_compute(&base, Some("tenant_a"), ttl, || async {
                Ok("value_a".to_string())
            })
            .await
            .unwrap();

        let v2: String = cache
            .get_or_compute(&base, Some("tenant_b"), ttl, || async {
                Ok("value_b".to_string())
            })
            .await
            .unwrap();

        assert_eq!(v1, "value_a");
        assert_eq!(v2, "value_b");
        assert_ne!(v1, v2);
    }

    /// Single-tenant (no tenant_id) mode must work without change.
    #[tokio::test]
    async fn test_single_tenant_mode_no_prefix() {
        let Some(cache) = make_cache() else {
            eprintln!("Skipping: no Redis available");
            return;
        };

        let base = format!("test:st:{}", uuid::Uuid::new_v4());
        let ttl = Duration::from_secs(60);

        let val: String = cache
            .get_or_compute(&base, None, ttl, || async { Ok("global".to_string()) })
            .await
            .unwrap();

        assert_eq!(val, "global");

        // Verify the key stored in Redis has no tenant prefix.
        let mut conn = cache.client.get_async_connection().await.unwrap();
        let raw: Option<Vec<u8>> = conn.get(&base).await.unwrap();
        assert!(raw.is_some(), "key should be stored without prefix");
    }

    /// Invalidation clears both tenant-specific and global keys.
    #[tokio::test]
    async fn test_invalidate_clears_both_keys() {
        let Some(cache) = make_cache() else {
            eprintln!("Skipping: no Redis available");
            return;
        };

        let base = format!("test:inv:{}", uuid::Uuid::new_v4());
        let ttl = Duration::from_secs(60);

        // Populate both global and tenant-scoped entries.
        cache
            .get_or_compute(&base, None, ttl, || async { Ok("global".to_string()) })
            .await
            .unwrap();
        cache
            .get_or_compute(&base, Some("t1"), ttl, || async {
                Ok("t1_val".to_string())
            })
            .await
            .unwrap();

        cache.invalidate(&base, Some("t1")).await.unwrap();

        let mut conn = cache.client.get_async_connection().await.unwrap();
        let global: Option<Vec<u8>> = conn.get(&base).await.unwrap();
        let tenant: Option<Vec<u8>> = conn.get(make_key(&base, Some("t1"))).await.unwrap();

        assert!(global.is_none(), "global key should be cleared");
        assert!(tenant.is_none(), "tenant key should be cleared");
    }

    // ── stampede prevention (carried over) ───────────────────────────────────

    #[tokio::test]
    async fn test_100_concurrent_requests_single_db_query() {
        let Some(cache) = make_cache() else {
            eprintln!("Skipping: no Redis available");
            return;
        };

        let base = format!("test:stampede:{}", uuid::Uuid::new_v4());
        let db_calls = Arc::new(AtomicUsize::new(0));
        let ttl = Duration::from_secs(60);

        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            let cache = cache.clone();
            let base = base.clone();
            let db_calls = db_calls.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_compute(&base, None, ttl, move || {
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

    #[tokio::test]
    async fn test_stale_value_served_during_early_refresh() {
        let Some(cache) = make_cache() else {
            eprintln!("Skipping: no Redis available");
            return;
        };

        let base = format!("test:early_refresh:{}", uuid::Uuid::new_v4());
        let ttl = Duration::from_secs(10);

        {
            let mut conn = cache.client.get_async_connection().await.unwrap();
            let bytes = serde_json::to_vec("stale").unwrap();
            let _: () = conn.set_ex(&base, bytes, 1u64).await.unwrap();
        }

        let val: String = cache
            .get_or_compute(&base, None, ttl, || async { Ok("fresh".to_string()) })
            .await
            .unwrap();
        assert_eq!(val, "stale");

        tokio::time::sleep(Duration::from_millis(300)).await;

        let val2: String = cache
            .get_or_compute(&base, None, ttl, || async { Ok("fresh".to_string()) })
            .await
            .unwrap();
        assert_eq!(val2, "fresh");
    }
}
