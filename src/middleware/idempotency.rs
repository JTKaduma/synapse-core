use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use failsafe::futures::CircuitBreaker as FuturesCircuitBreaker;
use failsafe::{backoff, failure_policy, Config, Error as FailsafeError, StateMachine};
use redis::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ── Circuit breaker type alias ────────────────────────────────────────────────

type RedisCBInner =
    StateMachine<failure_policy::ConsecutiveFailures<backoff::EqualJittered>, ()>;

/// Shared Redis circuit breaker (cheaply cloneable).
#[derive(Clone)]
pub struct RedisCircuitBreaker {
    inner: RedisCBInner,
}

impl RedisCircuitBreaker {
    pub fn new(failure_threshold: u32, reset_timeout_secs: u64) -> Self {
        let backoff = backoff::equal_jittered(
            Duration::from_secs(reset_timeout_secs),
            Duration::from_secs(reset_timeout_secs * 2),
        );
        let policy = failure_policy::consecutive_failures(failure_threshold, backoff);
        Self {
            inner: Config::new().failure_policy(policy).build(),
        }
    }

    pub fn from_env() -> Self {
        let threshold = std::env::var("REDIS_CB_FAILURE_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5u32);
        let timeout = std::env::var("REDIS_CB_RESET_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30u64);
        Self::new(threshold, timeout)
    }

    /// Returns `"open"` or `"closed"`.
    pub fn state(&self) -> String {
        if self.inner.is_call_permitted() {
            "closed".to_string()
        } else {
            "open".to_string()
        }
    }

    /// Execute `f` through the circuit breaker.
    pub async fn call<F, Fut, T>(&self, f: F) -> Result<T, RedisError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, redis::RedisError>>,
    {
        match self.inner.call(f()).await {
            Ok(v) => Ok(v),
            Err(FailsafeError::Rejected) => Err(RedisError::CircuitOpen),
            Err(FailsafeError::Inner(e)) => Err(RedisError::Redis(e)),
        }
    }
}

#[derive(Debug)]
pub enum RedisError {
    CircuitOpen,
    Redis(redis::RedisError),
}

impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisError::CircuitOpen => write!(f, "Redis circuit breaker is open"),
            RedisError::Redis(e) => write!(f, "Redis error: {e}"),
        }
    }
}

impl From<redis::RedisError> for RedisError {
    fn from(e: redis::RedisError) -> Self {
        RedisError::Redis(e)
    }
}

// ── IdempotencyService ────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct IdempotencyService {
    client: Client,
    cb: RedisCircuitBreaker,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CachedResponse {
    pub status: u16,
    pub body: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IdempotencyKey {
    pub key: String,
    pub ttl_seconds: u64,
}

#[derive(Debug)]
pub enum IdempotencyStatus {
    New,
    Processing,
    Completed(CachedResponse),
}

/// Value stored in the lock key: JSON with instance_id and locked_at (unix timestamp).
#[derive(Debug, Serialize, Deserialize)]
struct LockValue {
    instance_id: String,
    locked_at: u64,
}

fn cache_key(tenant_id: &str, key: &str) -> String {
    format!("idempotency:{}:{}", tenant_id, key)
}

fn lock_key(tenant_id: &str, key: &str) -> String {
    format!("idempotency:lock:{}:{}", tenant_id, key)
}

fn lock_value() -> String {
    let instance_id =
        std::env::var("INSTANCE_ID").unwrap_or_else(|_| std::process::id().to_string());
    let locked_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    serde_json::to_string(&LockValue {
        instance_id,
        locked_at,
    })
    .unwrap_or_else(|_| "processing".to_string())
}

impl IdempotencyService {
    pub fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = Client::open(redis_url)?;
        Ok(Self {
            client,
            cb: RedisCircuitBreaker::from_env(),
        })
    }

    pub async fn check_idempotency(
        &self,
        tenant_id: &str,
        key: &str,
    ) -> Result<IdempotencyStatus, RedisError> {
        let ck = cache_key(tenant_id, key);
        let lk = lock_key(tenant_id, key);
        let client = self.client.clone();

        self.cb
            .call(|| async move {
                let mut conn = client.get_multiplexed_async_connection().await?;

                let cached: Option<String> =
                    redis::cmd("GET").arg(&ck).query_async(&mut conn).await?;

                if let Some(data) = cached {
                    let response: CachedResponse =
                        serde_json::from_str(&data).map_err(|e| {
                            redis::RedisError::from((
                                redis::ErrorKind::TypeError,
                                "deserialization error",
                                e.to_string(),
                            ))
                        })?;
                    return Ok(IdempotencyStatus::Completed(response));
                }

                let acquired: bool = redis::cmd("SET")
                    .arg(&lk)
                    .arg(lock_value())
                    .arg("NX")
                    .arg("EX")
                    .arg(300u64)
                    .query_async(&mut conn)
                    .await?;

                if acquired {
                    Ok(IdempotencyStatus::New)
                } else {
                    Ok(IdempotencyStatus::Processing)
                }
            })
            .await
    }

    pub async fn store_response(
        &self,
        tenant_id: &str,
        key: &str,
        status: u16,
        body: String,
    ) -> Result<(), RedisError> {
        let ck = cache_key(tenant_id, key);
        let lk = lock_key(tenant_id, key);
        let client = self.client.clone();

        self.cb
            .call(|| async move {
                let cached = CachedResponse { status, body };
                let data = serde_json::to_string(&cached).map_err(|e| {
                    redis::RedisError::from((
                        redis::ErrorKind::TypeError,
                        "serialization error",
                        e.to_string(),
                    ))
                })?;

                let mut conn = client.get_multiplexed_async_connection().await?;

                redis::cmd("SETEX")
                    .arg(&ck)
                    .arg(86400u64)
                    .arg(&data)
                    .query_async::<_, ()>(&mut conn)
                    .await?;

                redis::cmd("DEL")
                    .arg(&lk)
                    .query_async::<_, ()>(&mut conn)
                    .await?;

                Ok(())
            })
            .await
    }

    pub async fn release_lock(&self, tenant_id: &str, key: &str) -> Result<(), RedisError> {
        let lk = lock_key(tenant_id, key);
        let client = self.client.clone();
        self.cb
            .call(|| async move {
                let mut conn = client.get_multiplexed_async_connection().await?;
                redis::cmd("DEL")
                    .arg(&lk)
                    .query_async::<_, ()>(&mut conn)
                    .await?;
                Ok(())
            })
            .await
    }

    pub async fn check_and_set(
        &self,
        key: &str,
        value: &str,
        ttl: Duration,
    ) -> Result<bool, RedisError> {
        let key = key.to_string();
        let value = value.to_string();
        let client = self.client.clone();
        self.cb
            .call(|| async move {
                let mut conn = client.get_multiplexed_async_connection().await?;
                let acquired: bool = redis::cmd("SET")
                    .arg(&key)
                    .arg(&value)
                    .arg("NX")
                    .arg("EX")
                    .arg(ttl.as_secs())
                    .query_async(&mut conn)
                    .await?;
                Ok(acquired)
            })
            .await
    }

    /// Returns the circuit breaker state: `"open"` or `"closed"`.
    pub fn circuit_state(&self) -> String {
        self.cb.state()
    }

    /// Background task: scan for stale locks (older than 2 minutes with no cached response)
    /// and delete them so the next request can reprocess.
    pub async fn recover_stale_locks(&self) -> Result<(), RedisError> {
        let client = self.client.clone();
        self.cb
            .call(|| async move {
                let mut conn = client.get_multiplexed_async_connection().await?;

                let lock_keys: Vec<String> = redis::cmd("KEYS")
                    .arg("idempotency:lock:*")
                    .query_async(&mut conn)
                    .await?;

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                for lk in lock_keys {
                    let raw: Option<String> =
                        redis::cmd("GET").arg(&lk).query_async(&mut conn).await?;

                    let Some(raw) = raw else { continue };

                    let locked_at = serde_json::from_str::<LockValue>(&raw)
                        .map(|v| v.locked_at)
                        .unwrap_or(0);

                    if locked_at == 0 || now.saturating_sub(locked_at) < 120 {
                        continue;
                    }

                    // Lock key: idempotency:lock:{tenant_id}:{key}
                    // Cache key: idempotency:{tenant_id}:{key}
                    let ck = lk.replacen("idempotency:lock:", "idempotency:", 1);
                    let cached: Option<String> =
                        redis::cmd("GET").arg(&ck).query_async(&mut conn).await?;

                    if cached.is_none() {
                        tracing::warn!(lock_key = %lk, "Recovering stale idempotency lock");
                        redis::cmd("DEL")
                            .arg(&lk)
                            .query_async::<_, ()>(&mut conn)
                            .await?;
                    }
                }

                Ok(())
            })
            .await
    }
}

/// Extract tenant ID from `X-Tenant-Id` header; falls back to `"default"`.
fn extract_tenant_id(request: &Request<Body>) -> String {
    request
        .headers()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or("default")
        .to_string()
}

/// Middleware to handle idempotency for webhook requests
pub async fn idempotency_middleware(
    State(service): State<IdempotencyService>,
    request: Request<Body>,
    next: Next<Body>,
) -> Response {
    let idempotency_key = match request.headers().get("x-idempotency-key") {
        Some(key) => match key.to_str() {
            Ok(k) => k.to_string(),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "Invalid idempotency key format"
                    })),
                )
                    .into_response();
            }
        },
        None => {
            return next.run(request).await;
        }
    };

    let tenant_id = extract_tenant_id(&request);

    match service
        .check_idempotency(&tenant_id, &idempotency_key)
        .await
    {
        Ok(IdempotencyStatus::New) => {
            let response: Response = next.run(request).await;

            if response.status().is_success() {
                let status = response.status().as_u16();
                let body = serde_json::json!({"status": "success"}).to_string();

                if let Err(e) = service
                    .store_response(&tenant_id, &idempotency_key, status, body)
                    .await
                {
                    tracing::error!("Failed to store idempotency response: {}", e);
                }
            } else if let Err(e) = service.release_lock(&tenant_id, &idempotency_key).await {
                tracing::error!("Failed to release idempotency lock: {}", e);
            }

            response
        }
        Ok(IdempotencyStatus::Processing) => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": "Request is currently being processed",
                "retry_after": 5
            })),
        )
            .into_response(),
        Ok(IdempotencyStatus::Completed(cached)) => {
            let status = StatusCode::from_u16(cached.status).unwrap_or(StatusCode::OK);
            (
                status,
                Json(serde_json::json!({
                    "cached": true,
                    "message": "Request already processed"
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Idempotency check failed: {}", e);
            next.run(request).await
        }
    }
}
