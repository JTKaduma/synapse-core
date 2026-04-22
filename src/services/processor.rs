use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::db::models::Transaction;
use crate::services::lock_manager::LeaderElection;
use crate::stellar::HorizonClient;

const POLL_INTERVAL_SECS: u64 = 5;
/// How often to attempt leader election renewal and heartbeat (seconds).
const LEADER_HEARTBEAT_SECS: u64 = 10;

/// Runs the background processor loop. Processes pending transactions asynchronously
/// without blocking the HTTP server. Uses `SELECT ... FOR UPDATE SKIP LOCKED`
/// for safe concurrent processing with multiple workers.
pub async fn run_processor(pool: PgPool, horizon_client: HorizonClient) {
    info!("Async transaction processor started");

    loop {
        if let Err(e) = process_batch(&pool, &horizon_client).await {
            error!("Processor batch error: {}", e);
        }

        sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
    }
}

pub async fn process_batch(pool: &PgPool, _horizon_client: &HorizonClient) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    // Fetch pending transactions with row locking. SKIP LOCKED ensures we don't
    // block on rows another worker is processing.
    let pending: Vec<Transaction> = sqlx::query_as::<_, Transaction>(
        r#"
        SELECT id, stellar_account, amount, asset_code, status, created_at, updated_at,
               anchor_transaction_id, callback_type, callback_status, settlement_id,
               memo, memo_type, metadata, priority
        FROM transactions
        WHERE status = 'pending'
        ORDER BY priority DESC, created_at ASC
        LIMIT 10
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .fetch_all(&mut *tx)
    .await?;

    if pending.is_empty() {
        tx.commit().await?;
        return Ok(());
    }

    debug!("Processing {} pending transaction(s)", pending.len());

    // Collect unique asset codes for cache invalidation
    let mut asset_codes = std::collections::HashSet::new();
    for transaction in &pending {
        asset_codes.insert(transaction.asset_code.clone());
    }

    // Process transactions here
    for _transaction in pending {
        // TODO: Implement transaction processing logic
    }

    tx.commit().await?;

    // Invalidate cache for all affected assets
    for asset_code in asset_codes {
        crate::db::queries::invalidate_caches_for_asset(&asset_code).await;
    }

    Ok(())
}

/// Runs the leader election + heartbeat loop.
///
/// - All instances call this; only the elected leader returns `true` from
///   `try_acquire_leadership`.
/// - The leader runs partition maintenance, settlement jobs, and webhook dispatch.
/// - All instances run `process_batch` (safe via SKIP LOCKED).
pub async fn run_processor_with_leader_election(
    pool: PgPool,
    horizon_client: HorizonClient,
    redis_url: &str,
) {
    let election = match LeaderElection::new(redis_url) {
        Ok(e) => e,
        Err(e) => {
            warn!("Failed to create LeaderElection (Redis unavailable?): {e}. Running without leader guard.");
            run_processor(pool, horizon_client).await;
            return;
        }
    };

    info!(
        instance_id = election.instance_id(),
        "Processor started with leader election"
    );

    let mut heartbeat_tick =
        tokio::time::interval(Duration::from_secs(LEADER_HEARTBEAT_SECS));
    let mut process_tick = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));

    loop {
        tokio::select! {
            _ = heartbeat_tick.tick() => {
                // Publish heartbeat regardless of leader status
                if let Err(e) = election.publish_heartbeat().await {
                    warn!("Heartbeat publish failed: {e}");
                }

                match election.try_acquire_leadership().await {
                    Ok(true) => debug!(instance_id = election.instance_id(), "This instance is leader"),
                    Ok(false) => debug!(instance_id = election.instance_id(), "This instance is follower"),
                    Err(e) => warn!("Leader election error: {e}"),
                }
            }
            _ = process_tick.tick() => {
                // All instances process transactions (SKIP LOCKED handles concurrency)
                if let Err(e) = process_batch(&pool, &horizon_client).await {
                    error!("Processor batch error: {e}");
                }
            }
        }
    }
}
