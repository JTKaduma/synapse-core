use crate::services::webhook_dispatcher::WebhookDispatcher;
use sqlx::PgPool;

#[derive(Clone)]
pub struct TransactionProcessor {
    pool: PgPool,
    webhook_dispatcher: Option<WebhookDispatcher>,
}

impl TransactionProcessor {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            webhook_dispatcher: None,
        }
    }

    /// Attach a WebhookDispatcher so state transitions trigger outgoing webhooks.
    pub fn with_webhook_dispatcher(mut self, dispatcher: WebhookDispatcher) -> Self {
        self.webhook_dispatcher = Some(dispatcher);
        self
    }

    pub async fn process_transaction(&self, tx_id: uuid::Uuid) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE transactions SET status = 'completed', updated_at = NOW() WHERE id = $1",
        )
        .bind(tx_id)
        .execute(&self.pool)
        .await?;

        // Notify external systems that the transaction completed.
        if let Some(dispatcher) = &self.webhook_dispatcher {
            let data = serde_json::json!({ "transaction_id": tx_id });
            if let Err(e) = dispatcher
                .enqueue(tx_id, "transaction.completed", data)
                .await
            {
                tracing::error!(
                    transaction_id = %tx_id,
                    "Failed to enqueue webhook for transaction.completed: {e}"
                );
            }
        }

        Ok(())
    }

    pub async fn fail_transaction(&self, tx_id: uuid::Uuid, reason: &str) -> anyhow::Result<()> {
        sqlx::query("UPDATE transactions SET status = 'failed', updated_at = NOW() WHERE id = $1")
            .bind(tx_id)
            .execute(&self.pool)
            .await?;

        // Notify external systems that the transaction failed.
        if let Some(dispatcher) = &self.webhook_dispatcher {
            let data = serde_json::json!({
                "transaction_id": tx_id,
                "reason": reason,
            });
            if let Err(e) = dispatcher.enqueue(tx_id, "transaction.failed", data).await {
                tracing::error!(
                    transaction_id = %tx_id,
                    "Failed to enqueue webhook for transaction.failed: {e}"
                );
            }
        }

        Ok(())
    }

    pub async fn requeue_dlq(&self, dlq_id: uuid::Uuid) -> anyhow::Result<()> {
        let tx_id: uuid::Uuid =
            sqlx::query_scalar("SELECT transaction_id FROM transaction_dlq WHERE id = $1")
                .bind(dlq_id)
                .fetch_one(&self.pool)
                .await?;

        sqlx::query("UPDATE transactions SET status = 'pending', updated_at = NOW() WHERE id = $1")
            .bind(tx_id)
            .execute(&self.pool)
            .await?;

        sqlx::query("DELETE FROM transaction_dlq WHERE id = $1")
            .bind(dlq_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
