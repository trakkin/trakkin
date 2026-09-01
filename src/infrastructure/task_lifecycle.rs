use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::application::sync_action_reconciliation::MAXIMUM_AUTOMATIC_SYNC_ACTION_RECONCILIATION_ATTEMPTS;
use crate::application::task_lifecycle::{
    InterruptActiveTasks, InterruptionSummary, TaskLifecycleRepository, TaskRepositoryFuture,
};
use crate::infrastructure::sync_execution::reduce_sync_run;

pub struct PostgresTaskLifecycleRepository {
    pool: PgPool,
}

impl PostgresTaskLifecycleRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn interrupt(
        &self,
        command: InterruptActiveTasks,
    ) -> Result<InterruptionSummary, TaskLifecycleError> {
        let mut transaction = self.pool.begin().await?;
        let task_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT id
FROM tasks
WHERE status IN ('queued', 'running', 'cancelling')
ORDER BY id
FOR UPDATE
",
        )
        .fetch_all(&mut *transaction)
        .await?;
        if task_ids.is_empty() {
            transaction.commit().await?;
            return Ok(InterruptionSummary::default());
        }
        let retriable_reconciliation_task_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT reconciliation_tasks.task_id
FROM sync_action_reconciliation_tasks AS reconciliation_tasks
LEFT JOIN task_attempts ON task_attempts.task_id = reconciliation_tasks.task_id
WHERE reconciliation_tasks.task_id = ANY($1)
GROUP BY reconciliation_tasks.task_id
HAVING COUNT(task_attempts.id) < $2
ORDER BY reconciliation_tasks.task_id
",
        )
        .bind(&task_ids)
        .bind(i64::from(
            MAXIMUM_AUTOMATIC_SYNC_ACTION_RECONCILIATION_ATTEMPTS,
        ))
        .fetch_all(&mut *transaction)
        .await?;

        let read_attempt_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT id
FROM read_attempts
WHERE task_id = ANY($1) AND status = 'staging'
ORDER BY id
FOR UPDATE
",
        )
        .bind(&task_ids)
        .fetch_all(&mut *transaction)
        .await?;
        clear_interrupted_read_staging(&mut transaction, &read_attempt_ids).await?;
        let read_attempts = sqlx::query(
            "UPDATE read_attempts
SET status = 'abandoned', updated_at = $1, completed_at = $1
WHERE id = ANY($2) AND status = 'staging'
",
        )
        .bind(command.interrupted_at)
        .bind(&read_attempt_ids)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let active_sync_action_attempt_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT task_attempt_id
FROM sync_action_attempts
WHERE task_id = ANY($1) AND state IN ('prepared', 'dispatched')
ORDER BY task_attempt_id
FOR UPDATE
",
        )
        .bind(&task_ids)
        .fetch_all(&mut *transaction)
        .await?;
        let dispatched_reconciliations = sqlx::query_as::<_, DispatchedReconciliationRow>(
            "SELECT
    sync_action_attempts.task_attempt_id AS write_task_attempt_id,
    sync_action_attempts.action_id,
    sync_action_attempts.receiver_source_id,
    tasks.workspace_id,
    tasks.connection_id
FROM sync_action_attempts
INNER JOIN tasks ON tasks.id = sync_action_attempts.task_id
WHERE
    sync_action_attempts.task_attempt_id = ANY($1)
    AND sync_action_attempts.state = 'dispatched'
ORDER BY sync_action_attempts.task_attempt_id
",
        )
        .bind(&active_sync_action_attempt_ids)
        .fetch_all(&mut *transaction)
        .await?;
        let sync_actions_confirmed_not_applied = sqlx::query(
            "UPDATE sync_action_attempts
SET
    state = 'settled',
    settlement_kind = 'recovery',
    recovery_result = 'confirmed_not_applied',
    settled_at = $1
WHERE task_attempt_id = ANY($2) AND state = 'prepared'
",
        )
        .bind(command.interrupted_at)
        .bind(&active_sync_action_attempt_ids)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let sync_actions_indeterminate = sqlx::query(
            "UPDATE sync_action_attempts
SET
    state = 'settled',
    settlement_kind = 'recovery',
    recovery_result = 'indeterminate',
    settled_at = $1
WHERE task_attempt_id = ANY($2) AND state = 'dispatched'
",
        )
        .bind(command.interrupted_at)
        .bind(&active_sync_action_attempt_ids)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if sync_actions_confirmed_not_applied + sync_actions_indeterminate
            != active_sync_action_attempt_ids.len() as u64
        {
            return Err(TaskLifecycleError::ActiveSyncActionAttemptSetChanged);
        }
        let mut recovered_sync_run_ids = sqlx::query_scalar::<_, Uuid>(
            "WITH recovered_attempts AS (
    SELECT action_id, recovery_result
    FROM sync_action_attempts
    WHERE task_attempt_id = ANY($3)
)

UPDATE sync_actions
SET
    status = CASE
        WHEN recovered_attempts.recovery_result = 'indeterminate'
            THEN 'indeterminate'
        WHEN tasks.status = 'cancelling' THEN 'cancelled'
        ELSE 'failed'
    END,
    finished_at = $1,
    updated_at = $1
FROM sync_action_tasks
INNER JOIN tasks ON tasks.id = sync_action_tasks.task_id
LEFT JOIN recovered_attempts
    ON recovered_attempts.action_id = sync_action_tasks.action_id
WHERE
    sync_action_tasks.task_id = ANY($2)
    AND sync_actions.id = sync_action_tasks.action_id
    AND sync_actions.status IN ('queued', 'running', 'retryable_failure')
RETURNING sync_actions.run_id
",
        )
        .bind(command.interrupted_at)
        .bind(&task_ids)
        .bind(&active_sync_action_attempt_ids)
        .fetch_all(&mut *transaction)
        .await?;
        recovered_sync_run_ids.sort_unstable();
        recovered_sync_run_ids.dedup();
        for run_id in recovered_sync_run_ids {
            if !reduce_sync_run(&mut transaction, run_id, command.interrupted_at).await? {
                return Err(TaskLifecycleError::ActiveSyncActionSetChanged);
            }
        }
        for reconciliation in dispatched_reconciliations {
            let task_id = reconciliation_task_id(reconciliation.write_task_attempt_id);
            let invocation_context = crate::observability::task_invocation_context(task_id);
            sqlx::query(
                "INSERT INTO tasks (
    id, workspace_id, connection_id, kind, status, cancellation_supported,
    invocation_context, created_at, updated_at
)
VALUES ($1, $2, $3, 'sync_action_reconciliation', 'queued', FALSE, $4, $5, $5)
",
            )
            .bind(task_id)
            .bind(reconciliation.workspace_id)
            .bind(reconciliation.connection_id)
            .bind(super::task_manager::invocation_context_json(
                &invocation_context,
            ))
            .bind(command.interrupted_at)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO sync_action_reconciliation_tasks (
    task_id, action_id, receiver_source_id, write_task_attempt_id
)
VALUES ($1, $2, $3, $4)
",
            )
            .bind(task_id)
            .bind(reconciliation.action_id)
            .bind(reconciliation.receiver_source_id)
            .bind(reconciliation.write_task_attempt_id)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "UPDATE sync_action_reconciliation_attempts
SET
    state = 'settled', outcome_kind = 'indeterminate', result = 'unresolved',
    outcome_error_code = 'sync_action_reconciliation_interrupted',
    outcome_error_retryable = TRUE,
    outcome_error_safe_message
    = 'reconciliation was interrupted by host restart',
    settled_at = $1
WHERE task_id = ANY($2) AND state = 'prepared'
",
        )
        .bind(command.interrupted_at)
        .bind(&task_ids)
        .execute(&mut *transaction)
        .await?;
        let task_attempts = sqlx::query(
            "UPDATE task_attempts
SET status = 'interrupted', failure = NULL, finished_at = $1
WHERE task_id = ANY($2) AND status = 'running'
",
        )
        .bind(command.interrupted_at)
        .bind(&task_ids)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let claims = sqlx::query(
            "DELETE FROM task_claims WHERE task_id = ANY($1)
",
        )
        .bind(&task_ids)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let tasks = sqlx::query(
            "UPDATE tasks
SET
    status = 'interrupted',
    failure = NULL,
    finished_at = $1,
    updated_at = $1
WHERE id = ANY($2) AND status IN ('queued', 'running', 'cancelling')
",
        )
        .bind(command.interrupted_at)
        .bind(&task_ids)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if tasks != task_ids.len() as u64 {
            return Err(TaskLifecycleError::ActiveTaskSetChanged);
        }
        if !retriable_reconciliation_task_ids.is_empty() {
            let requeued = sqlx::query(
                "UPDATE tasks
SET
    status = 'queued', failure = NULL, finished_at = NULL, updated_at = $1
WHERE id = ANY($2) AND status = 'interrupted'
",
            )
            .bind(command.interrupted_at)
            .bind(&retriable_reconciliation_task_ids)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            if requeued != retriable_reconciliation_task_ids.len() as u64 {
                return Err(TaskLifecycleError::ActiveReconciliationTaskSetChanged);
            }
        }

        transaction.commit().await?;
        Ok(InterruptionSummary {
            tasks,
            task_attempts,
            read_attempts,
            sync_actions_confirmed_not_applied,
            sync_actions_indeterminate,
            claims,
        })
    }
}

fn reconciliation_task_id(write_task_attempt_id: Uuid) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"trakkin.sync-action-reconciliation-task/v1\0");
    digest.update(write_task_attempt_id.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[derive(FromRow)]
struct DispatchedReconciliationRow {
    write_task_attempt_id: Uuid,
    action_id: Uuid,
    receiver_source_id: Uuid,
    workspace_id: Option<Uuid>,
    connection_id: Uuid,
}

impl TaskLifecycleRepository for PostgresTaskLifecycleRepository {
    type Error = TaskLifecycleError;

    fn interrupt_active_tasks(
        &self,
        command: InterruptActiveTasks,
    ) -> TaskRepositoryFuture<'_, InterruptionSummary, Self::Error> {
        Box::pin(self.interrupt(command))
    }
}

async fn clear_interrupted_read_staging(
    transaction: &mut Transaction<'_, Postgres>,
    read_attempt_ids: &[Uuid],
) -> Result<(), TaskLifecycleError> {
    sqlx::query(
        "DELETE FROM staged_catalog_relations
WHERE attempt_id = ANY($1)
",
    )
    .bind(read_attempt_ids)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM staged_catalog_items
WHERE attempt_id = ANY($1)
",
    )
    .bind(read_attempt_ids)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM staged_state_observations
WHERE attempt_id = ANY($1)
",
    )
    .bind(read_attempt_ids)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum TaskLifecycleError {
    #[error("active task set changed while interrupting startup work")]
    ActiveTaskSetChanged,
    #[error("active synchronization action attempt set changed during startup recovery")]
    ActiveSyncActionAttemptSetChanged,
    #[error("active sync action state changed during startup recovery")]
    ActiveSyncActionSetChanged,
    #[error("active synchronization reconciliation task set changed during startup recovery")]
    ActiveReconciliationTaskSetChanged,
    #[error("task lifecycle database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
