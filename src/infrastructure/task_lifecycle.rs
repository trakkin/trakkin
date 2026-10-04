use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

use super::catalog_publication::clear_read_staging;
use crate::application::task_lifecycle::{
    InterruptActiveTasks, TaskLifecycleRepository, TaskRepositoryFuture,
};

pub struct PostgresTaskLifecycleRepository {
    pool: PgPool,
}

impl PostgresTaskLifecycleRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn interrupt(&self, command: InterruptActiveTasks) -> Result<(), TaskLifecycleError> {
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
            return Ok(());
        }
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
        clear_read_staging(&mut transaction, &read_attempt_ids).await?;
        sqlx::query(
            "UPDATE read_attempts
SET status = 'abandoned', updated_at = $1, completed_at = $1
WHERE id = ANY($2) AND status = 'staging'
",
        )
        .bind(command.interrupted_at)
        .bind(&read_attempt_ids)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE task_attempts
SET status = 'interrupted', failure = NULL, finished_at = $1
WHERE task_id = ANY($2) AND status = 'running'
",
        )
        .bind(command.interrupted_at)
        .bind(&task_ids)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM task_claims WHERE task_id = ANY($1)
",
        )
        .bind(&task_ids)
        .execute(&mut *transaction)
        .await?;
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
        transaction.commit().await?;
        Ok(())
    }
}

impl TaskLifecycleRepository for PostgresTaskLifecycleRepository {
    type Error = TaskLifecycleError;

    fn interrupt_active_tasks(
        &self,
        command: InterruptActiveTasks,
    ) -> TaskRepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.interrupt(command))
    }
}

#[derive(Debug, Error)]
pub enum TaskLifecycleError {
    #[error("active task set changed while interrupting startup work")]
    ActiveTaskSetChanged,
    #[error("task lifecycle database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}
