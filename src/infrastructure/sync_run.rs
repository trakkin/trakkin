use sqlx::{FromRow, PgPool};
use thiserror::Error;

use crate::application::{
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    sync_run::{
        StoreSyncRunAdmission, SyncRunActionSnapshot, SyncRunAdmissionError, SyncRunFuture,
        SyncRunRepository, SyncRunSnapshot,
    },
    task_manager::{TaskDefinition, TaskSnapshot, TaskStatus},
};

#[derive(Clone)]
pub struct PostgresSyncRunRepository {
    pool: PgPool,
}

impl PostgresSyncRunRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn admit(
        &self,
        command: StoreSyncRunAdmission,
    ) -> Result<SyncRunSnapshot, SyncRunAdmissionError<PostgresSyncRunRepositoryError>> {
        let mut transaction = self.pool.begin().await?;
        let preview = sqlx::query_as::<_, PreviewAdmissionRow>(
            "SELECT content_hash, fingerprint_json, fresh_until, created_at
FROM sync_previews
WHERE id = $1 AND workspace_id = $2
FOR UPDATE
",
        )
        .bind(command.preview_id)
        .bind(command.workspace_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SyncRunAdmissionError::PreviewNotFound)?;
        if preview.content_hash != command.preview_hash {
            return Err(SyncRunAdmissionError::PreviewHashMismatch);
        }
        if preview.fingerprint_json != command.current_fingerprint_json
            || command.admitted_at < preview.created_at
            || command.admitted_at >= preview.fresh_until
        {
            return Err(SyncRunAdmissionError::PreviewStale);
        }
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM sync_runs WHERE preview_id = $1)
",
        )
        .bind(command.preview_id)
        .fetch_one(&mut *transaction)
        .await?
        {
            return Err(SyncRunAdmissionError::PreviewAlreadyAdmitted);
        }
        if let Some(occurrence_id) = command.schedule_occurrence_id
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
    SELECT 1 FROM sync_runs WHERE schedule_occurrence_id = $1
)
",
            )
            .bind(occurrence_id)
            .fetch_one(&mut *transaction)
            .await?
        {
            return Err(SyncRunAdmissionError::PreviewAlreadyAdmitted);
        }
        let requested_hashes = command
            .actions
            .iter()
            .map(|action| action.action_hash.clone())
            .collect::<Vec<_>>();
        let receiver_source_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT DISTINCT receiver_source_id
FROM sync_preview_actions
WHERE preview_id = $1 AND action_hash = ANY($2)
ORDER BY receiver_source_id
",
        )
        .bind(command.preview_id)
        .bind(&requested_hashes)
        .fetch_all(&mut *transaction)
        .await?;
        let locked_receiver_source_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT id
FROM sources
WHERE id = ANY($1)
ORDER BY id
FOR UPDATE
",
        )
        .bind(&receiver_source_ids)
        .fetch_all(&mut *transaction)
        .await?;
        if locked_receiver_source_ids != receiver_source_ids {
            return Err(SyncRunAdmissionError::ReceiverUnavailable);
        }
        let action_rows = sqlx::query_as::<_, AdmissionActionRow>(
            "SELECT
    preview_actions.action_hash,
    preview_actions.receiver_source_id,
    sources.connection_id
FROM sync_preview_actions AS preview_actions
INNER JOIN sources ON sources.id = preview_actions.receiver_source_id
INNER JOIN connections ON connections.id = sources.connection_id
INNER JOIN workspace_sources
    ON
        workspace_sources.workspace_id = preview_actions.workspace_id
        AND workspace_sources.source_id = preview_actions.receiver_source_id
WHERE
    preview_actions.preview_id = $1
    AND preview_actions.action_hash = ANY($2)
    AND sources.availability <> 'removed'
    AND connections.status <> 'disabled'
    AND connections.deleted_at IS NULL
ORDER BY preview_actions.action_hash
FOR SHARE OF connections, workspace_sources
",
        )
        .bind(command.preview_id)
        .bind(&requested_hashes)
        .fetch_all(&mut *transaction)
        .await?;
        if action_rows.len() != command.actions.len()
            || action_rows
                .iter()
                .zip(&command.actions)
                .any(|(row, action)| row.action_hash != action.action_hash)
        {
            return Err(SyncRunAdmissionError::ReceiverUnavailable);
        }
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1
    FROM sync_actions AS existing_actions
    INNER JOIN sync_preview_actions AS existing_preview_actions
        ON
            existing_preview_actions.preview_id = existing_actions.preview_id
            AND existing_preview_actions.action_hash
            = existing_actions.preview_action_hash
    INNER JOIN sync_preview_actions AS requested_preview_actions
        ON
            requested_preview_actions.preview_id = $1
            AND requested_preview_actions.action_hash = ANY($2)
            AND requested_preview_actions.receiver_source_id
            = existing_preview_actions.receiver_source_id
            AND requested_preview_actions.receiver_subject_kind
            = existing_preview_actions.receiver_subject_kind
            AND requested_preview_actions.receiver_subject_namespace
            = existing_preview_actions.receiver_subject_namespace
            AND requested_preview_actions.receiver_subject_value
            = existing_preview_actions.receiver_subject_value
            AND requested_preview_actions.semantic_field
            = existing_preview_actions.semantic_field
    WHERE existing_actions.status IN (
        'queued', 'running', 'retryable_failure', 'indeterminate'
    )
)
",
        )
        .bind(command.preview_id)
        .bind(&requested_hashes)
        .fetch_one(&mut *transaction)
        .await?
        {
            return Err(SyncRunAdmissionError::ReceiverActionBlocked);
        }

        sqlx::query(
            "INSERT INTO sync_runs (
    id, workspace_id, preview_id, schedule_occurrence_id, preview_hash,
    created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $6)
",
        )
        .bind(command.run_id)
        .bind(command.workspace_id)
        .bind(command.preview_id)
        .bind(command.schedule_occurrence_id)
        .bind(&command.preview_hash)
        .bind(command.admitted_at)
        .execute(&mut *transaction)
        .await?;

        let mut actions = Vec::with_capacity(command.actions.len());
        for (action, row) in command.actions.iter().zip(action_rows) {
            sqlx::query(
                "INSERT INTO sync_actions (
    id, run_id, workspace_id, preview_id, preview_action_hash,
    receiver_source_id, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
",
            )
            .bind(action.action_id)
            .bind(command.run_id)
            .bind(command.workspace_id)
            .bind(command.preview_id)
            .bind(&action.action_hash)
            .bind(row.receiver_source_id)
            .bind(command.admitted_at)
            .execute(&mut *transaction)
            .await?;
            let invocation_context = crate::observability::task_invocation_context(action.task_id);
            sqlx::query(
                "INSERT INTO tasks (
    id, workspace_id, connection_id, kind, status, cancellation_supported,
    invocation_context, created_at, updated_at
) VALUES ($1, $2, $3, 'sync_action', 'queued', TRUE, $4, $5, $5)
",
            )
            .bind(action.task_id)
            .bind(command.workspace_id)
            .bind(row.connection_id)
            .bind(super::task_manager::invocation_context_json(
                &invocation_context,
            ))
            .bind(command.admitted_at)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO sync_action_tasks (task_id, action_id, receiver_source_id)
VALUES ($1, $2, $3)
",
            )
            .bind(action.task_id)
            .bind(action.action_id)
            .bind(row.receiver_source_id)
            .execute(&mut *transaction)
            .await?;
            actions.push(SyncRunActionSnapshot {
                action_id: action.action_id,
                action_hash: action.action_hash.clone(),
                idempotency_key: action.action_id.to_string(),
                receiver_source_id: row.receiver_source_id,
                task: TaskSnapshot {
                    id: action.task_id,
                    workspace_id: Some(command.workspace_id),
                    connection_id: row.connection_id,
                    definition: TaskDefinition::SyncAction {
                        action_id: action.action_id,
                        receiver_source_id: row.receiver_source_id,
                    },
                    status: TaskStatus::Queued,
                    cancellation_supported: true,
                    cancellation_requested_at: None,
                    failure: None,
                    created_at: command.admitted_at,
                    started_at: None,
                    finished_at: None,
                    updated_at: command.admitted_at,
                },
            });
        }
        transaction.commit().await?;
        Ok(SyncRunSnapshot {
            run_id: command.run_id,
            preview_id: command.preview_id,
            workspace_id: command.workspace_id,
            preview_hash: command.preview_hash,
            actions,
            created_at: command.admitted_at,
        })
    }
}

impl SyncRunRepository for PostgresSyncRunRepository {
    type Error = PostgresSyncRunRepositoryError;

    fn admit_sync_run(
        &self,
        command: StoreSyncRunAdmission,
    ) -> SyncRunFuture<'_, SyncRunSnapshot, Self::Error> {
        Box::pin(self.admit(command))
    }
}

#[derive(FromRow)]
struct PreviewAdmissionRow {
    content_hash: String,
    fingerprint_json: Vec<u8>,
    fresh_until: time::OffsetDateTime,
    created_at: time::OffsetDateTime,
}

#[derive(FromRow)]
struct AdmissionActionRow {
    action_hash: String,
    receiver_source_id: uuid::Uuid,
    connection_id: uuid::Uuid,
}

#[derive(Debug, Error)]
pub enum PostgresSyncRunRepositoryError {
    #[error("sync run database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoApplicationError for PostgresSyncRunRepositoryError {
    fn into_application_error(self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::Internal,
            "synchronization run database operation failed",
        )
    }
}

impl From<sqlx::Error> for SyncRunAdmissionError<PostgresSyncRunRepositoryError> {
    fn from(error: sqlx::Error) -> Self {
        Self::Repository(PostgresSyncRunRepositoryError::Database(error))
    }
}
