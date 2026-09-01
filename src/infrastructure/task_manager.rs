use serde_json::{Value, json};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::OffsetDateTime;
use trakkin_provider_sdk::InvocationContext;
use uuid::Uuid;

use crate::application::{
    acquisition::ReadMode,
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    task_manager::{
        AdmitTask, CancellationDisposition, ClaimNextTask, ClaimedTask, CompleteTask,
        HeartbeatTask, RequestTaskCancellation, SourceStream, TaskCompletion, TaskDefinition,
        TaskFailure, TaskKind, TaskManagerFuture, TaskRepository, TaskSnapshot, TaskStatus,
    },
};
use crate::infrastructure::sync_execution::reduce_sync_run;

#[derive(Clone)]
pub struct PostgresTaskRepository {
    pool: PgPool,
}

impl PostgresTaskRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn admit(
        &self,
        command: AdmitTask,
        invocation_context: InvocationContext,
    ) -> Result<TaskSnapshot, TaskManagerError> {
        let mut transaction = self.pool.begin().await?;
        let connection_id =
            resolve_connection(&mut transaction, command.workspace_id, &command.definition).await?;
        let kind = task_kind(&command.definition);
        sqlx::query(
            "INSERT INTO tasks (
    id, workspace_id, connection_id, kind, status, cancellation_supported,
    invocation_context, created_at, updated_at
) VALUES ($1, $2, $3, $4, 'queued', $5, $6, $7, $7)
",
        )
        .bind(command.task_id)
        .bind(command.workspace_id)
        .bind(connection_id)
        .bind(kind)
        .bind(command.cancellation_supported)
        .bind(invocation_context_json(&invocation_context))
        .bind(command.admitted_at)
        .execute(&mut *transaction)
        .await?;
        insert_task_detail(&mut transaction, command.task_id, &command.definition).await?;
        transaction.commit().await?;

        Ok(TaskSnapshot {
            id: command.task_id,
            workspace_id: command.workspace_id,
            connection_id,
            definition: command.definition,
            status: TaskStatus::Queued,
            cancellation_supported: command.cancellation_supported,
            cancellation_requested_at: None,
            failure: None,
            created_at: command.admitted_at,
            started_at: None,
            finished_at: None,
            updated_at: command.admitted_at,
        })
    }

    async fn claim(&self, command: ClaimNextTask) -> Result<Option<ClaimedTask>, TaskManagerError> {
        if command.accepted_kinds.is_empty() {
            return Err(TaskManagerError::EmptyTaskKindSelection);
        }
        let accepted_kinds = command
            .accepted_kinds
            .into_iter()
            .map(task_kind_name)
            .collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        let Some(mut task) = sqlx::query_as::<_, TaskRow>(
            "SELECT
    tasks.id, tasks.workspace_id, tasks.connection_id, tasks.kind, tasks.status,
    tasks.cancellation_supported,
    tasks.cancellation_requested_at,
    tasks.invocation_context,
    tasks.failure,
    tasks.created_at, tasks.started_at, tasks.finished_at, tasks.updated_at
FROM tasks
INNER JOIN connections ON connections.id = tasks.connection_id
WHERE
    tasks.status = 'queued'
    AND tasks.kind = ANY($1)
    AND NOT EXISTS (
        SELECT 1
        FROM task_claims
        INNER JOIN
            tasks AS claimed_tasks
            ON claimed_tasks.id = task_claims.task_id
        WHERE claimed_tasks.connection_id = tasks.connection_id
    )
ORDER BY tasks.created_at, tasks.id
FOR UPDATE OF tasks, connections SKIP LOCKED
LIMIT 1
",
        )
        .bind(accepted_kinds)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };

        let attempt_ordinal = sqlx::query_scalar::<_, i32>(
            "SELECT COALESCE(MAX(ordinal) + 1, 0)
FROM task_attempts
WHERE task_id = $1
",
        )
        .bind(task.id)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE tasks
SET status = 'running', started_at = $1, updated_at = $1
WHERE id = $2 AND status = 'queued'
",
        )
        .bind(command.claimed_at)
        .bind(task.id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO task_claims (task_id, owner_id, claimed_at, heartbeat_at)
VALUES ($1, $2, $3, $3)
",
        )
        .bind(task.id)
        .bind(command.owner_id)
        .bind(command.claimed_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO task_attempts (id, task_id, ordinal, status, started_at)
VALUES ($1, $2, $3, 'running', $4)
",
        )
        .bind(command.attempt_id)
        .bind(task.id)
        .bind(attempt_ordinal)
        .bind(command.claimed_at)
        .execute(&mut *transaction)
        .await?;
        let invocation_context = parse_invocation_context(&task.invocation_context)?;
        let definition = load_task_definition(&mut transaction, task.id, &task.kind).await?;
        task.status = "running".to_owned();
        task.started_at = Some(command.claimed_at);
        task.updated_at = command.claimed_at;
        let task = task.into_snapshot(definition)?;
        transaction.commit().await?;

        Ok(Some(ClaimedTask {
            task,
            attempt_id: command.attempt_id,
            attempt_ordinal,
            owner_id: command.owner_id,
            invocation_context,
        }))
    }

    async fn heartbeat(&self, command: HeartbeatTask) -> Result<(), TaskManagerError> {
        let result = sqlx::query(
            "UPDATE task_claims
SET heartbeat_at = $1
WHERE
    task_id = $2 AND owner_id = $3
    AND EXISTS (
        SELECT 1 FROM tasks
        WHERE
            tasks.id = task_claims.task_id
            AND tasks.status IN ('running', 'cancelling')
    )
",
        )
        .bind(command.heartbeat_at)
        .bind(command.task_id)
        .bind(command.owner_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(TaskManagerError::ClaimNotOwned);
        }
        Ok(())
    }

    async fn cancel(
        &self,
        command: RequestTaskCancellation,
    ) -> Result<CancellationDisposition, TaskManagerError> {
        let mut transaction = self.pool.begin().await?;
        let task = lock_task(&mut transaction, command.task_id).await?;
        let status = parse_status(&task.status)?;
        let disposition = match status {
            TaskStatus::Queued => {
                ensure_cancellation_supported(&task)?;
                sqlx::query(
                    "UPDATE tasks
SET
    status = 'cancelled', cancellation_requested_at = $1,
    finished_at = $1, updated_at = $1
WHERE id = $2
",
                )
                .bind(command.requested_at)
                .bind(command.task_id)
                .execute(&mut *transaction)
                .await?;
                if task.kind == "sync_action" {
                    let run_id = sqlx::query_scalar::<_, Uuid>(
                        "UPDATE sync_actions
SET status = 'cancelled', finished_at = $1, updated_at = $1
FROM sync_action_tasks
WHERE
    sync_action_tasks.task_id = $2
    AND sync_actions.id = sync_action_tasks.action_id
    AND sync_actions.status IN ('queued', 'retryable_failure')
RETURNING sync_actions.run_id
",
                    )
                    .bind(command.requested_at)
                    .bind(command.task_id)
                    .fetch_optional(&mut *transaction)
                    .await?
                    .ok_or(TaskManagerError::InvalidCompletion)?;
                    if !reduce_sync_run(&mut transaction, run_id, command.requested_at).await? {
                        return Err(TaskManagerError::InvalidCompletion);
                    }
                }
                CancellationDisposition::CancelledBeforeStart
            }
            TaskStatus::Running => {
                ensure_cancellation_supported(&task)?;
                sqlx::query(
                    "UPDATE tasks
SET status = 'cancelling', cancellation_requested_at = $1, updated_at = $1
WHERE id = $2
",
                )
                .bind(command.requested_at)
                .bind(command.task_id)
                .execute(&mut *transaction)
                .await?;
                CancellationDisposition::CancellationRequested
            }
            TaskStatus::Cancelling => CancellationDisposition::AlreadyRequested,
            terminal if terminal.terminal() => CancellationDisposition::AlreadyTerminal(terminal),
            _ => return Err(TaskManagerError::InvalidTaskStatus(task.status)),
        };
        transaction.commit().await?;
        Ok(disposition)
    }

    async fn complete(&self, command: CompleteTask) -> Result<TaskSnapshot, TaskManagerError> {
        let mut transaction = self.pool.begin().await?;
        let mut task = lock_task(&mut transaction, command.task_id).await?;
        let status = parse_status(&task.status)?;
        let claim_owner = sqlx::query_scalar::<_, Uuid>(
            "SELECT owner_id
FROM task_claims
WHERE task_id = $1
FOR UPDATE
",
        )
        .bind(command.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(TaskManagerError::ClaimNotOwned)?;
        if claim_owner != command.owner_id {
            return Err(TaskManagerError::ClaimNotOwned);
        }

        let (terminal_status, failure) = match (status, &command.completion) {
            (TaskStatus::Cancelling, _) => ("cancelled", None),
            (TaskStatus::Running, TaskCompletion::Succeeded) => ("succeeded", None),
            (TaskStatus::Running, TaskCompletion::Failed(failure)) => {
                if failure.code.is_empty() {
                    return Err(TaskManagerError::InvalidFailure);
                }
                ("failed", Some(failure_json(failure)))
            }
            (TaskStatus::Running, TaskCompletion::Cancelled) => {
                return Err(TaskManagerError::InvalidCompletion);
            }
            _ => return Err(TaskManagerError::InvalidCompletion),
        };
        let attempt = sqlx::query(
            "UPDATE task_attempts
SET status = $1, failure = $2, finished_at = $3
WHERE id = $4 AND task_id = $5 AND status = 'running'
",
        )
        .bind(terminal_status)
        .bind(&failure)
        .bind(command.completed_at)
        .bind(command.attempt_id)
        .bind(command.task_id)
        .execute(&mut *transaction)
        .await?;
        if attempt.rows_affected() != 1 {
            return Err(TaskManagerError::InvalidCompletion);
        }
        sqlx::query(
            "DELETE FROM task_claims WHERE task_id = $1 AND owner_id = $2
",
        )
        .bind(command.task_id)
        .bind(command.owner_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE tasks
SET status = $1, failure = $2, finished_at = $3, updated_at = $3
WHERE id = $4
",
        )
        .bind(terminal_status)
        .bind(&failure)
        .bind(command.completed_at)
        .bind(command.task_id)
        .execute(&mut *transaction)
        .await?;

        let definition = load_task_definition(&mut transaction, task.id, &task.kind).await?;
        task.status = terminal_status.to_owned();
        task.failure = failure;
        task.finished_at = Some(command.completed_at);
        task.updated_at = command.completed_at;
        let snapshot = task.into_snapshot(definition)?;
        transaction.commit().await?;
        Ok(snapshot)
    }

    async fn get(&self, task_id: Uuid) -> Result<Option<TaskSnapshot>, TaskManagerError> {
        let mut transaction = self.pool.begin().await?;
        let Some(task) = sqlx::query_as::<_, TaskRow>(
            "SELECT
    id, workspace_id, connection_id, kind, status, cancellation_supported,
    cancellation_requested_at,
    invocation_context,
    failure,
    created_at,
    started_at,
    finished_at,
    updated_at
FROM tasks
WHERE id = $1
",
        )
        .bind(task_id)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };
        let definition = load_task_definition(&mut transaction, task.id, &task.kind).await?;
        let snapshot = task.into_snapshot(definition)?;
        transaction.commit().await?;
        Ok(Some(snapshot))
    }

    async fn list_workspace(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<TaskSnapshot>, TaskManagerError> {
        let mut transaction = self.pool.begin().await?;
        let rows = sqlx::query_as::<_, TaskRow>(
            "SELECT
    id, workspace_id, connection_id, kind, status, cancellation_supported,
    cancellation_requested_at,
    invocation_context,
    failure,
    created_at,
    started_at,
    finished_at,
    updated_at
FROM tasks
WHERE workspace_id = $1
ORDER BY created_at DESC, id DESC
LIMIT 100
",
        )
        .bind(workspace_id)
        .fetch_all(&mut *transaction)
        .await?;
        let mut snapshots = Vec::with_capacity(rows.len());
        for row in rows {
            let definition = load_task_definition(&mut transaction, row.id, &row.kind).await?;
            snapshots.push(row.into_snapshot(definition)?);
        }
        transaction.commit().await?;
        Ok(snapshots)
    }
}

impl TaskRepository for PostgresTaskRepository {
    type Error = TaskManagerError;

    fn admit_task(
        &self,
        command: AdmitTask,
        invocation_context: InvocationContext,
    ) -> TaskManagerFuture<'_, TaskSnapshot, Self::Error> {
        Box::pin(self.admit(command, invocation_context))
    }

    fn claim_next_task(
        &self,
        command: ClaimNextTask,
    ) -> TaskManagerFuture<'_, Option<ClaimedTask>, Self::Error> {
        Box::pin(self.claim(command))
    }

    fn heartbeat_task(&self, command: HeartbeatTask) -> TaskManagerFuture<'_, (), Self::Error> {
        Box::pin(self.heartbeat(command))
    }

    fn request_cancellation(
        &self,
        command: RequestTaskCancellation,
    ) -> TaskManagerFuture<'_, CancellationDisposition, Self::Error> {
        Box::pin(self.cancel(command))
    }

    fn complete_task(
        &self,
        command: CompleteTask,
    ) -> TaskManagerFuture<'_, TaskSnapshot, Self::Error> {
        Box::pin(self.complete(command))
    }

    fn get_task(&self, task_id: Uuid) -> TaskManagerFuture<'_, Option<TaskSnapshot>, Self::Error> {
        Box::pin(self.get(task_id))
    }

    fn list_workspace_tasks(
        &self,
        workspace_id: Uuid,
    ) -> TaskManagerFuture<'_, Vec<TaskSnapshot>, Self::Error> {
        Box::pin(self.list_workspace(workspace_id))
    }
}

async fn resolve_connection(
    transaction: &mut Transaction<'_, Postgres>,
    workspace_id: Option<Uuid>,
    definition: &TaskDefinition,
) -> Result<Uuid, TaskManagerError> {
    let connection_id = match definition {
        TaskDefinition::SourceDiscovery { connection_id } => sqlx::query_scalar::<_, Uuid>(
            "SELECT id
FROM connections
WHERE id = $1 AND status <> 'disabled' AND deleted_at IS NULL
FOR SHARE
",
        )
        .bind(connection_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(TaskManagerError::ResourceUnavailable("connection"))?,
        TaskDefinition::Read { source_id, .. }
        | TaskDefinition::Reset { source_id, .. }
        | TaskDefinition::SyncAction {
            receiver_source_id: source_id,
            ..
        }
        | TaskDefinition::SyncActionReconciliation {
            receiver_source_id: source_id,
            ..
        } => sqlx::query_scalar::<_, Uuid>(
            "SELECT sources.connection_id
FROM sources
INNER JOIN connections ON connections.id = sources.connection_id
WHERE
    sources.id = $1
    AND sources.availability <> 'removed'
    AND connections.status <> 'disabled'
    AND connections.deleted_at IS NULL
FOR SHARE OF connections
",
        )
        .bind(source_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(TaskManagerError::ResourceUnavailable("source"))?,
    };
    if let (Some(workspace_id), Some(source_id)) = (workspace_id, task_source_id(definition)) {
        let attached = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1 FROM workspace_sources
    WHERE workspace_id = $1 AND source_id = $2
)
",
        )
        .bind(workspace_id)
        .bind(source_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !attached {
            return Err(TaskManagerError::SourceNotAttached);
        }
    }
    Ok(connection_id)
}

async fn insert_task_detail(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    definition: &TaskDefinition,
) -> Result<(), TaskManagerError> {
    match definition {
        TaskDefinition::SourceDiscovery { connection_id } => {
            sqlx::query(
                "INSERT INTO source_discovery_tasks (task_id, connection_id)
VALUES ($1, $2)
",
            )
            .bind(task_id)
            .bind(connection_id)
            .execute(&mut **transaction)
            .await?;
        }
        TaskDefinition::Read {
            source_id,
            stream,
            mode,
        } => {
            sqlx::query(
                "INSERT INTO read_tasks (task_id, source_id, stream_kind, read_mode)
VALUES ($1, $2, $3, $4)
",
            )
            .bind(task_id)
            .bind(source_id)
            .bind(stream_name(*stream))
            .bind(read_mode_name(*mode))
            .execute(&mut **transaction)
            .await?;
        }
        TaskDefinition::Reset { source_id, stream } => {
            sqlx::query(
                "INSERT INTO reset_tasks (task_id, source_id, stream_kind)
VALUES ($1, $2, $3)
",
            )
            .bind(task_id)
            .bind(source_id)
            .bind(stream_name(*stream))
            .execute(&mut **transaction)
            .await?;
        }
        TaskDefinition::SyncAction {
            action_id,
            receiver_source_id,
        } => {
            sqlx::query(
                "INSERT INTO sync_action_tasks (task_id, action_id, receiver_source_id)
VALUES ($1, $2, $3)
",
            )
            .bind(task_id)
            .bind(action_id)
            .bind(receiver_source_id)
            .execute(&mut **transaction)
            .await?;
        }
        TaskDefinition::SyncActionReconciliation {
            action_id,
            receiver_source_id,
            write_task_attempt_id,
        } => {
            sqlx::query(
                "INSERT INTO sync_action_reconciliation_tasks (
    task_id,
    action_id,
    receiver_source_id,
    write_task_attempt_id
)
VALUES ($1, $2, $3, $4)
",
            )
            .bind(task_id)
            .bind(action_id)
            .bind(receiver_source_id)
            .bind(write_task_attempt_id)
            .execute(&mut **transaction)
            .await?;
        }
    }
    Ok(())
}

async fn load_task_definition(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    kind: &str,
) -> Result<TaskDefinition, TaskManagerError> {
    match kind {
        "source_discovery" => {
            let connection_id = sqlx::query_scalar::<_, Uuid>(
                "SELECT connection_id FROM source_discovery_tasks WHERE task_id = $1
",
            )
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(TaskManagerError::MissingTaskDetail)?;
            Ok(TaskDefinition::SourceDiscovery { connection_id })
        }
        "catalog_read" | "state_read" => {
            let detail = sqlx::query_as::<_, ReadTaskRow>(
                "SELECT source_id, stream_kind, read_mode
FROM read_tasks
WHERE task_id = $1
",
            )
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(TaskManagerError::MissingTaskDetail)?;
            Ok(TaskDefinition::Read {
                source_id: detail.source_id,
                stream: parse_stream(&detail.stream_kind)?,
                mode: parse_read_mode(&detail.read_mode)?,
            })
        }
        "catalog_reset" | "state_reset" => {
            let detail = sqlx::query_as::<_, ResetTaskRow>(
                "SELECT source_id, stream_kind
FROM reset_tasks
WHERE task_id = $1
",
            )
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(TaskManagerError::MissingTaskDetail)?;
            Ok(TaskDefinition::Reset {
                source_id: detail.source_id,
                stream: parse_stream(&detail.stream_kind)?,
            })
        }
        "sync_action" => {
            let detail = sqlx::query_as::<_, SyncActionTaskRow>(
                "SELECT action_id, receiver_source_id
FROM sync_action_tasks
WHERE task_id = $1
",
            )
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(TaskManagerError::MissingTaskDetail)?;
            Ok(TaskDefinition::SyncAction {
                action_id: detail.action_id,
                receiver_source_id: detail.receiver_source_id,
            })
        }
        "sync_action_reconciliation" => {
            let detail = sqlx::query_as::<_, SyncActionReconciliationTaskRow>(
                "SELECT action_id, receiver_source_id, write_task_attempt_id
FROM sync_action_reconciliation_tasks
WHERE task_id = $1
",
            )
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(TaskManagerError::MissingTaskDetail)?;
            Ok(TaskDefinition::SyncActionReconciliation {
                action_id: detail.action_id,
                receiver_source_id: detail.receiver_source_id,
                write_task_attempt_id: detail.write_task_attempt_id,
            })
        }
        _ => Err(TaskManagerError::UnknownTaskKind(kind.to_owned())),
    }
}

async fn lock_task(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
) -> Result<TaskRow, TaskManagerError> {
    sqlx::query_as::<_, TaskRow>(
        "SELECT
    id, workspace_id, connection_id, kind, status, cancellation_supported,
    cancellation_requested_at,
    invocation_context,
    failure,
    created_at,
    started_at,
    finished_at,
    updated_at
FROM tasks
WHERE id = $1
FOR UPDATE
",
    )
    .bind(task_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(TaskManagerError::TaskNotFound)
}

fn task_kind(definition: &TaskDefinition) -> &'static str {
    match definition {
        TaskDefinition::SourceDiscovery { .. } => "source_discovery",
        TaskDefinition::Read {
            stream: SourceStream::Catalog,
            ..
        } => "catalog_read",
        TaskDefinition::Read {
            stream: SourceStream::State,
            ..
        } => "state_read",
        TaskDefinition::Reset {
            stream: SourceStream::Catalog,
            ..
        } => "catalog_reset",
        TaskDefinition::Reset {
            stream: SourceStream::State,
            ..
        } => "state_reset",
        TaskDefinition::SyncAction { .. } => "sync_action",
        TaskDefinition::SyncActionReconciliation { .. } => "sync_action_reconciliation",
    }
}

fn task_kind_name(kind: TaskKind) -> &'static str {
    match kind {
        TaskKind::SourceDiscovery => "source_discovery",
        TaskKind::CatalogRead => "catalog_read",
        TaskKind::StateRead => "state_read",
        TaskKind::CatalogReset => "catalog_reset",
        TaskKind::StateReset => "state_reset",
        TaskKind::SyncAction => "sync_action",
        TaskKind::SyncActionReconciliation => "sync_action_reconciliation",
    }
}

fn task_source_id(definition: &TaskDefinition) -> Option<Uuid> {
    match definition {
        TaskDefinition::SourceDiscovery { .. } => None,
        TaskDefinition::Read { source_id, .. } | TaskDefinition::Reset { source_id, .. } => {
            Some(*source_id)
        }
        TaskDefinition::SyncAction {
            receiver_source_id, ..
        }
        | TaskDefinition::SyncActionReconciliation {
            receiver_source_id, ..
        } => Some(*receiver_source_id),
    }
}

fn stream_name(stream: SourceStream) -> &'static str {
    match stream {
        SourceStream::Catalog => "catalog",
        SourceStream::State => "state",
    }
}

fn parse_stream(value: &str) -> Result<SourceStream, TaskManagerError> {
    match value {
        "catalog" => Ok(SourceStream::Catalog),
        "state" => Ok(SourceStream::State),
        _ => Err(TaskManagerError::InvalidTaskDetail),
    }
}

fn read_mode_name(mode: ReadMode) -> &'static str {
    match mode {
        ReadMode::Full => "full",
        ReadMode::Incremental => "incremental",
    }
}

fn parse_read_mode(value: &str) -> Result<ReadMode, TaskManagerError> {
    match value {
        "full" => Ok(ReadMode::Full),
        "incremental" => Ok(ReadMode::Incremental),
        _ => Err(TaskManagerError::InvalidTaskDetail),
    }
}

fn parse_status(value: &str) -> Result<TaskStatus, TaskManagerError> {
    match value {
        "queued" => Ok(TaskStatus::Queued),
        "running" => Ok(TaskStatus::Running),
        "cancelling" => Ok(TaskStatus::Cancelling),
        "succeeded" => Ok(TaskStatus::Succeeded),
        "failed" => Ok(TaskStatus::Failed),
        "cancelled" => Ok(TaskStatus::Cancelled),
        "interrupted" => Ok(TaskStatus::Interrupted),
        _ => Err(TaskManagerError::InvalidTaskStatus(value.to_owned())),
    }
}

fn ensure_cancellation_supported(task: &TaskRow) -> Result<(), TaskManagerError> {
    if task.cancellation_supported {
        Ok(())
    } else {
        Err(TaskManagerError::CancellationUnsupported)
    }
}

pub(crate) fn invocation_context_json(context: &InvocationContext) -> Value {
    json!({
        "correlationId": context.correlation_id(),
        "traceparent": context.traceparent(),
        "tracestate": context.tracestate(),
    })
}

fn parse_invocation_context(value: &Value) -> Result<InvocationContext, TaskManagerError> {
    let correlation_id = value
        .get("correlationId")
        .and_then(Value::as_str)
        .ok_or(TaskManagerError::InvalidInvocationContext)?;
    let traceparent = optional_text(value, "traceparent")?;
    let tracestate = optional_text(value, "tracestate")?;
    InvocationContext::new(correlation_id, traceparent, tracestate)
        .map_err(|_| TaskManagerError::InvalidInvocationContext)
}

fn optional_text<'a>(value: &'a Value, key: &str) -> Result<Option<&'a str>, TaskManagerError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .map(Some)
            .ok_or(TaskManagerError::InvalidInvocationContext),
    }
}

pub(crate) fn failure_json(failure: &TaskFailure) -> Value {
    json!({
        "id": failure.id,
        "code": failure.code,
        "message": failure.message,
        "retryable": failure.retryable,
        "retryAfter": failure.retry_after.as_ref().map(|after| json!({
            "seconds": after.seconds,
            "nanoseconds": after.nanoseconds,
        })),
        "diagnosticId": failure.diagnostic_id,
    })
}

fn parse_failure(value: Option<Value>) -> Result<Option<TaskFailure>, TaskManagerError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let code = value
        .get("code")
        .and_then(Value::as_str)
        .ok_or(TaskManagerError::InvalidFailure)?;
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or(TaskManagerError::InvalidFailure)?;
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .ok_or(TaskManagerError::InvalidFailure)?;
    let retryable = value
        .get("retryable")
        .and_then(Value::as_bool)
        .ok_or(TaskManagerError::InvalidFailure)?;
    let retry_after = value
        .get("retryAfter")
        .filter(|value| !value.is_null())
        .map(|value| {
            let seconds = value
                .get("seconds")
                .and_then(Value::as_i64)
                .ok_or(TaskManagerError::InvalidFailure)?;
            let nanoseconds = value
                .get("nanoseconds")
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .ok_or(TaskManagerError::InvalidFailure)?;
            if seconds < 0 || !(0..1_000_000_000).contains(&nanoseconds) {
                return Err(TaskManagerError::InvalidFailure);
            }
            Ok(crate::application::task_manager::TaskRetryAfter {
                seconds,
                nanoseconds,
            })
        })
        .transpose()?;
    let diagnostic_id = value
        .get("diagnosticId")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or(TaskManagerError::InvalidFailure)
        })
        .transpose()?;
    Ok(Some(TaskFailure {
        id: id.to_owned(),
        code: code.to_owned(),
        message: message.to_owned(),
        retryable,
        retry_after,
        diagnostic_id,
    }))
}

#[derive(FromRow)]
struct TaskRow {
    id: Uuid,
    workspace_id: Option<Uuid>,
    connection_id: Option<Uuid>,
    kind: String,
    status: String,
    cancellation_supported: bool,
    cancellation_requested_at: Option<OffsetDateTime>,
    invocation_context: Value,
    failure: Option<Value>,
    created_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    finished_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
}

impl TaskRow {
    fn into_snapshot(self, definition: TaskDefinition) -> Result<TaskSnapshot, TaskManagerError> {
        Ok(TaskSnapshot {
            id: self.id,
            workspace_id: self.workspace_id,
            connection_id: self
                .connection_id
                .ok_or(TaskManagerError::MissingTaskConnection)?,
            definition,
            status: parse_status(&self.status)?,
            cancellation_supported: self.cancellation_supported,
            cancellation_requested_at: self.cancellation_requested_at,
            failure: parse_failure(self.failure)?,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(FromRow)]
struct ReadTaskRow {
    source_id: Uuid,
    stream_kind: String,
    read_mode: String,
}

#[derive(FromRow)]
struct ResetTaskRow {
    source_id: Uuid,
    stream_kind: String,
}

#[derive(FromRow)]
struct SyncActionTaskRow {
    action_id: Uuid,
    receiver_source_id: Uuid,
}

#[derive(FromRow)]
struct SyncActionReconciliationTaskRow {
    action_id: Uuid,
    receiver_source_id: Uuid,
    write_task_attempt_id: Uuid,
}

#[derive(Debug, Error)]
pub enum TaskManagerError {
    #[error("task does not exist")]
    TaskNotFound,
    #[error("task resource is unavailable: {0}")]
    ResourceUnavailable(&'static str),
    #[error("task source is not attached to its workspace")]
    SourceNotAttached,
    #[error("task detail is missing")]
    MissingTaskDetail,
    #[error("task connection is missing")]
    MissingTaskConnection,
    #[error("task detail is invalid")]
    InvalidTaskDetail,
    #[error("unknown task kind: {0}")]
    UnknownTaskKind(String),
    #[error("invalid task status: {0}")]
    InvalidTaskStatus(String),
    #[error("task cancellation is not supported")]
    CancellationUnsupported,
    #[error("task claim is not owned by this worker")]
    ClaimNotOwned,
    #[error("task completion is not valid for its current state")]
    InvalidCompletion,
    #[error("task failure evidence is invalid")]
    InvalidFailure,
    #[error("task invocation context is invalid")]
    InvalidInvocationContext,
    #[error("task claim must select at least one task kind")]
    EmptyTaskKindSelection,
    #[error("task manager database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoApplicationError for TaskManagerError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::TaskNotFound => {
                ApplicationError::new(ApplicationErrorCode::TaskNotFound, "task was not found")
            }
            Self::ResourceUnavailable(_) => ApplicationError::new(
                ApplicationErrorCode::TaskResourceUnavailable,
                "task resource is unavailable",
            ),
            Self::SourceNotAttached => ApplicationError::new(
                ApplicationErrorCode::SourceNotAttached,
                "source is not attached to the workspace",
            ),
            Self::CancellationUnsupported => ApplicationError::new(
                ApplicationErrorCode::TaskCancellationUnsupported,
                "task cancellation is not supported",
            ),
            Self::MissingTaskDetail
            | Self::MissingTaskConnection
            | Self::InvalidTaskDetail
            | Self::UnknownTaskKind(_)
            | Self::InvalidTaskStatus(_)
            | Self::ClaimNotOwned
            | Self::InvalidCompletion
            | Self::InvalidFailure
            | Self::InvalidInvocationContext
            | Self::EmptyTaskKindSelection
            | Self::Database(_) => {
                ApplicationError::new(ApplicationErrorCode::Internal, "task operation failed")
            }
        }
    }
}
