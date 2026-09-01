use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::{
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    sync_activity::{
        MAX_SYNC_ACTIVITY_PAGE_SIZE, QuerySyncActivity, SyncActivityActionRecord,
        SyncActivityActionStatus, SyncActivityActionSummary, SyncActivityError, SyncActivityFuture,
        SyncActivityPage, SyncActivityRepository, SyncActivityRunStatus, SyncActivityRunSummary,
        SyncActivityTaskSummary,
    },
    sync_provenance::{SyncActionProvenanceError, SyncActionProvenanceService},
    task_manager::TaskStatus,
};

use super::sync_provenance::PostgresSyncActionProvenanceRepository;

const CURSOR_VERSION: i64 = 1;

#[derive(Clone)]
pub struct PostgresSyncActivityRepository {
    pool: PgPool,
    provenance: SyncActionProvenanceService<PostgresSyncActionProvenanceRepository>,
}

impl PostgresSyncActivityRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            provenance: SyncActionProvenanceService::new(
                PostgresSyncActionProvenanceRepository::new(pool.clone()),
            ),
            pool,
        }
    }

    async fn list(
        &self,
        query: QuerySyncActivity,
    ) -> Result<SyncActivityPage, PostgresSyncActivityError> {
        if query.limit == 0 || query.limit > MAX_SYNC_ACTIVITY_PAGE_SIZE {
            return Err(PostgresSyncActivityError::InvalidQuery);
        }
        ensure_workspace(&self.pool, query.workspace_id).await?;
        let statuses = query
            .statuses
            .iter()
            .map(|status| run_status_name(*status).to_owned())
            .collect::<Vec<_>>();
        let cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| !cursor.matches_query(&query, &statuses))
        {
            return Err(PostgresSyncActivityError::InvalidCursor);
        }
        let after_created_at = cursor.as_ref().map(|cursor| cursor.created_at);
        let after_run_id = cursor.as_ref().map(|cursor| cursor.run_id);
        let status_filter = (!statuses.is_empty()).then_some(statuses.as_slice());
        let row_limit = i64::from(query.limit) + 1;
        let mut rows = sqlx::query_as::<_, RunRow>(
            "SELECT
    runs.id AS run_id, runs.workspace_id, runs.preview_id, runs.status,
    runs.created_at, runs.started_at, runs.finished_at, runs.updated_at
FROM sync_runs AS runs
WHERE
    runs.workspace_id = $1
    AND ($2::UUID IS NULL OR runs.id = $2)
    AND ($3::TEXT [] IS NULL OR runs.status = ANY($3))
    AND ($4::TIMESTAMPTZ IS NULL OR runs.created_at >= $4)
    AND ($5::TIMESTAMPTZ IS NULL OR runs.created_at < $5)
    AND (
        ($6::UUID IS NULL AND $7::TEXT IS NULL)
        OR EXISTS (
            SELECT 1
            FROM sync_actions AS filtered_actions
            INNER JOIN sync_preview_actions AS preview_actions
                ON
                    preview_actions.preview_id = filtered_actions.preview_id
                    AND preview_actions.action_hash
                    = filtered_actions.preview_action_hash
            WHERE
                filtered_actions.run_id = runs.id
                AND (
                    $6::UUID IS NULL
                    OR filtered_actions.receiver_source_id = $6
                )
                AND (
                    $7::TEXT IS NULL
                    OR preview_actions.semantic_field = $7
                )
        )
    )
    AND (
        $8::TIMESTAMPTZ IS NULL
        OR runs.created_at < $8
        OR (runs.created_at = $8 AND runs.id > $9::UUID)
    )
ORDER BY runs.created_at DESC, runs.id
LIMIT $10
",
        )
        .bind(query.workspace_id)
        .bind(query.run_id)
        .bind(status_filter)
        .bind(query.occurred_at_or_after)
        .bind(query.occurred_before)
        .bind(query.receiver_source_id)
        .bind(query.semantic_field.as_deref())
        .bind(after_created_at)
        .bind(after_run_id)
        .bind(row_limit)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > query.limit as usize;
        if has_more {
            rows.truncate(query.limit as usize);
        }
        let next_cursor = if has_more {
            rows.last()
                .map(|row| encode_cursor(&query, &statuses, row))
                .transpose()?
        } else {
            None
        };
        let run_ids = rows.iter().map(|row| row.run_id).collect::<Vec<_>>();
        let action_rows = load_action_rows(
            &self.pool,
            query.workspace_id,
            (!run_ids.is_empty()).then_some(run_ids),
            None,
        )
        .await?;
        let mut actions_by_run = BTreeMap::<Uuid, Vec<SyncActivityActionSummary>>::new();
        for row in action_rows {
            let action = row.into_summary()?;
            actions_by_run
                .entry(action.run_id)
                .or_default()
                .push(action);
        }
        let runs = rows
            .into_iter()
            .map(|row| {
                let run_id = row.run_id;
                row.into_summary(actions_by_run.remove(&run_id).unwrap_or_default())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SyncActivityPage { runs, next_cursor })
    }

    async fn action_record(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> Result<Option<SyncActivityActionRecord>, PostgresSyncActivityError> {
        let mut rows = load_action_rows(&self.pool, workspace_id, None, Some(action_id)).await?;
        let Some(row) = rows.pop() else {
            return Ok(None);
        };
        if !rows.is_empty() {
            return Err(PostgresSyncActivityError::CorruptProjection);
        }
        let summary = row.into_summary()?;
        let provenance = match self.provenance.get(workspace_id, action_id).await {
            Ok(provenance) => Some(provenance),
            Err(SyncActionProvenanceError::NotFound) => None,
            Err(
                SyncActionProvenanceError::RepositoryInvariant
                | SyncActionProvenanceError::Repository(_),
            ) => return Err(PostgresSyncActivityError::CorruptProjection),
        };
        Ok(Some(SyncActivityActionRecord {
            summary,
            provenance,
        }))
    }
}

impl SyncActivityRepository for PostgresSyncActivityRepository {
    type Error = PostgresSyncActivityError;

    fn query_activity(
        &self,
        query: QuerySyncActivity,
    ) -> SyncActivityFuture<'_, SyncActivityPage, Self::Error> {
        Box::pin(self.list(query))
    }

    fn get_action_record(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> SyncActivityFuture<'_, Option<SyncActivityActionRecord>, Self::Error> {
        Box::pin(self.action_record(workspace_id, action_id))
    }
}

async fn load_action_rows(
    pool: &PgPool,
    workspace_id: Uuid,
    run_ids: Option<Vec<Uuid>>,
    action_id: Option<Uuid>,
) -> Result<Vec<ActionRow>, PostgresSyncActivityError> {
    sqlx::query_as::<_, ActionRow>(
        "SELECT
    actions.id AS action_id, actions.run_id, actions.workspace_id,
    actions.preview_action_hash AS action_hash, actions.receiver_source_id,
    preview_actions.semantic_field, actions.status AS action_status,
    actions.created_at AS action_created_at,
    actions.started_at AS action_started_at,
    actions.finished_at AS action_finished_at,
    actions.updated_at AS action_updated_at,
    write_tasks.id AS write_task_id, write_tasks.status AS write_task_status,
    write_tasks.created_at AS write_task_created_at,
    write_tasks.started_at AS write_task_started_at,
    write_tasks.finished_at AS write_task_finished_at,
    write_tasks.updated_at AS write_task_updated_at,
    reconciliation.task_id AS reconciliation_task_id,
    reconciliation.status AS reconciliation_task_status,
    reconciliation.created_at AS reconciliation_task_created_at,
    reconciliation.started_at AS reconciliation_task_started_at,
    reconciliation.finished_at AS reconciliation_task_finished_at,
    reconciliation.updated_at AS reconciliation_task_updated_at
FROM sync_actions AS actions
INNER JOIN sync_preview_actions AS preview_actions
    ON
        preview_actions.preview_id = actions.preview_id
        AND preview_actions.action_hash = actions.preview_action_hash
INNER JOIN sync_action_tasks AS action_tasks
    ON action_tasks.action_id = actions.id
INNER JOIN tasks AS write_tasks ON write_tasks.id = action_tasks.task_id
LEFT JOIN LATERAL (
    SELECT
        reconciliation_tasks.task_id, tasks.status, tasks.created_at,
        tasks.started_at, tasks.finished_at, tasks.updated_at
    FROM sync_action_reconciliation_tasks AS reconciliation_tasks
    INNER JOIN tasks ON tasks.id = reconciliation_tasks.task_id
    WHERE reconciliation_tasks.action_id = actions.id
    ORDER BY tasks.created_at DESC, tasks.id
    LIMIT 1
) AS reconciliation ON TRUE
WHERE
    actions.workspace_id = $1
    AND ($2::UUID [] IS NULL OR actions.run_id = ANY($2))
    AND ($3::UUID IS NULL OR actions.id = $3)
ORDER BY actions.run_id, actions.created_at, actions.id
",
    )
    .bind(workspace_id)
    .bind(run_ids)
    .bind(action_id)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

#[derive(FromRow)]
struct RunRow {
    run_id: Uuid,
    workspace_id: Uuid,
    preview_id: Uuid,
    status: String,
    created_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    finished_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
}

impl RunRow {
    fn into_summary(
        self,
        actions: Vec<SyncActivityActionSummary>,
    ) -> Result<SyncActivityRunSummary, PostgresSyncActivityError> {
        Ok(SyncActivityRunSummary {
            run_id: self.run_id,
            workspace_id: self.workspace_id,
            preview_id: self.preview_id,
            status: run_status(&self.status)?,
            actions,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(FromRow)]
struct ActionRow {
    action_id: Uuid,
    run_id: Uuid,
    workspace_id: Uuid,
    action_hash: String,
    receiver_source_id: Uuid,
    semantic_field: String,
    action_status: String,
    action_created_at: OffsetDateTime,
    action_started_at: Option<OffsetDateTime>,
    action_finished_at: Option<OffsetDateTime>,
    action_updated_at: OffsetDateTime,
    write_task_id: Uuid,
    write_task_status: String,
    write_task_created_at: OffsetDateTime,
    write_task_started_at: Option<OffsetDateTime>,
    write_task_finished_at: Option<OffsetDateTime>,
    write_task_updated_at: OffsetDateTime,
    reconciliation_task_id: Option<Uuid>,
    reconciliation_task_status: Option<String>,
    reconciliation_task_created_at: Option<OffsetDateTime>,
    reconciliation_task_started_at: Option<OffsetDateTime>,
    reconciliation_task_finished_at: Option<OffsetDateTime>,
    reconciliation_task_updated_at: Option<OffsetDateTime>,
}

impl ActionRow {
    fn into_summary(self) -> Result<SyncActivityActionSummary, PostgresSyncActivityError> {
        let reconciliation_task = match (
            self.reconciliation_task_id,
            self.reconciliation_task_status,
            self.reconciliation_task_created_at,
            self.reconciliation_task_updated_at,
        ) {
            (Some(task_id), Some(status), Some(created_at), Some(updated_at)) => {
                Some(SyncActivityTaskSummary {
                    task_id,
                    status: task_status(&status)?,
                    created_at,
                    started_at: self.reconciliation_task_started_at,
                    finished_at: self.reconciliation_task_finished_at,
                    updated_at,
                })
            }
            (None, None, None, None)
                if self.reconciliation_task_started_at.is_none()
                    && self.reconciliation_task_finished_at.is_none() =>
            {
                None
            }
            _ => return Err(PostgresSyncActivityError::CorruptProjection),
        };
        Ok(SyncActivityActionSummary {
            action_id: self.action_id,
            run_id: self.run_id,
            workspace_id: self.workspace_id,
            action_hash: self.action_hash,
            receiver_source_id: self.receiver_source_id,
            semantic_field: self.semantic_field,
            status: action_status(&self.action_status)?,
            write_task: SyncActivityTaskSummary {
                task_id: self.write_task_id,
                status: task_status(&self.write_task_status)?,
                created_at: self.write_task_created_at,
                started_at: self.write_task_started_at,
                finished_at: self.write_task_finished_at,
                updated_at: self.write_task_updated_at,
            },
            reconciliation_task,
            created_at: self.action_created_at,
            started_at: self.action_started_at,
            finished_at: self.action_finished_at,
            updated_at: self.action_updated_at,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActivityCursor {
    version: i64,
    workspace_id: Uuid,
    filter_run_id: Option<Uuid>,
    statuses: Vec<String>,
    receiver_source_id: Option<Uuid>,
    semantic_field: Option<String>,
    occurred_at_or_after: Option<String>,
    occurred_before: Option<String>,
    created_at: String,
    run_id: Uuid,
}

impl ActivityCursor {
    fn matches_query(&self, query: &QuerySyncActivity, statuses: &[String]) -> bool {
        self.workspace_id == query.workspace_id
            && self.filter_run_id == query.run_id
            && self.statuses == statuses
            && self.receiver_source_id == query.receiver_source_id
            && self.semantic_field == query.semantic_field
            && self.occurred_at_or_after == timestamp_string(query.occurred_at_or_after)
            && self.occurred_before == timestamp_string(query.occurred_before)
    }
}

struct DecodedActivityCursor {
    scope: ActivityCursor,
    created_at: OffsetDateTime,
    run_id: Uuid,
}

impl DecodedActivityCursor {
    fn matches_query(&self, query: &QuerySyncActivity, statuses: &[String]) -> bool {
        self.scope.matches_query(query, statuses)
    }
}

fn encode_cursor(
    query: &QuerySyncActivity,
    statuses: &[String],
    row: &RunRow,
) -> Result<String, PostgresSyncActivityError> {
    let cursor = ActivityCursor {
        version: CURSOR_VERSION,
        workspace_id: query.workspace_id,
        filter_run_id: query.run_id,
        statuses: statuses.to_vec(),
        receiver_source_id: query.receiver_source_id,
        semantic_field: query.semantic_field.clone(),
        occurred_at_or_after: timestamp_string(query.occurred_at_or_after),
        occurred_before: timestamp_string(query.occurred_before),
        created_at: row.created_at.unix_timestamp_nanos().to_string(),
        run_id: row.run_id,
    };
    serde_json::to_vec(&cursor)
        .map(hex::encode)
        .map_err(|_| PostgresSyncActivityError::CursorEncoding)
}

fn decode_cursor(cursor: &str) -> Result<DecodedActivityCursor, PostgresSyncActivityError> {
    let bytes = hex::decode(cursor).map_err(|_| PostgresSyncActivityError::InvalidCursor)?;
    let scope = serde_json::from_slice::<ActivityCursor>(&bytes)
        .map_err(|_| PostgresSyncActivityError::InvalidCursor)?;
    if scope.version != CURSOR_VERSION || scope.run_id.is_nil() {
        return Err(PostgresSyncActivityError::InvalidCursor);
    }
    let timestamp = scope
        .created_at
        .parse::<i128>()
        .map_err(|_| PostgresSyncActivityError::InvalidCursor)?;
    let created_at = OffsetDateTime::from_unix_timestamp_nanos(timestamp)
        .map_err(|_| PostgresSyncActivityError::InvalidCursor)?;
    let run_id = scope.run_id;
    Ok(DecodedActivityCursor {
        scope,
        created_at,
        run_id,
    })
}

fn timestamp_string(timestamp: Option<OffsetDateTime>) -> Option<String> {
    timestamp.map(|timestamp| timestamp.unix_timestamp_nanos().to_string())
}

fn run_status_name(status: SyncActivityRunStatus) -> &'static str {
    match status {
        SyncActivityRunStatus::Queued => "queued",
        SyncActivityRunStatus::Running => "running",
        SyncActivityRunStatus::Succeeded => "succeeded",
        SyncActivityRunStatus::Failed => "failed",
        SyncActivityRunStatus::Partial => "partial",
        SyncActivityRunStatus::Indeterminate => "indeterminate",
        SyncActivityRunStatus::Cancelled => "cancelled",
    }
}

fn run_status(value: &str) -> Result<SyncActivityRunStatus, PostgresSyncActivityError> {
    match value {
        "queued" => Ok(SyncActivityRunStatus::Queued),
        "running" => Ok(SyncActivityRunStatus::Running),
        "succeeded" => Ok(SyncActivityRunStatus::Succeeded),
        "failed" => Ok(SyncActivityRunStatus::Failed),
        "partial" => Ok(SyncActivityRunStatus::Partial),
        "indeterminate" => Ok(SyncActivityRunStatus::Indeterminate),
        "cancelled" => Ok(SyncActivityRunStatus::Cancelled),
        _ => Err(PostgresSyncActivityError::CorruptProjection),
    }
}

fn action_status(value: &str) -> Result<SyncActivityActionStatus, PostgresSyncActivityError> {
    match value {
        "queued" => Ok(SyncActivityActionStatus::Queued),
        "running" => Ok(SyncActivityActionStatus::Running),
        "applied" => Ok(SyncActivityActionStatus::Applied),
        "no_op" => Ok(SyncActivityActionStatus::NoOp),
        "precondition_failed" => Ok(SyncActivityActionStatus::PreconditionFailed),
        "retryable_failure" => Ok(SyncActivityActionStatus::RetryableFailure),
        "failed" => Ok(SyncActivityActionStatus::Failed),
        "indeterminate" => Ok(SyncActivityActionStatus::Indeterminate),
        "cancelled" => Ok(SyncActivityActionStatus::Cancelled),
        _ => Err(PostgresSyncActivityError::CorruptProjection),
    }
}

fn task_status(value: &str) -> Result<TaskStatus, PostgresSyncActivityError> {
    match value {
        "queued" => Ok(TaskStatus::Queued),
        "running" => Ok(TaskStatus::Running),
        "cancelling" => Ok(TaskStatus::Cancelling),
        "succeeded" => Ok(TaskStatus::Succeeded),
        "failed" => Ok(TaskStatus::Failed),
        "cancelled" => Ok(TaskStatus::Cancelled),
        "interrupted" => Ok(TaskStatus::Interrupted),
        _ => Err(PostgresSyncActivityError::CorruptProjection),
    }
}

async fn ensure_workspace(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<(), PostgresSyncActivityError> {
    if sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
    SELECT 1
    FROM workspaces
    WHERE id = $1
)
",
    )
    .bind(workspace_id)
    .fetch_one(pool)
    .await?
    {
        Ok(())
    } else {
        Err(PostgresSyncActivityError::WorkspaceNotFound)
    }
}

#[derive(Debug, Error)]
pub enum PostgresSyncActivityError {
    #[error("activity query is invalid")]
    InvalidQuery,
    #[error("activity cursor is invalid")]
    InvalidCursor,
    #[error("activity cursor could not be encoded")]
    CursorEncoding,
    #[error("workspace was not found")]
    WorkspaceNotFound,
    #[error("stored Activity projection is corrupt")]
    CorruptProjection,
    #[error("activity database operation failed")]
    Database(#[from] sqlx::Error),
}

impl IntoApplicationError for SyncActivityError<PostgresSyncActivityError> {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::InvalidQuery => ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "Activity query is invalid",
            ),
            Self::NotFound => ApplicationError::new(
                ApplicationErrorCode::ActivityActionNotFound,
                "Activity action was not found",
            ),
            Self::RepositoryInvariant => {
                ApplicationError::new(ApplicationErrorCode::Internal, "Activity projection failed")
            }
            Self::Repository(error) => error.into_application_error(),
        }
    }
}

impl IntoApplicationError for PostgresSyncActivityError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::InvalidQuery => ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "Activity query is invalid",
            ),
            Self::InvalidCursor => ApplicationError::new(
                ApplicationErrorCode::ActivityCursorInvalid,
                "Activity cursor is invalid for this query",
            ),
            Self::WorkspaceNotFound => ApplicationError::new(
                ApplicationErrorCode::WorkspaceNotFound,
                "workspace was not found",
            ),
            Self::CursorEncoding | Self::CorruptProjection | Self::Database(_) => {
                ApplicationError::new(ApplicationErrorCode::Internal, "Activity projection failed")
            }
        }
    }
}
