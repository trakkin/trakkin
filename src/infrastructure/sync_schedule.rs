use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::error::{ApplicationError, ApplicationErrorCode, IntoApplicationError};
use crate::application::sync_schedule::{
    CompleteSyncScheduleOccurrence, MaterializeDueSyncSchedule, PauseSyncSchedule,
    ReplacePendingSyncPreview, StoreSyncSchedule, SyncScheduleFuture,
    SyncScheduleOccurrenceSnapshot, SyncScheduleOccurrenceStatus, SyncScheduleRecurrence,
    SyncScheduleRepository, SyncScheduleRepositoryError, SyncScheduleSnapshot, SyncScheduleState,
};

type ScheduleResult<T> =
    Result<T, SyncScheduleRepositoryError<PostgresSyncScheduleRepositoryError>>;

#[derive(Clone)]
pub struct PostgresSyncScheduleRepository {
    pool: PgPool,
}

impl PostgresSyncScheduleRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn store(&self, command: StoreSyncSchedule) -> ScheduleResult<SyncScheduleSnapshot> {
        if !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1
    FROM sync_policies
    WHERE id = $1 AND workspace_id = $2
)
",
        )
        .bind(command.policy_id)
        .bind(command.workspace_id)
        .fetch_one(&self.pool)
        .await?
        {
            return Err(SyncScheduleRepositoryError::PolicyNotFound);
        }
        let row = sqlx::query_as::<_, ScheduleRow>(
            "INSERT INTO sync_schedules (
    id, workspace_id, policy_id, interval_seconds, state, next_run_at,
    created_at, updated_at
) VALUES ($1, $2, $3, $4, 'enabled', $5, $6, $6)
ON CONFLICT (workspace_id, policy_id) DO UPDATE SET
interval_seconds = excluded.interval_seconds,
state = 'enabled',
next_run_at = excluded.next_run_at,
updated_at = excluded.updated_at
RETURNING
    id, workspace_id, policy_id, interval_seconds, state, next_run_at,
    created_at, updated_at
",
        )
        .bind(command.schedule_id)
        .bind(command.workspace_id)
        .bind(command.policy_id)
        .bind(i64::from(command.recurrence.interval_seconds()))
        .bind(command.next_run_at)
        .bind(command.enabled_at)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SyncScheduleRepositoryError::RepositoryInvariant)?;
        schedule_snapshot(row)
    }

    async fn get(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ScheduleResult<Option<SyncScheduleSnapshot>> {
        sqlx::query_as::<_, ScheduleRow>(
            "SELECT
    id, workspace_id, policy_id, interval_seconds, state, next_run_at,
    created_at, updated_at
FROM sync_schedules
WHERE policy_id = $1 AND workspace_id = $2
",
        )
        .bind(policy_id)
        .bind(workspace_id)
        .fetch_optional(&self.pool)
        .await?
        .map(schedule_snapshot)
        .transpose()
    }

    async fn pause(&self, command: PauseSyncSchedule) -> ScheduleResult<SyncScheduleSnapshot> {
        let row = sqlx::query_as::<_, ScheduleRow>(
            "UPDATE sync_schedules
SET state = 'paused', next_run_at = NULL, updated_at = $1
WHERE policy_id = $2 AND workspace_id = $3
RETURNING
    id, workspace_id, policy_id, interval_seconds, state, next_run_at,
    created_at, updated_at
",
        )
        .bind(command.paused_at)
        .bind(command.policy_id)
        .bind(command.workspace_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SyncScheduleRepositoryError::ScheduleNotFound)?;
        schedule_snapshot(row)
    }

    async fn materialize(
        &self,
        command: MaterializeDueSyncSchedule,
    ) -> ScheduleResult<Option<SyncScheduleOccurrenceSnapshot>> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, DueScheduleRow>(
            "SELECT id, workspace_id, policy_id, interval_seconds, next_run_at
FROM sync_schedules
WHERE state = 'enabled' AND next_run_at <= $1
ORDER BY next_run_at, id
FOR UPDATE SKIP LOCKED
LIMIT 1
",
        )
        .bind(command.observed_at)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let interval_seconds =
            u32::try_from(row.interval_seconds).map_err(|_| corrupt_schedule())?;
        let recurrence =
            SyncScheduleRecurrence::every(interval_seconds).map_err(|_| corrupt_schedule())?;
        let next_run_at = recurrence
            .next_after(row.next_run_at, command.observed_at)
            .map_err(|_| corrupt_schedule())?;
        let occurrence = sqlx::query_as::<_, OccurrenceRow>(
            "INSERT INTO sync_schedule_occurrences (
    id, schedule_id, workspace_id, policy_id, scheduled_for,
    preview_id, run_id, status, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)
RETURNING
    id, schedule_id, workspace_id, policy_id, scheduled_for,
    preview_id, run_id, status, created_at, completed_at
",
        )
        .bind(command.occurrence_id)
        .bind(row.id)
        .bind(row.workspace_id)
        .bind(row.policy_id)
        .bind(row.next_run_at)
        .bind(command.preview_id)
        .bind(command.run_id)
        .bind(command.observed_at)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE sync_schedules
SET next_run_at = $1, updated_at = $2
WHERE id = $3
",
        )
        .bind(next_run_at)
        .bind(command.observed_at)
        .bind(row.id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(occurrence_snapshot(occurrence)?))
    }

    async fn pending(&self) -> ScheduleResult<Option<SyncScheduleOccurrenceSnapshot>> {
        sqlx::query_as::<_, OccurrenceRow>(
            "SELECT
    id, schedule_id, workspace_id, policy_id, scheduled_for,
    preview_id, run_id, status, created_at, completed_at
FROM sync_schedule_occurrences
WHERE status = 'pending'
ORDER BY scheduled_for, id
LIMIT 1
",
        )
        .fetch_optional(&self.pool)
        .await?
        .map(occurrence_snapshot)
        .transpose()
    }

    async fn complete(
        &self,
        command: CompleteSyncScheduleOccurrence,
    ) -> ScheduleResult<SyncScheduleOccurrenceSnapshot> {
        let row = sqlx::query_as::<_, OccurrenceRow>(
            "UPDATE sync_schedule_occurrences
SET status = 'completed', completed_at = $1
WHERE id = $2 AND status = 'pending'
RETURNING
    id, schedule_id, workspace_id, policy_id, scheduled_for,
    preview_id, run_id, status, created_at, completed_at
",
        )
        .bind(command.completed_at)
        .bind(command.occurrence_id)
        .fetch_optional(&self.pool)
        .await?;
        let row = match row {
            Some(row) => row,
            None => sqlx::query_as::<_, OccurrenceRow>(
                "SELECT
    id, schedule_id, workspace_id, policy_id, scheduled_for,
    preview_id, run_id, status, created_at, completed_at
FROM sync_schedule_occurrences
WHERE id = $1 AND status = 'completed'
",
            )
            .bind(command.occurrence_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(SyncScheduleRepositoryError::OccurrenceNotFound)?,
        };
        occurrence_snapshot(row)
    }

    async fn occurrence_run(&self, occurrence_id: Uuid, run_id: Uuid) -> ScheduleResult<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1
    FROM sync_runs
    WHERE schedule_occurrence_id = $1 AND id = $2
)
",
        )
        .bind(occurrence_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn replace_preview(
        &self,
        command: ReplacePendingSyncPreview,
    ) -> ScheduleResult<SyncScheduleOccurrenceSnapshot> {
        let row = sqlx::query_as::<_, OccurrenceRow>(
            "UPDATE sync_schedule_occurrences AS occurrences
SET preview_id = $1
WHERE
    occurrences.id = $2
    AND occurrences.preview_id = $3
    AND occurrences.status = 'pending'
    AND NOT EXISTS (
        SELECT 1
        FROM sync_runs
        WHERE schedule_occurrence_id = occurrences.id
    )
RETURNING
    id, schedule_id, workspace_id, policy_id, scheduled_for,
    preview_id, run_id, status, created_at, completed_at
",
        )
        .bind(command.replacement_preview_id)
        .bind(command.occurrence_id)
        .bind(command.current_preview_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SyncScheduleRepositoryError::OccurrenceNotFound)?;
        occurrence_snapshot(row)
    }
}

impl SyncScheduleRepository for PostgresSyncScheduleRepository {
    type Error = PostgresSyncScheduleRepositoryError;

    fn store_schedule(
        &self,
        command: StoreSyncSchedule,
    ) -> SyncScheduleFuture<'_, SyncScheduleSnapshot, Self::Error> {
        Box::pin(self.store(command))
    }

    fn get_policy_schedule(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> SyncScheduleFuture<'_, Option<SyncScheduleSnapshot>, Self::Error> {
        Box::pin(self.get(workspace_id, policy_id))
    }

    fn pause_schedule(
        &self,
        command: PauseSyncSchedule,
    ) -> SyncScheduleFuture<'_, SyncScheduleSnapshot, Self::Error> {
        Box::pin(self.pause(command))
    }

    fn materialize_due_schedule(
        &self,
        command: MaterializeDueSyncSchedule,
    ) -> SyncScheduleFuture<'_, Option<SyncScheduleOccurrenceSnapshot>, Self::Error> {
        Box::pin(self.materialize(command))
    }

    fn oldest_pending_occurrence(
        &self,
    ) -> SyncScheduleFuture<'_, Option<SyncScheduleOccurrenceSnapshot>, Self::Error> {
        Box::pin(self.pending())
    }

    fn complete_occurrence(
        &self,
        command: CompleteSyncScheduleOccurrence,
    ) -> SyncScheduleFuture<'_, SyncScheduleOccurrenceSnapshot, Self::Error> {
        Box::pin(self.complete(command))
    }

    fn occurrence_run_exists(
        &self,
        occurrence_id: Uuid,
        run_id: Uuid,
    ) -> SyncScheduleFuture<'_, bool, Self::Error> {
        Box::pin(self.occurrence_run(occurrence_id, run_id))
    }

    fn replace_pending_preview(
        &self,
        command: ReplacePendingSyncPreview,
    ) -> SyncScheduleFuture<'_, SyncScheduleOccurrenceSnapshot, Self::Error> {
        Box::pin(self.replace_preview(command))
    }
}

#[derive(FromRow)]
struct ScheduleRow {
    id: Uuid,
    workspace_id: Uuid,
    policy_id: Uuid,
    interval_seconds: i64,
    state: String,
    next_run_at: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[derive(FromRow)]
struct DueScheduleRow {
    id: Uuid,
    workspace_id: Uuid,
    policy_id: Uuid,
    interval_seconds: i64,
    next_run_at: OffsetDateTime,
}

#[derive(FromRow)]
struct OccurrenceRow {
    id: Uuid,
    schedule_id: Uuid,
    workspace_id: Uuid,
    policy_id: Uuid,
    scheduled_for: OffsetDateTime,
    preview_id: Uuid,
    run_id: Uuid,
    status: String,
    created_at: OffsetDateTime,
    completed_at: Option<OffsetDateTime>,
}

fn schedule_snapshot(row: ScheduleRow) -> ScheduleResult<SyncScheduleSnapshot> {
    let interval_seconds = u32::try_from(row.interval_seconds).map_err(|_| corrupt_schedule())?;
    let recurrence =
        SyncScheduleRecurrence::every(interval_seconds).map_err(|_| corrupt_schedule())?;
    let state = match row.state.as_str() {
        "enabled" => SyncScheduleState::Enabled,
        "paused" => SyncScheduleState::Paused,
        _ => return Err(corrupt_schedule()),
    };
    if (state == SyncScheduleState::Enabled) != row.next_run_at.is_some() {
        return Err(corrupt_schedule());
    }
    Ok(SyncScheduleSnapshot {
        schedule_id: row.id,
        workspace_id: row.workspace_id,
        policy_id: row.policy_id,
        recurrence,
        state,
        next_run_at: row.next_run_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn occurrence_snapshot(row: OccurrenceRow) -> ScheduleResult<SyncScheduleOccurrenceSnapshot> {
    let status = match row.status.as_str() {
        "pending" => SyncScheduleOccurrenceStatus::Pending,
        "completed" => SyncScheduleOccurrenceStatus::Completed,
        _ => return Err(corrupt_schedule()),
    };
    if (status == SyncScheduleOccurrenceStatus::Completed) != row.completed_at.is_some() {
        return Err(corrupt_schedule());
    }
    Ok(SyncScheduleOccurrenceSnapshot {
        occurrence_id: row.id,
        schedule_id: row.schedule_id,
        workspace_id: row.workspace_id,
        policy_id: row.policy_id,
        scheduled_for: row.scheduled_for,
        preview_id: row.preview_id,
        run_id: row.run_id,
        status,
        created_at: row.created_at,
        completed_at: row.completed_at,
    })
}

fn corrupt_schedule() -> SyncScheduleRepositoryError<PostgresSyncScheduleRepositoryError> {
    SyncScheduleRepositoryError::Repository(PostgresSyncScheduleRepositoryError::CorruptSchedule)
}

#[derive(Debug, Error)]
pub enum PostgresSyncScheduleRepositoryError {
    #[error("synchronization schedule database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("stored synchronization schedule is corrupt")]
    CorruptSchedule,
}

impl IntoApplicationError for PostgresSyncScheduleRepositoryError {
    fn into_application_error(self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::Internal,
            "synchronization schedule database operation failed",
        )
    }
}

impl From<sqlx::Error> for SyncScheduleRepositoryError<PostgresSyncScheduleRepositoryError> {
    fn from(error: sqlx::Error) -> Self {
        Self::Repository(PostgresSyncScheduleRepositoryError::Database(error))
    }
}
