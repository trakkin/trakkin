use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    application::acquisition::{
        AbandonRead, BeginRead, ProviderSourceKey, PublicationRepository, PublishRead,
        ReadAbandonment, ReadFence, ReadMode, RepositoryFuture, ResetSourceStream,
        StageCatalogBatch, StageStateBatch,
    },
    domain::acquisition::{
        CatalogBatch, CatalogItem, CatalogRelation, CatalogRelationKey, ProviderItemKey,
        StateBatch, StateObservation, StateSubject, StateValue,
    },
};

pub struct PostgresPublicationRepository {
    pool: PgPool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamKind {
    Catalog,
    State,
}

impl StreamKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::State => "state",
        }
    }

    fn parse(value: &str) -> Result<Self, PublicationError> {
        match value {
            "catalog" => Ok(Self::Catalog),
            "state" => Ok(Self::State),
            _ => Err(PublicationError::InvalidTask {
                reason: "task has an unknown source stream",
            }),
        }
    }
}

impl PostgresPublicationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn begin(
        &self,
        command: BeginRead,
        stream_kind: StreamKind,
    ) -> Result<ReadFence, PublicationError> {
        let mut transaction = self.pool.begin().await?;
        let task = sqlx::query_as::<_, TaskRow>(
            "SELECT status, cancellation_requested_at IS NOT NULL AS cancellation_requested
FROM tasks
WHERE id = $1
FOR UPDATE
",
        )
        .bind(command.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound { resource: "task" })?;
        ensure_task_publishable(&task)?;

        let read_task = sqlx::query_as::<_, ReadTaskRow>(
            "SELECT source_id, stream_kind, read_mode
FROM read_tasks
WHERE task_id = $1
",
        )
        .bind(command.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "read task",
        })?;
        if read_task.stream_kind != stream_kind.as_str() {
            return Err(PublicationError::InvalidTask {
                reason: "read task stream does not match the requested stream",
            });
        }
        let read_mode = parse_read_mode(&read_task.read_mode)?;

        ensure_claimed_running_attempt(
            &mut transaction,
            command.task_id,
            command.task_attempt_id,
            command.owner_id,
        )
        .await?;

        let stream = sqlx::query_as::<_, SourceStreamRow>(
            "SELECT status, cursor, publication_id
FROM source_streams
WHERE source_id = $1 AND stream_kind = $2
FOR UPDATE
",
        )
        .bind(read_task.source_id)
        .bind(stream_kind.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "source stream",
        })?;
        if read_mode == ReadMode::Incremental && stream.status != "available" {
            return Err(PublicationError::InvalidTask {
                reason: "incremental read requires an available publication",
            });
        }

        let source = sqlx::query_as::<_, ReadSourceRow>(
            "SELECT
    provider_key_namespace, provider_key_value, availability,
    capabilities, capability_revision
FROM sources
WHERE id = $1
FOR SHARE
",
        )
        .bind(read_task.source_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound { resource: "source" })?;
        ensure_read_supported(&source, stream_kind, read_mode)?;

        sqlx::query(
            r#"INSERT INTO read_attempts (
    id,
    task_id,
    task_attempt_id,
    owner_id,
    source_id,
    stream_kind,
    read_mode,
    status,
    expected_cursor,
    expected_publication_id,
    expected_capability_revision,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'staging', $8, $9, $10, $11, $11)
"#,
        )
        .bind(command.attempt_id)
        .bind(command.task_id)
        .bind(command.task_attempt_id)
        .bind(command.owner_id)
        .bind(read_task.source_id)
        .bind(stream_kind.as_str())
        .bind(&read_task.read_mode)
        .bind(&stream.cursor)
        .bind(stream.publication_id)
        .bind(source.capability_revision)
        .bind(command.started_at)
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        Ok(ReadFence {
            source_id: read_task.source_id,
            provider_source_key: ProviderSourceKey {
                namespace: source.provider_key_namespace,
                value: source.provider_key_value,
            },
            read_mode,
            expected_cursor: stream.cursor,
            expected_publication_id: stream.publication_id,
            expected_capability_revision: source.capability_revision,
        })
    }

    async fn abandon(&self, command: AbandonRead) -> Result<(), PublicationError> {
        let locator = sqlx::query_as::<_, AttemptLocator>(
            "SELECT task_id, task_attempt_id, owner_id, source_id
FROM read_attempts
WHERE id = $1
",
        )
        .bind(command.attempt_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "read attempt",
        })?;

        let mut transaction = self.pool.begin().await?;
        let task = sqlx::query_as::<_, TaskRow>(
            "SELECT status, cancellation_requested_at IS NOT NULL AS cancellation_requested
FROM tasks
WHERE id = $1
FOR UPDATE
",
        )
        .bind(locator.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound { resource: "task" })?;
        ensure_claimed_running_attempt(
            &mut transaction,
            locator.task_id,
            locator.task_attempt_id,
            locator.owner_id,
        )
        .await?;
        let attempt = sqlx::query_as::<_, AbandonmentAttemptRow>(
            "SELECT task_id, status
FROM read_attempts
WHERE id = $1
FOR UPDATE
",
        )
        .bind(command.attempt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "read attempt",
        })?;
        if attempt.task_id != locator.task_id || attempt.status != "staging" {
            return Err(PublicationError::AttemptNotStaging);
        }

        let (terminal_status, failure) = match &command.outcome {
            ReadAbandonment::Failed(_)
                if task.cancellation_requested || task.status == "cancelling" =>
            {
                ("cancelled", None)
            }
            ReadAbandonment::Failed(failure) => {
                if failure.code.is_empty() {
                    return Err(PublicationError::InvalidAbandonment {
                        reason: "failure code must not be empty",
                    });
                }
                if task.status != "running" || task.cancellation_requested {
                    return Err(PublicationError::TaskNotAbandonable);
                }
                (
                    "failed",
                    Some(json!({
                        "code": failure.code,
                        "retryable": failure.retryable,
                    })),
                )
            }
            ReadAbandonment::Cancelled => {
                if task.status != "running" && task.status != "cancelling" {
                    return Err(PublicationError::TaskNotAbandonable);
                }
                ("cancelled", None)
            }
        };

        clear_attempt_staging(&mut transaction, command.attempt_id).await?;
        let read_attempt = sqlx::query(
            "UPDATE read_attempts
SET status = 'abandoned', updated_at = $1, completed_at = $1
WHERE id = $2 AND status = 'staging'
",
        )
        .bind(command.completed_at)
        .bind(command.attempt_id)
        .execute(&mut *transaction)
        .await?;
        if read_attempt.rows_affected() != 1 {
            return Err(PublicationError::AttemptNotStaging);
        }

        let task_attempt = sqlx::query(
            "UPDATE task_attempts
SET status = $1, failure = $2, finished_at = $3
WHERE id = $4 AND task_id = $5 AND status = 'running'
",
        )
        .bind(terminal_status)
        .bind(&failure)
        .bind(command.completed_at)
        .bind(locator.task_attempt_id)
        .bind(locator.task_id)
        .execute(&mut *transaction)
        .await?;
        if task_attempt.rows_affected() != 1 {
            return Err(PublicationError::InvalidTask {
                reason: "abandonment must complete exactly one running task attempt",
            });
        }

        let task = sqlx::query(
            "UPDATE tasks
SET status = $1, failure = $2, finished_at = $3, updated_at = $3
WHERE id = $4
",
        )
        .bind(terminal_status)
        .bind(&failure)
        .bind(command.completed_at)
        .bind(locator.task_id)
        .execute(&mut *transaction)
        .await?;
        if task.rows_affected() != 1 {
            return Err(PublicationError::TaskNotAbandonable);
        }

        let claim = sqlx::query(
            "DELETE FROM task_claims WHERE task_id = $1 AND owner_id = $2
",
        )
        .bind(locator.task_id)
        .bind(locator.owner_id)
        .execute(&mut *transaction)
        .await?;
        if claim.rows_affected() != 1 {
            return Err(PublicationError::InvalidTask {
                reason: "abandonment must release exactly one task claim",
            });
        }

        transaction.commit().await?;
        Ok(())
    }

    async fn reset(&self, command: ResetSourceStream) -> Result<(), PublicationError> {
        let mut transaction = self.pool.begin().await?;
        let task = sqlx::query_as::<_, TaskRow>(
            "SELECT status, cancellation_requested_at IS NOT NULL AS cancellation_requested
FROM tasks
WHERE id = $1
FOR UPDATE
",
        )
        .bind(command.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound { resource: "task" })?;
        ensure_task_publishable(&task)?;
        ensure_claimed_running_attempt(
            &mut transaction,
            command.task_id,
            command.task_attempt_id,
            command.owner_id,
        )
        .await?;

        let reset_task = sqlx::query_as::<_, ResetTaskRow>(
            "SELECT source_id, stream_kind
FROM reset_tasks
WHERE task_id = $1
",
        )
        .bind(command.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "reset task",
        })?;
        let stream_kind = StreamKind::parse(&reset_task.stream_kind)?;
        let stream = sqlx::query(
            "SELECT 1
FROM source_streams
WHERE source_id = $1 AND stream_kind = $2
FOR UPDATE
",
        )
        .bind(reset_task.source_id)
        .bind(stream_kind.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if stream.is_none() {
            return Err(PublicationError::NotFound {
                resource: "source stream",
            });
        }
        let active_read = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1
    FROM read_attempts
    WHERE source_id = $1 AND stream_kind = $2 AND status = 'staging'
)
",
        )
        .bind(reset_task.source_id)
        .bind(stream_kind.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        if active_read {
            return Err(PublicationError::SourceStreamBusy);
        }

        match stream_kind {
            StreamKind::Catalog => {
                sqlx::query(
                    "DELETE FROM catalog_relations WHERE source_id = $1
",
                )
                .bind(reset_task.source_id)
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    "DELETE FROM catalog_items WHERE source_id = $1
",
                )
                .bind(reset_task.source_id)
                .execute(&mut *transaction)
                .await?;
            }
            StreamKind::State => {
                sqlx::query(
                    "DELETE FROM state_observations WHERE source_id = $1
",
                )
                .bind(reset_task.source_id)
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    "DELETE FROM state_observation_tombstones WHERE source_id = $1
",
                )
                .bind(reset_task.source_id)
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    "DELETE FROM state_publication_coverage WHERE source_id = $1
",
                )
                .bind(reset_task.source_id)
                .execute(&mut *transaction)
                .await?;
            }
        }

        let stream = sqlx::query(
            "UPDATE source_streams
SET
    status = 'unavailable',
    cursor = NULL,
    publication_id = NULL,
    evidence_revision = NULL,
    last_success_at = NULL,
    updated_at = $1
WHERE source_id = $2 AND stream_kind = $3
",
        )
        .bind(command.completed_at)
        .bind(reset_task.source_id)
        .bind(stream_kind.as_str())
        .execute(&mut *transaction)
        .await?;
        if stream.rows_affected() != 1 {
            return Err(PublicationError::NotFound {
                resource: "source stream",
            });
        }

        complete_successful_task(
            &mut transaction,
            command.task_id,
            command.task_attempt_id,
            command.owner_id,
            command.completed_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn stage_catalog(&self, command: StageCatalogBatch) -> Result<(), PublicationError> {
        validate_batch_uniqueness(&command.batch)?;
        let sequence = i64::try_from(command.batch.sequence()).map_err(|_| {
            PublicationError::InvalidStaging {
                reason: "catalog batch sequence exceeds PostgreSQL BIGINT".to_owned(),
            }
        })?;
        let mut transaction = self.pool.begin().await?;
        let attempt = sqlx::query_as::<_, StagingAttemptRow>(
            "SELECT status, stream_kind, next_batch_sequence
FROM read_attempts
WHERE id = $1
FOR UPDATE
",
        )
        .bind(command.attempt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "read attempt",
        })?;
        if attempt.status != "staging" || attempt.stream_kind != "catalog" {
            return Err(PublicationError::AttemptNotStaging);
        }
        if attempt.next_batch_sequence != sequence {
            return Err(PublicationError::UnexpectedBatchSequence {
                expected: attempt.next_batch_sequence,
                actual: sequence,
            });
        }

        for (row_ordinal, item) in command.batch.item_upserts().iter().enumerate() {
            stage_item_upsert(
                &mut transaction,
                command.attempt_id,
                sequence,
                ordinal(row_ordinal)?,
                item,
            )
            .await?;
        }
        let item_delete_offset = command.batch.item_upserts().len();
        for (row_ordinal, key) in command.batch.item_deletes().iter().enumerate() {
            stage_item_delete(
                &mut transaction,
                command.attempt_id,
                sequence,
                ordinal(item_delete_offset + row_ordinal)?,
                key,
            )
            .await?;
        }
        for (row_ordinal, relation) in command.batch.relation_upserts().iter().enumerate() {
            stage_relation_upsert(
                &mut transaction,
                command.attempt_id,
                sequence,
                ordinal(row_ordinal)?,
                relation,
            )
            .await?;
        }
        let relation_delete_offset = command.batch.relation_upserts().len();
        for (row_ordinal, key) in command.batch.relation_deletes().iter().enumerate() {
            stage_relation_delete(
                &mut transaction,
                command.attempt_id,
                sequence,
                ordinal(relation_delete_offset + row_ordinal)?,
                key,
            )
            .await?;
        }

        sqlx::query(
            "UPDATE read_attempts
SET
    next_batch_sequence = next_batch_sequence + 1,
    updated_at = GREATEST(updated_at, CURRENT_TIMESTAMP)
WHERE id = $1
",
        )
        .bind(command.attempt_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn stage_state(&self, command: StageStateBatch) -> Result<(), PublicationError> {
        validate_state_batch_uniqueness(&command.batch)?;
        let sequence = i64::try_from(command.batch.sequence()).map_err(|_| {
            PublicationError::InvalidStaging {
                reason: "state batch sequence exceeds PostgreSQL BIGINT".to_owned(),
            }
        })?;
        let mut transaction = self.pool.begin().await?;
        let attempt = sqlx::query_as::<_, StagingAttemptRow>(
            "SELECT status, stream_kind, next_batch_sequence
FROM read_attempts
WHERE id = $1
FOR UPDATE
",
        )
        .bind(command.attempt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "read attempt",
        })?;
        if attempt.status != "staging" || attempt.stream_kind != StreamKind::State.as_str() {
            return Err(PublicationError::AttemptNotStaging);
        }
        if attempt.next_batch_sequence != sequence {
            return Err(PublicationError::UnexpectedBatchSequence {
                expected: attempt.next_batch_sequence,
                actual: sequence,
            });
        }

        for (row_ordinal, observation) in command.batch.observations().iter().enumerate() {
            stage_state_observation(
                &mut transaction,
                command.attempt_id,
                sequence,
                ordinal(row_ordinal)?,
                observation,
            )
            .await?;
        }

        sqlx::query(
            "UPDATE read_attempts
SET
    next_batch_sequence = next_batch_sequence + 1,
    updated_at = GREATEST(updated_at, CURRENT_TIMESTAMP)
WHERE id = $1
",
        )
        .bind(command.attempt_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn publish(
        &self,
        command: PublishRead,
        stream_kind: StreamKind,
        read_mode: ReadMode,
    ) -> Result<(), PublicationError> {
        if command.evidence_revision.is_empty() {
            return Err(PublicationError::InvalidCompletion {
                reason: "evidence revision must not be empty",
            });
        }

        let locator = sqlx::query_as::<_, AttemptLocator>(
            "SELECT task_id, task_attempt_id, owner_id, source_id
FROM read_attempts
WHERE id = $1
",
        )
        .bind(command.attempt_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "read attempt",
        })?;

        let mut transaction = self.pool.begin().await?;
        let task = sqlx::query_as::<_, TaskRow>(
            "SELECT status, cancellation_requested_at IS NOT NULL AS cancellation_requested
FROM tasks
WHERE id = $1
FOR UPDATE
",
        )
        .bind(locator.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound { resource: "task" })?;
        ensure_task_publishable(&task)?;
        ensure_claimed_running_attempt(
            &mut transaction,
            locator.task_id,
            locator.task_attempt_id,
            locator.owner_id,
        )
        .await?;

        let stream = sqlx::query_as::<_, SourceStreamRow>(
            "SELECT status, cursor, publication_id
FROM source_streams
WHERE source_id = $1 AND stream_kind = $2
FOR UPDATE
",
        )
        .bind(locator.source_id)
        .bind(stream_kind.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "source stream",
        })?;

        let attempt = sqlx::query_as::<_, PublicationAttemptRow>(
            "SELECT
    task_id,
    source_id,
    stream_kind,
    read_mode,
    status,
    expected_cursor,
    expected_publication_id,
    expected_capability_revision
FROM read_attempts
WHERE id = $1
FOR UPDATE
",
        )
        .bind(command.attempt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound {
            resource: "read attempt",
        })?;
        if attempt.task_id != locator.task_id || attempt.source_id != locator.source_id {
            return Err(PublicationError::FenceChanged {
                field: "attempt ownership",
            });
        }
        if attempt.stream_kind != stream_kind.as_str()
            || parse_read_mode(&attempt.read_mode)? != read_mode
            || attempt.status != "staging"
        {
            return Err(PublicationError::AttemptNotStaging);
        }
        if read_mode == ReadMode::Incremental && stream.status != "available" {
            return Err(PublicationError::FenceChanged {
                field: "stream status",
            });
        }
        if stream.cursor != attempt.expected_cursor {
            return Err(PublicationError::FenceChanged { field: "cursor" });
        }
        if stream.publication_id != attempt.expected_publication_id {
            return Err(PublicationError::FenceChanged {
                field: "publication",
            });
        }
        if stream.publication_id == Some(command.publication_id) {
            return Err(PublicationError::InvalidCompletion {
                reason: "publication ID must advance",
            });
        }

        let capability_revision = sqlx::query_scalar::<_, i64>(
            "SELECT capability_revision
FROM sources
WHERE id = $1
FOR SHARE
",
        )
        .bind(locator.source_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PublicationError::NotFound { resource: "source" })?;
        if capability_revision != attempt.expected_capability_revision {
            return Err(PublicationError::FenceChanged {
                field: "capability revision",
            });
        }

        match (stream_kind, read_mode) {
            (StreamKind::Catalog, ReadMode::Full) => {
                validate_full_catalog_staging(&mut transaction, command.attempt_id).await?;
                replace_catalog_projection(&mut transaction, locator.source_id, &command).await?;
            }
            (StreamKind::Catalog, ReadMode::Incremental) => {
                apply_catalog_delta(&mut transaction, locator.source_id, &command).await?;
                validate_published_catalog(&mut transaction, locator.source_id).await?;
            }
            (StreamKind::State, ReadMode::Full) => {
                validate_full_state_staging(&mut transaction, command.attempt_id).await?;
                replace_state_projection(&mut transaction, locator.source_id, &command).await?;
            }
            (StreamKind::State, ReadMode::Incremental) => {
                let prior_publication_id =
                    stream
                        .publication_id
                        .ok_or(PublicationError::FenceChanged {
                            field: "state coverage",
                        })?;
                apply_state_delta(
                    &mut transaction,
                    locator.source_id,
                    prior_publication_id,
                    &command,
                )
                .await?;
            }
        }
        complete_publication(
            &mut transaction,
            locator.task_id,
            locator.task_attempt_id,
            locator.owner_id,
            locator.source_id,
            stream_kind,
            &command,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl PublicationRepository for PostgresPublicationRepository {
    type Error = PublicationError;

    fn abandon_read(&self, command: AbandonRead) -> RepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.abandon(command))
    }

    fn begin_catalog_read(
        &self,
        command: BeginRead,
    ) -> RepositoryFuture<'_, ReadFence, Self::Error> {
        Box::pin(self.begin(command, StreamKind::Catalog))
    }

    fn stage_catalog_batch(
        &self,
        command: StageCatalogBatch,
    ) -> RepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.stage_catalog(command))
    }

    fn publish_full_catalog(&self, command: PublishRead) -> RepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.publish(command, StreamKind::Catalog, ReadMode::Full))
    }

    fn publish_incremental_catalog(
        &self,
        command: PublishRead,
    ) -> RepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.publish(command, StreamKind::Catalog, ReadMode::Incremental))
    }

    fn begin_state_read(&self, command: BeginRead) -> RepositoryFuture<'_, ReadFence, Self::Error> {
        Box::pin(self.begin(command, StreamKind::State))
    }

    fn stage_state_batch(&self, command: StageStateBatch) -> RepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.stage_state(command))
    }

    fn publish_full_state(&self, command: PublishRead) -> RepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.publish(command, StreamKind::State, ReadMode::Full))
    }

    fn publish_incremental_state(
        &self,
        command: PublishRead,
    ) -> RepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.publish(command, StreamKind::State, ReadMode::Incremental))
    }

    fn reset_source_stream(
        &self,
        command: ResetSourceStream,
    ) -> RepositoryFuture<'_, (), Self::Error> {
        Box::pin(self.reset(command))
    }
}

async fn ensure_claimed_running_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    task_attempt_id: Uuid,
    owner_id: Uuid,
) -> Result<(), PublicationError> {
    let claimed = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
    SELECT 1 FROM task_claims WHERE task_id = $1 AND owner_id = $2
)
",
    )
    .bind(task_id)
    .bind(owner_id)
    .fetch_one(&mut **transaction)
    .await?;
    let running_attempt = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
    SELECT 1
    FROM task_attempts
    WHERE id = $2 AND task_id = $1 AND status = 'running'
)
",
    )
    .bind(task_id)
    .bind(task_attempt_id)
    .fetch_one(&mut **transaction)
    .await?;
    if !claimed || !running_attempt {
        return Err(PublicationError::InvalidTask {
            reason: "read requires a claim and running task attempt",
        });
    }
    Ok(())
}

fn ensure_task_publishable(task: &TaskRow) -> Result<(), PublicationError> {
    if task.status != "running" || task.cancellation_requested {
        return Err(PublicationError::TaskNotPublishable);
    }
    Ok(())
}

fn parse_read_mode(value: &str) -> Result<ReadMode, PublicationError> {
    match value {
        "full" => Ok(ReadMode::Full),
        "incremental" => Ok(ReadMode::Incremental),
        _ => Err(PublicationError::InvalidTask {
            reason: "read task has an unknown read mode",
        }),
    }
}

fn ensure_read_supported(
    source: &ReadSourceRow,
    stream_kind: StreamKind,
    read_mode: ReadMode,
) -> Result<(), PublicationError> {
    if source.availability != "available" {
        return Err(PublicationError::SourceUnavailable);
    }
    let mode = match read_mode {
        ReadMode::Full => "full",
        ReadMode::Incremental => "incremental",
    };
    let supported = source
        .capabilities
        .get(stream_kind.as_str())
        .and_then(|capability| capability.get(mode))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !supported {
        return Err(PublicationError::ReadUnsupported);
    }
    Ok(())
}

fn ordinal(value: usize) -> Result<i32, PublicationError> {
    i32::try_from(value).map_err(|_| PublicationError::InvalidStaging {
        reason: "batch row count exceeds PostgreSQL INTEGER".to_owned(),
    })
}

fn validate_batch_uniqueness(batch: &CatalogBatch) -> Result<(), PublicationError> {
    let mut item_keys = HashSet::new();
    for item in batch.item_upserts() {
        if !item_keys.insert(item.key().clone()) {
            return duplicate_staging_key("provider item");
        }
        let mut references = HashSet::new();
        for reference in item.portable_reference_candidates() {
            if !references.insert(reference.clone()) {
                return duplicate_staging_key("portable reference");
            }
        }
        let mut assets = HashSet::new();
        for asset in item.binary_assets() {
            if !assets.insert(asset.key().clone()) {
                return duplicate_staging_key("binary asset");
            }
        }
    }
    for key in batch.item_deletes() {
        if !item_keys.insert(key.clone()) {
            return duplicate_staging_key("provider item");
        }
    }

    let mut relation_keys = HashSet::new();
    for relation in batch.relation_upserts() {
        if !relation_keys.insert(relation.key().clone()) {
            return duplicate_staging_key("catalog relation");
        }
    }
    for key in batch.relation_deletes() {
        if !relation_keys.insert(key.clone()) {
            return duplicate_staging_key("catalog relation");
        }
    }
    Ok(())
}

fn duplicate_staging_key<T>(kind: &str) -> Result<T, PublicationError> {
    Err(PublicationError::InvalidStaging {
        reason: format!("catalog batch contains a duplicate {kind} key"),
    })
}

fn validate_state_batch_uniqueness(batch: &StateBatch) -> Result<(), PublicationError> {
    let mut observation_keys = HashSet::new();
    for observation in batch.observations() {
        let (subject_kind, subject_namespace, subject_value) = state_subject(observation.subject());
        let unit = observation.field().unit();
        let key = (
            subject_kind,
            subject_namespace,
            subject_value,
            observation.field().field().namespace(),
            observation.field().field().name(),
            unit.map_or("", |term| term.namespace()),
            unit.map_or("", |term| term.name()),
        );
        if !observation_keys.insert(key) {
            return Err(PublicationError::InvalidStaging {
                reason: "state batch contains a duplicate observation key".to_owned(),
            });
        }
    }
    Ok(())
}

async fn stage_state_observation(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
    sequence: i64,
    row_ordinal: i32,
    observation: &StateObservation,
) -> Result<(), PublicationError> {
    let (subject_kind, subject_namespace, subject_value) = state_subject(observation.subject());
    let unit = observation.field().unit();
    let value = observation.value().map(state_value_json);
    let operation = if value.is_some() { "set" } else { "delete" };

    sqlx::query(
        "INSERT INTO staged_state_observations (
    attempt_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    operation, value, provider_revision, observed_at,
    batch_sequence, row_ordinal
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14
)
ON CONFLICT (
    attempt_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name
) DO UPDATE SET
operation = excluded.operation,
value = excluded.value,
provider_revision = excluded.provider_revision,
observed_at = excluded.observed_at,
batch_sequence = excluded.batch_sequence,
row_ordinal = excluded.row_ordinal
",
    )
    .bind(attempt_id)
    .bind(subject_kind)
    .bind(subject_namespace)
    .bind(subject_value)
    .bind(observation.field().field().namespace())
    .bind(observation.field().field().name())
    .bind(unit.map_or("", |term| term.namespace()))
    .bind(unit.map_or("", |term| term.name()))
    .bind(operation)
    .bind(value)
    .bind(observation.provider_revision())
    .bind(observation.observed_at())
    .bind(sequence)
    .bind(row_ordinal)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn state_subject(subject: &StateSubject) -> (&'static str, &str, &[u8]) {
    match subject {
        StateSubject::ProviderItem(key) => ("provider_item", key.namespace(), key.value()),
        StateSubject::CatalogRelation(key) => ("catalog_relation", key.namespace(), key.value()),
    }
}

fn state_value_json(value: &StateValue) -> Value {
    match value {
        StateValue::Boolean(value) => json!({"kind": "boolean", "value": value}),
        StateValue::Integer(value) => json!({"kind": "integer", "value": value}),
        StateValue::Decimal(value) => json!({"kind": "decimal", "value": value}),
        StateValue::Text(value) => json!({"kind": "text", "value": value}),
        StateValue::Bytes(value) => json!({"kind": "bytes", "value": hex::encode(value)}),
        StateValue::TimestampMilliseconds(value) => {
            json!({"kind": "timestamp_milliseconds", "value": value})
        }
    }
}

async fn stage_item_upsert(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
    sequence: i64,
    row_ordinal: i32,
    item: &CatalogItem,
) -> Result<(), PublicationError> {
    clear_staged_item_children(transaction, attempt_id, item.key()).await?;
    sqlx::query(
        "INSERT INTO staged_catalog_items (
    attempt_id, key_namespace, key_value, operation, kind_namespace,
    kind_name, display_name, attributes, batch_sequence, row_ordinal
)
VALUES ($1, $2, $3, 'upsert', $4, $5, $6, $7, $8, $9)
ON CONFLICT (attempt_id, key_namespace, key_value) DO UPDATE SET
operation = excluded.operation,
kind_namespace = excluded.kind_namespace,
kind_name = excluded.kind_name,
display_name = excluded.display_name,
attributes = excluded.attributes,
batch_sequence = excluded.batch_sequence,
row_ordinal = excluded.row_ordinal
",
    )
    .bind(attempt_id)
    .bind(item.key().namespace())
    .bind(item.key().value())
    .bind(item.kind().namespace())
    .bind(item.kind().name())
    .bind(item.display_name())
    .bind(item.attributes())
    .bind(sequence)
    .bind(row_ordinal)
    .execute(&mut **transaction)
    .await?;

    for reference in item.portable_reference_candidates() {
        sqlx::query(
            "INSERT INTO staged_catalog_item_references (
    attempt_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value, recommended_mapping_root
)
VALUES ($1, $2, $3, $4, $5, $6)
",
        )
        .bind(attempt_id)
        .bind(item.key().namespace())
        .bind(item.key().value())
        .bind(reference.namespace())
        .bind(reference.value())
        .bind(item.recommended_mapping_roots().contains(reference))
        .execute(&mut **transaction)
        .await?;
    }
    for asset in item.binary_assets() {
        sqlx::query(
            "INSERT INTO staged_catalog_assets (
    attempt_id, item_key_namespace, item_key_value,
    asset_key_namespace, asset_key_value, kind_namespace, kind_name
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
",
        )
        .bind(attempt_id)
        .bind(item.key().namespace())
        .bind(item.key().value())
        .bind(asset.key().namespace())
        .bind(asset.key().value())
        .bind(asset.kind().namespace())
        .bind(asset.kind().name())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn stage_item_delete(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
    sequence: i64,
    row_ordinal: i32,
    key: &ProviderItemKey,
) -> Result<(), PublicationError> {
    clear_staged_item_children(transaction, attempt_id, key).await?;
    sqlx::query(
        "INSERT INTO staged_catalog_items (
    attempt_id, key_namespace, key_value, operation,
    batch_sequence, row_ordinal
)
VALUES ($1, $2, $3, 'delete', $4, $5)
ON CONFLICT (attempt_id, key_namespace, key_value) DO UPDATE SET
operation = excluded.operation,
kind_namespace = NULL,
kind_name = NULL,
display_name = NULL,
attributes = NULL,
batch_sequence = excluded.batch_sequence,
row_ordinal = excluded.row_ordinal
",
    )
    .bind(attempt_id)
    .bind(key.namespace())
    .bind(key.value())
    .bind(sequence)
    .bind(row_ordinal)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn clear_staged_item_children(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
    key: &ProviderItemKey,
) -> Result<(), PublicationError> {
    sqlx::query(
        "DELETE FROM staged_catalog_item_references
WHERE attempt_id = $1 AND item_key_namespace = $2 AND item_key_value = $3
",
    )
    .bind(attempt_id)
    .bind(key.namespace())
    .bind(key.value())
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM staged_catalog_assets
WHERE attempt_id = $1 AND item_key_namespace = $2 AND item_key_value = $3
",
    )
    .bind(attempt_id)
    .bind(key.namespace())
    .bind(key.value())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn stage_relation_upsert(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
    sequence: i64,
    row_ordinal: i32,
    relation: &CatalogRelation,
) -> Result<(), PublicationError> {
    sqlx::query(
        "INSERT INTO staged_catalog_relations (
    attempt_id, key_namespace, key_value, operation,
    parent_key_namespace, parent_key_value,
    item_key_namespace, item_key_value,
    kind_namespace, kind_name, order_parts, attributes,
    batch_sequence, row_ordinal
)
VALUES ($1, $2, $3, 'upsert', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (attempt_id, key_namespace, key_value) DO UPDATE SET
operation = excluded.operation,
parent_key_namespace = excluded.parent_key_namespace,
parent_key_value = excluded.parent_key_value,
item_key_namespace = excluded.item_key_namespace,
item_key_value = excluded.item_key_value,
kind_namespace = excluded.kind_namespace,
kind_name = excluded.kind_name,
order_parts = excluded.order_parts,
attributes = excluded.attributes,
batch_sequence = excluded.batch_sequence,
row_ordinal = excluded.row_ordinal
",
    )
    .bind(attempt_id)
    .bind(relation.key().namespace())
    .bind(relation.key().value())
    .bind(relation.parent_key().map(CatalogRelationKey::namespace))
    .bind(relation.parent_key().map(CatalogRelationKey::value))
    .bind(relation.provider_item_key().map(ProviderItemKey::namespace))
    .bind(relation.provider_item_key().map(ProviderItemKey::value))
    .bind(relation.kind().namespace())
    .bind(relation.kind().name())
    .bind(relation.order())
    .bind(relation.attributes())
    .bind(sequence)
    .bind(row_ordinal)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn stage_relation_delete(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
    sequence: i64,
    row_ordinal: i32,
    key: &CatalogRelationKey,
) -> Result<(), PublicationError> {
    sqlx::query(
        "INSERT INTO staged_catalog_relations (
    attempt_id, key_namespace, key_value, operation,
    batch_sequence, row_ordinal
)
VALUES ($1, $2, $3, 'delete', $4, $5)
ON CONFLICT (attempt_id, key_namespace, key_value) DO UPDATE SET
operation = excluded.operation,
parent_key_namespace = NULL,
parent_key_value = NULL,
item_key_namespace = NULL,
item_key_value = NULL,
kind_namespace = NULL,
kind_name = NULL,
order_parts = NULL,
attributes = NULL,
batch_sequence = excluded.batch_sequence,
row_ordinal = excluded.row_ordinal
",
    )
    .bind(attempt_id)
    .bind(key.namespace())
    .bind(key.value())
    .bind(sequence)
    .bind(row_ordinal)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn validate_full_catalog_staging(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
) -> Result<(), PublicationError> {
    let items = sqlx::query_as::<_, StagedItemRow>(
        "SELECT key_namespace, key_value, operation
FROM staged_catalog_items
WHERE attempt_id = $1
",
    )
    .bind(attempt_id)
    .fetch_all(&mut **transaction)
    .await?;
    if items.iter().any(|item| item.operation != "upsert") {
        return Err(PublicationError::InvalidStaging {
            reason: "full catalog staging must contain only item upserts".to_owned(),
        });
    }
    let item_keys = items
        .into_iter()
        .map(|item| KeyIdentity(item.key_namespace, item.key_value))
        .collect::<HashSet<_>>();

    let relations = sqlx::query_as::<_, StagedRelationRow>(
        "SELECT
    key_namespace,
    key_value,
    operation,
    parent_key_namespace,
    parent_key_value,
    item_key_namespace,
    item_key_value
FROM staged_catalog_relations
WHERE attempt_id = $1
",
    )
    .bind(attempt_id)
    .fetch_all(&mut **transaction)
    .await?;
    if relations
        .iter()
        .any(|relation| relation.operation != "upsert")
    {
        return Err(PublicationError::InvalidStaging {
            reason: "full catalog staging must contain only relation upserts".to_owned(),
        });
    }

    let relation_keys = relations
        .iter()
        .map(|relation| KeyIdentity(relation.key_namespace.clone(), relation.key_value.clone()))
        .collect::<HashSet<_>>();
    validate_catalog_hierarchy(item_keys, relation_keys, relations)
}

async fn validate_full_state_staging(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
) -> Result<(), PublicationError> {
    let contains_delete = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
    SELECT 1
    FROM staged_state_observations
    WHERE attempt_id = $1 AND operation = 'delete'
)
",
    )
    .bind(attempt_id)
    .fetch_one(&mut **transaction)
    .await?;
    if contains_delete {
        return Err(PublicationError::InvalidStaging {
            reason: "full state staging must contain only set observations".to_owned(),
        });
    }
    Ok(())
}

async fn validate_published_catalog(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
) -> Result<(), PublicationError> {
    let item_keys = sqlx::query_as::<_, StagedItemRow>(
        "SELECT key_namespace, key_value, 'upsert'::TEXT AS operation
FROM catalog_items
WHERE source_id = $1
",
    )
    .bind(source_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|item| KeyIdentity(item.key_namespace, item.key_value))
    .collect::<HashSet<_>>();
    let relations = sqlx::query_as::<_, StagedRelationRow>(
        "SELECT
    key_namespace,
    key_value,
    'upsert'::TEXT AS operation,
    parent_key_namespace,
    parent_key_value,
    item_key_namespace,
    item_key_value
FROM catalog_relations
WHERE source_id = $1
",
    )
    .bind(source_id)
    .fetch_all(&mut **transaction)
    .await?;
    let relation_keys = relations
        .iter()
        .map(|relation| KeyIdentity(relation.key_namespace.clone(), relation.key_value.clone()))
        .collect::<HashSet<_>>();
    validate_catalog_hierarchy(item_keys, relation_keys, relations)
}

fn validate_catalog_hierarchy(
    item_keys: HashSet<KeyIdentity>,
    relation_keys: HashSet<KeyIdentity>,
    relations: Vec<StagedRelationRow>,
) -> Result<(), PublicationError> {
    let mut parents = HashMap::new();
    for relation in relations {
        let key = KeyIdentity(relation.key_namespace, relation.key_value);
        let parent = optional_key(relation.parent_key_namespace, relation.parent_key_value)?;
        let item = optional_key(relation.item_key_namespace, relation.item_key_value)?;
        if parent
            .as_ref()
            .is_some_and(|parent_key| !relation_keys.contains(parent_key))
        {
            return Err(PublicationError::InvalidStaging {
                reason: "catalog relation has a missing parent relation".to_owned(),
            });
        }
        if item
            .as_ref()
            .is_some_and(|item_key| !item_keys.contains(item_key))
        {
            return Err(PublicationError::InvalidStaging {
                reason: "catalog relation has a missing provider item".to_owned(),
            });
        }
        parents.insert(key, parent);
    }
    ensure_acyclic_hierarchy(&parents)
}

fn optional_key(
    namespace: Option<String>,
    value: Option<Vec<u8>>,
) -> Result<Option<KeyIdentity>, PublicationError> {
    match (namespace, value) {
        (Some(namespace), Some(value)) => Ok(Some(KeyIdentity(namespace, value))),
        (None, None) => Ok(None),
        _ => Err(PublicationError::InvalidStaging {
            reason: "catalog relation contains a partial key".to_owned(),
        }),
    }
}

fn ensure_acyclic_hierarchy(
    parents: &HashMap<KeyIdentity, Option<KeyIdentity>>,
) -> Result<(), PublicationError> {
    for relation_key in parents.keys() {
        let mut path = HashSet::new();
        let mut current = Some(relation_key);
        while let Some(key) = current {
            if !path.insert(key) {
                return Err(PublicationError::InvalidStaging {
                    reason: "catalog relation hierarchy contains a cycle".to_owned(),
                });
            }
            current = parents.get(key).and_then(Option::as_ref);
        }
    }
    Ok(())
}

async fn replace_catalog_projection(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    command: &PublishRead,
) -> Result<(), PublicationError> {
    sqlx::query(
        "DELETE FROM catalog_relations WHERE source_id = $1
",
    )
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM catalog_items WHERE source_id = $1
",
    )
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        "INSERT INTO catalog_items (
    source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, publication_id, updated_at
)
SELECT
    $1 AS source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, $2 AS publication_id, $3 AS updated_at
FROM staged_catalog_items
WHERE attempt_id = $4 AND operation = 'upsert'
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(command.published_at)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog_item_references (
    source_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value, recommended_mapping_root
)
SELECT
    $1 AS source_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value, recommended_mapping_root
FROM staged_catalog_item_references
WHERE attempt_id = $2
",
    )
    .bind(source_id)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog_assets (
    source_id, item_key_namespace, item_key_value,
    asset_key_namespace, asset_key_value, kind_namespace, kind_name,
    publication_id
)
SELECT
    $1 AS source_id, item_key_namespace, item_key_value,
    asset_key_namespace, asset_key_value, kind_namespace, kind_name,
    $2 AS publication_id
FROM staged_catalog_assets
WHERE attempt_id = $3
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog_relations (
    source_id, key_namespace, key_value,
    parent_key_namespace, parent_key_value,
    item_key_namespace, item_key_value,
    kind_namespace, kind_name, order_parts, attributes,
    publication_id, updated_at
)
SELECT
    $1 AS source_id, key_namespace, key_value,
    parent_key_namespace, parent_key_value,
    item_key_namespace, item_key_value,
    kind_namespace, kind_name, order_parts, attributes,
    $2 AS publication_id, $3 AS updated_at
FROM staged_catalog_relations
WHERE attempt_id = $4 AND operation = 'upsert'
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(command.published_at)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn apply_catalog_delta(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    command: &PublishRead,
) -> Result<(), PublicationError> {
    sqlx::query(
        "DELETE FROM catalog_relations AS published
USING staged_catalog_relations AS staged
WHERE
    staged.attempt_id = $1
    AND staged.operation = 'delete'
    AND published.source_id = $2
    AND published.key_namespace = staged.key_namespace
    AND published.key_value = staged.key_value
",
    )
    .bind(command.attempt_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM catalog_items AS published
USING staged_catalog_items AS staged
WHERE
    staged.attempt_id = $1
    AND staged.operation = 'delete'
    AND published.source_id = $2
    AND published.key_namespace = staged.key_namespace
    AND published.key_value = staged.key_value
",
    )
    .bind(command.attempt_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        "DELETE FROM catalog_item_references AS published
USING staged_catalog_items AS staged
WHERE
    staged.attempt_id = $1
    AND staged.operation = 'upsert'
    AND published.source_id = $2
    AND published.item_key_namespace = staged.key_namespace
    AND published.item_key_value = staged.key_value
",
    )
    .bind(command.attempt_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM catalog_assets AS published
USING staged_catalog_items AS staged
WHERE
    staged.attempt_id = $1
    AND staged.operation = 'upsert'
    AND published.source_id = $2
    AND published.item_key_namespace = staged.key_namespace
    AND published.item_key_value = staged.key_value
",
    )
    .bind(command.attempt_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        "INSERT INTO catalog_items (
    source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, publication_id, updated_at
)
SELECT
    $1 AS source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, $2 AS publication_id, $3 AS updated_at
FROM staged_catalog_items
WHERE attempt_id = $4 AND operation = 'upsert'
ON CONFLICT (source_id, key_namespace, key_value) DO UPDATE SET
kind_namespace = excluded.kind_namespace,
kind_name = excluded.kind_name,
display_name = excluded.display_name,
attributes = excluded.attributes,
publication_id = excluded.publication_id,
updated_at = excluded.updated_at
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(command.published_at)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog_item_references (
    source_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value, recommended_mapping_root
)
SELECT
    $1 AS source_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value, recommended_mapping_root
FROM staged_catalog_item_references
WHERE attempt_id = $2
",
    )
    .bind(source_id)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog_assets (
    source_id, item_key_namespace, item_key_value,
    asset_key_namespace, asset_key_value, kind_namespace, kind_name,
    publication_id
)
SELECT
    $1 AS source_id, item_key_namespace, item_key_value,
    asset_key_namespace, asset_key_value, kind_namespace, kind_name,
    $2 AS publication_id
FROM staged_catalog_assets
WHERE attempt_id = $3
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO catalog_relations (
    source_id, key_namespace, key_value,
    parent_key_namespace, parent_key_value,
    item_key_namespace, item_key_value,
    kind_namespace, kind_name, order_parts, attributes,
    publication_id, updated_at
)
SELECT
    $1 AS source_id, key_namespace, key_value,
    parent_key_namespace, parent_key_value,
    item_key_namespace, item_key_value,
    kind_namespace, kind_name, order_parts, attributes,
    $2 AS publication_id, $3 AS updated_at
FROM staged_catalog_relations
WHERE attempt_id = $4 AND operation = 'upsert'
ON CONFLICT (source_id, key_namespace, key_value) DO UPDATE SET
parent_key_namespace = excluded.parent_key_namespace,
parent_key_value = excluded.parent_key_value,
item_key_namespace = excluded.item_key_namespace,
item_key_value = excluded.item_key_value,
kind_namespace = excluded.kind_namespace,
kind_name = excluded.kind_name,
order_parts = excluded.order_parts,
attributes = excluded.attributes,
publication_id = excluded.publication_id,
updated_at = excluded.updated_at
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(command.published_at)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;

    sqlx::query(
        "UPDATE catalog_items SET publication_id = $1 WHERE source_id = $2
",
    )
    .bind(command.publication_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE catalog_assets SET publication_id = $1 WHERE source_id = $2
",
    )
    .bind(command.publication_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE catalog_relations SET publication_id = $1 WHERE source_id = $2
",
    )
    .bind(command.publication_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn replace_state_projection(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    command: &PublishRead,
) -> Result<(), PublicationError> {
    sqlx::query(
        "DELETE FROM state_observations WHERE source_id = $1
",
    )
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM state_observation_tombstones WHERE source_id = $1
",
    )
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    insert_state_upserts(transaction, source_id, command).await?;
    sqlx::query(
        "INSERT INTO state_publication_coverage (
    source_id,
    baseline_publication_id,
    publication_id,
    evidence_revision,
    observed_at,
    updated_at
) VALUES ($1, $2, $2, $3, $4, $5)
ON CONFLICT (source_id) DO UPDATE SET
baseline_publication_id = excluded.baseline_publication_id,
publication_id = excluded.publication_id,
evidence_revision = excluded.evidence_revision,
observed_at = excluded.observed_at,
updated_at = excluded.updated_at
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(&command.evidence_revision)
    .bind(command.observed_at)
    .bind(command.published_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn apply_state_delta(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    prior_publication_id: Uuid,
    command: &PublishRead,
) -> Result<(), PublicationError> {
    sqlx::query(
        "DELETE FROM state_observations AS published
USING staged_state_observations AS staged
WHERE
    staged.attempt_id = $1
    AND staged.operation = 'delete'
    AND published.source_id = $2
    AND published.subject_kind = staged.subject_kind
    AND published.subject_key_namespace = staged.subject_key_namespace
    AND published.subject_key_value = staged.subject_key_value
    AND published.field_namespace = staged.field_namespace
    AND published.field_name = staged.field_name
    AND published.unit_namespace = staged.unit_namespace
    AND published.unit_name = staged.unit_name
",
    )
    .bind(command.attempt_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM state_observation_tombstones AS published
USING staged_state_observations AS staged
WHERE
    staged.attempt_id = $1
    AND staged.operation = 'set'
    AND published.source_id = $2
    AND published.subject_kind = staged.subject_kind
    AND published.subject_key_namespace = staged.subject_key_namespace
    AND published.subject_key_value = staged.subject_key_value
    AND published.field_namespace = staged.field_namespace
    AND published.field_name = staged.field_name
    AND published.unit_namespace = staged.unit_namespace
    AND published.unit_name = staged.unit_name
",
    )
    .bind(command.attempt_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    insert_state_upserts(transaction, source_id, command).await?;
    sqlx::query(
        "INSERT INTO state_observation_tombstones (
    source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    provider_revision, observed_at, publication_id, updated_at
)
SELECT
    $1 AS source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    provider_revision, observed_at, $2 AS publication_id, $3 AS updated_at
FROM staged_state_observations
WHERE attempt_id = $4 AND operation = 'delete'
ON CONFLICT (
    source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name
) DO UPDATE SET
provider_revision = excluded.provider_revision,
observed_at = excluded.observed_at,
publication_id = excluded.publication_id,
updated_at = excluded.updated_at
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(command.published_at)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE state_observations SET publication_id = $1 WHERE source_id = $2
",
    )
    .bind(command.publication_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE state_observation_tombstones
SET publication_id = $1
WHERE source_id = $2
",
    )
    .bind(command.publication_id)
    .bind(source_id)
    .execute(&mut **transaction)
    .await?;
    let coverage = sqlx::query(
        "UPDATE state_publication_coverage
SET
    publication_id = $1,
    evidence_revision = $2,
    observed_at = $3,
    updated_at = $4
WHERE source_id = $5 AND publication_id = $6
",
    )
    .bind(command.publication_id)
    .bind(&command.evidence_revision)
    .bind(command.observed_at)
    .bind(command.published_at)
    .bind(source_id)
    .bind(prior_publication_id)
    .execute(&mut **transaction)
    .await?;
    if coverage.rows_affected() != 1 {
        return Err(PublicationError::FenceChanged {
            field: "state coverage",
        });
    }
    Ok(())
}

async fn insert_state_upserts(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    command: &PublishRead,
) -> Result<(), PublicationError> {
    sqlx::query(
        "INSERT INTO state_observations (
    source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    value, provider_revision, observed_at, publication_id, updated_at
)
SELECT
    $1 AS source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    value, provider_revision, observed_at,
    $2 AS publication_id, $3 AS updated_at
FROM staged_state_observations
WHERE attempt_id = $4 AND operation = 'set'
ON CONFLICT (
    source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name
) DO UPDATE SET
value = excluded.value,
provider_revision = excluded.provider_revision,
observed_at = excluded.observed_at,
publication_id = excluded.publication_id,
updated_at = excluded.updated_at
",
    )
    .bind(source_id)
    .bind(command.publication_id)
    .bind(command.published_at)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn complete_publication(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    task_attempt_id: Uuid,
    owner_id: Uuid,
    source_id: Uuid,
    stream_kind: StreamKind,
    command: &PublishRead,
) -> Result<(), PublicationError> {
    sqlx::query(
        "UPDATE source_streams
SET
    status = 'available',
    cursor = $1,
    publication_id = $2,
    evidence_revision = $3,
    last_success_at = $4,
    updated_at = $5
WHERE source_id = $6 AND stream_kind = $7
",
    )
    .bind(&command.next_cursor)
    .bind(command.publication_id)
    .bind(&command.evidence_revision)
    .bind(command.observed_at)
    .bind(command.published_at)
    .bind(source_id)
    .bind(stream_kind.as_str())
    .execute(&mut **transaction)
    .await?;

    clear_attempt_staging(transaction, command.attempt_id).await?;
    sqlx::query(
        "UPDATE read_attempts
SET
    status = 'published',
    next_cursor = $1,
    evidence_revision = $2,
    observed_at = $3,
    updated_at = $4,
    completed_at = $4
WHERE id = $5
",
    )
    .bind(&command.next_cursor)
    .bind(&command.evidence_revision)
    .bind(command.observed_at)
    .bind(command.published_at)
    .bind(command.attempt_id)
    .execute(&mut **transaction)
    .await?;

    complete_successful_task(
        transaction,
        task_id,
        task_attempt_id,
        owner_id,
        command.published_at,
    )
    .await
}

async fn complete_successful_task(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    task_attempt_id: Uuid,
    owner_id: Uuid,
    completed_at: time::OffsetDateTime,
) -> Result<(), PublicationError> {
    let task_attempt = sqlx::query(
        "UPDATE task_attempts
SET status = 'succeeded', finished_at = $1
WHERE id = $2 AND task_id = $3 AND status = 'running'
",
    )
    .bind(completed_at)
    .bind(task_attempt_id)
    .bind(task_id)
    .execute(&mut **transaction)
    .await?;
    if task_attempt.rows_affected() != 1 {
        return Err(PublicationError::InvalidTask {
            reason: "task must complete exactly one running task attempt",
        });
    }

    let task = sqlx::query(
        "UPDATE tasks
SET status = 'succeeded', finished_at = $1, updated_at = $1
WHERE id = $2 AND status = 'running' AND cancellation_requested_at IS NULL
",
    )
    .bind(completed_at)
    .bind(task_id)
    .execute(&mut **transaction)
    .await?;
    if task.rows_affected() != 1 {
        return Err(PublicationError::TaskNotPublishable);
    }

    let claim = sqlx::query(
        "DELETE FROM task_claims WHERE task_id = $1 AND owner_id = $2
",
    )
    .bind(task_id)
    .bind(owner_id)
    .execute(&mut **transaction)
    .await?;
    if claim.rows_affected() != 1 {
        return Err(PublicationError::InvalidTask {
            reason: "task must release exactly one claim",
        });
    }
    Ok(())
}

async fn clear_attempt_staging(
    transaction: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
) -> Result<(), PublicationError> {
    sqlx::query(
        "DELETE FROM staged_catalog_relations WHERE attempt_id = $1
",
    )
    .bind(attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM staged_catalog_items WHERE attempt_id = $1
",
    )
    .bind(attempt_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "DELETE FROM staged_state_observations WHERE attempt_id = $1
",
    )
    .bind(attempt_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum PublicationError {
    #[error("{resource} was not found")]
    NotFound { resource: &'static str },
    #[error("read task is invalid: {reason}")]
    InvalidTask { reason: &'static str },
    #[error("read task cannot publish in its current state")]
    TaskNotPublishable,
    #[error("read task cannot be abandoned with the requested outcome")]
    TaskNotAbandonable,
    #[error("source stream has an active staged read")]
    SourceStreamBusy,
    #[error("source is unavailable for reads")]
    SourceUnavailable,
    #[error("source does not support the requested read stream and mode")]
    ReadUnsupported,
    #[error("read attempt is not staging the requested stream and mode")]
    AttemptNotStaging,
    #[error("read batch sequence mismatch: expected {expected}, received {actual}")]
    UnexpectedBatchSequence { expected: i64, actual: i64 },
    #[error("publication fence changed: {field}")]
    FenceChanged { field: &'static str },
    #[error("publication staging is invalid: {reason}")]
    InvalidStaging { reason: String },
    #[error("publication completion is invalid: {reason}")]
    InvalidCompletion { reason: &'static str },
    #[error("read abandonment is invalid: {reason}")]
    InvalidAbandonment { reason: &'static str },
    #[error("publication database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(FromRow)]
struct TaskRow {
    status: String,
    cancellation_requested: bool,
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
struct SourceStreamRow {
    status: String,
    cursor: Option<Vec<u8>>,
    publication_id: Option<Uuid>,
}

#[derive(FromRow)]
struct ReadSourceRow {
    provider_key_namespace: String,
    provider_key_value: Vec<u8>,
    availability: String,
    capabilities: Value,
    capability_revision: i64,
}

#[derive(FromRow)]
struct StagingAttemptRow {
    status: String,
    stream_kind: String,
    next_batch_sequence: i64,
}

#[derive(FromRow)]
struct AttemptLocator {
    task_id: Uuid,
    task_attempt_id: Uuid,
    owner_id: Uuid,
    source_id: Uuid,
}

#[derive(FromRow)]
struct AbandonmentAttemptRow {
    task_id: Uuid,
    status: String,
}

#[derive(FromRow)]
struct PublicationAttemptRow {
    task_id: Uuid,
    source_id: Uuid,
    stream_kind: String,
    read_mode: String,
    status: String,
    expected_cursor: Option<Vec<u8>>,
    expected_publication_id: Option<Uuid>,
    expected_capability_revision: i64,
}

#[derive(FromRow)]
struct StagedItemRow {
    key_namespace: String,
    key_value: Vec<u8>,
    operation: String,
}

#[derive(FromRow)]
struct StagedRelationRow {
    key_namespace: String,
    key_value: Vec<u8>,
    operation: String,
    parent_key_namespace: Option<String>,
    parent_key_value: Option<Vec<u8>>,
    item_key_namespace: Option<String>,
    item_key_value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct KeyIdentity(String, Vec<u8>);
