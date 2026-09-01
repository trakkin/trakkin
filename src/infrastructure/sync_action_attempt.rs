use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::{
            AdapterCallError, AdapterFailureCategory, AdapterFieldProblem, AdapterKey,
            AdapterRetryAfter, AdapterSourceMembership, AdapterTargetedStateFieldEffect,
            AdapterTargetedStateFieldEffectKind, AdapterTargetedStateFieldWriteCapability,
            AdapterTargetedStateMembershipEffect, AdapterTargetedStateWriteCapability,
            AdapterTargetedStateWriteCertainty, AdapterTargetedStateWriteIdempotencyMode,
            AdapterTargetedStateWriteOutcome, AdapterTargetedStateWritePreconditionMode,
            AdapterTargetedStateWriteRetryDisposition, AdapterTargetedStateWriteStatus,
        },
        sync_action_attempt::{
            CheckSyncActionCancellation, FailSyncActionClaim,
            MAXIMUM_AUTOMATIC_SYNC_ACTION_ATTEMPTS, PrepareSyncActionAttempt,
            RecordSyncActionDispatch, RecordSyncActionOutcome, RecordSyncActionUncertainty,
            SyncActionAttemptError, SyncActionAttemptFuture, SyncActionAttemptRepository,
            SyncActionAttemptSnapshot, SyncActionAttemptState, SyncActionDispatchTransition,
            SyncActionOutcomeDisposition, SyncActionOutcomeSettlement, SyncActionRecoveryResult,
            SyncActionWriteRequest,
        },
        task_manager::{TaskCompletion, TaskDefinition},
    },
    domain::acquisition::{
        CatalogRelationKey, ProviderItemKey, StateField, StateSubject, StateValue, Term,
    },
    infrastructure::sync_execution::reduce_sync_run,
};

#[derive(Clone)]
pub struct PostgresSyncActionAttemptRepository {
    pool: PgPool,
}

impl PostgresSyncActionAttemptRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn fail_claim(
        &self,
        command: FailSyncActionClaim,
    ) -> Result<(), SyncActionAttemptError<PostgresSyncActionAttemptError>> {
        let mut transaction = self.pool.begin().await?;
        let claim = sqlx::query_as::<_, FailedClaimRow>(
            "SELECT
    tasks.status AS task_status,
    task_attempts.status AS task_attempt_status,
    task_claims.owner_id,
    sync_actions.id AS action_id,
    sync_runs.id AS run_id
FROM tasks
INNER JOIN task_attempts ON task_attempts.task_id = tasks.id
INNER JOIN task_claims ON task_claims.task_id = tasks.id
INNER JOIN sync_action_tasks ON sync_action_tasks.task_id = tasks.id
INNER JOIN sync_actions ON sync_actions.id = sync_action_tasks.action_id
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE tasks.id = $1 AND task_attempts.id = $2
FOR UPDATE OF
tasks,
task_attempts,
task_claims,
sync_actions,
sync_runs
",
        )
        .bind(command.claimed.task.id)
        .bind(command.claimed.attempt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SyncActionAttemptError::InvalidClaim)?;
        if !matches!(claim.task_status.as_str(), "running" | "cancelling")
            || claim.task_attempt_status != "running"
            || claim.owner_id != command.claimed.owner_id
        {
            return Err(SyncActionAttemptError::InvalidClaim);
        }
        let action_status = if claim.task_status == "cancelling" {
            "cancelled"
        } else {
            "failed"
        };
        let action = sqlx::query(
            "UPDATE sync_actions
SET status = $1, finished_at = $2, updated_at = $2
WHERE id = $3 AND status IN ('queued', 'retryable_failure')
",
        )
        .bind(action_status)
        .bind(command.failed_at)
        .bind(claim.action_id)
        .execute(&mut *transaction)
        .await?;
        if action.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let completion = TaskCompletion::Failed(command.failure);
        terminalize_task(
            &mut transaction,
            &claim.task_status,
            command.claimed.task.id,
            command.claimed.attempt_id,
            command.claimed.owner_id,
            Some(&completion),
            command.failed_at,
        )
        .await?;
        if !reduce_sync_run(&mut transaction, claim.run_id, command.failed_at).await? {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn prepare(
        &self,
        command: PrepareSyncActionAttempt,
    ) -> Result<SyncActionAttemptSnapshot, SyncActionAttemptError<PostgresSyncActionAttemptError>>
    {
        let TaskDefinition::SyncAction {
            action_id,
            receiver_source_id,
        } = &command.claimed.task.definition
        else {
            return Err(SyncActionAttemptError::InvalidClaim);
        };
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, PreparationRow>(
            "SELECT
    tasks.status AS task_status,
    task_claims.owner_id,
    task_attempts.status AS task_attempt_status,
    task_attempts.started_at,
    tasks.connection_id AS task_connection_id,
    sync_actions.preview_action_hash,
    preview_actions.receiver_subject_kind,
    preview_actions.receiver_subject_namespace,
    preview_actions.receiver_subject_value,
    preview_actions.semantic_field,
    preview_actions.canonical_json,
    sources.connection_id AS source_connection_id,
    sources.provider_key_namespace AS source_key_namespace,
    sources.provider_key_value AS source_key_value,
    sources.availability AS source_availability,
    sources.capabilities AS source_capabilities
FROM tasks
INNER JOIN task_claims ON task_claims.task_id = tasks.id
INNER JOIN task_attempts ON task_attempts.task_id = tasks.id
INNER JOIN sync_action_tasks ON sync_action_tasks.task_id = tasks.id
INNER JOIN sync_actions ON sync_actions.id = sync_action_tasks.action_id
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
INNER JOIN sources ON sources.id = sync_action_tasks.receiver_source_id
INNER JOIN sync_preview_actions AS preview_actions
    ON
        preview_actions.preview_id = sync_actions.preview_id
        AND preview_actions.action_hash = sync_actions.preview_action_hash
WHERE
    tasks.id = $1
    AND task_attempts.id = $2
    AND sync_action_tasks.action_id = $3
    AND sync_action_tasks.receiver_source_id = $4
FOR UPDATE OF tasks, task_claims, task_attempts, sync_actions, sync_runs
",
        )
        .bind(command.claimed.task.id)
        .bind(command.claimed.attempt_id)
        .bind(action_id)
        .bind(receiver_source_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SyncActionAttemptError::InvalidClaim)?;
        if !matches!(row.task_status.as_str(), "running" | "cancelling")
            || row.task_attempt_status != "running"
            || row.owner_id != command.claimed.owner_id
            || row.started_at > command.prepared_at
            || row.task_connection_id != row.source_connection_id
            || row.source_availability != "available"
        {
            return Err(SyncActionAttemptError::InvalidClaim);
        }
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1 FROM sync_action_attempts WHERE task_attempt_id = $1
)
",
        )
        .bind(command.claimed.attempt_id)
        .fetch_one(&mut *transaction)
        .await?
        {
            return Err(SyncActionAttemptError::AttemptAlreadyPrepared);
        }
        let request = parse_request(*action_id, *receiver_source_id, command.operation_id, &row)?;
        let write_capability = row
            .source_capabilities
            .get("targetedStateWrite")
            .cloned()
            .ok_or(SyncActionAttemptError::CorruptAction)?;
        let (subject_kind, subject_namespace, subject_value) =
            subject_parts(&request.receiver_subject);
        sqlx::query(
            "INSERT INTO sync_action_attempts (
    task_attempt_id,
    task_id,
    action_id,
    receiver_source_id,
    owner_id,
    operation_id,
    idempotency_key,
    preview_action_hash,
    receiver_subject_kind,
    receiver_subject_namespace,
    receiver_subject_value,
    semantic_field,
    provider_field_namespace,
    provider_field_name,
    provider_unit_namespace,
    provider_unit_name,
    intended_value,
    expected_membership,
    precondition,
    allow_create_membership,
    source_key_namespace,
    source_key_value,
    write_capability,
    state,
    prepared_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23,
    'prepared', $24
)
",
        )
        .bind(command.claimed.attempt_id)
        .bind(command.claimed.task.id)
        .bind(request.action_id)
        .bind(request.receiver_source_id)
        .bind(command.claimed.owner_id)
        .bind(request.operation_id)
        .bind(request.idempotency_key)
        .bind(&request.preview_action_hash)
        .bind(subject_kind)
        .bind(subject_namespace)
        .bind(subject_value)
        .bind(&request.semantic_field)
        .bind(request.provider_field.field().namespace())
        .bind(request.provider_field.field().name())
        .bind(request.provider_field.unit().map(Term::namespace))
        .bind(request.provider_field.unit().map(Term::name))
        .bind(request.intended_value)
        .bind(membership_name(request.expected_membership))
        .bind(&request.precondition)
        .bind(request.allow_create_membership)
        .bind(&request.source_key.namespace)
        .bind(&request.source_key.value)
        .bind(write_capability)
        .bind(command.prepared_at)
        .execute(&mut *transaction)
        .await?;
        let action = sqlx::query(
            "UPDATE sync_actions
SET
    status = 'running',
    started_at = coalesce(started_at, $1),
    finished_at = NULL,
    updated_at = $1
WHERE id = $2 AND status IN ('queued', 'retryable_failure')
",
        )
        .bind(command.prepared_at)
        .bind(action_id)
        .execute(&mut *transaction)
        .await?;
        if action.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let run = sqlx::query(
            "UPDATE sync_runs
SET
    status = 'running',
    started_at = coalesce(started_at, $1),
    updated_at = greatest(updated_at, $1)
WHERE
    id = (SELECT run_id FROM sync_actions WHERE id = $2)
    AND status IN ('queued', 'running')
",
        )
        .bind(command.prepared_at)
        .bind(action_id)
        .execute(&mut *transaction)
        .await?;
        if run.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        transaction.commit().await?;
        Ok(SyncActionAttemptSnapshot {
            task_id: command.claimed.task.id,
            task_attempt_id: command.claimed.attempt_id,
            owner_id: command.claimed.owner_id,
            request,
            state: SyncActionAttemptState::Prepared,
            recovery_result: None,
            outcome: None,
            prepared_at: command.prepared_at,
            dispatched_at: None,
            settled_at: None,
        })
    }

    async fn record_dispatch(
        &self,
        command: RecordSyncActionDispatch,
    ) -> Result<SyncActionDispatchTransition, SyncActionAttemptError<PostgresSyncActionAttemptError>>
    {
        let mut transaction = self.pool.begin().await?;
        let claim = sqlx::query_as::<_, DispatchFenceRow>(
            "SELECT
    tasks.status AS task_status,
    task_attempts.status AS task_attempt_status,
    task_claims.owner_id,
    sync_action_attempts.state,
    sync_action_attempts.prepared_at,
    sync_actions.id AS action_id,
    sync_runs.id AS run_id
FROM sync_action_attempts
INNER JOIN tasks ON tasks.id = sync_action_attempts.task_id
INNER JOIN task_attempts
    ON task_attempts.id = sync_action_attempts.task_attempt_id
INNER JOIN task_claims ON task_claims.task_id = sync_action_attempts.task_id
INNER JOIN sync_actions ON sync_actions.id = sync_action_attempts.action_id
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE
    sync_action_attempts.task_id = $1
    AND sync_action_attempts.task_attempt_id = $2
FOR UPDATE OF
tasks,
task_attempts,
task_claims,
sync_action_attempts,
sync_actions,
sync_runs
",
        )
        .bind(command.task_id)
        .bind(command.task_attempt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SyncActionAttemptError::InvalidClaim)?;
        if !matches!(claim.task_status.as_str(), "running" | "cancelling")
            || claim.task_attempt_status != "running"
            || claim.owner_id != command.owner_id
            || claim.state != "prepared"
            || command.dispatched_at < claim.prepared_at
        {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        if claim.task_status == "cancelling" {
            let attempt = sqlx::query(
                "UPDATE sync_action_attempts
SET
    state = 'settled',
    settlement_kind = 'recovery',
    recovery_result = 'confirmed_not_applied',
    settled_at = $1
WHERE task_attempt_id = $2 AND state = 'prepared'
",
            )
            .bind(command.dispatched_at)
            .bind(command.task_attempt_id)
            .execute(&mut *transaction)
            .await?;
            if attempt.rows_affected() != 1 {
                return Err(SyncActionAttemptError::InvalidTransition);
            }
            let action = sqlx::query(
                "UPDATE sync_actions
SET status = 'cancelled', finished_at = $1, updated_at = $1
WHERE id = $2 AND status = 'running'
",
            )
            .bind(command.dispatched_at)
            .bind(claim.action_id)
            .execute(&mut *transaction)
            .await?;
            if action.rows_affected() != 1 {
                return Err(SyncActionAttemptError::InvalidTransition);
            }
            terminalize_task(
                &mut transaction,
                &claim.task_status,
                command.task_id,
                command.task_attempt_id,
                command.owner_id,
                None,
                command.dispatched_at,
            )
            .await?;
            if !reduce_sync_run(&mut transaction, claim.run_id, command.dispatched_at).await? {
                return Err(SyncActionAttemptError::InvalidTransition);
            }
            let snapshot = load_attempt(&mut transaction, command.task_attempt_id).await?;
            transaction.commit().await?;
            return Ok(SyncActionDispatchTransition::Cancelled(snapshot));
        }
        let updated = sqlx::query(
            "UPDATE sync_action_attempts
SET state = 'dispatched', dispatched_at = $1
WHERE task_attempt_id = $2 AND state = 'prepared'
",
        )
        .bind(command.dispatched_at)
        .bind(command.task_attempt_id)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let snapshot = load_attempt(&mut transaction, command.task_attempt_id).await?;
        transaction.commit().await?;
        Ok(SyncActionDispatchTransition::Dispatched(snapshot))
    }

    async fn record_outcome(
        &self,
        command: RecordSyncActionOutcome,
    ) -> Result<SyncActionOutcomeSettlement, SyncActionAttemptError<PostgresSyncActionAttemptError>>
    {
        let mut transaction = self.pool.begin().await?;
        let claim =
            lock_outcome_fence(&mut transaction, command.task_id, command.task_attempt_id).await?;
        if !matches!(claim.task_status.as_str(), "running" | "cancelling")
            || claim.task_attempt_status != "running"
            || claim.owner_id != command.owner_id
            || claim.state != "dispatched"
            || command.settled_at < claim.dispatched_at
        {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        for (ordinal, effect) in command.outcome.field_effects.iter().enumerate() {
            let value = StoredStateValue::from(effect.value.as_ref());
            sqlx::query(
                "INSERT INTO sync_action_attempt_field_effects (
    task_attempt_id, ordinal, field_namespace, field_name,
    unit_namespace, unit_name, effect, value_kind, boolean_value,
    integer_value, decimal_value, text_value, bytes_value,
    timestamp_milliseconds
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
)
",
            )
            .bind(command.task_attempt_id)
            .bind(i32::try_from(ordinal).map_err(|_| SyncActionAttemptError::RepositoryInvariant)?)
            .bind(effect.field.field().namespace())
            .bind(effect.field.field().name())
            .bind(effect.field.unit().map(Term::namespace))
            .bind(effect.field.unit().map(Term::name))
            .bind(field_effect_name(effect.effect))
            .bind(value.kind)
            .bind(value.boolean)
            .bind(value.integer)
            .bind(value.decimal)
            .bind(value.text)
            .bind(value.bytes)
            .bind(value.timestamp_milliseconds)
            .execute(&mut *transaction)
            .await?;
        }
        let error = command.outcome.error.as_ref();
        if let Some(error) = error {
            for (ordinal, problem) in error.field_problems.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO sync_action_attempt_error_field_problems (
    task_attempt_id, ordinal, path, code, message
) VALUES ($1, $2, $3, $4, $5)
",
                )
                .bind(command.task_attempt_id)
                .bind(
                    i32::try_from(ordinal)
                        .map_err(|_| SyncActionAttemptError::RepositoryInvariant)?,
                )
                .bind(&problem.path)
                .bind(&problem.code)
                .bind(&problem.message)
                .execute(&mut *transaction)
                .await?;
            }
        }
        let retry_after = error.and_then(|error| error.retry_after.as_ref());
        let updated = sqlx::query(
            "UPDATE sync_action_attempts
SET
    state = 'settled',
    settlement_kind = 'provider_outcome',
    outcome_status = $1,
    outcome_certainty = $2,
    outcome_retry_disposition = $3,
    outcome_membership_effect = $4,
    outcome_provider_revision = $5,
    outcome_successor_precondition = $6,
    outcome_receipt = $7,
    outcome_error_category = $8,
    outcome_error_code = $9,
    outcome_error_retryable = $10,
    outcome_error_safe_message = $11,
    outcome_error_diagnostic_id = $12,
    outcome_error_retry_after_seconds = $13,
    outcome_error_retry_after_nanoseconds = $14,
    settled_at = $15
WHERE task_attempt_id = $16 AND state = 'dispatched'
",
        )
        .bind(write_status_name(command.outcome.status))
        .bind(write_certainty_name(command.outcome.certainty))
        .bind(write_retry_name(command.outcome.retry_disposition))
        .bind(membership_effect_name(command.outcome.membership_effect))
        .bind(&command.outcome.provider_revision)
        .bind(&command.outcome.successor_precondition)
        .bind(&command.outcome.receipt)
        .bind(error.map(|error| failure_category_name(error.category)))
        .bind(error.map(|error| &error.code))
        .bind(error.map(|error| error.retryable))
        .bind(error.map(|error| &error.safe_message))
        .bind(error.map(|error| &error.diagnostic_id))
        .bind(retry_after.map(|retry_after| retry_after.seconds))
        .bind(retry_after.map(|retry_after| retry_after.nanoseconds))
        .bind(command.settled_at)
        .bind(command.task_attempt_id)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let retry_queued = claim.task_status == "running"
            && command.outcome.status == AdapterTargetedStateWriteStatus::Failed
            && command.outcome.retry_disposition
                == AdapterTargetedStateWriteRetryDisposition::SafeSameKey
            && claim.task_attempt_ordinal + 1 < MAXIMUM_AUTOMATIC_SYNC_ACTION_ATTEMPTS;
        let action = sqlx::query(
            "UPDATE sync_actions
SET status = $1, finished_at = $2, updated_at = $2
WHERE id = (
    SELECT action_id
    FROM sync_action_attempts
    WHERE task_attempt_id = $3
)
AND status = 'running'
",
        )
        .bind(action_status(&command.outcome, retry_queued))
        .bind(command.settled_at)
        .bind(command.task_attempt_id)
        .execute(&mut *transaction)
        .await?;
        if action.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        if retry_queued {
            requeue_task(
                &mut transaction,
                command.task_id,
                command.task_attempt_id,
                command.owner_id,
                &command.completion,
                command.settled_at,
            )
            .await?;
        } else {
            terminalize_task(
                &mut transaction,
                &claim.task_status,
                command.task_id,
                command.task_attempt_id,
                command.owner_id,
                Some(&command.completion),
                command.settled_at,
            )
            .await?;
            if command.outcome.status != AdapterTargetedStateWriteStatus::Indeterminate {
                let provenance = sqlx::query(
                    "INSERT INTO sync_action_provenance (
    action_id, terminal_task_attempt_id, recorded_at
) VALUES ($1, $2, $3)
",
                )
                .bind(claim.action_id)
                .bind(command.task_attempt_id)
                .bind(command.settled_at)
                .execute(&mut *transaction)
                .await?;
                if provenance.rows_affected() != 1 {
                    return Err(SyncActionAttemptError::InvalidTransition);
                }
            }
        }
        if !reduce_sync_run(&mut transaction, claim.run_id, command.settled_at).await? {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let snapshot = load_attempt(&mut transaction, command.task_attempt_id).await?;
        transaction.commit().await?;
        Ok(SyncActionOutcomeSettlement {
            attempt: snapshot,
            disposition: if retry_queued {
                SyncActionOutcomeDisposition::RetryQueued
            } else {
                SyncActionOutcomeDisposition::Terminalized
            },
        })
    }

    async fn cancellation_requested(
        &self,
        command: CheckSyncActionCancellation,
    ) -> Result<bool, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
        let status = sqlx::query_scalar::<_, String>(
            "SELECT tasks.status
FROM sync_action_attempts
INNER JOIN tasks ON tasks.id = sync_action_attempts.task_id
INNER JOIN task_attempts
    ON task_attempts.id = sync_action_attempts.task_attempt_id
INNER JOIN task_claims ON task_claims.task_id = sync_action_attempts.task_id
WHERE
    sync_action_attempts.task_id = $1
    AND sync_action_attempts.task_attempt_id = $2
    AND sync_action_attempts.state = 'dispatched'
    AND task_attempts.status = 'running'
    AND task_claims.owner_id = $3
",
        )
        .bind(command.task_id)
        .bind(command.task_attempt_id)
        .bind(command.owner_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SyncActionAttemptError::InvalidClaim)?;
        match status.as_str() {
            "running" => Ok(false),
            "cancelling" => Ok(true),
            _ => Err(SyncActionAttemptError::InvalidTransition),
        }
    }

    async fn record_uncertainty(
        &self,
        command: RecordSyncActionUncertainty,
    ) -> Result<SyncActionAttemptSnapshot, SyncActionAttemptError<PostgresSyncActionAttemptError>>
    {
        let mut transaction = self.pool.begin().await?;
        let claim =
            lock_outcome_fence(&mut transaction, command.task_id, command.task_attempt_id).await?;
        if !matches!(claim.task_status.as_str(), "running" | "cancelling")
            || claim.task_attempt_status != "running"
            || claim.owner_id != command.owner_id
            || claim.state != "dispatched"
            || command.settled_at < claim.dispatched_at
        {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let attempt = sqlx::query(
            "UPDATE sync_action_attempts
SET
    state = 'settled',
    settlement_kind = 'recovery',
    recovery_result = 'indeterminate',
    settled_at = $1
WHERE task_attempt_id = $2 AND state = 'dispatched'
",
        )
        .bind(command.settled_at)
        .bind(command.task_attempt_id)
        .execute(&mut *transaction)
        .await?;
        if attempt.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let action = sqlx::query(
            "UPDATE sync_actions
SET status = 'indeterminate', finished_at = $1, updated_at = $1
WHERE id = $2 AND status = 'running'
",
        )
        .bind(command.settled_at)
        .bind(claim.action_id)
        .execute(&mut *transaction)
        .await?;
        if action.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let invocation_context =
            crate::observability::task_invocation_context(command.reconciliation_task_id);
        let task = sqlx::query(
            "INSERT INTO tasks (
    id, workspace_id, connection_id, kind, status, cancellation_supported,
    invocation_context, created_at, updated_at
)
VALUES ($1, $2, $3, 'sync_action_reconciliation', 'queued', FALSE, $4, $5, $5)
",
        )
        .bind(command.reconciliation_task_id)
        .bind(claim.workspace_id)
        .bind(claim.connection_id)
        .bind(super::task_manager::invocation_context_json(
            &invocation_context,
        ))
        .bind(command.settled_at)
        .execute(&mut *transaction)
        .await?;
        if task.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let reconciliation = sqlx::query(
            "INSERT INTO sync_action_reconciliation_tasks (
    task_id, action_id, receiver_source_id, write_task_attempt_id
)
VALUES ($1, $2, $3, $4)
",
        )
        .bind(command.reconciliation_task_id)
        .bind(claim.action_id)
        .bind(claim.receiver_source_id)
        .bind(command.task_attempt_id)
        .execute(&mut *transaction)
        .await?;
        if reconciliation.rows_affected() != 1 {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let completion =
            TaskCompletion::Failed(crate::application::task_manager::TaskFailure::coded(
                "sync_action_write_indeterminate",
                false,
            ));
        terminalize_task(
            &mut transaction,
            &claim.task_status,
            command.task_id,
            command.task_attempt_id,
            command.owner_id,
            Some(&completion),
            command.settled_at,
        )
        .await?;
        if !reduce_sync_run(&mut transaction, claim.run_id, command.settled_at).await? {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let snapshot = load_attempt(&mut transaction, command.task_attempt_id).await?;
        transaction.commit().await?;
        Ok(snapshot)
    }
}

impl SyncActionAttemptRepository for PostgresSyncActionAttemptRepository {
    type Error = PostgresSyncActionAttemptError;

    fn prepare_sync_action_attempt(
        &self,
        command: PrepareSyncActionAttempt,
    ) -> SyncActionAttemptFuture<'_, SyncActionAttemptSnapshot, Self::Error> {
        Box::pin(self.prepare(command))
    }

    fn fail_sync_action_claim(
        &self,
        command: FailSyncActionClaim,
    ) -> SyncActionAttemptFuture<'_, (), Self::Error> {
        Box::pin(self.fail_claim(command))
    }

    fn record_sync_action_dispatch(
        &self,
        command: RecordSyncActionDispatch,
    ) -> SyncActionAttemptFuture<'_, SyncActionDispatchTransition, Self::Error> {
        Box::pin(self.record_dispatch(command))
    }

    fn record_sync_action_outcome(
        &self,
        command: RecordSyncActionOutcome,
    ) -> SyncActionAttemptFuture<'_, SyncActionOutcomeSettlement, Self::Error> {
        Box::pin(self.record_outcome(command))
    }

    fn check_sync_action_cancellation(
        &self,
        command: CheckSyncActionCancellation,
    ) -> SyncActionAttemptFuture<'_, bool, Self::Error> {
        Box::pin(self.cancellation_requested(command))
    }

    fn record_sync_action_uncertainty(
        &self,
        command: RecordSyncActionUncertainty,
    ) -> SyncActionAttemptFuture<'_, SyncActionAttemptSnapshot, Self::Error> {
        Box::pin(self.record_uncertainty(command))
    }
}

async fn lock_outcome_fence(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    task_attempt_id: Uuid,
) -> Result<OutcomeFenceRow, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    sqlx::query_as::<_, OutcomeFenceRow>(
        "SELECT
    tasks.status AS task_status,
    task_attempts.status AS task_attempt_status,
    task_attempts.ordinal AS task_attempt_ordinal,
    task_claims.owner_id,
    sync_action_attempts.state,
    sync_action_attempts.dispatched_at,
    sync_actions.id AS action_id,
    sync_actions.receiver_source_id,
    sync_actions.workspace_id,
    tasks.connection_id,
    sync_runs.id AS run_id
FROM sync_action_attempts
INNER JOIN tasks ON tasks.id = sync_action_attempts.task_id
INNER JOIN task_attempts
    ON task_attempts.id = sync_action_attempts.task_attempt_id
INNER JOIN task_claims ON task_claims.task_id = sync_action_attempts.task_id
INNER JOIN sync_actions ON sync_actions.id = sync_action_attempts.action_id
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE
    sync_action_attempts.task_id = $1
    AND sync_action_attempts.task_attempt_id = $2
FOR UPDATE OF
tasks,
task_attempts,
task_claims,
sync_action_attempts,
sync_actions,
sync_runs
",
    )
    .bind(task_id)
    .bind(task_attempt_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(SyncActionAttemptError::InvalidClaim)
}

fn action_status(outcome: &AdapterTargetedStateWriteOutcome, retry_queued: bool) -> &'static str {
    match outcome.status {
        AdapterTargetedStateWriteStatus::Applied => "applied",
        AdapterTargetedStateWriteStatus::NoOp => "no_op",
        AdapterTargetedStateWriteStatus::Rejected
            if outcome
                .error
                .as_ref()
                .is_some_and(|error| error.code == "precondition_failed") =>
        {
            "precondition_failed"
        }
        AdapterTargetedStateWriteStatus::Rejected => "failed",
        AdapterTargetedStateWriteStatus::Failed if retry_queued => "retryable_failure",
        AdapterTargetedStateWriteStatus::Failed => "failed",
        AdapterTargetedStateWriteStatus::Indeterminate => "indeterminate",
    }
}

async fn requeue_task(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    task_attempt_id: Uuid,
    owner_id: Uuid,
    completion: &TaskCompletion,
    completed_at: time::OffsetDateTime,
) -> Result<(), SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    let TaskCompletion::Failed(failure) = completion else {
        return Err(SyncActionAttemptError::InvalidTransition);
    };
    if failure.code.is_empty() {
        return Err(SyncActionAttemptError::InvalidTransition);
    }
    let failure = super::task_manager::failure_json(failure);
    let attempt = sqlx::query(
        "UPDATE task_attempts
SET status = 'failed', failure = $1, finished_at = $2
WHERE id = $3 AND task_id = $4 AND status = 'running'
",
    )
    .bind(&failure)
    .bind(completed_at)
    .bind(task_attempt_id)
    .bind(task_id)
    .execute(&mut **transaction)
    .await?;
    if attempt.rows_affected() != 1 {
        return Err(SyncActionAttemptError::InvalidTransition);
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
        return Err(SyncActionAttemptError::InvalidTransition);
    }
    let task = sqlx::query(
        "UPDATE tasks
SET status = 'queued', failure = NULL, finished_at = NULL, updated_at = $1
WHERE id = $2 AND status = 'running'
",
    )
    .bind(completed_at)
    .bind(task_id)
    .execute(&mut **transaction)
    .await?;
    if task.rows_affected() != 1 {
        return Err(SyncActionAttemptError::InvalidTransition);
    }
    Ok(())
}

async fn terminalize_task(
    transaction: &mut Transaction<'_, Postgres>,
    task_status: &str,
    task_id: Uuid,
    task_attempt_id: Uuid,
    owner_id: Uuid,
    completion: Option<&TaskCompletion>,
    completed_at: time::OffsetDateTime,
) -> Result<(), SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    let (terminal_status, failure) = if task_status == "cancelling" {
        ("cancelled", None)
    } else {
        match completion {
            Some(TaskCompletion::Succeeded) => ("succeeded", None),
            Some(TaskCompletion::Failed(failure)) if !failure.code.is_empty() => {
                ("failed", Some(super::task_manager::failure_json(failure)))
            }
            Some(TaskCompletion::Failed(_) | TaskCompletion::Cancelled) | None => {
                return Err(SyncActionAttemptError::InvalidTransition);
            }
        }
    };
    let attempt = sqlx::query(
        "UPDATE task_attempts
SET status = $1, failure = $2, finished_at = $3
WHERE id = $4 AND task_id = $5 AND status = 'running'
",
    )
    .bind(terminal_status)
    .bind(&failure)
    .bind(completed_at)
    .bind(task_attempt_id)
    .bind(task_id)
    .execute(&mut **transaction)
    .await?;
    if attempt.rows_affected() != 1 {
        return Err(SyncActionAttemptError::InvalidTransition);
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
        return Err(SyncActionAttemptError::InvalidTransition);
    }
    let task = sqlx::query(
        "UPDATE tasks
SET status = $1, failure = $2, finished_at = $3, updated_at = $3
WHERE id = $4 AND status IN ('running', 'cancelling')
",
    )
    .bind(terminal_status)
    .bind(failure)
    .bind(completed_at)
    .bind(task_id)
    .execute(&mut **transaction)
    .await?;
    if task.rows_affected() != 1 {
        return Err(SyncActionAttemptError::InvalidTransition);
    }
    Ok(())
}

fn parse_request(
    action_id: Uuid,
    receiver_source_id: Uuid,
    operation_id: Uuid,
    row: &PreparationRow,
) -> Result<SyncActionWriteRequest, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    if format!("sha256:{:x}", Sha256::digest(&row.canonical_json)) != row.preview_action_hash {
        return Err(SyncActionAttemptError::CorruptAction);
    }
    let action: Value = serde_json::from_slice(&row.canonical_json)
        .map_err(|_| SyncActionAttemptError::CorruptAction)?;
    if action["receiverSourceId"] != receiver_source_id.to_string()
        || action["semanticField"] != row.semantic_field
        || action["receiverSubject"]["kind"] != row.receiver_subject_kind
        || action["receiverSubject"]["namespace"] != row.receiver_subject_namespace
        || action["receiverSubject"]["value"]
            .as_array()
            .and_then(|values| json_bytes(values))
            .as_deref()
            != Some(row.receiver_subject_value.as_slice())
    {
        return Err(SyncActionAttemptError::CorruptAction);
    }
    let expected_membership = match action["targetedState"]["membership"].as_str() {
        Some("present") => AdapterSourceMembership::Present,
        Some("absent") => AdapterSourceMembership::Absent,
        _ => return Err(SyncActionAttemptError::CorruptAction),
    };
    let allow_create_membership = match action["membershipEffect"].as_str() {
        Some("create") => true,
        Some("update") => false,
        _ => return Err(SyncActionAttemptError::CorruptAction),
    };
    if (expected_membership == AdapterSourceMembership::Absent) != allow_create_membership {
        return Err(SyncActionAttemptError::CorruptAction);
    }
    let targeted_fields = action["targetedState"]["fields"]
        .as_array()
        .filter(|fields| fields.len() == 1)
        .ok_or(SyncActionAttemptError::CorruptAction)?;
    let provider_field = parse_state_field(&targeted_fields[0]["field"])?;
    let intended_value = action["intendedValue"]
        .as_u64()
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(SyncActionAttemptError::CorruptAction)?;
    let precondition = match &action["targetedState"]["precondition"] {
        Value::Null => None,
        Value::Array(values) => {
            Some(json_bytes(values).ok_or(SyncActionAttemptError::CorruptAction)?)
        }
        _ => return Err(SyncActionAttemptError::CorruptAction),
    };
    let capability = parse_write_capability(&row.source_capabilities)?;
    if !capability
        .fields
        .iter()
        .any(|field| field.field == provider_field && field.set_supported)
        || (allow_create_membership && !capability.may_create_source_membership)
        || capability.idempotency_mode != Some(AdapterTargetedStateWriteIdempotencyMode::StableKey)
    {
        return Err(SyncActionAttemptError::CorruptAction);
    }
    Ok(SyncActionWriteRequest {
        action_id,
        preview_action_hash: row.preview_action_hash.clone(),
        receiver_source_id,
        receiver_subject: parse_subject(
            &row.receiver_subject_kind,
            row.receiver_subject_namespace.clone(),
            row.receiver_subject_value.clone(),
        )?,
        semantic_field: row.semantic_field.clone(),
        provider_field,
        intended_value,
        expected_membership,
        precondition,
        allow_create_membership,
        idempotency_key: action_id,
        operation_id,
        source_key: AdapterKey {
            namespace: row.source_key_namespace.clone(),
            value: row.source_key_value.clone(),
        },
        capability,
    })
}

fn parse_write_capability(
    capabilities: &Value,
) -> Result<
    AdapterTargetedStateWriteCapability,
    SyncActionAttemptError<PostgresSyncActionAttemptError>,
> {
    let capability = capabilities
        .get("targetedStateWrite")
        .filter(|capability| capability.is_object())
        .ok_or(SyncActionAttemptError::CorruptAction)?;
    let fields = capability["fields"]
        .as_array()
        .ok_or(SyncActionAttemptError::CorruptAction)?
        .iter()
        .map(
            |field| -> Result<_, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
                Ok(AdapterTargetedStateFieldWriteCapability {
                    field: parse_state_field(field)?,
                    set_supported: field["setSupported"]
                        .as_bool()
                        .ok_or(SyncActionAttemptError::CorruptAction)?,
                    clear_supported: field["clearSupported"]
                        .as_bool()
                        .ok_or(SyncActionAttemptError::CorruptAction)?,
                })
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    let precondition_mode = match capability["preconditionMode"].as_str() {
        Some("provider_token") => Some(AdapterTargetedStateWritePreconditionMode::ProviderToken),
        Some("host_recheck_only") => {
            Some(AdapterTargetedStateWritePreconditionMode::HostRecheckOnly)
        }
        None => None,
        _ => return Err(SyncActionAttemptError::CorruptAction),
    };
    let idempotency_mode = match capability["idempotencyMode"].as_str() {
        Some("not_supported") => Some(AdapterTargetedStateWriteIdempotencyMode::NotSupported),
        Some("stable_key") => Some(AdapterTargetedStateWriteIdempotencyMode::StableKey),
        None => None,
        _ => return Err(SyncActionAttemptError::CorruptAction),
    };
    Ok(AdapterTargetedStateWriteCapability {
        fields,
        may_create_source_membership: capability["mayCreateSourceMembership"]
            .as_bool()
            .ok_or(SyncActionAttemptError::CorruptAction)?,
        precondition_mode,
        idempotency_mode,
        maximum_fields: json_u32(capability, "maximumFields")?,
        maximum_request_bytes: json_u64(capability, "maximumRequestBytes")?,
        maximum_response_bytes: json_u64(capability, "maximumResponseBytes")?,
        maximum_receipt_bytes: json_u64(capability, "maximumReceiptBytes")?,
    })
}

fn json_u32(
    value: &Value,
    field: &str,
) -> Result<u32, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    value[field]
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(SyncActionAttemptError::CorruptAction)
}

fn json_u64(
    value: &Value,
    field: &str,
) -> Result<u64, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    value[field]
        .as_u64()
        .ok_or(SyncActionAttemptError::CorruptAction)
}

fn parse_state_field(
    value: &Value,
) -> Result<StateField, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    let field = parse_term(&value["field"])?;
    let unit = match &value["unit"] {
        Value::Null => None,
        unit => Some(parse_term(unit)?),
    };
    Ok(StateField::new(field, unit))
}

fn parse_term(
    value: &Value,
) -> Result<Term, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    Term::new(
        value["namespace"]
            .as_str()
            .ok_or(SyncActionAttemptError::CorruptAction)?,
        value["name"]
            .as_str()
            .ok_or(SyncActionAttemptError::CorruptAction)?,
    )
    .map_err(|_| SyncActionAttemptError::CorruptAction)
}

fn parse_subject(
    kind: &str,
    namespace: String,
    value: Vec<u8>,
) -> Result<StateSubject, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    match kind {
        "provider_item" => ProviderItemKey::new(namespace, value)
            .map(StateSubject::ProviderItem)
            .map_err(|_| SyncActionAttemptError::CorruptAction),
        "catalog_relation" => CatalogRelationKey::new(namespace, value)
            .map(StateSubject::CatalogRelation)
            .map_err(|_| SyncActionAttemptError::CorruptAction),
        _ => Err(SyncActionAttemptError::CorruptAction),
    }
}

fn subject_parts(subject: &StateSubject) -> (&'static str, &str, &[u8]) {
    match subject {
        StateSubject::ProviderItem(key) => ("provider_item", key.namespace(), key.value()),
        StateSubject::CatalogRelation(key) => ("catalog_relation", key.namespace(), key.value()),
    }
}

fn json_bytes(values: &[Value]) -> Option<Vec<u8>> {
    values
        .iter()
        .map(|value| value.as_u64().and_then(|value| u8::try_from(value).ok()))
        .collect()
}

pub(crate) async fn load_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    task_attempt_id: Uuid,
) -> Result<SyncActionAttemptSnapshot, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    let row = sqlx::query_as::<_, SyncActionAttemptRow>(
        "SELECT
    task_attempt_id,
    task_id,
    action_id,
    receiver_source_id,
    owner_id,
    operation_id,
    idempotency_key,
    preview_action_hash,
    receiver_subject_kind,
    receiver_subject_namespace,
    receiver_subject_value,
    semantic_field,
    provider_field_namespace,
    provider_field_name,
    provider_unit_namespace,
    provider_unit_name,
    intended_value,
    expected_membership,
    precondition,
    allow_create_membership,
    source_key_namespace,
    source_key_value,
    write_capability,
    state,
    recovery_result,
    settlement_kind,
    outcome_status,
    outcome_certainty,
    outcome_retry_disposition,
    outcome_membership_effect,
    outcome_provider_revision,
    outcome_successor_precondition,
    outcome_receipt,
    outcome_error_category,
    outcome_error_code,
    outcome_error_retryable,
    outcome_error_safe_message,
    outcome_error_diagnostic_id,
    outcome_error_retry_after_seconds,
    outcome_error_retry_after_nanoseconds,
    prepared_at,
    dispatched_at,
    settled_at
FROM sync_action_attempts
WHERE task_attempt_id = $1
",
    )
    .bind(task_attempt_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(SyncActionAttemptError::InvalidTransition)?;
    let field_effects = sqlx::query_as::<_, FieldEffectRow>(
        "SELECT
    field_namespace,
    field_name,
    unit_namespace,
    unit_name,
    effect,
    value_kind,
    boolean_value,
    integer_value,
    decimal_value,
    text_value,
    bytes_value,
    timestamp_milliseconds
FROM sync_action_attempt_field_effects
WHERE task_attempt_id = $1
ORDER BY ordinal
",
    )
    .bind(task_attempt_id)
    .fetch_all(&mut **transaction)
    .await?;
    let field_problems = sqlx::query_as::<_, ErrorFieldProblemRow>(
        "SELECT path, code, message
FROM sync_action_attempt_error_field_problems
WHERE task_attempt_id = $1
ORDER BY ordinal
",
    )
    .bind(task_attempt_id)
    .fetch_all(&mut **transaction)
    .await?;
    row.into_snapshot(field_effects, field_problems)
}

fn membership_name(membership: AdapterSourceMembership) -> &'static str {
    match membership {
        AdapterSourceMembership::Present => "present",
        AdapterSourceMembership::Absent => "absent",
        AdapterSourceMembership::Unknown => "unknown",
    }
}

#[derive(FromRow)]
struct PreparationRow {
    task_status: String,
    owner_id: Uuid,
    task_attempt_status: String,
    started_at: time::OffsetDateTime,
    task_connection_id: Uuid,
    preview_action_hash: String,
    receiver_subject_kind: String,
    receiver_subject_namespace: String,
    receiver_subject_value: Vec<u8>,
    semantic_field: String,
    canonical_json: Vec<u8>,
    source_connection_id: Uuid,
    source_key_namespace: String,
    source_key_value: Vec<u8>,
    source_availability: String,
    source_capabilities: Value,
}

#[derive(FromRow)]
struct FailedClaimRow {
    task_status: String,
    task_attempt_status: String,
    owner_id: Uuid,
    action_id: Uuid,
    run_id: Uuid,
}

#[derive(FromRow)]
struct DispatchFenceRow {
    task_status: String,
    task_attempt_status: String,
    owner_id: Uuid,
    state: String,
    prepared_at: time::OffsetDateTime,
    action_id: Uuid,
    run_id: Uuid,
}

#[derive(FromRow)]
struct OutcomeFenceRow {
    task_status: String,
    task_attempt_status: String,
    owner_id: Uuid,
    task_attempt_ordinal: i32,
    state: String,
    dispatched_at: time::OffsetDateTime,
    action_id: Uuid,
    receiver_source_id: Uuid,
    workspace_id: Uuid,
    connection_id: Uuid,
    run_id: Uuid,
}

#[derive(FromRow)]
struct SyncActionAttemptRow {
    task_attempt_id: Uuid,
    task_id: Uuid,
    action_id: Uuid,
    receiver_source_id: Uuid,
    owner_id: Uuid,
    operation_id: Uuid,
    idempotency_key: Uuid,
    preview_action_hash: String,
    receiver_subject_kind: String,
    receiver_subject_namespace: String,
    receiver_subject_value: Vec<u8>,
    semantic_field: String,
    provider_field_namespace: String,
    provider_field_name: String,
    provider_unit_namespace: Option<String>,
    provider_unit_name: Option<String>,
    intended_value: i64,
    expected_membership: String,
    precondition: Option<Vec<u8>>,
    allow_create_membership: bool,
    source_key_namespace: Option<String>,
    source_key_value: Option<Vec<u8>>,
    write_capability: Option<Value>,
    state: String,
    recovery_result: Option<String>,
    settlement_kind: Option<String>,
    outcome_status: Option<String>,
    outcome_certainty: Option<String>,
    outcome_retry_disposition: Option<String>,
    outcome_membership_effect: Option<String>,
    outcome_provider_revision: Option<Vec<u8>>,
    outcome_successor_precondition: Option<Vec<u8>>,
    outcome_receipt: Option<Vec<u8>>,
    outcome_error_category: Option<String>,
    outcome_error_code: Option<String>,
    outcome_error_retryable: Option<bool>,
    outcome_error_safe_message: Option<String>,
    outcome_error_diagnostic_id: Option<String>,
    outcome_error_retry_after_seconds: Option<i64>,
    outcome_error_retry_after_nanoseconds: Option<i32>,
    prepared_at: time::OffsetDateTime,
    dispatched_at: Option<time::OffsetDateTime>,
    settled_at: Option<time::OffsetDateTime>,
}

impl SyncActionAttemptRow {
    fn into_snapshot(
        self,
        field_effects: Vec<FieldEffectRow>,
        field_problems: Vec<ErrorFieldProblemRow>,
    ) -> Result<SyncActionAttemptSnapshot, SyncActionAttemptError<PostgresSyncActionAttemptError>>
    {
        let outcome = self.parse_outcome(field_effects, field_problems)?;
        let provider_unit = match (self.provider_unit_namespace, self.provider_unit_name) {
            (Some(namespace), Some(name)) => Some(
                Term::new(namespace, name).map_err(|_| SyncActionAttemptError::CorruptAction)?,
            ),
            (None, None) => None,
            _ => return Err(SyncActionAttemptError::CorruptAction),
        };
        let state = match self.state.as_str() {
            "prepared" => SyncActionAttemptState::Prepared,
            "dispatched" => SyncActionAttemptState::Dispatched,
            "settled" => SyncActionAttemptState::Settled,
            _ => return Err(SyncActionAttemptError::CorruptAction),
        };
        let recovery_result = match self.recovery_result.as_deref() {
            None => None,
            Some("confirmed_not_applied") => Some(SyncActionRecoveryResult::ConfirmedNotApplied),
            Some("indeterminate") => Some(SyncActionRecoveryResult::Indeterminate),
            _ => return Err(SyncActionAttemptError::CorruptAction),
        };
        Ok(SyncActionAttemptSnapshot {
            task_id: self.task_id,
            task_attempt_id: self.task_attempt_id,
            owner_id: self.owner_id,
            request: SyncActionWriteRequest {
                action_id: self.action_id,
                preview_action_hash: self.preview_action_hash,
                receiver_source_id: self.receiver_source_id,
                receiver_subject: parse_subject(
                    &self.receiver_subject_kind,
                    self.receiver_subject_namespace,
                    self.receiver_subject_value,
                )?,
                semantic_field: self.semantic_field,
                provider_field: StateField::new(
                    Term::new(self.provider_field_namespace, self.provider_field_name)
                        .map_err(|_| SyncActionAttemptError::CorruptAction)?,
                    provider_unit,
                ),
                intended_value: self.intended_value,
                expected_membership: match self.expected_membership.as_str() {
                    "present" => AdapterSourceMembership::Present,
                    "absent" => AdapterSourceMembership::Absent,
                    _ => return Err(SyncActionAttemptError::CorruptAction),
                },
                precondition: self.precondition,
                allow_create_membership: self.allow_create_membership,
                idempotency_key: self.idempotency_key,
                operation_id: self.operation_id,
                source_key: AdapterKey {
                    namespace: self
                        .source_key_namespace
                        .ok_or(SyncActionAttemptError::CorruptAction)?,
                    value: self
                        .source_key_value
                        .ok_or(SyncActionAttemptError::CorruptAction)?,
                },
                capability: parse_write_capability(&serde_json::json!({
                    "targetedStateWrite": self
                        .write_capability
                        .ok_or(SyncActionAttemptError::CorruptAction)?,
                }))?,
            },
            state,
            recovery_result,
            outcome,
            prepared_at: self.prepared_at,
            dispatched_at: self.dispatched_at,
            settled_at: self.settled_at,
        })
    }

    fn parse_outcome(
        &self,
        field_effects: Vec<FieldEffectRow>,
        field_problems: Vec<ErrorFieldProblemRow>,
    ) -> Result<
        Option<AdapterTargetedStateWriteOutcome>,
        SyncActionAttemptError<PostgresSyncActionAttemptError>,
    > {
        let Some(status) = self.outcome_status.as_deref() else {
            if self.settlement_kind.as_deref() == Some("provider_outcome")
                || !field_effects.is_empty()
                || !field_problems.is_empty()
            {
                return Err(SyncActionAttemptError::CorruptAction);
            }
            return Ok(None);
        };
        if self.settlement_kind.as_deref() != Some("provider_outcome") {
            return Err(SyncActionAttemptError::CorruptAction);
        }
        let error = match (
            self.outcome_error_category.as_deref(),
            self.outcome_error_code.as_ref(),
            self.outcome_error_retryable,
            self.outcome_error_safe_message.as_ref(),
            self.outcome_error_diagnostic_id.as_ref(),
        ) {
            (None, None, None, None, None) if field_problems.is_empty() => None,
            (
                Some(category),
                Some(code),
                Some(retryable),
                Some(safe_message),
                Some(diagnostic_id),
            ) => Some(AdapterCallError {
                category: parse_failure_category(category)?,
                code: code.clone(),
                safe_message: safe_message.clone(),
                retryable,
                retry_after: match (
                    self.outcome_error_retry_after_seconds,
                    self.outcome_error_retry_after_nanoseconds,
                ) {
                    (Some(seconds), Some(nanoseconds)) => Some(AdapterRetryAfter {
                        seconds,
                        nanoseconds,
                    }),
                    (None, None) => None,
                    _ => return Err(SyncActionAttemptError::CorruptAction),
                },
                field_problems: field_problems
                    .into_iter()
                    .map(ErrorFieldProblemRow::into_problem)
                    .collect(),
                diagnostic_id: diagnostic_id.clone(),
            }),
            _ => return Err(SyncActionAttemptError::CorruptAction),
        };
        Ok(Some(AdapterTargetedStateWriteOutcome {
            status: parse_write_status(status)?,
            certainty: parse_write_certainty(
                self.outcome_certainty
                    .as_deref()
                    .ok_or(SyncActionAttemptError::CorruptAction)?,
            )?,
            retry_disposition: parse_write_retry(
                self.outcome_retry_disposition
                    .as_deref()
                    .ok_or(SyncActionAttemptError::CorruptAction)?,
            )?,
            membership_effect: parse_membership_effect(
                self.outcome_membership_effect
                    .as_deref()
                    .ok_or(SyncActionAttemptError::CorruptAction)?,
            )?,
            field_effects: field_effects
                .into_iter()
                .map(FieldEffectRow::into_effect)
                .collect::<Result<Vec<_>, _>>()?,
            provider_revision: self
                .outcome_provider_revision
                .clone()
                .ok_or(SyncActionAttemptError::CorruptAction)?,
            successor_precondition: self.outcome_successor_precondition.clone(),
            receipt: self
                .outcome_receipt
                .clone()
                .ok_or(SyncActionAttemptError::CorruptAction)?,
            error,
        }))
    }
}

struct StoredStateValue<'a> {
    kind: Option<&'static str>,
    boolean: Option<bool>,
    integer: Option<i64>,
    decimal: Option<&'a str>,
    text: Option<&'a str>,
    bytes: Option<&'a [u8]>,
    timestamp_milliseconds: Option<i64>,
}

impl<'a> From<Option<&'a StateValue>> for StoredStateValue<'a> {
    fn from(value: Option<&'a StateValue>) -> Self {
        let mut stored = Self {
            kind: None,
            boolean: None,
            integer: None,
            decimal: None,
            text: None,
            bytes: None,
            timestamp_milliseconds: None,
        };
        match value {
            None => {}
            Some(StateValue::Boolean(value)) => {
                stored.kind = Some("boolean");
                stored.boolean = Some(*value);
            }
            Some(StateValue::Integer(value)) => {
                stored.kind = Some("integer");
                stored.integer = Some(*value);
            }
            Some(StateValue::Decimal(value)) => {
                stored.kind = Some("decimal");
                stored.decimal = Some(value);
            }
            Some(StateValue::Text(value)) => {
                stored.kind = Some("text");
                stored.text = Some(value);
            }
            Some(StateValue::Bytes(value)) => {
                stored.kind = Some("bytes");
                stored.bytes = Some(value);
            }
            Some(StateValue::TimestampMilliseconds(value)) => {
                stored.kind = Some("timestamp_milliseconds");
                stored.timestamp_milliseconds = Some(*value);
            }
        }
        stored
    }
}

#[derive(FromRow)]
struct FieldEffectRow {
    field_namespace: String,
    field_name: String,
    unit_namespace: Option<String>,
    unit_name: Option<String>,
    effect: String,
    value_kind: Option<String>,
    boolean_value: Option<bool>,
    integer_value: Option<i64>,
    decimal_value: Option<String>,
    text_value: Option<String>,
    bytes_value: Option<Vec<u8>>,
    timestamp_milliseconds: Option<i64>,
}

impl FieldEffectRow {
    fn into_effect(
        self,
    ) -> Result<
        AdapterTargetedStateFieldEffect,
        SyncActionAttemptError<PostgresSyncActionAttemptError>,
    > {
        let unit = match (self.unit_namespace, self.unit_name) {
            (Some(namespace), Some(name)) => Some(
                Term::new(namespace, name).map_err(|_| SyncActionAttemptError::CorruptAction)?,
            ),
            (None, None) => None,
            _ => return Err(SyncActionAttemptError::CorruptAction),
        };
        let value = match self.value_kind.as_deref() {
            None if self.boolean_value.is_none()
                && self.integer_value.is_none()
                && self.decimal_value.is_none()
                && self.text_value.is_none()
                && self.bytes_value.is_none()
                && self.timestamp_milliseconds.is_none() =>
            {
                None
            }
            Some("boolean") => self.boolean_value.map(StateValue::Boolean),
            Some("integer") => self.integer_value.map(StateValue::Integer),
            Some("decimal") => self.decimal_value.map(StateValue::Decimal),
            Some("text") => self.text_value.map(StateValue::Text),
            Some("bytes") => self.bytes_value.map(StateValue::Bytes),
            Some("timestamp_milliseconds") => self
                .timestamp_milliseconds
                .map(StateValue::TimestampMilliseconds),
            _ => return Err(SyncActionAttemptError::CorruptAction),
        };
        Ok(AdapterTargetedStateFieldEffect {
            field: StateField::new(
                Term::new(self.field_namespace, self.field_name)
                    .map_err(|_| SyncActionAttemptError::CorruptAction)?,
                unit,
            ),
            effect: parse_field_effect(&self.effect)?,
            value,
        })
    }
}

#[derive(FromRow)]
struct ErrorFieldProblemRow {
    path: String,
    code: String,
    message: String,
}

impl ErrorFieldProblemRow {
    fn into_problem(self) -> AdapterFieldProblem {
        AdapterFieldProblem {
            path: self.path,
            code: self.code,
            message: self.message,
        }
    }
}

fn failure_category_name(category: AdapterFailureCategory) -> &'static str {
    match category {
        AdapterFailureCategory::InvalidInput => "invalid_input",
        AdapterFailureCategory::Authentication => "authentication",
        AdapterFailureCategory::Authorization => "authorization",
        AdapterFailureCategory::RateLimited => "rate_limited",
        AdapterFailureCategory::Unavailable => "unavailable",
        AdapterFailureCategory::Conflict => "conflict",
        AdapterFailureCategory::Unsupported => "unsupported",
        AdapterFailureCategory::InvalidRemoteData => "invalid_remote_data",
        AdapterFailureCategory::Internal => "internal",
    }
}

fn parse_failure_category(
    value: &str,
) -> Result<AdapterFailureCategory, SyncActionAttemptError<PostgresSyncActionAttemptError>> {
    match value {
        "invalid_input" => Ok(AdapterFailureCategory::InvalidInput),
        "authentication" => Ok(AdapterFailureCategory::Authentication),
        "authorization" => Ok(AdapterFailureCategory::Authorization),
        "rate_limited" => Ok(AdapterFailureCategory::RateLimited),
        "unavailable" => Ok(AdapterFailureCategory::Unavailable),
        "conflict" => Ok(AdapterFailureCategory::Conflict),
        "unsupported" => Ok(AdapterFailureCategory::Unsupported),
        "invalid_remote_data" => Ok(AdapterFailureCategory::InvalidRemoteData),
        "internal" => Ok(AdapterFailureCategory::Internal),
        _ => Err(SyncActionAttemptError::CorruptAction),
    }
}

fn write_status_name(status: AdapterTargetedStateWriteStatus) -> &'static str {
    match status {
        AdapterTargetedStateWriteStatus::Applied => "applied",
        AdapterTargetedStateWriteStatus::NoOp => "no_op",
        AdapterTargetedStateWriteStatus::Rejected => "rejected",
        AdapterTargetedStateWriteStatus::Failed => "failed",
        AdapterTargetedStateWriteStatus::Indeterminate => "indeterminate",
    }
}

fn parse_write_status(
    status: &str,
) -> Result<AdapterTargetedStateWriteStatus, SyncActionAttemptError<PostgresSyncActionAttemptError>>
{
    match status {
        "applied" => Ok(AdapterTargetedStateWriteStatus::Applied),
        "no_op" => Ok(AdapterTargetedStateWriteStatus::NoOp),
        "rejected" => Ok(AdapterTargetedStateWriteStatus::Rejected),
        "failed" => Ok(AdapterTargetedStateWriteStatus::Failed),
        "indeterminate" => Ok(AdapterTargetedStateWriteStatus::Indeterminate),
        _ => Err(SyncActionAttemptError::CorruptAction),
    }
}

fn write_certainty_name(certainty: AdapterTargetedStateWriteCertainty) -> &'static str {
    match certainty {
        AdapterTargetedStateWriteCertainty::ConfirmedApplied => "confirmed_applied",
        AdapterTargetedStateWriteCertainty::ConfirmedNotApplied => "confirmed_not_applied",
        AdapterTargetedStateWriteCertainty::Unknown => "unknown",
    }
}

fn parse_write_certainty(
    certainty: &str,
) -> Result<
    AdapterTargetedStateWriteCertainty,
    SyncActionAttemptError<PostgresSyncActionAttemptError>,
> {
    match certainty {
        "confirmed_applied" => Ok(AdapterTargetedStateWriteCertainty::ConfirmedApplied),
        "confirmed_not_applied" => Ok(AdapterTargetedStateWriteCertainty::ConfirmedNotApplied),
        "unknown" => Ok(AdapterTargetedStateWriteCertainty::Unknown),
        _ => Err(SyncActionAttemptError::CorruptAction),
    }
}

fn write_retry_name(retry: AdapterTargetedStateWriteRetryDisposition) -> &'static str {
    match retry {
        AdapterTargetedStateWriteRetryDisposition::NotRetryable => "not_retryable",
        AdapterTargetedStateWriteRetryDisposition::SafeSameKey => "safe_same_key",
        AdapterTargetedStateWriteRetryDisposition::ReconcileFirst => "reconcile_first",
    }
}

fn parse_write_retry(
    retry: &str,
) -> Result<
    AdapterTargetedStateWriteRetryDisposition,
    SyncActionAttemptError<PostgresSyncActionAttemptError>,
> {
    match retry {
        "not_retryable" => Ok(AdapterTargetedStateWriteRetryDisposition::NotRetryable),
        "safe_same_key" => Ok(AdapterTargetedStateWriteRetryDisposition::SafeSameKey),
        "reconcile_first" => Ok(AdapterTargetedStateWriteRetryDisposition::ReconcileFirst),
        _ => Err(SyncActionAttemptError::CorruptAction),
    }
}

fn membership_effect_name(effect: AdapterTargetedStateMembershipEffect) -> &'static str {
    match effect {
        AdapterTargetedStateMembershipEffect::Unchanged => "unchanged",
        AdapterTargetedStateMembershipEffect::Created => "created",
        AdapterTargetedStateMembershipEffect::Unknown => "unknown",
    }
}

fn parse_membership_effect(
    effect: &str,
) -> Result<
    AdapterTargetedStateMembershipEffect,
    SyncActionAttemptError<PostgresSyncActionAttemptError>,
> {
    match effect {
        "unchanged" => Ok(AdapterTargetedStateMembershipEffect::Unchanged),
        "created" => Ok(AdapterTargetedStateMembershipEffect::Created),
        "unknown" => Ok(AdapterTargetedStateMembershipEffect::Unknown),
        _ => Err(SyncActionAttemptError::CorruptAction),
    }
}

fn field_effect_name(effect: AdapterTargetedStateFieldEffectKind) -> &'static str {
    match effect {
        AdapterTargetedStateFieldEffectKind::Set => "set",
        AdapterTargetedStateFieldEffectKind::Cleared => "cleared",
        AdapterTargetedStateFieldEffectKind::Unchanged => "unchanged",
        AdapterTargetedStateFieldEffectKind::Unknown => "unknown",
    }
}

fn parse_field_effect(
    effect: &str,
) -> Result<
    AdapterTargetedStateFieldEffectKind,
    SyncActionAttemptError<PostgresSyncActionAttemptError>,
> {
    match effect {
        "set" => Ok(AdapterTargetedStateFieldEffectKind::Set),
        "cleared" => Ok(AdapterTargetedStateFieldEffectKind::Cleared),
        "unchanged" => Ok(AdapterTargetedStateFieldEffectKind::Unchanged),
        "unknown" => Ok(AdapterTargetedStateFieldEffectKind::Unknown),
        _ => Err(SyncActionAttemptError::CorruptAction),
    }
}

#[derive(Debug, Error)]
pub enum PostgresSyncActionAttemptError {
    #[error("sync action attempt database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl From<sqlx::Error> for SyncActionAttemptError<PostgresSyncActionAttemptError> {
    fn from(error: sqlx::Error) -> Self {
        Self::Repository(PostgresSyncActionAttemptError::Database(error))
    }
}
