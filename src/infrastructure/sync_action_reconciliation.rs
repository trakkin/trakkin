use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::{
            AdapterKey, AdapterSourceMembership, AdapterStatePresence,
            AdapterTargetedStateReadCapability, AdapterTargetedStateReadOutcome,
        },
        sync_action_reconciliation::{
            MAXIMUM_AUTOMATIC_SYNC_ACTION_RECONCILIATION_ATTEMPTS, PrepareSyncActionReconciliation,
            RecordSyncActionReconciliation, SyncActionReconciliationDisposition,
            SyncActionReconciliationError, SyncActionReconciliationFuture,
            SyncActionReconciliationRepository, SyncActionReconciliationRequest,
            SyncActionReconciliationResult, SyncActionReconciliationSettlement,
            SyncActionReconciliationSnapshot, SyncActionReconciliationState,
            classify_reconciliation,
        },
        task_manager::TaskDefinition,
    },
    domain::acquisition::{
        CatalogRelationKey, ProviderItemKey, StateField, StateSubject, StateValue, Term,
    },
    infrastructure::sync_execution::reduce_sync_run,
};

#[derive(Clone)]
pub struct PostgresSyncActionReconciliationRepository {
    pool: PgPool,
}

impl PostgresSyncActionReconciliationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn prepare(
        &self,
        command: PrepareSyncActionReconciliation,
    ) -> Result<
        SyncActionReconciliationSnapshot,
        SyncActionReconciliationError<PostgresSyncActionReconciliationError>,
    > {
        let TaskDefinition::SyncActionReconciliation {
            action_id,
            receiver_source_id,
            write_task_attempt_id,
        } = &command.claimed.task.definition
        else {
            return Err(SyncActionReconciliationError::InvalidClaim);
        };
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, PreparationRow>(
            "SELECT
    tasks.status AS task_status,
    task_attempts.status AS task_attempt_status,
    task_attempts.started_at,
    task_claims.owner_id,
    tasks.connection_id AS task_connection_id,
    sync_actions.status AS action_status,
    write_attempts.recovery_result AS write_recovery_result,
    write_attempts.source_key_namespace,
    write_attempts.source_key_value,
    write_attempts.receiver_subject_kind,
    write_attempts.receiver_subject_namespace,
    write_attempts.receiver_subject_value,
    write_attempts.provider_field_namespace,
    write_attempts.provider_field_name,
    write_attempts.provider_unit_namespace,
    write_attempts.provider_unit_name,
    write_attempts.intended_value,
    write_attempts.preview_action_hash,
    preview_actions.canonical_json,
    sources.connection_id AS source_connection_id,
    sources.availability AS source_availability,
    sources.capabilities AS source_capabilities
FROM tasks
INNER JOIN task_attempts ON task_attempts.task_id = tasks.id
INNER JOIN task_claims ON task_claims.task_id = tasks.id
INNER JOIN sync_action_reconciliation_tasks AS reconciliation_tasks
    ON reconciliation_tasks.task_id = tasks.id
INNER JOIN sync_action_attempts AS write_attempts
    ON
        write_attempts.task_attempt_id
        = reconciliation_tasks.write_task_attempt_id
        AND write_attempts.action_id = reconciliation_tasks.action_id
INNER JOIN sync_actions
    ON
        sync_actions.id = reconciliation_tasks.action_id
        AND sync_actions.receiver_source_id
        = reconciliation_tasks.receiver_source_id
INNER JOIN sync_preview_actions AS preview_actions
    ON
        preview_actions.preview_id = sync_actions.preview_id
        AND preview_actions.action_hash = sync_actions.preview_action_hash
INNER JOIN sources ON sources.id = reconciliation_tasks.receiver_source_id
WHERE
    tasks.id = $1
    AND task_attempts.id = $2
    AND reconciliation_tasks.action_id = $3
    AND reconciliation_tasks.receiver_source_id = $4
    AND reconciliation_tasks.write_task_attempt_id = $5
FOR UPDATE OF tasks, task_attempts, task_claims, sync_actions
",
        )
        .bind(command.claimed.task.id)
        .bind(command.claimed.attempt_id)
        .bind(action_id)
        .bind(receiver_source_id)
        .bind(write_task_attempt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SyncActionReconciliationError::InvalidClaim)?;
        if row.task_status != "running"
            || row.task_attempt_status != "running"
            || row.owner_id != command.claimed.owner_id
            || row.started_at > command.prepared_at
            || row.task_connection_id != row.source_connection_id
            || row.source_availability != "available"
            || row.action_status != "indeterminate"
            || row.write_recovery_result.as_deref() != Some("indeterminate")
        {
            return Err(SyncActionReconciliationError::InvalidClaim);
        }
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1 FROM sync_action_reconciliation_attempts WHERE task_attempt_id = $1
)
",
        )
        .bind(command.claimed.attempt_id)
        .fetch_one(&mut *transaction)
        .await?
        {
            return Err(SyncActionReconciliationError::AttemptAlreadyPrepared);
        }
        let request = parse_request(
            *action_id,
            *receiver_source_id,
            *write_task_attempt_id,
            command.operation_id,
            &row,
        )?;
        let (subject_kind, subject_namespace, subject_value) =
            subject_parts(&request.receiver_subject);
        let (unit_namespace, unit_name) =
            request.provider_field.unit().map_or((None, None), |unit| {
                (Some(unit.namespace()), Some(unit.name()))
            });
        sqlx::query(
            "INSERT INTO sync_action_reconciliation_attempts (
    task_attempt_id, task_id, action_id, receiver_source_id,
    write_task_attempt_id, owner_id, operation_id, idempotency_key,
    source_key_namespace, source_key_value,
    receiver_subject_kind, receiver_subject_namespace, receiver_subject_value,
    provider_field_namespace, provider_field_name,
    provider_unit_namespace, provider_unit_name, intended_value,
    before_membership, before_presence, before_value,
    before_provider_revision, maximum_response_bytes, state, prepared_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $3, $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, 'prepared', $23
)
",
        )
        .bind(command.claimed.attempt_id)
        .bind(command.claimed.task.id)
        .bind(action_id)
        .bind(receiver_source_id)
        .bind(write_task_attempt_id)
        .bind(command.claimed.owner_id)
        .bind(command.operation_id)
        .bind(&request.source_key.namespace)
        .bind(&request.source_key.value)
        .bind(subject_kind)
        .bind(subject_namespace)
        .bind(subject_value)
        .bind(request.provider_field.field().namespace())
        .bind(request.provider_field.field().name())
        .bind(unit_namespace)
        .bind(unit_name)
        .bind(request.intended_value)
        .bind(membership_name(request.before_membership))
        .bind(presence_name(request.before_presence))
        .bind(request.before_value)
        .bind(&request.before_provider_revision)
        .bind(
            i64::try_from(request.capability.maximum_response_bytes)
                .map_err(|_| SyncActionReconciliationError::CorruptAction)?,
        )
        .bind(command.prepared_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(SyncActionReconciliationSnapshot {
            task_id: command.claimed.task.id,
            task_attempt_id: command.claimed.attempt_id,
            owner_id: command.claimed.owner_id,
            request,
            state: SyncActionReconciliationState::Prepared,
            outcome: None,
            result: None,
            prepared_at: command.prepared_at,
            settled_at: None,
        })
    }

    async fn record(
        &self,
        command: RecordSyncActionReconciliation,
    ) -> Result<
        SyncActionReconciliationSettlement,
        SyncActionReconciliationError<PostgresSyncActionReconciliationError>,
    > {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, SettlementRow>(
            "SELECT
    tasks.status AS task_status,
    task_attempts.status AS task_attempt_status,
    task_attempts.ordinal AS task_attempt_ordinal,
    task_claims.owner_id AS claim_owner_id,
    reconciliation_attempts.owner_id,
    reconciliation_attempts.action_id,
    reconciliation_attempts.receiver_source_id,
    reconciliation_attempts.write_task_attempt_id,
    reconciliation_attempts.operation_id,
    reconciliation_attempts.source_key_namespace,
    reconciliation_attempts.source_key_value,
    reconciliation_attempts.receiver_subject_kind,
    reconciliation_attempts.receiver_subject_namespace,
    reconciliation_attempts.receiver_subject_value,
    reconciliation_attempts.provider_field_namespace,
    reconciliation_attempts.provider_field_name,
    reconciliation_attempts.provider_unit_namespace,
    reconciliation_attempts.provider_unit_name,
    reconciliation_attempts.intended_value,
    reconciliation_attempts.before_membership,
    reconciliation_attempts.before_presence,
    reconciliation_attempts.before_value,
    reconciliation_attempts.before_provider_revision,
    reconciliation_attempts.maximum_response_bytes,
    reconciliation_attempts.state,
    reconciliation_attempts.prepared_at,
    sync_actions.status AS action_status,
    sync_actions.run_id
FROM sync_action_reconciliation_attempts AS reconciliation_attempts
INNER JOIN tasks ON tasks.id = reconciliation_attempts.task_id
INNER JOIN task_attempts
    ON task_attempts.id = reconciliation_attempts.task_attempt_id
INNER JOIN task_claims
    ON task_claims.task_id = reconciliation_attempts.task_id
INNER JOIN sync_actions
    ON sync_actions.id = reconciliation_attempts.action_id
WHERE
    reconciliation_attempts.task_id = $1
    AND reconciliation_attempts.task_attempt_id = $2
FOR UPDATE OF tasks, task_attempts, task_claims,
reconciliation_attempts, sync_actions
",
        )
        .bind(command.task_id)
        .bind(command.task_attempt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SyncActionReconciliationError::InvalidClaim)?;
        if row.task_status != "running"
            || row.task_attempt_status != "running"
            || row.claim_owner_id != command.owner_id
            || row.owner_id != command.owner_id
            || row.state != "prepared"
            || row.action_status != "indeterminate"
            || command.settled_at < row.prepared_at
        {
            return Err(SyncActionReconciliationError::InvalidTransition);
        }
        let request = row.request()?;
        let result = classify_reconciliation(&request, &command.outcome)?;
        let retry_queued = matches!(
            &command.outcome,
            AdapterTargetedStateReadOutcome::Indeterminate(error) if error.retryable
        ) && row.task_attempt_ordinal + 1
            < MAXIMUM_AUTOMATIC_SYNC_ACTION_RECONCILIATION_ATTEMPTS;
        let stored = StoredOutcome::from_outcome(&command.outcome)?;
        let update = sqlx::query(
            "UPDATE sync_action_reconciliation_attempts
SET
    state = 'settled', outcome_kind = $1,
    observed_membership = $2, observed_presence = $3, observed_value = $4,
    observed_provider_revision = $5, observed_at = $6, expires_at = $7,
    observed_precondition = $8, causation_idempotency_key = $9,
    causation_receipt = $10, causation_provider_revision = $11,
    result = $12, outcome_error_code = $13,
    outcome_error_retryable = $14, outcome_error_safe_message = $15,
    settled_at = $16
WHERE task_attempt_id = $17 AND state = 'prepared'
",
        )
        .bind(stored.kind)
        .bind(stored.membership)
        .bind(stored.presence)
        .bind(stored.value)
        .bind(stored.provider_revision)
        .bind(stored.observed_at)
        .bind(stored.expires_at)
        .bind(stored.precondition)
        .bind(stored.causation_idempotency_key)
        .bind(stored.causation_receipt)
        .bind(stored.causation_provider_revision)
        .bind(result_name(result))
        .bind(stored.error_code)
        .bind(stored.error_retryable)
        .bind(stored.error_safe_message)
        .bind(command.settled_at)
        .bind(command.task_attempt_id)
        .execute(&mut *transaction)
        .await?;
        if update.rows_affected() != 1 {
            return Err(SyncActionReconciliationError::InvalidTransition);
        }
        let action_status = match result {
            SyncActionReconciliationResult::ConfirmedApplied => "applied",
            SyncActionReconciliationResult::ConfirmedNotApplied => "failed",
            SyncActionReconciliationResult::StateMatchesCausationUnknown
            | SyncActionReconciliationResult::Unresolved => "indeterminate",
        };
        let action = sqlx::query(
            "UPDATE sync_actions
SET status = $1, finished_at = $2, updated_at = $2
WHERE id = $3 AND status = 'indeterminate'
",
        )
        .bind(action_status)
        .bind(command.settled_at)
        .bind(row.action_id)
        .execute(&mut *transaction)
        .await?;
        if action.rows_affected() != 1 {
            return Err(SyncActionReconciliationError::InvalidTransition);
        }
        if matches!(
            result,
            SyncActionReconciliationResult::ConfirmedApplied
                | SyncActionReconciliationResult::ConfirmedNotApplied
        ) {
            let provenance = sqlx::query(
                "INSERT INTO sync_action_provenance (
    action_id, terminal_task_attempt_id,
    reconciliation_task_attempt_id, recorded_at
) VALUES ($1, $2, $3, $4)
",
            )
            .bind(row.action_id)
            .bind(row.write_task_attempt_id)
            .bind(command.task_attempt_id)
            .bind(command.settled_at)
            .execute(&mut *transaction)
            .await?;
            if provenance.rows_affected() != 1 {
                return Err(SyncActionReconciliationError::InvalidTransition);
            }
        }
        let operational_failure = match &command.outcome {
            AdapterTargetedStateReadOutcome::Indeterminate(error) => Some(json!({
                "code": error.code,
                "retryable": error.retryable,
            })),
            _ => None,
        };
        let attempt = if retry_queued || operational_failure.is_some() {
            sqlx::query(
                "UPDATE task_attempts
SET status = 'failed', failure = $1, finished_at = $2
WHERE id = $3 AND task_id = $4 AND status = 'running'
",
            )
            .bind(&operational_failure)
            .bind(command.settled_at)
            .bind(command.task_attempt_id)
            .bind(command.task_id)
            .execute(&mut *transaction)
            .await?
        } else {
            sqlx::query(
                "UPDATE task_attempts
SET status = 'succeeded', failure = NULL, finished_at = $1
WHERE id = $2 AND task_id = $3 AND status = 'running'
",
            )
            .bind(command.settled_at)
            .bind(command.task_attempt_id)
            .bind(command.task_id)
            .execute(&mut *transaction)
            .await?
        };
        if attempt.rows_affected() != 1 {
            return Err(SyncActionReconciliationError::InvalidTransition);
        }
        let claim = sqlx::query(
            "DELETE FROM task_claims WHERE task_id = $1 AND owner_id = $2
",
        )
        .bind(command.task_id)
        .bind(command.owner_id)
        .execute(&mut *transaction)
        .await?;
        if claim.rows_affected() != 1 {
            return Err(SyncActionReconciliationError::InvalidTransition);
        }
        let task = if retry_queued {
            sqlx::query(
                "UPDATE tasks
SET status = 'queued', failure = NULL, finished_at = NULL, updated_at = $1
WHERE id = $2 AND status = 'running'
",
            )
            .bind(command.settled_at)
            .bind(command.task_id)
            .execute(&mut *transaction)
            .await?
        } else if operational_failure.is_some() {
            sqlx::query(
                "UPDATE tasks
SET status = 'failed', failure = $1, finished_at = $2, updated_at = $2
WHERE id = $3 AND status = 'running'
",
            )
            .bind(&operational_failure)
            .bind(command.settled_at)
            .bind(command.task_id)
            .execute(&mut *transaction)
            .await?
        } else {
            sqlx::query(
                "UPDATE tasks
SET status = 'succeeded', failure = NULL, finished_at = $1, updated_at = $1
WHERE id = $2 AND status = 'running'
",
            )
            .bind(command.settled_at)
            .bind(command.task_id)
            .execute(&mut *transaction)
            .await?
        };
        if task.rows_affected() != 1
            || !reduce_sync_run(&mut transaction, row.run_id, command.settled_at).await?
        {
            return Err(SyncActionReconciliationError::InvalidTransition);
        }
        transaction.commit().await?;
        Ok(SyncActionReconciliationSettlement {
            attempt: SyncActionReconciliationSnapshot {
                task_id: command.task_id,
                task_attempt_id: command.task_attempt_id,
                owner_id: command.owner_id,
                request,
                state: SyncActionReconciliationState::Settled,
                outcome: Some(command.outcome),
                result: Some(result),
                prepared_at: row.prepared_at,
                settled_at: Some(command.settled_at),
            },
            disposition: if retry_queued {
                SyncActionReconciliationDisposition::RetryQueued
            } else {
                SyncActionReconciliationDisposition::Terminalized
            },
        })
    }
}

impl SyncActionReconciliationRepository for PostgresSyncActionReconciliationRepository {
    type Error = PostgresSyncActionReconciliationError;

    fn prepare_sync_action_reconciliation(
        &self,
        command: PrepareSyncActionReconciliation,
    ) -> SyncActionReconciliationFuture<'_, SyncActionReconciliationSnapshot, Self::Error> {
        Box::pin(self.prepare(command))
    }

    fn record_sync_action_reconciliation(
        &self,
        command: RecordSyncActionReconciliation,
    ) -> SyncActionReconciliationFuture<'_, SyncActionReconciliationSettlement, Self::Error> {
        Box::pin(self.record(command))
    }
}

fn parse_request(
    action_id: Uuid,
    receiver_source_id: Uuid,
    write_task_attempt_id: Uuid,
    operation_id: Uuid,
    row: &PreparationRow,
) -> Result<
    SyncActionReconciliationRequest,
    SyncActionReconciliationError<PostgresSyncActionReconciliationError>,
> {
    if format!("sha256:{:x}", Sha256::digest(&row.canonical_json)) != row.preview_action_hash {
        return Err(SyncActionReconciliationError::CorruptAction);
    }
    let action: Value = serde_json::from_slice(&row.canonical_json)
        .map_err(|_| SyncActionReconciliationError::CorruptAction)?;
    let fields = action["targetedState"]["fields"]
        .as_array()
        .filter(|fields| fields.len() == 1)
        .ok_or(SyncActionReconciliationError::CorruptAction)?;
    let field = &fields[0];
    let provider_field = state_field(
        &row.provider_field_namespace,
        &row.provider_field_name,
        row.provider_unit_namespace.as_deref(),
        row.provider_unit_name.as_deref(),
    )?;
    if field["field"]["field"]["namespace"] != row.provider_field_namespace
        || field["field"]["field"]["name"] != row.provider_field_name
        || field["field"]["unit"]["namespace"]
            != row
                .provider_unit_namespace
                .as_deref()
                .map(Value::from)
                .unwrap_or(Value::Null)
        || field["field"]["unit"]["name"]
            != row
                .provider_unit_name
                .as_deref()
                .map(Value::from)
                .unwrap_or(Value::Null)
    {
        return Err(SyncActionReconciliationError::CorruptAction);
    }
    let before_membership = match action["targetedState"]["membership"].as_str() {
        Some("present") => AdapterSourceMembership::Present,
        Some("absent") => AdapterSourceMembership::Absent,
        _ => return Err(SyncActionReconciliationError::CorruptAction),
    };
    let before_presence = parse_presence(
        field["presence"]
            .as_str()
            .ok_or(SyncActionReconciliationError::CorruptAction)?,
    )?;
    let before_value = match &field["value"] {
        Value::Null => None,
        Value::Object(value) if value.get("kind").and_then(Value::as_str) == Some("integer") => {
            Some(
                value
                    .get("integer")
                    .and_then(Value::as_i64)
                    .ok_or(SyncActionReconciliationError::CorruptAction)?,
            )
        }
        _ => return Err(SyncActionReconciliationError::CorruptAction),
    };
    if (before_presence == AdapterStatePresence::Present) != before_value.is_some() {
        return Err(SyncActionReconciliationError::CorruptAction);
    }
    let before_provider_revision = json_bytes(
        action["targetedState"]["providerRevision"]
            .as_array()
            .ok_or(SyncActionReconciliationError::CorruptAction)?,
    )?;
    if before_provider_revision.is_empty() {
        return Err(SyncActionReconciliationError::CorruptAction);
    }
    let capability = parse_read_capability(&row.source_capabilities)?;
    Ok(SyncActionReconciliationRequest {
        action_id,
        receiver_source_id,
        write_task_attempt_id,
        source_key: AdapterKey {
            namespace: row.source_key_namespace.clone(),
            value: row.source_key_value.clone(),
        },
        receiver_subject: parse_subject(
            &row.receiver_subject_kind,
            row.receiver_subject_namespace.clone(),
            row.receiver_subject_value.clone(),
        )?,
        provider_field,
        intended_value: row.intended_value,
        before_membership,
        before_presence,
        before_value,
        before_provider_revision,
        operation_id,
        capability,
    })
}

fn parse_read_capability(
    capabilities: &Value,
) -> Result<
    AdapterTargetedStateReadCapability,
    SyncActionReconciliationError<PostgresSyncActionReconciliationError>,
> {
    let read = capabilities
        .get("targetedStateRead")
        .ok_or(SyncActionReconciliationError::CorruptAction)?;
    let maximum_fields = read["maximumFields"]
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(SyncActionReconciliationError::CorruptAction)?;
    let maximum_response_bytes = read["maximumResponseBytes"]
        .as_u64()
        .ok_or(SyncActionReconciliationError::CorruptAction)?;
    if maximum_fields < 1 || maximum_response_bytes == 0 {
        return Err(SyncActionReconciliationError::CorruptAction);
    }
    Ok(AdapterTargetedStateReadCapability {
        maximum_fields,
        maximum_response_bytes,
    })
}

fn state_field(
    namespace: &str,
    name: &str,
    unit_namespace: Option<&str>,
    unit_name: Option<&str>,
) -> Result<StateField, SyncActionReconciliationError<PostgresSyncActionReconciliationError>> {
    let unit = match (unit_namespace, unit_name) {
        (Some(namespace), Some(name)) => Some(
            Term::new(namespace, name).map_err(|_| SyncActionReconciliationError::CorruptAction)?,
        ),
        (None, None) => None,
        _ => return Err(SyncActionReconciliationError::CorruptAction),
    };
    Ok(StateField::new(
        Term::new(namespace, name).map_err(|_| SyncActionReconciliationError::CorruptAction)?,
        unit,
    ))
}

fn parse_subject(
    kind: &str,
    namespace: String,
    value: Vec<u8>,
) -> Result<StateSubject, SyncActionReconciliationError<PostgresSyncActionReconciliationError>> {
    match kind {
        "provider_item" => ProviderItemKey::new(namespace, value)
            .map(StateSubject::ProviderItem)
            .map_err(|_| SyncActionReconciliationError::CorruptAction),
        "catalog_relation" => CatalogRelationKey::new(namespace, value)
            .map(StateSubject::CatalogRelation)
            .map_err(|_| SyncActionReconciliationError::CorruptAction),
        _ => Err(SyncActionReconciliationError::CorruptAction),
    }
}

fn subject_parts(subject: &StateSubject) -> (&'static str, &str, &[u8]) {
    match subject {
        StateSubject::ProviderItem(key) => ("provider_item", key.namespace(), key.value()),
        StateSubject::CatalogRelation(key) => ("catalog_relation", key.namespace(), key.value()),
    }
}

fn json_bytes(
    values: &[Value],
) -> Result<Vec<u8>, SyncActionReconciliationError<PostgresSyncActionReconciliationError>> {
    values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u8::try_from(value).ok())
                .ok_or(SyncActionReconciliationError::CorruptAction)
        })
        .collect()
}

fn parse_presence(
    value: &str,
) -> Result<
    AdapterStatePresence,
    SyncActionReconciliationError<PostgresSyncActionReconciliationError>,
> {
    match value {
        "present" => Ok(AdapterStatePresence::Present),
        "deleted" => Ok(AdapterStatePresence::Deleted),
        "absent" => Ok(AdapterStatePresence::Absent),
        "unknown" => Ok(AdapterStatePresence::Unknown),
        "stale" => Ok(AdapterStatePresence::Stale),
        _ => Err(SyncActionReconciliationError::CorruptAction),
    }
}

fn membership_name(value: AdapterSourceMembership) -> &'static str {
    match value {
        AdapterSourceMembership::Present => "present",
        AdapterSourceMembership::Absent => "absent",
        AdapterSourceMembership::Unknown => "unknown",
    }
}

fn presence_name(value: AdapterStatePresence) -> &'static str {
    match value {
        AdapterStatePresence::Present => "present",
        AdapterStatePresence::Deleted => "deleted",
        AdapterStatePresence::Absent => "absent",
        AdapterStatePresence::Unknown => "unknown",
        AdapterStatePresence::Stale => "stale",
    }
}

fn result_name(value: SyncActionReconciliationResult) -> &'static str {
    match value {
        SyncActionReconciliationResult::ConfirmedApplied => "confirmed_applied",
        SyncActionReconciliationResult::ConfirmedNotApplied => "confirmed_not_applied",
        SyncActionReconciliationResult::StateMatchesCausationUnknown => {
            "state_matches_causation_unknown"
        }
        SyncActionReconciliationResult::Unresolved => "unresolved",
    }
}

struct StoredOutcome<'a> {
    kind: &'static str,
    membership: Option<&'static str>,
    presence: Option<&'static str>,
    value: Option<i64>,
    provider_revision: Option<&'a [u8]>,
    observed_at: Option<time::OffsetDateTime>,
    expires_at: Option<time::OffsetDateTime>,
    precondition: Option<&'a [u8]>,
    causation_idempotency_key: Option<Uuid>,
    causation_receipt: Option<&'a [u8]>,
    causation_provider_revision: Option<&'a [u8]>,
    error_code: Option<&'a str>,
    error_retryable: Option<bool>,
    error_safe_message: Option<&'a str>,
}

impl<'a> StoredOutcome<'a> {
    fn from_outcome(
        outcome: &'a AdapterTargetedStateReadOutcome,
    ) -> Result<Self, SyncActionReconciliationError<PostgresSyncActionReconciliationError>> {
        match outcome {
            AdapterTargetedStateReadOutcome::Matched(observed) => {
                let field = observed
                    .fields
                    .first()
                    .ok_or(SyncActionReconciliationError::InvalidTransition)?;
                let value = match &field.value {
                    Some(StateValue::Integer(value)) => Some(*value),
                    None => None,
                    Some(_) => return Err(SyncActionReconciliationError::InvalidTransition),
                };
                let causation_idempotency_key = observed
                    .write_causation
                    .as_ref()
                    .map(|causation| Uuid::from_slice(&causation.idempotency_key))
                    .transpose()
                    .map_err(|_| SyncActionReconciliationError::InvalidTransition)?;
                Ok(Self {
                    kind: "matched",
                    membership: Some(membership_name(observed.membership)),
                    presence: Some(presence_name(field.presence)),
                    value,
                    provider_revision: Some(&observed.provider_revision),
                    observed_at: Some(observed.observed_at),
                    expires_at: observed.expires_at,
                    precondition: observed.precondition.as_deref(),
                    causation_idempotency_key,
                    causation_receipt: observed
                        .write_causation
                        .as_ref()
                        .map(|causation| causation.receipt.as_slice()),
                    causation_provider_revision: observed
                        .write_causation
                        .as_ref()
                        .map(|causation| causation.provider_revision.as_slice()),
                    error_code: None,
                    error_retryable: None,
                    error_safe_message: None,
                })
            }
            AdapterTargetedStateReadOutcome::Unsupported => Ok(Self::empty("unsupported")),
            AdapterTargetedStateReadOutcome::NotFound => Ok(Self::empty("not_found")),
            AdapterTargetedStateReadOutcome::Ambiguous(_) => Ok(Self::empty("ambiguous")),
            AdapterTargetedStateReadOutcome::Indeterminate(error) => Ok(Self {
                kind: "indeterminate",
                error_code: Some(&error.code),
                error_retryable: Some(error.retryable),
                error_safe_message: Some(&error.safe_message),
                ..Self::empty("indeterminate")
            }),
        }
    }

    fn empty(kind: &'static str) -> Self {
        Self {
            kind,
            membership: None,
            presence: None,
            value: None,
            provider_revision: None,
            observed_at: None,
            expires_at: None,
            precondition: None,
            causation_idempotency_key: None,
            causation_receipt: None,
            causation_provider_revision: None,
            error_code: None,
            error_retryable: None,
            error_safe_message: None,
        }
    }
}

#[derive(FromRow)]
struct PreparationRow {
    task_status: String,
    task_attempt_status: String,
    started_at: time::OffsetDateTime,
    owner_id: Uuid,
    task_connection_id: Uuid,
    action_status: String,
    write_recovery_result: Option<String>,
    source_key_namespace: String,
    source_key_value: Vec<u8>,
    receiver_subject_kind: String,
    receiver_subject_namespace: String,
    receiver_subject_value: Vec<u8>,
    provider_field_namespace: String,
    provider_field_name: String,
    provider_unit_namespace: Option<String>,
    provider_unit_name: Option<String>,
    intended_value: i64,
    preview_action_hash: String,
    canonical_json: Vec<u8>,
    source_connection_id: Uuid,
    source_availability: String,
    source_capabilities: Value,
}

#[derive(FromRow)]
struct SettlementRow {
    task_status: String,
    task_attempt_status: String,
    task_attempt_ordinal: i32,
    claim_owner_id: Uuid,
    owner_id: Uuid,
    action_id: Uuid,
    receiver_source_id: Uuid,
    write_task_attempt_id: Uuid,
    operation_id: Uuid,
    source_key_namespace: String,
    source_key_value: Vec<u8>,
    receiver_subject_kind: String,
    receiver_subject_namespace: String,
    receiver_subject_value: Vec<u8>,
    provider_field_namespace: String,
    provider_field_name: String,
    provider_unit_namespace: Option<String>,
    provider_unit_name: Option<String>,
    intended_value: i64,
    before_membership: String,
    before_presence: String,
    before_value: Option<i64>,
    before_provider_revision: Vec<u8>,
    maximum_response_bytes: i64,
    state: String,
    prepared_at: time::OffsetDateTime,
    action_status: String,
    run_id: Uuid,
}

impl SettlementRow {
    fn request(
        &self,
    ) -> Result<
        SyncActionReconciliationRequest,
        SyncActionReconciliationError<PostgresSyncActionReconciliationError>,
    > {
        Ok(SyncActionReconciliationRequest {
            action_id: self.action_id,
            receiver_source_id: self.receiver_source_id,
            write_task_attempt_id: self.write_task_attempt_id,
            source_key: AdapterKey {
                namespace: self.source_key_namespace.clone(),
                value: self.source_key_value.clone(),
            },
            receiver_subject: parse_subject(
                &self.receiver_subject_kind,
                self.receiver_subject_namespace.clone(),
                self.receiver_subject_value.clone(),
            )?,
            provider_field: state_field(
                &self.provider_field_namespace,
                &self.provider_field_name,
                self.provider_unit_namespace.as_deref(),
                self.provider_unit_name.as_deref(),
            )?,
            intended_value: self.intended_value,
            before_membership: match self.before_membership.as_str() {
                "present" => AdapterSourceMembership::Present,
                "absent" => AdapterSourceMembership::Absent,
                _ => return Err(SyncActionReconciliationError::CorruptAction),
            },
            before_presence: parse_presence(&self.before_presence)?,
            before_value: self.before_value,
            before_provider_revision: self.before_provider_revision.clone(),
            operation_id: self.operation_id,
            capability: AdapterTargetedStateReadCapability {
                maximum_fields: 1,
                maximum_response_bytes: u64::try_from(self.maximum_response_bytes)
                    .map_err(|_| SyncActionReconciliationError::CorruptAction)?,
            },
        })
    }
}

#[derive(Debug, Error)]
pub enum PostgresSyncActionReconciliationError {
    #[error("sync action reconciliation database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl From<sqlx::Error> for SyncActionReconciliationError<PostgresSyncActionReconciliationError> {
    fn from(error: sqlx::Error) -> Self {
        Self::Repository(PostgresSyncActionReconciliationError::Database(error))
    }
}
