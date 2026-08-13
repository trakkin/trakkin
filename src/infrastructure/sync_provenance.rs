use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::{
            AdapterKey, AdapterSourceMembership, AdapterStatePresence,
            AdapterTargetedStateWriteCausation,
        },
        sync_action_reconciliation::SyncActionReconciliationResult,
        sync_preview::{SyncPreviewFingerprint, sync_preview_policy_revision_pin},
        sync_provenance::{
            SyncActionProvenanceRepository, SyncActionProvenanceSnapshot,
            SyncActionReconciliationProvenance, SyncProvenanceFuture,
            SyncReconciliationErrorProvenance, SyncReconciliationOutcomeKind,
            parse_preview_action_provenance,
        },
    },
    domain::acquisition::{CatalogRelationKey, ProviderItemKey, StateField, StateSubject, Term},
};

use super::sync_action_attempt::load_attempt;

#[derive(Clone)]
pub struct PostgresSyncActionProvenanceRepository {
    pool: PgPool,
}

impl PostgresSyncActionProvenanceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn get(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> Result<Option<SyncActionProvenanceSnapshot>, PostgresSyncActionProvenanceError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, ProvenanceRow>(
            "SELECT
    provenance.action_id,
    provenance.terminal_task_attempt_id,
    provenance.reconciliation_task_attempt_id,
    provenance.recorded_at,
    actions.workspace_id,
    actions.preview_action_hash,
    actions.preview_id,
    actions.run_id,
    runs.preview_hash,
    runs.created_at AS run_created_at,
    previews.policy_id,
    previews.policy_revision,
    previews.policy_content_hash,
    previews.fingerprint_json,
    preview_actions.canonical_json AS preview_action_json
FROM sync_action_provenance AS provenance
INNER JOIN sync_actions AS actions ON actions.id = provenance.action_id
INNER JOIN sync_runs AS runs ON runs.id = actions.run_id
INNER JOIN sync_previews AS previews ON previews.id = actions.preview_id
INNER JOIN sync_preview_actions AS preview_actions
    ON
        preview_actions.preview_id = actions.preview_id
        AND preview_actions.action_hash = actions.preview_action_hash
WHERE provenance.action_id = $1 AND actions.workspace_id = $2
",
        )
        .bind(action_id)
        .bind(workspace_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let fingerprint = SyncPreviewFingerprint::from_canonical_json(&row.fingerprint_json)
            .map_err(|_| PostgresSyncActionProvenanceError::CorruptProvenance)?;
        let policy_revision = u64::try_from(row.policy_revision)
            .map_err(|_| PostgresSyncActionProvenanceError::CorruptProvenance)?;
        if fingerprint.policy_revision
            != sync_preview_policy_revision_pin(policy_revision, &row.policy_content_hash)
        {
            return Err(PostgresSyncActionProvenanceError::CorruptProvenance);
        }
        let preview_action =
            parse_preview_action_provenance(&row.preview_action_hash, &row.preview_action_json)
                .ok_or(PostgresSyncActionProvenanceError::CorruptProvenance)?;
        let attempt = load_attempt(&mut transaction, row.terminal_task_attempt_id)
            .await
            .map_err(|_| PostgresSyncActionProvenanceError::CorruptProvenance)?;
        let reconciliation = match row.reconciliation_task_attempt_id {
            Some(task_attempt_id) => Some(
                load_reconciliation(&mut transaction, task_attempt_id)
                    .await
                    .map_err(|_| PostgresSyncActionProvenanceError::CorruptProvenance)?,
            ),
            None => None,
        };
        transaction.commit().await?;
        Ok(Some(SyncActionProvenanceSnapshot {
            workspace_id: row.workspace_id,
            policy_id: row.policy_id,
            policy_revision,
            policy_content_hash: row.policy_content_hash,
            state_registry_digest: fingerprint.state_registry_digest,
            preview_id: row.preview_id,
            preview_hash: row.preview_hash,
            preview_action_hash: row.preview_action_hash,
            preview_action,
            run_id: row.run_id,
            run_created_at: row.run_created_at,
            action_id: row.action_id,
            attempt,
            reconciliation,
            recorded_at: row.recorded_at,
        }))
    }
}

impl SyncActionProvenanceRepository for PostgresSyncActionProvenanceRepository {
    type Error = PostgresSyncActionProvenanceError;

    fn get_action_provenance(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> SyncProvenanceFuture<'_, Option<SyncActionProvenanceSnapshot>, Self::Error> {
        Box::pin(self.get(workspace_id, action_id))
    }
}

#[derive(Debug, Error)]
pub enum PostgresSyncActionProvenanceError {
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("stored synchronization action provenance is corrupt")]
    CorruptProvenance,
}

#[derive(FromRow)]
struct ProvenanceRow {
    action_id: Uuid,
    terminal_task_attempt_id: Uuid,
    reconciliation_task_attempt_id: Option<Uuid>,
    recorded_at: OffsetDateTime,
    workspace_id: Uuid,
    preview_action_hash: String,
    preview_id: Uuid,
    run_id: Uuid,
    preview_hash: String,
    run_created_at: OffsetDateTime,
    policy_id: Uuid,
    policy_revision: i64,
    policy_content_hash: String,
    fingerprint_json: Vec<u8>,
    preview_action_json: Vec<u8>,
}

async fn load_reconciliation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task_attempt_id: Uuid,
) -> Result<SyncActionReconciliationProvenance, PostgresSyncActionProvenanceError> {
    let row = sqlx::query_as::<_, ReconciliationRow>(
        "SELECT
    task_id, task_attempt_id, action_id, receiver_source_id,
    write_task_attempt_id, operation_id, idempotency_key,
    source_key_namespace, source_key_value,
    receiver_subject_kind, receiver_subject_namespace, receiver_subject_value,
    provider_field_namespace, provider_field_name,
    provider_unit_namespace, provider_unit_name, intended_value,
    before_membership, before_presence, before_value,
    before_provider_revision, maximum_response_bytes,
    outcome_kind, observed_membership, observed_presence, observed_value,
    observed_provider_revision, observed_at, expires_at,
    observed_precondition, causation_idempotency_key,
    causation_receipt, causation_provider_revision, result,
    outcome_error_code, outcome_error_retryable, outcome_error_safe_message,
    prepared_at, settled_at
FROM sync_action_reconciliation_attempts
WHERE task_attempt_id = $1 AND state = 'settled'
",
    )
    .bind(task_attempt_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(PostgresSyncActionProvenanceError::CorruptProvenance)?;
    row.into_provenance()
}

#[derive(FromRow)]
struct ReconciliationRow {
    task_id: Uuid,
    task_attempt_id: Uuid,
    action_id: Uuid,
    receiver_source_id: Uuid,
    write_task_attempt_id: Uuid,
    operation_id: Uuid,
    idempotency_key: Uuid,
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
    outcome_kind: String,
    observed_membership: Option<String>,
    observed_presence: Option<String>,
    observed_value: Option<i64>,
    observed_provider_revision: Option<Vec<u8>>,
    observed_at: Option<OffsetDateTime>,
    expires_at: Option<OffsetDateTime>,
    observed_precondition: Option<Vec<u8>>,
    causation_idempotency_key: Option<Uuid>,
    causation_receipt: Option<Vec<u8>>,
    causation_provider_revision: Option<Vec<u8>>,
    result: String,
    outcome_error_code: Option<String>,
    outcome_error_retryable: Option<bool>,
    outcome_error_safe_message: Option<String>,
    prepared_at: OffsetDateTime,
    settled_at: OffsetDateTime,
}

impl ReconciliationRow {
    fn into_provenance(
        self,
    ) -> Result<SyncActionReconciliationProvenance, PostgresSyncActionProvenanceError> {
        let provider_unit = match (self.provider_unit_namespace, self.provider_unit_name) {
            (Some(namespace), Some(name)) => Some(term(namespace, name)?),
            (None, None) => None,
            _ => return Err(PostgresSyncActionProvenanceError::CorruptProvenance),
        };
        let causation = match (
            self.causation_idempotency_key,
            self.causation_receipt,
            self.causation_provider_revision,
        ) {
            (Some(idempotency_key), Some(receipt), Some(provider_revision)) => {
                Some(AdapterTargetedStateWriteCausation {
                    idempotency_key: idempotency_key.as_bytes().to_vec(),
                    receipt,
                    provider_revision,
                })
            }
            (None, None, None) => None,
            _ => return Err(PostgresSyncActionProvenanceError::CorruptProvenance),
        };
        let error = match (
            self.outcome_error_code,
            self.outcome_error_retryable,
            self.outcome_error_safe_message,
        ) {
            (Some(code), Some(retryable), Some(safe_message)) => {
                Some(SyncReconciliationErrorProvenance {
                    code,
                    retryable,
                    safe_message,
                })
            }
            (None, None, None) => None,
            _ => return Err(PostgresSyncActionProvenanceError::CorruptProvenance),
        };
        Ok(SyncActionReconciliationProvenance {
            task_id: self.task_id,
            task_attempt_id: self.task_attempt_id,
            action_id: self.action_id,
            receiver_source_id: self.receiver_source_id,
            write_task_attempt_id: self.write_task_attempt_id,
            operation_id: self.operation_id,
            idempotency_key: self.idempotency_key,
            source_key: AdapterKey {
                namespace: self.source_key_namespace,
                value: self.source_key_value,
            },
            receiver_subject: subject(
                &self.receiver_subject_kind,
                self.receiver_subject_namespace,
                self.receiver_subject_value,
            )?,
            provider_field: StateField::new(
                term(self.provider_field_namespace, self.provider_field_name)?,
                provider_unit,
            ),
            intended_value: self.intended_value,
            before_membership: membership(&self.before_membership)?,
            before_presence: presence(&self.before_presence)?,
            before_value: self.before_value,
            before_provider_revision: self.before_provider_revision,
            maximum_response_bytes: u64::try_from(self.maximum_response_bytes)
                .map_err(|_| PostgresSyncActionProvenanceError::CorruptProvenance)?,
            outcome_kind: outcome_kind(&self.outcome_kind)?,
            observed_membership: self
                .observed_membership
                .as_deref()
                .map(membership)
                .transpose()?,
            observed_presence: self
                .observed_presence
                .as_deref()
                .map(presence)
                .transpose()?,
            observed_value: self.observed_value,
            observed_provider_revision: self.observed_provider_revision,
            observed_at: self.observed_at,
            expires_at: self.expires_at,
            observed_precondition: self.observed_precondition,
            causation,
            result: reconciliation_result(&self.result)?,
            error,
            prepared_at: self.prepared_at,
            settled_at: self.settled_at,
        })
    }
}

fn term(namespace: String, name: String) -> Result<Term, PostgresSyncActionProvenanceError> {
    Term::new(namespace, name).map_err(|_| PostgresSyncActionProvenanceError::CorruptProvenance)
}

fn subject(
    kind: &str,
    namespace: String,
    value: Vec<u8>,
) -> Result<StateSubject, PostgresSyncActionProvenanceError> {
    match kind {
        "provider_item" => ProviderItemKey::new(namespace, value)
            .map(StateSubject::ProviderItem)
            .map_err(|_| PostgresSyncActionProvenanceError::CorruptProvenance),
        "catalog_relation" => CatalogRelationKey::new(namespace, value)
            .map(StateSubject::CatalogRelation)
            .map_err(|_| PostgresSyncActionProvenanceError::CorruptProvenance),
        _ => Err(PostgresSyncActionProvenanceError::CorruptProvenance),
    }
}

fn membership(value: &str) -> Result<AdapterSourceMembership, PostgresSyncActionProvenanceError> {
    match value {
        "present" => Ok(AdapterSourceMembership::Present),
        "absent" => Ok(AdapterSourceMembership::Absent),
        "unknown" => Ok(AdapterSourceMembership::Unknown),
        _ => Err(PostgresSyncActionProvenanceError::CorruptProvenance),
    }
}

fn presence(value: &str) -> Result<AdapterStatePresence, PostgresSyncActionProvenanceError> {
    match value {
        "present" => Ok(AdapterStatePresence::Present),
        "deleted" => Ok(AdapterStatePresence::Deleted),
        "absent" => Ok(AdapterStatePresence::Absent),
        "unknown" => Ok(AdapterStatePresence::Unknown),
        "stale" => Ok(AdapterStatePresence::Stale),
        _ => Err(PostgresSyncActionProvenanceError::CorruptProvenance),
    }
}

fn outcome_kind(
    value: &str,
) -> Result<SyncReconciliationOutcomeKind, PostgresSyncActionProvenanceError> {
    match value {
        "matched" => Ok(SyncReconciliationOutcomeKind::Matched),
        "unsupported" => Ok(SyncReconciliationOutcomeKind::Unsupported),
        "not_found" => Ok(SyncReconciliationOutcomeKind::NotFound),
        "ambiguous" => Ok(SyncReconciliationOutcomeKind::Ambiguous),
        "indeterminate" => Ok(SyncReconciliationOutcomeKind::Indeterminate),
        _ => Err(PostgresSyncActionProvenanceError::CorruptProvenance),
    }
}

fn reconciliation_result(
    value: &str,
) -> Result<SyncActionReconciliationResult, PostgresSyncActionProvenanceError> {
    match value {
        "confirmed_applied" => Ok(SyncActionReconciliationResult::ConfirmedApplied),
        "confirmed_not_applied" => Ok(SyncActionReconciliationResult::ConfirmedNotApplied),
        "state_matches_causation_unknown" => {
            Ok(SyncActionReconciliationResult::StateMatchesCausationUnknown)
        }
        "unresolved" => Ok(SyncActionReconciliationResult::Unresolved),
        _ => Err(PostgresSyncActionProvenanceError::CorruptProvenance),
    }
}
