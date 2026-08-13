use std::{collections::BTreeSet, future::Future, pin::Pin};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    acquisition::{StateField, StateSubject},
    synchronization::{ExactRational, Quantizer, Reducer, StateUnit},
};

use super::{
    adapter_runtime::{
        AdapterKey, AdapterSourceMembership, AdapterStatePresence,
        AdapterTargetedStateWriteCausation, AdapterTargetedStateWriteStatus,
    },
    sync_action_attempt::{SyncActionAttemptSnapshot, SyncActionAttemptState},
    sync_action_reconciliation::SyncActionReconciliationResult,
    sync_preview::{PreviewMembershipEffect, PublishedStatePresence},
};

pub type SyncProvenanceFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncContributorProvenance {
    pub evidence_id: String,
    pub state_publication_id: Uuid,
    pub presence: PublishedStatePresence,
    pub mapping_proof_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncMappingProofProvenance {
    pub key: String,
    pub revision_hash: String,
    pub row_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncTranslationProvenance {
    pub unit: StateUnit,
    pub quantizer: Quantizer,
    pub exact_value: ExactRational,
    pub intended_value: u64,
    pub lossy: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewActionProvenance {
    pub reducer: Reducer,
    pub contributors: Vec<SyncContributorProvenance>,
    pub mapping_proofs: Vec<SyncMappingProofProvenance>,
    pub translation: SyncTranslationProvenance,
    pub membership_effect: PreviewMembershipEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncReconciliationOutcomeKind {
    Matched,
    Unsupported,
    NotFound,
    Ambiguous,
    Indeterminate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncReconciliationErrorProvenance {
    pub code: String,
    pub retryable: bool,
    pub safe_message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActionReconciliationProvenance {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub action_id: Uuid,
    pub receiver_source_id: Uuid,
    pub write_task_attempt_id: Uuid,
    pub operation_id: Uuid,
    pub idempotency_key: Uuid,
    pub source_key: AdapterKey,
    pub receiver_subject: StateSubject,
    pub provider_field: StateField,
    pub intended_value: i64,
    pub before_membership: AdapterSourceMembership,
    pub before_presence: AdapterStatePresence,
    pub before_value: Option<i64>,
    pub before_provider_revision: Vec<u8>,
    pub maximum_response_bytes: u64,
    pub outcome_kind: SyncReconciliationOutcomeKind,
    pub observed_membership: Option<AdapterSourceMembership>,
    pub observed_presence: Option<AdapterStatePresence>,
    pub observed_value: Option<i64>,
    pub observed_provider_revision: Option<Vec<u8>>,
    pub observed_at: Option<OffsetDateTime>,
    pub expires_at: Option<OffsetDateTime>,
    pub observed_precondition: Option<Vec<u8>>,
    pub causation: Option<AdapterTargetedStateWriteCausation>,
    pub result: SyncActionReconciliationResult,
    pub error: Option<SyncReconciliationErrorProvenance>,
    pub prepared_at: OffsetDateTime,
    pub settled_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncActionProvenanceSnapshot {
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub policy_revision: u64,
    pub policy_content_hash: String,
    pub state_registry_digest: String,
    pub preview_id: Uuid,
    pub preview_hash: String,
    pub preview_action_hash: String,
    pub preview_action: SyncPreviewActionProvenance,
    pub run_id: Uuid,
    pub run_created_at: OffsetDateTime,
    pub action_id: Uuid,
    pub attempt: SyncActionAttemptSnapshot,
    pub reconciliation: Option<SyncActionReconciliationProvenance>,
    pub recorded_at: OffsetDateTime,
}

impl SyncActionProvenanceSnapshot {
    #[must_use]
    pub fn receipt_edge(&self) -> Option<&[u8]> {
        self.attempt
            .outcome
            .as_ref()
            .filter(|outcome| outcome.status == AdapterTargetedStateWriteStatus::Applied)
            .map(|outcome| outcome.receipt.as_slice())
            .filter(|receipt| !receipt.is_empty())
    }

    #[must_use]
    pub fn reconciliation_edge(&self) -> Option<&SyncActionReconciliationProvenance> {
        self.reconciliation.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncActionProvenanceError<E> {
    NotFound,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncActionProvenanceRepository: Send + Sync {
    type Error;

    fn get_action_provenance(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> SyncProvenanceFuture<'_, Option<SyncActionProvenanceSnapshot>, Self::Error>;
}

#[derive(Clone)]
pub struct SyncActionProvenanceService<R> {
    repository: R,
}

impl<R> SyncActionProvenanceService<R>
where
    R: SyncActionProvenanceRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn get(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> Result<SyncActionProvenanceSnapshot, SyncActionProvenanceError<R::Error>> {
        let snapshot = self
            .repository
            .get_action_provenance(workspace_id, action_id)
            .await
            .map_err(SyncActionProvenanceError::Repository)?
            .ok_or(SyncActionProvenanceError::NotFound)?;
        validate_snapshot(&snapshot, workspace_id, action_id)?;
        Ok(snapshot)
    }
}

fn validate_snapshot<E>(
    snapshot: &SyncActionProvenanceSnapshot,
    workspace_id: Uuid,
    action_id: Uuid,
) -> Result<(), SyncActionProvenanceError<E>> {
    let proof_keys = snapshot
        .preview_action
        .mapping_proofs
        .iter()
        .map(|proof| proof.key.as_str())
        .collect::<BTreeSet<_>>();
    let valid_outcome = match (&snapshot.attempt.outcome, &snapshot.reconciliation) {
        (Some(_), None) => snapshot.attempt.settled_at == Some(snapshot.recorded_at),
        (None, Some(reconciliation)) => {
            snapshot.attempt.requires_reconciliation()
                && reconciliation.action_id == action_id
                && reconciliation.write_task_attempt_id == snapshot.attempt.task_attempt_id
                && reconciliation.idempotency_key == action_id
                && matches!(
                    reconciliation.result,
                    SyncActionReconciliationResult::ConfirmedApplied
                        | SyncActionReconciliationResult::ConfirmedNotApplied
                )
                && reconciliation.settled_at == snapshot.recorded_at
        }
        _ => false,
    };
    if snapshot.workspace_id != workspace_id
        || snapshot.action_id != action_id
        || snapshot.policy_revision == 0
        || snapshot.preview_action.contributors.is_empty()
        || snapshot.preview_action.mapping_proofs.is_empty()
        || snapshot
            .preview_action
            .contributors
            .iter()
            .any(|contributor| !proof_keys.contains(contributor.mapping_proof_key.as_str()))
        || snapshot.attempt.state != SyncActionAttemptState::Settled
        || snapshot.attempt.request.action_id != action_id
        || snapshot.attempt.request.preview_action_hash != snapshot.preview_action_hash
        || !valid_outcome
        || snapshot.run_created_at > snapshot.recorded_at
    {
        return Err(SyncActionProvenanceError::RepositoryInvariant);
    }
    Ok(())
}

pub(crate) fn parse_preview_action_provenance(
    action_hash: &str,
    canonical_json: &[u8],
) -> Option<SyncPreviewActionProvenance> {
    if format!("sha256:{:x}", Sha256::digest(canonical_json)) != action_hash {
        return None;
    }
    let stored = serde_json::from_slice::<StoredPreviewAction>(canonical_json).ok()?;
    let exact_value = ExactRational::new(
        stored.translation.exact_value.numerator,
        stored.translation.exact_value.denominator,
    )
    .ok()?;
    let mapping_proofs = stored
        .aggregation
        .mapping_proofs
        .into_iter()
        .map(|proof| SyncMappingProofProvenance {
            key: proof.key,
            revision_hash: proof.revision_hash,
            row_hash: proof.row_hash,
        })
        .collect::<Vec<_>>();
    let contributors = stored
        .aggregation
        .contributors
        .into_iter()
        .map(|contributor| SyncContributorProvenance {
            evidence_id: contributor.evidence_id,
            state_publication_id: contributor.state_publication_id,
            presence: contributor.presence,
            mapping_proof_key: contributor.mapping_proof_key,
        })
        .collect::<Vec<_>>();
    Some(SyncPreviewActionProvenance {
        reducer: stored.aggregation.reducer,
        contributors,
        mapping_proofs,
        translation: SyncTranslationProvenance {
            unit: stored.translation.unit,
            quantizer: stored.translation.quantizer,
            exact_value,
            intended_value: stored.translation.intended_value,
            lossy: stored.translation.lossy,
        },
        membership_effect: stored.membership_effect,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredPreviewAction {
    aggregation: StoredAggregation,
    translation: StoredTranslation,
    membership_effect: PreviewMembershipEffect,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredAggregation {
    reducer: Reducer,
    contributors: Vec<StoredContributor>,
    mapping_proofs: Vec<StoredMappingProof>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredContributor {
    evidence_id: String,
    state_publication_id: Uuid,
    presence: PublishedStatePresence,
    mapping_proof_key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredMappingProof {
    key: String,
    revision_hash: String,
    row_hash: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredTranslation {
    unit: StateUnit,
    quantizer: Quantizer,
    exact_value: StoredRational,
    intended_value: u64,
    lossy: bool,
}

#[derive(Deserialize)]
struct StoredRational {
    numerator: u64,
    denominator: u64,
}
