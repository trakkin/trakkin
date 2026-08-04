use std::{cmp::Ordering, collections::BTreeMap, future::Future, pin::Pin};

use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use trakkin_mapping_contract::PortableEndpoint;
use uuid::Uuid;

use crate::domain::{
    acquisition::{StateField, StateSubject, StateValue},
    synchronization::{
        ExactRational, OrdinalSpan, PresenceDisposition, Quantizer, Reducer, StateRegistry,
        StateUnit, SyncComputationError, aggregate_contiguous_prefix, quantize_partition_coverage,
    },
};

use super::{
    adapter_runtime::{
        AdapterSourceMembership, AdapterStatePresence, AdapterTargetedStateMatch,
        AdapterTargetedStateReadCapability, AdapterTargetedStateReadOutcome,
    },
    mapping_resolution::ResolutionProof,
    sync_policy::SyncPolicyRevisionSnapshot,
};

pub const SYNC_PREVIEW_SCHEMA: &str = "trakkin.sync-preview/v1";

pub type SyncPreviewFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, SyncPreviewLifecycleError<E>>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SyncPreviewMembershipPin {
    Present,
    Absent,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SyncPreviewFingerprint {
    pub policy_revision: String,
    pub state_registry_digest: String,
    pub resolver_version: String,
    pub active_mapping_hashes: Vec<String>,
    pub mapping_activation_generation: u64,
    pub mapping_proof_digest: String,
    pub attachment_digest: String,
    pub source_capability_revision: String,
    pub connection_capability_revision: String,
    pub state_publication: String,
    pub contributor_evidence_revisions: Vec<String>,
    pub catalog_publication: String,
    pub coordinate_publication: String,
    pub lookup_process: String,
    pub lookup_revision: String,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub lookup_observed_at: OffsetDateTime,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub lookup_expiry: OffsetDateTime,
    pub targeted_membership: SyncPreviewMembershipPin,
    pub targeted_state_revision: String,
    pub targeted_precondition: String,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub targeted_expiry: OffsetDateTime,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub preview_time: OffsetDateTime,
    #[serde(deserialize_with = "deserialize_rfc3339")]
    pub preview_expiry: OffsetDateTime,
}

impl SyncPreviewFingerprint {
    pub fn canonical_json(&self) -> Result<Vec<u8>, SyncPreviewFingerprintError> {
        validate_fingerprint(self)?;
        let mut active_mapping_hashes = self.active_mapping_hashes.clone();
        active_mapping_hashes.sort_unstable();
        let mut contributor_evidence_revisions = self.contributor_evidence_revisions.clone();
        contributor_evidence_revisions.sort_unstable();
        Ok(canonical_bytes(&json!({
            "activeMappingHashes": active_mapping_hashes,
            "attachmentDigest": self.attachment_digest,
            "catalogPublication": self.catalog_publication,
            "connectionCapabilityRevision": self.connection_capability_revision,
            "contributorEvidenceRevisions": contributor_evidence_revisions,
            "coordinatePublication": self.coordinate_publication,
            "lookupExpiry": canonical_time(self.lookup_expiry),
            "lookupObservedAt": canonical_time(self.lookup_observed_at),
            "lookupProcess": self.lookup_process,
            "lookupRevision": self.lookup_revision,
            "mappingActivationGeneration": self.mapping_activation_generation,
            "mappingProofDigest": self.mapping_proof_digest,
            "policyRevision": self.policy_revision,
            "previewExpiry": canonical_time(self.preview_expiry),
            "previewTime": canonical_time(self.preview_time),
            "resolverVersion": self.resolver_version,
            "sourceCapabilityRevision": self.source_capability_revision,
            "statePublication": self.state_publication,
            "stateRegistryDigest": self.state_registry_digest,
            "targetedExpiry": canonical_time(self.targeted_expiry),
            "targetedMembership": match self.targeted_membership {
                SyncPreviewMembershipPin::Present => "present",
                SyncPreviewMembershipPin::Absent => "absent",
                SyncPreviewMembershipPin::Unknown => "unknown",
            },
            "targetedPrecondition": self.targeted_precondition,
            "targetedStateRevision": self.targeted_state_revision,
        })))
    }

    pub fn content_hash(&self) -> Result<String, SyncPreviewFingerprintError> {
        self.canonical_json().map(|bytes| sha256_hash(&bytes))
    }

    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, SyncPreviewFingerprintError> {
        let fingerprint = serde_json::from_slice::<Self>(bytes)
            .map_err(|_| SyncPreviewFingerprintError::Malformed)?;
        if fingerprint.canonical_json()?.as_slice() != bytes {
            return Err(SyncPreviewFingerprintError::NonCanonical);
        }
        Ok(fingerprint)
    }

    #[must_use]
    pub fn fresh_until(&self) -> OffsetDateTime {
        self.lookup_expiry
            .min(self.targeted_expiry)
            .min(self.preview_expiry)
    }
}

#[must_use]
pub fn sync_preview_policy_revision_pin(revision: u64, content_hash: &str) -> String {
    format!("{content_hash}@{revision}")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPreviewFreshness {
    Fresh,
    Stale,
}

#[must_use]
pub fn evaluate_sync_preview_freshness(
    expected: &SyncPreviewFingerprint,
    current: &SyncPreviewFingerprint,
    checked_at: OffsetDateTime,
) -> SyncPreviewFreshness {
    let exact_match = expected
        .canonical_json()
        .ok()
        .zip(current.canonical_json().ok())
        .is_some_and(|(expected, current)| expected == current);
    if !exact_match
        || checked_at < expected.preview_time
        || checked_at >= expected.lookup_expiry
        || checked_at >= expected.targeted_expiry
        || checked_at >= expected.preview_expiry
    {
        SyncPreviewFreshness::Stale
    } else {
        SyncPreviewFreshness::Fresh
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SyncPreviewFingerprintError {
    #[error("synchronization preview fingerprint is malformed")]
    Malformed,
    #[error("synchronization preview fingerprint is not canonical")]
    NonCanonical,
    #[error("synchronization preview fingerprint field {field} is empty")]
    EmptyField { field: &'static str },
    #[error("synchronization preview fingerprint list {field} is empty")]
    EmptyList { field: &'static str },
    #[error("synchronization preview fingerprint list {field} contains duplicates")]
    DuplicateListValue { field: &'static str },
    #[error("synchronization preview fingerprint expiry {field} is invalid")]
    InvalidExpiry { field: &'static str },
    #[error("synchronization preview fingerprint time {field} exceeds microsecond precision")]
    UnsupportedTimePrecision { field: &'static str },
}

fn validate_fingerprint(
    fingerprint: &SyncPreviewFingerprint,
) -> Result<(), SyncPreviewFingerprintError> {
    for (field, value) in [
        ("policyRevision", fingerprint.policy_revision.as_str()),
        (
            "stateRegistryDigest",
            fingerprint.state_registry_digest.as_str(),
        ),
        ("resolverVersion", fingerprint.resolver_version.as_str()),
        (
            "mappingProofDigest",
            fingerprint.mapping_proof_digest.as_str(),
        ),
        ("attachmentDigest", fingerprint.attachment_digest.as_str()),
        (
            "sourceCapabilityRevision",
            fingerprint.source_capability_revision.as_str(),
        ),
        (
            "connectionCapabilityRevision",
            fingerprint.connection_capability_revision.as_str(),
        ),
        ("statePublication", fingerprint.state_publication.as_str()),
        (
            "catalogPublication",
            fingerprint.catalog_publication.as_str(),
        ),
        (
            "coordinatePublication",
            fingerprint.coordinate_publication.as_str(),
        ),
        ("lookupProcess", fingerprint.lookup_process.as_str()),
        ("lookupRevision", fingerprint.lookup_revision.as_str()),
        (
            "targetedStateRevision",
            fingerprint.targeted_state_revision.as_str(),
        ),
        (
            "targetedPrecondition",
            fingerprint.targeted_precondition.as_str(),
        ),
    ] {
        if value.is_empty() {
            return Err(SyncPreviewFingerprintError::EmptyField { field });
        }
    }
    validate_fingerprint_list("activeMappingHashes", &fingerprint.active_mapping_hashes)?;
    validate_fingerprint_list(
        "contributorEvidenceRevisions",
        &fingerprint.contributor_evidence_revisions,
    )?;
    for (field, value) in [
        ("lookupObservedAt", fingerprint.lookup_observed_at),
        ("lookupExpiry", fingerprint.lookup_expiry),
        ("targetedExpiry", fingerprint.targeted_expiry),
        ("previewTime", fingerprint.preview_time),
        ("previewExpiry", fingerprint.preview_expiry),
    ] {
        if value.nanosecond() % 1_000 != 0 {
            return Err(SyncPreviewFingerprintError::UnsupportedTimePrecision { field });
        }
    }
    if fingerprint.lookup_expiry <= fingerprint.lookup_observed_at
        || fingerprint.lookup_expiry <= fingerprint.preview_time
    {
        return Err(SyncPreviewFingerprintError::InvalidExpiry {
            field: "lookupExpiry",
        });
    }
    if fingerprint.targeted_expiry <= fingerprint.preview_time {
        return Err(SyncPreviewFingerprintError::InvalidExpiry {
            field: "targetedExpiry",
        });
    }
    if fingerprint.preview_expiry <= fingerprint.preview_time {
        return Err(SyncPreviewFingerprintError::InvalidExpiry {
            field: "previewExpiry",
        });
    }
    Ok(())
}

fn validate_fingerprint_list(
    field: &'static str,
    values: &[String],
) -> Result<(), SyncPreviewFingerprintError> {
    if values.is_empty() {
        return Err(SyncPreviewFingerprintError::EmptyList { field });
    }
    if values.iter().any(String::is_empty) {
        return Err(SyncPreviewFingerprintError::EmptyField { field });
    }
    let mut normalized = values.to_vec();
    normalized.sort_unstable();
    if normalized.windows(2).any(|values| values[0] == values[1]) {
        return Err(SyncPreviewFingerprintError::DuplicateListValue { field });
    }
    Ok(())
}

fn deserialize_rfc3339<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    OffsetDateTime::parse(&value, &Rfc3339).map_err(serde::de::Error::custom)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublishedStatePresence {
    Present,
    Deleted,
    Absent,
    Unknown,
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedSyncContributor {
    pub evidence_id: String,
    pub source_id: Uuid,
    pub subject: StateSubject,
    pub semantic_field: String,
    pub root: PortableEndpoint,
    pub presence: PublishedStatePresence,
    pub coverage: Vec<OrdinalSpan>,
    pub state_publication_id: Uuid,
    pub provider_revision: Vec<u8>,
    pub mapping_proof_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiverProgressCapability {
    pub unit: StateUnit,
    pub minimum: u64,
    pub maximum: u64,
    pub step: u64,
    pub quantizer: Quantizer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetedReceiverPreflight {
    pub source_id: Uuid,
    pub subject: StateSubject,
    pub state_read_capability: AdapterTargetedStateReadCapability,
    pub progress_capability: ReceiverProgressCapability,
    pub outcome: AdapterTargetedStateReadOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewPartitionInput {
    pub semantic_field: String,
    pub segment: OrdinalSpan,
    pub mapping_proofs: Vec<ResolutionProof>,
    pub contributors: Vec<PublishedSyncContributor>,
    pub receivers: Vec<TargetedReceiverPreflight>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConstructSyncPreview {
    pub policy: SyncPolicyRevisionSnapshot,
    pub partitions: Vec<SyncPreviewPartitionInput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncAggregationExplanation {
    pub reducer: Reducer,
    pub contributors: Vec<PublishedSyncContributor>,
    pub mapping_proofs: Vec<ResolutionProof>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewCandidate {
    pub semantic_field: String,
    pub segment: OrdinalSpan,
    pub value: ExactRational,
    pub explanation: SyncAggregationExplanation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PreviewMembershipEffect {
    Create,
    Update,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncTranslationExplanation {
    pub unit: StateUnit,
    pub receiver_minimum: u64,
    pub receiver_maximum: u64,
    pub receiver_step: u64,
    pub quantizer: Quantizer,
    pub exact_value: ExactRational,
    pub intended_value: u64,
    pub lossy: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewAction {
    pub receiver_source_id: Uuid,
    pub receiver_subject: StateSubject,
    pub semantic_field: String,
    pub segment: OrdinalSpan,
    pub candidate: ExactRational,
    pub intended_value: u64,
    pub lossy: bool,
    pub membership_effect: PreviewMembershipEffect,
    pub targeted_read_capability: AdapterTargetedStateReadCapability,
    pub receiver_capability: ReceiverProgressCapability,
    pub targeted_state: AdapterTargetedStateMatch,
    pub aggregation: SyncAggregationExplanation,
    pub translation: SyncTranslationExplanation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewDraft {
    pub policy_id: Uuid,
    pub policy_revision: u64,
    pub policy_content_hash: String,
    pub candidates: Vec<SyncPreviewCandidate>,
    pub actions: Vec<SyncPreviewAction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSyncPreviewSnapshot {
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub draft: SyncPreviewDraft,
    pub fingerprint: SyncPreviewFingerprint,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewSnapshotAction {
    pub ordinal: u32,
    pub action_hash: String,
    pub receiver_source_id: Uuid,
    pub receiver_subject: StateSubject,
    pub semantic_field: String,
    pub segment: OrdinalSpan,
    pub canonical_json: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewSnapshot {
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub policy_revision: u64,
    pub policy_content_hash: String,
    pub content_hash: String,
    pub canonical_json: Vec<u8>,
    pub fingerprint_hash: String,
    pub fingerprint_json: Vec<u8>,
    pub fresh_until: OffsetDateTime,
    pub actions: Vec<SyncPreviewSnapshotAction>,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

impl SyncPreviewSnapshot {
    pub fn from_draft(
        mut command: CreateSyncPreviewSnapshot,
    ) -> Result<Self, SyncPreviewSnapshotError> {
        if command.expires_at <= command.created_at {
            return Err(SyncPreviewSnapshotError::InvalidExpiry);
        }
        if command.draft.policy_revision == 0 {
            return Err(SyncPreviewSnapshotError::InvalidPolicyRevision);
        }
        crate::domain::synchronization::validate_sync_policy_hash(
            &command.draft.policy_content_hash,
        )
        .map_err(|_| SyncPreviewSnapshotError::InvalidPolicyHash)?;
        if command.fingerprint.policy_revision
            != sync_preview_policy_revision_pin(
                command.draft.policy_revision,
                &command.draft.policy_content_hash,
            )
            || command.fingerprint.preview_time != command.created_at
            || command.fingerprint.preview_expiry != command.expires_at
        {
            return Err(SyncPreviewSnapshotError::FingerprintDoesNotMatchPreview);
        }
        let fingerprint_json = command.fingerprint.canonical_json()?;
        let fingerprint_hash = sha256_hash(&fingerprint_json);
        let fresh_until = command.fingerprint.fresh_until();
        normalize_draft(&mut command.draft)?;
        command
            .draft
            .candidates
            .sort_unstable_by(compare_candidates);
        if command
            .draft
            .candidates
            .windows(2)
            .any(|candidates| compare_candidates(&candidates[0], &candidates[1]).is_eq())
        {
            return Err(SyncPreviewSnapshotError::DuplicateCandidate);
        }
        command.draft.actions.sort_unstable_by(compare_actions);
        if command
            .draft
            .actions
            .windows(2)
            .any(|actions| compare_actions(&actions[0], &actions[1]).is_eq())
        {
            return Err(SyncPreviewSnapshotError::DuplicateReceiverAction);
        }

        let actions = command
            .draft
            .actions
            .iter()
            .enumerate()
            .map(|(ordinal, action)| {
                let canonical_json = canonical_bytes(&action_value(action));
                Ok(SyncPreviewSnapshotAction {
                    ordinal: u32::try_from(ordinal)
                        .map_err(|_| SyncPreviewSnapshotError::TooManyActions)?,
                    action_hash: sha256_hash(&canonical_json),
                    receiver_source_id: action.receiver_source_id,
                    receiver_subject: action.receiver_subject.clone(),
                    semantic_field: action.semantic_field.clone(),
                    segment: action.segment.clone(),
                    canonical_json,
                })
            })
            .collect::<Result<Vec<_>, SyncPreviewSnapshotError>>()?;
        let mut action_hashes = actions
            .iter()
            .map(|action| action.action_hash.as_str())
            .collect::<Vec<_>>();
        action_hashes.sort_unstable();
        if action_hashes
            .windows(2)
            .any(|hashes| hashes[0] == hashes[1])
        {
            return Err(SyncPreviewSnapshotError::DuplicateAction);
        }

        let action_values = actions
            .iter()
            .map(|action| {
                let mut value = serde_json::from_slice::<Value>(&action.canonical_json)
                    .expect("typed synchronization action serializes");
                value
                    .as_object_mut()
                    .expect("synchronization action is an object")
                    .insert("actionHash".to_owned(), json!(action.action_hash));
                value
                    .as_object_mut()
                    .expect("synchronization action is an object")
                    .insert("ordinal".to_owned(), json!(action.ordinal));
                value
            })
            .collect::<Vec<_>>();
        let preview_value = json!({
            "actions": action_values,
            "candidates": command
                .draft
                .candidates
                .iter()
                .map(candidate_value)
                .collect::<Vec<_>>(),
            "createdAt": canonical_time(command.created_at),
            "expiresAt": canonical_time(command.expires_at),
            "fingerprint": serde_json::from_slice::<Value>(&fingerprint_json)
                .expect("typed synchronization fingerprint serializes"),
            "fingerprintHash": fingerprint_hash,
            "freshUntil": canonical_time(fresh_until),
            "policyContentHash": command.draft.policy_content_hash,
            "policyId": command.draft.policy_id,
            "policyRevision": command.draft.policy_revision,
            "schema": SYNC_PREVIEW_SCHEMA,
            "workspaceId": command.workspace_id,
        });
        let canonical_json = canonical_bytes(&preview_value);

        Ok(Self {
            preview_id: command.preview_id,
            workspace_id: command.workspace_id,
            policy_id: command.draft.policy_id,
            policy_revision: command.draft.policy_revision,
            policy_content_hash: command.draft.policy_content_hash,
            content_hash: sha256_hash(&canonical_json),
            canonical_json,
            fingerprint_hash,
            fingerprint_json,
            fresh_until,
            actions,
            created_at: command.created_at,
            expires_at: command.expires_at,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckSyncPreviewFreshness {
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub preview_hash: String,
    pub current_fingerprint: SyncPreviewFingerprint,
    pub checked_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewFreshnessSnapshot {
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub preview_hash: String,
    pub fingerprint_hash: String,
    pub status: SyncPreviewFreshness,
    pub checked_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncPreviewLifecycleError<E> {
    InvalidSnapshot(SyncPreviewSnapshotError),
    PreviewNotFound,
    PreviewHashMismatch,
    PreviewExpired,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncPreviewRepository: Send + Sync {
    type Error;

    fn get_preview(
        &self,
        workspace_id: Uuid,
        preview_id: Uuid,
    ) -> SyncPreviewFuture<'_, Option<SyncPreviewSnapshot>, Self::Error>;

    fn create_preview(
        &self,
        snapshot: SyncPreviewSnapshot,
    ) -> SyncPreviewFuture<'_, SyncPreviewSnapshot, Self::Error>;
}

#[derive(Clone)]
pub struct SyncPreviewService<R> {
    repository: R,
}

impl<R> SyncPreviewService<R>
where
    R: SyncPreviewRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn get(
        &self,
        workspace_id: Uuid,
        preview_id: Uuid,
    ) -> Result<SyncPreviewSnapshot, SyncPreviewLifecycleError<R::Error>> {
        let snapshot = self
            .repository
            .get_preview(workspace_id, preview_id)
            .await?
            .ok_or(SyncPreviewLifecycleError::PreviewNotFound)?;
        validate_preview_snapshot(snapshot, workspace_id, preview_id)
    }

    pub async fn create(
        &self,
        command: CreateSyncPreviewSnapshot,
    ) -> Result<SyncPreviewSnapshot, SyncPreviewLifecycleError<R::Error>> {
        let workspace_id = command.workspace_id;
        let preview_id = command.preview_id;
        let snapshot = SyncPreviewSnapshot::from_draft(command)
            .map_err(SyncPreviewLifecycleError::InvalidSnapshot)?;
        let stored = self.repository.create_preview(snapshot).await?;
        validate_preview_snapshot(stored, workspace_id, preview_id)
    }

    pub async fn recheck(
        &self,
        command: CheckSyncPreviewFreshness,
    ) -> Result<SyncPreviewFreshnessSnapshot, SyncPreviewLifecycleError<R::Error>> {
        let snapshot = self.get(command.workspace_id, command.preview_id).await?;
        if snapshot.content_hash != command.preview_hash {
            return Err(SyncPreviewLifecycleError::PreviewHashMismatch);
        }
        let current_fingerprint = command.current_fingerprint.canonical_json().ok();
        let status = if current_fingerprint.as_deref() == Some(snapshot.fingerprint_json.as_slice())
            && command.checked_at >= snapshot.created_at
            && command.checked_at < snapshot.fresh_until
        {
            SyncPreviewFreshness::Fresh
        } else {
            SyncPreviewFreshness::Stale
        };
        Ok(SyncPreviewFreshnessSnapshot {
            preview_id: snapshot.preview_id,
            workspace_id: snapshot.workspace_id,
            preview_hash: snapshot.content_hash,
            fingerprint_hash: snapshot.fingerprint_hash,
            status,
            checked_at: command.checked_at,
        })
    }
}

fn validate_preview_snapshot<E>(
    snapshot: SyncPreviewSnapshot,
    workspace_id: Uuid,
    preview_id: Uuid,
) -> Result<SyncPreviewSnapshot, SyncPreviewLifecycleError<E>> {
    if snapshot.workspace_id != workspace_id
        || snapshot.preview_id != preview_id
        || snapshot.policy_revision == 0
        || snapshot.expires_at <= snapshot.created_at
        || snapshot.content_hash != sha256_hash(&snapshot.canonical_json)
        || snapshot.fingerprint_hash != sha256_hash(&snapshot.fingerprint_json)
        || snapshot.fresh_until <= snapshot.created_at
        || snapshot.fresh_until > snapshot.expires_at
    {
        return Err(SyncPreviewLifecycleError::RepositoryInvariant);
    }
    crate::domain::synchronization::validate_sync_policy_hash(&snapshot.policy_content_hash)
        .map_err(|_| SyncPreviewLifecycleError::RepositoryInvariant)?;
    for (ordinal, action) in snapshot.actions.iter().enumerate() {
        if action.ordinal != u32::try_from(ordinal).unwrap_or(u32::MAX)
            || action.action_hash != sha256_hash(&action.canonical_json)
        {
            return Err(SyncPreviewLifecycleError::RepositoryInvariant);
        }
    }
    Ok(snapshot)
}

fn candidate_value(candidate: &SyncPreviewCandidate) -> Value {
    json!({
        "aggregation": aggregation_value(&candidate.explanation),
        "segment": span_value(&candidate.segment),
        "semanticField": candidate.semantic_field,
        "value": rational_value(candidate.value),
    })
}

fn action_value(action: &SyncPreviewAction) -> Value {
    json!({
        "aggregation": aggregation_value(&action.aggregation),
        "candidate": rational_value(action.candidate),
        "intendedValue": action.intended_value,
        "lossy": action.lossy,
        "membershipEffect": match action.membership_effect {
            PreviewMembershipEffect::Create => "create",
            PreviewMembershipEffect::Update => "update",
        },
        "receiverCapability": receiver_capability_value(&action.receiver_capability),
        "receiverSourceId": action.receiver_source_id,
        "receiverSubject": subject_value(&action.receiver_subject),
        "segment": span_value(&action.segment),
        "semanticField": action.semantic_field,
        "targetedReadCapability": {
            "maximumFields": action.targeted_read_capability.maximum_fields,
            "maximumResponseBytes": action.targeted_read_capability.maximum_response_bytes,
        },
        "targetedState": targeted_state_value(&action.targeted_state),
        "translation": translation_value(&action.translation),
    })
}

fn aggregation_value(explanation: &SyncAggregationExplanation) -> Value {
    json!({
        "contributors": explanation
            .contributors
            .iter()
            .map(contributor_value)
            .collect::<Vec<_>>(),
        "mappingProofs": explanation
            .mapping_proofs
            .iter()
            .map(mapping_proof_value)
            .collect::<Vec<_>>(),
        "reducer": explanation.reducer.as_str(),
    })
}

fn contributor_value(contributor: &PublishedSyncContributor) -> Value {
    json!({
        "coverage": contributor.coverage.iter().map(span_value).collect::<Vec<_>>(),
        "evidenceId": contributor.evidence_id,
        "mappingProofKey": contributor.mapping_proof_key,
        "presence": match contributor.presence {
            PublishedStatePresence::Present => "present",
            PublishedStatePresence::Deleted => "deleted",
            PublishedStatePresence::Absent => "absent",
            PublishedStatePresence::Unknown => "unknown",
            PublishedStatePresence::Stale => "stale",
        },
        "providerRevision": contributor.provider_revision,
        "root": contributor.root.as_str(),
        "semanticField": contributor.semantic_field,
        "sourceId": contributor.source_id,
        "statePublicationId": contributor.state_publication_id,
        "subject": subject_value(&contributor.subject),
    })
}

fn mapping_proof_value(proof: &ResolutionProof) -> Value {
    json!({
        "catalogPublications": proof.catalog_publications,
        "coordinatePublications": proof.coordinate_publications,
        "endpointHash": proof.endpoint_hash,
        "key": proof.key,
        "kind": match proof.kind {
            super::mapping_resolution::ResolutionProofKind::Direct => "direct",
            super::mapping_resolution::ResolutionProofKind::Derived => "derived",
        },
        "parentProofs": proof.parent_proofs,
        "revisionHash": proof.revision_hash,
        "rowHash": proof.row_hash,
        "transform": {
            "denominator": proof.transform.denominator,
            "numerator": proof.transform.numerator,
        },
    })
}

fn receiver_capability_value(capability: &ReceiverProgressCapability) -> Value {
    json!({
        "maximum": capability.maximum,
        "minimum": capability.minimum,
        "quantizer": capability.quantizer.as_str(),
        "step": capability.step,
        "unit": capability.unit.as_str(),
    })
}

fn targeted_state_value(targeted_state: &AdapterTargetedStateMatch) -> Value {
    json!({
        "expiresAt": targeted_state.expires_at.map(canonical_time),
        "fields": targeted_state
            .fields
            .iter()
            .map(|field| json!({
                "field": state_field_value(&field.field),
                "presence": match field.presence {
                    AdapterStatePresence::Present => "present",
                    AdapterStatePresence::Deleted => "deleted",
                    AdapterStatePresence::Absent => "absent",
                    AdapterStatePresence::Unknown => "unknown",
                    AdapterStatePresence::Stale => "stale",
                },
                "value": field.value.as_ref().map(state_value),
            }))
            .collect::<Vec<_>>(),
        "membership": match targeted_state.membership {
            AdapterSourceMembership::Present => "present",
            AdapterSourceMembership::Absent => "absent",
            AdapterSourceMembership::Unknown => "unknown",
        },
        "observedAt": canonical_time(targeted_state.observed_at),
        "precondition": targeted_state.precondition,
        "providerRevision": targeted_state.provider_revision,
    })
}

fn translation_value(translation: &SyncTranslationExplanation) -> Value {
    json!({
        "exactValue": rational_value(translation.exact_value),
        "intendedValue": translation.intended_value,
        "lossy": translation.lossy,
        "quantizer": translation.quantizer.as_str(),
        "receiverMaximum": translation.receiver_maximum,
        "receiverMinimum": translation.receiver_minimum,
        "receiverStep": translation.receiver_step,
        "unit": translation.unit.as_str(),
    })
}

fn subject_value(subject: &StateSubject) -> Value {
    match subject {
        StateSubject::ProviderItem(key) => json!({
            "kind": "provider_item",
            "namespace": key.namespace(),
            "value": key.value(),
        }),
        StateSubject::CatalogRelation(key) => json!({
            "kind": "catalog_relation",
            "namespace": key.namespace(),
            "value": key.value(),
        }),
    }
}

fn state_field_value(field: &StateField) -> Value {
    json!({
        "field": {
            "name": field.field().name(),
            "namespace": field.field().namespace(),
        },
        "unit": field.unit().map(|unit| json!({
            "name": unit.name(),
            "namespace": unit.namespace(),
        })),
    })
}

fn state_value(value: &StateValue) -> Value {
    match value {
        StateValue::Boolean(value) => json!({"boolean": value, "kind": "boolean"}),
        StateValue::Integer(value) => json!({"integer": value, "kind": "integer"}),
        StateValue::Decimal(value) => json!({"decimal": value, "kind": "decimal"}),
        StateValue::Text(value) => json!({"kind": "text", "text": value}),
        StateValue::Bytes(value) => json!({"bytes": value, "kind": "bytes"}),
        StateValue::TimestampMilliseconds(value) => {
            json!({"kind": "timestamp_milliseconds", "timestampMilliseconds": value})
        }
    }
}

fn rational_value(value: ExactRational) -> Value {
    json!({
        "denominator": value.denominator(),
        "numerator": value.numerator(),
    })
}

fn span_value(span: &OrdinalSpan) -> Value {
    json!({
        "coordinate": span.coordinate(),
        "end": span.end(),
        "start": span.start(),
    })
}

fn canonical_time(value: OffsetDateTime) -> String {
    value
        .to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .expect("valid synchronization time serializes")
}

fn canonical_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("typed synchronization preview serializes")
}

fn sha256_hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SyncPreviewSnapshotError {
    #[error("synchronization preview expiry must follow creation")]
    InvalidExpiry,
    #[error("synchronization preview policy hash is invalid")]
    InvalidPolicyHash,
    #[error("synchronization preview policy revision must be positive")]
    InvalidPolicyRevision,
    #[error("synchronization preview fingerprint does not match its preview inputs")]
    FingerprintDoesNotMatchPreview,
    #[error(transparent)]
    InvalidFingerprint(#[from] SyncPreviewFingerprintError),
    #[error("synchronization preview has too many actions")]
    TooManyActions,
    #[error("synchronization preview contains a duplicate aggregate candidate")]
    DuplicateCandidate,
    #[error("synchronization preview contains conflicting contributor evidence identity")]
    ConflictingEvidenceIdentity,
    #[error("synchronization preview contains conflicting mapping proof identity")]
    ConflictingMappingProofIdentity,
    #[error("synchronization preview contains more than one action for a receiver target")]
    DuplicateReceiverAction,
    #[error("synchronization preview contains duplicate receiver actions")]
    DuplicateAction,
}

impl<E> From<E> for SyncPreviewLifecycleError<E> {
    fn from(error: E) -> Self {
        Self::Repository(error)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SyncPreviewBuilder {
    registry: StateRegistry,
}

impl SyncPreviewBuilder {
    #[must_use]
    pub const fn new(registry: StateRegistry) -> Self {
        Self { registry }
    }

    pub fn construct(
        self,
        mut command: ConstructSyncPreview,
    ) -> Result<SyncPreviewDraft, SyncPreviewConstructionError> {
        if command.policy.content_hash != command.policy.document.content_hash()
            || command.policy.revision == 0
        {
            return Err(SyncPreviewConstructionError::InvalidPolicySnapshot);
        }
        for partition in &mut command.partitions {
            partition
                .mapping_proofs
                .sort_unstable_by(compare_mapping_proofs);
            if partition
                .mapping_proofs
                .windows(2)
                .any(|proofs| proofs[0].key == proofs[1].key && proofs[0] != proofs[1])
            {
                return Err(SyncPreviewConstructionError::ConflictingMappingProofIdentity);
            }
            partition
                .mapping_proofs
                .dedup_by(|left, right| left == right);
        }
        command.partitions.sort_unstable_by(|left, right| {
            left.semantic_field
                .cmp(&right.semantic_field)
                .then_with(|| left.segment.cmp(&right.segment))
                .then_with(|| {
                    left.mapping_proofs
                        .iter()
                        .map(|proof| &proof.key)
                        .cmp(right.mapping_proofs.iter().map(|proof| &proof.key))
                })
        });
        if command.partitions.windows(2).any(|partitions| {
            partitions[0].semantic_field == partitions[1].semantic_field
                && partitions[0].segment == partitions[1].segment
                && partitions[0]
                    .mapping_proofs
                    .iter()
                    .map(|proof| &proof.key)
                    .eq(partitions[1].mapping_proofs.iter().map(|proof| &proof.key))
        }) {
            return Err(SyncPreviewConstructionError::DuplicatePartition);
        }

        let mut candidates = Vec::with_capacity(command.partitions.len());
        let mut actions = Vec::new();
        for mut partition in command.partitions {
            let rule = command
                .policy
                .document
                .rules()
                .iter()
                .find(|rule| rule.semantic_field() == partition.semantic_field)
                .ok_or_else(|| SyncPreviewConstructionError::MissingFieldRule {
                    field: partition.semantic_field.clone(),
                })?;
            self.registry
                .validate_rule(
                    rule.semantic_field(),
                    rule.reducer(),
                    rule.translation().quantizer(),
                )
                .map_err(|_| SyncPreviewConstructionError::InvalidFieldRule {
                    field: partition.semantic_field.clone(),
                })?;
            if rule.reducer() != Reducer::CoverageContiguousPrefix {
                return Err(SyncPreviewConstructionError::ReducerNotConstructible {
                    reducer: rule.reducer(),
                });
            }
            let segment_unit = unit_for_coordinate(&partition.segment)?;
            if !self
                .registry
                .field(rule.semantic_field())
                .is_some_and(|field| field.units().contains(&segment_unit))
            {
                return Err(SyncPreviewConstructionError::UnitNotSupported {
                    field: partition.semantic_field.clone(),
                    coordinate: partition.segment.coordinate().to_owned(),
                });
            }

            if partition.mapping_proofs.is_empty() {
                return Err(SyncPreviewConstructionError::MappingProofRequired);
            }
            let proof_keys = partition
                .mapping_proofs
                .iter()
                .map(|proof| proof.key.as_str())
                .collect::<Vec<_>>();

            partition
                .contributors
                .sort_unstable_by(compare_contributors);
            let mut unique_contributors = Vec::with_capacity(partition.contributors.len());
            let mut evidence_by_id = BTreeMap::new();
            for contributor in partition.contributors {
                if contributor.semantic_field != partition.semantic_field
                    || !command.policy.document.contributes(contributor.source_id)
                    || !proof_keys.contains(&contributor.mapping_proof_key.as_str())
                {
                    return Err(SyncPreviewConstructionError::ContributorOutsidePartition {
                        evidence_id: contributor.evidence_id,
                    });
                }
                match evidence_by_id.get(&contributor.evidence_id) {
                    Some(existing) if existing != &contributor => {
                        return Err(SyncPreviewConstructionError::ConflictingEvidenceIdentity {
                            evidence_id: contributor.evidence_id,
                        });
                    }
                    Some(_) => continue,
                    None => {
                        evidence_by_id.insert(contributor.evidence_id.clone(), contributor.clone());
                        unique_contributors.push(contributor);
                    }
                }
            }

            let mut coverage = Vec::new();
            let mut consumed = Vec::new();
            for contributor in unique_contributors {
                match contributor.presence {
                    PublishedStatePresence::Present => {
                        let contributor_coverage = contributor
                            .coverage
                            .iter()
                            .filter_map(|span| span.intersection(&partition.segment))
                            .collect::<Vec<_>>();
                        if !contributor_coverage.is_empty() {
                            coverage.extend(contributor_coverage);
                            consumed.push(contributor);
                        }
                    }
                    PublishedStatePresence::Deleted => match rule.presence().deleted() {
                        PresenceDisposition::Ignore => {}
                        PresenceDisposition::Clear | PresenceDisposition::Require => {
                            return Err(SyncPreviewConstructionError::PresenceBranchDeferred {
                                evidence_id: contributor.evidence_id,
                            });
                        }
                    },
                    PublishedStatePresence::Absent => match rule.presence().absent() {
                        PresenceDisposition::Ignore => {}
                        PresenceDisposition::Clear | PresenceDisposition::Require => {
                            return Err(SyncPreviewConstructionError::PresenceBranchDeferred {
                                evidence_id: contributor.evidence_id,
                            });
                        }
                    },
                    PublishedStatePresence::Unknown | PublishedStatePresence::Stale => {
                        return Err(SyncPreviewConstructionError::BlockingPresence {
                            evidence_id: contributor.evidence_id,
                        });
                    }
                }
            }
            if consumed.is_empty() {
                return Err(SyncPreviewConstructionError::ContributorEvidenceRequired);
            }

            let candidate_value = aggregate_contiguous_prefix(&partition.segment, &coverage)?;
            let aggregation = SyncAggregationExplanation {
                reducer: rule.reducer(),
                contributors: consumed,
                mapping_proofs: partition.mapping_proofs.clone(),
            };
            candidates.push(SyncPreviewCandidate {
                semantic_field: partition.semantic_field.clone(),
                segment: partition.segment.clone(),
                value: candidate_value,
                explanation: aggregation.clone(),
            });

            partition.receivers.sort_unstable_by(compare_receivers);
            for receiver in partition.receivers {
                if !command.policy.document.receives(receiver.source_id) {
                    return Err(SyncPreviewConstructionError::ReceiverOutsidePolicy {
                        source_id: receiver.source_id,
                    });
                }
                validate_targeted_read_capability(&receiver.state_read_capability)?;
                let AdapterTargetedStateReadOutcome::Matched(targeted_state) = receiver.outcome
                else {
                    return Err(SyncPreviewConstructionError::ReceiverPreflightNotMatched {
                        source_id: receiver.source_id,
                    });
                };
                validate_targeted_field(&targeted_state, &partition.semantic_field, segment_unit)?;
                if rule.write_constraints().require_strong_precondition()
                    && targeted_state.precondition.is_none()
                {
                    return Err(SyncPreviewConstructionError::StrongPreconditionRequired {
                        source_id: receiver.source_id,
                    });
                }
                let membership_effect = match targeted_state.membership {
                    AdapterSourceMembership::Present => PreviewMembershipEffect::Update,
                    AdapterSourceMembership::Absent => {
                        if !rule.write_constraints().allow_create_membership() {
                            return Err(
                                SyncPreviewConstructionError::MembershipCreationNotAllowed {
                                    source_id: receiver.source_id,
                                },
                            );
                        }
                        PreviewMembershipEffect::Create
                    }
                    AdapterSourceMembership::Unknown => {
                        return Err(SyncPreviewConstructionError::ReceiverMembershipUnknown {
                            source_id: receiver.source_id,
                        });
                    }
                };
                if receiver.progress_capability.unit != segment_unit {
                    return Err(SyncPreviewConstructionError::ReceiverUnitMismatch {
                        source_id: receiver.source_id,
                    });
                }
                if receiver.progress_capability.quantizer != Quantizer::Exact
                    && receiver.progress_capability.quantizer != rule.translation().quantizer()
                {
                    return Err(SyncPreviewConstructionError::ReceiverQuantizerNotAllowed {
                        source_id: receiver.source_id,
                    });
                }
                let translated = quantize_partition_coverage(
                    candidate_value,
                    partition.segment.length(),
                    receiver.progress_capability.minimum,
                    receiver.progress_capability.maximum,
                    receiver.progress_capability.step,
                    receiver.progress_capability.quantizer,
                )?;
                if translated.lossy() && !rule.translation().allow_lossy() {
                    return Err(SyncPreviewConstructionError::LossyTranslationNotAllowed {
                        source_id: receiver.source_id,
                    });
                }
                let translation = SyncTranslationExplanation {
                    unit: receiver.progress_capability.unit,
                    receiver_minimum: receiver.progress_capability.minimum,
                    receiver_maximum: receiver.progress_capability.maximum,
                    receiver_step: receiver.progress_capability.step,
                    quantizer: receiver.progress_capability.quantizer,
                    exact_value: translated.exact(),
                    intended_value: translated.intended(),
                    lossy: translated.lossy(),
                };
                actions.push(SyncPreviewAction {
                    receiver_source_id: receiver.source_id,
                    receiver_subject: receiver.subject,
                    semantic_field: partition.semantic_field.clone(),
                    segment: partition.segment.clone(),
                    candidate: candidate_value,
                    intended_value: translated.intended(),
                    lossy: translated.lossy(),
                    membership_effect,
                    targeted_read_capability: receiver.state_read_capability,
                    receiver_capability: receiver.progress_capability,
                    targeted_state,
                    aggregation: aggregation.clone(),
                    translation,
                });
            }
        }
        actions.sort_unstable_by(compare_actions);
        if actions
            .windows(2)
            .any(|actions| compare_actions(&actions[0], &actions[1]).is_eq())
        {
            return Err(SyncPreviewConstructionError::DuplicateReceiverAction);
        }

        Ok(SyncPreviewDraft {
            policy_id: command.policy.policy_id,
            policy_revision: command.policy.revision,
            policy_content_hash: command.policy.content_hash,
            candidates,
            actions,
        })
    }
}

fn unit_for_coordinate(span: &OrdinalSpan) -> Result<StateUnit, SyncPreviewConstructionError> {
    match span.coordinate() {
        "episode" => Ok(StateUnit::Episode),
        "chapter" => Ok(StateUnit::Chapter),
        "page" => Ok(StateUnit::Page),
        "time" => Ok(StateUnit::Time),
        coordinate => Err(SyncPreviewConstructionError::UnsupportedCoordinate {
            coordinate: coordinate.to_owned(),
        }),
    }
}

fn compare_contributors(
    left: &PublishedSyncContributor,
    right: &PublishedSyncContributor,
) -> Ordering {
    left.source_id
        .cmp(&right.source_id)
        .then_with(|| compare_subjects(&left.subject, &right.subject))
        .then_with(|| left.semantic_field.cmp(&right.semantic_field))
        .then_with(|| left.mapping_proof_key.cmp(&right.mapping_proof_key))
        .then_with(|| left.evidence_id.cmp(&right.evidence_id))
}

fn compare_candidates(left: &SyncPreviewCandidate, right: &SyncPreviewCandidate) -> Ordering {
    left.semantic_field
        .cmp(&right.semantic_field)
        .then_with(|| left.segment.cmp(&right.segment))
        .then_with(|| {
            left.explanation
                .mapping_proofs
                .iter()
                .map(|proof| &proof.key)
                .cmp(
                    right
                        .explanation
                        .mapping_proofs
                        .iter()
                        .map(|proof| &proof.key),
                )
        })
}

fn compare_mapping_proofs(left: &ResolutionProof, right: &ResolutionProof) -> Ordering {
    (
        &left.key,
        &left.revision_hash,
        &left.row_hash,
        &left.endpoint_hash,
        left.transform.numerator,
        left.transform.denominator,
        &left.parent_proofs,
        &left.catalog_publications,
        &left.coordinate_publications,
    )
        .cmp(&(
            &right.key,
            &right.revision_hash,
            &right.row_hash,
            &right.endpoint_hash,
            right.transform.numerator,
            right.transform.denominator,
            &right.parent_proofs,
            &right.catalog_publications,
            &right.coordinate_publications,
        ))
}

fn normalize_draft(draft: &mut SyncPreviewDraft) -> Result<(), SyncPreviewSnapshotError> {
    for candidate in &mut draft.candidates {
        normalize_aggregation(&mut candidate.explanation)?;
    }
    for action in &mut draft.actions {
        normalize_aggregation(&mut action.aggregation)?;
        action
            .targeted_state
            .fields
            .sort_unstable_by(|left, right| {
                state_field_parts(&left.field).cmp(&state_field_parts(&right.field))
            });
        action.targeted_state.fields.dedup();
    }
    Ok(())
}

fn normalize_aggregation(
    explanation: &mut SyncAggregationExplanation,
) -> Result<(), SyncPreviewSnapshotError> {
    for contributor in &mut explanation.contributors {
        contributor.coverage.sort_unstable();
        contributor.coverage.dedup();
    }
    explanation
        .contributors
        .sort_unstable_by(compare_contributors);
    if explanation.contributors.windows(2).any(|contributors| {
        contributors[0].evidence_id == contributors[1].evidence_id
            && contributors[0] != contributors[1]
    }) {
        return Err(SyncPreviewSnapshotError::ConflictingEvidenceIdentity);
    }
    explanation.contributors.dedup();
    for proof in &mut explanation.mapping_proofs {
        proof.parent_proofs.sort_unstable();
        proof.parent_proofs.dedup();
        proof.catalog_publications.sort_unstable();
        proof.catalog_publications.dedup();
        proof.coordinate_publications.sort_unstable();
        proof.coordinate_publications.dedup();
    }
    explanation
        .mapping_proofs
        .sort_unstable_by(compare_mapping_proofs);
    if explanation
        .mapping_proofs
        .windows(2)
        .any(|proofs| proofs[0].key == proofs[1].key && proofs[0] != proofs[1])
    {
        return Err(SyncPreviewSnapshotError::ConflictingMappingProofIdentity);
    }
    explanation.mapping_proofs.dedup();
    Ok(())
}

fn compare_receivers(
    left: &TargetedReceiverPreflight,
    right: &TargetedReceiverPreflight,
) -> Ordering {
    left.source_id
        .cmp(&right.source_id)
        .then_with(|| compare_subjects(&left.subject, &right.subject))
}

fn compare_actions(left: &SyncPreviewAction, right: &SyncPreviewAction) -> Ordering {
    left.receiver_source_id
        .cmp(&right.receiver_source_id)
        .then_with(|| compare_subjects(&left.receiver_subject, &right.receiver_subject))
        .then_with(|| left.semantic_field.cmp(&right.semantic_field))
        .then_with(|| left.segment.cmp(&right.segment))
}

fn compare_subjects(left: &StateSubject, right: &StateSubject) -> Ordering {
    subject_parts(left).cmp(&subject_parts(right))
}

fn subject_parts(subject: &StateSubject) -> (u8, &str, &[u8]) {
    match subject {
        StateSubject::ProviderItem(key) => (0, key.namespace(), key.value()),
        StateSubject::CatalogRelation(key) => (1, key.namespace(), key.value()),
    }
}

fn validate_targeted_read_capability(
    capability: &AdapterTargetedStateReadCapability,
) -> Result<(), SyncPreviewConstructionError> {
    if capability.maximum_fields == 0 || capability.maximum_response_bytes == 0 {
        return Err(SyncPreviewConstructionError::TargetedReadUnsupported);
    }
    Ok(())
}

fn validate_targeted_field(
    targeted_state: &AdapterTargetedStateMatch,
    semantic_field: &str,
    expected_unit: StateUnit,
) -> Result<(), SyncPreviewConstructionError> {
    let mut fields = targeted_state
        .fields
        .iter()
        .filter(|field| state_field_id(&field.field) == semantic_field);
    let Some(field) = fields.next() else {
        return Err(SyncPreviewConstructionError::TargetedFieldMissing {
            field: semantic_field.to_owned(),
        });
    };
    if fields.next().is_some() {
        return Err(SyncPreviewConstructionError::TargetedFieldMissing {
            field: semantic_field.to_owned(),
        });
    }
    if matches!(
        field.presence,
        AdapterStatePresence::Unknown | AdapterStatePresence::Stale
    ) {
        return Err(SyncPreviewConstructionError::TargetedFieldBlocked {
            field: semantic_field.to_owned(),
        });
    }
    if field.field.unit().is_none_or(|unit| {
        unit.namespace() != "trakkin.unit" || unit.name() != expected_unit.as_str()
    }) {
        return Err(SyncPreviewConstructionError::TargetedFieldUnitMismatch {
            field: semantic_field.to_owned(),
        });
    }
    Ok(())
}

fn state_field_id(field: &StateField) -> String {
    format!("{}:{}", field.field().namespace(), field.field().name())
}

fn state_field_parts(field: &StateField) -> (&str, &str, Option<(&str, &str)>) {
    (
        field.field().namespace(),
        field.field().name(),
        field.unit().map(|unit| (unit.namespace(), unit.name())),
    )
}

#[derive(Debug, Error)]
pub enum SyncPreviewConstructionError {
    #[error("synchronization policy snapshot is inconsistent")]
    InvalidPolicySnapshot,
    #[error("synchronization policy has no rule for {field}")]
    MissingFieldRule { field: String },
    #[error("synchronization field rule is invalid for {field}")]
    InvalidFieldRule { field: String },
    #[error("reducer {reducer} is not implemented by this preview constructor")]
    ReducerNotConstructible { reducer: Reducer },
    #[error("field {field} does not support coordinate {coordinate}")]
    UnitNotSupported { field: String, coordinate: String },
    #[error("coordinate {coordinate} is not supported for preview construction")]
    UnsupportedCoordinate { coordinate: String },
    #[error("each synchronization partition requires mapping proof")]
    MappingProofRequired,
    #[error("synchronization mapping proof identity has conflicting evidence")]
    ConflictingMappingProofIdentity,
    #[error("synchronization partitions must have unique field, segment, and proof identity")]
    DuplicatePartition,
    #[error("contributor evidence {evidence_id} is outside the policy or partition")]
    ContributorOutsidePartition { evidence_id: String },
    #[error("contributor evidence identity {evidence_id} has conflicting values")]
    ConflictingEvidenceIdentity { evidence_id: String },
    #[error("contributor evidence {evidence_id} requires a later presence branch")]
    PresenceBranchDeferred { evidence_id: String },
    #[error("contributor evidence {evidence_id} has blocking presence")]
    BlockingPresence { evidence_id: String },
    #[error("synchronization partition requires contributor evidence")]
    ContributorEvidenceRequired,
    #[error("receiver source {source_id} is outside the synchronization policy")]
    ReceiverOutsidePolicy { source_id: Uuid },
    #[error("targeted receiver reads are unsupported")]
    TargetedReadUnsupported,
    #[error("receiver preflight did not match source {source_id}")]
    ReceiverPreflightNotMatched { source_id: Uuid },
    #[error("targeted receiver field {field} is missing or duplicated")]
    TargetedFieldMissing { field: String },
    #[error("targeted receiver field {field} has unknown or stale evidence")]
    TargetedFieldBlocked { field: String },
    #[error("targeted receiver field {field} uses a different unit")]
    TargetedFieldUnitMismatch { field: String },
    #[error("receiver source {source_id} has unknown membership")]
    ReceiverMembershipUnknown { source_id: Uuid },
    #[error("receiver source {source_id} cannot create membership under this policy")]
    MembershipCreationNotAllowed { source_id: Uuid },
    #[error("receiver source {source_id} requires a strong targeted-read precondition")]
    StrongPreconditionRequired { source_id: Uuid },
    #[error("synchronization preview contains more than one action for a receiver target")]
    DuplicateReceiverAction,
    #[error("receiver source {source_id} uses a different progress unit")]
    ReceiverUnitMismatch { source_id: Uuid },
    #[error("receiver source {source_id} requires a quantizer not permitted by the policy")]
    ReceiverQuantizerNotAllowed { source_id: Uuid },
    #[error("lossy translation is not allowed for receiver source {source_id}")]
    LossyTranslationNotAllowed { source_id: Uuid },
    #[error(transparent)]
    Computation(#[from] SyncComputationError),
}

#[cfg(test)]
#[path = "sync_preview_tests.rs"]
mod tests;
