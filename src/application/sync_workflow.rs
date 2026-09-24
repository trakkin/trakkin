use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Arc};

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    acquisition::{ProviderItemKey, StateField, StateSubject, Term},
    synchronization::{OrdinalSpan, ResolutionMode as PolicyResolutionMode, StateRegistry},
};

use super::{
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    facade::ApplicationFuture,
    mapping_resolution::{
        DirectResolutionPins, EndpointLookupMatch, MappingResolution, MappingResolutionInteraction,
        PublishedReferenceMatch, ResolutionCompleteness, ResolutionMode as MappingResolutionMode,
        ResolutionProof, ResolutionRequest, ResolvedCorrespondence,
    },
    sync_policy::{SyncPolicyLifecycleError, SyncPolicyRepository, SyncPolicyService},
    sync_preview::{
        ConstructSyncPreview, CreateSyncPreviewSnapshot, PreviewMembershipEffect,
        PublishedStatePresence, PublishedSyncContributor, SyncPreviewBuilder,
        SyncPreviewConstructionError, SyncPreviewFingerprint, SyncPreviewLifecycleError,
        SyncPreviewMembershipPin, SyncPreviewPartitionInput, SyncPreviewRepository,
        SyncPreviewService, SyncPreviewSnapshot, TargetedReceiverPreflight,
        sync_preview_policy_revision_pin,
    },
    sync_run::{
        AdmitSyncPreview, SyncActionAdmission, SyncRunAdmissionError, SyncRunRepository,
        SyncRunService, SyncRunSnapshot,
    },
    sync_state_evidence::{
        DiscoverSyncContributorRoots, SyncContributorRoot, SyncStateEvidencePresence,
        SyncStateEvidenceRepository, SyncStatePublicationPin,
    },
};

pub type SyncPreviewCollectionFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<CollectedSyncPreviewInputs, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorkspaceSyncPreview {
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectSyncPreviewInputs {
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub policy: super::sync_policy::SyncPolicyRevisionSnapshot,
    pub preview_time: OffsetDateTime,
    pub preview_expiry: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectedSyncPreviewInputs {
    pub partitions: Vec<SyncPreviewPartitionInput>,
    pub fingerprint: SyncPreviewFingerprint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncReceiverPreflightTarget {
    pub source_id: Uuid,
    pub subject: StateSubject,
    pub field: StateField,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreflightSyncReceivers {
    pub operation_id: Uuid,
    pub workspace_id: Uuid,
    pub targets: Vec<SyncReceiverPreflightTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncReceiverPreflightResult {
    pub target: SyncReceiverPreflightTarget,
    pub receiver: TargetedReceiverPreflight,
    pub connection_id: Uuid,
    pub source_capability_revision: i64,
    pub connection_capability_revision: i64,
    pub process_instance_id: String,
}

pub trait SyncReceiverPreflightApi: Send + Sync {
    fn preflight_receivers(
        &self,
        command: PreflightSyncReceivers,
    ) -> ApplicationFuture<'_, Vec<SyncReceiverPreflightResult>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPreviewEntryCategory {
    Proposed,
    NoOp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPreviewFindingKind {
    DestinationAlreadyMatches,
    LossyTranslation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewEntry {
    pub ordinal: u32,
    pub action_hash: String,
    pub category: SyncPreviewEntryCategory,
    pub finding: Option<SyncPreviewFindingKind>,
    pub receiver_source_id: Uuid,
    pub receiver_subject: StateSubject,
    pub semantic_field: String,
    pub segment_coordinate: String,
    pub segment_start: u64,
    pub segment_end: u64,
    pub before: Option<i64>,
    pub intended: u64,
    pub contributor_source_ids: Vec<Uuid>,
    pub membership_effect: PreviewMembershipEffect,
    pub lossy: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPreviewCounts {
    pub proposed: u32,
    pub no_op: u32,
    pub findings: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceSyncPreview {
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub policy_revision: u64,
    pub preview_hash: String,
    pub counts: SyncPreviewCounts,
    pub entries: Vec<SyncPreviewEntry>,
    pub fresh_until: OffsetDateTime,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

pub trait SyncPreviewMutationApi: Send + Sync {
    fn create_workspace_preview(
        &self,
        command: CreateWorkspaceSyncPreview,
    ) -> ApplicationFuture<'_, WorkspaceSyncPreview>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartWorkspaceSyncRun {
    pub run_id: Uuid,
    pub schedule_occurrence_id: Option<Uuid>,
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub preview_hash: String,
    pub admitted_at: OffsetDateTime,
}

pub trait SyncRunMutationApi: Send + Sync {
    fn start_workspace_sync_run(
        &self,
        command: StartWorkspaceSyncRun,
    ) -> ApplicationFuture<'_, SyncRunSnapshot>;
}

pub trait SyncPreviewInputCollector: Send + Sync {
    type Error;

    fn collect(
        &self,
        command: CollectSyncPreviewInputs,
    ) -> SyncPreviewCollectionFuture<'_, Self::Error>;
}

impl<C> SyncPreviewInputCollector for Arc<C>
where
    C: SyncPreviewInputCollector,
{
    type Error = C::Error;

    fn collect(
        &self,
        command: CollectSyncPreviewInputs,
    ) -> SyncPreviewCollectionFuture<'_, Self::Error> {
        self.as_ref().collect(command)
    }
}

#[derive(Clone)]
pub struct ProductionSyncPreviewInputCollector<E, M, R> {
    evidence: E,
    mappings: M,
    receivers: R,
    registry: StateRegistry,
}

impl<E, M, R> ProductionSyncPreviewInputCollector<E, M, R> {
    pub fn new(evidence: E, mappings: M, receivers: R, registry: StateRegistry) -> Self {
        Self {
            evidence,
            mappings,
            receivers,
            registry,
        }
    }
}

impl<E, M, R> SyncPreviewInputCollector for ProductionSyncPreviewInputCollector<E, M, R>
where
    E: SyncStateEvidenceRepository,
    E::Error: IntoApplicationError,
    M: MappingResolutionInteraction,
    R: SyncReceiverPreflightApi,
{
    type Error = ApplicationError;

    fn collect(
        &self,
        command: CollectSyncPreviewInputs,
    ) -> SyncPreviewCollectionFuture<'_, Self::Error> {
        Box::pin(async move {
            let semantic_fields = command
                .policy
                .document
                .rules()
                .iter()
                .map(|rule| parse_semantic_term(rule.semantic_field()))
                .collect::<Result<Vec<_>, _>>()?;
            let roots = self
                .evidence
                .discover_sync_contributor_roots(DiscoverSyncContributorRoots {
                    workspace_id: command.workspace_id,
                    policy_id: command.policy.policy_id,
                    policy_revision: command.policy.revision,
                    semantic_fields,
                })
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            let references = roots
                .contributors
                .iter()
                .flat_map(|contributor| contributor.roots.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            if references.is_empty() {
                return Err(collection_error(
                    "synchronization contributors have no portable mapping roots",
                ));
            }
            let resolution = self
                .mappings
                .resolve_workspace_mappings(ResolutionRequest {
                    workspace_id: command.workspace_id,
                    mode: match command.policy.document.resolution_mode() {
                        PolicyResolutionMode::Direct => MappingResolutionMode::Direct,
                        PolicyResolutionMode::Transitive => MappingResolutionMode::Transitive,
                    },
                    references,
                    window: None,
                    resolved_at: command.preview_time,
                })
                .await?;
            if resolution.completeness != ResolutionCompleteness::Complete
                || !resolution.findings.is_empty()
            {
                return Err(collection_error(
                    "synchronization mapping resolution is incomplete",
                ));
            }
            let pending = collect_pending_partitions(
                &command,
                &roots.publications,
                &roots.contributors,
                &resolution,
            )?;
            if pending.is_empty() {
                return Err(collection_error(
                    "synchronization mapping resolution produced no receiver partitions",
                ));
            }
            let targets = pending
                .iter()
                .flat_map(|partition| partition.targets.iter().cloned())
                .fold(Vec::new(), |mut targets, target| {
                    if !targets.contains(&target) {
                        targets.push(target);
                    }
                    targets
                });
            let preflights = self
                .receivers
                .preflight_receivers(PreflightSyncReceivers {
                    operation_id: command.preview_id,
                    workspace_id: command.workspace_id,
                    targets,
                })
                .await?;
            let partitions = complete_partitions(pending, &preflights)?;
            let fingerprint = collect_fingerprint(
                &command,
                self.registry,
                &roots.publications,
                &roots.contributors,
                &resolution,
                &preflights,
            )?;
            Ok(CollectedSyncPreviewInputs {
                partitions,
                fingerprint,
            })
        })
    }
}

struct PendingSyncPreviewPartition {
    semantic_field: String,
    segment: OrdinalSpan,
    mapping_proofs: Vec<ResolutionProof>,
    contributors: Vec<PublishedSyncContributor>,
    targets: Vec<SyncReceiverPreflightTarget>,
}

fn collect_pending_partitions(
    command: &CollectSyncPreviewInputs,
    publications: &[SyncStatePublicationPin],
    contributors: &[SyncContributorRoot],
    resolution: &MappingResolution,
) -> Result<Vec<PendingSyncPreviewPartition>, ApplicationError> {
    let pins = resolution
        .pins
        .as_ref()
        .ok_or_else(|| collection_error("synchronization mapping pins are missing"))?;
    let mut partitions = Vec::<PendingSyncPreviewPartition>::new();
    for correspondence in &resolution.correspondences {
        let Some(segment) = correspondence.segment.as_ref() else {
            continue;
        };
        let segment = OrdinalSpan::new(
            segment.coordinate.clone(),
            segment.source_start,
            segment.source_end,
        )
        .map_err(|_| collection_error("synchronization mapping segment is invalid"))?;
        let receiver_subjects =
            collect_receiver_subjects(command, correspondence, resolution, pins)?;
        if receiver_subjects.is_empty() {
            continue;
        }
        for contributor in contributors.iter().filter(|contributor| {
            contributor
                .roots
                .iter()
                .any(|root| correspondence.endpoints.contains(root))
        }) {
            let semantic_field = state_field_id(&contributor.evidence.slot.field);
            if !command
                .policy
                .document
                .rules()
                .iter()
                .any(|rule| rule.semantic_field() == semantic_field)
            {
                continue;
            }
            let root = contributor
                .roots
                .iter()
                .find(|root| correspondence.endpoints.contains(root))
                .cloned()
                .ok_or_else(|| collection_error("synchronization contributor root is missing"))?;
            let publication_id = publications
                .iter()
                .find(|publication| publication.source_id == contributor.evidence.slot.source_id)
                .and_then(|publication| publication.publication_id)
                .ok_or_else(|| {
                    collection_error("synchronization contributor publication is unavailable")
                })?;
            let provider_revision = contributor
                .evidence
                .provider_revision
                .clone()
                .filter(|revision| !revision.is_empty())
                .ok_or_else(|| {
                    collection_error("synchronization contributor revision is missing")
                })?;
            let mut mapping_proofs = vec![correspondence.proof.clone()];
            mapping_proofs.extend(correspondence.supporting_proofs.clone());
            mapping_proofs.sort_unstable_by(|left, right| left.key.cmp(&right.key));
            mapping_proofs.dedup_by(|left, right| left.key == right.key);
            let evidence_id = digest(&json!({
                "field": semantic_field,
                "providerRevision": hex::encode(&provider_revision),
                "root": root.as_str(),
                "sourceId": contributor.evidence.slot.source_id,
                "subject": subject_value(&contributor.evidence.slot.subject),
            }));
            let published = PublishedSyncContributor {
                evidence_id,
                source_id: contributor.evidence.slot.source_id,
                subject: contributor.evidence.slot.subject.clone(),
                semantic_field: semantic_field.clone(),
                root,
                presence: match contributor.evidence.presence {
                    SyncStateEvidencePresence::Present => PublishedStatePresence::Present,
                    SyncStateEvidencePresence::Deleted => PublishedStatePresence::Deleted,
                    SyncStateEvidencePresence::Absent => PublishedStatePresence::Absent,
                    SyncStateEvidencePresence::Unknown => PublishedStatePresence::Unknown,
                },
                coverage: vec![segment.clone()],
                state_publication_id: publication_id,
                provider_revision,
                mapping_proof_key: correspondence.proof.key.clone(),
            };
            let targets = receiver_subjects
                .iter()
                .map(|(source_id, subject)| SyncReceiverPreflightTarget {
                    source_id: *source_id,
                    subject: subject.clone(),
                    field: contributor.evidence.slot.field.clone(),
                })
                .collect();
            if let Some(partition) = partitions.iter_mut().find(|partition| {
                partition.semantic_field == semantic_field
                    && partition.segment == segment
                    && partition.mapping_proofs == mapping_proofs
                    && partition.targets == targets
            }) {
                if !partition
                    .contributors
                    .iter()
                    .any(|contributor| contributor.evidence_id == published.evidence_id)
                {
                    partition.contributors.push(published);
                }
            } else {
                partitions.push(PendingSyncPreviewPartition {
                    semantic_field,
                    segment: segment.clone(),
                    mapping_proofs,
                    contributors: vec![published],
                    targets,
                });
            }
        }
    }
    for partition in &mut partitions {
        partition
            .contributors
            .sort_unstable_by(|left, right| left.evidence_id.cmp(&right.evidence_id));
    }
    partitions.sort_unstable_by(|left, right| {
        (
            left.semantic_field.as_str(),
            &left.segment,
            &left.mapping_proofs[0].key,
        )
            .cmp(&(
                right.semantic_field.as_str(),
                &right.segment,
                &right.mapping_proofs[0].key,
            ))
    });
    Ok(partitions)
}

fn collect_receiver_subjects(
    command: &CollectSyncPreviewInputs,
    correspondence: &ResolvedCorrespondence,
    resolution: &MappingResolution,
    pins: &DirectResolutionPins,
) -> Result<Vec<(Uuid, StateSubject)>, ApplicationError> {
    let mut receivers = Vec::new();
    for published in resolution.published_matches.iter().filter(|published| {
        command.policy.document.receives(published.source_id)
            && correspondence.endpoints.contains(&published.endpoint)
    }) {
        push_receiver(
            &mut receivers,
            published.source_id,
            published_subject(published)?,
        );
    }
    for lookup in resolution
        .endpoint_lookup_matches
        .iter()
        .filter(|lookup| correspondence.endpoints.contains(&lookup.endpoint))
    {
        for source in pins.sources.iter().filter(|source| {
            source.connection_id == lookup.connection_id
                && command.policy.document.receives(source.source_id)
        }) {
            push_receiver(&mut receivers, source.source_id, lookup_subject(lookup)?);
        }
    }
    receivers.sort_unstable_by_key(|receiver| receiver.0);
    Ok(receivers)
}

fn push_receiver(
    receivers: &mut Vec<(Uuid, StateSubject)>,
    source_id: Uuid,
    subject: StateSubject,
) {
    if !receivers
        .iter()
        .any(|receiver| receiver.0 == source_id && receiver.1 == subject)
    {
        receivers.push((source_id, subject));
    }
}

fn published_subject(match_: &PublishedReferenceMatch) -> Result<StateSubject, ApplicationError> {
    provider_subject(
        match_.item_key_namespace.clone(),
        match_.item_key_value.clone(),
    )
}

fn lookup_subject(match_: &EndpointLookupMatch) -> Result<StateSubject, ApplicationError> {
    provider_subject(
        match_.item_key_namespace.clone(),
        match_.item_key_value.clone(),
    )
}

fn provider_subject(namespace: String, value: Vec<u8>) -> Result<StateSubject, ApplicationError> {
    ProviderItemKey::new(namespace, value)
        .map(StateSubject::ProviderItem)
        .map_err(|_| collection_error("synchronization receiver subject is invalid"))
}

fn complete_partitions(
    pending: Vec<PendingSyncPreviewPartition>,
    preflights: &[SyncReceiverPreflightResult],
) -> Result<Vec<SyncPreviewPartitionInput>, ApplicationError> {
    pending
        .into_iter()
        .map(|partition| {
            let receivers = partition
                .targets
                .iter()
                .map(|target| {
                    preflights
                        .iter()
                        .find(|preflight| preflight.target == *target)
                        .map(|preflight| preflight.receiver.clone())
                        .ok_or_else(|| {
                            collection_error("synchronization receiver preflight is missing")
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SyncPreviewPartitionInput {
                semantic_field: partition.semantic_field,
                segment: partition.segment,
                mapping_proofs: partition.mapping_proofs,
                contributors: partition.contributors,
                receivers,
            })
        })
        .collect()
}

fn collect_fingerprint(
    command: &CollectSyncPreviewInputs,
    registry: StateRegistry,
    publications: &[SyncStatePublicationPin],
    contributors: &[SyncContributorRoot],
    resolution: &MappingResolution,
    preflights: &[SyncReceiverPreflightResult],
) -> Result<SyncPreviewFingerprint, ApplicationError> {
    let pins = resolution
        .pins
        .as_ref()
        .ok_or_else(|| collection_error("synchronization mapping pins are missing"))?;
    for preflight in preflights {
        let source = pins
            .sources
            .iter()
            .find(|source| source.source_id == preflight.target.source_id)
            .ok_or_else(|| collection_error("synchronization receiver pin is missing"))?;
        if source.connection_id != preflight.connection_id
            || source.source_capability_revision != preflight.source_capability_revision
            || source.connection_capability_revision != preflight.connection_capability_revision
            || preflight.process_instance_id.is_empty()
        {
            return Err(collection_error(
                "synchronization receiver preflight pins changed",
            ));
        }
    }
    let mut active_mapping_hashes = pins
        .revisions
        .iter()
        .map(|revision| revision.content_hash.clone())
        .collect::<Vec<_>>();
    active_mapping_hashes.sort_unstable();
    active_mapping_hashes.dedup();
    if active_mapping_hashes.is_empty() {
        return Err(collection_error(
            "synchronization active mapping pins are missing",
        ));
    }
    let mut contributor_evidence_revisions = Vec::new();
    for publication in publications {
        if let Some(revision) = &publication.evidence_revision {
            contributor_evidence_revisions.push(digest_bytes(revision));
        }
        if let Some(coverage) = &publication.coverage {
            contributor_evidence_revisions.push(digest_bytes(&coverage.evidence_revision));
        }
    }
    contributor_evidence_revisions.extend(contributors.iter().filter_map(|contributor| {
        contributor
            .evidence
            .provider_revision
            .as_deref()
            .map(digest_bytes)
    }));
    contributor_evidence_revisions.sort_unstable();
    contributor_evidence_revisions.dedup();
    if contributor_evidence_revisions.is_empty() {
        return Err(collection_error(
            "synchronization contributor evidence revisions are missing",
        ));
    }
    let mapping_proofs = resolution
        .correspondences
        .iter()
        .flat_map(|correspondence| {
            std::iter::once(&correspondence.proof).chain(correspondence.supporting_proofs.iter())
        })
        .map(proof_value)
        .collect::<Vec<_>>();
    let lookup_observed_at = resolution
        .endpoint_lookup_matches
        .iter()
        .map(|match_| match_.observed_at)
        .min()
        .unwrap_or(command.preview_time);
    let lookup_expiry = resolution
        .endpoint_lookup_matches
        .iter()
        .filter_map(|match_| match_.expires_at)
        .min()
        .unwrap_or(command.preview_expiry);
    let targeted_expiry = preflights
        .iter()
        .filter_map(|preflight| match &preflight.receiver.outcome {
            super::adapter_runtime::AdapterTargetedStateReadOutcome::Matched(match_) => {
                match_.expires_at
            }
            _ => None,
        })
        .min()
        .unwrap_or(command.preview_expiry);
    let memberships = preflights
        .iter()
        .filter_map(|preflight| match &preflight.receiver.outcome {
            super::adapter_runtime::AdapterTargetedStateReadOutcome::Matched(match_) => {
                Some(match_.membership)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let targeted_membership =
        if memberships.iter().all(|membership| {
            *membership == super::adapter_runtime::AdapterSourceMembership::Present
        }) {
            SyncPreviewMembershipPin::Present
        } else if memberships.iter().all(|membership| {
            *membership == super::adapter_runtime::AdapterSourceMembership::Absent
        }) {
            SyncPreviewMembershipPin::Absent
        } else {
            SyncPreviewMembershipPin::Unknown
        };
    let fingerprint = SyncPreviewFingerprint {
        policy_revision: sync_preview_policy_revision_pin(
            command.policy.revision,
            &command.policy.content_hash,
        ),
        state_registry_digest: digest(&json!({"schema": registry.schema()})),
        resolver_version: "trakkin.mapping-resolver/v1".to_owned(),
        active_mapping_hashes,
        mapping_activation_generation: pins
            .revisions
            .iter()
            .filter_map(|revision| revision.activation_generation)
            .filter_map(|generation| u64::try_from(generation).ok())
            .max()
            .ok_or_else(|| collection_error("synchronization mapping activation pin is missing"))?,
        mapping_proof_digest: digest(&Value::Array(mapping_proofs)),
        attachment_digest: digest(&Value::Array(
            pins.sources.iter().map(attachment_value).collect(),
        )),
        source_capability_revision: digest(&Value::Array(
            pins.sources
                .iter()
                .map(|source| json!([source.source_id, source.source_capability_revision]))
                .collect(),
        )),
        connection_capability_revision: digest(&Value::Array(
            pins.sources
                .iter()
                .map(|source| json!([source.connection_id, source.connection_capability_revision]))
                .collect(),
        )),
        state_publication: digest(&Value::Array(
            publications.iter().map(publication_value).collect(),
        )),
        contributor_evidence_revisions,
        catalog_publication: digest(&Value::Array(
            pins.sources
                .iter()
                .map(|source| json!([source.source_id, source.catalog_publication_id]))
                .collect(),
        )),
        coordinate_publication: digest(&Value::Array(
            pins.sources
                .iter()
                .map(|source| json!([source.source_id, source.coordinate_publication_id]))
                .collect(),
        )),
        lookup_process: digest(&Value::Array(
            resolution
                .endpoint_lookup_matches
                .iter()
                .map(|match_| json!([match_.connection_id, match_.process_instance_id]))
                .collect(),
        )),
        lookup_revision: digest(&Value::Array(
            resolution
                .endpoint_lookup_matches
                .iter()
                .map(|match_| digest_bytes(&match_.lookup_evidence_revision))
                .map(Value::String)
                .collect(),
        )),
        lookup_observed_at,
        lookup_expiry,
        targeted_membership,
        targeted_state_revision: digest(&Value::Array(
            preflights
                .iter()
                .filter_map(|preflight| match &preflight.receiver.outcome {
                    super::adapter_runtime::AdapterTargetedStateReadOutcome::Matched(match_) => {
                        Some(json!({
                            "processInstanceId": preflight.process_instance_id,
                            "providerRevision": digest_bytes(&match_.provider_revision),
                            "sourceId": preflight.target.source_id,
                        }))
                    }
                    _ => None,
                })
                .collect(),
        )),
        targeted_precondition: digest(&Value::Array(
            preflights
                .iter()
                .filter_map(|preflight| match &preflight.receiver.outcome {
                    super::adapter_runtime::AdapterTargetedStateReadOutcome::Matched(match_) => {
                        match_
                            .precondition
                            .as_deref()
                            .map(digest_bytes)
                            .map(Value::String)
                    }
                    _ => None,
                })
                .collect(),
        )),
        targeted_expiry,
        preview_time: command.preview_time,
        preview_expiry: command.preview_expiry,
    };
    fingerprint
        .canonical_json()
        .map_err(|_| collection_error("synchronization preview fingerprint is invalid"))?;
    Ok(fingerprint)
}

fn parse_semantic_term(value: &str) -> Result<Term, ApplicationError> {
    let (namespace, name) = value
        .split_once(':')
        .ok_or_else(|| collection_error("synchronization semantic field is invalid"))?;
    Term::new(namespace, name)
        .map_err(|_| collection_error("synchronization semantic field is invalid"))
}

fn state_field_id(field: &StateField) -> String {
    format!("{}:{}", field.field().namespace(), field.field().name())
}

fn subject_value(subject: &StateSubject) -> Value {
    match subject {
        StateSubject::ProviderItem(key) => json!({
            "kind": "provider_item",
            "namespace": key.namespace(),
            "value": hex::encode(key.value()),
        }),
        StateSubject::CatalogRelation(key) => json!({
            "kind": "catalog_relation",
            "namespace": key.namespace(),
            "value": hex::encode(key.value()),
        }),
    }
}

fn proof_value(proof: &ResolutionProof) -> Value {
    json!({
        "catalogPublications": proof.catalog_publications,
        "coordinatePublications": proof.coordinate_publications,
        "endpointHash": proof.endpoint_hash,
        "key": proof.key,
        "parentProofs": proof.parent_proofs,
        "revisionHash": proof.revision_hash,
        "rowHash": proof.row_hash,
        "transform": [proof.transform.numerator, proof.transform.denominator],
    })
}

fn attachment_value(source: &super::mapping_resolution::ResolutionSourcePin) -> Value {
    json!({
        "attachedAt": source.attached_at.unix_timestamp_nanos().to_string(),
        "connectionDeleted": source.connection_deleted,
        "connectionId": source.connection_id,
        "connectionReady": source.connection_ready,
        "mappingRootNamespaceAllowlist": source.mapping_root_namespace_allowlist,
        "mappingRootPolicyGeneration": source.mapping_root_policy_generation,
        "sourceId": source.source_id,
        "sourceRemoved": source.source_removed,
    })
}

fn publication_value(publication: &SyncStatePublicationPin) -> Value {
    json!({
        "coverage": publication.coverage.as_ref().map(|coverage| json!({
            "baselinePublicationId": coverage.baseline_publication_id,
            "evidenceRevision": hex::encode(&coverage.evidence_revision),
            "observedAt": coverage.observed_at.unix_timestamp_nanos().to_string(),
            "publicationId": coverage.publication_id,
        })),
        "evidenceRevision": publication.evidence_revision.as_deref().map(hex::encode),
        "lastSuccessAt": publication.last_success_at.map(|time| time.unix_timestamp_nanos().to_string()),
        "publicationId": publication.publication_id,
        "sourceId": publication.source_id,
        "status": match publication.status {
            super::catalog_queries::PublishedStreamStatus::Unavailable => "unavailable",
            super::catalog_queries::PublishedStreamStatus::Available => "available",
            super::catalog_queries::PublishedStreamStatus::Failed => "failed",
        },
    })
}

fn digest(value: &Value) -> String {
    digest_bytes(&serde_json::to_vec(value).expect("typed fingerprint pin serializes"))
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn collection_error(message: &'static str) -> ApplicationError {
    ApplicationError::new(ApplicationErrorCode::InvalidProviderData, message)
}

#[derive(Debug)]
pub enum SyncWorkflowError<P, C, V> {
    Policy(SyncPolicyLifecycleError<P>),
    Collection(C),
    Construction(SyncPreviewConstructionError),
    Preview(SyncPreviewLifecycleError<V>),
}

#[derive(Clone)]
pub struct SyncWorkflowService<P, C, V> {
    policies: SyncPolicyService<P>,
    collector: C,
    previews: SyncPreviewService<V>,
    registry: StateRegistry,
}

impl<P, C, V> SyncWorkflowService<P, C, V>
where
    P: SyncPolicyRepository,
    C: SyncPreviewInputCollector,
    V: SyncPreviewRepository,
{
    pub fn new(policies: P, collector: C, previews: V, registry: StateRegistry) -> Self {
        Self {
            policies: SyncPolicyService::new(policies),
            collector,
            previews: SyncPreviewService::new(previews),
            registry,
        }
    }

    pub async fn create_preview(
        &self,
        command: CreateWorkspaceSyncPreview,
    ) -> Result<SyncPreviewSnapshot, SyncWorkflowError<P::Error, C::Error, V::Error>> {
        let policy = self
            .policies
            .head(command.workspace_id, command.policy_id)
            .await
            .map_err(SyncWorkflowError::Policy)?;
        let collected = self
            .collector
            .collect(CollectSyncPreviewInputs {
                preview_id: command.preview_id,
                workspace_id: command.workspace_id,
                policy: policy.clone(),
                preview_time: command.created_at,
                preview_expiry: command.expires_at,
            })
            .await
            .map_err(SyncWorkflowError::Collection)?;
        let draft = SyncPreviewBuilder::new(self.registry)
            .construct(ConstructSyncPreview {
                policy,
                partitions: collected.partitions,
            })
            .map_err(SyncWorkflowError::Construction)?;
        self.previews
            .create(CreateSyncPreviewSnapshot {
                preview_id: command.preview_id,
                workspace_id: command.workspace_id,
                draft,
                fingerprint: collected.fingerprint,
                created_at: command.created_at,
                expires_at: command.expires_at,
            })
            .await
            .map_err(SyncWorkflowError::Preview)
    }
}

impl<P, C, V> SyncPreviewMutationApi for SyncWorkflowService<P, C, V>
where
    P: SyncPolicyRepository + 'static,
    P::Error: IntoApplicationError,
    C: SyncPreviewInputCollector + 'static,
    C::Error: IntoApplicationError,
    V: SyncPreviewRepository + 'static,
    V::Error: IntoApplicationError,
{
    fn create_workspace_preview(
        &self,
        command: CreateWorkspaceSyncPreview,
    ) -> ApplicationFuture<'_, WorkspaceSyncPreview> {
        Box::pin(async move {
            let preview_id = command.preview_id;
            let workspace_id = command.workspace_id;
            let policy_id = command.policy_id;
            let snapshot = match self.create_preview(command).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let stage = match &error {
                        SyncWorkflowError::Policy(_) => "policy",
                        SyncWorkflowError::Collection(_) => "collection",
                        SyncWorkflowError::Construction(_) => "construction",
                        SyncWorkflowError::Preview(_) => "persistence",
                    };
                    tracing::warn!(
                        sync.preview.id = %preview_id,
                        workspace.id = %workspace_id,
                        sync.policy.id = %policy_id,
                        failure.stage = stage,
                        "synchronization preview creation failed"
                    );
                    return Err(sync_workflow_error(error));
                }
            };
            let preview = project_preview(snapshot).map_err(|_| {
                tracing::warn!(
                    sync.preview.id = %preview_id,
                    workspace.id = %workspace_id,
                    sync.policy.id = %policy_id,
                    failure.stage = "projection",
                    "synchronization preview creation failed"
                );
                ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "synchronization preview could not be projected",
                )
            })?;
            tracing::debug!(
                sync.preview.id = %preview_id,
                workspace.id = %workspace_id,
                sync.policy.id = %policy_id,
                proposed_count = preview.counts.proposed,
                no_op_count = preview.counts.no_op,
                finding_count = preview.counts.findings,
                "synchronization preview created"
            );
            Ok(preview)
        })
    }
}

#[derive(Debug)]
pub enum SyncRunWorkflowError<P, C, V, R> {
    Policy(SyncPolicyLifecycleError<P>),
    Collection(C),
    Preview(SyncPreviewLifecycleError<V>),
    Projection,
    IdentityGeneration,
    Admission(SyncRunAdmissionError<R>),
}

#[derive(Clone)]
pub struct SyncRunWorkflowService<P, C, V, R> {
    policies: SyncPolicyService<P>,
    collector: C,
    previews: SyncPreviewService<V>,
    runs: SyncRunService<R>,
}

impl<P, C, V, R> SyncRunWorkflowService<P, C, V, R>
where
    P: SyncPolicyRepository,
    C: SyncPreviewInputCollector,
    V: SyncPreviewRepository,
    R: SyncRunRepository,
{
    pub fn new(policies: P, collector: C, previews: V, runs: R) -> Self {
        Self {
            policies: SyncPolicyService::new(policies),
            collector,
            previews: SyncPreviewService::new(previews),
            runs: SyncRunService::new(runs),
        }
    }

    pub async fn start_run(
        &self,
        command: StartWorkspaceSyncRun,
    ) -> Result<SyncRunSnapshot, SyncRunWorkflowError<P::Error, C::Error, V::Error, R::Error>> {
        let preview = self
            .previews
            .get(command.workspace_id, command.preview_id)
            .await
            .map_err(SyncRunWorkflowError::Preview)?;
        if preview.content_hash != command.preview_hash {
            return Err(SyncRunWorkflowError::Admission(
                SyncRunAdmissionError::PreviewHashMismatch,
            ));
        }
        let actions = project_preview(preview.clone())
            .map_err(|_| SyncRunWorkflowError::Projection)?
            .entries
            .into_iter()
            .filter(|entry| entry.category == SyncPreviewEntryCategory::Proposed)
            .map(|entry| {
                Ok(SyncActionAdmission {
                    action_hash: entry.action_hash,
                    action_id: generate_uuid()
                        .map_err(|_| SyncRunWorkflowError::IdentityGeneration)?,
                    task_id: generate_uuid()
                        .map_err(|_| SyncRunWorkflowError::IdentityGeneration)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let policy = self
            .policies
            .head(command.workspace_id, preview.policy_id)
            .await
            .map_err(SyncRunWorkflowError::Policy)?;
        let collected = self
            .collector
            .collect(CollectSyncPreviewInputs {
                preview_id: command.preview_id,
                workspace_id: command.workspace_id,
                policy,
                preview_time: preview.created_at,
                preview_expiry: preview.expires_at,
            })
            .await
            .map_err(SyncRunWorkflowError::Collection)?;
        self.runs
            .admit(AdmitSyncPreview {
                run_id: command.run_id,
                schedule_occurrence_id: command.schedule_occurrence_id,
                preview_id: command.preview_id,
                workspace_id: command.workspace_id,
                preview_hash: command.preview_hash,
                current_fingerprint: collected.fingerprint,
                actions,
                admitted_at: command.admitted_at,
            })
            .await
            .map_err(SyncRunWorkflowError::Admission)
    }
}

impl<P, C, V, R> SyncRunMutationApi for SyncRunWorkflowService<P, C, V, R>
where
    P: SyncPolicyRepository + 'static,
    P::Error: IntoApplicationError,
    C: SyncPreviewInputCollector + 'static,
    C::Error: IntoApplicationError,
    V: SyncPreviewRepository + 'static,
    V::Error: IntoApplicationError,
    R: SyncRunRepository + 'static,
    R::Error: IntoApplicationError,
{
    fn start_workspace_sync_run(
        &self,
        command: StartWorkspaceSyncRun,
    ) -> ApplicationFuture<'_, SyncRunSnapshot> {
        Box::pin(async move {
            let run_id = command.run_id;
            let preview_id = command.preview_id;
            let workspace_id = command.workspace_id;
            let scheduled = command.schedule_occurrence_id.is_some();
            let run = match self.start_run(command).await {
                Ok(run) => run,
                Err(error) => {
                    let stage = match &error {
                        SyncRunWorkflowError::Policy(_) => "policy",
                        SyncRunWorkflowError::Collection(_) => "collection",
                        SyncRunWorkflowError::Preview(_) => "preview_load",
                        SyncRunWorkflowError::Projection => "projection",
                        SyncRunWorkflowError::IdentityGeneration => "identity_generation",
                        SyncRunWorkflowError::Admission(_) => "admission",
                    };
                    tracing::warn!(
                        sync.run.id = %run_id,
                        sync.preview.id = %preview_id,
                        workspace.id = %workspace_id,
                        scheduled,
                        failure.stage = stage,
                        "synchronization run admission failed"
                    );
                    return Err(sync_run_workflow_error(error));
                }
            };
            tracing::info!(
                sync.run.id = %run_id,
                sync.preview.id = %preview_id,
                workspace.id = %workspace_id,
                scheduled,
                action_count = run.actions.len(),
                "synchronization run admitted"
            );
            Ok(run)
        })
    }
}

fn project_preview(
    snapshot: SyncPreviewSnapshot,
) -> Result<WorkspaceSyncPreview, SyncPreviewProjectionError> {
    let mut entries = Vec::with_capacity(snapshot.actions.len());
    for action in snapshot.actions {
        let stored = serde_json::from_slice::<StoredSyncPreviewAction>(&action.canonical_json)
            .map_err(|_| SyncPreviewProjectionError::MalformedAction)?;
        let before = stored.targeted_state.fields.iter().find_map(|field| {
            (field.field.field.id() == action.semantic_field)
                .then(|| field.value.as_ref().and_then(StoredStateValue::integer))
                .flatten()
        });
        let category =
            if before.and_then(|value| u64::try_from(value).ok()) == Some(stored.intended_value) {
                SyncPreviewEntryCategory::NoOp
            } else {
                SyncPreviewEntryCategory::Proposed
            };
        let finding = match (category, stored.lossy) {
            (SyncPreviewEntryCategory::NoOp, _) => {
                Some(SyncPreviewFindingKind::DestinationAlreadyMatches)
            }
            (_, true) => Some(SyncPreviewFindingKind::LossyTranslation),
            (_, false) => None,
        };
        let mut contributor_source_ids = stored
            .aggregation
            .contributors
            .into_iter()
            .map(|contributor| contributor.source_id)
            .collect::<Vec<_>>();
        contributor_source_ids.sort_unstable();
        contributor_source_ids.dedup();
        entries.push(SyncPreviewEntry {
            ordinal: action.ordinal,
            action_hash: action.action_hash,
            category,
            finding,
            receiver_source_id: action.receiver_source_id,
            receiver_subject: action.receiver_subject,
            semantic_field: action.semantic_field,
            segment_coordinate: action.segment.coordinate().to_owned(),
            segment_start: action.segment.start(),
            segment_end: action.segment.end(),
            before,
            intended: stored.intended_value,
            contributor_source_ids,
            membership_effect: stored.membership_effect,
            lossy: stored.lossy,
        });
    }
    let counts = SyncPreviewCounts {
        proposed: count_entries(&entries, SyncPreviewEntryCategory::Proposed)?,
        no_op: count_entries(&entries, SyncPreviewEntryCategory::NoOp)?,
        findings: u32::try_from(
            entries
                .iter()
                .filter(|entry| entry.finding.is_some())
                .count(),
        )
        .map_err(|_| SyncPreviewProjectionError::TooManyEntries)?,
    };
    Ok(WorkspaceSyncPreview {
        preview_id: snapshot.preview_id,
        workspace_id: snapshot.workspace_id,
        policy_id: snapshot.policy_id,
        policy_revision: snapshot.policy_revision,
        preview_hash: snapshot.content_hash,
        counts,
        entries,
        fresh_until: snapshot.fresh_until,
        created_at: snapshot.created_at,
        expires_at: snapshot.expires_at,
    })
}

fn count_entries(
    entries: &[SyncPreviewEntry],
    category: SyncPreviewEntryCategory,
) -> Result<u32, SyncPreviewProjectionError> {
    u32::try_from(
        entries
            .iter()
            .filter(|entry| entry.category == category)
            .count(),
    )
    .map_err(|_| SyncPreviewProjectionError::TooManyEntries)
}

fn sync_preview_lifecycle_error<E>(error: SyncPreviewLifecycleError<E>) -> ApplicationError
where
    E: IntoApplicationError,
{
    match error {
        SyncPreviewLifecycleError::PreviewNotFound => ApplicationError::new(
            ApplicationErrorCode::SyncPreviewNotFound,
            "synchronization preview was not found",
        ),
        SyncPreviewLifecycleError::Repository(error) => error.into_application_error(),
        SyncPreviewLifecycleError::InvalidSnapshot(_)
        | SyncPreviewLifecycleError::PreviewHashMismatch
        | SyncPreviewLifecycleError::PreviewExpired
        | SyncPreviewLifecycleError::RepositoryInvariant => ApplicationError::new(
            ApplicationErrorCode::Internal,
            "synchronization preview operation failed",
        ),
    }
}

fn sync_run_workflow_error<P, C, V, R>(error: SyncRunWorkflowError<P, C, V, R>) -> ApplicationError
where
    P: IntoApplicationError,
    C: IntoApplicationError,
    V: IntoApplicationError,
    R: IntoApplicationError,
{
    match error {
        SyncRunWorkflowError::Policy(SyncPolicyLifecycleError::PolicyNotFound) => {
            ApplicationError::new(
                ApplicationErrorCode::SyncPreviewStale,
                "synchronization preview policy is no longer available",
            )
        }
        SyncRunWorkflowError::Policy(SyncPolicyLifecycleError::Repository(error)) => {
            error.into_application_error()
        }
        SyncRunWorkflowError::Collection(error) => error.into_application_error(),
        SyncRunWorkflowError::Preview(error) => sync_preview_lifecycle_error(error),
        SyncRunWorkflowError::Projection | SyncRunWorkflowError::IdentityGeneration => {
            ApplicationError::new(
                ApplicationErrorCode::Internal,
                "synchronization run admission could not be prepared",
            )
        }
        SyncRunWorkflowError::Admission(SyncRunAdmissionError::InvalidActions) => {
            ApplicationError::new(
                ApplicationErrorCode::Internal,
                "synchronization run admission is invalid",
            )
        }
        SyncRunWorkflowError::Admission(SyncRunAdmissionError::PreviewNotFound) => {
            ApplicationError::new(
                ApplicationErrorCode::SyncPreviewNotFound,
                "synchronization preview was not found",
            )
        }
        SyncRunWorkflowError::Admission(SyncRunAdmissionError::PreviewHashMismatch) => {
            ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "synchronization run admission is invalid",
                "previewHash",
                "preview_hash_mismatch",
                "preview hash does not match the synchronization preview",
            )
        }
        SyncRunWorkflowError::Admission(SyncRunAdmissionError::PreviewStale) => {
            ApplicationError::new(
                ApplicationErrorCode::SyncPreviewStale,
                "synchronization preview inputs are stale",
            )
        }
        SyncRunWorkflowError::Admission(SyncRunAdmissionError::PreviewAlreadyAdmitted) => {
            ApplicationError::new(
                ApplicationErrorCode::SyncPreviewAlreadyAdmitted,
                "synchronization preview is already admitted",
            )
        }
        SyncRunWorkflowError::Admission(SyncRunAdmissionError::ReceiverUnavailable) => {
            ApplicationError::new(
                ApplicationErrorCode::ConnectionUnavailable,
                "synchronization receiver is unavailable",
            )
        }
        SyncRunWorkflowError::Admission(SyncRunAdmissionError::ReceiverActionBlocked) => {
            ApplicationError::new(
                ApplicationErrorCode::TaskResourceUnavailable,
                "synchronization receiver action is already active",
            )
        }
        SyncRunWorkflowError::Admission(SyncRunAdmissionError::Repository(error)) => {
            error.into_application_error()
        }
        SyncRunWorkflowError::Policy(
            SyncPolicyLifecycleError::InvalidPolicy(_)
            | SyncPolicyLifecycleError::InvalidPage
            | SyncPolicyLifecycleError::HeadMoved { .. }
            | SyncPolicyLifecycleError::RevisionOverflow
            | SyncPolicyLifecycleError::RepositoryInvariant,
        )
        | SyncRunWorkflowError::Admission(SyncRunAdmissionError::RepositoryInvariant) => {
            ApplicationError::new(
                ApplicationErrorCode::Internal,
                "synchronization run admission failed",
            )
        }
    }
}

fn generate_uuid() -> Result<Uuid, getrandom::Error> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

fn sync_workflow_error<P, C, V>(error: SyncWorkflowError<P, C, V>) -> ApplicationError
where
    P: IntoApplicationError,
    C: IntoApplicationError,
    V: IntoApplicationError,
{
    match error {
        SyncWorkflowError::Policy(SyncPolicyLifecycleError::PolicyNotFound) => {
            ApplicationError::new(
                ApplicationErrorCode::SyncPolicyNotFound,
                "synchronization policy was not found",
            )
        }
        SyncWorkflowError::Policy(SyncPolicyLifecycleError::Repository(error)) => {
            error.into_application_error()
        }
        SyncWorkflowError::Collection(error) => error.into_application_error(),
        SyncWorkflowError::Preview(error) => sync_preview_lifecycle_error(error),
        SyncWorkflowError::Policy(
            SyncPolicyLifecycleError::InvalidPolicy(_)
            | SyncPolicyLifecycleError::InvalidPage
            | SyncPolicyLifecycleError::HeadMoved { .. }
            | SyncPolicyLifecycleError::RevisionOverflow
            | SyncPolicyLifecycleError::RepositoryInvariant,
        )
        | SyncWorkflowError::Construction(_) => ApplicationError::new(
            ApplicationErrorCode::InvalidProviderData,
            "synchronization preview inputs are invalid",
        ),
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredSyncPreviewAction {
    intended_value: u64,
    lossy: bool,
    membership_effect: PreviewMembershipEffect,
    targeted_state: StoredTargetedState,
    aggregation: StoredAggregation,
}

#[derive(Clone, Debug, Deserialize)]
struct StoredAggregation {
    contributors: Vec<StoredContributor>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredContributor {
    source_id: Uuid,
}

#[derive(Clone, Debug, Deserialize)]
struct StoredTargetedState {
    fields: Vec<StoredTargetedField>,
}

#[derive(Clone, Debug, Deserialize)]
struct StoredTargetedField {
    field: StoredStateField,
    value: Option<StoredStateValue>,
}

#[derive(Clone, Debug, Deserialize)]
struct StoredStateField {
    field: StoredTerm,
}

#[derive(Clone, Debug, Deserialize)]
struct StoredTerm {
    namespace: String,
    name: String,
}

impl StoredTerm {
    fn id(&self) -> String {
        format!("{}:{}", self.namespace, self.name)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct StoredStateValue {
    kind: String,
    integer: Option<i64>,
}

impl StoredStateValue {
    fn integer(&self) -> Option<i64> {
        (self.kind == "integer").then_some(self.integer).flatten()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
enum SyncPreviewProjectionError {
    #[error("stored synchronization preview action is malformed")]
    MalformedAction,
    #[error("synchronization preview contains too many entries")]
    TooManyEntries,
}

#[cfg(test)]
#[path = "sync_workflow_tests.rs"]
mod tests;
