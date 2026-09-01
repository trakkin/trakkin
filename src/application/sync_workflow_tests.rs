use std::sync::{Arc, Mutex};

use time::{OffsetDateTime, macros::datetime};
use trakkin_mapping::PortableEndpoint;
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::{
            AdapterSourceMembership, AdapterStatePresence, AdapterTargetedStateField,
            AdapterTargetedStateMatch, AdapterTargetedStateReadCapability,
            AdapterTargetedStateReadOutcome,
        },
        error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
        mapping_resolution::{
            DirectResolutionPins, EndpointLookupMatch, ExactSegment, MappingResolution,
            MappingResolutionFuture, MappingResolutionInteraction, PublishedReferenceMatch,
            ResolutionCompleteness, ResolutionProof, ResolutionProofKind, ResolutionRequest,
            ResolutionRevisionPin, ResolutionSemanticWork, ResolutionSourcePin,
            ResolvedCorrespondence,
        },
        sync_policy::SyncPolicyRevisionSnapshot,
        sync_preview::{
            ReceiverProgressCapability, SyncPreviewMembershipPin, TargetedReceiverPreflight,
        },
        sync_state_evidence::{
            DiscoverSyncContributorRoots, SyncContributorRoot, SyncContributorRootFuture,
            SyncContributorRootProjection, SyncStateEvidence, SyncStateEvidenceFuture,
            SyncStateEvidencePresence, SyncStateEvidenceProjection, SyncStateEvidenceRepository,
            SyncStateEvidenceSlot, SyncStatePublicationPin,
        },
    },
    domain::{
        acquisition::{ProviderItemKey, StateField, StateSubject, StateValue, Term},
        synchronization::{OrdinalSpan, Quantizer, StateRegistry, StateUnit, SyncPolicyDocument},
    },
};

use super::{
    CollectSyncPreviewInputs, PreflightSyncReceivers, ProductionSyncPreviewInputCollector,
    SyncPreviewInputCollector, SyncReceiverPreflightApi, SyncReceiverPreflightResult,
    SyncReceiverPreflightTarget,
};

const WORKSPACE_ID: Uuid = Uuid::from_u128(1);
const POLICY_ID: Uuid = Uuid::from_u128(2);
const PREVIEW_ID: Uuid = Uuid::from_u128(3);
const CONTRIBUTOR_ID: Uuid = Uuid::from_u128(4);
const RECEIVER_ID: Uuid = Uuid::from_u128(5);
const CONNECTION_ID: Uuid = Uuid::from_u128(6);
const PUBLICATION_ID: Uuid = Uuid::from_u128(7);
const SECOND_CONTRIBUTOR_ID: Uuid = Uuid::from_u128(8);
const CREATED_AT: OffsetDateTime = datetime!(2030-01-02 3:04:05 UTC);
const EXPIRES_AT: OffsetDateTime = datetime!(2030-01-02 3:09:05 UTC);

#[derive(Clone)]
struct StaticEvidenceRepository {
    projection: SyncContributorRootProjection,
    queries: Arc<Mutex<Vec<DiscoverSyncContributorRoots>>>,
}

#[derive(Debug)]
struct StaticEvidenceError;

impl IntoApplicationError for StaticEvidenceError {
    fn into_application_error(self) -> ApplicationError {
        ApplicationError::new(ApplicationErrorCode::Internal, "static evidence failed")
    }
}

impl SyncStateEvidenceRepository for StaticEvidenceRepository {
    type Error = StaticEvidenceError;

    fn discover_sync_contributor_roots(
        &self,
        query: DiscoverSyncContributorRoots,
    ) -> SyncContributorRootFuture<'_, Self::Error> {
        self.queries
            .lock()
            .expect("evidence query lock")
            .push(query);
        let projection = self.projection.clone();
        Box::pin(async move { Ok(projection) })
    }

    fn query_sync_state_evidence(
        &self,
        _query: crate::application::sync_state_evidence::QuerySyncStateEvidence,
    ) -> SyncStateEvidenceFuture<'_, Self::Error> {
        Box::pin(async move {
            Ok(SyncStateEvidenceProjection {
                publications: Vec::new(),
                evidence: Vec::new(),
            })
        })
    }
}

#[derive(Clone)]
struct StaticMappingResolution {
    resolution: MappingResolution,
    requests: Arc<Mutex<Vec<ResolutionRequest>>>,
}

impl MappingResolutionInteraction for StaticMappingResolution {
    fn resolve_workspace_mappings(
        &self,
        request: ResolutionRequest,
    ) -> MappingResolutionFuture<'_, MappingResolution, ApplicationError> {
        self.requests
            .lock()
            .expect("resolution request lock")
            .push(request);
        let resolution = self.resolution.clone();
        Box::pin(async move { Ok(resolution) })
    }

    fn resolve_mapping_revision(
        &self,
        _revision: crate::application::mapping_resolution::ExactMappingRevision,
        _request: ResolutionRequest,
    ) -> MappingResolutionFuture<'_, MappingResolution, ApplicationError> {
        let resolution = self.resolution.clone();
        Box::pin(async move { Ok(resolution) })
    }
}

#[derive(Clone)]
struct StaticReceiverPreflight {
    result: SyncReceiverPreflightResult,
    commands: Arc<Mutex<Vec<PreflightSyncReceivers>>>,
}

impl SyncReceiverPreflightApi for StaticReceiverPreflight {
    fn preflight_receivers(
        &self,
        command: PreflightSyncReceivers,
    ) -> super::ApplicationFuture<'_, Vec<SyncReceiverPreflightResult>> {
        self.commands
            .lock()
            .expect("preflight command lock")
            .push(command);
        let result = self.result.clone();
        Box::pin(async move { Ok(vec![result]) })
    }
}

#[tokio::test]
async fn production_collector_joins_bounded_pinned_inputs_without_writes() {
    let root =
        PortableEndpoint::parse("example.media:original[episode:1..12]").expect("contributor root");
    let receiver_endpoint = PortableEndpoint::parse("example.media:receiver[episode:1..12]")
        .expect("receiver endpoint");
    let field = progress_field();
    let contributor_subject = provider_subject("contributor");
    let receiver_subject = provider_subject("receiver");
    let evidence_queries = Arc::new(Mutex::new(Vec::new()));
    let resolution_requests = Arc::new(Mutex::new(Vec::new()));
    let preflight_commands = Arc::new(Mutex::new(Vec::new()));
    let preflight_result = SyncReceiverPreflightResult {
        target: SyncReceiverPreflightTarget {
            source_id: RECEIVER_ID,
            subject: receiver_subject.clone(),
            field: field.clone(),
        },
        receiver: TargetedReceiverPreflight {
            source_id: RECEIVER_ID,
            subject: receiver_subject,
            state_read_capability: AdapterTargetedStateReadCapability {
                maximum_fields: 1,
                maximum_response_bytes: 4096,
            },
            progress_capability: ReceiverProgressCapability {
                unit: StateUnit::Episode,
                minimum: 0,
                maximum: 12,
                step: 1,
                quantizer: Quantizer::Exact,
            },
            outcome: AdapterTargetedStateReadOutcome::Matched(AdapterTargetedStateMatch {
                membership: AdapterSourceMembership::Absent,
                fields: vec![AdapterTargetedStateField {
                    field: field.clone(),
                    presence: AdapterStatePresence::Absent,
                    value: None,
                }],
                provider_revision: b"receiver-r1".to_vec(),
                observed_at: CREATED_AT,
                expires_at: Some(EXPIRES_AT),
                precondition: Some(b"receiver-precondition-r1".to_vec()),
                write_causation: None,
            }),
        },
        connection_id: CONNECTION_ID,
        source_capability_revision: 1,
        connection_capability_revision: 1,
        process_instance_id: "fixture-process-1".to_owned(),
    };
    let collector = ProductionSyncPreviewInputCollector::new(
        StaticEvidenceRepository {
            projection: contributor_projection(root.clone(), contributor_subject, field),
            queries: evidence_queries.clone(),
        },
        StaticMappingResolution {
            resolution: mapping_resolution(root, receiver_endpoint),
            requests: resolution_requests.clone(),
        },
        StaticReceiverPreflight {
            result: preflight_result,
            commands: preflight_commands.clone(),
        },
        StateRegistry::v1(),
    );
    let policy = policy_snapshot();

    let collected = collector
        .collect(CollectSyncPreviewInputs {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            policy: policy.clone(),
            preview_time: CREATED_AT,
            preview_expiry: EXPIRES_AT,
        })
        .await
        .expect("production inputs collect");

    assert_eq!(evidence_queries.lock().unwrap().len(), 1);
    assert_eq!(resolution_requests.lock().unwrap().len(), 1);
    let preflight_commands = preflight_commands.lock().unwrap();
    assert_eq!(preflight_commands.len(), 1);
    assert_eq!(preflight_commands[0].operation_id, PREVIEW_ID);
    assert_eq!(preflight_commands[0].targets.len(), 1);
    assert_eq!(collected.partitions.len(), 1);
    let partition = &collected.partitions[0];
    assert_eq!(partition.semantic_field, "trakkin.state:progress");
    assert_eq!(
        partition.segment,
        OrdinalSpan::new("episode", 1, 12).unwrap()
    );
    assert_eq!(partition.contributors.len(), 2);
    assert_eq!(partition.receivers.len(), 1);
    assert_eq!(
        collected.fingerprint.active_mapping_hashes,
        vec![mapping_hash()]
    );
    assert_eq!(collected.fingerprint.mapping_activation_generation, 1);
    assert_eq!(
        collected.fingerprint.targeted_membership,
        SyncPreviewMembershipPin::Absent
    );
    assert_eq!(collected.fingerprint.preview_time, CREATED_AT);
    assert_eq!(collected.fingerprint.preview_expiry, EXPIRES_AT);
    collected
        .fingerprint
        .canonical_json()
        .expect("collected fingerprint is canonical");
}

fn contributor_projection(
    root: PortableEndpoint,
    subject: StateSubject,
    field: StateField,
) -> SyncContributorRootProjection {
    SyncContributorRootProjection {
        publications: [CONTRIBUTOR_ID, SECOND_CONTRIBUTOR_ID]
            .into_iter()
            .map(|source_id| SyncStatePublicationPin {
                source_id,
                status: crate::application::catalog_queries::PublishedStreamStatus::Available,
                publication_id: Some(PUBLICATION_ID),
                evidence_revision: Some(format!("state-publication-{source_id}").into_bytes()),
                last_success_at: Some(CREATED_AT),
                coverage: None,
            })
            .collect(),
        contributors: [CONTRIBUTOR_ID, SECOND_CONTRIBUTOR_ID]
            .into_iter()
            .map(|source_id| SyncContributorRoot {
                evidence: SyncStateEvidence {
                    slot: SyncStateEvidenceSlot {
                        source_id,
                        subject: subject.clone(),
                        field: field.clone(),
                    },
                    presence: SyncStateEvidencePresence::Present,
                    value: Some(StateValue::Integer(12)),
                    provider_revision: Some(format!("contributor-{source_id}").into_bytes()),
                    observed_at: Some(CREATED_AT),
                },
                roots: vec![root.clone()],
            })
            .collect(),
    }
}

fn mapping_resolution(
    root: PortableEndpoint,
    receiver_endpoint: PortableEndpoint,
) -> MappingResolution {
    MappingResolution {
        completeness: ResolutionCompleteness::Complete,
        pins: Some(DirectResolutionPins {
            revisions: vec![ResolutionRevisionPin {
                mapping_id: Uuid::from_u128(10),
                content_hash: mapping_hash(),
                activation_generation: Some(1),
            }],
            sources: vec![
                source_pin(CONTRIBUTOR_ID, Uuid::from_u128(11)),
                source_pin(SECOND_CONTRIBUTOR_ID, Uuid::from_u128(15)),
                source_pin(RECEIVER_ID, CONNECTION_ID),
            ],
        }),
        correspondences: vec![ResolvedCorrespondence {
            endpoints: vec![root, receiver_endpoint.clone()],
            segment: Some(ExactSegment {
                coordinate: "episode".to_owned(),
                source_start: 1,
                source_end: 12,
                target_coordinate: "episode".to_owned(),
                target_start_numerator: 1,
                target_end_exclusive_numerator: 13,
                target_denominator: 1,
            }),
            support_count: 1,
            proof: ResolutionProof {
                key: "mapping-proof".to_owned(),
                kind: ResolutionProofKind::Direct,
                revision_hash: mapping_hash(),
                row_hash: format!("sha256:{}", "b".repeat(64)),
                endpoint_hash: format!("sha256:{}", "c".repeat(64)),
                parent_proofs: Vec::new(),
                transform: crate::application::mapping_resolution::ExactTransform {
                    numerator: 1,
                    denominator: 1,
                },
                catalog_publications: vec![PUBLICATION_ID],
                coordinate_publications: vec![Uuid::from_u128(12)],
            },
            supporting_proofs: Vec::new(),
        }],
        findings: Vec::new(),
        published_matches: vec![PublishedReferenceMatch {
            endpoint: receiver_endpoint,
            source_id: RECEIVER_ID,
            item_key_namespace: "fixture.subject".to_owned(),
            item_key_value: b"receiver".to_vec(),
            catalog_publication_id: Uuid::from_u128(13),
            coordinate_publication_id: Some(Uuid::from_u128(14)),
            coordinate_evidence_revision: Some(b"coordinate-r1".to_vec()),
            coordinate_observed_at: Some(CREATED_AT),
            coordinate_expires_at: Some(EXPIRES_AT),
            backing: None,
        }],
        endpoint_lookup_matches: Vec::<EndpointLookupMatch>::new(),
        lookup_calls: 0,
        semantic_work: ResolutionSemanticWork::default(),
        next_work_key: None,
    }
}

fn source_pin(source_id: Uuid, connection_id: Uuid) -> ResolutionSourcePin {
    ResolutionSourcePin {
        source_id,
        connection_id,
        attached_at: CREATED_AT,
        source_removed: false,
        source_capability_revision: 1,
        connection_ready: true,
        connection_deleted: false,
        connection_capability_revision: 1,
        catalog_available: true,
        catalog_publication_id: Some(PUBLICATION_ID),
        catalog_evidence_revision: Some(b"catalog-r1".to_vec()),
        coordinate_publication_id: Some(Uuid::from_u128(14)),
        coordinate_evidence_revision: Some(b"coordinate-r1".to_vec()),
        coordinate_expires_at: Some(EXPIRES_AT),
    }
}

fn policy_snapshot() -> SyncPolicyRevisionSnapshot {
    let document = SyncPolicyDocument::from_json(
        &serde_json::json!({
            "contributors": [CONTRIBUTOR_ID, SECOND_CONTRIBUTOR_ID],
            "receivers": [RECEIVER_ID],
            "resolutionMode": "direct",
            "rules": [{
                "conflict": "require_reducer",
                "presence": {
                    "absent": "ignore",
                    "deleted": "ignore",
                    "stale": "block",
                    "unknown": "block"
                },
                "reducer": "coverage_contiguous_prefix",
                "semanticField": "trakkin.state:progress",
                "translation": {"allowLossy": false, "quantizer": "exact"},
                "writeConstraints": {
                    "allowClear": false,
                    "allowCreateMembership": true,
                    "requireStrongPrecondition": false
                }
            }],
            "schema": "trakkin.sync-policy/v1"
        })
        .to_string(),
        StateRegistry::v1(),
    )
    .expect("policy document");
    SyncPolicyRevisionSnapshot {
        policy_id: POLICY_ID,
        workspace_id: WORKSPACE_ID,
        revision: 1,
        content_hash: document.content_hash(),
        document,
        created_at: CREATED_AT,
    }
}

fn progress_field() -> StateField {
    StateField::new(
        Term::new("trakkin.state", "progress").unwrap(),
        Some(Term::new("trakkin.unit", "episode").unwrap()),
    )
}

fn provider_subject(value: &str) -> StateSubject {
    StateSubject::ProviderItem(
        ProviderItemKey::new("fixture.subject", value.as_bytes().to_vec()).unwrap(),
    )
}

fn mapping_hash() -> String {
    format!("sha256:{}", "a".repeat(64))
}
