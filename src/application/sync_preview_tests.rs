use serde::Deserialize;
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339, macros::datetime};
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::{
            AdapterSourceMembership, AdapterStatePresence, AdapterTargetedStateField,
            AdapterTargetedStateMatch, AdapterTargetedStateReadCapability,
            AdapterTargetedStateReadOutcome,
        },
        mapping_resolution::{ExactTransform, ResolutionProof, ResolutionProofKind},
        sync_policy::SyncPolicyRevisionSnapshot,
    },
    domain::{
        acquisition::{ProviderItemKey, StateField, StateSubject, StateValue, Term},
        synchronization::{
            ExactRational, OrdinalSpan, Quantizer, StateRegistry, StateUnit, SyncPolicyDocument,
        },
    },
};

use super::{
    ConstructSyncPreview, CreateSyncPreviewSnapshot, PreviewMembershipEffect,
    PublishedStatePresence, PublishedSyncContributor, ReceiverProgressCapability,
    SyncPreviewBuilder, SyncPreviewConstructionError, SyncPreviewFingerprint, SyncPreviewFreshness,
    SyncPreviewMembershipPin, SyncPreviewPartitionInput, SyncPreviewSnapshot,
    TargetedReceiverPreflight, evaluate_sync_preview_freshness, sync_preview_policy_revision_pin,
};

const AGGREGATION_VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures.sync/v1/aggregation/vectors.json"
));
const PREVIEW_VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures.sync/v1/preview/vectors.json"
));

#[derive(Deserialize)]
struct StalePreviewVector {
    input: StalePreviewInput,
}

#[derive(Deserialize)]
struct StalePreviewInput {
    fingerprint: serde_json::Value,
    mutations: Vec<String>,
}

#[derive(Deserialize)]
struct AggregationVector {
    input: AggregationInput,
    expected: AggregationExpected,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AggregationInput {
    field: String,
    contributors: Vec<VectorContributor>,
    mapping: Vec<VectorMapping>,
    receivers: Vec<VectorReceiver>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorContributor {
    evidence_id: String,
    source: String,
    subject: String,
    root: String,
    presence: String,
    coverage: Vec<String>,
    publication: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorMapping {
    proof_id: String,
    endpoints: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorReceiver {
    source: String,
    unit: String,
    range: [u64; 2],
    step: u64,
    quantizer: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AggregationExpected {
    candidates: Vec<ExpectedCandidate>,
    receiver_actions: Vec<ExpectedAction>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedCandidate {
    partition: String,
    value: String,
    contributor_evidence_ids: Vec<String>,
    mapping_proof_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedAction {
    partition_id: String,
    source: String,
    value: u64,
    lossy: bool,
    mapping_proof_ids: Vec<String>,
}

#[test]
fn sync_stale_preview_rejects_every_composite_fingerprint_mutation() {
    let suite: serde_json::Value = serde_json::from_str(PREVIEW_VECTORS).expect("valid vectors");
    let vector = suite["vectors"]
        .as_array()
        .expect("preview vectors")
        .iter()
        .find(|vector| vector["id"] == "SYNC-STALE-PREVIEW-001")
        .cloned()
        .map(serde_json::from_value::<StalePreviewVector>)
        .expect("stale preview vector")
        .expect("valid stale preview vector");
    let fingerprint = fingerprint_from_value(&vector.input.fingerprint);
    assert_eq!(
        evaluate_sync_preview_freshness(&fingerprint, &fingerprint, fingerprint.preview_time),
        SyncPreviewFreshness::Fresh
    );

    for mutation in &vector.input.mutations {
        let mut current = fingerprint.clone();
        mutate_fingerprint(&mut current, mutation);
        assert_eq!(
            evaluate_sync_preview_freshness(&fingerprint, &current, fingerprint.preview_time),
            SyncPreviewFreshness::Stale,
            "fingerprint mutation {mutation} must be stale"
        );
    }

    assert_eq!(
        evaluate_sync_preview_freshness(&fingerprint, &fingerprint, fingerprint.lookup_expiry),
        SyncPreviewFreshness::Stale
    );
    assert_eq!(
        evaluate_sync_preview_freshness(&fingerprint, &fingerprint, fingerprint.targeted_expiry),
        SyncPreviewFreshness::Stale
    );
    assert_eq!(
        evaluate_sync_preview_freshness(&fingerprint, &fingerprint, fingerprint.preview_expiry),
        SyncPreviewFreshness::Stale
    );
}

#[test]
fn sync_nm_split_merge_constructs_a_deterministic_no_write_preview() {
    let suite: serde_json::Value =
        serde_json::from_str(AGGREGATION_VECTORS).expect("valid vectors");
    let vector = suite["vectors"]
        .as_array()
        .expect("aggregation vectors")
        .iter()
        .find(|vector| vector["id"] == "SYNC-NM-SPLIT-MERGE-001")
        .cloned()
        .map(serde_json::from_value::<AggregationVector>)
        .expect("N:M split/merge vector")
        .expect("valid N:M split/merge vector");
    let workspace_id = Uuid::from_u128(1);
    let policy_id = Uuid::from_u128(2);
    let source_ids = source_ids();
    let receiver_quantizers = vector
        .input
        .receivers
        .iter()
        .map(|receiver| {
            (
                receiver.source.clone(),
                parse_quantizer(&receiver.quantizer),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let policy = policy_snapshot(workspace_id, policy_id, &source_ids, true, false);
    let command = preview_command(vector.input, policy, &source_ids);
    let mut reordered = command.clone();
    reordered.partitions.reverse();
    for partition in &mut reordered.partitions {
        partition.contributors.reverse();
        partition.receivers.reverse();
        partition.mapping_proofs.reverse();
    }

    let builder = SyncPreviewBuilder::new(StateRegistry::v1());
    let preview = builder
        .construct(command.clone())
        .expect("preview constructs");
    let reordered_preview = builder
        .construct(reordered)
        .expect("reordered preview constructs");
    let preview_snapshot = SyncPreviewSnapshot::from_draft(CreateSyncPreviewSnapshot {
        preview_id: Uuid::from_u128(40),
        workspace_id,
        fingerprint: snapshot_fingerprint(
            &preview,
            datetime!(2030-01-02 3:04:05 UTC),
            datetime!(2030-01-02 3:09:05 UTC),
        ),
        draft: preview.clone(),
        created_at: datetime!(2030-01-02 3:04:05 UTC),
        expires_at: datetime!(2030-01-02 3:09:05 UTC),
    })
    .expect("preview snapshot is canonical");
    let reordered_snapshot = SyncPreviewSnapshot::from_draft(CreateSyncPreviewSnapshot {
        preview_id: Uuid::from_u128(41),
        workspace_id,
        fingerprint: snapshot_fingerprint(
            &reordered_preview,
            datetime!(2030-01-02 3:04:05 UTC),
            datetime!(2030-01-02 3:09:05 UTC),
        ),
        draft: reordered_preview.clone(),
        created_at: datetime!(2030-01-02 3:04:05 UTC),
        expires_at: datetime!(2030-01-02 3:09:05 UTC),
    })
    .expect("reordered preview snapshot is canonical");

    let mut stale_preflight = command.clone();
    let AdapterTargetedStateReadOutcome::Matched(targeted_state) =
        &mut stale_preflight.partitions[0].receivers[0].outcome
    else {
        panic!("matched preflight")
    };
    targeted_state.fields[0].presence = AdapterStatePresence::Stale;
    assert!(matches!(
        builder.construct(stale_preflight),
        Err(SyncPreviewConstructionError::TargetedFieldBlocked { .. })
    ));

    let mut missing_precondition = command.clone();
    missing_precondition.policy = policy_snapshot(workspace_id, policy_id, &source_ids, true, true);
    let AdapterTargetedStateReadOutcome::Matched(targeted_state) =
        &mut missing_precondition.partitions[0].receivers[0].outcome
    else {
        panic!("matched preflight")
    };
    targeted_state.precondition = None;
    assert!(matches!(
        builder.construct(missing_precondition),
        Err(SyncPreviewConstructionError::StrongPreconditionRequired { .. })
    ));

    let mut forbidden_creation = command.clone();
    forbidden_creation.policy = policy_snapshot(workspace_id, policy_id, &source_ids, false, false);
    assert!(matches!(
        builder.construct(forbidden_creation),
        Err(SyncPreviewConstructionError::MembershipCreationNotAllowed { .. })
    ));

    assert_eq!(preview, reordered_preview);
    assert_eq!(
        preview_snapshot.content_hash,
        reordered_snapshot.content_hash
    );
    assert_eq!(
        preview_snapshot.canonical_json,
        reordered_snapshot.canonical_json
    );
    assert_eq!(preview_snapshot.actions, reordered_snapshot.actions);
    assert_ne!(preview_snapshot.preview_id, reordered_snapshot.preview_id);
    assert_eq!(preview_snapshot.actions.len(), 4);
    assert!(
        preview_snapshot
            .actions
            .iter()
            .enumerate()
            .all(|(ordinal, action)| {
                action.ordinal == u32::try_from(ordinal).expect("bounded action ordinal")
                    && action.action_hash.starts_with("sha256:")
                    && action.action_hash.len() == 71
            })
    );
    assert_eq!(preview.policy_id, policy_id);
    assert_eq!(preview.policy_revision, 1);
    assert_eq!(preview.candidates.len(), 2);
    assert_eq!(preview.actions.len(), 4);

    for expected in &vector.expected.candidates {
        let expected_span = parse_span(&expected.partition);
        let candidate = preview
            .candidates
            .iter()
            .find(|candidate| candidate.segment == expected_span)
            .expect("expected candidate partition");
        assert_eq!(candidate.value, parse_rational(&expected.value));
        assert_eq!(
            candidate
                .explanation
                .contributors
                .iter()
                .map(|contributor| contributor.evidence_id.clone())
                .collect::<Vec<_>>(),
            expected.contributor_evidence_ids
        );
        assert_eq!(
            candidate
                .explanation
                .mapping_proofs
                .iter()
                .map(|proof| proof.key.as_str())
                .collect::<Vec<_>>(),
            expected
                .mapping_proof_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
    }

    for expected in &vector.expected.receiver_actions {
        let partition = vector
            .expected
            .candidates
            .iter()
            .find(|candidate| {
                candidate
                    .partition
                    .ends_with(if expected.partition_id == "partition-a" {
                        "1..6"
                    } else {
                        "7..12"
                    })
            })
            .expect("expected action partition");
        let expected_span = parse_span(&partition.partition);
        let source_id = source_ids[expected.source.as_str()];
        let action = preview
            .actions
            .iter()
            .find(|action| {
                action.receiver_source_id == source_id && action.segment == expected_span
            })
            .expect("expected receiver action");
        assert_eq!(action.intended_value, expected.value);
        assert_eq!(action.lossy, expected.lossy);
        assert_eq!(action.translation.lossy, expected.lossy);
        assert_eq!(
            action.translation.quantizer,
            receiver_quantizers[expected.source.as_str()]
        );
        assert_eq!(
            action.translation.exact_value,
            ExactRational::new(
                action.candidate.numerator() * action.segment.length(),
                action.candidate.denominator(),
            )
            .expect("exact translated candidate")
        );
        assert_eq!(
            action
                .aggregation
                .mapping_proofs
                .iter()
                .map(|proof| proof.key.as_str())
                .collect::<Vec<_>>(),
            expected
                .mapping_proof_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        assert_eq!(action.targeted_read_capability.maximum_fields, 1);
        assert_eq!(action.receiver_capability.unit, StateUnit::Episode);
        assert_eq!(
            action.targeted_state.fields[0].presence,
            if expected.source == "source-local-receiver" {
                AdapterStatePresence::Absent
            } else {
                AdapterStatePresence::Present
            }
        );
        if expected.source == "source-local-receiver" {
            assert_eq!(action.membership_effect, PreviewMembershipEffect::Create);
            assert_eq!(
                action.targeted_state.membership,
                AdapterSourceMembership::Absent
            );
        } else {
            assert_eq!(action.membership_effect, PreviewMembershipEffect::Update);
            assert_eq!(
                action.targeted_state.membership,
                AdapterSourceMembership::Present
            );
        }
    }

    let action_order = preview
        .actions
        .iter()
        .map(|action| {
            (
                action.receiver_source_id,
                subject_order_key(&action.receiver_subject),
                action.semantic_field.clone(),
                action.segment.clone(),
            )
        })
        .collect::<Vec<_>>();
    let mut sorted_action_order = action_order.clone();
    sorted_action_order.sort_unstable();
    assert_eq!(action_order, sorted_action_order);
}

fn source_ids() -> std::collections::BTreeMap<&'static str, Uuid> {
    std::collections::BTreeMap::from([
        ("source-local-contributor-a", Uuid::from_u128(10)),
        ("source-local-contributor-b", Uuid::from_u128(11)),
        ("source-local-receiver", Uuid::from_u128(12)),
        ("source-local-coarse", Uuid::from_u128(13)),
    ])
}

fn fingerprint_from_value(value: &serde_json::Value) -> SyncPreviewFingerprint {
    let string = |field: &str| {
        value[field]
            .as_str()
            .unwrap_or_else(|| panic!("fingerprint field {field}"))
            .to_owned()
    };
    let time = |field: &str| {
        OffsetDateTime::parse(
            value[field]
                .as_str()
                .unwrap_or_else(|| panic!("fingerprint time {field}")),
            &Rfc3339,
        )
        .unwrap_or_else(|_| panic!("valid fingerprint time {field}"))
    };
    SyncPreviewFingerprint {
        policy_revision: string("policyRevision"),
        state_registry_digest: string("stateRegistryDigest"),
        resolver_version: string("resolverVersion"),
        active_mapping_hashes: value["activeMappingHashes"]
            .as_array()
            .expect("active mapping hashes")
            .iter()
            .map(|hash| hash.as_str().expect("active mapping hash").to_owned())
            .collect(),
        mapping_activation_generation: value["mappingActivationGeneration"]
            .as_u64()
            .expect("mapping activation generation"),
        mapping_proof_digest: string("mappingProofDigest"),
        attachment_digest: string("attachmentDigest"),
        source_capability_revision: string("sourceCapabilityRevision"),
        connection_capability_revision: string("connectionCapabilityRevision"),
        state_publication: string("statePublication"),
        contributor_evidence_revisions: value["contributorEvidenceRevisions"]
            .as_array()
            .expect("contributor evidence revisions")
            .iter()
            .map(|revision| {
                revision
                    .as_str()
                    .expect("contributor evidence revision")
                    .to_owned()
            })
            .collect(),
        catalog_publication: string("catalogPublication"),
        coordinate_publication: string("coordinatePublication"),
        lookup_process: string("lookupProcess"),
        lookup_revision: string("lookupRevision"),
        lookup_observed_at: time("lookupObservedAt"),
        lookup_expiry: time("lookupExpiry"),
        targeted_membership: match value["targetedMembership"]
            .as_str()
            .expect("targeted membership")
        {
            "present" => SyncPreviewMembershipPin::Present,
            "absent" => SyncPreviewMembershipPin::Absent,
            "unknown" => SyncPreviewMembershipPin::Unknown,
            other => panic!("unsupported targeted membership {other}"),
        },
        targeted_state_revision: string("targetedStateRevision"),
        targeted_precondition: string("targetedPrecondition"),
        targeted_expiry: time("targetedExpiry"),
        preview_time: time("previewTime"),
        preview_expiry: time("previewExpiry"),
    }
}

fn snapshot_fingerprint(
    draft: &super::SyncPreviewDraft,
    preview_time: OffsetDateTime,
    preview_expiry: OffsetDateTime,
) -> SyncPreviewFingerprint {
    SyncPreviewFingerprint {
        policy_revision: sync_preview_policy_revision_pin(
            draft.policy_revision,
            &draft.policy_content_hash,
        ),
        state_registry_digest: "sha256:state-registry-v1".to_owned(),
        resolver_version: "trakkin.mapping-resolver/v1".to_owned(),
        active_mapping_hashes: vec!["sha256:mapping-preview".to_owned()],
        mapping_activation_generation: 1,
        mapping_proof_digest: "sha256:mapping-proofs".to_owned(),
        attachment_digest: "sha256:attachments".to_owned(),
        source_capability_revision: "sha256:source-capabilities".to_owned(),
        connection_capability_revision: "sha256:connection-capabilities".to_owned(),
        state_publication: "sha256:state-publications".to_owned(),
        contributor_evidence_revisions: vec!["sha256:state-evidence".to_owned()],
        catalog_publication: "sha256:catalog-publications".to_owned(),
        coordinate_publication: "sha256:coordinate-publications".to_owned(),
        lookup_process: "sha256:lookup-processes".to_owned(),
        lookup_revision: "sha256:lookup-revisions".to_owned(),
        lookup_observed_at: preview_time,
        lookup_expiry: preview_time + Duration::minutes(1),
        targeted_membership: SyncPreviewMembershipPin::Absent,
        targeted_state_revision: "sha256:targeted-state".to_owned(),
        targeted_precondition: "sha256:targeted-preconditions".to_owned(),
        targeted_expiry: preview_time + Duration::minutes(1),
        preview_time,
        preview_expiry,
    }
}

fn mutate_fingerprint(fingerprint: &mut SyncPreviewFingerprint, mutation: &str) {
    match mutation {
        "policyRevision" => fingerprint.policy_revision.push_str("-changed"),
        "stateRegistryDigest" => fingerprint.state_registry_digest.push_str("-changed"),
        "resolverVersion" => fingerprint.resolver_version.push_str("-changed"),
        "activeMappingHashes" => fingerprint
            .active_mapping_hashes
            .push("sha256:mapping-r2".to_owned()),
        "mappingActivationGeneration" => fingerprint.mapping_activation_generation += 1,
        "mappingProofDigest" => fingerprint.mapping_proof_digest.push_str("-changed"),
        "attachmentDigest" => fingerprint.attachment_digest.push_str("-changed"),
        "sourceCapabilityRevision" => fingerprint.source_capability_revision.push_str("-changed"),
        "connectionCapabilityRevision" => fingerprint
            .connection_capability_revision
            .push_str("-changed"),
        "statePublication" => fingerprint.state_publication.push_str("-changed"),
        "contributorEvidenceRevisions" => fingerprint
            .contributor_evidence_revisions
            .push("state-evidence-r2".to_owned()),
        "catalogPublication" => fingerprint.catalog_publication.push_str("-changed"),
        "coordinatePublication" => fingerprint.coordinate_publication.push_str("-changed"),
        "lookupProcess" => fingerprint.lookup_process.push_str("-changed"),
        "lookupRevision" => fingerprint.lookup_revision.push_str("-changed"),
        "lookupObservedAt" => fingerprint.lookup_observed_at += Duration::SECOND,
        "lookupExpiry" => fingerprint.lookup_expiry += Duration::SECOND,
        "targetedMembership" => fingerprint.targeted_membership = SyncPreviewMembershipPin::Present,
        "targetedStateRevision" => fingerprint.targeted_state_revision.push_str("-changed"),
        "targetedPrecondition" => fingerprint.targeted_precondition.push_str("-changed"),
        "targetedExpiry" => fingerprint.targeted_expiry += Duration::SECOND,
        "previewTime" => fingerprint.preview_time += Duration::SECOND,
        "previewExpiry" => fingerprint.preview_expiry += Duration::SECOND,
        other => panic!("unsupported fingerprint mutation {other}"),
    }
}

fn policy_snapshot(
    workspace_id: Uuid,
    policy_id: Uuid,
    source_ids: &std::collections::BTreeMap<&str, Uuid>,
    allow_create_membership: bool,
    require_strong_precondition: bool,
) -> SyncPolicyRevisionSnapshot {
    let document = SyncPolicyDocument::from_json(
        &serde_json::json!({
            "contributors": [
                source_ids["source-local-contributor-b"],
                source_ids["source-local-contributor-a"]
            ],
            "receivers": [
                source_ids["source-local-coarse"],
                source_ids["source-local-receiver"]
            ],
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
                "translation": {"allowLossy": true, "quantizer": "nearest"},
                "writeConstraints": {
                    "allowClear": false,
                    "allowCreateMembership": allow_create_membership,
                    "requireStrongPrecondition": require_strong_precondition
                }
            }],
            "schema": "trakkin.sync-policy/v1"
        })
        .to_string(),
        StateRegistry::v1(),
    )
    .expect("valid policy");
    SyncPolicyRevisionSnapshot {
        policy_id,
        workspace_id,
        revision: 1,
        content_hash: document.content_hash(),
        document,
        created_at: datetime!(2030-01-02 3:04:05 UTC),
    }
}

fn preview_command(
    input: AggregationInput,
    policy: SyncPolicyRevisionSnapshot,
    source_ids: &std::collections::BTreeMap<&str, Uuid>,
) -> ConstructSyncPreview {
    let contributors = input
        .contributors
        .into_iter()
        .map(|contributor| PublishedSyncContributor {
            evidence_id: contributor.evidence_id.clone(),
            source_id: source_ids[contributor.source.as_str()],
            subject: provider_subject(&contributor.subject),
            semantic_field: input.field.clone(),
            root: contributor.root.parse().expect("portable contributor root"),
            presence: match contributor.presence.as_str() {
                "present" => PublishedStatePresence::Present,
                other => panic!("unsupported vector presence {other}"),
            },
            coverage: contributor
                .coverage
                .iter()
                .map(|coverage| parse_span(coverage))
                .collect(),
            state_publication_id: stable_uuid(&contributor.publication),
            provider_revision: contributor.evidence_id.into_bytes(),
            mapping_proof_key: String::new(),
        })
        .collect::<Vec<_>>();
    let receivers = input.receivers;
    let partitions = input
        .mapping
        .into_iter()
        .map(|mapping| {
            let segment = mapping
                .endpoints
                .first()
                .and_then(|endpoint| endpoint.split_once('['))
                .and_then(|(_, selector)| selector.strip_suffix(']'))
                .map(parse_span)
                .expect("mapping partition span");
            let proof = resolution_proof(&mapping.proof_id);
            SyncPreviewPartitionInput {
                semantic_field: input.field.clone(),
                segment,
                mapping_proofs: vec![proof],
                contributors: contributors
                    .iter()
                    .cloned()
                    .map(|mut contributor| {
                        contributor.mapping_proof_key = mapping.proof_id.clone();
                        contributor
                    })
                    .collect(),
                receivers: receivers
                    .iter()
                    .map(|receiver| receiver_preflight(receiver, source_ids, &mapping.proof_id))
                    .collect(),
            }
        })
        .collect();
    ConstructSyncPreview { policy, partitions }
}

fn receiver_preflight(
    receiver: &VectorReceiver,
    source_ids: &std::collections::BTreeMap<&str, Uuid>,
    partition: &str,
) -> TargetedReceiverPreflight {
    let source_id = source_ids[receiver.source.as_str()];
    let membership = if receiver.source == "source-local-receiver" {
        AdapterSourceMembership::Absent
    } else {
        AdapterSourceMembership::Present
    };
    let field = progress_field();
    TargetedReceiverPreflight {
        source_id,
        subject: provider_subject(&format!("{}-{partition}", receiver.source)),
        state_read_capability: AdapterTargetedStateReadCapability {
            maximum_fields: 1,
            maximum_response_bytes: 4096,
        },
        progress_capability: ReceiverProgressCapability {
            unit: match receiver.unit.as_str() {
                "episode" => StateUnit::Episode,
                other => panic!("unsupported receiver unit {other}"),
            },
            minimum: receiver.range[0],
            maximum: receiver.range[1],
            step: receiver.step,
            quantizer: parse_quantizer(&receiver.quantizer),
        },
        outcome: AdapterTargetedStateReadOutcome::Matched(AdapterTargetedStateMatch {
            membership,
            fields: vec![AdapterTargetedStateField {
                field,
                presence: if membership == AdapterSourceMembership::Absent {
                    AdapterStatePresence::Absent
                } else {
                    AdapterStatePresence::Present
                },
                value: (membership == AdapterSourceMembership::Present)
                    .then_some(StateValue::Integer(0)),
            }],
            provider_revision: format!("receiver-{partition}-r1").into_bytes(),
            observed_at: datetime!(2030-01-02 3:04:05 UTC),
            expires_at: Some(datetime!(2030-01-02 3:05:05 UTC)),
            precondition: Some(format!("receiver-{partition}-precondition-r1").into_bytes()),
            write_causation: None,
        }),
    }
}

fn resolution_proof(key: &str) -> ResolutionProof {
    ResolutionProof {
        key: key.to_owned(),
        kind: ResolutionProofKind::Direct,
        revision_hash: "sha256:mapping-nm".to_owned(),
        row_hash: format!("sha256:{key}"),
        endpoint_hash: format!("sha256:{key}-endpoint"),
        parent_proofs: Vec::new(),
        transform: ExactTransform {
            numerator: 1,
            denominator: 1,
        },
        catalog_publications: vec![Uuid::from_u128(30)],
        coordinate_publications: vec![Uuid::from_u128(31)],
    }
}

fn progress_field() -> StateField {
    StateField::new(
        Term::new("trakkin.state", "progress").expect("progress term"),
        Some(Term::new("trakkin.unit", "episode").expect("episode unit")),
    )
}

fn provider_subject(value: &str) -> StateSubject {
    StateSubject::ProviderItem(
        ProviderItemKey::new("fixture.subject", value.as_bytes().to_vec())
            .expect("provider subject"),
    )
}

fn parse_span(value: &str) -> OrdinalSpan {
    let (coordinate, range) = value.split_once(':').expect("coordinate range");
    let (start, end) = range.split_once("..").expect("finite range");
    OrdinalSpan::new(
        coordinate,
        start.parse().expect("range start"),
        end.parse().expect("range end"),
    )
    .expect("valid ordinal span")
}

fn parse_rational(value: &str) -> ExactRational {
    let (numerator, denominator) = value.split_once('/').expect("rational");
    ExactRational::new(
        numerator.parse().expect("numerator"),
        denominator.parse().expect("denominator"),
    )
    .expect("valid rational")
}

fn parse_quantizer(value: &str) -> Quantizer {
    match value {
        "exact" => Quantizer::Exact,
        "nearest" => Quantizer::Nearest,
        other => panic!("unsupported receiver quantizer {other}"),
    }
}

fn subject_order_key(subject: &StateSubject) -> (u8, String, Vec<u8>) {
    match subject {
        StateSubject::ProviderItem(key) => (0, key.namespace().to_owned(), key.value().to_vec()),
        StateSubject::CatalogRelation(key) => (1, key.namespace().to_owned(), key.value().to_vec()),
    }
}

fn stable_uuid(value: &str) -> Uuid {
    let digest = Sha256::digest(value.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}
