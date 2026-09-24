use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde_json::json;
use tempfile::tempdir;
use time::{Duration as TimeDuration, OffsetDateTime, macros::datetime};
use tokio::sync::{Notify, Semaphore};
use trakkin::{
    application::{
        adapter_runtime::{
            AdapterCallError, AdapterIdentity, AdapterRetirement, AdapterSourceMembership,
            AdapterStatePresence, AdapterTargetedStateField, AdapterTargetedStateFieldEffect,
            AdapterTargetedStateFieldEffectKind, AdapterTargetedStateMatch,
            AdapterTargetedStateMembershipEffect, AdapterTargetedStateReadCapability,
            AdapterTargetedStateReadOutcome, AdapterTargetedStateWriteCausation,
            AdapterTargetedStateWriteCertainty, AdapterTargetedStateWriteOutcome,
            AdapterTargetedStateWriteRetryDisposition, AdapterTargetedStateWriteStatus,
            ReadAdapterTargetedState, WriteAdapterTargetedState,
        },
        connection::ConnectionSessionRegistry,
        error::ApplicationError,
        mapping_resolution::{ExactTransform, ResolutionProof, ResolutionProofKind},
        sync_action_attempt::{
            PrepareSyncActionAttempt, RecordSyncActionDispatch, RecordSyncActionOutcome,
            SyncActionAttemptError, SyncActionAttemptService, SyncActionOutcomeDisposition,
            sync_action_task_completion,
        },
        sync_action_reconciliation::{
            PrepareSyncActionReconciliation, SyncActionReconciliationResult,
            SyncActionReconciliationService,
        },
        sync_activity::{
            QuerySyncActivity, SyncActivityActionStatus, SyncActivityReconciliationGuidance,
            SyncActivityRunStatus, SyncActivityService,
        },
        sync_policy::{CreateSyncPolicy, QuerySyncPolicies, SyncPolicyService, UpdateSyncPolicy},
        sync_preview::{
            CheckSyncPreviewFreshness, ConstructSyncPreview, CreateSyncPreviewSnapshot,
            PreviewMembershipEffect, PublishedStatePresence, PublishedSyncContributor,
            ReceiverProgressCapability, SyncPreviewBuilder, SyncPreviewFingerprint,
            SyncPreviewFingerprintError, SyncPreviewFreshness, SyncPreviewLifecycleError,
            SyncPreviewMembershipPin, SyncPreviewPartitionInput, SyncPreviewRepository,
            SyncPreviewService, SyncPreviewSnapshot, SyncPreviewSnapshotError,
            TargetedReceiverPreflight, sync_preview_policy_revision_pin,
        },
        sync_provenance::{SyncActionProvenanceError, SyncActionProvenanceService},
        sync_retention::{
            RetainSyncEvidence, SyncRetentionClass, SyncRetentionPolicy, SyncRetentionService,
        },
        sync_run::{AdmitSyncPreview, SyncActionAdmission, SyncRunAdmissionError, SyncRunService},
        sync_schedule::{
            EnableSyncSchedule, SyncScheduleRecurrence, SyncScheduleRunner, SyncScheduleService,
            SyncScheduleTickResult, TickSyncSchedules,
        },
        sync_workflow::{
            CollectSyncPreviewInputs, CollectedSyncPreviewInputs, CreateWorkspaceSyncPreview,
            StartWorkspaceSyncRun, SyncPreviewCollectionFuture, SyncPreviewEntryCategory,
            SyncPreviewInputCollector, SyncPreviewMutationApi, SyncRunWorkflowService,
            SyncWorkflowService,
        },
        task_dispatcher::{DispatchNextTask, TaskDispatcher, TaskHandler, TaskHandlerOutcome},
        task_manager::{
            AdmitTask, CancellationDisposition, ClaimNextTask, RequestTaskCancellation,
            TaskDefinition, TaskKind, TaskManager, TaskStatus,
        },
    },
    database::DatabaseRuntime,
    domain::{
        acquisition::{ProviderItemKey, StateField, StateSubject, StateValue, Term},
        synchronization::{OrdinalSpan, Quantizer, StateRegistry, StateUnit, SyncPolicyDocument},
    },
    infrastructure::{
        sync_action_attempt::PostgresSyncActionAttemptRepository,
        sync_action_dispatch::{SyncActionAdapterSession, SyncActionTaskHandler},
        sync_action_reconciliation::PostgresSyncActionReconciliationRepository,
        sync_action_reconciliation_dispatch::{
            SyncActionReconciliationAdapterSession, SyncActionReconciliationTaskHandler,
        },
        sync_activity::PostgresSyncActivityRepository,
        sync_policy::PostgresSyncPolicyRepository,
        sync_preview::{PostgresSyncPreviewRepository, PostgresSyncPreviewRepositoryError},
        sync_provenance::PostgresSyncActionProvenanceRepository,
        sync_retention::PostgresSyncRetentionRepository,
        sync_run::PostgresSyncRunRepository,
        sync_schedule::PostgresSyncScheduleRepository,
        task_manager::PostgresTaskRepository,
    },
};
use trakkin_mapping::PortableEndpoint;
use uuid::Uuid;

const WORKSPACE_ID: Uuid = Uuid::from_u128(1);
const OTHER_WORKSPACE_ID: Uuid = Uuid::from_u128(2);
const CONNECTION_ID: Uuid = Uuid::from_u128(3);
const ACCOUNT_ID: Uuid = Uuid::from_u128(4);
const SECOND_CONNECTION_ID: Uuid = Uuid::from_u128(6);
const SECOND_ACCOUNT_ID: Uuid = Uuid::from_u128(7);
const POLICY_ID: Uuid = Uuid::from_u128(5);
const CONTRIBUTOR_ID: Uuid = Uuid::from_u128(10);
const RECEIVER_A_ID: Uuid = Uuid::from_u128(11);
const RECEIVER_B_ID: Uuid = Uuid::from_u128(12);
const RECEIVER_C_ID: Uuid = Uuid::from_u128(13);
const PREVIEW_ID: Uuid = Uuid::from_u128(20);
const RUN_ID: Uuid = Uuid::from_u128(31);
const ACTION_ID: Uuid = Uuid::from_u128(32);
const TASK_ID: Uuid = Uuid::from_u128(33);
const SECOND_ACTION_ID: Uuid = Uuid::from_u128(34);
const SECOND_TASK_ID: Uuid = Uuid::from_u128(35);
const THIRD_ACTION_ID: Uuid = Uuid::from_u128(36);
const THIRD_TASK_ID: Uuid = Uuid::from_u128(37);
const CREATED_AT: OffsetDateTime = datetime!(2030-01-02 3:04:05 UTC);
const EXPIRES_AT: OffsetDateTime = datetime!(2030-01-02 3:09:05 UTC);

#[derive(Clone)]
struct StaticSyncPreviewInputCollector {
    collected: CollectedSyncPreviewInputs,
    commands: Arc<Mutex<Vec<CollectSyncPreviewInputs>>>,
}

impl SyncPreviewInputCollector for StaticSyncPreviewInputCollector {
    type Error = ApplicationError;

    fn collect(
        &self,
        command: CollectSyncPreviewInputs,
    ) -> SyncPreviewCollectionFuture<'_, Self::Error> {
        self.commands
            .lock()
            .expect("collector command lock")
            .push(command);
        let collected = self.collected.clone();
        Box::pin(async move { Ok(collected) })
    }
}

async fn admit_receiver_actions(
    pool: &sqlx::PgPool,
    assignments: &[(Uuid, Uuid, Uuid)],
) -> trakkin::application::sync_preview::SyncPreviewDraft {
    admit_receiver_actions_with_connection_layout(
        pool,
        assignments,
        ReceiverConnectionLayout::Shared,
    )
    .await
}

async fn admit_receiver_actions_on_distinct_connections(
    pool: &sqlx::PgPool,
    assignments: &[(Uuid, Uuid, Uuid)],
) -> trakkin::application::sync_preview::SyncPreviewDraft {
    admit_receiver_actions_with_connection_layout(
        pool,
        assignments,
        ReceiverConnectionLayout::ReceiverBSeparate,
    )
    .await
}

async fn admit_concurrent_receiver_actions(
    pool: &sqlx::PgPool,
    assignments: &[(Uuid, Uuid, Uuid)],
) -> trakkin::application::sync_preview::SyncPreviewDraft {
    admit_receiver_actions_with_connection_layout(
        pool,
        assignments,
        ReceiverConnectionLayout::ReceiverCSeparate,
    )
    .await
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReceiverConnectionLayout {
    Shared,
    ReceiverBSeparate,
    ReceiverCSeparate,
}

async fn admit_receiver_actions_with_connection_layout(
    pool: &sqlx::PgPool,
    assignments: &[(Uuid, Uuid, Uuid)],
    connection_layout: ReceiverConnectionLayout,
) -> trakkin::application::sync_preview::SyncPreviewDraft {
    seed_sources(pool).await;
    if connection_layout != ReceiverConnectionLayout::Shared {
        seed_secondary_receiver_connection(
            pool,
            match connection_layout {
                ReceiverConnectionLayout::ReceiverBSeparate => RECEIVER_B_ID,
                ReceiverConnectionLayout::ReceiverCSeparate => RECEIVER_C_ID,
                ReceiverConnectionLayout::Shared => unreachable!(),
            },
        )
        .await;
    }
    let concurrency_case = connection_layout == ReceiverConnectionLayout::ReceiverCSeparate;
    let policy = SyncPolicyService::new(PostgresSyncPolicyRepository::new(pool.clone()))
        .create(CreateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            document: if concurrency_case {
                concurrency_policy_document()
            } else {
                policy_document()
            },
            created_at: CREATED_AT,
        })
        .await
        .expect("policy revision is stored");
    let draft = SyncPreviewBuilder::new(StateRegistry::v1())
        .construct(if concurrency_case {
            concurrency_preview_command(policy)
        } else {
            preview_command(policy)
        })
        .expect("preview draft constructs");
    let fingerprint = preview_fingerprint(&draft);
    let admitted_draft = draft.clone();
    let preview_service = SyncPreviewService::new(PostgresSyncPreviewRepository::new(pool.clone()));
    let stored = preview_service
        .create(CreateSyncPreviewSnapshot {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            draft,
            fingerprint: fingerprint.clone(),
            created_at: CREATED_AT,
            expires_at: EXPIRES_AT,
        })
        .await
        .expect("preview is stored");
    let selected = assignments
        .iter()
        .map(|(receiver_source_id, action_id, task_id)| {
            let action = stored
                .actions
                .iter()
                .find(|action| action.receiver_source_id == *receiver_source_id)
                .expect("assigned receiver action exists");
            SyncActionAdmission {
                action_hash: action.action_hash.clone(),
                action_id: *action_id,
                task_id: *task_id,
            }
        })
        .collect::<Vec<_>>();
    SyncRunService::new(PostgresSyncRunRepository::new(pool.clone()))
        .admit(AdmitSyncPreview {
            run_id: RUN_ID,
            schedule_occurrence_id: None,
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            preview_hash: stored.content_hash,
            current_fingerprint: fingerprint,
            actions: selected,
            admitted_at: CREATED_AT,
        })
        .await
        .expect("assigned receiver actions are admitted");
    admitted_draft
}
fn applied_outcome() -> AdapterTargetedStateWriteOutcome {
    AdapterTargetedStateWriteOutcome {
        status: AdapterTargetedStateWriteStatus::Applied,
        certainty: AdapterTargetedStateWriteCertainty::ConfirmedApplied,
        retry_disposition: AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        membership_effect: AdapterTargetedStateMembershipEffect::Created,
        field_effects: vec![AdapterTargetedStateFieldEffect {
            field: progress_field(),
            effect: AdapterTargetedStateFieldEffectKind::Set,
            value: Some(StateValue::Integer(6)),
        }],
        provider_revision: b"receiver-proof-b-r2".to_vec(),
        successor_precondition: Some(b"receiver-proof-b-precondition-r2".to_vec()),
        receipt: b"created-receiver-membership".to_vec(),
        error: None,
    }
}

fn failed_outcome(
    retry_disposition: AdapterTargetedStateWriteRetryDisposition,
    retryable: bool,
) -> AdapterTargetedStateWriteOutcome {
    AdapterTargetedStateWriteOutcome {
        status: AdapterTargetedStateWriteStatus::Failed,
        certainty: AdapterTargetedStateWriteCertainty::ConfirmedNotApplied,
        retry_disposition,
        membership_effect: AdapterTargetedStateMembershipEffect::Unchanged,
        field_effects: Vec::new(),
        provider_revision: Vec::new(),
        successor_precondition: None,
        receipt: Vec::new(),
        error: Some(AdapterCallError {
            category: trakkin::application::adapter_runtime::AdapterFailureCategory::Unavailable,
            code: "temporary_provider_failure".to_owned(),
            safe_message: "provider temporarily unavailable".to_owned(),
            retryable,
            retry_after: None,
            field_problems: Vec::new(),
        }),
    }
}

#[derive(Clone)]
struct RecordingSessionRegistry {
    session: Arc<Mutex<Option<RecordingSyncSession>>>,
}

struct MultiConnectionSessionRegistry<S> {
    sessions: Arc<Mutex<HashMap<Uuid, S>>>,
}

impl<S> Clone for MultiConnectionSessionRegistry<S> {
    fn clone(&self) -> Self {
        Self {
            sessions: Arc::clone(&self.sessions),
        }
    }
}

impl<S> MultiConnectionSessionRegistry<S> {
    fn new<const COUNT: usize>(sessions: [(Uuid, S); COUNT]) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::from(sessions))),
        }
    }
}

impl<S> ConnectionSessionRegistry<S> for MultiConnectionSessionRegistry<S>
where
    S: Send,
{
    fn replace(&self, connection_id: Uuid, session: S) -> Option<S> {
        self.sessions
            .lock()
            .expect("multi-connection sessions lock")
            .insert(connection_id, session)
    }

    fn remove(&self, connection_id: Uuid) -> Option<S> {
        self.sessions
            .lock()
            .expect("multi-connection sessions lock")
            .remove(&connection_id)
    }

    fn identity(&self, _connection_id: Uuid) -> Option<AdapterIdentity> {
        None
    }
}

impl RecordingSessionRegistry {
    fn new(session: RecordingSyncSession) -> Self {
        Self {
            session: Arc::new(Mutex::new(Some(session))),
        }
    }
}

impl ConnectionSessionRegistry<RecordingSyncSession> for RecordingSessionRegistry {
    fn replace(
        &self,
        _connection_id: Uuid,
        session: RecordingSyncSession,
    ) -> Option<RecordingSyncSession> {
        self.session
            .lock()
            .expect("recording session lock")
            .replace(session)
    }

    fn remove(&self, _connection_id: Uuid) -> Option<RecordingSyncSession> {
        self.session.lock().expect("recording session lock").take()
    }

    fn identity(&self, _connection_id: Uuid) -> Option<AdapterIdentity> {
        None
    }
}

struct RecordingSyncSession {
    writes: Arc<Mutex<Vec<WriteAdapterTargetedState>>>,
    provider_mutations: Arc<Mutex<u64>>,
    cancelled_operations: Arc<Mutex<Vec<Vec<u8>>>>,
    outcomes: VecDeque<Option<AdapterTargetedStateWriteOutcome>>,
}

#[derive(Clone)]
struct ReconciliationSessionRegistry {
    session: Arc<Mutex<Option<ReconciliationRecordingSession>>>,
    collide_on_next_replace: Arc<Mutex<bool>>,
}

impl ReconciliationSessionRegistry {
    fn new(session: ReconciliationRecordingSession) -> Self {
        Self {
            session: Arc::new(Mutex::new(Some(session))),
            collide_on_next_replace: Arc::new(Mutex::new(false)),
        }
    }

    fn with_restore_collision(session: ReconciliationRecordingSession) -> Self {
        Self {
            session: Arc::new(Mutex::new(Some(session))),
            collide_on_next_replace: Arc::new(Mutex::new(true)),
        }
    }
}

impl ConnectionSessionRegistry<ReconciliationRecordingSession> for ReconciliationSessionRegistry {
    fn replace(
        &self,
        _connection_id: Uuid,
        session: ReconciliationRecordingSession,
    ) -> Option<ReconciliationRecordingSession> {
        if std::mem::take(
            &mut *self
                .collide_on_next_replace
                .lock()
                .expect("reconciliation collision lock"),
        ) {
            let unexpected = session.clone();
            self.session
                .lock()
                .expect("reconciliation session lock")
                .replace(session);
            return Some(unexpected);
        }
        self.session
            .lock()
            .expect("reconciliation session lock")
            .replace(session)
    }

    fn remove(&self, _connection_id: Uuid) -> Option<ReconciliationRecordingSession> {
        self.session
            .lock()
            .expect("reconciliation session lock")
            .take()
    }

    fn identity(&self, _connection_id: Uuid) -> Option<AdapterIdentity> {
        None
    }
}

#[derive(Clone)]
struct ReconciliationRecordingSession {
    writes: Arc<Mutex<Vec<WriteAdapterTargetedState>>>,
    reads: Arc<Mutex<Vec<ReadAdapterTargetedState>>>,
    write_outcomes: VecDeque<AdapterTargetedStateWriteOutcome>,
    read_outcomes: VecDeque<Result<AdapterTargetedStateReadOutcome, ()>>,
}

impl SyncActionAdapterSession for ReconciliationRecordingSession {
    type Error = ();

    async fn write_sync_action(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> Result<AdapterTargetedStateWriteOutcome, Self::Error> {
        self.writes
            .lock()
            .expect("recorded writes lock")
            .push(command);
        Ok(self
            .write_outcomes
            .pop_front()
            .expect("reconciliation write outcome is configured"))
    }

    async fn cancel_sync_action(
        &mut self,
        _operation_id: Vec<u8>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn retire_sync_action(self) -> Result<AdapterRetirement, Self::Error> {
        Ok(AdapterRetirement::Graceful)
    }
}

impl SyncActionReconciliationAdapterSession for ReconciliationRecordingSession {
    type Error = ();

    async fn read_sync_action_reconciliation(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> Result<AdapterTargetedStateReadOutcome, Self::Error> {
        self.reads
            .lock()
            .expect("recorded reads lock")
            .push(command);
        self
            .read_outcomes
            .pop_front()
            .expect("reconciliation read outcome is configured")
    }

    async fn retire_sync_action_reconciliation(
        self,
    ) -> Result<AdapterRetirement, Self::Error> {
        Ok(AdapterRetirement::Graceful)
    }
}

fn matched_reconciliation(
    intended_state: bool,
    with_causation: bool,
) -> AdapterTargetedStateReadOutcome {
    let provider_revision = if intended_state {
        b"receiver-proof-b-r2".to_vec()
    } else {
        b"receiver-proof-b-r1".to_vec()
    };
    AdapterTargetedStateReadOutcome::Matched(AdapterTargetedStateMatch {
        membership: if intended_state {
            AdapterSourceMembership::Present
        } else {
            AdapterSourceMembership::Absent
        },
        fields: vec![AdapterTargetedStateField {
            field: progress_field(),
            presence: if intended_state {
                AdapterStatePresence::Present
            } else {
                AdapterStatePresence::Absent
            },
            value: intended_state.then_some(StateValue::Integer(6)),
        }],
        provider_revision: provider_revision.clone(),
        observed_at: CREATED_AT + time::Duration::seconds(1),
        expires_at: Some(EXPIRES_AT),
        precondition: Some(b"reconciliation-precondition".to_vec()),
        write_causation: with_causation.then_some(AdapterTargetedStateWriteCausation {
            idempotency_key: ACTION_ID.as_bytes().to_vec(),
            receipt: b"reconciliation-receipt".to_vec(),
            provider_revision,
        }),
    })
}

impl SyncActionAdapterSession for RecordingSyncSession {
    type Error = std::convert::Infallible;

    async fn write_sync_action(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> Result<AdapterTargetedStateWriteOutcome, Self::Error> {
        let writes = Arc::clone(&self.writes);
        let provider_mutations = Arc::clone(&self.provider_mutations);
        let outcome = self
            .outcomes
            .pop_front()
            .expect("recording sync outcome is configured");
        writes.lock().expect("recorded writes lock").push(command);
        match outcome {
            Some(outcome) => {
                if outcome.status == AdapterTargetedStateWriteStatus::Applied {
                    *provider_mutations.lock().expect("provider mutations lock") += 1;
                }
                Ok(outcome)
            }
            None => std::future::pending().await,
        }
    }

    async fn cancel_sync_action(
        &mut self,
        operation_id: Vec<u8>,
    ) -> Result<(), Self::Error> {
        self.cancelled_operations
            .lock()
            .expect("cancelled operations lock")
            .push(operation_id);
        Ok(())
    }

    async fn retire_sync_action(self) -> Result<AdapterRetirement, Self::Error> {
        Ok(AdapterRetirement::Graceful)
    }
}
fn admission_command(
    preview_hash: &str,
    action_hash: &str,
    current_fingerprint: SyncPreviewFingerprint,
    admitted_at: OffsetDateTime,
) -> AdmitSyncPreview {
    AdmitSyncPreview {
        run_id: RUN_ID,
        schedule_occurrence_id: None,
        preview_id: PREVIEW_ID,
        workspace_id: WORKSPACE_ID,
        preview_hash: preview_hash.to_owned(),
        current_fingerprint,
        actions: vec![SyncActionAdmission {
            action_hash: action_hash.to_owned(),
            action_id: ACTION_ID,
            task_id: TASK_ID,
        }],
        admitted_at,
    }
}

fn preview_fingerprint(
    draft: &trakkin::application::sync_preview::SyncPreviewDraft,
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
        lookup_observed_at: CREATED_AT,
        lookup_expiry: CREATED_AT + time::Duration::minutes(1),
        targeted_membership: SyncPreviewMembershipPin::Absent,
        targeted_state_revision: "sha256:targeted-state".to_owned(),
        targeted_precondition: "sha256:targeted-preconditions".to_owned(),
        targeted_expiry: CREATED_AT + time::Duration::minutes(1),
        preview_time: CREATED_AT,
        preview_expiry: EXPIRES_AT,
    }
}

fn policy_document() -> SyncPolicyDocument {
    SyncPolicyDocument::from_json(
        &json!({
            "contributors": [CONTRIBUTOR_ID],
            "receivers": [RECEIVER_B_ID, RECEIVER_A_ID],
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
    .expect("preview policy is valid")
}

fn concurrency_policy_document() -> SyncPolicyDocument {
    SyncPolicyDocument::from_json(
        &json!({
            "contributors": [CONTRIBUTOR_ID],
            "receivers": [RECEIVER_A_ID, RECEIVER_B_ID, RECEIVER_C_ID],
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
    .expect("concurrency preview policy is valid")
}

fn preview_command(
    policy: trakkin::application::sync_policy::SyncPolicyRevisionSnapshot,
) -> ConstructSyncPreview {
    preview_command_with_partitions(
        policy,
        &[
            ("proof-a", 1, 6, RECEIVER_A_ID),
            ("proof-b", 7, 12, RECEIVER_B_ID),
        ],
    )
}

fn concurrency_preview_command(
    policy: trakkin::application::sync_policy::SyncPolicyRevisionSnapshot,
) -> ConstructSyncPreview {
    preview_command_with_partitions(
        policy,
        &[
            ("proof-a", 1, 4, RECEIVER_A_ID),
            ("proof-b", 5, 8, RECEIVER_B_ID),
            ("proof-c", 9, 12, RECEIVER_C_ID),
        ],
    )
}

fn preview_command_with_partitions(
    policy: trakkin::application::sync_policy::SyncPolicyRevisionSnapshot,
    partitions: &[(&str, u64, u64, Uuid)],
) -> ConstructSyncPreview {
    ConstructSyncPreview {
        policy,
        partitions: partitions
            .iter()
            .copied()
            .map(
                |(proof_key, start, end, receiver_id)| SyncPreviewPartitionInput {
                    semantic_field: "trakkin.state:progress".to_owned(),
                    segment: OrdinalSpan::new("episode", start, end).expect("valid segment"),
                    mapping_proofs: vec![mapping_proof(proof_key)],
                    contributors: vec![PublishedSyncContributor {
                        evidence_id: format!("evidence-{proof_key}"),
                        source_id: CONTRIBUTOR_ID,
                        subject: provider_subject(&format!("contributor-{proof_key}")),
                        semantic_field: "trakkin.state:progress".to_owned(),
                        root: PortableEndpoint::parse(&format!(
                            "example.media://original[episode:{start}..{end}]"
                        ))
                        .expect("portable contributor root"),
                        presence: PublishedStatePresence::Present,
                        coverage: vec![
                            OrdinalSpan::new("episode", start, end).expect("valid coverage"),
                        ],
                        state_publication_id: Uuid::from_u128(40 + u128::from(start)),
                        provider_revision: format!("state-{proof_key}-r1").into_bytes(),
                        mapping_proof_key: proof_key.to_owned(),
                    }],
                    receivers: vec![receiver_preflight(receiver_id, proof_key)],
                },
            )
            .collect(),
    }
}

fn receiver_preflight(source_id: Uuid, proof_key: &str) -> TargetedReceiverPreflight {
    let creates_membership = source_id == RECEIVER_B_ID;
    TargetedReceiverPreflight {
        source_id,
        subject: provider_subject(&format!("receiver-{proof_key}")),
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
            membership: if creates_membership {
                AdapterSourceMembership::Absent
            } else {
                AdapterSourceMembership::Present
            },
            fields: vec![AdapterTargetedStateField {
                field: progress_field(),
                presence: if creates_membership {
                    AdapterStatePresence::Absent
                } else {
                    AdapterStatePresence::Present
                },
                value: (!creates_membership).then_some(StateValue::Integer(0)),
            }],
            provider_revision: format!("receiver-{proof_key}-r1").into_bytes(),
            observed_at: CREATED_AT,
            expires_at: Some(EXPIRES_AT),
            precondition: Some(format!("receiver-{proof_key}-precondition-r1").into_bytes()),
            write_causation: None,
        }),
    }
}

fn mapping_proof(key: &str) -> ResolutionProof {
    ResolutionProof {
        key: key.to_owned(),
        kind: ResolutionProofKind::Direct,
        revision_hash: "sha256:mapping-preview".to_owned(),
        row_hash: format!("sha256:{key}"),
        endpoint_hash: format!("sha256:{key}-endpoint"),
        parent_proofs: Vec::new(),
        transform: ExactTransform {
            numerator: 1,
            denominator: 1,
        },
        catalog_publications: vec![Uuid::from_u128(50)],
        coordinate_publications: vec![Uuid::from_u128(51)],
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

async fn table_count(pool: &sqlx::PgPool, table: &str) -> i64 {
    match table {
        "tasks" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM tasks")
                .fetch_one(pool)
                .await
        }
        "sync_previews" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_previews")
                .fetch_one(pool)
                .await
        }
        "sync_preview_actions" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_preview_actions")
                .fetch_one(pool)
                .await
        }
        "sync_runs" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_runs")
                .fetch_one(pool)
                .await
        }
        "sync_actions" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_actions")
                .fetch_one(pool)
                .await
        }
        "sync_action_tasks" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_action_tasks")
                .fetch_one(pool)
                .await
        }
        "sync_action_attempts" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_action_attempts")
                .fetch_one(pool)
                .await
        }
        "sync_action_provenance" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_action_provenance")
                .fetch_one(pool)
                .await
        }
        "sync_action_reconciliation_tasks" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_action_reconciliation_tasks")
                .fetch_one(pool)
                .await
        }
        "sync_action_reconciliation_attempts" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM sync_action_reconciliation_attempts")
                .fetch_one(pool)
                .await
        }
        "task_attempts" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM task_attempts")
                .fetch_one(pool)
                .await
        }
        "task_claims" => {
            sqlx::query_scalar("SELECT COUNT(*) FROM task_claims")
                .fetch_one(pool)
                .await
        }
        other => panic!("unsupported table count {other}"),
    }
    .expect("table count is queryable")
}

async fn seed_sources(pool: &sqlx::PgPool) {
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES
    ($1, 'Previews', 'active', $3, $3),
    ($2, 'Other previews', 'active', $3, $3)
",
    )
    .bind(WORKSPACE_ID)
    .bind(OTHER_WORKSPACE_ID)
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("workspaces are seeded");
    sqlx::query(
        "INSERT INTO connections (
    id, provider_id, display_name, status, capabilities,
    capability_revision, created_at, updated_at
) VALUES ($1, 'fixture', 'Fixture', 'ready', '{}'::JSONB, 1, $2, $2)
",
    )
    .bind(CONNECTION_ID)
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("connection is seeded");
    sqlx::query(
        "INSERT INTO accounts (
    id, connection_id, provider_key_namespace, provider_key_value,
    display_name, first_seen_at, last_seen_at
) VALUES ($1, $2, 'fixture.account', $3, 'Fixture account', $4, $4)
",
    )
    .bind(ACCOUNT_ID)
    .bind(CONNECTION_ID)
    .bind(b"account".as_slice())
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("account is seeded");
    for (source_id, key) in [
        (CONTRIBUTOR_ID, b"contributor".as_slice()),
        (RECEIVER_A_ID, b"receiver-a".as_slice()),
        (RECEIVER_B_ID, b"receiver-b".as_slice()),
        (RECEIVER_C_ID, b"receiver-c".as_slice()),
    ] {
        let capabilities = json!({
            "targetedStateRead": {
                "maximumFields": 1,
                "maximumResponseBytes": 4096
            },
            "targetedStateWrite": {
                "fields": [{
                    "field": {"namespace": "trakkin.state", "name": "progress"},
                    "unit": {"namespace": "trakkin.unit", "name": "episode"},
                    "setSupported": true,
                    "clearSupported": false
                }],
                "mayCreateSourceMembership": true,
                "preconditionMode": "provider_token",
                "idempotencyMode": "stable_key",
                "maximumFields": 1,
                "maximumRequestBytes": 4096,
                "maximumResponseBytes": 4096,
                "maximumReceiptBytes": 64
            }
        });
        sqlx::query(
            "INSERT INTO sources (
    id, connection_id, account_id, provider_key_namespace, provider_key_value,
    display_name, kind_namespace, kind_name, availability, capabilities,
    capability_revision, first_seen_at, last_seen_at
) VALUES (
    $1, $2, $3, 'fixture.source', $4, 'Fixture source',
    'fixture', 'state', 'available', $6, 1, $5, $5
)
",
        )
        .bind(source_id)
        .bind(CONNECTION_ID)
        .bind(ACCOUNT_ID)
        .bind(key)
        .bind(CREATED_AT)
        .bind(capabilities)
        .execute(pool)
        .await
        .expect("source is seeded");
        sqlx::query(
            "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
VALUES ($1, $2, $3)
",
        )
        .bind(WORKSPACE_ID)
        .bind(source_id)
        .bind(CREATED_AT)
        .execute(pool)
        .await
        .expect("source is attached");
    }
}

async fn seed_secondary_receiver_connection(pool: &sqlx::PgPool, receiver_id: Uuid) {
    sqlx::query(
        "INSERT INTO connections (
    id, provider_id, display_name, status, capabilities,
    capability_revision, created_at, updated_at
) VALUES ($1, 'fixture', 'Second fixture', 'ready', '{}'::JSONB, 1, $2, $2)
",
    )
    .bind(SECOND_CONNECTION_ID)
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("second connection is seeded");
    sqlx::query(
        "INSERT INTO accounts (
    id, connection_id, provider_key_namespace, provider_key_value,
    display_name, first_seen_at, last_seen_at
) VALUES ($1, $2, 'fixture.account', $3, 'Second fixture account', $4, $4)
",
    )
    .bind(SECOND_ACCOUNT_ID)
    .bind(SECOND_CONNECTION_ID)
    .bind(b"account-b".as_slice())
    .bind(CREATED_AT)
    .execute(pool)
    .await
    .expect("second account is seeded");
    sqlx::query(
        "UPDATE sources
SET connection_id = $1, account_id = $2
WHERE id = $3
",
    )
    .bind(SECOND_CONNECTION_ID)
    .bind(SECOND_ACCOUNT_ID)
    .bind(receiver_id)
    .execute(pool)
    .await
    .expect("second receiver is moved to its connection");
}
