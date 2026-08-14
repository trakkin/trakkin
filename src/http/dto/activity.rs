use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::application::{
    adapter_runtime::{
        AdapterSourceMembership, AdapterStatePresence, AdapterTargetedStateMembershipEffect,
        AdapterTargetedStateWriteCertainty, AdapterTargetedStateWriteRetryDisposition,
        AdapterTargetedStateWriteStatus,
    },
    error::ApplicationError,
    sync_action_attempt::{
        SyncActionAttemptSnapshot, SyncActionAttemptState, SyncActionRecoveryResult,
    },
    sync_action_reconciliation::SyncActionReconciliationResult,
    sync_activity::{
        SyncActivityActionDetail, SyncActivityActionStatus, SyncActivityActionSummary,
        SyncActivityPage, SyncActivityReconciliationGuidance, SyncActivityRunStatus,
        SyncActivityRunSummary, SyncActivityTaskSummary,
    },
    sync_preview::PublishedStatePresence,
    sync_provenance::{
        SyncActionProvenanceSnapshot, SyncActionReconciliationProvenance,
        SyncReconciliationOutcomeKind,
    },
};

use super::{
    connections::AdapterCallErrorResponse,
    sources::{
        KeyResponse, StateFieldResponse, StateSubjectResponse, key_from_adapter, term_response,
    },
    synchronization::{PreviewMembershipEffectResponse, SyncPolicyReducer},
    tasks::TaskStatusResponse,
    timestamp,
};

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct ActivityQueryParameters {
    pub status: Option<String>,
    pub source_id: Option<uuid::Uuid>,
    pub field: Option<String>,
    pub occurred_at_or_after: Option<String>,
    pub occurred_before: Option<String>,
    #[param(minimum = 1, maximum = 100)]
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityPageResponse {
    pub runs: Vec<ActivityRunResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityRunResponse {
    pub run_id: String,
    pub workspace_id: String,
    pub preview_id: String,
    pub status: ActivityRunStatusResponse,
    pub actions: Vec<ActivityActionResponse>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityRunStatusResponse {
    Queued,
    Running,
    Succeeded,
    Failed,
    Partial,
    Indeterminate,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityActionStatusResponse {
    Queued,
    Running,
    Applied,
    NoOp,
    PreconditionFailed,
    RetryableFailure,
    Failed,
    Indeterminate,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityReconciliationGuidanceResponse {
    NotRequired,
    Required,
    ConfirmedApplied,
    ConfirmedNotApplied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityActionResponse {
    pub action_id: String,
    pub run_id: String,
    pub workspace_id: String,
    pub action_hash: String,
    pub receiver_source_id: String,
    pub semantic_field: String,
    pub status: ActivityActionStatusResponse,
    pub write_task: ActivityTaskResponse,
    pub reconciliation_task: Option<ActivityTaskResponse>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityTaskResponse {
    pub task_id: String,
    pub status: TaskStatusResponse,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityActionDetailResponse {
    pub action: ActivityActionResponse,
    pub provenance: Option<ActivityProvenanceResponse>,
    pub reconciliation_guidance: ActivityReconciliationGuidanceResponse,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityProvenanceResponse {
    pub policy_id: String,
    pub policy_revision: u64,
    pub policy_content_hash: String,
    pub state_registry_digest: String,
    pub preview_id: String,
    pub preview_hash: String,
    pub preview_action_hash: String,
    pub run_id: String,
    pub run_created_at: String,
    pub reducer: SyncPolicyReducer,
    pub contributors: Vec<ActivityContributorResponse>,
    pub mapping_proofs: Vec<ActivityMappingProofResponse>,
    pub translation: ActivityTranslationResponse,
    pub membership_effect: PreviewMembershipEffectResponse,
    pub attempt: ActivityAttemptResponse,
    pub reconciliation: Option<ActivityReconciliationResponse>,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityContributorResponse {
    pub evidence_id: String,
    pub state_publication_id: String,
    pub presence: PublishedStatePresenceResponse,
    pub mapping_proof_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityMappingProofResponse {
    pub key: String,
    pub revision_hash: String,
    pub row_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityTranslationResponse {
    pub unit: String,
    pub quantizer: String,
    pub exact_value_numerator: u64,
    pub exact_value_denominator: u64,
    pub intended_value: u64,
    pub lossy: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityAttemptResponse {
    pub task_id: String,
    pub task_attempt_id: String,
    pub operation_id: String,
    pub idempotency_key: String,
    pub source_key: KeyResponse,
    pub receiver_source_id: String,
    pub receiver_subject: StateSubjectResponse,
    pub semantic_field: String,
    pub provider_field: StateFieldResponse,
    pub intended_value: i64,
    pub expected_membership: SourceMembershipResponse,
    pub allow_create_membership: bool,
    pub state: ActionAttemptStateResponse,
    pub recovery_result: Option<ActionRecoveryResultResponse>,
    pub outcome: Option<ActivityOutcomeResponse>,
    pub prepared_at: String,
    pub dispatched_at: Option<String>,
    pub settled_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityOutcomeResponse {
    pub status: TargetedStateWriteStatusResponse,
    pub certainty: TargetedStateWriteCertaintyResponse,
    pub retry_disposition: TargetedStateWriteRetryDispositionResponse,
    pub membership_effect: TargetedStateMembershipEffectResponse,
    pub provider_revision: Vec<u8>,
    pub successor_precondition: Option<Vec<u8>>,
    pub receipt: Vec<u8>,
    pub error: Option<AdapterCallErrorResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivityReconciliationResponse {
    pub task_id: String,
    pub task_attempt_id: String,
    pub write_task_attempt_id: String,
    pub operation_id: String,
    pub idempotency_key: String,
    pub source_key: KeyResponse,
    pub receiver_subject: StateSubjectResponse,
    pub provider_field: StateFieldResponse,
    pub intended_value: i64,
    pub before_membership: SourceMembershipResponse,
    pub before_presence: StatePresenceResponse,
    pub before_value: Option<i64>,
    pub outcome_kind: ReconciliationOutcomeKindResponse,
    pub observed_membership: Option<SourceMembershipResponse>,
    pub observed_presence: Option<StatePresenceResponse>,
    pub observed_value: Option<i64>,
    pub observed_at: Option<String>,
    pub causation_receipt: Option<Vec<u8>>,
    pub result: ActionReconciliationResultResponse,
    pub prepared_at: String,
    pub settled_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublishedStatePresenceResponse {
    Present,
    Deleted,
    Absent,
    Unknown,
    Stale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceMembershipResponse {
    Present,
    Absent,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum StatePresenceResponse {
    Present,
    Deleted,
    Absent,
    Unknown,
    Stale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionAttemptStateResponse {
    Prepared,
    Dispatched,
    Settled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionRecoveryResultResponse {
    ConfirmedNotApplied,
    Indeterminate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TargetedStateWriteStatusResponse {
    Applied,
    NoOp,
    Rejected,
    Failed,
    Indeterminate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TargetedStateWriteCertaintyResponse {
    ConfirmedApplied,
    ConfirmedNotApplied,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TargetedStateWriteRetryDispositionResponse {
    NotRetryable,
    SafeSameKey,
    ReconcileFirst,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TargetedStateMembershipEffectResponse {
    Unchanged,
    Created,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationOutcomeKindResponse {
    Matched,
    Unsupported,
    NotFound,
    Ambiguous,
    Indeterminate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionReconciliationResultResponse {
    ConfirmedApplied,
    ConfirmedNotApplied,
    StateMatchesCausationUnknown,
    Unresolved,
}

impl ActivityPageResponse {
    pub fn from_page(page: SyncActivityPage) -> Result<Self, ApplicationError> {
        Ok(Self {
            runs: page
                .runs
                .into_iter()
                .map(ActivityRunResponse::from_summary)
                .collect::<Result<_, _>>()?,
            next_cursor: page.next_cursor,
        })
    }
}

impl ActivityRunResponse {
    fn from_summary(summary: SyncActivityRunSummary) -> Result<Self, ApplicationError> {
        Ok(Self {
            run_id: summary.run_id.to_string(),
            workspace_id: summary.workspace_id.to_string(),
            preview_id: summary.preview_id.to_string(),
            status: summary.status.into(),
            actions: summary
                .actions
                .into_iter()
                .map(ActivityActionResponse::from_summary)
                .collect::<Result<_, _>>()?,
            created_at: timestamp(summary.created_at)?,
            started_at: summary.started_at.map(timestamp).transpose()?,
            finished_at: summary.finished_at.map(timestamp).transpose()?,
            updated_at: timestamp(summary.updated_at)?,
        })
    }
}

impl ActivityActionResponse {
    fn from_summary(summary: SyncActivityActionSummary) -> Result<Self, ApplicationError> {
        Ok(Self {
            action_id: summary.action_id.to_string(),
            run_id: summary.run_id.to_string(),
            workspace_id: summary.workspace_id.to_string(),
            action_hash: summary.action_hash,
            receiver_source_id: summary.receiver_source_id.to_string(),
            semantic_field: summary.semantic_field,
            status: ActivityActionStatusResponse::from(summary.status),
            write_task: ActivityTaskResponse::from_summary(summary.write_task)?,
            reconciliation_task: summary
                .reconciliation_task
                .map(ActivityTaskResponse::from_summary)
                .transpose()?,
            created_at: timestamp(summary.created_at)?,
            started_at: summary.started_at.map(timestamp).transpose()?,
            finished_at: summary.finished_at.map(timestamp).transpose()?,
            updated_at: timestamp(summary.updated_at)?,
        })
    }
}

impl ActivityTaskResponse {
    fn from_summary(summary: SyncActivityTaskSummary) -> Result<Self, ApplicationError> {
        Ok(Self {
            task_id: summary.task_id.to_string(),
            status: TaskStatusResponse::from(summary.status),
            created_at: timestamp(summary.created_at)?,
            started_at: summary.started_at.map(timestamp).transpose()?,
            finished_at: summary.finished_at.map(timestamp).transpose()?,
            updated_at: timestamp(summary.updated_at)?,
        })
    }
}

impl ActivityActionDetailResponse {
    pub fn from_detail(detail: SyncActivityActionDetail) -> Result<Self, ApplicationError> {
        Ok(Self {
            action: ActivityActionResponse::from_summary(detail.summary)?,
            provenance: detail
                .provenance
                .map(ActivityProvenanceResponse::from_snapshot)
                .transpose()?,
            reconciliation_guidance: ActivityReconciliationGuidanceResponse::from(
                detail.reconciliation_guidance,
            ),
        })
    }
}

impl ActivityProvenanceResponse {
    fn from_snapshot(snapshot: SyncActionProvenanceSnapshot) -> Result<Self, ApplicationError> {
        let preview_action = snapshot.preview_action;
        Ok(Self {
            policy_id: snapshot.policy_id.to_string(),
            policy_revision: snapshot.policy_revision,
            policy_content_hash: snapshot.policy_content_hash,
            state_registry_digest: snapshot.state_registry_digest,
            preview_id: snapshot.preview_id.to_string(),
            preview_hash: snapshot.preview_hash,
            preview_action_hash: snapshot.preview_action_hash,
            run_id: snapshot.run_id.to_string(),
            run_created_at: timestamp(snapshot.run_created_at)?,
            reducer: SyncPolicyReducer::from(preview_action.reducer),
            contributors: preview_action
                .contributors
                .into_iter()
                .map(|contributor| ActivityContributorResponse {
                    evidence_id: contributor.evidence_id,
                    state_publication_id: contributor.state_publication_id.to_string(),
                    presence: PublishedStatePresenceResponse::from(contributor.presence),
                    mapping_proof_key: contributor.mapping_proof_key,
                })
                .collect(),
            mapping_proofs: preview_action
                .mapping_proofs
                .into_iter()
                .map(|proof| ActivityMappingProofResponse {
                    key: proof.key,
                    revision_hash: proof.revision_hash,
                    row_hash: proof.row_hash,
                })
                .collect(),
            translation: ActivityTranslationResponse {
                unit: preview_action.translation.unit.as_str().to_owned(),
                quantizer: preview_action.translation.quantizer.as_str().to_owned(),
                exact_value_numerator: preview_action.translation.exact_value.numerator(),
                exact_value_denominator: preview_action.translation.exact_value.denominator(),
                intended_value: preview_action.translation.intended_value,
                lossy: preview_action.translation.lossy,
            },
            membership_effect: PreviewMembershipEffectResponse::from(
                preview_action.membership_effect,
            ),
            attempt: ActivityAttemptResponse::from_snapshot(snapshot.attempt)?,
            reconciliation: snapshot
                .reconciliation
                .map(ActivityReconciliationResponse::from_provenance)
                .transpose()?,
            recorded_at: timestamp(snapshot.recorded_at)?,
        })
    }
}

impl ActivityAttemptResponse {
    fn from_snapshot(snapshot: SyncActionAttemptSnapshot) -> Result<Self, ApplicationError> {
        let request = snapshot.request;
        Ok(Self {
            task_id: snapshot.task_id.to_string(),
            task_attempt_id: snapshot.task_attempt_id.to_string(),
            operation_id: request.operation_id.to_string(),
            idempotency_key: request.idempotency_key.to_string(),
            source_key: key_from_adapter(request.source_key),
            receiver_source_id: request.receiver_source_id.to_string(),
            receiver_subject: StateSubjectResponse::from(&request.receiver_subject),
            semantic_field: request.semantic_field,
            provider_field: StateFieldResponse {
                field: term_response(request.provider_field.field()),
                unit: request.provider_field.unit().map(term_response),
            },
            intended_value: request.intended_value,
            expected_membership: SourceMembershipResponse::from(request.expected_membership),
            allow_create_membership: request.allow_create_membership,
            state: ActionAttemptStateResponse::from(snapshot.state),
            recovery_result: snapshot
                .recovery_result
                .map(ActionRecoveryResultResponse::from),
            outcome: snapshot.outcome.map(|outcome| ActivityOutcomeResponse {
                status: TargetedStateWriteStatusResponse::from(outcome.status),
                certainty: TargetedStateWriteCertaintyResponse::from(outcome.certainty),
                retry_disposition: TargetedStateWriteRetryDispositionResponse::from(
                    outcome.retry_disposition,
                ),
                membership_effect: TargetedStateMembershipEffectResponse::from(
                    outcome.membership_effect,
                ),
                provider_revision: outcome.provider_revision,
                successor_precondition: outcome.successor_precondition,
                receipt: outcome.receipt,
                error: outcome.error.map(AdapterCallErrorResponse::from),
            }),
            prepared_at: timestamp(snapshot.prepared_at)?,
            dispatched_at: snapshot.dispatched_at.map(timestamp).transpose()?,
            settled_at: snapshot.settled_at.map(timestamp).transpose()?,
        })
    }
}

impl ActivityReconciliationResponse {
    fn from_provenance(
        provenance: SyncActionReconciliationProvenance,
    ) -> Result<Self, ApplicationError> {
        Ok(Self {
            task_id: provenance.task_id.to_string(),
            task_attempt_id: provenance.task_attempt_id.to_string(),
            write_task_attempt_id: provenance.write_task_attempt_id.to_string(),
            operation_id: provenance.operation_id.to_string(),
            idempotency_key: provenance.idempotency_key.to_string(),
            source_key: key_from_adapter(provenance.source_key),
            receiver_subject: StateSubjectResponse::from(&provenance.receiver_subject),
            provider_field: StateFieldResponse {
                field: term_response(provenance.provider_field.field()),
                unit: provenance.provider_field.unit().map(term_response),
            },
            intended_value: provenance.intended_value,
            before_membership: SourceMembershipResponse::from(provenance.before_membership),
            before_presence: StatePresenceResponse::from(provenance.before_presence),
            before_value: provenance.before_value,
            outcome_kind: ReconciliationOutcomeKindResponse::from(provenance.outcome_kind),
            observed_membership: provenance
                .observed_membership
                .map(SourceMembershipResponse::from),
            observed_presence: provenance
                .observed_presence
                .map(StatePresenceResponse::from),
            observed_value: provenance.observed_value,
            observed_at: provenance.observed_at.map(timestamp).transpose()?,
            causation_receipt: provenance.causation.map(|causation| causation.receipt),
            result: ActionReconciliationResultResponse::from(provenance.result),
            prepared_at: timestamp(provenance.prepared_at)?,
            settled_at: timestamp(provenance.settled_at)?,
        })
    }
}

impl From<SyncActivityRunStatus> for ActivityRunStatusResponse {
    fn from(status: SyncActivityRunStatus) -> Self {
        match status {
            SyncActivityRunStatus::Queued => Self::Queued,
            SyncActivityRunStatus::Running => Self::Running,
            SyncActivityRunStatus::Succeeded => Self::Succeeded,
            SyncActivityRunStatus::Failed => Self::Failed,
            SyncActivityRunStatus::Partial => Self::Partial,
            SyncActivityRunStatus::Indeterminate => Self::Indeterminate,
            SyncActivityRunStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<SyncActivityActionStatus> for ActivityActionStatusResponse {
    fn from(status: SyncActivityActionStatus) -> Self {
        match status {
            SyncActivityActionStatus::Queued => Self::Queued,
            SyncActivityActionStatus::Running => Self::Running,
            SyncActivityActionStatus::Applied => Self::Applied,
            SyncActivityActionStatus::NoOp => Self::NoOp,
            SyncActivityActionStatus::PreconditionFailed => Self::PreconditionFailed,
            SyncActivityActionStatus::RetryableFailure => Self::RetryableFailure,
            SyncActivityActionStatus::Failed => Self::Failed,
            SyncActivityActionStatus::Indeterminate => Self::Indeterminate,
            SyncActivityActionStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<SyncActivityReconciliationGuidance> for ActivityReconciliationGuidanceResponse {
    fn from(guidance: SyncActivityReconciliationGuidance) -> Self {
        match guidance {
            SyncActivityReconciliationGuidance::NotRequired => Self::NotRequired,
            SyncActivityReconciliationGuidance::Required => Self::Required,
            SyncActivityReconciliationGuidance::ConfirmedApplied => Self::ConfirmedApplied,
            SyncActivityReconciliationGuidance::ConfirmedNotApplied => Self::ConfirmedNotApplied,
        }
    }
}

impl From<PublishedStatePresence> for PublishedStatePresenceResponse {
    fn from(presence: PublishedStatePresence) -> Self {
        match presence {
            PublishedStatePresence::Present => Self::Present,
            PublishedStatePresence::Deleted => Self::Deleted,
            PublishedStatePresence::Absent => Self::Absent,
            PublishedStatePresence::Unknown => Self::Unknown,
            PublishedStatePresence::Stale => Self::Stale,
        }
    }
}

impl From<AdapterSourceMembership> for SourceMembershipResponse {
    fn from(membership: AdapterSourceMembership) -> Self {
        match membership {
            AdapterSourceMembership::Present => Self::Present,
            AdapterSourceMembership::Absent => Self::Absent,
            AdapterSourceMembership::Unknown => Self::Unknown,
        }
    }
}

impl From<AdapterStatePresence> for StatePresenceResponse {
    fn from(presence: AdapterStatePresence) -> Self {
        match presence {
            AdapterStatePresence::Present => Self::Present,
            AdapterStatePresence::Deleted => Self::Deleted,
            AdapterStatePresence::Absent => Self::Absent,
            AdapterStatePresence::Unknown => Self::Unknown,
            AdapterStatePresence::Stale => Self::Stale,
        }
    }
}

impl From<SyncActionAttemptState> for ActionAttemptStateResponse {
    fn from(state: SyncActionAttemptState) -> Self {
        match state {
            SyncActionAttemptState::Prepared => Self::Prepared,
            SyncActionAttemptState::Dispatched => Self::Dispatched,
            SyncActionAttemptState::Settled => Self::Settled,
        }
    }
}

impl From<SyncActionRecoveryResult> for ActionRecoveryResultResponse {
    fn from(result: SyncActionRecoveryResult) -> Self {
        match result {
            SyncActionRecoveryResult::ConfirmedNotApplied => Self::ConfirmedNotApplied,
            SyncActionRecoveryResult::Indeterminate => Self::Indeterminate,
        }
    }
}

impl From<AdapterTargetedStateWriteStatus> for TargetedStateWriteStatusResponse {
    fn from(status: AdapterTargetedStateWriteStatus) -> Self {
        match status {
            AdapterTargetedStateWriteStatus::Applied => Self::Applied,
            AdapterTargetedStateWriteStatus::NoOp => Self::NoOp,
            AdapterTargetedStateWriteStatus::Rejected => Self::Rejected,
            AdapterTargetedStateWriteStatus::Failed => Self::Failed,
            AdapterTargetedStateWriteStatus::Indeterminate => Self::Indeterminate,
        }
    }
}

impl From<AdapterTargetedStateWriteCertainty> for TargetedStateWriteCertaintyResponse {
    fn from(certainty: AdapterTargetedStateWriteCertainty) -> Self {
        match certainty {
            AdapterTargetedStateWriteCertainty::ConfirmedApplied => Self::ConfirmedApplied,
            AdapterTargetedStateWriteCertainty::ConfirmedNotApplied => Self::ConfirmedNotApplied,
            AdapterTargetedStateWriteCertainty::Unknown => Self::Unknown,
        }
    }
}

impl From<AdapterTargetedStateWriteRetryDisposition>
    for TargetedStateWriteRetryDispositionResponse
{
    fn from(disposition: AdapterTargetedStateWriteRetryDisposition) -> Self {
        match disposition {
            AdapterTargetedStateWriteRetryDisposition::NotRetryable => Self::NotRetryable,
            AdapterTargetedStateWriteRetryDisposition::SafeSameKey => Self::SafeSameKey,
            AdapterTargetedStateWriteRetryDisposition::ReconcileFirst => Self::ReconcileFirst,
        }
    }
}

impl From<AdapterTargetedStateMembershipEffect> for TargetedStateMembershipEffectResponse {
    fn from(effect: AdapterTargetedStateMembershipEffect) -> Self {
        match effect {
            AdapterTargetedStateMembershipEffect::Unchanged => Self::Unchanged,
            AdapterTargetedStateMembershipEffect::Created => Self::Created,
            AdapterTargetedStateMembershipEffect::Unknown => Self::Unknown,
        }
    }
}

impl From<SyncReconciliationOutcomeKind> for ReconciliationOutcomeKindResponse {
    fn from(kind: SyncReconciliationOutcomeKind) -> Self {
        match kind {
            SyncReconciliationOutcomeKind::Matched => Self::Matched,
            SyncReconciliationOutcomeKind::Unsupported => Self::Unsupported,
            SyncReconciliationOutcomeKind::NotFound => Self::NotFound,
            SyncReconciliationOutcomeKind::Ambiguous => Self::Ambiguous,
            SyncReconciliationOutcomeKind::Indeterminate => Self::Indeterminate,
        }
    }
}

impl From<SyncActionReconciliationResult> for ActionReconciliationResultResponse {
    fn from(result: SyncActionReconciliationResult) -> Self {
        match result {
            SyncActionReconciliationResult::ConfirmedApplied => Self::ConfirmedApplied,
            SyncActionReconciliationResult::ConfirmedNotApplied => Self::ConfirmedNotApplied,
            SyncActionReconciliationResult::StateMatchesCausationUnknown => {
                Self::StateMatchesCausationUnknown
            }
            SyncActionReconciliationResult::Unresolved => Self::Unresolved,
        }
    }
}
