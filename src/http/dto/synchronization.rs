use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    application::{
        error::{ApplicationError, ApplicationErrorCode},
        sync_policy::{SyncPolicyPage, SyncPolicyRevisionSnapshot},
        sync_preview::PreviewMembershipEffect,
        sync_run::SyncRunSnapshot,
        sync_schedule::{SyncScheduleSnapshot, SyncScheduleState},
        sync_workflow::{SyncPreviewEntryCategory, SyncPreviewFindingKind, WorkspaceSyncPreview},
    },
    domain::synchronization::{Reducer, StateRegistry, SyncPolicyDocument},
};

use super::{sources::StateSubjectResponse, timestamp};

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct SyncPolicyPageParameters {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct CreateSyncPolicyRequest {
    pub document: SyncPolicyDocumentPayload,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSyncPreviewRequest {
    pub policy_id: uuid::Uuid,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartSyncRunRequest {
    pub preview_hash: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSyncPolicyRequest {
    pub base_content_hash: String,
    pub document: SyncPolicyDocumentPayload,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPolicyDocumentPayload {
    pub schema: String,
    pub contributors: Vec<uuid::Uuid>,
    pub receivers: Vec<uuid::Uuid>,
    pub resolution_mode: SyncPolicyResolutionMode,
    pub rules: Vec<SyncPolicyFieldRulePayload>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicyResolutionMode {
    Direct,
    Transitive,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPolicyFieldRulePayload {
    pub semantic_field: String,
    pub reducer: SyncPolicyReducer,
    pub conflict: SyncPolicyConflictDisposition,
    pub presence: SyncPolicyPresenceRulesPayload,
    pub translation: SyncPolicyTranslationRulePayload,
    pub write_constraints: SyncPolicyWriteConstraintsPayload,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicyReducer {
    All,
    Any,
    CoverageContiguousPrefix,
    CoverageIntersection,
    CoverageUnion,
    Equal,
    Maximum,
    Mean,
    Minimum,
    Sum,
}

impl From<Reducer> for SyncPolicyReducer {
    fn from(reducer: Reducer) -> Self {
        match reducer {
            Reducer::All => Self::All,
            Reducer::Any => Self::Any,
            Reducer::CoverageContiguousPrefix => Self::CoverageContiguousPrefix,
            Reducer::CoverageIntersection => Self::CoverageIntersection,
            Reducer::CoverageUnion => Self::CoverageUnion,
            Reducer::Equal => Self::Equal,
            Reducer::Maximum => Self::Maximum,
            Reducer::Mean => Self::Mean,
            Reducer::Minimum => Self::Minimum,
            Reducer::Sum => Self::Sum,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicyConflictDisposition {
    RequireReducer,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPolicyPresenceRulesPayload {
    pub absent: SyncPolicyPresenceDisposition,
    pub deleted: SyncPolicyPresenceDisposition,
    pub stale: SyncPolicyBlockingPresenceDisposition,
    pub unknown: SyncPolicyBlockingPresenceDisposition,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicyPresenceDisposition {
    Clear,
    Ignore,
    Require,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicyBlockingPresenceDisposition {
    Block,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPolicyTranslationRulePayload {
    pub allow_lossy: bool,
    pub quantizer: SyncPolicyQuantizer,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicyQuantizer {
    Ceiling,
    Exact,
    Floor,
    Nearest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPolicyWriteConstraintsPayload {
    pub allow_clear: bool,
    pub allow_create_membership: bool,
    pub require_strong_precondition: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPolicyResponse {
    pub policy_id: String,
    pub workspace_id: String,
    pub revision: u64,
    pub document: SyncPolicyDocumentPayload,
    pub content_hash: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPolicyPageResponse {
    pub policies: Vec<SyncPolicyResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnableSyncScheduleRequest {
    pub interval_seconds: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncScheduleStateResponse {
    Enabled,
    Paused,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncScheduleResponse {
    pub schedule_id: String,
    pub workspace_id: String,
    pub policy_id: String,
    pub interval_seconds: u32,
    pub state: SyncScheduleStateResponse,
    pub next_run_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl SyncPolicyDocumentPayload {
    pub fn into_domain(self) -> Result<SyncPolicyDocument, ApplicationError> {
        let json = serde_json::to_string(&self).map_err(|_| {
            ApplicationError::new(
                ApplicationErrorCode::Internal,
                "synchronization policy request could not be serialized",
            )
        })?;
        SyncPolicyDocument::from_json(&json, StateRegistry::v1()).map_err(|error| {
            ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "synchronization policy is invalid",
                "document",
                error.code(),
                error.to_string(),
            )
        })
    }
}

impl SyncPolicyResponse {
    pub fn from_snapshot(snapshot: SyncPolicyRevisionSnapshot) -> Result<Self, ApplicationError> {
        let document =
            serde_json::from_slice(&snapshot.document.canonical_json()).map_err(|_| {
                ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "synchronization policy response could not be projected",
                )
            })?;
        Ok(Self {
            policy_id: snapshot.policy_id.to_string(),
            workspace_id: snapshot.workspace_id.to_string(),
            revision: snapshot.revision,
            document,
            content_hash: snapshot.content_hash,
            created_at: timestamp(snapshot.created_at)?,
        })
    }
}

impl SyncPolicyPageResponse {
    pub fn from_page(page: SyncPolicyPage) -> Result<Self, ApplicationError> {
        Ok(Self {
            policies: page
                .policies
                .into_iter()
                .map(SyncPolicyResponse::from_snapshot)
                .collect::<Result<_, _>>()?,
            next_cursor: page.next_cursor.map(|cursor| cursor.to_string()),
        })
    }
}

impl SyncScheduleResponse {
    pub fn from_snapshot(snapshot: SyncScheduleSnapshot) -> Result<Self, ApplicationError> {
        Ok(Self {
            schedule_id: snapshot.schedule_id.to_string(),
            workspace_id: snapshot.workspace_id.to_string(),
            policy_id: snapshot.policy_id.to_string(),
            interval_seconds: snapshot.recurrence.interval_seconds(),
            state: match snapshot.state {
                SyncScheduleState::Enabled => SyncScheduleStateResponse::Enabled,
                SyncScheduleState::Paused => SyncScheduleStateResponse::Paused,
            },
            next_run_at: snapshot.next_run_at.map(timestamp).transpose()?,
            created_at: timestamp(snapshot.created_at)?,
            updated_at: timestamp(snapshot.updated_at)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPreviewResponse {
    pub preview_id: String,
    pub workspace_id: String,
    pub policy_id: String,
    pub policy_revision: u64,
    pub preview_hash: String,
    pub counts: SyncPreviewCountsResponse,
    pub entries: Vec<SyncPreviewEntryResponse>,
    pub fresh_until: String,
    pub created_at: String,
    pub expires_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StartSyncRunResponse {
    pub run_id: String,
}

impl StartSyncRunResponse {
    pub fn from_snapshot(snapshot: SyncRunSnapshot) -> Self {
        Self {
            run_id: snapshot.run_id.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPreviewCountsResponse {
    pub proposed: u32,
    pub no_op: u32,
    pub findings: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncPreviewEntryResponse {
    pub ordinal: u32,
    pub action_hash: String,
    pub category: SyncPreviewEntryCategoryResponse,
    pub finding: Option<SyncPreviewFindingKindResponse>,
    pub receiver_source_id: String,
    pub receiver_subject: StateSubjectResponse,
    pub semantic_field: String,
    pub segment_coordinate: String,
    pub segment_start: u64,
    pub segment_end: u64,
    pub before: Option<i64>,
    pub intended: u64,
    pub contributor_source_ids: Vec<String>,
    pub membership_effect: PreviewMembershipEffectResponse,
    pub lossy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PreviewMembershipEffectResponse {
    Create,
    Update,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncPreviewEntryCategoryResponse {
    Proposed,
    NoOp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncPreviewFindingKindResponse {
    DestinationAlreadyMatches,
    LossyTranslation,
}

impl SyncPreviewResponse {
    pub fn from_view(view: WorkspaceSyncPreview) -> Result<Self, ApplicationError> {
        Ok(Self {
            preview_id: view.preview_id.to_string(),
            workspace_id: view.workspace_id.to_string(),
            policy_id: view.policy_id.to_string(),
            policy_revision: view.policy_revision,
            preview_hash: view.preview_hash,
            counts: SyncPreviewCountsResponse {
                proposed: view.counts.proposed,
                no_op: view.counts.no_op,
                findings: view.counts.findings,
            },
            entries: view
                .entries
                .into_iter()
                .map(|entry| SyncPreviewEntryResponse {
                    ordinal: entry.ordinal,
                    action_hash: entry.action_hash,
                    category: match entry.category {
                        SyncPreviewEntryCategory::Proposed => {
                            SyncPreviewEntryCategoryResponse::Proposed
                        }
                        SyncPreviewEntryCategory::NoOp => SyncPreviewEntryCategoryResponse::NoOp,
                    },
                    finding: entry.finding.map(|finding| match finding {
                        SyncPreviewFindingKind::DestinationAlreadyMatches => {
                            SyncPreviewFindingKindResponse::DestinationAlreadyMatches
                        }
                        SyncPreviewFindingKind::LossyTranslation => {
                            SyncPreviewFindingKindResponse::LossyTranslation
                        }
                    }),
                    receiver_source_id: entry.receiver_source_id.to_string(),
                    receiver_subject: StateSubjectResponse::from(&entry.receiver_subject),
                    semantic_field: entry.semantic_field,
                    segment_coordinate: entry.segment_coordinate,
                    segment_start: entry.segment_start,
                    segment_end: entry.segment_end,
                    before: entry.before,
                    intended: entry.intended,
                    contributor_source_ids: entry
                        .contributor_source_ids
                        .into_iter()
                        .map(|source_id| source_id.to_string())
                        .collect(),
                    membership_effect: PreviewMembershipEffectResponse::from(
                        entry.membership_effect,
                    ),
                    lossy: entry.lossy,
                })
                .collect(),
            fresh_until: timestamp(view.fresh_until)?,
            created_at: timestamp(view.created_at)?,
            expires_at: timestamp(view.expires_at)?,
        })
    }
}

impl From<PreviewMembershipEffect> for PreviewMembershipEffectResponse {
    fn from(effect: PreviewMembershipEffect) -> Self {
        match effect {
            PreviewMembershipEffect::Create => Self::Create,
            PreviewMembershipEffect::Update => Self::Update,
        }
    }
}
