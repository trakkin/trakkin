use crate::application::mappings::{
    MappingOperator, MappingPageSnapshot, MappingRecordStatus, MappingResult, MappingSource,
    MappingSourceKind, MappingStateSnapshot,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct MappingQueryParameters {
    pub expected_fingerprint: Option<String>,
    pub search: Option<String>,
    pub canonical_statement: Option<String>,
    pub claim_id: Option<String>,
    pub operator: Option<MappingOperatorParameter>,
    pub endpoint: Option<String>,
    pub annotation_name: Option<String>,
    pub annotation_value: Option<String>,
    #[param(minimum = 1, maximum = 1000)]
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MappingOperatorParameter {
    Exact,
    Coverage,
    Implication,
}

impl From<MappingOperatorParameter> for MappingOperator {
    fn from(value: MappingOperatorParameter) -> Self {
        match value {
            MappingOperatorParameter::Exact => Self::Exact,
            MappingOperatorParameter::Coverage => Self::Coverage,
            MappingOperatorParameter::Implication => Self::Implication,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingStateResponse {
    pub fingerprint: String,
    pub records: u64,
    pub active: u64,
    pub rejected: u64,
    pub resolved: u64,
    pub unresolved: u64,
    pub conflicting: u64,
    pub units: u64,
}

impl From<MappingStateSnapshot> for MappingStateResponse {
    fn from(snapshot: MappingStateSnapshot) -> Self {
        Self {
            fingerprint: snapshot.fingerprint,
            records: snapshot.records,
            active: snapshot.active,
            rejected: snapshot.rejected,
            resolved: snapshot.resolved,
            unresolved: snapshot.unresolved,
            conflicting: snapshot.conflicting,
            units: snapshot.units,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MappingStatusResponse {
    Resolved,
    Unresolved,
    Invalid,
    Conflicting,
    Rejected,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingSourceResponse {
    pub id: String,
    pub display_name: String,
    pub kind: MappingSourceKindResponse,
    pub url: Option<String>,
    pub branch: Option<String>,
    pub repository_path: Option<String>,
    pub poll_interval_seconds: Option<u64>,
    pub enabled: bool,
    pub cached_records: u64,
    pub revision: Option<String>,
    #[schema(format = DateTime)]
    pub last_checked_at: Option<String>,
    #[schema(format = DateTime)]
    pub last_updated_at: Option<String>,
    #[schema(format = DateTime)]
    pub next_check_at: Option<String>,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingSourcesResponse {
    pub fingerprint: String,
    pub sources: Vec<MappingSourceResponse>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MappingSourceKindResponse {
    Local,
    Upstream,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SaveMappingSourceRequest {
    pub expected_fingerprint: String,
    pub display_name: String,
    pub url: String,
    pub branch: String,
    pub poll_interval_seconds: u64,
    pub enabled: bool,
}

impl TryFrom<MappingSource> for MappingSourceResponse {
    type Error = time::error::Format;

    fn try_from(status: MappingSource) -> Result<Self, Self::Error> {
        let timestamp = |value: time::OffsetDateTime| {
            value.format(&time::format_description::well_known::Rfc3339)
        };
        Ok(Self {
            id: status.id,
            display_name: status.display_name,
            kind: match status.kind {
                MappingSourceKind::Local => MappingSourceKindResponse::Local,
                MappingSourceKind::Upstream => MappingSourceKindResponse::Upstream,
            },
            url: status.url,
            branch: status.branch,
            repository_path: status.repository_path,
            poll_interval_seconds: status.poll_interval_seconds,
            enabled: status.enabled,
            cached_records: status.cached_records,
            revision: status.revision,
            last_checked_at: status.last_checked_at.map(timestamp).transpose()?,
            last_updated_at: status.last_updated_at.map(timestamp).transpose()?,
            next_check_at: status.next_check_at.map(timestamp).transpose()?,
            consecutive_failures: status.consecutive_failures,
            last_error: status.last_error,
        })
    }
}

impl From<MappingRecordStatus> for MappingStatusResponse {
    fn from(status: MappingRecordStatus) -> Self {
        match status {
            MappingRecordStatus::Resolved => Self::Resolved,
            MappingRecordStatus::Unresolved => Self::Unresolved,
            MappingRecordStatus::Invalid => Self::Invalid,
            MappingRecordStatus::Conflicting => Self::Conflicting,
            MappingRecordStatus::Rejected => Self::Rejected,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingResultResponse {
    pub claim_id: String,
    pub canonical_statement: String,
    pub canonical_record: String,
    pub operator: String,
    pub metadata: Vec<String>,
    pub rejected: bool,
    pub sources: Vec<String>,
    pub status: MappingStatusResponse,
    pub diagnostics: Vec<String>,
}

impl From<MappingResult> for MappingResultResponse {
    fn from(result: MappingResult) -> Self {
        Self {
            claim_id: result.claim_id,
            canonical_statement: result.canonical_statement,
            canonical_record: result.canonical_record,
            operator: result.operator,
            metadata: result.metadata,
            rejected: result.rejected,
            sources: result.sources,
            status: result.status.into(),
            diagnostics: result.diagnostics,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingPageResponse {
    pub fingerprint: String,
    pub items: Vec<MappingResultResponse>,
    pub next_cursor: Option<String>,
}

impl From<MappingPageSnapshot> for MappingPageResponse {
    fn from(page: MappingPageSnapshot) -> Self {
        Self {
            fingerprint: page.fingerprint,
            items: page
                .items
                .into_iter()
                .map(MappingResultResponse::from)
                .collect(),
            next_cursor: page.next_cursor,
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreviewMappingRequest {
    pub record: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SaveMappingRequest {
    pub expected_fingerprint: String,
    pub record: String,
    pub replaces: Option<String>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DecideMappingRequest {
    pub expected_fingerprint: String,
    pub rejected: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeleteMappingRequest {
    pub expected_fingerprint: String,
}
