use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::application::{
    error::ApplicationError,
    mappings::{
        MappingPageSnapshot, MappingResult, MappingSourceKind, MappingSourceSnapshot,
        MappingStateSnapshot,
    },
};

use super::timestamp;

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct CreateUploadedMappingSourceParameters {
    pub source_key: String,
    pub display_name: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateRemoteMappingSourceRequest {
    pub source_key: String,
    pub display_name: String,
    pub remote_url: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMappingSourceRequest {
    pub display_name: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WriteLocalMappingRequest {
    pub record: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReorderMappingSourcesRequest {
    pub source_ids: Vec<Uuid>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct MappingQueryParameters {
    pub expected_chain_fingerprint: Option<String>,
    pub search: Option<String>,
    pub canonical_statement: Option<String>,
    pub statement_id: Option<String>,
    pub relation_key: Option<String>,
    pub source_key: Option<String>,
    pub endpoint: Option<String>,
    pub annotation_name: Option<String>,
    pub annotation_value: Option<String>,
    #[serde(default)]
    pub include_shadowed: bool,
    #[param(minimum = 1, maximum = 250)]
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MappingSourceKindValue {
    Local,
    Upload,
    Remote,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingSourceResponse {
    pub id: Uuid,
    pub source_key: String,
    pub display_name: String,
    pub kind: MappingSourceKindValue,
    pub remote_url: Option<String>,
    pub priority: i64,
    pub enabled: bool,
    pub content_hash: String,
    pub byte_length: i64,
    pub last_checked_at: Option<String>,
    pub last_refresh_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl MappingSourceResponse {
    fn from_snapshot(snapshot: MappingSourceSnapshot) -> Result<Self, ApplicationError> {
        Ok(Self {
            id: snapshot.id,
            source_key: snapshot.source_key,
            display_name: snapshot.display_name,
            kind: match snapshot.kind {
                MappingSourceKind::Local => MappingSourceKindValue::Local,
                MappingSourceKind::Upload => MappingSourceKindValue::Upload,
                MappingSourceKind::Remote => MappingSourceKindValue::Remote,
            },
            remote_url: snapshot.remote_url,
            priority: snapshot.priority,
            enabled: snapshot.enabled,
            content_hash: snapshot.content_hash,
            byte_length: snapshot.byte_length,
            last_checked_at: snapshot.last_checked_at.map(timestamp).transpose()?,
            last_refresh_error: snapshot.last_refresh_error,
            created_at: timestamp(snapshot.created_at)?,
            updated_at: timestamp(snapshot.updated_at)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingStateResponse {
    pub active_chain_fingerprint: Option<String>,
    pub sources: Vec<MappingSourceResponse>,
}

impl MappingStateResponse {
    pub fn from_snapshot(snapshot: MappingStateSnapshot) -> Result<Self, ApplicationError> {
        Ok(Self {
            active_chain_fingerprint: snapshot.active_chain_fingerprint,
            sources: snapshot
                .sources
                .into_iter()
                .map(MappingSourceResponse::from_snapshot)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingResultResponse {
    pub relation_key: String,
    pub statement_id: String,
    pub canonical_statement: String,
    pub canonical_record: String,
    pub operator: String,
    pub source_key: String,
    pub source_content_hash: String,
    pub layer_position: u64,
    pub record_ordinal: u64,
    pub active: bool,
    pub occurrence_count: u64,
    pub evidence_fingerprint: String,
    pub metadata: Vec<String>,
}

impl From<MappingResult> for MappingResultResponse {
    fn from(result: MappingResult) -> Self {
        Self {
            relation_key: result.relation_key,
            statement_id: result.statement_id,
            canonical_statement: result.canonical_statement,
            canonical_record: result.canonical_record,
            operator: result.operator,
            source_key: result.source_key,
            source_content_hash: result.source_content_hash,
            layer_position: result.layer_position,
            record_ordinal: result.record_ordinal,
            active: result.active,
            occurrence_count: result.occurrence_count,
            evidence_fingerprint: result.evidence_fingerprint,
            metadata: result.metadata,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingPageResponse {
    pub chain_fingerprint: String,
    pub items: Vec<MappingResultResponse>,
    pub next_cursor: Option<String>,
}

impl From<MappingPageSnapshot> for MappingPageResponse {
    fn from(page: MappingPageSnapshot) -> Self {
        Self {
            chain_fingerprint: page.chain_fingerprint,
            items: page
                .items
                .into_iter()
                .map(MappingResultResponse::from)
                .collect(),
            next_cursor: page.next_cursor,
        }
    }
}
