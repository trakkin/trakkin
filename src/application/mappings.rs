use std::{future::Future, pin::Pin, sync::Arc};

use time::OffsetDateTime;
use tokio::io::AsyncRead;
use uuid::Uuid;

use super::error::ApplicationError;

pub type MappingFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ApplicationError>> + Send + 'a>>;
pub type MappingContentReader = Pin<Box<dyn AsyncRead + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingSourceKind {
    Local,
    Upload,
    Remote,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingSourceSnapshot {
    pub id: Uuid,
    pub source_key: String,
    pub display_name: String,
    pub kind: MappingSourceKind,
    pub remote_url: Option<String>,
    pub priority: i64,
    pub enabled: bool,
    pub content_hash: String,
    pub byte_length: i64,
    pub last_checked_at: Option<OffsetDateTime>,
    pub last_refresh_error: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingStateSnapshot {
    pub active_chain_fingerprint: Option<String>,
    pub sources: Vec<MappingSourceSnapshot>,
}

pub struct MappingSourceContent {
    pub content_hash: String,
    pub byte_length: u64,
    pub reader: MappingContentReader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateUploadedMappingSource {
    pub source_id: Uuid,
    pub source_key: String,
    pub display_name: String,
    pub enabled: bool,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateRemoteMappingSource {
    pub source_id: Uuid,
    pub source_key: String,
    pub display_name: String,
    pub remote_url: String,
    pub enabled: bool,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateMappingSource {
    pub source_id: Uuid,
    pub display_name: String,
    pub enabled: bool,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateLocalMapping {
    pub record: String,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateLocalMapping {
    pub statement_id: String,
    pub record: String,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveLocalMapping {
    pub statement_id: String,
    pub removed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReorderMappingSources {
    pub source_ids: Vec<Uuid>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveMappingSource {
    pub source_id: Uuid,
    pub removed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshRemoteMappingSource {
    pub source_id: Uuid,
    pub refreshed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryMappings {
    pub expected_chain_fingerprint: Option<String>,
    pub search: Option<String>,
    pub canonical_statement: Option<String>,
    pub statement_id: Option<String>,
    pub relation_key: Option<String>,
    pub source_key: Option<String>,
    pub endpoint: Option<String>,
    pub annotation_name: Option<String>,
    pub annotation_value: Option<String>,
    pub include_shadowed: bool,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingResult {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingPageSnapshot {
    pub chain_fingerprint: String,
    pub items: Vec<MappingResult>,
    pub next_cursor: Option<String>,
}

pub trait MappingApi: Send + Sync {
    fn state(&self) -> MappingFuture<'_, MappingStateSnapshot>;
    fn create_uploaded_source(
        &self,
        command: CreateUploadedMappingSource,
        content: MappingContentReader,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn create_remote_source(
        &self,
        command: CreateRemoteMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn update_source(
        &self,
        command: UpdateMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn create_local_mapping(
        &self,
        command: CreateLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn update_local_mapping(
        &self,
        command: UpdateLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn remove_local_mapping(
        &self,
        command: RemoveLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn reorder_sources(
        &self,
        command: ReorderMappingSources,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn remove_source(
        &self,
        command: RemoveMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn refresh_remote_source(
        &self,
        command: RefreshRemoteMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot>;
    fn read_source_content(&self, source_id: Uuid) -> MappingFuture<'_, MappingSourceContent>;
    fn query(&self, query: QueryMappings) -> MappingFuture<'_, MappingPageSnapshot>;
}

pub type SharedMappingApi = Arc<dyn MappingApi>;
