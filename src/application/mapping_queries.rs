use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use super::mapping_lifecycle::{MappingRevisionOrigin, MappingRevisionSnapshot};

pub type MappingQueryFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryMappingRegistry {
    pub workspace_id: Uuid,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryMappingHistory {
    pub workspace_id: Uuid,
    pub mapping_id: Uuid,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingActivationView {
    pub active_content_hash: Option<String>,
    pub generation: i64,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingSummary {
    pub mapping_id: Uuid,
    pub head_content_hash: String,
    pub portable_revision: u64,
    pub row_count: u64,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub activation: Option<MappingActivationView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingRegistryPage {
    pub mappings: Vec<MappingSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingRevisionSummary {
    pub content_hash: String,
    pub portable_revision: u64,
    pub row_count: u64,
    pub origin: MappingRevisionOrigin,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingHistoryPage {
    pub mapping_id: Uuid,
    pub head_content_hash: String,
    pub revisions: Vec<MappingRevisionSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingDetail {
    pub revision: MappingRevisionSnapshot,
    pub record_created_at: OffsetDateTime,
    pub record_updated_at: OffsetDateTime,
    pub activation: Option<MappingActivationView>,
}

pub trait MappingQueryRepository: Send + Sync {
    type Error;

    fn query_registry(
        &self,
        query: QueryMappingRegistry,
    ) -> MappingQueryFuture<'_, MappingRegistryPage, Self::Error>;

    fn query_head(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
    ) -> MappingQueryFuture<'_, MappingDetail, Self::Error>;

    fn query_history(
        &self,
        query: QueryMappingHistory,
    ) -> MappingQueryFuture<'_, MappingHistoryPage, Self::Error>;

    fn query_revision(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> MappingQueryFuture<'_, MappingDetail, Self::Error>;
}
