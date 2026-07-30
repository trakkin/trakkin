use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use trakkin_mapping_contract::PortableEndpoint;
use uuid::Uuid;

use crate::domain::acquisition::{StateField, StateSubject, StateValue, Term};

use super::catalog_queries::PublishedStreamStatus;

pub const MAX_SYNC_STATE_EVIDENCE_SLOTS: usize = 256;
pub const MAX_SYNC_CONTRIBUTOR_ROOTS: usize = 1_024;

pub type SyncStateEvidenceFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<SyncStateEvidenceProjection, E>> + Send + 'a>>;

pub type SyncContributorRootFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<SyncContributorRootProjection, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoverSyncContributorRoots {
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub policy_revision: u64,
    pub semantic_fields: Vec<Term>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySyncStateEvidence {
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub policy_revision: u64,
    pub slots: Vec<SyncStateEvidenceSlot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncStateEvidenceSlot {
    pub source_id: Uuid,
    pub subject: StateSubject,
    pub field: StateField,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncStateEvidencePresence {
    Present,
    Deleted,
    Absent,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncStatePublicationCoverage {
    pub baseline_publication_id: Uuid,
    pub publication_id: Uuid,
    pub evidence_revision: Vec<u8>,
    pub observed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncStatePublicationPin {
    pub source_id: Uuid,
    pub status: PublishedStreamStatus,
    pub publication_id: Option<Uuid>,
    pub evidence_revision: Option<Vec<u8>>,
    pub last_success_at: Option<OffsetDateTime>,
    pub coverage: Option<SyncStatePublicationCoverage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncStateEvidence {
    pub slot: SyncStateEvidenceSlot,
    pub presence: SyncStateEvidencePresence,
    pub value: Option<StateValue>,
    pub provider_revision: Option<Vec<u8>>,
    pub observed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncStateEvidenceProjection {
    pub publications: Vec<SyncStatePublicationPin>,
    pub evidence: Vec<SyncStateEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncContributorRoot {
    pub evidence: SyncStateEvidence,
    pub roots: Vec<PortableEndpoint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncContributorRootProjection {
    pub publications: Vec<SyncStatePublicationPin>,
    pub contributors: Vec<SyncContributorRoot>,
}

pub trait SyncStateEvidenceRepository: Send + Sync {
    type Error;

    fn discover_sync_contributor_roots(
        &self,
        query: DiscoverSyncContributorRoots,
    ) -> SyncContributorRootFuture<'_, Self::Error>;

    fn query_sync_state_evidence(
        &self,
        query: QuerySyncStateEvidence,
    ) -> SyncStateEvidenceFuture<'_, Self::Error>;
}
