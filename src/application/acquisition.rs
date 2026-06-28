use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::acquisition::{CatalogBatch, StateBatch};

pub type RepositoryFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginRead {
    pub attempt_id: Uuid,
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
    pub started_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbandonRead {
    pub attempt_id: Uuid,
    pub outcome: ReadAbandonment,
    pub completed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadAbandonment {
    Failed(ReadFailure),
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadFailure {
    pub code: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSourceKey {
    pub namespace: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadFence {
    pub source_id: Uuid,
    pub provider_source_key: ProviderSourceKey,
    pub read_mode: ReadMode,
    pub expected_cursor: Option<Vec<u8>>,
    pub expected_publication_id: Option<Uuid>,
    pub expected_capability_revision: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadMode {
    Full,
    Incremental,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StageCatalogBatch {
    pub attempt_id: Uuid,
    pub batch: CatalogBatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageStateBatch {
    pub attempt_id: Uuid,
    pub batch: StateBatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishRead {
    pub attempt_id: Uuid,
    pub publication_id: Uuid,
    pub next_cursor: Option<Vec<u8>>,
    pub evidence_revision: Vec<u8>,
    pub observed_at: OffsetDateTime,
    pub published_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetSourceStream {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
    pub completed_at: OffsetDateTime,
}

pub trait PublicationRepository: Send + Sync {
    type Error;

    fn abandon_read(&self, command: AbandonRead) -> RepositoryFuture<'_, (), Self::Error>;

    fn begin_catalog_read(
        &self,
        command: BeginRead,
    ) -> RepositoryFuture<'_, ReadFence, Self::Error>;

    fn stage_catalog_batch(
        &self,
        command: StageCatalogBatch,
    ) -> RepositoryFuture<'_, (), Self::Error>;

    fn publish_full_catalog(&self, command: PublishRead) -> RepositoryFuture<'_, (), Self::Error>;

    fn publish_incremental_catalog(
        &self,
        command: PublishRead,
    ) -> RepositoryFuture<'_, (), Self::Error>;

    fn begin_state_read(&self, command: BeginRead) -> RepositoryFuture<'_, ReadFence, Self::Error>;

    fn stage_state_batch(&self, command: StageStateBatch) -> RepositoryFuture<'_, (), Self::Error>;

    fn publish_full_state(&self, command: PublishRead) -> RepositoryFuture<'_, (), Self::Error>;

    fn publish_incremental_state(
        &self,
        command: PublishRead,
    ) -> RepositoryFuture<'_, (), Self::Error>;

    fn reset_source_stream(
        &self,
        command: ResetSourceStream,
    ) -> RepositoryFuture<'_, (), Self::Error>;
}
