use std::{future::Future, pin::Pin};

use time::{Duration, OffsetDateTime};
use uuid::Uuid;

pub const DEFAULT_SYNC_AUDIT_RETENTION: Duration = Duration::days(90);
pub const DEFAULT_SYNC_RETENTION_BATCH_ROWS: u32 = 512;

pub type SyncRetentionFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, SyncRetentionError<E>>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncRetentionPolicy {
    pub audit_retention: Duration,
    pub maximum_deleted_rows: u32,
}

impl Default for SyncRetentionPolicy {
    fn default() -> Self {
        Self {
            audit_retention: DEFAULT_SYNC_AUDIT_RETENTION,
            maximum_deleted_rows: DEFAULT_SYNC_RETENTION_BATCH_ROWS,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainSyncEvidence {
    pub workspace_id: Uuid,
    pub retained_at: OffsetDateTime,
    pub policy: SyncRetentionPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRetentionClass {
    UnusedPreview,
    RunBundle,
    UnreferencedPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncRetentionClassResult {
    pub class: SyncRetentionClass,
    pub deleted_roots: u32,
    pub deleted_rows: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncRetentionReport {
    pub workspace_id: Uuid,
    pub classes: [SyncRetentionClassResult; 3],
    pub deferred: bool,
    pub next_batch_minimum_rows: Option<u32>,
}

impl SyncRetentionReport {
    pub fn deleted_rows(&self) -> u32 {
        self.classes
            .iter()
            .fold(0, |total, class| total.saturating_add(class.deleted_rows))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncRetentionError<E> {
    InvalidPolicy,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncRetentionRepository: Send + Sync {
    type Error;

    fn retain_sync_evidence(
        &self,
        command: RetainSyncEvidence,
    ) -> SyncRetentionFuture<'_, SyncRetentionReport, Self::Error>;
}

#[derive(Clone)]
pub struct SyncRetentionService<R> {
    repository: R,
}

impl<R> SyncRetentionService<R>
where
    R: SyncRetentionRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn retain(
        &self,
        command: RetainSyncEvidence,
    ) -> Result<SyncRetentionReport, SyncRetentionError<R::Error>> {
        validate_command(&command)?;
        let report = self.repository.retain_sync_evidence(command).await?;
        validate_report(&report, &command)?;
        Ok(report)
    }
}

fn validate_command<E>(command: &RetainSyncEvidence) -> Result<(), SyncRetentionError<E>> {
    if command.workspace_id.is_nil()
        || command.policy.audit_retention <= Duration::ZERO
        || command.policy.maximum_deleted_rows == 0
    {
        return Err(SyncRetentionError::InvalidPolicy);
    }
    Ok(())
}

fn validate_report<E>(
    report: &SyncRetentionReport,
    command: &RetainSyncEvidence,
) -> Result<(), SyncRetentionError<E>> {
    const ORDER: [SyncRetentionClass; 3] = [
        SyncRetentionClass::UnusedPreview,
        SyncRetentionClass::RunBundle,
        SyncRetentionClass::UnreferencedPolicy,
    ];
    if report.workspace_id != command.workspace_id
        || report.classes.iter().map(|result| result.class).ne(ORDER)
        || report.deleted_rows() > command.policy.maximum_deleted_rows
        || report.next_batch_minimum_rows == Some(0)
    {
        return Err(SyncRetentionError::RepositoryInvariant);
    }
    Ok(())
}
