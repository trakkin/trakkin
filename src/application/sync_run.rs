use std::{collections::BTreeSet, future::Future, pin::Pin};

use time::OffsetDateTime;
use trakkin_mapping::validate_content_hash;
use uuid::Uuid;

use super::{
    sync_preview::SyncPreviewFingerprint,
    task_manager::{TaskDefinition, TaskSnapshot, TaskStatus},
};

pub type SyncRunFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, SyncRunAdmissionError<E>>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActionAdmission {
    pub action_hash: String,
    pub action_id: Uuid,
    pub task_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmitSyncPreview {
    pub run_id: Uuid,
    pub schedule_occurrence_id: Option<Uuid>,
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub preview_hash: String,
    pub current_fingerprint: SyncPreviewFingerprint,
    pub actions: Vec<SyncActionAdmission>,
    pub admitted_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreSyncRunAdmission {
    pub run_id: Uuid,
    pub schedule_occurrence_id: Option<Uuid>,
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub preview_hash: String,
    pub current_fingerprint_json: Vec<u8>,
    pub actions: Vec<SyncActionAdmission>,
    pub admitted_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncRunActionSnapshot {
    pub action_id: Uuid,
    pub action_hash: String,
    pub idempotency_key: String,
    pub receiver_source_id: Uuid,
    pub task: TaskSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncRunSnapshot {
    pub run_id: Uuid,
    pub preview_id: Uuid,
    pub workspace_id: Uuid,
    pub preview_hash: String,
    pub actions: Vec<SyncRunActionSnapshot>,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncRunAdmissionError<E> {
    InvalidActions,
    PreviewNotFound,
    PreviewHashMismatch,
    PreviewStale,
    PreviewAlreadyAdmitted,
    ReceiverUnavailable,
    ReceiverActionBlocked,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncRunRepository: Send + Sync {
    type Error;

    fn admit_sync_run(
        &self,
        command: StoreSyncRunAdmission,
    ) -> SyncRunFuture<'_, SyncRunSnapshot, Self::Error>;
}

#[derive(Clone)]
pub struct SyncRunService<R> {
    repository: R,
}

impl<R> SyncRunService<R>
where
    R: SyncRunRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn admit(
        &self,
        command: AdmitSyncPreview,
    ) -> Result<SyncRunSnapshot, SyncRunAdmissionError<R::Error>> {
        self.admit_preview(command).await
    }

    async fn admit_preview(
        &self,
        mut command: AdmitSyncPreview,
    ) -> Result<SyncRunSnapshot, SyncRunAdmissionError<R::Error>> {
        validate_content_hash(&command.preview_hash)
            .map_err(|_| SyncRunAdmissionError::PreviewHashMismatch)?;
        let current_fingerprint_json = command
            .current_fingerprint
            .canonical_json()
            .map_err(|_| SyncRunAdmissionError::PreviewStale)?;
        validate_actions(&command.actions)?;
        command
            .actions
            .sort_unstable_by(|left, right| left.action_hash.cmp(&right.action_hash));
        let expected_actions = command.actions.clone();
        let stored = self
            .repository
            .admit_sync_run(StoreSyncRunAdmission {
                run_id: command.run_id,
                schedule_occurrence_id: command.schedule_occurrence_id,
                preview_id: command.preview_id,
                workspace_id: command.workspace_id,
                preview_hash: command.preview_hash.clone(),
                current_fingerprint_json,
                actions: command.actions,
                admitted_at: command.admitted_at,
            })
            .await?;
        if stored.run_id != command.run_id
            || stored.preview_id != command.preview_id
            || stored.workspace_id != command.workspace_id
            || stored.preview_hash != command.preview_hash
            || stored.created_at != command.admitted_at
            || stored.actions.len() != expected_actions.len()
        {
            return Err(SyncRunAdmissionError::RepositoryInvariant);
        }
        for (stored, expected) in stored.actions.iter().zip(expected_actions) {
            if stored.action_id != expected.action_id
                || stored.action_hash != expected.action_hash
                || stored.idempotency_key != expected.action_id.to_string()
                || stored.task.id != expected.task_id
                || stored.task.workspace_id != Some(command.workspace_id)
                || stored.task.connection_id == Uuid::nil()
                || stored.task.definition
                    != (TaskDefinition::SyncAction {
                        action_id: expected.action_id,
                        receiver_source_id: stored.receiver_source_id,
                    })
                || stored.task.status != TaskStatus::Queued
                || !stored.task.cancellation_supported
                || stored.task.created_at != command.admitted_at
            {
                return Err(SyncRunAdmissionError::RepositoryInvariant);
            }
        }
        Ok(stored)
    }
}

fn validate_actions<E>(actions: &[SyncActionAdmission]) -> Result<(), SyncRunAdmissionError<E>> {
    if actions.is_empty() {
        return Err(SyncRunAdmissionError::InvalidActions);
    }
    let mut action_hashes = BTreeSet::new();
    let mut action_ids = BTreeSet::new();
    let mut task_ids = BTreeSet::new();
    for action in actions {
        if validate_content_hash(&action.action_hash).is_err()
            || !action_hashes.insert(&action.action_hash)
            || !action_ids.insert(action.action_id)
            || !task_ids.insert(action.task_id)
        {
            return Err(SyncRunAdmissionError::InvalidActions);
        }
    }
    Ok(())
}
