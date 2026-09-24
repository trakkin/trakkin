use std::{future::Future, pin::Pin};

use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use super::{
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    facade::ApplicationFuture,
    sync_policy::SyncPolicyRepository,
    sync_preview::{SyncPreviewLifecycleError, SyncPreviewRepository, SyncPreviewService},
    sync_run::{SyncRunAdmissionError, SyncRunRepository, SyncRunSnapshot},
    sync_workflow::{
        CreateWorkspaceSyncPreview, StartWorkspaceSyncRun, SyncPreviewInputCollector,
        SyncRunWorkflowError, SyncRunWorkflowService, SyncWorkflowError, SyncWorkflowService,
    },
};

pub const MINIMUM_SYNC_INTERVAL_SECONDS: u32 = 60;
pub const MAXIMUM_SYNC_INTERVAL_SECONDS: u32 = 31 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncScheduleRecurrence {
    interval_seconds: u32,
}

impl SyncScheduleRecurrence {
    pub fn every(interval_seconds: u32) -> Result<Self, SyncScheduleError> {
        if !(MINIMUM_SYNC_INTERVAL_SECONDS..=MAXIMUM_SYNC_INTERVAL_SECONDS)
            .contains(&interval_seconds)
        {
            return Err(SyncScheduleError::InvalidInterval);
        }
        Ok(Self { interval_seconds })
    }

    #[must_use]
    pub fn interval_seconds(self) -> u32 {
        self.interval_seconds
    }

    pub fn first_after(
        self,
        enabled_at: OffsetDateTime,
    ) -> Result<OffsetDateTime, SyncScheduleError> {
        enabled_at
            .checked_add(Duration::seconds(i64::from(self.interval_seconds)))
            .ok_or(SyncScheduleError::TimeOverflow)
    }

    pub fn next_after(
        self,
        scheduled_for: OffsetDateTime,
        observed_at: OffsetDateTime,
    ) -> Result<OffsetDateTime, SyncScheduleError> {
        if observed_at < scheduled_for {
            return Ok(scheduled_for);
        }
        let elapsed_seconds = (observed_at - scheduled_for).whole_seconds();
        let intervals = elapsed_seconds
            .checked_div(i64::from(self.interval_seconds))
            .and_then(|elapsed| elapsed.checked_add(1))
            .ok_or(SyncScheduleError::TimeOverflow)?;
        let offset_seconds = i64::from(self.interval_seconds)
            .checked_mul(intervals)
            .ok_or(SyncScheduleError::TimeOverflow)?;
        scheduled_for
            .checked_add(Duration::seconds(offset_seconds))
            .ok_or(SyncScheduleError::TimeOverflow)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncScheduleState {
    Enabled,
    Paused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncScheduleSnapshot {
    pub schedule_id: Uuid,
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub recurrence: SyncScheduleRecurrence,
    pub state: SyncScheduleState,
    pub next_run_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnableSyncSchedule {
    pub schedule_id: Uuid,
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub recurrence: SyncScheduleRecurrence,
    pub enabled_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreSyncSchedule {
    pub schedule_id: Uuid,
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub recurrence: SyncScheduleRecurrence,
    pub next_run_at: OffsetDateTime,
    pub enabled_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PauseSyncSchedule {
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub paused_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaterializeDueSyncSchedule {
    pub occurrence_id: Uuid,
    pub preview_id: Uuid,
    pub run_id: Uuid,
    pub observed_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncScheduleOccurrenceStatus {
    Pending,
    Completed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncScheduleOccurrenceSnapshot {
    pub occurrence_id: Uuid,
    pub schedule_id: Uuid,
    pub workspace_id: Uuid,
    pub policy_id: Uuid,
    pub scheduled_for: OffsetDateTime,
    pub preview_id: Uuid,
    pub run_id: Uuid,
    pub status: SyncScheduleOccurrenceStatus,
    pub created_at: OffsetDateTime,
    pub completed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompleteSyncScheduleOccurrence {
    pub occurrence_id: Uuid,
    pub completed_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplacePendingSyncPreview {
    pub occurrence_id: Uuid,
    pub current_preview_id: Uuid,
    pub replacement_preview_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncScheduleError {
    InvalidInterval,
    TimeOverflow,
}

pub type SyncScheduleFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, SyncScheduleRepositoryError<E>>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncScheduleRepositoryError<E> {
    PolicyNotFound,
    ScheduleNotFound,
    OccurrenceNotFound,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncScheduleRepository: Send + Sync {
    type Error;

    fn store_schedule(
        &self,
        command: StoreSyncSchedule,
    ) -> SyncScheduleFuture<'_, SyncScheduleSnapshot, Self::Error>;

    fn get_policy_schedule(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> SyncScheduleFuture<'_, Option<SyncScheduleSnapshot>, Self::Error>;

    fn pause_schedule(
        &self,
        command: PauseSyncSchedule,
    ) -> SyncScheduleFuture<'_, SyncScheduleSnapshot, Self::Error>;

    fn materialize_due_schedule(
        &self,
        command: MaterializeDueSyncSchedule,
    ) -> SyncScheduleFuture<'_, Option<SyncScheduleOccurrenceSnapshot>, Self::Error>;

    fn oldest_pending_occurrence(
        &self,
    ) -> SyncScheduleFuture<'_, Option<SyncScheduleOccurrenceSnapshot>, Self::Error>;

    fn complete_occurrence(
        &self,
        command: CompleteSyncScheduleOccurrence,
    ) -> SyncScheduleFuture<'_, SyncScheduleOccurrenceSnapshot, Self::Error>;

    fn occurrence_run_exists(
        &self,
        occurrence_id: Uuid,
        run_id: Uuid,
    ) -> SyncScheduleFuture<'_, bool, Self::Error>;

    fn replace_pending_preview(
        &self,
        command: ReplacePendingSyncPreview,
    ) -> SyncScheduleFuture<'_, SyncScheduleOccurrenceSnapshot, Self::Error>;
}

pub trait SyncScheduleApi: Send + Sync {
    fn enable_schedule(
        &self,
        command: EnableSyncSchedule,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot>;
    fn get_schedule(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot>;
    fn pause_schedule(
        &self,
        command: PauseSyncSchedule,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot>;
}

#[derive(Clone)]
pub struct SyncScheduleService<R> {
    repository: R,
}

impl<R> SyncScheduleService<R>
where
    R: SyncScheduleRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn enable(
        &self,
        command: EnableSyncSchedule,
    ) -> Result<SyncScheduleSnapshot, SyncScheduleRepositoryError<R::Error>> {
        let next_run_at = command
            .recurrence
            .first_after(command.enabled_at)
            .map_err(|_| SyncScheduleRepositoryError::RepositoryInvariant)?;
        let snapshot = self
            .repository
            .store_schedule(StoreSyncSchedule {
                schedule_id: command.schedule_id,
                workspace_id: command.workspace_id,
                policy_id: command.policy_id,
                recurrence: command.recurrence,
                next_run_at,
                enabled_at: command.enabled_at,
            })
            .await?;
        let snapshot = validate_schedule(
            snapshot,
            command.workspace_id,
            command.policy_id,
            SyncScheduleState::Enabled,
        )?;
        if snapshot.recurrence != command.recurrence || snapshot.next_run_at != Some(next_run_at) {
            return Err(SyncScheduleRepositoryError::RepositoryInvariant);
        }
        tracing::info!(
            workspace.id = %snapshot.workspace_id,
            sync.policy.id = %snapshot.policy_id,
            sync.schedule.id = %snapshot.schedule_id,
            "synchronization schedule enabled"
        );
        Ok(snapshot)
    }

    pub async fn get(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> Result<SyncScheduleSnapshot, SyncScheduleRepositoryError<R::Error>> {
        let snapshot = self
            .repository
            .get_policy_schedule(workspace_id, policy_id)
            .await?
            .ok_or(SyncScheduleRepositoryError::ScheduleNotFound)?;
        let state = snapshot.state;
        validate_schedule(snapshot, workspace_id, policy_id, state)
    }

    pub async fn pause(
        &self,
        command: PauseSyncSchedule,
    ) -> Result<SyncScheduleSnapshot, SyncScheduleRepositoryError<R::Error>> {
        let snapshot = self.repository.pause_schedule(command).await?;
        let snapshot = validate_schedule(
            snapshot,
            command.workspace_id,
            command.policy_id,
            SyncScheduleState::Paused,
        )?;
        tracing::info!(
            workspace.id = %snapshot.workspace_id,
            sync.policy.id = %snapshot.policy_id,
            sync.schedule.id = %snapshot.schedule_id,
            "synchronization schedule paused"
        );
        Ok(snapshot)
    }

    pub async fn materialize_due(
        &self,
        command: MaterializeDueSyncSchedule,
    ) -> Result<Option<SyncScheduleOccurrenceSnapshot>, SyncScheduleRepositoryError<R::Error>> {
        self.repository
            .materialize_due_schedule(command)
            .await?
            .map(validate_pending_occurrence)
            .transpose()
    }

    pub async fn oldest_pending(
        &self,
    ) -> Result<Option<SyncScheduleOccurrenceSnapshot>, SyncScheduleRepositoryError<R::Error>> {
        self.repository
            .oldest_pending_occurrence()
            .await?
            .map(validate_pending_occurrence)
            .transpose()
    }

    pub async fn complete(
        &self,
        command: CompleteSyncScheduleOccurrence,
    ) -> Result<SyncScheduleOccurrenceSnapshot, SyncScheduleRepositoryError<R::Error>> {
        let snapshot = self.repository.complete_occurrence(command).await?;
        if snapshot.occurrence_id != command.occurrence_id
            || snapshot.status != SyncScheduleOccurrenceStatus::Completed
            || snapshot.completed_at.is_none()
        {
            return Err(SyncScheduleRepositoryError::RepositoryInvariant);
        }
        Ok(snapshot)
    }

    pub async fn occurrence_has_run(
        &self,
        occurrence_id: Uuid,
        run_id: Uuid,
    ) -> Result<bool, SyncScheduleRepositoryError<R::Error>> {
        self.repository
            .occurrence_run_exists(occurrence_id, run_id)
            .await
    }

    pub async fn replace_pending_preview(
        &self,
        command: ReplacePendingSyncPreview,
    ) -> Result<SyncScheduleOccurrenceSnapshot, SyncScheduleRepositoryError<R::Error>> {
        let snapshot = self.repository.replace_pending_preview(command).await?;
        let snapshot = validate_pending_occurrence(snapshot)?;
        if snapshot.occurrence_id != command.occurrence_id
            || snapshot.preview_id != command.replacement_preview_id
        {
            return Err(SyncScheduleRepositoryError::RepositoryInvariant);
        }
        Ok(snapshot)
    }
}

impl<R> SyncScheduleApi for SyncScheduleService<R>
where
    R: SyncScheduleRepository + 'static,
    R::Error: IntoApplicationError,
{
    fn enable_schedule(
        &self,
        command: EnableSyncSchedule,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot> {
        Box::pin(async move { self.enable(command).await.map_err(schedule_error) })
    }

    fn get_schedule(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot> {
        Box::pin(async move {
            self.get(workspace_id, policy_id)
                .await
                .map_err(schedule_error)
        })
    }

    fn pause_schedule(
        &self,
        command: PauseSyncSchedule,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot> {
        Box::pin(async move { self.pause(command).await.map_err(schedule_error) })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TickSyncSchedules {
    pub occurrence_id: Uuid,
    pub preview_id: Uuid,
    pub run_id: Uuid,
    pub observed_at: OffsetDateTime,
    pub preview_expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncScheduleTickResult {
    Idle,
    NoChanges {
        occurrence_id: Uuid,
    },
    RunStarted {
        occurrence_id: Uuid,
        run: SyncRunSnapshot,
    },
    RunAlreadyStarted {
        occurrence_id: Uuid,
        run_id: Uuid,
    },
}

#[derive(Debug)]
pub enum SyncScheduleRunnerError<S, P, C, V, R> {
    Schedule(SyncScheduleRepositoryError<S>),
    PreviewRead(SyncPreviewLifecycleError<V>),
    Preview(SyncWorkflowError<P, C, V>),
    Run(SyncRunWorkflowError<P, C, V, R>),
}

#[derive(Clone)]
pub struct SyncScheduleRunner<S, P, C, V, R> {
    schedules: SyncScheduleService<S>,
    previews: SyncPreviewService<V>,
    preview_workflow: SyncWorkflowService<P, C, V>,
    run_workflow: SyncRunWorkflowService<P, C, V, R>,
}

impl<S, P, C, V, R> SyncScheduleRunner<S, P, C, V, R>
where
    S: SyncScheduleRepository,
    P: SyncPolicyRepository + Clone,
    C: SyncPreviewInputCollector + Clone,
    V: SyncPreviewRepository + Clone,
    R: SyncRunRepository,
{
    pub fn new(schedules: S, policies: P, collector: C, previews: V, runs: R) -> Self {
        Self {
            schedules: SyncScheduleService::new(schedules),
            previews: SyncPreviewService::new(previews.clone()),
            preview_workflow: SyncWorkflowService::new(
                policies.clone(),
                collector.clone(),
                previews.clone(),
                crate::domain::synchronization::StateRegistry::v1(),
            ),
            run_workflow: SyncRunWorkflowService::new(policies, collector, previews, runs),
        }
    }

    pub async fn tick(
        &self,
        command: TickSyncSchedules,
    ) -> Result<
        SyncScheduleTickResult,
        SyncScheduleRunnerError<S::Error, P::Error, C::Error, V::Error, R::Error>,
    > {
        if command.preview_expires_at <= command.observed_at {
            return Err(SyncScheduleRunnerError::Schedule(
                SyncScheduleRepositoryError::RepositoryInvariant,
            ));
        }
        self.schedules
            .materialize_due(MaterializeDueSyncSchedule {
                occurrence_id: command.occurrence_id,
                preview_id: command.preview_id,
                run_id: command.run_id,
                observed_at: command.observed_at,
            })
            .await
            .map_err(SyncScheduleRunnerError::Schedule)?;
        let Some(mut occurrence) = self
            .schedules
            .oldest_pending()
            .await
            .map_err(SyncScheduleRunnerError::Schedule)?
        else {
            return Ok(SyncScheduleTickResult::Idle);
        };
        if self
            .schedules
            .occurrence_has_run(occurrence.occurrence_id, occurrence.run_id)
            .await
            .map_err(SyncScheduleRunnerError::Schedule)?
        {
            self.complete_occurrence(occurrence.occurrence_id, command.observed_at)
                .await?;
            return Ok(SyncScheduleTickResult::RunAlreadyStarted {
                occurrence_id: occurrence.occurrence_id,
                run_id: occurrence.run_id,
            });
        }
        let stored_preview = match self
            .previews
            .get(occurrence.workspace_id, occurrence.preview_id)
            .await
        {
            Ok(preview) if command.observed_at < preview.fresh_until => Some(preview),
            Ok(_) => {
                occurrence = self
                    .schedules
                    .replace_pending_preview(ReplacePendingSyncPreview {
                        occurrence_id: occurrence.occurrence_id,
                        current_preview_id: occurrence.preview_id,
                        replacement_preview_id: command.preview_id,
                    })
                    .await
                    .map_err(SyncScheduleRunnerError::Schedule)?;
                None
            }
            Err(SyncPreviewLifecycleError::PreviewNotFound) => None,
            Err(error) => return Err(SyncScheduleRunnerError::PreviewRead(error)),
        };
        let preview = match stored_preview {
            Some(preview) => preview,
            None => {
                match self
                    .preview_workflow
                    .create_preview(CreateWorkspaceSyncPreview {
                        preview_id: occurrence.preview_id,
                        workspace_id: occurrence.workspace_id,
                        policy_id: occurrence.policy_id,
                        created_at: command.observed_at,
                        expires_at: command.preview_expires_at,
                    })
                    .await
                {
                    Ok(preview) => preview,
                    Err(error) => match self
                        .previews
                        .get(occurrence.workspace_id, occurrence.preview_id)
                        .await
                    {
                        Ok(preview) => preview,
                        Err(_) => return Err(SyncScheduleRunnerError::Preview(error)),
                    },
                }
            }
        };
        if preview.actions.is_empty() {
            self.complete_occurrence(occurrence.occurrence_id, command.observed_at)
                .await?;
            return Ok(SyncScheduleTickResult::NoChanges {
                occurrence_id: occurrence.occurrence_id,
            });
        }
        match self
            .run_workflow
            .start_run(StartWorkspaceSyncRun {
                run_id: occurrence.run_id,
                schedule_occurrence_id: Some(occurrence.occurrence_id),
                preview_id: occurrence.preview_id,
                workspace_id: occurrence.workspace_id,
                preview_hash: preview.content_hash,
                admitted_at: command.observed_at,
            })
            .await
        {
            Ok(run) => {
                self.complete_occurrence(occurrence.occurrence_id, command.observed_at)
                    .await?;
                Ok(SyncScheduleTickResult::RunStarted {
                    occurrence_id: occurrence.occurrence_id,
                    run,
                })
            }
            Err(
                error @ SyncRunWorkflowError::Admission(
                    SyncRunAdmissionError::PreviewAlreadyAdmitted,
                ),
            ) => {
                let exists = self
                    .schedules
                    .occurrence_has_run(occurrence.occurrence_id, occurrence.run_id)
                    .await
                    .map_err(SyncScheduleRunnerError::Schedule)?;
                if !exists {
                    return Err(SyncScheduleRunnerError::Run(error));
                }
                self.complete_occurrence(occurrence.occurrence_id, command.observed_at)
                    .await?;
                Ok(SyncScheduleTickResult::RunAlreadyStarted {
                    occurrence_id: occurrence.occurrence_id,
                    run_id: occurrence.run_id,
                })
            }
            Err(error) => Err(SyncScheduleRunnerError::Run(error)),
        }
    }

    async fn complete_occurrence(
        &self,
        occurrence_id: Uuid,
        completed_at: OffsetDateTime,
    ) -> Result<(), SyncScheduleRunnerError<S::Error, P::Error, C::Error, V::Error, R::Error>> {
        self.schedules
            .complete(CompleteSyncScheduleOccurrence {
                occurrence_id,
                completed_at,
            })
            .await
            .map_err(SyncScheduleRunnerError::Schedule)?;
        Ok(())
    }
}

fn validate_schedule<E>(
    snapshot: SyncScheduleSnapshot,
    workspace_id: Uuid,
    policy_id: Uuid,
    expected_state: SyncScheduleState,
) -> Result<SyncScheduleSnapshot, SyncScheduleRepositoryError<E>> {
    if snapshot.workspace_id != workspace_id
        || snapshot.policy_id != policy_id
        || snapshot.state != expected_state
        || (snapshot.state == SyncScheduleState::Enabled) != snapshot.next_run_at.is_some()
        || snapshot.updated_at < snapshot.created_at
    {
        return Err(SyncScheduleRepositoryError::RepositoryInvariant);
    }
    Ok(snapshot)
}

fn schedule_error<E>(error: SyncScheduleRepositoryError<E>) -> ApplicationError
where
    E: IntoApplicationError,
{
    match error {
        SyncScheduleRepositoryError::PolicyNotFound => ApplicationError::new(
            ApplicationErrorCode::SyncPolicyNotFound,
            "synchronization policy was not found",
        ),
        SyncScheduleRepositoryError::ScheduleNotFound => ApplicationError::new(
            ApplicationErrorCode::SyncScheduleNotFound,
            "synchronization schedule was not found",
        ),
        SyncScheduleRepositoryError::OccurrenceNotFound
        | SyncScheduleRepositoryError::RepositoryInvariant => ApplicationError::new(
            ApplicationErrorCode::Internal,
            "synchronization schedule operation failed",
        ),
        SyncScheduleRepositoryError::Repository(error) => error.into_application_error(),
    }
}

fn validate_pending_occurrence<E>(
    snapshot: SyncScheduleOccurrenceSnapshot,
) -> Result<SyncScheduleOccurrenceSnapshot, SyncScheduleRepositoryError<E>> {
    if snapshot.status != SyncScheduleOccurrenceStatus::Pending
        || snapshot.completed_at.is_some()
        || snapshot.created_at < snapshot.scheduled_for
    {
        return Err(SyncScheduleRepositoryError::RepositoryInvariant);
    }
    Ok(snapshot)
}

impl<E> From<E> for SyncScheduleRepositoryError<E> {
    fn from(error: E) -> Self {
        Self::Repository(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_recurrence_coalesces_missed_slots() {
        let recurrence = SyncScheduleRecurrence::every(300).expect("five minutes is valid");
        let enabled_at = timestamp(1_000);
        let first = recurrence
            .first_after(enabled_at)
            .expect("first occurrence is representable");

        assert_eq!(first, timestamp(1_300));
        assert_eq!(
            recurrence
                .next_after(first, timestamp(1_300))
                .expect("on-time occurrence advances once"),
            timestamp(1_600)
        );
        assert_eq!(
            recurrence
                .next_after(first, timestamp(2_001))
                .expect("missed occurrences coalesce"),
            timestamp(2_200)
        );
        assert_eq!(
            recurrence
                .next_after(first, timestamp(1_200))
                .expect("future occurrence remains unchanged"),
            first
        );
    }

    #[test]
    fn fixed_recurrence_rejects_unbounded_intervals() {
        assert_eq!(
            SyncScheduleRecurrence::every(MINIMUM_SYNC_INTERVAL_SECONDS - 1),
            Err(SyncScheduleError::InvalidInterval)
        );
        assert_eq!(
            SyncScheduleRecurrence::every(MAXIMUM_SYNC_INTERVAL_SECONDS + 1),
            Err(SyncScheduleError::InvalidInterval)
        );
    }

    fn timestamp(second: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(second).expect("test timestamp is valid")
    }
}
