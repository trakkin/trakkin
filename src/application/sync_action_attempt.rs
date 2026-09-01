use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::acquisition::{StateField, StateSubject};

use super::{
    adapter_runtime::{
        AdapterKey, AdapterSourceMembership, AdapterTargetedStateFieldEffectKind,
        AdapterTargetedStateMembershipEffect, AdapterTargetedStateWriteCapability,
        AdapterTargetedStateWriteCertainty, AdapterTargetedStateWriteOutcome,
        AdapterTargetedStateWriteRetryDisposition, AdapterTargetedStateWriteStatus,
    },
    task_manager::{ClaimedTask, TaskCompletion, TaskDefinition, TaskFailure, TaskStatus},
};

pub type SyncActionAttemptFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, SyncActionAttemptError<E>>> + Send + 'a>>;

pub const MAXIMUM_AUTOMATIC_SYNC_ACTION_ATTEMPTS: i32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncActionAttemptState {
    Prepared,
    Dispatched,
    Settled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncActionRecoveryResult {
    ConfirmedNotApplied,
    Indeterminate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActionWriteRequest {
    pub action_id: Uuid,
    pub preview_action_hash: String,
    pub receiver_source_id: Uuid,
    pub receiver_subject: StateSubject,
    pub semantic_field: String,
    pub provider_field: StateField,
    pub intended_value: i64,
    pub expected_membership: AdapterSourceMembership,
    pub precondition: Option<Vec<u8>>,
    pub allow_create_membership: bool,
    pub idempotency_key: Uuid,
    pub operation_id: Uuid,
    pub source_key: AdapterKey,
    pub capability: AdapterTargetedStateWriteCapability,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncActionAttemptSnapshot {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
    pub request: SyncActionWriteRequest,
    pub state: SyncActionAttemptState,
    pub recovery_result: Option<SyncActionRecoveryResult>,
    pub outcome: Option<AdapterTargetedStateWriteOutcome>,
    pub prepared_at: OffsetDateTime,
    pub dispatched_at: Option<OffsetDateTime>,
    pub settled_at: Option<OffsetDateTime>,
}

impl SyncActionAttemptSnapshot {
    #[must_use]
    pub fn requires_reconciliation(&self) -> bool {
        self.recovery_result == Some(SyncActionRecoveryResult::Indeterminate)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareSyncActionAttempt {
    pub claimed: ClaimedTask,
    pub operation_id: Uuid,
    pub prepared_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailSyncActionClaim {
    pub claimed: ClaimedTask,
    pub failure: TaskFailure,
    pub failed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordSyncActionDispatch {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
    pub dispatched_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SyncActionDispatchTransition {
    Dispatched(SyncActionAttemptSnapshot),
    Cancelled(SyncActionAttemptSnapshot),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordSyncActionOutcome {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
    pub outcome: AdapterTargetedStateWriteOutcome,
    pub completion: TaskCompletion,
    pub settled_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncActionOutcomeDisposition {
    Terminalized,
    RetryQueued,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncActionOutcomeSettlement {
    pub attempt: SyncActionAttemptSnapshot,
    pub disposition: SyncActionOutcomeDisposition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckSyncActionCancellation {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordSyncActionUncertainty {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
    pub reconciliation_task_id: Uuid,
    pub settled_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncActionAttemptError<E> {
    InvalidClaim,
    InvalidOperationId,
    AttemptAlreadyPrepared,
    InvalidTransition,
    CorruptAction,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncActionAttemptRepository: Send + Sync {
    type Error;

    fn prepare_sync_action_attempt(
        &self,
        command: PrepareSyncActionAttempt,
    ) -> SyncActionAttemptFuture<'_, SyncActionAttemptSnapshot, Self::Error>;

    fn fail_sync_action_claim(
        &self,
        command: FailSyncActionClaim,
    ) -> SyncActionAttemptFuture<'_, (), Self::Error>;

    fn record_sync_action_dispatch(
        &self,
        command: RecordSyncActionDispatch,
    ) -> SyncActionAttemptFuture<'_, SyncActionDispatchTransition, Self::Error>;

    fn record_sync_action_outcome(
        &self,
        command: RecordSyncActionOutcome,
    ) -> SyncActionAttemptFuture<'_, SyncActionOutcomeSettlement, Self::Error>;

    fn check_sync_action_cancellation(
        &self,
        command: CheckSyncActionCancellation,
    ) -> SyncActionAttemptFuture<'_, bool, Self::Error>;

    fn record_sync_action_uncertainty(
        &self,
        command: RecordSyncActionUncertainty,
    ) -> SyncActionAttemptFuture<'_, SyncActionAttemptSnapshot, Self::Error>;
}

#[derive(Clone)]
pub struct SyncActionAttemptService<R> {
    repository: R,
}

impl<R> SyncActionAttemptService<R>
where
    R: SyncActionAttemptRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn prepare(
        &self,
        command: PrepareSyncActionAttempt,
    ) -> Result<SyncActionAttemptSnapshot, SyncActionAttemptError<R::Error>> {
        let TaskDefinition::SyncAction {
            action_id,
            receiver_source_id,
        } = command.claimed.task.definition
        else {
            return Err(SyncActionAttemptError::InvalidClaim);
        };
        if command.operation_id == Uuid::nil() {
            return Err(SyncActionAttemptError::InvalidOperationId);
        }
        if command.claimed.task.status != TaskStatus::Running
            || command.claimed.task.id == Uuid::nil()
            || command.claimed.attempt_id == Uuid::nil()
            || command.claimed.owner_id == Uuid::nil()
            || command
                .claimed
                .task
                .started_at
                .is_none_or(|started_at| command.prepared_at < started_at)
        {
            return Err(SyncActionAttemptError::InvalidClaim);
        }
        let expected_task_id = command.claimed.task.id;
        let expected_attempt_id = command.claimed.attempt_id;
        let expected_owner_id = command.claimed.owner_id;
        let expected_operation_id = command.operation_id;
        let expected_prepared_at = command.prepared_at;
        let snapshot = self.repository.prepare_sync_action_attempt(command).await?;
        if snapshot.task_id != expected_task_id
            || snapshot.task_attempt_id != expected_attempt_id
            || snapshot.owner_id != expected_owner_id
            || snapshot.request.action_id != action_id
            || snapshot.request.receiver_source_id != receiver_source_id
            || snapshot.request.idempotency_key != action_id
            || snapshot.request.operation_id != expected_operation_id
            || snapshot.state != SyncActionAttemptState::Prepared
            || snapshot.recovery_result.is_some()
            || snapshot.outcome.is_some()
            || snapshot.prepared_at != expected_prepared_at
            || snapshot.dispatched_at.is_some()
            || snapshot.settled_at.is_some()
        {
            return Err(SyncActionAttemptError::RepositoryInvariant);
        }
        Ok(snapshot)
    }

    pub async fn fail_claim(
        &self,
        command: FailSyncActionClaim,
    ) -> Result<(), SyncActionAttemptError<R::Error>> {
        if !matches!(
            command.claimed.task.definition,
            TaskDefinition::SyncAction { .. }
        ) || command.claimed.task.status != TaskStatus::Running
            || command.claimed.task.id == Uuid::nil()
            || command.claimed.attempt_id == Uuid::nil()
            || command.claimed.owner_id == Uuid::nil()
            || command.failure.code.is_empty()
            || command
                .claimed
                .task
                .started_at
                .is_none_or(|started_at| command.failed_at < started_at)
        {
            return Err(SyncActionAttemptError::InvalidClaim);
        }
        self.repository.fail_sync_action_claim(command).await
    }

    pub async fn record_dispatch(
        &self,
        command: RecordSyncActionDispatch,
    ) -> Result<SyncActionDispatchTransition, SyncActionAttemptError<R::Error>> {
        if command.task_id == Uuid::nil()
            || command.task_attempt_id == Uuid::nil()
            || command.owner_id == Uuid::nil()
        {
            return Err(SyncActionAttemptError::InvalidClaim);
        }
        let expected_task_id = command.task_id;
        let expected_attempt_id = command.task_attempt_id;
        let expected_owner_id = command.owner_id;
        let expected_dispatched_at = command.dispatched_at;
        let transition = self.repository.record_sync_action_dispatch(command).await?;
        let snapshot = match &transition {
            SyncActionDispatchTransition::Dispatched(snapshot)
                if snapshot.state == SyncActionAttemptState::Dispatched
                    && snapshot.recovery_result.is_none()
                    && snapshot.dispatched_at == Some(expected_dispatched_at)
                    && snapshot.settled_at.is_none() =>
            {
                snapshot
            }
            SyncActionDispatchTransition::Cancelled(snapshot)
                if snapshot.state == SyncActionAttemptState::Settled
                    && snapshot.recovery_result
                        == Some(SyncActionRecoveryResult::ConfirmedNotApplied)
                    && snapshot.dispatched_at.is_none()
                    && snapshot.settled_at == Some(expected_dispatched_at) =>
            {
                snapshot
            }
            _ => return Err(SyncActionAttemptError::RepositoryInvariant),
        };
        if snapshot.task_id != expected_task_id
            || snapshot.task_attempt_id != expected_attempt_id
            || snapshot.owner_id != expected_owner_id
            || snapshot.outcome.is_some()
        {
            return Err(SyncActionAttemptError::RepositoryInvariant);
        }
        Ok(transition)
    }

    pub async fn record_outcome(
        &self,
        command: RecordSyncActionOutcome,
    ) -> Result<SyncActionOutcomeSettlement, SyncActionAttemptError<R::Error>> {
        if command.task_id == Uuid::nil()
            || command.task_attempt_id == Uuid::nil()
            || command.owner_id == Uuid::nil()
            || !valid_outcome(&command.outcome)
            || !completion_matches_outcome(&command.completion, &command.outcome)
        {
            return Err(SyncActionAttemptError::InvalidTransition);
        }
        let expected_task_id = command.task_id;
        let expected_attempt_id = command.task_attempt_id;
        let expected_owner_id = command.owner_id;
        let expected_outcome = command.outcome.clone();
        let expected_settled_at = command.settled_at;
        let settlement = self.repository.record_sync_action_outcome(command).await?;
        let snapshot = &settlement.attempt;
        if snapshot.task_id != expected_task_id
            || snapshot.task_attempt_id != expected_attempt_id
            || snapshot.owner_id != expected_owner_id
            || snapshot.state != SyncActionAttemptState::Settled
            || snapshot.recovery_result.is_some()
            || snapshot.outcome.as_ref() != Some(&expected_outcome)
            || snapshot.dispatched_at.is_none()
            || snapshot.settled_at != Some(expected_settled_at)
        {
            return Err(SyncActionAttemptError::RepositoryInvariant);
        }
        if settlement.disposition == SyncActionOutcomeDisposition::RetryQueued
            && !matches!(
                (expected_outcome.status, expected_outcome.retry_disposition,),
                (
                    AdapterTargetedStateWriteStatus::Failed,
                    AdapterTargetedStateWriteRetryDisposition::SafeSameKey,
                )
            )
        {
            return Err(SyncActionAttemptError::RepositoryInvariant);
        }
        Ok(settlement)
    }

    pub async fn cancellation_requested(
        &self,
        command: CheckSyncActionCancellation,
    ) -> Result<bool, SyncActionAttemptError<R::Error>> {
        if command.task_id == Uuid::nil()
            || command.task_attempt_id == Uuid::nil()
            || command.owner_id == Uuid::nil()
        {
            return Err(SyncActionAttemptError::InvalidClaim);
        }
        self.repository
            .check_sync_action_cancellation(command)
            .await
    }

    pub async fn record_uncertainty(
        &self,
        command: RecordSyncActionUncertainty,
    ) -> Result<SyncActionAttemptSnapshot, SyncActionAttemptError<R::Error>> {
        if command.task_id == Uuid::nil()
            || command.task_attempt_id == Uuid::nil()
            || command.owner_id == Uuid::nil()
            || command.reconciliation_task_id == Uuid::nil()
        {
            return Err(SyncActionAttemptError::InvalidClaim);
        }
        let expected_task_id = command.task_id;
        let expected_attempt_id = command.task_attempt_id;
        let expected_owner_id = command.owner_id;
        let expected_settled_at = command.settled_at;
        let snapshot = self
            .repository
            .record_sync_action_uncertainty(command)
            .await?;
        if snapshot.task_id != expected_task_id
            || snapshot.task_attempt_id != expected_attempt_id
            || snapshot.owner_id != expected_owner_id
            || snapshot.state != SyncActionAttemptState::Settled
            || snapshot.recovery_result != Some(SyncActionRecoveryResult::Indeterminate)
            || snapshot.outcome.is_some()
            || snapshot.dispatched_at.is_none()
            || snapshot.settled_at != Some(expected_settled_at)
        {
            return Err(SyncActionAttemptError::RepositoryInvariant);
        }
        Ok(snapshot)
    }
}

#[must_use]
pub fn sync_action_task_completion(outcome: &AdapterTargetedStateWriteOutcome) -> TaskCompletion {
    match outcome.status {
        AdapterTargetedStateWriteStatus::Applied | AdapterTargetedStateWriteStatus::NoOp => {
            TaskCompletion::Succeeded
        }
        AdapterTargetedStateWriteStatus::Rejected
        | AdapterTargetedStateWriteStatus::Failed
        | AdapterTargetedStateWriteStatus::Indeterminate => {
            TaskCompletion::Failed(outcome.error.as_ref().map_or_else(
                || TaskFailure::coded("sync_action_write_failed", false),
                TaskFailure::from_adapter,
            ))
        }
    }
}

fn completion_matches_outcome(
    completion: &TaskCompletion,
    outcome: &AdapterTargetedStateWriteOutcome,
) -> bool {
    match (completion, sync_action_task_completion(outcome)) {
        (TaskCompletion::Succeeded, TaskCompletion::Succeeded)
        | (TaskCompletion::Cancelled, TaskCompletion::Cancelled) => true,
        (TaskCompletion::Failed(actual), TaskCompletion::Failed(expected)) => {
            actual.code == expected.code
                && actual.message == expected.message
                && actual.retryable == expected.retryable
                && actual.retry_after == expected.retry_after
                && actual.diagnostic_id == expected.diagnostic_id
        }
        _ => false,
    }
}

fn valid_outcome(outcome: &AdapterTargetedStateWriteOutcome) -> bool {
    let valid_tuple = matches!(
        (outcome.status, outcome.certainty, outcome.retry_disposition,),
        (
            AdapterTargetedStateWriteStatus::Applied,
            AdapterTargetedStateWriteCertainty::ConfirmedApplied,
            AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        ) | (
            AdapterTargetedStateWriteStatus::NoOp | AdapterTargetedStateWriteStatus::Rejected,
            AdapterTargetedStateWriteCertainty::ConfirmedNotApplied,
            AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        ) | (
            AdapterTargetedStateWriteStatus::Failed,
            AdapterTargetedStateWriteCertainty::ConfirmedNotApplied,
            AdapterTargetedStateWriteRetryDisposition::SafeSameKey
                | AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        ) | (
            AdapterTargetedStateWriteStatus::Indeterminate,
            AdapterTargetedStateWriteCertainty::Unknown,
            AdapterTargetedStateWriteRetryDisposition::ReconcileFirst,
        )
    );
    let valid_membership = match outcome.status {
        AdapterTargetedStateWriteStatus::Applied => matches!(
            outcome.membership_effect,
            AdapterTargetedStateMembershipEffect::Unchanged
                | AdapterTargetedStateMembershipEffect::Created
        ),
        AdapterTargetedStateWriteStatus::NoOp
        | AdapterTargetedStateWriteStatus::Rejected
        | AdapterTargetedStateWriteStatus::Failed => {
            outcome.membership_effect == AdapterTargetedStateMembershipEffect::Unchanged
        }
        AdapterTargetedStateWriteStatus::Indeterminate => {
            outcome.membership_effect == AdapterTargetedStateMembershipEffect::Unknown
        }
    };
    let valid_effects = outcome
        .field_effects
        .iter()
        .all(|effect| match outcome.status {
            AdapterTargetedStateWriteStatus::Applied => {
                !matches!(effect.effect, AdapterTargetedStateFieldEffectKind::Unknown)
            }
            AdapterTargetedStateWriteStatus::NoOp => {
                effect.effect == AdapterTargetedStateFieldEffectKind::Unchanged
            }
            AdapterTargetedStateWriteStatus::Rejected | AdapterTargetedStateWriteStatus::Failed => {
                false
            }
            AdapterTargetedStateWriteStatus::Indeterminate => {
                effect.effect == AdapterTargetedStateFieldEffectKind::Unknown
            }
        });
    let valid_error = match outcome.status {
        AdapterTargetedStateWriteStatus::Applied | AdapterTargetedStateWriteStatus::NoOp => {
            outcome.error.is_none()
        }
        AdapterTargetedStateWriteStatus::Rejected
        | AdapterTargetedStateWriteStatus::Failed
        | AdapterTargetedStateWriteStatus::Indeterminate => outcome.error.is_some(),
    };
    let valid_revision = match outcome.status {
        AdapterTargetedStateWriteStatus::Applied | AdapterTargetedStateWriteStatus::NoOp => {
            !outcome.provider_revision.is_empty()
        }
        AdapterTargetedStateWriteStatus::Rejected
        | AdapterTargetedStateWriteStatus::Failed
        | AdapterTargetedStateWriteStatus::Indeterminate => outcome.provider_revision.is_empty(),
    };
    valid_tuple && valid_membership && valid_effects && valid_error && valid_revision
}
