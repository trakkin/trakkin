use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::acquisition::{StateField, StateSubject, StateValue};

use super::{
    adapter_runtime::{
        AdapterKey, AdapterSourceMembership, AdapterStatePresence,
        AdapterTargetedStateReadCapability, AdapterTargetedStateReadOutcome,
    },
    task_manager::{ClaimedTask, TaskDefinition, TaskStatus},
};

pub type SyncActionReconciliationFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, SyncActionReconciliationError<E>>> + Send + 'a>>;

pub const MAXIMUM_AUTOMATIC_SYNC_ACTION_RECONCILIATION_ATTEMPTS: i32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncActionReconciliationState {
    Prepared,
    Settled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncActionReconciliationResult {
    ConfirmedApplied,
    ConfirmedNotApplied,
    StateMatchesCausationUnknown,
    Unresolved,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActionReconciliationRequest {
    pub action_id: Uuid,
    pub receiver_source_id: Uuid,
    pub write_task_attempt_id: Uuid,
    pub source_key: AdapterKey,
    pub receiver_subject: StateSubject,
    pub provider_field: StateField,
    pub intended_value: i64,
    pub before_membership: AdapterSourceMembership,
    pub before_presence: AdapterStatePresence,
    pub before_value: Option<i64>,
    pub before_provider_revision: Vec<u8>,
    pub operation_id: Uuid,
    pub capability: AdapterTargetedStateReadCapability,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActionReconciliationSnapshot {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
    pub request: SyncActionReconciliationRequest,
    pub state: SyncActionReconciliationState,
    pub outcome: Option<AdapterTargetedStateReadOutcome>,
    pub result: Option<SyncActionReconciliationResult>,
    pub prepared_at: OffsetDateTime,
    pub settled_at: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncActionReconciliationDisposition {
    Terminalized,
    RetryQueued,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActionReconciliationSettlement {
    pub attempt: SyncActionReconciliationSnapshot,
    pub disposition: SyncActionReconciliationDisposition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareSyncActionReconciliation {
    pub claimed: ClaimedTask,
    pub operation_id: Uuid,
    pub prepared_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordSyncActionReconciliation {
    pub task_id: Uuid,
    pub task_attempt_id: Uuid,
    pub owner_id: Uuid,
    pub outcome: AdapterTargetedStateReadOutcome,
    pub settled_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncActionReconciliationError<E> {
    InvalidClaim,
    InvalidOperationId,
    AttemptAlreadyPrepared,
    InvalidTransition,
    CorruptAction,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncActionReconciliationRepository: Send + Sync {
    type Error;

    fn prepare_sync_action_reconciliation(
        &self,
        command: PrepareSyncActionReconciliation,
    ) -> SyncActionReconciliationFuture<'_, SyncActionReconciliationSnapshot, Self::Error>;

    fn record_sync_action_reconciliation(
        &self,
        command: RecordSyncActionReconciliation,
    ) -> SyncActionReconciliationFuture<'_, SyncActionReconciliationSettlement, Self::Error>;
}

#[derive(Clone)]
pub struct SyncActionReconciliationService<R> {
    repository: R,
}

impl<R> SyncActionReconciliationService<R>
where
    R: SyncActionReconciliationRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn prepare(
        &self,
        command: PrepareSyncActionReconciliation,
    ) -> Result<SyncActionReconciliationSnapshot, SyncActionReconciliationError<R::Error>> {
        let TaskDefinition::SyncActionReconciliation {
            action_id,
            receiver_source_id,
            write_task_attempt_id,
        } = command.claimed.task.definition
        else {
            return Err(SyncActionReconciliationError::InvalidClaim);
        };
        if command.operation_id == Uuid::nil()
            || command.claimed.task.status != TaskStatus::Running
            || command.claimed.task.id == Uuid::nil()
            || command.claimed.attempt_id == Uuid::nil()
            || command.claimed.owner_id == Uuid::nil()
            || command
                .claimed
                .task
                .started_at
                .is_none_or(|started_at| command.prepared_at < started_at)
        {
            return Err(SyncActionReconciliationError::InvalidClaim);
        }
        let expected_task_id = command.claimed.task.id;
        let expected_attempt_id = command.claimed.attempt_id;
        let expected_owner_id = command.claimed.owner_id;
        let expected_operation_id = command.operation_id;
        let expected_prepared_at = command.prepared_at;
        let snapshot = self
            .repository
            .prepare_sync_action_reconciliation(command)
            .await?;
        if snapshot.task_id != expected_task_id
            || snapshot.task_attempt_id != expected_attempt_id
            || snapshot.owner_id != expected_owner_id
            || snapshot.request.action_id != action_id
            || snapshot.request.receiver_source_id != receiver_source_id
            || snapshot.request.write_task_attempt_id != write_task_attempt_id
            || snapshot.request.operation_id != expected_operation_id
            || snapshot.request.before_provider_revision.is_empty()
            || snapshot.request.capability.maximum_fields < 1
            || snapshot.request.capability.maximum_response_bytes == 0
            || snapshot.state != SyncActionReconciliationState::Prepared
            || snapshot.outcome.is_some()
            || snapshot.result.is_some()
            || snapshot.prepared_at != expected_prepared_at
            || snapshot.settled_at.is_some()
        {
            return Err(SyncActionReconciliationError::RepositoryInvariant);
        }
        Ok(snapshot)
    }

    pub async fn record(
        &self,
        command: RecordSyncActionReconciliation,
    ) -> Result<SyncActionReconciliationSettlement, SyncActionReconciliationError<R::Error>> {
        if command.task_id == Uuid::nil()
            || command.task_attempt_id == Uuid::nil()
            || command.owner_id == Uuid::nil()
        {
            return Err(SyncActionReconciliationError::InvalidClaim);
        }
        let expected_task_id = command.task_id;
        let expected_attempt_id = command.task_attempt_id;
        let expected_owner_id = command.owner_id;
        let expected_outcome = command.outcome.clone();
        let expected_settled_at = command.settled_at;
        let settlement = self
            .repository
            .record_sync_action_reconciliation(command)
            .await?;
        let snapshot = &settlement.attempt;
        let expected_result = classify_reconciliation(&snapshot.request, &expected_outcome)?;
        if snapshot.task_id != expected_task_id
            || snapshot.task_attempt_id != expected_attempt_id
            || snapshot.owner_id != expected_owner_id
            || snapshot.state != SyncActionReconciliationState::Settled
            || snapshot.outcome.as_ref() != Some(&expected_outcome)
            || snapshot.result != Some(expected_result)
            || snapshot.settled_at != Some(expected_settled_at)
        {
            return Err(SyncActionReconciliationError::RepositoryInvariant);
        }
        if settlement.disposition == SyncActionReconciliationDisposition::RetryQueued
            && !matches!(
                expected_outcome,
                AdapterTargetedStateReadOutcome::Indeterminate(ref error) if error.retryable
            )
        {
            return Err(SyncActionReconciliationError::RepositoryInvariant);
        }
        Ok(settlement)
    }
}

pub fn classify_reconciliation<E>(
    request: &SyncActionReconciliationRequest,
    outcome: &AdapterTargetedStateReadOutcome,
) -> Result<SyncActionReconciliationResult, SyncActionReconciliationError<E>> {
    let AdapterTargetedStateReadOutcome::Matched(observed) = outcome else {
        return Ok(SyncActionReconciliationResult::Unresolved);
    };
    if observed.provider_revision.is_empty()
        || observed.fields.len() != 1
        || observed.fields[0].field != request.provider_field
    {
        return Err(SyncActionReconciliationError::InvalidTransition);
    }
    if let Some(causation) = &observed.write_causation
        && (causation.idempotency_key != request.action_id.as_bytes()
            || causation.receipt.is_empty()
            || causation.provider_revision != observed.provider_revision)
    {
        return Err(SyncActionReconciliationError::InvalidTransition);
    }
    let field = &observed.fields[0];
    let observed_value = match &field.value {
        Some(StateValue::Integer(value)) => Some(*value),
        None => None,
        Some(_) => return Err(SyncActionReconciliationError::InvalidTransition),
    };
    let intended_matches = observed.membership == AdapterSourceMembership::Present
        && field.presence == AdapterStatePresence::Present
        && observed_value == Some(request.intended_value);
    if intended_matches && observed.write_causation.is_some() {
        return Ok(SyncActionReconciliationResult::ConfirmedApplied);
    }
    let before_matches = observed.membership == request.before_membership
        && field.presence == request.before_presence
        && observed_value == request.before_value
        && observed.provider_revision == request.before_provider_revision;
    if before_matches && observed.write_causation.is_none() {
        return Ok(SyncActionReconciliationResult::ConfirmedNotApplied);
    }
    if intended_matches {
        return Ok(SyncActionReconciliationResult::StateMatchesCausationUnknown);
    }
    Ok(SyncActionReconciliationResult::Unresolved)
}
