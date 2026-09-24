use std::time::Duration;

use time::OffsetDateTime;
use tokio::time::{MissedTickBehavior, interval};
use uuid::Uuid;

use crate::application::{
    adapter_runtime::{
        AdapterRetirement, AdapterSession, AdapterTargetedStateWriteIntent,
        AdapterTargetedStateWriteOperation, WriteAdapterTargetedState,
    },
    connection::ConnectionSessionRegistry,
    sync_action_attempt::{
        CheckSyncActionCancellation, FailSyncActionClaim, PrepareSyncActionAttempt,
        RecordSyncActionDispatch, RecordSyncActionOutcome, RecordSyncActionUncertainty,
        SyncActionAttemptError, SyncActionAttemptRepository, SyncActionAttemptService,
        SyncActionDispatchTransition, SyncActionOutcomeDisposition, sync_action_task_completion,
    },
    task_dispatcher::{TaskHandler, TaskHandlerFuture, TaskHandlerOutcome},
    task_manager::{ClaimedTask, TaskCompletion, TaskDefinition, TaskFailure, TaskKind},
};
use crate::domain::acquisition::StateValue;

const SYNC_ACTION_TASK_KINDS: [TaskKind; 1] = [TaskKind::SyncAction];
const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[trait_variant::make(Send)]
pub trait SyncActionAdapterSession {
    type Error;

    async fn write_sync_action(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> Result<crate::application::adapter_runtime::AdapterTargetedStateWriteOutcome, Self::Error>;

    async fn cancel_sync_action(&mut self, operation_id: Vec<u8>) -> Result<(), Self::Error>;

    async fn retire_sync_action(self) -> Result<AdapterRetirement, Self::Error>;
}

impl<S> SyncActionAdapterSession for S
where
    S: AdapterSession,
{
    type Error = S::Error;

    async fn write_sync_action(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> Result<crate::application::adapter_runtime::AdapterTargetedStateWriteOutcome, Self::Error>
    {
        self.write_targeted_state(command).await
    }

    async fn cancel_sync_action(&mut self, operation_id: Vec<u8>) -> Result<(), Self::Error> {
        self.cancel_operation(operation_id).await
    }

    async fn retire_sync_action(self) -> Result<AdapterRetirement, Self::Error> {
        self.retire().await
    }
}

pub struct SyncActionTaskHandler<R, G, S> {
    attempts: SyncActionAttemptService<R>,
    sessions: G,
    cancellation_poll_interval: Duration,
    session: std::marker::PhantomData<fn() -> S>,
}

impl<R, G, S> SyncActionTaskHandler<R, G, S>
where
    R: SyncActionAttemptRepository,
{
    pub fn new(repository: R, sessions: G) -> Self {
        Self {
            attempts: SyncActionAttemptService::new(repository),
            sessions,
            cancellation_poll_interval: CANCELLATION_POLL_INTERVAL,
            session: std::marker::PhantomData,
        }
    }

    pub fn with_cancellation_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.cancellation_poll_interval = poll_interval;
        self
    }
}

impl<R, G, S> TaskHandler for SyncActionTaskHandler<R, G, S>
where
    R: SyncActionAttemptRepository,
    R::Error: Send,
    G: ConnectionSessionRegistry<S>,
    S: SyncActionAdapterSession,
    S::Error: Send,
{
    type Error = SyncActionDispatchError<R::Error, S::Error>;

    fn accepted_kinds(&self) -> &'static [TaskKind] {
        &SYNC_ACTION_TASK_KINDS
    }

    fn execute<'a>(
        &'a self,
        claimed: &'a ClaimedTask,
        completed_at: OffsetDateTime,
    ) -> TaskHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            if !matches!(claimed.task.definition, TaskDefinition::SyncAction { .. }) {
                return Ok(failed_completion("invalid_sync_action_task", false));
            }
            let Some(mut session) = self.sessions.remove(claimed.task.connection_id) else {
                self.attempts
                    .fail_claim(FailSyncActionClaim {
                        claimed: claimed.clone(),
                        failure: TaskFailure::coded("connection_session_unavailable", true),
                        failed_at: completed_at,
                    })
                    .await
                    .map_err(SyncActionDispatchError::Attempt)?;
                return Ok(TaskHandlerOutcome::Terminalized);
            };
            let operation_id = match generate_uuid() {
                Ok(operation_id) => operation_id,
                Err(_) => {
                    self.restore_session(claimed.task.connection_id, session)
                        .await;
                    return Ok(failed_completion("operation_id_generation_failed", false));
                }
            };
            let reconciliation_task_id = match generate_uuid() {
                Ok(task_id) => task_id,
                Err(_) => {
                    self.restore_session(claimed.task.connection_id, session)
                        .await;
                    return Ok(failed_completion("task_id_generation_failed", false));
                }
            };
            let prepared = match self
                .attempts
                .prepare(PrepareSyncActionAttempt {
                    claimed: claimed.clone(),
                    operation_id,
                    prepared_at: completed_at,
                })
                .await
            {
                Ok(prepared) => prepared,
                Err(error) => {
                    self.restore_session(claimed.task.connection_id, session)
                        .await;
                    return Err(SyncActionDispatchError::Attempt(error));
                }
            };
            let transition = match self
                .attempts
                .record_dispatch(RecordSyncActionDispatch {
                    task_id: prepared.task_id,
                    task_attempt_id: prepared.task_attempt_id,
                    owner_id: prepared.owner_id,
                    dispatched_at: completed_at,
                })
                .await
            {
                Ok(dispatched) => dispatched,
                Err(error) => {
                    self.restore_session(claimed.task.connection_id, session)
                        .await;
                    return Err(SyncActionDispatchError::Attempt(error));
                }
            };
            let dispatched = match transition {
                SyncActionDispatchTransition::Dispatched(dispatched) => dispatched,
                SyncActionDispatchTransition::Cancelled(_) => {
                    self.restore_session(claimed.task.connection_id, session)
                        .await;
                    return Ok(TaskHandlerOutcome::Terminalized);
                }
            };
            let wait_result = {
                let mut cancellation = interval(self.cancellation_poll_interval);
                cancellation.set_missed_tick_behavior(MissedTickBehavior::Delay);
                cancellation.tick().await;
                let write = session.write_sync_action(write_command(&dispatched.request));
                tokio::pin!(write);
                loop {
                    tokio::select! {
                        result = &mut write => break Ok(Some(result)),
                        _ = cancellation.tick() => {
                            match self.attempts
                                .cancellation_requested(CheckSyncActionCancellation {
                                    task_id: dispatched.task_id,
                                    task_attempt_id: dispatched.task_attempt_id,
                                    owner_id: dispatched.owner_id,
                                })
                                .await
                            {
                                Ok(true) => break Ok(None),
                                Ok(false) => {}
                                Err(error) => break Err(error),
                            }
                        }
                    }
                }
            };
            let outcome = match wait_result {
                Ok(Some(Ok(outcome))) => outcome,
                Ok(Some(Err(_))) => {
                    let _ = session.retire_sync_action().await;
                    self.attempts
                        .record_uncertainty(RecordSyncActionUncertainty {
                            task_id: dispatched.task_id,
                            task_attempt_id: dispatched.task_attempt_id,
                            owner_id: dispatched.owner_id,
                            reconciliation_task_id,
                            settled_at: completed_at,
                        })
                        .await
                        .map_err(SyncActionDispatchError::Attempt)?;
                    return Ok(TaskHandlerOutcome::Terminalized);
                }
                Ok(None) => {
                    let cancelled = session
                        .cancel_sync_action(dispatched.request.operation_id.as_bytes().to_vec())
                        .await;
                    if let Err(error) = self
                        .attempts
                        .record_uncertainty(RecordSyncActionUncertainty {
                            task_id: dispatched.task_id,
                            task_attempt_id: dispatched.task_attempt_id,
                            owner_id: dispatched.owner_id,
                            reconciliation_task_id,
                            settled_at: completed_at,
                        })
                        .await
                    {
                        let _ = session.retire_sync_action().await;
                        return Err(SyncActionDispatchError::Attempt(error));
                    }
                    if cancelled.is_ok() {
                        self.restore_session(claimed.task.connection_id, session)
                            .await;
                    } else {
                        let _ = session.retire_sync_action().await;
                    }
                    return Ok(TaskHandlerOutcome::Terminalized);
                }
                Err(error) => {
                    let _ = session.retire_sync_action().await;
                    return Err(SyncActionDispatchError::Attempt(error));
                }
            };
            self.restore_session(claimed.task.connection_id, session)
                .await;
            let settlement = match self
                .attempts
                .record_outcome(RecordSyncActionOutcome {
                    task_id: dispatched.task_id,
                    task_attempt_id: dispatched.task_attempt_id,
                    owner_id: dispatched.owner_id,
                    completion: sync_action_task_completion(&outcome),
                    outcome,
                    settled_at: completed_at,
                })
                .await
            {
                Ok(settlement) => settlement,
                Err(settlement_error) => {
                    if let Err(uncertainty_error) = self
                        .attempts
                        .record_uncertainty(RecordSyncActionUncertainty {
                            task_id: dispatched.task_id,
                            task_attempt_id: dispatched.task_attempt_id,
                            owner_id: dispatched.owner_id,
                            reconciliation_task_id,
                            settled_at: completed_at,
                        })
                        .await
                    {
                        return Err(SyncActionDispatchError::OutcomeSettlement {
                            settlement: settlement_error,
                            uncertainty: uncertainty_error,
                        });
                    }
                    return Ok(TaskHandlerOutcome::Terminalized);
                }
            };
            if settlement.attempt.outcome.is_none() {
                return Err(SyncActionDispatchError::MissingOutcome);
            }
            Ok(match settlement.disposition {
                SyncActionOutcomeDisposition::Terminalized => TaskHandlerOutcome::Terminalized,
                SyncActionOutcomeDisposition::RetryQueued => TaskHandlerOutcome::RetryQueued,
            })
        })
    }
}

impl<R, G, S> SyncActionTaskHandler<R, G, S>
where
    R: SyncActionAttemptRepository,
    G: ConnectionSessionRegistry<S>,
    S: SyncActionAdapterSession,
{
    async fn restore_session(&self, connection_id: Uuid, session: S) {
        if let Some(unexpected) = self.sessions.replace(connection_id, session) {
            if let Some(installed) = self.sessions.remove(connection_id) {
                let _ = installed.retire_sync_action().await;
            }
            let _ = unexpected.retire_sync_action().await;
        }
    }
}

fn write_command(
    request: &crate::application::sync_action_attempt::SyncActionWriteRequest,
) -> WriteAdapterTargetedState {
    WriteAdapterTargetedState {
        operation_id: request.operation_id.as_bytes().to_vec(),
        source_key: request.source_key.clone(),
        subject: request.receiver_subject.clone(),
        idempotency_key: request.idempotency_key.as_bytes().to_vec(),
        expected_membership: request.expected_membership,
        precondition: request.precondition.clone(),
        allow_create_membership: request.allow_create_membership,
        intents: vec![AdapterTargetedStateWriteIntent {
            field: request.provider_field.clone(),
            operation: AdapterTargetedStateWriteOperation::Set(StateValue::Integer(
                request.intended_value,
            )),
        }],
        maximum_receipt_bytes: request.capability.maximum_receipt_bytes,
        maximum_response_bytes: request.capability.maximum_response_bytes,
        capability: request.capability.clone(),
    }
}

fn failed_completion(code: &str, retryable: bool) -> TaskHandlerOutcome {
    TaskHandlerOutcome::Complete(TaskCompletion::Failed(TaskFailure::coded(code, retryable)))
}

fn generate_uuid() -> Result<Uuid, getrandom::Error> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

#[derive(Debug, thiserror::Error)]
pub enum SyncActionDispatchError<AttemptError, AdapterError> {
    #[error("sync action attempt failed: {0}")]
    Attempt(#[source] SyncActionAttemptError<AttemptError>),
    #[error("sync action adapter operation failed: {0}")]
    Adapter(#[source] AdapterError),
    #[error("sync action settlement failed: settlement: {settlement}; uncertainty: {uncertainty}")]
    OutcomeSettlement {
        settlement: SyncActionAttemptError<AttemptError>,
        uncertainty: SyncActionAttemptError<AttemptError>,
    },
    #[error("sync action adapter returned no outcome")]
    MissingOutcome,
}
