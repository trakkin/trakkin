use time::OffsetDateTime;
use uuid::Uuid;

use crate::application::{
    adapter_runtime::{
        AdapterCallError, AdapterRetirement, AdapterSession, AdapterTargetedStateReadOutcome,
        ReadAdapterTargetedState,
    },
    connection::ConnectionSessionRegistry,
    sync_action_reconciliation::{
        PrepareSyncActionReconciliation, RecordSyncActionReconciliation,
        SyncActionReconciliationDisposition, SyncActionReconciliationError,
        SyncActionReconciliationRepository, SyncActionReconciliationService,
    },
    task_dispatcher::{TaskHandler, TaskHandlerFuture, TaskHandlerOutcome},
    task_manager::{ClaimedTask, TaskCompletion, TaskDefinition, TaskFailure, TaskKind},
};

const RECONCILIATION_TASK_KINDS: [TaskKind; 1] = [TaskKind::SyncActionReconciliation];

#[trait_variant::make(Send)]
pub trait SyncActionReconciliationAdapterSession {
    type Error;

    async fn read_sync_action_reconciliation(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> Result<AdapterTargetedStateReadOutcome, Self::Error>;

    async fn retire_sync_action_reconciliation(self) -> Result<AdapterRetirement, Self::Error>;
}

impl<S> SyncActionReconciliationAdapterSession for S
where
    S: AdapterSession,
{
    type Error = S::Error;

    async fn read_sync_action_reconciliation(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> Result<AdapterTargetedStateReadOutcome, Self::Error> {
        self.read_targeted_state(command).await
    }

    async fn retire_sync_action_reconciliation(self) -> Result<AdapterRetirement, Self::Error> {
        self.retire().await
    }
}

pub struct SyncActionReconciliationTaskHandler<R, G, S> {
    reconciliations: SyncActionReconciliationService<R>,
    sessions: G,
    session: std::marker::PhantomData<fn() -> S>,
}

impl<R, G, S> SyncActionReconciliationTaskHandler<R, G, S>
where
    R: SyncActionReconciliationRepository,
{
    pub fn new(repository: R, sessions: G) -> Self {
        Self {
            reconciliations: SyncActionReconciliationService::new(repository),
            sessions,
            session: std::marker::PhantomData,
        }
    }
}

impl<R, G, S> TaskHandler for SyncActionReconciliationTaskHandler<R, G, S>
where
    R: SyncActionReconciliationRepository,
    R::Error: Send,
    G: ConnectionSessionRegistry<S>,
    S: SyncActionReconciliationAdapterSession,
    S::Error: Send,
{
    type Error = SyncActionReconciliationDispatchError<R::Error, S::Error>;

    fn accepted_kinds(&self) -> &'static [TaskKind] {
        &RECONCILIATION_TASK_KINDS
    }

    fn execute<'a>(
        &'a self,
        claimed: &'a ClaimedTask,
        completed_at: OffsetDateTime,
    ) -> TaskHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            if !matches!(
                claimed.task.definition,
                TaskDefinition::SyncActionReconciliation { .. }
            ) {
                return Ok(failed_completion(
                    "invalid_sync_action_reconciliation_task",
                    false,
                ));
            }
            let prepared = self
                .reconciliations
                .prepare(PrepareSyncActionReconciliation {
                    claimed: claimed.clone(),
                    operation_id: claimed.attempt_id,
                    prepared_at: completed_at,
                })
                .await
                .map_err(SyncActionReconciliationDispatchError::Reconciliation)?;
            let outcome = match self.sessions.remove(claimed.task.connection_id) {
                Some(mut session) => match session
                    .read_sync_action_reconciliation(ReadAdapterTargetedState {
                        operation_id: prepared.request.operation_id.as_bytes().to_vec(),
                        source_key: prepared.request.source_key.clone(),
                        subject: prepared.request.receiver_subject.clone(),
                        fields: vec![prepared.request.provider_field.clone()],
                        maximum_response_bytes: prepared.request.capability.maximum_response_bytes,
                        capability: prepared.request.capability.clone(),
                        reconciliation_idempotency_key: Some(
                            prepared.request.action_id.as_bytes().to_vec(),
                        ),
                    })
                    .await
                {
                    Ok(outcome) => {
                        if self
                            .restore_session(claimed.task.connection_id, session)
                            .await
                        {
                            outcome
                        } else {
                            indeterminate_outcome(
                                "connection_session_collision",
                                "connection session changed during reconciliation",
                            )
                        }
                    }
                    Err(_) => {
                        let _ = session.retire_sync_action_reconciliation().await;
                        indeterminate_outcome(
                            "sync_action_reconciliation_transport_failed",
                            "targeted reconciliation read did not complete",
                        )
                    }
                },
                None => indeterminate_outcome(
                    "connection_session_unavailable",
                    "connection session is unavailable for reconciliation",
                ),
            };
            let settlement = self
                .reconciliations
                .record(RecordSyncActionReconciliation {
                    task_id: prepared.task_id,
                    task_attempt_id: prepared.task_attempt_id,
                    owner_id: prepared.owner_id,
                    outcome,
                    settled_at: completed_at,
                })
                .await
                .map_err(SyncActionReconciliationDispatchError::Reconciliation)?;
            Ok(match settlement.disposition {
                SyncActionReconciliationDisposition::Terminalized => {
                    TaskHandlerOutcome::Terminalized
                }
                SyncActionReconciliationDisposition::RetryQueued => TaskHandlerOutcome::RetryQueued,
            })
        })
    }
}

impl<R, G, S> SyncActionReconciliationTaskHandler<R, G, S>
where
    R: SyncActionReconciliationRepository,
    G: ConnectionSessionRegistry<S>,
    S: SyncActionReconciliationAdapterSession,
{
    async fn restore_session(&self, connection_id: Uuid, session: S) -> bool {
        if let Some(unexpected) = self.sessions.replace(connection_id, session) {
            if let Some(installed) = self.sessions.remove(connection_id) {
                let _ = installed.retire_sync_action_reconciliation().await;
            }
            let _ = unexpected.retire_sync_action_reconciliation().await;
            return false;
        }
        true
    }
}

fn failed_completion(code: &str, retryable: bool) -> TaskHandlerOutcome {
    TaskHandlerOutcome::Complete(TaskCompletion::Failed(TaskFailure::coded(code, retryable)))
}

fn indeterminate_outcome(code: &str, safe_message: &str) -> AdapterTargetedStateReadOutcome {
    AdapterTargetedStateReadOutcome::Indeterminate(AdapterCallError {
        category: crate::application::adapter_runtime::AdapterFailureCategory::Unavailable,
        code: code.to_owned(),
        safe_message: safe_message.to_owned(),
        retryable: true,
        retry_after: None,
        field_problems: Vec::new(),
    })
}

#[derive(Debug)]
pub enum SyncActionReconciliationDispatchError<RepositoryError, AdapterError> {
    Reconciliation(SyncActionReconciliationError<RepositoryError>),
    Adapter(AdapterError),
}
