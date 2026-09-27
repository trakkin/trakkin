use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use super::task_manager::{
    ClaimNextTask, ClaimedTask, CompleteTask, TaskCompletion, TaskDefinition, TaskKind,
    TaskManager, TaskRepository, TaskSnapshot, TaskStatus,
};

pub type TaskHandlerFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<TaskHandlerOutcome, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskHandlerOutcome {
    Complete(TaskCompletion),
    Terminalized,
    RetryQueued,
    Abandoned,
}

pub trait TaskHandler: Send + Sync {
    type Error;

    fn accepted_kinds(&self) -> &'static [TaskKind];

    fn execute<'a>(
        &'a self,
        task: &'a ClaimedTask,
        completed_at: OffsetDateTime,
    ) -> TaskHandlerFuture<'a, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchNextTask {
    pub owner_id: Uuid,
    pub attempt_id: Uuid,
    pub claimed_at: OffsetDateTime,
    pub completed_at: OffsetDateTime,
}

pub struct TaskDispatcher<R, H> {
    task_manager: TaskManager<R>,
    handler: H,
}

impl<R, H> TaskDispatcher<R, H>
where
    R: TaskRepository,
    H: TaskHandler,
{
    pub fn new(task_manager: TaskManager<R>, handler: H) -> Self {
        Self {
            task_manager,
            handler,
        }
    }

    pub fn accepted_kinds(&self) -> &'static [TaskKind] {
        self.handler.accepted_kinds()
    }

    pub async fn dispatch_next(
        &self,
        command: DispatchNextTask,
    ) -> Result<Option<TaskSnapshot>, TaskDispatcherError<R::Error, H::Error>>
    where
        H::Error: std::fmt::Display,
    {
        let Some(claimed) = self
            .task_manager
            .claim_next(ClaimNextTask {
                owner_id: command.owner_id,
                attempt_id: command.attempt_id,
                accepted_kinds: self.handler.accepted_kinds().to_vec(),
                claimed_at: command.claimed_at,
            })
            .await
            .map_err(TaskDispatcherError::Repository)?
        else {
            return Ok(None);
        };
        let task_kind = task_kind(&claimed.task.definition);
        tracing::debug!(
            task.id = %claimed.task.id,
            task.attempt.id = %claimed.attempt_id,
            task.attempt.ordinal = claimed.attempt_ordinal,
            task.kind = task_kind,
            workspace.id = ?claimed.task.workspace_id,
            connection.id = %claimed.task.connection_id,
            "task claimed"
        );
        let outcome = match self.handler.execute(&claimed, command.completed_at).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(
                    task.id = %claimed.task.id,
                    task.attempt.id = %claimed.attempt_id,
                    task.attempt.ordinal = claimed.attempt_ordinal,
                    task.kind = task_kind,
                    task.attempt.outcome = "handler_failed",
                    workspace.id = ?claimed.task.workspace_id,
                    connection.id = %claimed.task.connection_id,
                    failure.stage = "handler",
                    error.message = %error,
                    "task dispatch failed"
                );
                return Err(TaskDispatcherError::Handler(error));
            }
        };
        let completed = match outcome {
            TaskHandlerOutcome::Complete(completion) => self
                .task_manager
                .complete(CompleteTask {
                    task_id: claimed.task.id,
                    attempt_id: claimed.attempt_id,
                    owner_id: claimed.owner_id,
                    completion,
                    completed_at: command.completed_at,
                })
                .await
                .map_err(TaskDispatcherError::Repository)?,
            TaskHandlerOutcome::Terminalized => {
                let task = self
                    .task_manager
                    .get(claimed.task.id)
                    .await
                    .map_err(TaskDispatcherError::Repository)?
                    .ok_or(TaskDispatcherError::TerminalTaskMissing)?;
                if !task.status.terminal() {
                    return Err(TaskDispatcherError::TerminalTaskNotCompleted(task.status));
                }
                task
            }
            TaskHandlerOutcome::RetryQueued => {
                let task = self
                    .task_manager
                    .get(claimed.task.id)
                    .await
                    .map_err(TaskDispatcherError::Repository)?
                    .ok_or(TaskDispatcherError::TerminalTaskMissing)?;
                if task.status != TaskStatus::Queued {
                    return Err(TaskDispatcherError::RetryTaskNotQueued(task.status));
                }
                task
            }
            TaskHandlerOutcome::Abandoned => return Ok(Some(claimed.task)),
        };
        if completed.status.terminal() {
            if matches!(
                completed.status,
                TaskStatus::Failed | TaskStatus::Interrupted
            ) {
                let (failure_code, failure_message, retryable) = completed.failure.as_ref().map_or(
                    ("task_interrupted", "task interrupted", false),
                    |failure| {
                        (
                            failure.code.as_str(),
                            failure.message.as_str(),
                            failure.retryable,
                        )
                    },
                );
                tracing::warn!(
                    task.id = %completed.id,
                    task.attempt.id = %claimed.attempt_id,
                    task.attempt.ordinal = claimed.attempt_ordinal,
                    task.kind = task_kind,
                    task.status = ?completed.status,
                    workspace.id = ?claimed.task.workspace_id,
                    connection.id = %claimed.task.connection_id,
                    failure.code = failure_code,
                    failure.message = failure_message,
                    failure.retryable = retryable,
                    "task completed unsuccessfully"
                );
            } else {
                tracing::debug!(
                    task.id = %completed.id,
                    task.attempt.id = %claimed.attempt_id,
                    task.attempt.ordinal = claimed.attempt_ordinal,
                    task.kind = task_kind,
                    task.status = ?completed.status,
                    workspace.id = ?claimed.task.workspace_id,
                    connection.id = %claimed.task.connection_id,
                    "task completed"
                );
            }
        } else {
            tracing::debug!(
                task.id = %completed.id,
                task.attempt.id = %claimed.attempt_id,
                task.attempt.ordinal = claimed.attempt_ordinal,
                task.kind = task_kind,
                task.status = ?completed.status,
                task.attempt.outcome = "retry_queued",
                workspace.id = ?claimed.task.workspace_id,
                connection.id = %claimed.task.connection_id,
                "task attempt completed"
            );
        }
        Ok(Some(completed))
    }
}

fn task_kind(definition: &TaskDefinition) -> &'static str {
    match definition {
        TaskDefinition::SourceDiscovery { .. } => "source_discovery",
        TaskDefinition::Read {
            stream: super::task_manager::SourceStream::Catalog,
            ..
        } => "catalog_read",
        TaskDefinition::Read {
            stream: super::task_manager::SourceStream::State,
            ..
        } => "state_read",
        TaskDefinition::Reset {
            stream: super::task_manager::SourceStream::Catalog,
            ..
        } => "catalog_reset",
        TaskDefinition::Reset {
            stream: super::task_manager::SourceStream::State,
            ..
        } => "state_reset",
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TaskDispatcherError<RepositoryError, HandlerError> {
    #[error("task repository operation failed: {0}")]
    Repository(#[source] RepositoryError),
    #[error("task handler failed: {0}")]
    Handler(#[source] HandlerError),
    #[error("terminal task no longer exists")]
    TerminalTaskMissing,
    #[error("task handler returned without completing task in status {0:?}")]
    TerminalTaskNotCompleted(TaskStatus),
    #[error("task handler returned a retry without queuing task in status {0:?}")]
    RetryTaskNotQueued(TaskStatus),
}
