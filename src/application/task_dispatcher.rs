use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use super::task_manager::{
    ClaimNextTask, ClaimedTask, CompleteTask, TaskCompletion, TaskKind, TaskManager,
    TaskRepository, TaskSnapshot, TaskStatus,
};

pub type TaskHandlerFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<TaskHandlerOutcome, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskHandlerOutcome {
    Complete(TaskCompletion),
    Terminalized,
    RetryQueued,
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

    pub async fn dispatch_next(
        &self,
        command: DispatchNextTask,
    ) -> Result<Option<TaskSnapshot>, TaskDispatcherError<R::Error, H::Error>> {
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
        let outcome = self
            .handler
            .execute(&claimed, command.completed_at)
            .await
            .map_err(TaskDispatcherError::Handler)?;
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
        };
        Ok(Some(completed))
    }
}

#[derive(Debug)]
pub enum TaskDispatcherError<RepositoryError, HandlerError> {
    Repository(RepositoryError),
    Handler(HandlerError),
    TerminalTaskMissing,
    TerminalTaskNotCompleted(TaskStatus),
    RetryTaskNotQueued(TaskStatus),
}
