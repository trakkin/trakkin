use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};

use time::OffsetDateTime;
use uuid::Uuid;

use super::task_manager::{
    ClaimNextTask, ClaimedTask, CompleteTask, TaskCompletion, TaskDefinition, TaskFailure,
    TaskKind, TaskRepository, TaskSnapshot, TaskStatus,
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

    fn execute<'a>(&'a self, task: &'a ClaimedTask) -> TaskHandlerFuture<'a, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchNextTask {
    pub owner_id: Uuid,
    pub attempt_id: Uuid,
    pub claimed_at: OffsetDateTime,
}

pub struct TaskDispatcher<R, H> {
    repository: R,
    handler: H,
    execution: ConnectionExecutionLocks,
}

#[derive(Clone, Default)]
pub struct ConnectionExecutionLocks {
    connections: Arc<Mutex<HashMap<Uuid, Weak<tokio::sync::Mutex<()>>>>>,
}

impl ConnectionExecutionLocks {
    pub async fn lock(&self, connection_id: Uuid) -> tokio::sync::OwnedMutexGuard<()> {
        let connection = {
            let mut connections = self
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            connections.retain(|_, lock| lock.strong_count() > 0);
            match connections.get(&connection_id).and_then(Weak::upgrade) {
                Some(connection) => connection,
                None => {
                    let connection = Arc::new(tokio::sync::Mutex::new(()));
                    connections.insert(connection_id, Arc::downgrade(&connection));
                    connection
                }
            }
        };
        connection.lock_owned().await
    }
}

#[cfg(test)]
mod execution_tests {
    use super::ConnectionExecutionLocks;
    use uuid::Uuid;

    #[tokio::test]
    async fn connection_execution_waits_for_cleanup_without_blocking_other_connections() {
        let locks = ConnectionExecutionLocks::default();
        let connection_id = Uuid::from_u128(1);
        let cleanup = locks.lock(connection_id).await;
        let worker = locks.clone();
        let next = worker.lock(connection_id);
        tokio::pin!(next);
        assert!(futures_util::poll!(&mut next).is_pending());
        let independent = locks.lock(Uuid::from_u128(2)).await;
        drop(cleanup);
        let resumed = next.await;
        drop(resumed);
        drop(independent);
        let reused = locks.lock(connection_id).await;
        drop(reused);
    }
}

impl<R, H> TaskDispatcher<R, H>
where
    R: TaskRepository,
    H: TaskHandler,
{
    pub fn new(repository: R, handler: H) -> Self {
        Self {
            repository,
            handler,
            execution: ConnectionExecutionLocks::default(),
        }
    }

    pub fn with_execution_locks(mut self, execution: ConnectionExecutionLocks) -> Self {
        self.execution = execution;
        self
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
            .repository
            .claim_next_task(ClaimNextTask {
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
        let _execution = self.execution.lock(claimed.task.connection_id).await;
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
        let outcome = match self.handler.execute(&claimed).await {
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
                let cleanup = async {
                    if self
                        .repository
                        .get_task(claimed.task.id)
                        .await?
                        .is_some_and(|task| task.status.terminal())
                    {
                        return Ok(());
                    }
                    self.repository
                        .complete_task(CompleteTask {
                            task_id: claimed.task.id,
                            attempt_id: claimed.attempt_id,
                            owner_id: claimed.owner_id,
                            completion: TaskCompletion::Failed(TaskFailure::coded(
                                "task_handler_failed",
                                false,
                            )),
                            completed_at: OffsetDateTime::now_utc(),
                        })
                        .await?;
                    Ok::<(), R::Error>(())
                }
                .await;
                if let Err(cleanup_error) = cleanup {
                    return Err(TaskDispatcherError::HandlerCleanup {
                        handler: error,
                        cleanup: cleanup_error,
                    });
                }
                return Err(TaskDispatcherError::Handler(error));
            }
        };
        let completed = match outcome {
            TaskHandlerOutcome::Complete(completion) => self
                .repository
                .complete_task(CompleteTask {
                    task_id: claimed.task.id,
                    attempt_id: claimed.attempt_id,
                    owner_id: claimed.owner_id,
                    completion,
                    completed_at: OffsetDateTime::now_utc(),
                })
                .await
                .map_err(TaskDispatcherError::Repository)?,
            TaskHandlerOutcome::Terminalized => {
                let task = self
                    .repository
                    .get_task(claimed.task.id)
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
                    .repository
                    .get_task(claimed.task.id)
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
    #[error("task handler failed: {handler}; failure cleanup failed: {cleanup}")]
    HandlerCleanup {
        #[source]
        handler: HandlerError,
        cleanup: RepositoryError,
    },
    #[error("terminal task no longer exists")]
    TerminalTaskMissing,
    #[error("task handler returned without completing task in status {0:?}")]
    TerminalTaskNotCompleted(TaskStatus),
    #[error("task handler returned a retry without queuing task in status {0:?}")]
    RetryTaskNotQueued(TaskStatus),
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::application::task_manager::{
        AdmitTask, CancellationDisposition, HeartbeatTask, RequestTaskCancellation, TaskFailure,
        TaskManagerFuture,
    };

    #[derive(Clone)]
    struct Repository {
        task: TaskSnapshot,
        handler_finished_at: Arc<Mutex<Option<OffsetDateTime>>>,
        persisted: Arc<Mutex<Option<TaskSnapshot>>>,
        completions: Arc<Mutex<Vec<CompleteTask>>>,
        completion_error: Option<&'static str>,
    }

    impl TaskRepository for Repository {
        type Error = &'static str;

        fn claim_next_task(
            &self,
            command: ClaimNextTask,
        ) -> TaskManagerFuture<'_, Option<ClaimedTask>, Self::Error> {
            let mut task = self.task.clone();
            task.status = TaskStatus::Running;
            task.started_at = Some(command.claimed_at);
            Box::pin(async move {
                Ok(Some(ClaimedTask {
                    task,
                    attempt_id: command.attempt_id,
                    attempt_ordinal: 1,
                    owner_id: command.owner_id,
                }))
            })
        }

        fn complete_task(
            &self,
            command: CompleteTask,
        ) -> TaskManagerFuture<'_, TaskSnapshot, Self::Error> {
            let finished_at = self
                .handler_finished_at
                .lock()
                .unwrap()
                .expect("handler finished before completion");
            assert!(command.completed_at >= finished_at);
            assert_eq!(command.task_id, self.task.id);
            assert_eq!(command.owner_id, Uuid::from_u128(3));
            assert_eq!(command.attempt_id, Uuid::from_u128(4));
            self.completions.lock().unwrap().push(command.clone());
            if let Some(error) = self.completion_error {
                return Box::pin(async move { Err(error) });
            }
            let mut task = self.task.clone();
            task.status = match command.completion {
                TaskCompletion::Succeeded => TaskStatus::Succeeded,
                TaskCompletion::Failed(failure) => {
                    task.failure = Some(failure);
                    TaskStatus::Failed
                }
                TaskCompletion::Cancelled => TaskStatus::Cancelled,
            };
            task.finished_at = Some(command.completed_at);
            task.updated_at = command.completed_at;
            *self.persisted.lock().unwrap() = Some(task.clone());
            Box::pin(async move { Ok(task) })
        }

        fn admit_task(&self, _: AdmitTask) -> TaskManagerFuture<'_, TaskSnapshot, Self::Error> {
            unreachable!()
        }
        fn heartbeat_task(&self, _: HeartbeatTask) -> TaskManagerFuture<'_, (), Self::Error> {
            unreachable!()
        }
        fn request_cancellation(
            &self,
            _: RequestTaskCancellation,
        ) -> TaskManagerFuture<'_, CancellationDisposition, Self::Error> {
            unreachable!()
        }
        fn get_task(&self, _: Uuid) -> TaskManagerFuture<'_, Option<TaskSnapshot>, Self::Error> {
            let task = self.persisted.lock().unwrap().clone().unwrap_or_else(|| {
                let mut task = self.task.clone();
                task.status = TaskStatus::Running;
                task
            });
            Box::pin(async move { Ok(Some(task)) })
        }
        fn list_workspace_tasks(
            &self,
            _: Uuid,
        ) -> TaskManagerFuture<'_, Vec<TaskSnapshot>, Self::Error> {
            unreachable!()
        }
    }

    struct Handler {
        completion: TaskCompletion,
        finished_at: Arc<Mutex<Option<OffsetDateTime>>>,
    }

    impl TaskHandler for Handler {
        type Error = Infallible;

        fn accepted_kinds(&self) -> &'static [TaskKind] {
            &[TaskKind::SourceDiscovery]
        }

        fn execute<'a>(&'a self, _: &'a ClaimedTask) -> TaskHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                tokio::task::yield_now().await;
                *self.finished_at.lock().unwrap() = Some(OffsetDateTime::now_utc());
                Ok(TaskHandlerOutcome::Complete(self.completion.clone()))
            })
        }
    }

    #[tokio::test]
    async fn terminal_completions_use_time_after_handler_execution() {
        for completion in [
            TaskCompletion::Succeeded,
            TaskCompletion::Failed(TaskFailure::coded("test_failure", false)),
            TaskCompletion::Cancelled,
        ] {
            let claimed_at = OffsetDateTime::UNIX_EPOCH;
            let handler_finished_at = Arc::new(Mutex::new(None));
            let connection_id = Uuid::from_u128(1);
            let repository = Repository {
                task: TaskSnapshot {
                    id: Uuid::from_u128(2),
                    workspace_id: None,
                    connection_id,
                    definition: TaskDefinition::SourceDiscovery { connection_id },
                    status: TaskStatus::Queued,
                    cancellation_supported: true,
                    cancellation_requested_at: None,
                    failure: None,
                    created_at: claimed_at,
                    started_at: None,
                    finished_at: None,
                    updated_at: claimed_at,
                },
                handler_finished_at: Arc::clone(&handler_finished_at),
                persisted: Arc::new(Mutex::new(None)),
                completions: Arc::new(Mutex::new(Vec::new())),
                completion_error: None,
            };
            let dispatcher = TaskDispatcher::new(
                repository,
                Handler {
                    completion: completion.clone(),
                    finished_at: handler_finished_at,
                },
            );
            let task = dispatcher
                .dispatch_next(DispatchNextTask {
                    owner_id: Uuid::from_u128(3),
                    attempt_id: Uuid::from_u128(4),
                    claimed_at,
                })
                .await
                .unwrap()
                .unwrap();
            assert!(task.status.terminal());
            assert!(task.finished_at.unwrap() > claimed_at);
            assert_eq!(task.updated_at, task.finished_at.unwrap());
            assert_eq!(
                task.status,
                match completion {
                    TaskCompletion::Succeeded => TaskStatus::Succeeded,
                    TaskCompletion::Failed(_) => TaskStatus::Failed,
                    TaskCompletion::Cancelled => TaskStatus::Cancelled,
                }
            );
        }
    }

    struct ErrorHandler {
        repository: Repository,
        terminal_status: Option<TaskStatus>,
    }

    impl TaskHandler for ErrorHandler {
        type Error = &'static str;

        fn accepted_kinds(&self) -> &'static [TaskKind] {
            &[TaskKind::SourceDiscovery]
        }

        fn execute<'a>(&'a self, _: &'a ClaimedTask) -> TaskHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                let finished_at = OffsetDateTime::now_utc();
                *self.repository.handler_finished_at.lock().unwrap() = Some(finished_at);
                if let Some(status) = self.terminal_status {
                    let mut task = self.repository.task.clone();
                    task.status = status;
                    task.finished_at = Some(finished_at);
                    task.updated_at = finished_at;
                    if status == TaskStatus::Failed {
                        task.failure = Some(TaskFailure::coded("publication_failed", true));
                    }
                    *self.repository.persisted.lock().unwrap() = Some(task);
                }
                Err("session restore failed")
            })
        }
    }

    fn error_repository() -> Repository {
        let connection_id = Uuid::from_u128(1);
        Repository {
            task: TaskSnapshot {
                id: Uuid::from_u128(2),
                workspace_id: None,
                connection_id,
                definition: TaskDefinition::SourceDiscovery { connection_id },
                status: TaskStatus::Queued,
                cancellation_supported: true,
                cancellation_requested_at: None,
                failure: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                started_at: None,
                finished_at: None,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            },
            handler_finished_at: Arc::new(Mutex::new(None)),
            persisted: Arc::new(Mutex::new(None)),
            completions: Arc::new(Mutex::new(Vec::new())),
            completion_error: None,
        }
    }

    async fn dispatch_error(
        repository: Repository,
        terminal_status: Option<TaskStatus>,
    ) -> TaskDispatcherError<&'static str, &'static str> {
        TaskDispatcher::new(
            repository.clone(),
            ErrorHandler {
                repository,
                terminal_status,
            },
        )
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(3),
            attempt_id: Uuid::from_u128(4),
            claimed_at: OffsetDateTime::UNIX_EPOCH,
        })
        .await
        .unwrap_err()
    }

    #[tokio::test]
    async fn handler_error_completes_failed_task_and_preserves_error() {
        let repository = error_repository();
        assert!(matches!(
            dispatch_error(repository.clone(), None).await,
            TaskDispatcherError::Handler("session restore failed")
        ));
        let task = repository.persisted.lock().unwrap().clone().unwrap();
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(
            task.failure,
            Some(TaskFailure::coded("task_handler_failed", false))
        );
        assert!(
            task.finished_at.unwrap() >= repository.handler_finished_at.lock().unwrap().unwrap()
        );
        assert_eq!(task.updated_at, task.finished_at.unwrap());
        assert_eq!(repository.completions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn handler_error_preserves_already_terminalized_tasks() {
        for status in [
            TaskStatus::Succeeded,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
            TaskStatus::Interrupted,
        ] {
            let repository = error_repository();
            assert!(matches!(
                dispatch_error(repository.clone(), Some(status)).await,
                TaskDispatcherError::Handler("session restore failed")
            ));
            let task = repository.persisted.lock().unwrap().clone().unwrap();
            assert_eq!(task.status, status);
            assert_eq!(
                task.finished_at,
                *repository.handler_finished_at.lock().unwrap()
            );
            if status == TaskStatus::Failed {
                assert_eq!(
                    task.failure,
                    Some(TaskFailure::coded("publication_failed", true))
                );
            } else {
                assert_eq!(task.failure, None);
            }
            assert!(repository.completions.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn handler_error_preserves_completion_repository_failure() {
        let mut repository = error_repository();
        repository.completion_error = Some("completion unavailable");
        let error = dispatch_error(repository.clone(), None).await;
        assert!(matches!(
            error,
            TaskDispatcherError::HandlerCleanup {
                handler: "session restore failed",
                cleanup: "completion unavailable",
            }
        ));
        assert_eq!(repository.completions.lock().unwrap().len(), 1);
        assert!(repository.persisted.lock().unwrap().is_none());
    }
}
