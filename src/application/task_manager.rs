use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use super::acquisition::ReadMode;

pub type TaskManagerFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceStream {
    Catalog,
    State,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskKind {
    SourceDiscovery,
    CatalogRead,
    StateRead,
    CatalogReset,
    StateReset,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskDefinition {
    SourceDiscovery {
        connection_id: Uuid,
    },
    Read {
        source_id: Uuid,
        stream: SourceStream,
        mode: ReadMode,
    },
    Reset {
        source_id: Uuid,
        stream: SourceStream,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStatus {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}

impl TaskStatus {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskRetryAfter {
    pub seconds: i64,
    pub nanoseconds: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after: Option<TaskRetryAfter>,
}

impl TaskFailure {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
            retry_after: None,
        }
    }

    pub fn coded(code: impl Into<String>, retryable: bool) -> Self {
        let code = code.into();
        let message = code.replace('_', " ");
        Self::new(code, message, retryable)
    }

    pub fn from_adapter(error: &super::adapter_runtime::AdapterCallError) -> Self {
        Self {
            code: error.code.clone(),
            message: error.safe_message.clone(),
            retryable: error.retryable,
            retry_after: error.retry_after.as_ref().map(|after| TaskRetryAfter {
                seconds: after.seconds,
                nanoseconds: after.nanoseconds,
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSnapshot {
    pub id: Uuid,
    pub workspace_id: Option<Uuid>,
    pub connection_id: Uuid,
    pub definition: TaskDefinition,
    pub status: TaskStatus,
    pub cancellation_supported: bool,
    pub cancellation_requested_at: Option<OffsetDateTime>,
    pub failure: Option<TaskFailure>,
    pub created_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmitTask {
    pub task_id: Uuid,
    pub workspace_id: Option<Uuid>,
    pub definition: TaskDefinition,
    pub cancellation_supported: bool,
    pub admitted_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimNextTask {
    pub owner_id: Uuid,
    pub attempt_id: Uuid,
    pub accepted_kinds: Vec<TaskKind>,
    pub claimed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedTask {
    pub task: TaskSnapshot,
    pub attempt_id: Uuid,
    pub attempt_ordinal: i32,
    pub owner_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeartbeatTask {
    pub task_id: Uuid,
    pub owner_id: Uuid,
    pub heartbeat_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestTaskCancellation {
    pub task_id: Uuid,
    pub requested_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationDisposition {
    CancelledBeforeStart,
    CancellationRequested,
    AlreadyRequested,
    AlreadyTerminal(TaskStatus),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskCompletion {
    Succeeded,
    Failed(TaskFailure),
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteTask {
    pub task_id: Uuid,
    pub attempt_id: Uuid,
    pub owner_id: Uuid,
    pub completion: TaskCompletion,
    pub completed_at: OffsetDateTime,
}

pub trait TaskRepository: Send + Sync {
    type Error;

    fn admit_task(&self, command: AdmitTask) -> TaskManagerFuture<'_, TaskSnapshot, Self::Error>;

    fn claim_next_task(
        &self,
        command: ClaimNextTask,
    ) -> TaskManagerFuture<'_, Option<ClaimedTask>, Self::Error>;

    fn heartbeat_task(&self, command: HeartbeatTask) -> TaskManagerFuture<'_, (), Self::Error>;

    fn request_cancellation(
        &self,
        command: RequestTaskCancellation,
    ) -> TaskManagerFuture<'_, CancellationDisposition, Self::Error>;

    fn complete_task(
        &self,
        command: CompleteTask,
    ) -> TaskManagerFuture<'_, TaskSnapshot, Self::Error>;

    fn get_task(&self, task_id: Uuid) -> TaskManagerFuture<'_, Option<TaskSnapshot>, Self::Error>;

    fn list_workspace_tasks(
        &self,
        workspace_id: Uuid,
    ) -> TaskManagerFuture<'_, Vec<TaskSnapshot>, Self::Error>;
}

use super::{
    error::{ApplicationError, ApplicationErrorCode, ApplicationFuture, IntoApplicationError},
    workspaces::WorkspaceLookup,
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskScope {
    Workspace(Uuid),
    ConnectionDiscovery(Uuid),
}

impl TaskScope {
    fn contains(self, task: &TaskSnapshot) -> bool {
        match self {
            Self::Workspace(id) => task.workspace_id == Some(id),
            Self::ConnectionDiscovery(id) => {
                task.workspace_id.is_none()
                    && task.connection_id == id
                    && matches!(task.definition, TaskDefinition::SourceDiscovery { .. })
            }
        }
    }
}

pub trait TaskApi: Send + Sync {
    fn admit_task(&self, command: AdmitTask) -> ApplicationFuture<'_, TaskSnapshot>;
    fn list_tasks(&self, workspace_id: Uuid) -> ApplicationFuture<'_, Vec<TaskSnapshot>>;
    fn get_task(&self, workspace_id: Uuid, task_id: Uuid) -> ApplicationFuture<'_, TaskSnapshot>;
    fn cancel_task(
        &self,
        workspace_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot>;
    fn get_connection_task(
        &self,
        connection_id: Uuid,
        task_id: Uuid,
    ) -> ApplicationFuture<'_, TaskSnapshot>;
    fn cancel_connection_task(
        &self,
        connection_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot>;
}

pub struct TaskService<R> {
    tasks: R,
    workspaces: Arc<dyn WorkspaceLookup>,
}

impl<R: TaskRepository> TaskService<R>
where
    R::Error: IntoApplicationError,
{
    pub fn new(repository: R, workspaces: Arc<dyn WorkspaceLookup>) -> Self {
        Self {
            tasks: repository,
            workspaces,
        }
    }
    async fn scoped_task(
        &self,
        scope: TaskScope,
        task_id: Uuid,
    ) -> Result<TaskSnapshot, ApplicationError> {
        self.tasks
            .get_task(task_id)
            .await
            .map_err(IntoApplicationError::into_application_error)?
            .filter(|task| scope.contains(task))
            .ok_or_else(|| {
                ApplicationError::new(ApplicationErrorCode::TaskNotFound, "task was not found")
            })
    }

    async fn cancel_scoped_task(
        &self,
        scope: TaskScope,
        command: RequestTaskCancellation,
    ) -> Result<TaskSnapshot, ApplicationError> {
        self.scoped_task(scope, command.task_id).await?;
        self.tasks
            .request_cancellation(command.clone())
            .await
            .map_err(IntoApplicationError::into_application_error)?;
        self.scoped_task(scope, command.task_id).await
    }
}

impl<R: TaskRepository> TaskApi for TaskService<R>
where
    R::Error: IntoApplicationError,
{
    fn admit_task(&self, command: AdmitTask) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(async move {
            self.tasks
                .admit_task(command)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }
    fn list_tasks(&self, workspace_id: Uuid) -> ApplicationFuture<'_, Vec<TaskSnapshot>> {
        Box::pin(async move {
            self.workspaces.ensure_workspace(workspace_id).await?;
            self.tasks
                .list_workspace_tasks(workspace_id)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }
    fn get_task(&self, workspace_id: Uuid, task_id: Uuid) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(self.scoped_task(TaskScope::Workspace(workspace_id), task_id))
    }
    fn cancel_task(
        &self,
        workspace_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(self.cancel_scoped_task(TaskScope::Workspace(workspace_id), command))
    }
    fn get_connection_task(
        &self,
        connection_id: Uuid,
        task_id: Uuid,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(self.scoped_task(TaskScope::ConnectionDiscovery(connection_id), task_id))
    }
    fn cancel_connection_task(
        &self,
        connection_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(self.cancel_scoped_task(TaskScope::ConnectionDiscovery(connection_id), command))
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct Tasks {
        task: Mutex<TaskSnapshot>,
        cancellations: AtomicUsize,
        lists: AtomicUsize,
    }

    impl TaskRepository for Arc<Tasks> {
        type Error = ApplicationError;

        fn admit_task(&self, _: AdmitTask) -> TaskManagerFuture<'_, TaskSnapshot, Self::Error> {
            unreachable!()
        }
        fn claim_next_task(
            &self,
            _: ClaimNextTask,
        ) -> TaskManagerFuture<'_, Option<ClaimedTask>, Self::Error> {
            unreachable!()
        }
        fn heartbeat_task(&self, _: HeartbeatTask) -> TaskManagerFuture<'_, (), Self::Error> {
            unreachable!()
        }
        fn complete_task(
            &self,
            _: CompleteTask,
        ) -> TaskManagerFuture<'_, TaskSnapshot, Self::Error> {
            unreachable!()
        }
        fn get_task(
            &self,
            task_id: Uuid,
        ) -> TaskManagerFuture<'_, Option<TaskSnapshot>, Self::Error> {
            Box::pin(async move {
                Ok(Some(self.task.lock().unwrap().clone()).filter(|task| task.id == task_id))
            })
        }
        fn request_cancellation(
            &self,
            command: RequestTaskCancellation,
        ) -> TaskManagerFuture<'_, CancellationDisposition, Self::Error> {
            Box::pin(async move {
                self.cancellations.fetch_add(1, Ordering::SeqCst);
                let mut task = self.task.lock().unwrap();
                task.status = TaskStatus::Cancelling;
                task.cancellation_requested_at = Some(command.requested_at);
                Ok(CancellationDisposition::CancellationRequested)
            })
        }
        fn list_workspace_tasks(
            &self,
            _: Uuid,
        ) -> TaskManagerFuture<'_, Vec<TaskSnapshot>, Self::Error> {
            Box::pin(async move {
                self.lists.fetch_add(1, Ordering::SeqCst);
                Ok(vec![self.task.lock().unwrap().clone()])
            })
        }
    }

    struct WorkspaceExists(bool);
    impl WorkspaceLookup for WorkspaceExists {
        fn ensure_workspace(&self, _: Uuid) -> ApplicationFuture<'_, ()> {
            Box::pin(async move {
                if self.0 {
                    Ok(())
                } else {
                    Err(ApplicationError::new(
                        ApplicationErrorCode::WorkspaceNotFound,
                        "workspace was not found",
                    ))
                }
            })
        }
    }

    fn tasks(workspace_id: Option<Uuid>, definition: TaskDefinition) -> Arc<Tasks> {
        Arc::new(Tasks {
            task: Mutex::new(TaskSnapshot {
                id: Uuid::from_u128(1),
                workspace_id,
                connection_id: Uuid::from_u128(2),
                definition,
                status: TaskStatus::Running,
                cancellation_supported: true,
                cancellation_requested_at: None,
                failure: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                started_at: None,
                finished_at: None,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            }),
            cancellations: AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
        })
    }

    fn cancellation() -> RequestTaskCancellation {
        RequestTaskCancellation {
            task_id: Uuid::from_u128(1),
            requested_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn wrong_workspace_cannot_cancel_or_read_task() {
        let repository = tasks(
            Some(Uuid::from_u128(3)),
            TaskDefinition::SourceDiscovery {
                connection_id: Uuid::from_u128(2),
            },
        );
        let service = TaskService::new(repository.clone(), Arc::new(WorkspaceExists(true)));
        assert_eq!(
            service
                .get_task(Uuid::from_u128(4), Uuid::from_u128(1))
                .await
                .unwrap_err()
                .code,
            ApplicationErrorCode::TaskNotFound
        );
        assert_eq!(
            service
                .cancel_task(Uuid::from_u128(4), cancellation())
                .await
                .unwrap_err()
                .code,
            ApplicationErrorCode::TaskNotFound
        );
        assert_eq!(repository.cancellations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn connection_scope_hides_workspace_and_non_discovery_tasks() {
        for (workspace, definition) in [
            (
                Some(Uuid::from_u128(3)),
                TaskDefinition::SourceDiscovery {
                    connection_id: Uuid::from_u128(2),
                },
            ),
            (
                None,
                TaskDefinition::Reset {
                    source_id: Uuid::from_u128(5),
                    stream: SourceStream::Catalog,
                },
            ),
        ] {
            let repository = tasks(workspace, definition);
            let service = TaskService::new(repository.clone(), Arc::new(WorkspaceExists(true)));
            assert_eq!(
                service
                    .cancel_connection_task(Uuid::from_u128(2), cancellation())
                    .await
                    .unwrap_err()
                    .code,
                ApplicationErrorCode::TaskNotFound
            );
            assert_eq!(repository.cancellations.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn cancellation_returns_reloaded_state_and_requires_matching_connection() {
        let repository = tasks(
            None,
            TaskDefinition::SourceDiscovery {
                connection_id: Uuid::from_u128(2),
            },
        );
        let service = TaskService::new(repository.clone(), Arc::new(WorkspaceExists(true)));
        assert_eq!(
            service
                .cancel_connection_task(Uuid::from_u128(4), cancellation())
                .await
                .unwrap_err()
                .code,
            ApplicationErrorCode::TaskNotFound
        );
        let task = service
            .cancel_connection_task(Uuid::from_u128(2), cancellation())
            .await
            .unwrap();
        assert_eq!(task.status, TaskStatus::Cancelling);
        assert_eq!(
            task.cancellation_requested_at,
            Some(OffsetDateTime::UNIX_EPOCH)
        );
        assert_eq!(repository.cancellations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn missing_workspace_stops_listing_before_repository_access() {
        let repository = tasks(
            None,
            TaskDefinition::SourceDiscovery {
                connection_id: Uuid::from_u128(2),
            },
        );
        let service = TaskService::new(repository.clone(), Arc::new(WorkspaceExists(false)));
        assert_eq!(
            service
                .list_tasks(Uuid::from_u128(3))
                .await
                .unwrap_err()
                .code,
            ApplicationErrorCode::WorkspaceNotFound
        );
        assert_eq!(repository.lists.load(Ordering::SeqCst), 0);
    }
}
