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
    SyncAction,
    SyncActionReconciliation,
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
    SyncAction {
        action_id: Uuid,
        receiver_source_id: Uuid,
    },
    SyncActionReconciliation {
        action_id: Uuid,
        receiver_source_id: Uuid,
        write_task_attempt_id: Uuid,
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
pub struct TaskFailure {
    pub code: String,
    pub retryable: bool,
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

#[derive(Clone)]
pub struct TaskManager<R> {
    repository: R,
}

impl<R> TaskManager<R>
where
    R: TaskRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn admit(&self, command: AdmitTask) -> Result<TaskSnapshot, R::Error> {
        self.repository.admit_task(command).await
    }

    pub async fn claim_next(
        &self,
        command: ClaimNextTask,
    ) -> Result<Option<ClaimedTask>, R::Error> {
        self.repository.claim_next_task(command).await
    }

    pub async fn heartbeat(&self, command: HeartbeatTask) -> Result<(), R::Error> {
        self.repository.heartbeat_task(command).await
    }

    pub async fn request_cancellation(
        &self,
        command: RequestTaskCancellation,
    ) -> Result<CancellationDisposition, R::Error> {
        self.repository.request_cancellation(command).await
    }

    pub async fn complete(&self, command: CompleteTask) -> Result<TaskSnapshot, R::Error> {
        self.repository.complete_task(command).await
    }

    pub async fn get(&self, task_id: Uuid) -> Result<Option<TaskSnapshot>, R::Error> {
        self.repository.get_task(task_id).await
    }

    pub async fn list_workspace(&self, workspace_id: Uuid) -> Result<Vec<TaskSnapshot>, R::Error> {
        self.repository.list_workspace_tasks(workspace_id).await
    }
}
