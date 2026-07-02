use serde::Serialize;
use utoipa::ToSchema;

use crate::application::{
    acquisition::ReadMode,
    error::ApplicationError,
    task_manager::{SourceStream, TaskDefinition, TaskFailure, TaskSnapshot, TaskStatus},
};

use super::timestamp;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskResponse {
    pub id: String,
    pub workspace_id: Option<String>,
    pub connection_id: String,
    pub kind: TaskKindResponse,
    pub source_id: Option<String>,
    pub stream: Option<SourceStreamResponse>,
    pub mode: Option<ReadModeResponse>,
    pub status: TaskStatusResponse,
    pub terminal: bool,
    pub cancellation_supported: bool,
    pub cancellation_requested_at: Option<String>,
    pub failure: Option<TaskFailureResponse>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum TaskKindResponse {
    SourceDiscovery,
    CatalogRead,
    StateRead,
    CatalogReset,
    StateReset,
    SyncAction,
    SyncActionReconciliation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SourceStreamResponse {
    Catalog,
    State,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ReadModeResponse {
    Full,
    Incremental,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum TaskStatusResponse {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct TaskFailureResponse {
    pub code: String,
    pub retryable: bool,
}

impl TaskResponse {
    pub fn from_snapshot(task: TaskSnapshot) -> Result<Self, ApplicationError> {
        let (kind, source_id, stream, mode) = match task.definition {
            TaskDefinition::SourceDiscovery { .. } => {
                (TaskKindResponse::SourceDiscovery, None, None, None)
            }
            TaskDefinition::Read {
                source_id,
                stream,
                mode,
            } => (
                match stream {
                    SourceStream::Catalog => TaskKindResponse::CatalogRead,
                    SourceStream::State => TaskKindResponse::StateRead,
                },
                Some(source_id.to_string()),
                Some(SourceStreamResponse::from(stream)),
                Some(ReadModeResponse::from(mode)),
            ),
            TaskDefinition::Reset { source_id, stream } => (
                match stream {
                    SourceStream::Catalog => TaskKindResponse::CatalogReset,
                    SourceStream::State => TaskKindResponse::StateReset,
                },
                Some(source_id.to_string()),
                Some(SourceStreamResponse::from(stream)),
                None,
            ),
            TaskDefinition::SyncAction {
                receiver_source_id, ..
            } => (
                TaskKindResponse::SyncAction,
                Some(receiver_source_id.to_string()),
                None,
                None,
            ),
            TaskDefinition::SyncActionReconciliation {
                receiver_source_id, ..
            } => (
                TaskKindResponse::SyncActionReconciliation,
                Some(receiver_source_id.to_string()),
                None,
                None,
            ),
        };
        Ok(Self {
            id: task.id.to_string(),
            workspace_id: task.workspace_id.map(|id| id.to_string()),
            connection_id: task.connection_id.to_string(),
            kind,
            source_id,
            stream,
            mode,
            status: TaskStatusResponse::from(task.status),
            terminal: task.status.terminal(),
            cancellation_supported: task.cancellation_supported,
            cancellation_requested_at: task.cancellation_requested_at.map(timestamp).transpose()?,
            failure: task.failure.map(TaskFailureResponse::from),
            created_at: timestamp(task.created_at)?,
            started_at: task.started_at.map(timestamp).transpose()?,
            finished_at: task.finished_at.map(timestamp).transpose()?,
            updated_at: timestamp(task.updated_at)?,
        })
    }
}

impl From<TaskFailure> for TaskFailureResponse {
    fn from(failure: TaskFailure) -> Self {
        Self {
            code: failure.code,
            retryable: failure.retryable,
        }
    }
}

impl From<SourceStream> for SourceStreamResponse {
    fn from(stream: SourceStream) -> Self {
        match stream {
            SourceStream::Catalog => Self::Catalog,
            SourceStream::State => Self::State,
        }
    }
}

impl From<ReadMode> for ReadModeResponse {
    fn from(mode: ReadMode) -> Self {
        match mode {
            ReadMode::Full => Self::Full,
            ReadMode::Incremental => Self::Incremental,
        }
    }
}

impl From<TaskStatus> for TaskStatusResponse {
    fn from(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Queued => Self::Queued,
            TaskStatus::Running => Self::Running,
            TaskStatus::Cancelling => Self::Cancelling,
            TaskStatus::Succeeded => Self::Succeeded,
            TaskStatus::Failed => Self::Failed,
            TaskStatus::Cancelled => Self::Cancelled,
            TaskStatus::Interrupted => Self::Interrupted,
        }
    }
}
