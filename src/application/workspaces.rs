use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

pub type WorkspaceFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceSnapshot {
    pub id: Uuid,
    pub name: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceSourceAttachment {
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub attached_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorkspace {
    pub workspace_id: Uuid,
    pub name: String,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachWorkspaceSource {
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub attached_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetachWorkspaceSource {
    pub workspace_id: Uuid,
    pub source_id: Uuid,
}

pub trait WorkspaceRepository: Send + Sync {
    type Error;

    fn create_workspace(
        &self,
        command: CreateWorkspace,
    ) -> WorkspaceFuture<'_, WorkspaceSnapshot, Self::Error>;

    fn list_workspaces(&self) -> WorkspaceFuture<'_, Vec<WorkspaceSnapshot>, Self::Error>;

    fn get_workspace(
        &self,
        workspace_id: Uuid,
    ) -> WorkspaceFuture<'_, Option<WorkspaceSnapshot>, Self::Error>;

    fn attach_source(
        &self,
        command: AttachWorkspaceSource,
    ) -> WorkspaceFuture<'_, WorkspaceSourceAttachment, Self::Error>;

    fn detach_source(
        &self,
        command: DetachWorkspaceSource,
    ) -> WorkspaceFuture<'_, bool, Self::Error>;

    fn list_source_attachments(
        &self,
        workspace_id: Uuid,
    ) -> WorkspaceFuture<'_, Vec<WorkspaceSourceAttachment>, Self::Error>;
}

#[derive(Clone)]
pub struct WorkspaceService<R> {
    repository: R,
}

impl<R> WorkspaceService<R>
where
    R: WorkspaceRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn create(&self, command: CreateWorkspace) -> Result<WorkspaceSnapshot, R::Error> {
        self.repository.create_workspace(command).await
    }

    pub async fn list(&self) -> Result<Vec<WorkspaceSnapshot>, R::Error> {
        self.repository.list_workspaces().await
    }

    pub async fn get(&self, workspace_id: Uuid) -> Result<Option<WorkspaceSnapshot>, R::Error> {
        self.repository.get_workspace(workspace_id).await
    }

    pub async fn attach(
        &self,
        command: AttachWorkspaceSource,
    ) -> Result<WorkspaceSourceAttachment, R::Error> {
        self.repository.attach_source(command).await
    }

    pub async fn detach(&self, command: DetachWorkspaceSource) -> Result<bool, R::Error> {
        self.repository.detach_source(command).await
    }

    pub async fn attachments(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkspaceSourceAttachment>, R::Error> {
        self.repository.list_source_attachments(workspace_id).await
    }
}
