use std::{future::Future, pin::Pin};

use super::error::{
    ApplicationError, ApplicationErrorCode, ApplicationFuture, IntoApplicationError,
};

use time::OffsetDateTime;
use uuid::Uuid;

pub type WorkspaceFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceView {
    pub workspace: WorkspaceSnapshot,
    pub sources: Vec<WorkspaceSourceAttachment>,
}

pub struct WorkspaceService<R> {
    repository: R,
}

impl<R: WorkspaceRepository> WorkspaceService<R>
where
    R::Error: IntoApplicationError,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn view(&self, workspace_id: Uuid) -> Result<WorkspaceView, ApplicationError> {
        let workspace = self
            .repository
            .get_workspace(workspace_id)
            .await
            .map_err(IntoApplicationError::into_application_error)?
            .ok_or_else(|| {
                ApplicationError::new(
                    ApplicationErrorCode::WorkspaceNotFound,
                    "workspace was not found",
                )
            })?;
        let sources = self
            .repository
            .list_source_attachments(workspace_id)
            .await
            .map_err(IntoApplicationError::into_application_error)?;
        Ok(WorkspaceView { workspace, sources })
    }
}

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

pub trait WorkspaceLookup: Send + Sync {
    fn ensure_workspace(&self, workspace_id: Uuid) -> ApplicationFuture<'_, ()>;
}

pub trait WorkspaceApi: WorkspaceLookup {
    fn create_workspace(&self, command: CreateWorkspace) -> ApplicationFuture<'_, WorkspaceView>;
    fn list_workspaces(&self) -> ApplicationFuture<'_, Vec<WorkspaceView>>;
    fn get_workspace(&self, workspace_id: Uuid) -> ApplicationFuture<'_, WorkspaceView>;
    fn attach_source(&self, command: AttachWorkspaceSource)
    -> ApplicationFuture<'_, WorkspaceView>;
    fn detach_source(&self, command: DetachWorkspaceSource)
    -> ApplicationFuture<'_, WorkspaceView>;
}

impl<R: WorkspaceRepository> WorkspaceLookup for WorkspaceService<R>
where
    R::Error: IntoApplicationError,
{
    fn ensure_workspace(&self, workspace_id: Uuid) -> ApplicationFuture<'_, ()> {
        Box::pin(async move {
            self.repository
                .get_workspace(workspace_id)
                .await
                .map_err(IntoApplicationError::into_application_error)?
                .ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorCode::WorkspaceNotFound,
                        "workspace was not found",
                    )
                })?;
            Ok(())
        })
    }
}

impl<R: WorkspaceRepository> WorkspaceApi for WorkspaceService<R>
where
    R::Error: IntoApplicationError,
{
    fn create_workspace(&self, command: CreateWorkspace) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(async move {
            let workspace_id = command.workspace_id;
            let workspace = self
                .repository
                .create_workspace(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            tracing::info!(workspace.id = %workspace_id, "workspace created");
            Ok(WorkspaceView {
                workspace,
                sources: Vec::new(),
            })
        })
    }
    fn list_workspaces(&self) -> ApplicationFuture<'_, Vec<WorkspaceView>> {
        Box::pin(async move {
            let workspaces = self
                .repository
                .list_workspaces()
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            let mut views = Vec::with_capacity(workspaces.len());
            for workspace in workspaces {
                let sources = self
                    .repository
                    .list_source_attachments(workspace.id)
                    .await
                    .map_err(IntoApplicationError::into_application_error)?;
                views.push(WorkspaceView { workspace, sources });
            }
            Ok(views)
        })
    }
    fn get_workspace(&self, workspace_id: Uuid) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(self.view(workspace_id))
    }
    fn attach_source(
        &self,
        command: AttachWorkspaceSource,
    ) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(async move {
            let workspace_id = command.workspace_id;
            let source_id = command.source_id;
            self.repository
                .attach_source(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            tracing::info!(%workspace_id, %source_id, "workspace source attached");
            self.view(workspace_id).await
        })
    }
    fn detach_source(
        &self,
        command: DetachWorkspaceSource,
    ) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(async move {
            let workspace_id = command.workspace_id;
            let source_id = command.source_id;
            let detached = self
                .repository
                .detach_source(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            if detached {
                tracing::info!(%workspace_id, %source_id, "workspace source detached");
            }
            self.view(workspace_id).await
        })
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Repository {
        workspace: Option<WorkspaceSnapshot>,
        attachment_reads: AtomicUsize,
    }

    impl WorkspaceRepository for Arc<Repository> {
        type Error = ApplicationError;

        fn create_workspace(
            &self,
            _: CreateWorkspace,
        ) -> WorkspaceFuture<'_, WorkspaceSnapshot, Self::Error> {
            unreachable!()
        }
        fn attach_source(
            &self,
            _: AttachWorkspaceSource,
        ) -> WorkspaceFuture<'_, WorkspaceSourceAttachment, Self::Error> {
            unreachable!()
        }
        fn detach_source(
            &self,
            _: DetachWorkspaceSource,
        ) -> WorkspaceFuture<'_, bool, Self::Error> {
            unreachable!()
        }
        fn get_workspace(
            &self,
            _: Uuid,
        ) -> WorkspaceFuture<'_, Option<WorkspaceSnapshot>, Self::Error> {
            Box::pin(async move { Ok(self.workspace.clone()) })
        }
        fn list_workspaces(&self) -> WorkspaceFuture<'_, Vec<WorkspaceSnapshot>, Self::Error> {
            Box::pin(async move { Ok(self.workspace.clone().into_iter().collect()) })
        }
        fn list_source_attachments(
            &self,
            workspace_id: Uuid,
        ) -> WorkspaceFuture<'_, Vec<WorkspaceSourceAttachment>, Self::Error> {
            Box::pin(async move {
                self.attachment_reads.fetch_add(1, Ordering::SeqCst);
                Ok(vec![WorkspaceSourceAttachment {
                    workspace_id,
                    source_id: Uuid::from_u128(2),
                    attached_at: OffsetDateTime::UNIX_EPOCH,
                }])
            })
        }
    }

    #[tokio::test]
    async fn existence_check_does_not_assemble_view_and_views_include_sources() {
        let workspace = WorkspaceSnapshot {
            id: Uuid::from_u128(1),
            name: "Workspace".to_owned(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        };
        let repository = Arc::new(Repository {
            workspace: Some(workspace.clone()),
            attachment_reads: AtomicUsize::new(0),
        });
        let service = WorkspaceService::new(repository.clone());
        service.ensure_workspace(workspace.id).await.unwrap();
        assert_eq!(repository.attachment_reads.load(Ordering::SeqCst), 0);
        let view = service.get_workspace(workspace.id).await.unwrap();
        assert_eq!(view.workspace, workspace);
        assert_eq!(view.sources.len(), 1);
        assert_eq!(view.sources[0].workspace_id, workspace.id);
        assert_eq!(service.list_workspaces().await.unwrap(), vec![view]);
        assert_eq!(repository.attachment_reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn missing_workspace_returns_public_error_without_loading_sources() {
        let repository = Arc::new(Repository {
            workspace: None,
            attachment_reads: AtomicUsize::new(0),
        });
        let service = WorkspaceService::new(repository.clone());
        assert_eq!(
            service
                .get_workspace(Uuid::from_u128(1))
                .await
                .unwrap_err()
                .code,
            ApplicationErrorCode::WorkspaceNotFound
        );
        assert_eq!(
            service
                .ensure_workspace(Uuid::from_u128(1))
                .await
                .unwrap_err()
                .code,
            ApplicationErrorCode::WorkspaceNotFound
        );
        assert_eq!(repository.attachment_reads.load(Ordering::SeqCst), 0);
    }
}
