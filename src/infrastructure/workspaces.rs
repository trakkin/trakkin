use sqlx::{FromRow, PgPool};
use thiserror::Error;
use uuid::Uuid;

use crate::application::error::{ApplicationError, ApplicationErrorCode, IntoApplicationError};
use crate::application::workspaces::{
    AttachWorkspaceSource, CreateWorkspace, DetachWorkspaceSource, WorkspaceFuture,
    WorkspaceRepository, WorkspaceSnapshot, WorkspaceSourceAttachment,
};

#[derive(Clone)]
pub struct PostgresWorkspaceRepository {
    pool: PgPool,
}

impl PostgresWorkspaceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl WorkspaceRepository for PostgresWorkspaceRepository {
    type Error = WorkspaceError;

    fn create_workspace(
        &self,
        command: CreateWorkspace,
    ) -> WorkspaceFuture<'_, WorkspaceSnapshot, Self::Error> {
        Box::pin(async move {
            let name = command.name.trim();
            if name.is_empty() {
                return Err(WorkspaceError::InvalidName);
            }
            let row = sqlx::query_as::<_, WorkspaceRow>(
                "INSERT INTO workspaces (
    id, name, status, created_at, updated_at
)
VALUES ($1, $2, 'active', $3, $3)
RETURNING id, name, created_at, updated_at
",
            )
            .bind(command.workspace_id)
            .bind(name)
            .bind(command.created_at)
            .fetch_one(&self.pool)
            .await
            .map_err(map_workspace_insert_error)?;
            Ok(row.into_snapshot())
        })
    }

    fn list_workspaces(&self) -> WorkspaceFuture<'_, Vec<WorkspaceSnapshot>, Self::Error> {
        Box::pin(async move {
            sqlx::query_as::<_, WorkspaceRow>(
                "SELECT id, name, created_at, updated_at
FROM workspaces
WHERE status = 'active'
ORDER BY created_at, id
",
            )
            .fetch_all(&self.pool)
            .await
            .map(|rows| rows.into_iter().map(WorkspaceRow::into_snapshot).collect())
            .map_err(WorkspaceError::Database)
        })
    }

    fn get_workspace(
        &self,
        workspace_id: Uuid,
    ) -> WorkspaceFuture<'_, Option<WorkspaceSnapshot>, Self::Error> {
        Box::pin(async move {
            sqlx::query_as::<_, WorkspaceRow>(
                "SELECT id, name, created_at, updated_at
FROM workspaces
WHERE id = $1 AND status = 'active'
",
            )
            .bind(workspace_id)
            .fetch_optional(&self.pool)
            .await
            .map(|row| row.map(WorkspaceRow::into_snapshot))
            .map_err(WorkspaceError::Database)
        })
    }

    fn attach_source(
        &self,
        command: AttachWorkspaceSource,
    ) -> WorkspaceFuture<'_, WorkspaceSourceAttachment, Self::Error> {
        Box::pin(async move {
            let row = sqlx::query_as::<_, AttachmentRow>(
                "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
SELECT $1 AS workspace_id, $2 AS source_id, $3 AS attached_at
FROM workspaces AS workspace
CROSS JOIN sources AS source
WHERE
    workspace.id = $1
    AND workspace.status = 'active'
    AND source.id = $2
    AND source.availability <> 'removed'
ON CONFLICT (workspace_id, source_id) DO UPDATE
SET attached_at = workspace_sources.attached_at
RETURNING
    workspace_id,
    source_id,
    attached_at
",
            )
            .bind(command.workspace_id)
            .bind(command.source_id)
            .bind(command.attached_at)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(WorkspaceError::WorkspaceOrSourceNotFound)?;
            row.into_attachment()
        })
    }

    fn detach_source(
        &self,
        command: DetachWorkspaceSource,
    ) -> WorkspaceFuture<'_, bool, Self::Error> {
        Box::pin(async move {
            let result = sqlx::query(
                "DELETE FROM workspace_sources
WHERE workspace_id = $1 AND source_id = $2
",
            )
            .bind(command.workspace_id)
            .bind(command.source_id)
            .execute(&self.pool)
            .await?;
            Ok(result.rows_affected() == 1)
        })
    }

    fn list_source_attachments(
        &self,
        workspace_id: Uuid,
    ) -> WorkspaceFuture<'_, Vec<WorkspaceSourceAttachment>, Self::Error> {
        Box::pin(async move {
            let exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(
    SELECT 1 FROM workspaces WHERE id = $1 AND status = 'active'
)
",
            )
            .bind(workspace_id)
            .fetch_one(&self.pool)
            .await?;
            if !exists {
                return Err(WorkspaceError::WorkspaceNotFound);
            }
            let rows = sqlx::query_as::<_, AttachmentRow>(
                "SELECT
    workspace_id,
    source_id,
    attached_at
FROM workspace_sources
WHERE workspace_id = $1
ORDER BY attached_at, source_id
",
            )
            .bind(workspace_id)
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(AttachmentRow::into_attachment)
                .collect()
        })
    }
}

fn map_workspace_insert_error(error: sqlx::Error) -> WorkspaceError {
    if error
        .as_database_error()
        .and_then(|database| database.code())
        .as_deref()
        == Some("23505")
    {
        WorkspaceError::AlreadyExists
    } else {
        WorkspaceError::Database(error)
    }
}

#[derive(FromRow)]
struct WorkspaceRow {
    id: Uuid,
    name: String,
    created_at: time::OffsetDateTime,
    updated_at: time::OffsetDateTime,
}

impl WorkspaceRow {
    fn into_snapshot(self) -> WorkspaceSnapshot {
        WorkspaceSnapshot {
            id: self.id,
            name: self.name,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(FromRow)]
struct AttachmentRow {
    workspace_id: Uuid,
    source_id: Uuid,
    attached_at: time::OffsetDateTime,
}

impl AttachmentRow {
    fn into_attachment(self) -> Result<WorkspaceSourceAttachment, WorkspaceError> {
        Ok(WorkspaceSourceAttachment {
            workspace_id: self.workspace_id,
            source_id: self.source_id,
            attached_at: self.attached_at,
        })
    }
}

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("workspace name must not be empty")]
    InvalidName,
    #[error("workspace already exists")]
    AlreadyExists,
    #[error("workspace was not found")]
    WorkspaceNotFound,
    #[error("workspace or attachable source was not found")]
    WorkspaceOrSourceNotFound,
    #[error("source is not attached to the workspace")]
    SourceNotAttached,
    #[error("workspace data is invalid")]
    InvalidStoredData,
    #[error("workspace database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoApplicationError for WorkspaceError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::InvalidName => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "workspace is invalid",
                "name",
                "required",
                "workspace name must not be empty",
            ),
            Self::AlreadyExists => ApplicationError::new(
                ApplicationErrorCode::WorkspaceAlreadyExists,
                "workspace already exists",
            ),
            Self::WorkspaceNotFound => ApplicationError::new(
                ApplicationErrorCode::WorkspaceNotFound,
                "workspace was not found",
            ),
            Self::WorkspaceOrSourceNotFound => ApplicationError::new(
                ApplicationErrorCode::WorkspaceOrSourceNotFound,
                "workspace or attachable source was not found",
            ),
            Self::SourceNotAttached => ApplicationError::new(
                ApplicationErrorCode::SourceNotAttached,
                "source is not attached to the workspace",
            ),
            Self::InvalidStoredData => {
                ApplicationError::new(ApplicationErrorCode::Internal, "workspace data is invalid")
            }
            Self::Database(_) => {
                ApplicationError::new(ApplicationErrorCode::Internal, "workspace operation failed")
            }
        }
    }
}
