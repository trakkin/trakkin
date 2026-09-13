use std::collections::BTreeMap;

use sqlx::{FromRow, PgPool};
use thiserror::Error;
use trakkin_mapping::PortableEndpoint;
use uuid::Uuid;

use crate::application::error::{ApplicationError, ApplicationErrorCode, IntoApplicationError};
use crate::application::workspaces::{
    AttachWorkspaceSource, CreateWorkspace, DetachWorkspaceSource, MappingRootNamespaceStat,
    UpdateWorkspaceSourceMappingRootPolicy, WorkspaceFuture, WorkspaceRepository,
    WorkspaceSnapshot, WorkspaceSourceAttachment, WorkspaceSourceMappingRootPolicy,
};

#[derive(Clone)]
pub struct PostgresWorkspaceRepository {
    pool: PgPool,
}

impl PostgresWorkspaceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn load_mapping_root_policy(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
    ) -> Result<WorkspaceSourceMappingRootPolicy, WorkspaceError> {
        let attachment = sqlx::query_as::<_, AttachmentRow>(
            "SELECT
    workspace_id,
    source_id,
    attached_at,
    mapping_root_namespace_allowlist,
    mapping_root_policy_generation
FROM workspace_sources
WHERE workspace_id = $1 AND source_id = $2
",
        )
        .bind(workspace_id)
        .bind(source_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(WorkspaceError::SourceNotAttached)?
        .into_attachment()?;
        let rows = sqlx::query_as::<_, MappingRootNamespaceStatRow>(
            "SELECT
    reference_namespace AS namespace,
    COUNT(*) AS candidate_count
FROM catalog_item_references
WHERE source_id = $1
GROUP BY reference_namespace
ORDER BY reference_namespace
",
        )
        .bind(source_id)
        .fetch_all(&self.pool)
        .await?;
        let mut namespaces = rows
            .into_iter()
            .map(|row| {
                let candidate_count = u64::try_from(row.candidate_count)
                    .map_err(|_| WorkspaceError::InvalidStoredData)?;
                Ok((
                    row.namespace.clone(),
                    MappingRootNamespaceStat {
                        namespace: row.namespace,
                        candidate_count,
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, WorkspaceError>>()?;
        if let Some(allowlist) = &attachment.mapping_root_namespace_allowlist {
            for namespace in allowlist {
                namespaces
                    .entry(namespace.clone())
                    .or_insert_with(|| MappingRootNamespaceStat {
                        namespace: namespace.clone(),
                        candidate_count: 0,
                    });
            }
        }
        Ok(WorkspaceSourceMappingRootPolicy {
            attachment,
            namespaces: namespaces.into_values().collect(),
        })
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
    attached_at,
    mapping_root_namespace_allowlist,
    mapping_root_policy_generation
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
    attached_at,
    mapping_root_namespace_allowlist,
    mapping_root_policy_generation
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

    fn get_mapping_root_policy(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
    ) -> WorkspaceFuture<'_, WorkspaceSourceMappingRootPolicy, Self::Error> {
        Box::pin(self.load_mapping_root_policy(workspace_id, source_id))
    }

    fn update_mapping_root_policy(
        &self,
        command: UpdateWorkspaceSourceMappingRootPolicy,
    ) -> WorkspaceFuture<'_, WorkspaceSourceMappingRootPolicy, Self::Error> {
        Box::pin(async move {
            let namespaces = normalize_mapping_root_namespaces(command.namespaces)?;
            let result = sqlx::query(
                "UPDATE workspace_sources
SET
    mapping_root_namespace_allowlist = $3,
    mapping_root_policy_generation = CASE
        WHEN mapping_root_namespace_allowlist IS DISTINCT FROM $3
            THEN mapping_root_policy_generation + 1
        ELSE mapping_root_policy_generation
    END
WHERE workspace_id = $1 AND source_id = $2
",
            )
            .bind(command.workspace_id)
            .bind(command.source_id)
            .bind(namespaces)
            .execute(&self.pool)
            .await?;
            if result.rows_affected() == 0 {
                return Err(WorkspaceError::SourceNotAttached);
            }
            self.load_mapping_root_policy(command.workspace_id, command.source_id)
                .await
        })
    }
}

fn normalize_mapping_root_namespaces(
    namespaces: Option<Vec<String>>,
) -> Result<Option<Vec<String>>, WorkspaceError> {
    namespaces
        .map(|namespaces| {
            let mut normalized = namespaces
                .into_iter()
                .map(|namespace| namespace.trim().to_owned())
                .collect::<Vec<_>>();
            if normalized
                .iter()
                .any(|namespace| PortableEndpoint::from_parts(namespace, b"_", None).is_err())
            {
                return Err(WorkspaceError::InvalidMappingRootNamespace);
            }
            normalized.sort_unstable();
            normalized.dedup();
            Ok(normalized)
        })
        .transpose()
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
    mapping_root_namespace_allowlist: Option<Vec<String>>,
    mapping_root_policy_generation: i64,
}

impl AttachmentRow {
    fn into_attachment(self) -> Result<WorkspaceSourceAttachment, WorkspaceError> {
        Ok(WorkspaceSourceAttachment {
            workspace_id: self.workspace_id,
            source_id: self.source_id,
            attached_at: self.attached_at,
            mapping_root_namespace_allowlist: self.mapping_root_namespace_allowlist,
            mapping_root_policy_generation: u64::try_from(self.mapping_root_policy_generation)
                .ok()
                .filter(|generation| *generation > 0)
                .ok_or(WorkspaceError::InvalidStoredData)?,
        })
    }
}

#[derive(FromRow)]
struct MappingRootNamespaceStatRow {
    namespace: String,
    candidate_count: i64,
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
    #[error("mapping root namespace is invalid")]
    InvalidMappingRootNamespace,
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
            Self::InvalidMappingRootNamespace => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "mapping root policy is invalid",
                "namespaces",
                "invalid_namespace",
                "mapping root namespaces must be canonical portable-reference namespaces",
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
