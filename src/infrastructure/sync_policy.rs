use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    application::sync_policy::{
        AppendSyncPolicyRevision, QuerySyncPolicies, StoreSyncPolicyRevision, SyncPolicyFuture,
        SyncPolicyLifecycleError, SyncPolicyPage, SyncPolicyRepository, SyncPolicyRevisionSnapshot,
    },
    domain::synchronization::{StateRegistry, SyncPolicyDocument},
};

type PolicyResult<T> = Result<T, SyncPolicyLifecycleError<PostgresSyncPolicyRepositoryError>>;

#[derive(Clone)]
pub struct PostgresSyncPolicyRepository {
    pool: PgPool,
}

impl PostgresSyncPolicyRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn head(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> PolicyResult<Option<SyncPolicyRevisionSnapshot>> {
        let revision = sqlx::query_scalar::<_, i64>(
            "SELECT head_revision
FROM sync_policies
WHERE id = $1 AND workspace_id = $2
",
        )
        .bind(policy_id)
        .bind(workspace_id)
        .fetch_optional(&self.pool)
        .await?;
        match revision {
            Some(revision) => self.revision(workspace_id, policy_id, revision).await,
            None => Ok(None),
        }
    }

    async fn list(&self, query: QuerySyncPolicies) -> PolicyResult<SyncPolicyPage> {
        let fetch_limit = i64::from(query.limit) + 1;
        let mut rows = sqlx::query_as::<_, RevisionRow>(
            "SELECT
    revisions.policy_id,
    revisions.workspace_id,
    revisions.revision,
    revisions.content_hash,
    revisions.canonical_json,
    revisions.created_at
FROM sync_policies AS policies
INNER JOIN sync_policy_revisions AS revisions
    ON
        revisions.policy_id = policies.id
        AND revisions.workspace_id = policies.workspace_id
        AND revisions.revision = policies.head_revision
WHERE
    policies.workspace_id = $1
    AND ($2::uuid IS NULL OR policies.id > $2)
ORDER BY policies.id
LIMIT $3
",
        )
        .bind(query.workspace_id)
        .bind(query.after_policy_id)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > query.limit as usize;
        rows.truncate(query.limit as usize);
        let mut policies = Vec::with_capacity(rows.len());
        for row in rows {
            policies.push(self.snapshot(row).await?);
        }
        let next_cursor = has_more.then(|| {
            policies
                .last()
                .expect("non-empty bounded policy page has a cursor")
                .policy_id
        });
        Ok(SyncPolicyPage {
            policies,
            next_cursor,
        })
    }

    async fn revision(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
        revision: i64,
    ) -> PolicyResult<Option<SyncPolicyRevisionSnapshot>> {
        let row = sqlx::query_as::<_, RevisionRow>(
            "SELECT
    policy_id,
    workspace_id,
    revision,
    content_hash,
    canonical_json,
    created_at
FROM sync_policy_revisions
WHERE policy_id = $1 AND workspace_id = $2 AND revision = $3
",
        )
        .bind(policy_id)
        .bind(workspace_id)
        .bind(revision)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(self.snapshot(row).await?))
    }

    async fn snapshot(&self, row: RevisionRow) -> PolicyResult<SyncPolicyRevisionSnapshot> {
        let json = std::str::from_utf8(&row.canonical_json)
            .map_err(|_| repository_error(PostgresSyncPolicyRepositoryError::CorruptDocument))?;
        let document = SyncPolicyDocument::from_json(json, StateRegistry::v1())
            .map_err(|_| repository_error(PostgresSyncPolicyRepositoryError::CorruptDocument))?;
        if document.content_hash() != row.content_hash {
            return Err(repository_error(
                PostgresSyncPolicyRepositoryError::CorruptDocument,
            ));
        }

        let roles = sqlx::query_as::<_, SourceRoleRow>(
            "SELECT source_id, role
FROM sync_policy_source_roles
WHERE policy_id = $1 AND workspace_id = $2 AND revision = $3
ORDER BY role, source_id
",
        )
        .bind(row.policy_id)
        .bind(row.workspace_id)
        .bind(row.revision)
        .fetch_all(&self.pool)
        .await?;
        let contributors = roles
            .iter()
            .filter(|role| role.role == "contributor")
            .map(|role| role.source_id)
            .collect::<Vec<_>>();
        let receivers = roles
            .iter()
            .filter(|role| role.role == "receiver")
            .map(|role| role.source_id)
            .collect::<Vec<_>>();
        if roles
            .iter()
            .any(|role| role.role != "contributor" && role.role != "receiver")
            || contributors != document.contributors()
            || receivers != document.receivers()
        {
            return Err(repository_error(
                PostgresSyncPolicyRepositoryError::CorruptSourceRoles,
            ));
        }

        Ok(SyncPolicyRevisionSnapshot {
            policy_id: row.policy_id,
            workspace_id: row.workspace_id,
            revision: u64::try_from(row.revision).map_err(|_| {
                repository_error(PostgresSyncPolicyRepositoryError::CorruptDocument)
            })?,
            document,
            content_hash: row.content_hash,
            created_at: row.created_at,
        })
    }

    async fn create(
        &self,
        revision: StoreSyncPolicyRevision,
    ) -> PolicyResult<SyncPolicyRevisionSnapshot> {
        if revision.revision != 1 {
            return Err(repository_error(
                PostgresSyncPolicyRepositoryError::InvalidRevisionSequence,
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM sync_policies WHERE id = $1)
",
        )
        .bind(revision.policy_id)
        .fetch_one(&mut *transaction)
        .await?;
        if exists {
            return Err(repository_error(
                PostgresSyncPolicyRepositoryError::DuplicatePolicy,
            ));
        }
        ensure_sources_attached(&mut transaction, revision.workspace_id, &revision.document)
            .await?;
        let content_hash = revision.document.content_hash();
        sqlx::query(
            "INSERT INTO sync_policies (
    id, workspace_id, head_revision, head_content_hash, created_at, updated_at
) VALUES ($1, $2, 1, $3, $4, $4)
",
        )
        .bind(revision.policy_id)
        .bind(revision.workspace_id)
        .bind(&content_hash)
        .bind(revision.created_at)
        .execute(&mut *transaction)
        .await?;
        insert_revision(&mut transaction, &revision).await?;
        transaction.commit().await?;
        self.revision(revision.workspace_id, revision.policy_id, 1)
            .await?
            .ok_or_else(|| repository_error(PostgresSyncPolicyRepositoryError::MissingWrite))
    }

    async fn append(
        &self,
        command: AppendSyncPolicyRevision,
    ) -> PolicyResult<SyncPolicyRevisionSnapshot> {
        let revision = command.revision;
        let proposed_revision = i64::try_from(revision.revision)
            .map_err(|_| repository_error(PostgresSyncPolicyRepositoryError::RevisionOverflow))?;
        let mut transaction = self.pool.begin().await?;
        let head = sqlx::query_as::<_, HeadRow>(
            "SELECT head_revision, head_content_hash
FROM sync_policies
WHERE id = $1 AND workspace_id = $2
FOR UPDATE
",
        )
        .bind(revision.policy_id)
        .bind(revision.workspace_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SyncPolicyLifecycleError::PolicyNotFound)?;
        if head.head_content_hash != command.expected_head {
            return Err(SyncPolicyLifecycleError::HeadMoved {
                current_head: head.head_content_hash,
            });
        }
        let expected_revision = head
            .head_revision
            .checked_add(1)
            .ok_or_else(|| repository_error(PostgresSyncPolicyRepositoryError::RevisionOverflow))?;
        if proposed_revision != expected_revision {
            return Err(repository_error(
                PostgresSyncPolicyRepositoryError::InvalidRevisionSequence,
            ));
        }
        ensure_sources_attached(&mut transaction, revision.workspace_id, &revision.document)
            .await?;
        let duplicate = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1
    FROM sync_policy_revisions
    WHERE policy_id = $1 AND content_hash = $2
)
",
        )
        .bind(revision.policy_id)
        .bind(revision.document.content_hash())
        .fetch_one(&mut *transaction)
        .await?;
        if duplicate {
            return Err(repository_error(
                PostgresSyncPolicyRepositoryError::DuplicateRevision,
            ));
        }
        insert_revision(&mut transaction, &revision).await?;
        let content_hash = revision.document.content_hash();
        sqlx::query(
            "UPDATE sync_policies
SET head_revision = $1, head_content_hash = $2, updated_at = $3
WHERE id = $4 AND workspace_id = $5
",
        )
        .bind(proposed_revision)
        .bind(&content_hash)
        .bind(revision.created_at)
        .bind(revision.policy_id)
        .bind(revision.workspace_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.revision(revision.workspace_id, revision.policy_id, proposed_revision)
            .await?
            .ok_or_else(|| repository_error(PostgresSyncPolicyRepositoryError::MissingWrite))
    }
}

impl SyncPolicyRepository for PostgresSyncPolicyRepository {
    type Error = PostgresSyncPolicyRepositoryError;

    fn get_head(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> SyncPolicyFuture<'_, Option<SyncPolicyRevisionSnapshot>, Self::Error> {
        Box::pin(self.head(workspace_id, policy_id))
    }

    fn list_heads(
        &self,
        query: QuerySyncPolicies,
    ) -> SyncPolicyFuture<'_, SyncPolicyPage, Self::Error> {
        Box::pin(self.list(query))
    }

    fn create_policy(
        &self,
        revision: StoreSyncPolicyRevision,
    ) -> SyncPolicyFuture<'_, SyncPolicyRevisionSnapshot, Self::Error> {
        Box::pin(self.create(revision))
    }

    fn append_revision(
        &self,
        revision: AppendSyncPolicyRevision,
    ) -> SyncPolicyFuture<'_, SyncPolicyRevisionSnapshot, Self::Error> {
        Box::pin(self.append(revision))
    }
}

async fn ensure_sources_attached(
    transaction: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    document: &SyncPolicyDocument,
) -> PolicyResult<()> {
    let mut source_ids = document.contributors().to_vec();
    source_ids.extend_from_slice(document.receivers());
    source_ids.sort_unstable();
    source_ids.dedup();
    let attached = sqlx::query_scalar::<_, Uuid>(
        "SELECT source_id
FROM workspace_sources
WHERE workspace_id = $1 AND source_id = ANY($2)
ORDER BY source_id
",
    )
    .bind(workspace_id)
    .bind(&source_ids)
    .fetch_all(&mut **transaction)
    .await?;
    if let Some(source_id) = source_ids
        .into_iter()
        .find(|source_id| attached.binary_search(source_id).is_err())
    {
        return Err(repository_error(
            PostgresSyncPolicyRepositoryError::SourceNotAttached { source_id },
        ));
    }
    Ok(())
}

async fn insert_revision(
    transaction: &mut Transaction<'_, Postgres>,
    revision: &StoreSyncPolicyRevision,
) -> PolicyResult<()> {
    let revision_number = i64::try_from(revision.revision)
        .map_err(|_| repository_error(PostgresSyncPolicyRepositoryError::RevisionOverflow))?;
    let content_hash = revision.document.content_hash();
    sqlx::query(
        "INSERT INTO sync_policy_revisions (
    policy_id,
    workspace_id,
    revision,
    content_hash,
    schema_name,
    canonical_json,
    created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7)
",
    )
    .bind(revision.policy_id)
    .bind(revision.workspace_id)
    .bind(revision_number)
    .bind(&content_hash)
    .bind(revision.document.schema())
    .bind(revision.document.canonical_json())
    .bind(revision.created_at)
    .execute(&mut **transaction)
    .await?;
    for source_id in revision.document.contributors() {
        insert_source_role(
            transaction,
            revision,
            revision_number,
            *source_id,
            "contributor",
        )
        .await?;
    }
    for source_id in revision.document.receivers() {
        insert_source_role(
            transaction,
            revision,
            revision_number,
            *source_id,
            "receiver",
        )
        .await?;
    }
    Ok(())
}

async fn insert_source_role(
    transaction: &mut Transaction<'_, Postgres>,
    revision: &StoreSyncPolicyRevision,
    revision_number: i64,
    source_id: Uuid,
    role: &'static str,
) -> PolicyResult<()> {
    sqlx::query(
        "INSERT INTO sync_policy_source_roles (
    policy_id, workspace_id, revision, source_id, role
) VALUES ($1, $2, $3, $4, $5)
",
    )
    .bind(revision.policy_id)
    .bind(revision.workspace_id)
    .bind(revision_number)
    .bind(source_id)
    .bind(role)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn repository_error(
    error: PostgresSyncPolicyRepositoryError,
) -> SyncPolicyLifecycleError<PostgresSyncPolicyRepositoryError> {
    SyncPolicyLifecycleError::Repository(error)
}

impl From<sqlx::Error> for SyncPolicyLifecycleError<PostgresSyncPolicyRepositoryError> {
    fn from(error: sqlx::Error) -> Self {
        repository_error(error.into())
    }
}

#[derive(Debug, Error)]
pub enum PostgresSyncPolicyRepositoryError {
    #[error("sync policy database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("stored sync policy document is corrupt")]
    CorruptDocument,
    #[error("stored sync policy source roles are corrupt")]
    CorruptSourceRoles,
    #[error("sync policy already exists")]
    DuplicatePolicy,
    #[error("sync policy revision content already exists")]
    DuplicateRevision,
    #[error("sync policy revision sequence is invalid")]
    InvalidRevisionSequence,
    #[error("sync policy revision overflow")]
    RevisionOverflow,
    #[error("sync policy source {source_id} is not attached to the workspace")]
    SourceNotAttached { source_id: Uuid },
    #[error("sync policy write could not be reloaded")]
    MissingWrite,
}

impl IntoApplicationError for PostgresSyncPolicyRepositoryError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::DuplicatePolicy => ApplicationError::new(
                ApplicationErrorCode::SyncPolicyAlreadyExists,
                "synchronization policy already exists",
            ),
            Self::SourceNotAttached { .. } => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "synchronization policy is invalid",
                "document",
                "source_not_attached",
                self.to_string(),
            ),
            Self::Database(_)
            | Self::CorruptDocument
            | Self::CorruptSourceRoles
            | Self::DuplicateRevision
            | Self::InvalidRevisionSequence
            | Self::RevisionOverflow
            | Self::MissingWrite => ApplicationError::new(
                ApplicationErrorCode::Internal,
                "synchronization policy database operation failed",
            ),
        }
    }
}

#[derive(FromRow)]
struct HeadRow {
    head_revision: i64,
    head_content_hash: String,
}

#[derive(FromRow)]
struct RevisionRow {
    policy_id: Uuid,
    workspace_id: Uuid,
    revision: i64,
    content_hash: String,
    canonical_json: Vec<u8>,
    created_at: OffsetDateTime,
}

#[derive(FromRow)]
struct SourceRoleRow {
    source_id: Uuid,
    role: String,
}
