use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::synchronization::{
    SyncPolicyDocument, SyncPolicyError, validate_sync_policy_hash,
};

use super::{
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    facade::ApplicationFuture,
};

pub type SyncPolicyFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, SyncPolicyLifecycleError<E>>> + Send + 'a>>;

pub const MAX_SYNC_POLICY_PAGE_SIZE: u32 = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPolicyRevisionSnapshot {
    pub policy_id: Uuid,
    pub workspace_id: Uuid,
    pub revision: u64,
    pub document: SyncPolicyDocument,
    pub content_hash: String,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySyncPolicies {
    pub workspace_id: Uuid,
    pub limit: u32,
    pub after_policy_id: Option<Uuid>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncPolicyPage {
    pub policies: Vec<SyncPolicyRevisionSnapshot>,
    pub next_cursor: Option<Uuid>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreSyncPolicyRevision {
    pub policy_id: Uuid,
    pub workspace_id: Uuid,
    pub revision: u64,
    pub document: SyncPolicyDocument,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendSyncPolicyRevision {
    pub expected_head: String,
    pub revision: StoreSyncPolicyRevision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSyncPolicy {
    pub policy_id: Uuid,
    pub workspace_id: Uuid,
    pub document: SyncPolicyDocument,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateSyncPolicy {
    pub policy_id: Uuid,
    pub workspace_id: Uuid,
    pub base_content_hash: String,
    pub document: SyncPolicyDocument,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncPolicyLifecycleError<E> {
    InvalidPolicy(SyncPolicyError),
    InvalidPage,
    PolicyNotFound,
    HeadMoved { current_head: String },
    RevisionOverflow,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncPolicyRepository: Send + Sync {
    type Error;

    fn get_head(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> SyncPolicyFuture<'_, Option<SyncPolicyRevisionSnapshot>, Self::Error>;

    fn list_heads(
        &self,
        query: QuerySyncPolicies,
    ) -> SyncPolicyFuture<'_, SyncPolicyPage, Self::Error>;

    fn create_policy(
        &self,
        revision: StoreSyncPolicyRevision,
    ) -> SyncPolicyFuture<'_, SyncPolicyRevisionSnapshot, Self::Error>;

    fn append_revision(
        &self,
        revision: AppendSyncPolicyRevision,
    ) -> SyncPolicyFuture<'_, SyncPolicyRevisionSnapshot, Self::Error>;
}

pub trait SyncPolicyApi: Send + Sync {
    fn list_policies(&self, query: QuerySyncPolicies) -> ApplicationFuture<'_, SyncPolicyPage>;
    fn get_policy(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot>;
    fn create_policy(
        &self,
        command: CreateSyncPolicy,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot>;
    fn update_policy(
        &self,
        command: UpdateSyncPolicy,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot>;
}

#[derive(Clone)]
pub struct SyncPolicyService<R> {
    repository: R,
}

impl<R> SyncPolicyService<R>
where
    R: SyncPolicyRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn head(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> Result<SyncPolicyRevisionSnapshot, SyncPolicyLifecycleError<R::Error>> {
        let snapshot = self
            .repository
            .get_head(workspace_id, policy_id)
            .await?
            .ok_or(SyncPolicyLifecycleError::PolicyNotFound)?;
        validate_snapshot(snapshot, workspace_id, policy_id, None)
    }

    pub async fn list(
        &self,
        query: QuerySyncPolicies,
    ) -> Result<SyncPolicyPage, SyncPolicyLifecycleError<R::Error>> {
        if query.limit == 0 || query.limit > MAX_SYNC_POLICY_PAGE_SIZE {
            return Err(SyncPolicyLifecycleError::InvalidPage);
        }
        let workspace_id = query.workspace_id;
        let limit = query.limit;
        let page = self.repository.list_heads(query).await?;
        if page.policies.len() > limit as usize
            || (page.policies.len() < limit as usize && page.next_cursor.is_some())
            || page
                .policies
                .windows(2)
                .any(|policies| policies[0].policy_id >= policies[1].policy_id)
            || page.policies.iter().any(|policy| {
                validate_snapshot::<R::Error>(policy.clone(), workspace_id, policy.policy_id, None)
                    .is_err()
            })
            || page.next_cursor.is_some_and(|cursor| {
                page.policies
                    .last()
                    .is_none_or(|policy| policy.policy_id != cursor)
            })
        {
            return Err(SyncPolicyLifecycleError::RepositoryInvariant);
        }
        Ok(page)
    }

    pub async fn create(
        &self,
        command: CreateSyncPolicy,
    ) -> Result<SyncPolicyRevisionSnapshot, SyncPolicyLifecycleError<R::Error>> {
        let workspace_id = command.workspace_id;
        let policy_id = command.policy_id;
        let snapshot = self
            .repository
            .create_policy(StoreSyncPolicyRevision {
                policy_id: command.policy_id,
                workspace_id: command.workspace_id,
                revision: 1,
                document: command.document,
                created_at: command.created_at,
            })
            .await?;
        let snapshot = validate_snapshot(snapshot, workspace_id, policy_id, Some(1))?;
        tracing::info!(
            workspace.id = %workspace_id,
            sync.policy.id = %policy_id,
            sync.policy.revision = snapshot.revision,
            "synchronization policy created"
        );
        Ok(snapshot)
    }

    pub async fn update(
        &self,
        command: UpdateSyncPolicy,
    ) -> Result<SyncPolicyRevisionSnapshot, SyncPolicyLifecycleError<R::Error>> {
        let workspace_id = command.workspace_id;
        let policy_id = command.policy_id;
        validate_sync_policy_hash(&command.base_content_hash)
            .map_err(SyncPolicyLifecycleError::InvalidPolicy)?;
        let current = self
            .repository
            .get_head(command.workspace_id, command.policy_id)
            .await?
            .ok_or(SyncPolicyLifecycleError::PolicyNotFound)?;
        let current = validate_snapshot(current, command.workspace_id, command.policy_id, None)?;
        if current.content_hash != command.base_content_hash {
            return Err(SyncPolicyLifecycleError::HeadMoved {
                current_head: current.content_hash,
            });
        }
        let revision = current
            .revision
            .checked_add(1)
            .ok_or(SyncPolicyLifecycleError::RevisionOverflow)?;
        let snapshot = self
            .repository
            .append_revision(AppendSyncPolicyRevision {
                expected_head: command.base_content_hash,
                revision: StoreSyncPolicyRevision {
                    policy_id: command.policy_id,
                    workspace_id: command.workspace_id,
                    revision,
                    document: command.document,
                    created_at: command.updated_at,
                },
            })
            .await?;
        let snapshot = validate_snapshot(snapshot, workspace_id, policy_id, Some(revision))?;
        tracing::info!(
            workspace.id = %workspace_id,
            sync.policy.id = %policy_id,
            sync.policy.revision = snapshot.revision,
            "synchronization policy updated"
        );
        Ok(snapshot)
    }
}

impl<R> SyncPolicyApi for SyncPolicyService<R>
where
    R: SyncPolicyRepository + 'static,
    R::Error: IntoApplicationError,
{
    fn list_policies(&self, query: QuerySyncPolicies) -> ApplicationFuture<'_, SyncPolicyPage> {
        Box::pin(async move { self.list(query).await.map_err(sync_policy_error) })
    }

    fn get_policy(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot> {
        Box::pin(async move {
            self.head(workspace_id, policy_id)
                .await
                .map_err(sync_policy_error)
        })
    }

    fn create_policy(
        &self,
        command: CreateSyncPolicy,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot> {
        Box::pin(async move { self.create(command).await.map_err(sync_policy_error) })
    }

    fn update_policy(
        &self,
        command: UpdateSyncPolicy,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot> {
        Box::pin(async move { self.update(command).await.map_err(sync_policy_error) })
    }
}

fn sync_policy_error<E>(error: SyncPolicyLifecycleError<E>) -> ApplicationError
where
    E: IntoApplicationError,
{
    match error {
        SyncPolicyLifecycleError::InvalidPolicy(error) => ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "synchronization policy is invalid",
            "document",
            error.code(),
            error.to_string(),
        ),
        SyncPolicyLifecycleError::InvalidPage => ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            "synchronization policy page is invalid",
        ),
        SyncPolicyLifecycleError::PolicyNotFound => ApplicationError::new(
            ApplicationErrorCode::SyncPolicyNotFound,
            "synchronization policy was not found",
        ),
        SyncPolicyLifecycleError::HeadMoved { current_head } => ApplicationError::field(
            ApplicationErrorCode::SyncPolicyHeadMoved,
            "synchronization policy head moved",
            "baseContentHash",
            "head_moved",
            format!("current synchronization policy head is {current_head}"),
        ),
        SyncPolicyLifecycleError::RevisionOverflow
        | SyncPolicyLifecycleError::RepositoryInvariant => ApplicationError::new(
            ApplicationErrorCode::Internal,
            "synchronization policy operation failed",
        ),
        SyncPolicyLifecycleError::Repository(error) => error.into_application_error(),
    }
}

fn validate_snapshot<E>(
    snapshot: SyncPolicyRevisionSnapshot,
    workspace_id: Uuid,
    policy_id: Uuid,
    revision: Option<u64>,
) -> Result<SyncPolicyRevisionSnapshot, SyncPolicyLifecycleError<E>> {
    if snapshot.workspace_id != workspace_id
        || snapshot.policy_id != policy_id
        || snapshot.revision == 0
        || revision.is_some_and(|revision| snapshot.revision != revision)
        || snapshot.content_hash != snapshot.document.content_hash()
    {
        return Err(SyncPolicyLifecycleError::RepositoryInvariant);
    }
    Ok(snapshot)
}

impl<E> From<E> for SyncPolicyLifecycleError<E> {
    fn from(error: E) -> Self {
        Self::Repository(error)
    }
}

#[cfg(test)]
#[path = "sync_policy_tests.rs"]
mod tests;
