use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

pub type WorkspaceFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MappingRootPolicy {
    ProviderRecommended,
    NamespaceAllowlist(Vec<String>),
}

impl MappingRootPolicy {
    pub fn may_select_namespace(&self, namespace: &str) -> bool {
        match self {
            Self::ProviderRecommended => true,
            Self::NamespaceAllowlist(namespaces) => {
                namespaces.iter().any(|candidate| candidate == namespace)
            }
        }
    }

    pub fn selects(&self, namespace: &str, provider_recommended: bool) -> bool {
        match self {
            Self::ProviderRecommended => provider_recommended,
            Self::NamespaceAllowlist(namespaces) => {
                namespaces.iter().any(|candidate| candidate == namespace)
            }
        }
    }
}

impl From<Option<Vec<String>>> for MappingRootPolicy {
    fn from(namespaces: Option<Vec<String>>) -> Self {
        namespaces.map_or(Self::ProviderRecommended, Self::NamespaceAllowlist)
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
    pub mapping_root_namespace_allowlist: Option<Vec<String>>,
    pub mapping_root_policy_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingRootNamespaceStat {
    pub namespace: String,
    pub candidate_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceSourceMappingRootPolicy {
    pub attachment: WorkspaceSourceAttachment,
    pub namespaces: Vec<MappingRootNamespaceStat>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateWorkspaceSourceMappingRootPolicy {
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub namespaces: Option<Vec<String>>,
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

    fn get_mapping_root_policy(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
    ) -> WorkspaceFuture<'_, WorkspaceSourceMappingRootPolicy, Self::Error>;

    fn update_mapping_root_policy(
        &self,
        command: UpdateWorkspaceSourceMappingRootPolicy,
    ) -> WorkspaceFuture<'_, WorkspaceSourceMappingRootPolicy, Self::Error>;
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

    pub async fn mapping_root_policy(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
    ) -> Result<WorkspaceSourceMappingRootPolicy, R::Error> {
        self.repository
            .get_mapping_root_policy(workspace_id, source_id)
            .await
    }

    pub async fn update_mapping_root_policy(
        &self,
        command: UpdateWorkspaceSourceMappingRootPolicy,
    ) -> Result<WorkspaceSourceMappingRootPolicy, R::Error> {
        self.repository.update_mapping_root_policy(command).await
    }
}

#[cfg(test)]
mod tests {
    use super::MappingRootPolicy;

    #[test]
    fn mapping_root_policy_selects_recommendations_or_namespaces() {
        let provider = MappingRootPolicy::ProviderRecommended;
        assert!(provider.may_select_namespace("example.media"));
        assert!(provider.selects("example.media", true));
        assert!(!provider.selects("example.media", false));

        let custom = MappingRootPolicy::NamespaceAllowlist(vec!["example.media".to_owned()]);
        assert!(custom.may_select_namespace("example.media"));
        assert!(custom.selects("example.media", false));
        assert!(!custom.selects("example.other", true));

        let disabled = MappingRootPolicy::NamespaceAllowlist(Vec::new());
        assert!(!disabled.may_select_namespace("example.media"));
        assert!(!disabled.selects("example.media", true));
    }
}
