use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

pub type ProviderFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProviderLaunchError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderLaunchTarget {
    pub provider_id: String,
    pub version: String,
    pub installation_id: Uuid,
    pub generation: Uuid,
    pub executable: PathBuf,
    pub working_directory: PathBuf,
}

pub trait ProviderLaunchResolver: Clone + Send + Sync {
    fn resolve_provider(&self, provider_id: &str) -> ProviderFuture<'_, ProviderLaunchTarget>;
}

pub type ProviderApiFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, super::error::ApplicationError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRepositorySnapshot {
    pub id: String,
    pub display_name: String,
    pub metadata_base_url: String,
    pub targets_base_url: String,
    pub official: bool,
    pub enabled: bool,
    pub catalog_sequence: Option<u64>,
    pub refreshed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReleaseSnapshot {
    pub repository_id: String,
    pub provider_id: String,
    pub name: String,
    pub summary: String,
    pub version: String,
    pub channels: Vec<String>,
    pub targets: Vec<String>,
    pub installed_targets: Vec<String>,
    pub active_target: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderInstallationSnapshot {
    pub id: Uuid,
    pub repository_id: String,
    pub provider_id: String,
    pub version: String,
    pub target: String,
    pub package_sha256: String,
    pub active: bool,
    pub generation: Option<Uuid>,
    pub installed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterProviderRepository {
    pub id: String,
    pub display_name: String,
    pub metadata_base_url: String,
    pub targets_base_url: String,
    pub trusted_root: Vec<u8>,
    pub trusted_root_sha256: String,
    pub registered_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshProviderRepository {
    pub repository_id: String,
    pub refreshed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallProvider {
    pub installation_id: Uuid,
    pub repository_id: String,
    pub provider_id: String,
    pub version: String,
    pub target: String,
    pub installed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivateProviderInstallation {
    pub provider_id: String,
    pub installation_id: Uuid,
    pub generation: Uuid,
    pub activated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveProviderInstallation {
    pub installation_id: Uuid,
}

pub trait ProviderRegistryApi: Send + Sync {
    fn ensure_provider_active(&self, provider_id: &str) -> ProviderApiFuture<'_, ()>;
    fn list_provider_repositories(&self) -> ProviderApiFuture<'_, Vec<ProviderRepositorySnapshot>>;
    fn register_provider_repository(
        &self,
        command: RegisterProviderRepository,
    ) -> ProviderApiFuture<'_, ProviderRepositorySnapshot>;
    fn refresh_provider_repository(
        &self,
        command: RefreshProviderRepository,
    ) -> ProviderApiFuture<'_, ProviderRepositorySnapshot>;
    fn list_provider_releases(&self) -> ProviderApiFuture<'_, Vec<ProviderReleaseSnapshot>>;
    fn list_provider_installations(
        &self,
    ) -> ProviderApiFuture<'_, Vec<ProviderInstallationSnapshot>>;
    fn install_provider(
        &self,
        command: InstallProvider,
    ) -> ProviderApiFuture<'_, ProviderInstallationSnapshot>;
    fn activate_provider_installation(
        &self,
        command: ActivateProviderInstallation,
    ) -> ProviderApiFuture<'_, ProviderInstallationSnapshot>;
    fn remove_provider_installation(
        &self,
        command: RemoveProviderInstallation,
    ) -> ProviderApiFuture<'_, ()>;
}

pub type SharedProviderRegistry = Arc<dyn ProviderRegistryApi>;

#[derive(Debug, Error)]
pub enum ProviderLaunchError {
    #[error("provider {0} has no active installation")]
    NotInstalled(String),
    #[error("provider {provider_id} installation is invalid: {reason}")]
    InvalidInstallation { provider_id: String, reason: String },
    #[error("provider registry failed: {0}")]
    Registry(String),
    #[error("provider storage failed: {0}")]
    Storage(#[from] std::io::Error),
}
