use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

pub type ProviderFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProviderLaunchError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderLaunchTarget {
    pub provider_id: String,
    pub version: String,
    pub artifact_id: Uuid,
    pub executable: PathBuf,
    pub working_directory: PathBuf,
}

pub trait ProviderLaunchResolver: Clone + Send + Sync {
    fn resolve_provider(&self, provider_id: &str) -> ProviderFuture<'_, ProviderLaunchTarget>;
}

pub type ProviderApiFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, super::error::ApplicationError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderUpdatePolicy {
    Manual,
    Automatic,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AvailableProviderSnapshot {
    pub provider_id: String,
    pub versions: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSourceSnapshot {
    pub id: Uuid,
    pub repository_url: String,
    pub available_providers: Vec<AvailableProviderSnapshot>,
    pub trusted_at: OffsetDateTime,
    pub last_checked_at: Option<OffsetDateTime>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderUpdateSnapshot {
    pub version: String,
    pub discovered_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSnapshot {
    pub provider_id: String,
    pub source_id: Uuid,
    pub name: Option<String>,
    pub description: Option<String>,
    pub version: String,
    pub platform: String,
    pub index_digest: String,
    pub binary_digest: String,
    pub update_policy: ProviderUpdatePolicy,
    pub available_update: Option<ProviderUpdateSnapshot>,
    pub last_update_error: Option<String>,
    pub installed_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustProviderSource {
    pub source_id: Uuid,
    pub repository_url: String,
    pub trusted_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshProviderSource {
    pub source_id: Uuid,
    pub checked_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveProviderSource {
    pub source_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallProvider {
    pub artifact_id: Uuid,
    pub source_id: Uuid,
    pub provider_id: String,
    pub version: String,
    pub update_policy: ProviderUpdatePolicy,
    pub installed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateProvider {
    pub provider_id: String,
    pub artifact_id: Uuid,
    pub version: String,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeProviderUpdatePolicy {
    pub provider_id: String,
    pub update_policy: ProviderUpdatePolicy,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveProvider {
    pub provider_id: String,
}

pub trait ProviderRegistryApi: Send + Sync {
    fn ensure_provider_installed(&self, provider_id: &str) -> ProviderApiFuture<'_, ()>;
    fn list_provider_sources(&self) -> ProviderApiFuture<'_, Vec<ProviderSourceSnapshot>>;
    fn trust_provider_source(
        &self,
        command: TrustProviderSource,
    ) -> ProviderApiFuture<'_, ProviderSourceSnapshot>;
    fn refresh_provider_source(
        &self,
        command: RefreshProviderSource,
    ) -> ProviderApiFuture<'_, ProviderSourceSnapshot>;
    fn remove_provider_source(&self, command: RemoveProviderSource) -> ProviderApiFuture<'_, ()>;
    fn list_providers(&self) -> ProviderApiFuture<'_, Vec<ProviderSnapshot>>;
    fn install_provider(&self, command: InstallProvider)
    -> ProviderApiFuture<'_, ProviderSnapshot>;
    fn update_provider(&self, command: UpdateProvider) -> ProviderApiFuture<'_, ProviderSnapshot>;
    fn change_provider_update_policy(
        &self,
        command: ChangeProviderUpdatePolicy,
    ) -> ProviderApiFuture<'_, ProviderSnapshot>;
    fn remove_provider(&self, command: RemoveProvider) -> ProviderApiFuture<'_, ()>;
}

pub type SharedProviderRegistry = Arc<dyn ProviderRegistryApi>;

#[derive(Debug, Error)]
pub enum ProviderLaunchError {
    #[error("provider {0} is not installed")]
    NotInstalled(String),
    #[error("provider {provider_id} installation is invalid: {reason}")]
    InvalidInstallation { provider_id: String, reason: String },
    #[error("provider registry failed: {0}")]
    Registry(String),
    #[error("provider storage failed: {0}")]
    Storage(#[from] std::io::Error),
}
