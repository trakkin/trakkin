use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::application::{
    error::ApplicationError,
    providers::{
        ProviderInstallationSnapshot, ProviderReleaseSnapshot, ProviderRepositorySnapshot,
    },
};

use super::timestamp;

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RegisterProviderRepositoryRequest {
    pub id: String,
    pub display_name: String,
    pub metadata_base_url: String,
    pub targets_base_url: String,
    pub trusted_root: Vec<u8>,
    pub trusted_root_sha256: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstallProviderRequest {
    pub repository_id: String,
    pub provider_id: String,
    pub version: String,
    pub target: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivateProviderRequest {
    pub installation_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderRepositoryResponse {
    pub id: String,
    pub display_name: String,
    pub metadata_base_url: String,
    pub targets_base_url: String,
    pub official: bool,
    pub enabled: bool,
    pub catalog_sequence: Option<u64>,
    pub refreshed_at: Option<String>,
}

impl ProviderRepositoryResponse {
    pub fn from_snapshot(snapshot: ProviderRepositorySnapshot) -> Result<Self, ApplicationError> {
        Ok(Self {
            id: snapshot.id,
            display_name: snapshot.display_name,
            metadata_base_url: snapshot.metadata_base_url,
            targets_base_url: snapshot.targets_base_url,
            official: snapshot.official,
            enabled: snapshot.enabled,
            catalog_sequence: snapshot.catalog_sequence,
            refreshed_at: snapshot.refreshed_at.map(timestamp).transpose()?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderReleaseResponse {
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

impl From<ProviderReleaseSnapshot> for ProviderReleaseResponse {
    fn from(snapshot: ProviderReleaseSnapshot) -> Self {
        Self {
            repository_id: snapshot.repository_id,
            provider_id: snapshot.provider_id,
            name: snapshot.name,
            summary: snapshot.summary,
            version: snapshot.version,
            channels: snapshot.channels,
            targets: snapshot.targets,
            installed_targets: snapshot.installed_targets,
            active_target: snapshot.active_target,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderInstallationResponse {
    pub id: Uuid,
    pub repository_id: String,
    pub provider_id: String,
    pub version: String,
    pub target: String,
    pub package_sha256: String,
    pub active: bool,
    pub generation: Option<Uuid>,
    pub installed_at: String,
}

impl ProviderInstallationResponse {
    pub fn from_snapshot(snapshot: ProviderInstallationSnapshot) -> Result<Self, ApplicationError> {
        Ok(Self {
            id: snapshot.id,
            repository_id: snapshot.repository_id,
            provider_id: snapshot.provider_id,
            version: snapshot.version,
            target: snapshot.target,
            package_sha256: snapshot.package_sha256,
            active: snapshot.active,
            generation: snapshot.generation,
            installed_at: timestamp(snapshot.installed_at)?,
        })
    }
}
