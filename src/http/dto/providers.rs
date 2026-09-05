use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::application::{
    error::ApplicationError,
    providers::{
        AvailableProviderSnapshot, ProviderSnapshot, ProviderSourceSnapshot, ProviderUpdatePolicy,
    },
};

use super::timestamp;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ProviderUpdatePolicyValue {
    Manual,
    Automatic,
}

impl From<ProviderUpdatePolicyValue> for ProviderUpdatePolicy {
    fn from(value: ProviderUpdatePolicyValue) -> Self {
        match value {
            ProviderUpdatePolicyValue::Manual => Self::Manual,
            ProviderUpdatePolicyValue::Automatic => Self::Automatic,
        }
    }
}

impl From<ProviderUpdatePolicy> for ProviderUpdatePolicyValue {
    fn from(value: ProviderUpdatePolicy) -> Self {
        match value {
            ProviderUpdatePolicy::Manual => Self::Manual,
            ProviderUpdatePolicy::Automatic => Self::Automatic,
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrustProviderSourceRequest {
    pub repository_url: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstallProviderRequest {
    pub source_id: Uuid,
    pub provider_id: String,
    pub version: String,
    pub update_policy: ProviderUpdatePolicyValue,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProviderRequest {
    pub version: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChangeProviderUpdatePolicyRequest {
    pub update_policy: ProviderUpdatePolicyValue,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderSourceResponse {
    pub id: Uuid,
    pub repository_url: String,
    pub available_providers: Vec<AvailableProviderResponse>,
    pub trusted_at: String,
    pub last_checked_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AvailableProviderResponse {
    pub provider_id: String,
    pub versions: Vec<String>,
}

impl From<AvailableProviderSnapshot> for AvailableProviderResponse {
    fn from(snapshot: AvailableProviderSnapshot) -> Self {
        Self {
            provider_id: snapshot.provider_id,
            versions: snapshot.versions,
        }
    }
}

impl ProviderSourceResponse {
    pub fn from_snapshot(snapshot: ProviderSourceSnapshot) -> Result<Self, ApplicationError> {
        Ok(Self {
            id: snapshot.id,
            repository_url: snapshot.repository_url,
            available_providers: snapshot
                .available_providers
                .into_iter()
                .map(AvailableProviderResponse::from)
                .collect(),
            trusted_at: timestamp(snapshot.trusted_at)?,
            last_checked_at: snapshot.last_checked_at.map(timestamp).transpose()?,
            last_error: snapshot.last_error,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderUpdateResponse {
    pub version: String,
    pub discovered_at: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderResponse {
    pub provider_id: String,
    pub source_id: Uuid,
    pub name: Option<String>,
    pub description: Option<String>,
    pub version: String,
    pub platform: String,
    pub index_digest: String,
    pub binary_digest: String,
    pub update_policy: ProviderUpdatePolicyValue,
    pub available_update: Option<ProviderUpdateResponse>,
    pub last_update_error: Option<String>,
    pub installed_at: String,
    pub updated_at: String,
}

impl ProviderResponse {
    pub fn from_snapshot(snapshot: ProviderSnapshot) -> Result<Self, ApplicationError> {
        Ok(Self {
            provider_id: snapshot.provider_id,
            source_id: snapshot.source_id,
            name: snapshot.name,
            description: snapshot.description,
            version: snapshot.version,
            platform: snapshot.platform,
            index_digest: snapshot.index_digest,
            binary_digest: snapshot.binary_digest,
            update_policy: snapshot.update_policy.into(),
            available_update: snapshot
                .available_update
                .map(|update| {
                    Ok(ProviderUpdateResponse {
                        version: update.version,
                        discovered_at: timestamp(update.discovered_at)?,
                    })
                })
                .transpose()?,
            last_update_error: snapshot.last_update_error,
            installed_at: timestamp(snapshot.installed_at)?,
            updated_at: timestamp(snapshot.updated_at)?,
        })
    }
}
