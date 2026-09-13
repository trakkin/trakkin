use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

use crate::application::{
    error::ApplicationError, facade::WorkspaceView, workspaces::WorkspaceSourceMappingRootPolicy,
};

use super::timestamp;

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct CreateWorkspaceRequest {
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceResponse {
    pub id: String,
    pub name: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub sources: Vec<WorkspaceSourceResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSourceResponse {
    pub source_id: String,
    pub attached_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, ToSchema)]
pub struct UpdateMappingRootPolicyRequest {
    #[serde(deserialize_with = "deserialize_required_nullable_namespaces")]
    #[schema(required = true)]
    pub namespaces: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingRootPolicyResponse {
    #[schema(required = true)]
    pub namespaces: Option<Vec<String>>,
    pub available_namespaces: Vec<MappingRootNamespaceResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MappingRootNamespaceResponse {
    pub namespace: String,
    pub candidate_count: u64,
}

fn deserialize_required_nullable_namespaces<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<String>>::deserialize(deserializer)
}

impl WorkspaceResponse {
    pub fn from_view(view: WorkspaceView) -> Result<Self, ApplicationError> {
        Ok(Self {
            id: view.workspace.id.to_string(),
            name: view.workspace.name,
            status: "active".to_owned(),
            created_at: timestamp(view.workspace.created_at)?,
            updated_at: timestamp(view.workspace.updated_at)?,
            sources: view
                .sources
                .into_iter()
                .map(|source| {
                    Ok(WorkspaceSourceResponse {
                        source_id: source.source_id.to_string(),
                        attached_at: timestamp(source.attached_at)?,
                    })
                })
                .collect::<Result<_, ApplicationError>>()?,
        })
    }
}

impl MappingRootPolicyResponse {
    pub fn from_policy(policy: WorkspaceSourceMappingRootPolicy) -> Self {
        let namespaces = policy.attachment.mapping_root_namespace_allowlist;
        Self {
            namespaces,
            available_namespaces: policy
                .namespaces
                .into_iter()
                .map(|namespace| MappingRootNamespaceResponse {
                    namespace: namespace.namespace,
                    candidate_count: namespace.candidate_count,
                })
                .collect(),
        }
    }
}
