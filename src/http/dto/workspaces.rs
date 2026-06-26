use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::application::{error::ApplicationError, facade::WorkspaceView};

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
