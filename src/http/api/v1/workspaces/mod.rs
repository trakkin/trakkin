pub(super) mod activity;
pub(super) mod mappings;
pub(super) mod sources;
pub(super) mod synchronization;
pub(super) mod tasks;

use axum::{
    Json, Router,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::StatusCode,
    routing::get,
};
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::workspaces::CreateWorkspace,
    http::{
        dto::workspaces::{CreateWorkspaceRequest, WorkspaceResponse},
        error::{ErrorResponse, HttpError},
    },
};

use super::support::{HttpState, generate_uuid};

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route(
            "/api/v1/workspaces",
            get(list_workspaces).post(create_workspace),
        )
        .route("/api/v1/workspaces/{workspace_id}", get(get_workspace))
        .merge(synchronization::router())
        .merge(mappings::router())
        .merge(sources::router())
        .merge(tasks::router())
        .merge(activity::router())
}

#[utoipa::path(get, path = "/api/v1/workspaces", responses((status = 200, body = [WorkspaceResponse]), (status = 500, body = ErrorResponse)), tag = "workspaces")]
pub(super) async fn list_workspaces(
    State(state): State<HttpState>,
) -> Result<Json<Vec<WorkspaceResponse>>, HttpError> {
    let workspaces = state.application.list_workspaces().await?;
    Ok(Json(
        workspaces
            .into_iter()
            .map(WorkspaceResponse::from_view)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

#[utoipa::path(post, path = "/api/v1/workspaces", request_body = CreateWorkspaceRequest, responses((status = 201, body = WorkspaceResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "workspaces")]
pub(super) async fn create_workspace(
    State(state): State<HttpState>,
    payload: Result<Json<CreateWorkspaceRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<WorkspaceResponse>), HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let workspace = state
        .application
        .create_workspace(CreateWorkspace {
            workspace_id: generate_uuid()?,
            name: payload.name,
            created_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(WorkspaceResponse::from_view(workspace)?),
    ))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}", params(("workspace_id" = Uuid, Path)), responses((status = 200, body = WorkspaceResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse)), tag = "workspaces")]
pub(super) async fn get_workspace(
    State(state): State<HttpState>,
    path: Result<Path<WorkspacePath>, PathRejection>,
) -> Result<Json<WorkspaceResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let workspace = state.application.get_workspace(path.workspace_id).await?;
    Ok(Json(WorkspaceResponse::from_view(workspace)?))
}

#[derive(Deserialize)]
pub(super) struct WorkspacePath {
    pub(super) workspace_id: Uuid,
}
