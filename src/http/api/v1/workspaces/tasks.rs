use axum::{
    Json, Router,
    extract::{Path, State, rejection::PathRejection},
    routing::{get, post},
};
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::task_manager::RequestTaskCancellation,
    http::{
        dto::tasks::TaskResponse,
        error::{ErrorResponse, HttpError},
    },
};

use super::{super::support::HttpState, WorkspacePath};

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route("/api/v1/workspaces/{workspace_id}/tasks", get(list_tasks))
        .route(
            "/api/v1/workspaces/{workspace_id}/tasks/{task_id}",
            get(get_task),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/tasks/{task_id}/cancellation",
            post(cancel_task),
        )
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/tasks", params(("workspace_id" = Uuid, Path)), responses((status = 200, body = [TaskResponse]), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "tasks")]
pub(in crate::http::api::v1) async fn list_tasks(
    State(state): State<HttpState>,
    path: Result<Path<WorkspacePath>, PathRejection>,
) -> Result<Json<Vec<TaskResponse>>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let tasks = state.application.list_tasks(path.workspace_id).await?;
    Ok(Json(
        tasks
            .into_iter()
            .map(TaskResponse::from_snapshot)
            .collect::<Result<_, _>>()?,
    ))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/tasks/{task_id}", params(("workspace_id" = Uuid, Path), ("task_id" = Uuid, Path)), responses((status = 200, body = TaskResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse)), tag = "tasks")]
pub(in crate::http::api::v1) async fn get_task(
    State(state): State<HttpState>,
    path: Result<Path<TaskPath>, PathRejection>,
) -> Result<Json<TaskResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let task = state
        .application
        .get_task(path.workspace_id, path.task_id)
        .await?;
    Ok(Json(TaskResponse::from_snapshot(task)?))
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/tasks/{task_id}/cancellation", params(("workspace_id" = Uuid, Path), ("task_id" = Uuid, Path)), responses((status = 200, body = TaskResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "tasks")]
pub(in crate::http::api::v1) async fn cancel_task(
    State(state): State<HttpState>,
    path: Result<Path<TaskPath>, PathRejection>,
) -> Result<Json<TaskResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let task = state
        .application
        .cancel_task(
            path.workspace_id,
            RequestTaskCancellation {
                task_id: path.task_id,
                requested_at: OffsetDateTime::now_utc(),
            },
        )
        .await?;
    Ok(Json(TaskResponse::from_snapshot(task)?))
}

#[derive(Deserialize)]
pub(in crate::http::api::v1) struct TaskPath {
    workspace_id: Uuid,
    task_id: Uuid,
}
