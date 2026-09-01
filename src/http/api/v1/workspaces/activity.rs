use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    routing::get,
};
use serde::Deserialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    application::sync_activity::{QuerySyncActivity, SyncActivityRunStatus},
    http::{
        dto::activity::{
            ActivityActionDetailResponse, ActivityPageResponse, ActivityQueryParameters,
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::{super::support::HttpState, WorkspacePath};

const DEFAULT_ACTIVITY_PAGE_SIZE: u32 = 25;

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route(
            "/api/v1/workspaces/{workspace_id}/activity",
            get(list_activity),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/activity/actions/{action_id}",
            get(get_activity_action),
        )
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/activity", params(("workspace_id" = Uuid, Path), ActivityQueryParameters), responses((status = 200, body = ActivityPageResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "activity")]
pub(in crate::http::api::v1) async fn list_activity(
    State(state): State<HttpState>,
    path: Result<Path<WorkspacePath>, PathRejection>,
    query: Result<Query<ActivityQueryParameters>, QueryRejection>,
) -> Result<Json<ActivityPageResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Query(query) =
        query.map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let statuses = query
        .status
        .as_deref()
        .map(|statuses| {
            statuses
                .split(',')
                .map(parse_activity_status)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let page = state
        .application
        .list_sync_activity(QuerySyncActivity {
            workspace_id: path.workspace_id,
            run_id: query.run_id,
            statuses,
            receiver_source_id: query.source_id,
            semantic_field: query.field,
            occurred_at_or_after: parse_activity_timestamp(
                query.occurred_at_or_after.as_deref(),
                "occurredAtOrAfter",
            )?,
            occurred_before: parse_activity_timestamp(
                query.occurred_before.as_deref(),
                "occurredBefore",
            )?,
            limit: query.limit.unwrap_or(DEFAULT_ACTIVITY_PAGE_SIZE),
            cursor: query.cursor,
        })
        .await?;
    Ok(Json(ActivityPageResponse::from_page(page)?))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/activity/actions/{action_id}", params(("workspace_id" = Uuid, Path), ("action_id" = Uuid, Path)), responses((status = 200, body = ActivityActionDetailResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "activity")]
pub(in crate::http::api::v1) async fn get_activity_action(
    State(state): State<HttpState>,
    path: Result<Path<ActivityActionPath>, PathRejection>,
) -> Result<Json<ActivityActionDetailResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let detail = state
        .application
        .get_sync_activity_action(path.workspace_id, path.action_id)
        .await?;
    Ok(Json(ActivityActionDetailResponse::from_detail(detail)?))
}

fn parse_activity_status(value: &str) -> Result<SyncActivityRunStatus, HttpError> {
    match value {
        "queued" => Ok(SyncActivityRunStatus::Queued),
        "running" => Ok(SyncActivityRunStatus::Running),
        "succeeded" => Ok(SyncActivityRunStatus::Succeeded),
        "failed" => Ok(SyncActivityRunStatus::Failed),
        "partial" => Ok(SyncActivityRunStatus::Partial),
        "indeterminate" => Ok(SyncActivityRunStatus::Indeterminate),
        "cancelled" => Ok(SyncActivityRunStatus::Cancelled),
        _ => Err(crate::application::error::ApplicationError::field(
            crate::application::error::ApplicationErrorCode::InvalidField,
            "Activity query is invalid",
            "status",
            "unsupported_status",
            "Activity status is unsupported",
        )
        .into()),
    }
}

fn parse_activity_timestamp(
    value: Option<&str>,
    path: &'static str,
) -> Result<Option<OffsetDateTime>, HttpError> {
    value
        .map(|value| {
            OffsetDateTime::parse(value, &Rfc3339).map_err(|_| {
                crate::application::error::ApplicationError::field(
                    crate::application::error::ApplicationErrorCode::InvalidField,
                    "Activity query is invalid",
                    path,
                    "invalid_timestamp",
                    "Activity timestamp must be RFC 3339",
                )
                .into()
            })
        })
        .transpose()
}

#[derive(Deserialize)]
pub(in crate::http::api::v1) struct ActivityActionPath {
    workspace_id: Uuid,
    action_id: Uuid,
}
