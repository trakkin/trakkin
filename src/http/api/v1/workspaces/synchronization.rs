use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{
    application::{
        sync_policy::{CreateSyncPolicy, QuerySyncPolicies, UpdateSyncPolicy},
        sync_schedule::{EnableSyncSchedule, PauseSyncSchedule, SyncScheduleRecurrence},
        sync_workflow::{CreateWorkspaceSyncPreview, StartWorkspaceSyncRun},
    },
    http::{
        dto::synchronization::{
            CreateSyncPolicyRequest, CreateSyncPreviewRequest, EnableSyncScheduleRequest,
            StartSyncRunRequest, StartSyncRunResponse, SyncPolicyPageParameters,
            SyncPolicyPageResponse, SyncPolicyResponse, SyncPreviewResponse, SyncScheduleResponse,
            UpdateSyncPolicyRequest,
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::{
    super::support::{HttpState, generate_uuid},
    WorkspacePath,
};

const DEFAULT_SYNC_POLICY_PAGE_SIZE: u32 = 25;
const SYNC_PREVIEW_LIFETIME_MINUTES: i64 = 5;

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route(
            "/api/v1/workspaces/{workspace_id}/sync-policies",
            get(list_sync_policies).post(create_sync_policy),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sync-policies/{policy_id}",
            get(get_sync_policy).patch(update_sync_policy),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sync-policies/{policy_id}/schedule",
            get(get_sync_schedule).put(enable_sync_schedule),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sync-policies/{policy_id}/schedule/pause",
            post(pause_sync_schedule),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sync-previews",
            post(create_sync_preview),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sync-previews/{preview_id}/runs",
            post(start_sync_run),
        )
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/sync-policies", params(("workspace_id" = Uuid, Path), SyncPolicyPageParameters), responses((status = 200, body = SyncPolicyPageResponse), (status = 400, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn list_sync_policies(
    State(state): State<HttpState>,
    path: Result<Path<WorkspacePath>, PathRejection>,
    query: Result<Query<SyncPolicyPageParameters>, QueryRejection>,
) -> Result<Json<SyncPolicyPageResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Query(query) =
        query.map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let after_policy_id = query
        .cursor
        .map(|cursor| Uuid::parse_str(&cursor))
        .transpose()
        .map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let page = state
        .application
        .list_sync_policies(QuerySyncPolicies {
            workspace_id: path.workspace_id,
            limit: query.limit.unwrap_or(DEFAULT_SYNC_POLICY_PAGE_SIZE),
            after_policy_id,
        })
        .await?;
    Ok(Json(SyncPolicyPageResponse::from_page(page)?))
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/sync-policies", params(("workspace_id" = Uuid, Path)), request_body = CreateSyncPolicyRequest, responses((status = 201, body = SyncPolicyResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn create_sync_policy(
    State(state): State<HttpState>,
    path: Result<Path<WorkspacePath>, PathRejection>,
    payload: Result<Json<CreateSyncPolicyRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<SyncPolicyResponse>), HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let policy = state
        .application
        .create_sync_policy(CreateSyncPolicy {
            policy_id: generate_uuid()?,
            workspace_id: path.workspace_id,
            document: payload.document.into_domain()?,
            created_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(SyncPolicyResponse::from_snapshot(policy)?),
    ))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/sync-policies/{policy_id}", params(("workspace_id" = Uuid, Path), ("policy_id" = Uuid, Path)), responses((status = 200, body = SyncPolicyResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn get_sync_policy(
    State(state): State<HttpState>,
    path: Result<Path<SyncPolicyPath>, PathRejection>,
) -> Result<Json<SyncPolicyResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    Ok(Json(SyncPolicyResponse::from_snapshot(
        state
            .application
            .get_sync_policy(path.workspace_id, path.policy_id)
            .await?,
    )?))
}

#[utoipa::path(patch, path = "/api/v1/workspaces/{workspace_id}/sync-policies/{policy_id}", params(("workspace_id" = Uuid, Path), ("policy_id" = Uuid, Path)), request_body = UpdateSyncPolicyRequest, responses((status = 200, body = SyncPolicyResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn update_sync_policy(
    State(state): State<HttpState>,
    path: Result<Path<SyncPolicyPath>, PathRejection>,
    payload: Result<Json<UpdateSyncPolicyRequest>, JsonRejection>,
) -> Result<Json<SyncPolicyResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let policy = state
        .application
        .update_sync_policy(UpdateSyncPolicy {
            policy_id: path.policy_id,
            workspace_id: path.workspace_id,
            base_content_hash: payload.base_content_hash,
            document: payload.document.into_domain()?,
            updated_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(SyncPolicyResponse::from_snapshot(policy)?))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/sync-policies/{policy_id}/schedule", params(("workspace_id" = Uuid, Path), ("policy_id" = Uuid, Path)), responses((status = 200, body = SyncScheduleResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn get_sync_schedule(
    State(state): State<HttpState>,
    path: Result<Path<SyncPolicyPath>, PathRejection>,
) -> Result<Json<SyncScheduleResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    Ok(Json(SyncScheduleResponse::from_snapshot(
        state
            .application
            .get_sync_schedule(path.workspace_id, path.policy_id)
            .await?,
    )?))
}

#[utoipa::path(put, path = "/api/v1/workspaces/{workspace_id}/sync-policies/{policy_id}/schedule", params(("workspace_id" = Uuid, Path), ("policy_id" = Uuid, Path)), request_body = EnableSyncScheduleRequest, responses((status = 200, body = SyncScheduleResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn enable_sync_schedule(
    State(state): State<HttpState>,
    path: Result<Path<SyncPolicyPath>, PathRejection>,
    payload: Result<Json<EnableSyncScheduleRequest>, JsonRejection>,
) -> Result<Json<SyncScheduleResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let recurrence = SyncScheduleRecurrence::every(payload.interval_seconds)
        .map_err(|_| HttpError::invalid_request("synchronization schedule interval is invalid"))?;
    let schedule = state
        .application
        .enable_sync_schedule(EnableSyncSchedule {
            schedule_id: generate_uuid()?,
            workspace_id: path.workspace_id,
            policy_id: path.policy_id,
            recurrence,
            enabled_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(SyncScheduleResponse::from_snapshot(schedule)?))
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/sync-policies/{policy_id}/schedule/pause", params(("workspace_id" = Uuid, Path), ("policy_id" = Uuid, Path)), responses((status = 200, body = SyncScheduleResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn pause_sync_schedule(
    State(state): State<HttpState>,
    path: Result<Path<SyncPolicyPath>, PathRejection>,
) -> Result<Json<SyncScheduleResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let schedule = state
        .application
        .pause_sync_schedule(PauseSyncSchedule {
            workspace_id: path.workspace_id,
            policy_id: path.policy_id,
            paused_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(SyncScheduleResponse::from_snapshot(schedule)?))
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/sync-previews", params(("workspace_id" = Uuid, Path)), request_body = CreateSyncPreviewRequest, responses((status = 201, body = SyncPreviewResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn create_sync_preview(
    State(state): State<HttpState>,
    path: Result<Path<WorkspacePath>, PathRejection>,
    payload: Result<Json<CreateSyncPreviewRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<SyncPreviewResponse>), HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let created_at = OffsetDateTime::now_utc();
    let preview = state
        .application
        .create_workspace_sync_preview(CreateWorkspaceSyncPreview {
            preview_id: generate_uuid()?,
            workspace_id: path.workspace_id,
            policy_id: payload.policy_id,
            created_at,
            expires_at: created_at + Duration::minutes(SYNC_PREVIEW_LIFETIME_MINUTES),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(SyncPreviewResponse::from_view(preview)?),
    ))
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/sync-previews/{preview_id}/runs", params(("workspace_id" = Uuid, Path), ("preview_id" = Uuid, Path)), request_body = StartSyncRunRequest, responses((status = 201, body = StartSyncRunResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "synchronization")]
pub(in crate::http::api::v1) async fn start_sync_run(
    State(state): State<HttpState>,
    path: Result<Path<SyncPreviewPath>, PathRejection>,
    payload: Result<Json<StartSyncRunRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<StartSyncRunResponse>), HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let run = state
        .application
        .start_workspace_sync_run(StartWorkspaceSyncRun {
            run_id: generate_uuid()?,
            schedule_occurrence_id: None,
            preview_id: path.preview_id,
            workspace_id: path.workspace_id,
            preview_hash: payload.preview_hash,
            admitted_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(StartSyncRunResponse::from_snapshot(run)),
    ))
}

#[derive(Deserialize)]
pub(in crate::http::api::v1) struct SyncPolicyPath {
    workspace_id: Uuid,
    policy_id: Uuid,
}

#[derive(Deserialize)]
pub(in crate::http::api::v1) struct SyncPreviewPath {
    workspace_id: Uuid,
    preview_id: Uuid,
}
