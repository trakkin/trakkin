use axum::{
    Json, Router,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::StatusCode,
    routing::{get, patch, post},
};
use serde::Deserialize;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{
    application::{
        adapter_runtime::{AdapterSecret, AdapterSetting},
        connection::{
            BeginConnectionDeletion, CancelConnectionAuthentication,
            ContinueConnectionAuthentication, CreateConnection, InspectConnection,
            RenameConnection, StartConnectionAuthentication,
        },
        task_manager::{AdmitTask, RequestTaskCancellation, TaskDefinition},
    },
    http::{
        dto::{
            connections::{
                AdapterSettingRequest, AuthenticationStepResponse,
                ConnectionDeletionImpactResponse, ConnectionInspectionResponse, ConnectionResponse,
                ConnectionSourceResponse, ContinueAuthenticationRequest, CreateConnectionRequest,
                DeleteConnectionRequest, RenameConnectionRequest, StartAuthenticationRequest,
            },
            tasks::TaskResponse,
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::support::{HttpState, generate_uuid};

const AUTHENTICATION_LIFETIME_MINUTES: i64 = 10;

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route(
            "/api/v1/connections",
            get(list_connections).post(create_connection),
        )
        .route(
            "/api/v1/connections/{connection_id}",
            patch(rename_connection),
        )
        .route(
            "/api/v1/connections/{connection_id}/sources",
            get(list_connection_sources),
        )
        .route(
            "/api/v1/connections/{connection_id}/deletion-impact",
            get(get_connection_deletion_impact),
        )
        .route(
            "/api/v1/connections/{connection_id}/deletion",
            post(delete_connection),
        )
        .route(
            "/api/v1/connections/{connection_id}/inspection",
            get(inspect_connection),
        )
        .route(
            "/api/v1/connections/{connection_id}/authentications",
            post(start_authentication),
        )
        .route(
            "/api/v1/connections/{connection_id}/authentications/{authentication_id}/continuation",
            post(continue_authentication),
        )
        .route(
            "/api/v1/connections/{connection_id}/authentications/{authentication_id}/cancellation",
            post(cancel_authentication),
        )
        .route(
            "/api/v1/connections/{connection_id}/discovery-tasks",
            post(start_source_discovery),
        )
        .route(
            "/api/v1/connections/{connection_id}/tasks/{task_id}",
            get(get_connection_task),
        )
        .route(
            "/api/v1/connections/{connection_id}/tasks/{task_id}/cancellation",
            post(cancel_connection_task),
        )
}

#[utoipa::path(
    get,
    path = "/api/v1/connections",
    responses((status = 200, body = [ConnectionResponse]), (status = 500, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn list_connections(
    State(state): State<HttpState>,
) -> Result<Json<Vec<ConnectionResponse>>, HttpError> {
    let connections = state.application.list_connections().await?;
    Ok(Json(
        connections
            .into_iter()
            .map(ConnectionResponse::from_summary)
            .collect::<Result<_, _>>()?,
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/connections",
    request_body = CreateConnectionRequest,
    responses((status = 201, body = ConnectionResponse), (status = 400, body = ErrorResponse), (status = 500, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn create_connection(
    State(state): State<HttpState>,
    payload: Result<Json<CreateConnectionRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ConnectionResponse>), HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let connection = state
        .application
        .create_connection(CreateConnection {
            connection_id: generate_uuid()?,
            provider_id: payload.provider_id,
            display_name: payload.display_name,
            settings: payload
                .settings
                .into_iter()
                .map(|setting| AdapterSetting {
                    key: setting.key,
                    value: setting.value.into(),
                })
                .collect(),
            secrets: payload
                .secrets
                .into_iter()
                .map(|secret| AdapterSecret {
                    key: secret.key,
                    value: secret.value,
                })
                .collect(),
            created_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ConnectionResponse::from_summary(connection)?),
    ))
}

#[utoipa::path(
    patch,
    path = "/api/v1/connections/{connection_id}",
    params(("connection_id" = Uuid, Path)),
    request_body = RenameConnectionRequest,
    responses((status = 200, body = ConnectionResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn rename_connection(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionPath>, PathRejection>,
    payload: Result<Json<RenameConnectionRequest>, JsonRejection>,
) -> Result<Json<ConnectionResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let connection = state
        .application
        .rename_connection(RenameConnection {
            connection_id: path.connection_id,
            display_name: payload.display_name,
            renamed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(ConnectionResponse::from_summary(connection)?))
}

#[utoipa::path(
    get,
    path = "/api/v1/connections/{connection_id}/sources",
    params(("connection_id" = Uuid, Path)),
    responses((status = 200, body = [ConnectionSourceResponse]), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn list_connection_sources(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionPath>, PathRejection>,
) -> Result<Json<Vec<ConnectionSourceResponse>>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let sources = state
        .application
        .list_connection_sources(path.connection_id)
        .await?;
    Ok(Json(
        sources
            .into_iter()
            .map(ConnectionSourceResponse::from)
            .collect(),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/connections/{connection_id}/deletion-impact",
    params(("connection_id" = Uuid, Path)),
    responses((status = 200, body = ConnectionDeletionImpactResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn get_connection_deletion_impact(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionPath>, PathRejection>,
) -> Result<Json<ConnectionDeletionImpactResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let impact = state
        .application
        .connection_deletion_impact(path.connection_id)
        .await?;
    Ok(Json(ConnectionDeletionImpactResponse::from(impact)))
}

#[utoipa::path(
    post,
    path = "/api/v1/connections/{connection_id}/deletion",
    params(("connection_id" = Uuid, Path)),
    request_body = DeleteConnectionRequest,
    responses((status = 200, body = ConnectionDeletionImpactResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn delete_connection(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionPath>, PathRejection>,
    payload: Result<Json<DeleteConnectionRequest>, JsonRejection>,
) -> Result<Json<ConnectionDeletionImpactResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    if !payload.confirmed {
        return Err(HttpError::invalid_field(
            "confirmed",
            "confirmation_required",
            "connection deletion must be explicitly confirmed",
        ));
    }
    let impact = state
        .application
        .delete_connection(BeginConnectionDeletion {
            connection_id: path.connection_id,
            disabled_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(ConnectionDeletionImpactResponse::from(impact)))
}

#[utoipa::path(
    get,
    path = "/api/v1/connections/{connection_id}/inspection",
    params(("connection_id" = Uuid, Path)),
    responses((status = 200, body = ConnectionInspectionResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn inspect_connection(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionPath>, PathRejection>,
) -> Result<Json<ConnectionInspectionResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let inspection = state
        .application
        .inspect_connection(InspectConnection {
            connection_id: path.connection_id,
            process_instance_id: format!("inspection-{}", generate_uuid()?),
        })
        .await?;
    Ok(Json(ConnectionInspectionResponse::from_inspection(
        inspection,
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/connections/{connection_id}/authentications",
    params(("connection_id" = Uuid, Path)),
    request_body = StartAuthenticationRequest,
    responses((status = 201, body = AuthenticationStepResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn start_authentication(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionPath>, PathRejection>,
    payload: Result<Json<StartAuthenticationRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<AuthenticationStepResponse>), HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    if payload.method.trim().is_empty() {
        return Err(HttpError::invalid_field(
            "method",
            "required",
            "authentication method is required",
        ));
    }
    let now = OffsetDateTime::now_utc();
    let started = state
        .application
        .start_authentication(StartConnectionAuthentication {
            authentication_id: generate_uuid()?,
            connection_id: path.connection_id,
            process_instance_id: format!("authentication-{}", generate_uuid()?),
            method: payload.method,
            values: adapter_settings(payload.values),
            expires_at: now + Duration::minutes(AUTHENTICATION_LIFETIME_MINUTES),
            started_at: now,
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(AuthenticationStepResponse::from_started(started)),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/connections/{connection_id}/authentications/{authentication_id}/continuation",
    params(("connection_id" = Uuid, Path), ("authentication_id" = Uuid, Path)),
    request_body = ContinueAuthenticationRequest,
    responses((status = 200, body = AuthenticationStepResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn continue_authentication(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionAuthenticationPath>, PathRejection>,
    payload: Result<Json<ContinueAuthenticationRequest>, JsonRejection>,
) -> Result<Json<AuthenticationStepResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let now = OffsetDateTime::now_utc();
    let step = state
        .application
        .continue_authentication(ContinueConnectionAuthentication {
            authentication_id: path.authentication_id,
            connection_id: path.connection_id,
            values: adapter_settings(payload.values),
            expires_at: now + Duration::minutes(AUTHENTICATION_LIFETIME_MINUTES),
            continued_at: now,
        })
        .await?;
    Ok(Json(AuthenticationStepResponse::from_step(
        path.authentication_id,
        step,
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/connections/{connection_id}/authentications/{authentication_id}/cancellation",
    params(("connection_id" = Uuid, Path), ("authentication_id" = Uuid, Path)),
    responses((status = 200, body = AuthenticationStepResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)),
    tag = "connections"
)]
pub(super) async fn cancel_authentication(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionAuthenticationPath>, PathRejection>,
) -> Result<Json<AuthenticationStepResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    state
        .application
        .cancel_authentication(CancelConnectionAuthentication {
            authentication_id: path.authentication_id,
            connection_id: path.connection_id,
            cancelled_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(AuthenticationStepResponse::cancelled(
        path.authentication_id,
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/connections/{connection_id}/discovery-tasks",
    params(("connection_id" = Uuid, Path)),
    responses((status = 202, body = TaskResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse)),
    tag = "tasks"
)]
pub(super) async fn start_source_discovery(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionPath>, PathRejection>,
) -> Result<(StatusCode, Json<TaskResponse>), HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let task = state
        .application
        .admit_task(AdmitTask {
            task_id: generate_uuid()?,
            workspace_id: None,
            definition: TaskDefinition::SourceDiscovery {
                connection_id: path.connection_id,
            },
            cancellation_supported: true,
            admitted_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(TaskResponse::from_snapshot(task)?),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/connections/{connection_id}/tasks/{task_id}",
    params(("connection_id" = Uuid, Path), ("task_id" = Uuid, Path)),
    responses((status = 200, body = TaskResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse)),
    tag = "tasks"
)]
pub(super) async fn get_connection_task(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionTaskPath>, PathRejection>,
) -> Result<Json<TaskResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let task = state
        .application
        .get_connection_task(path.connection_id, path.task_id)
        .await?;
    Ok(Json(TaskResponse::from_snapshot(task)?))
}

#[utoipa::path(
    post,
    path = "/api/v1/connections/{connection_id}/tasks/{task_id}/cancellation",
    params(("connection_id" = Uuid, Path), ("task_id" = Uuid, Path)),
    responses((status = 200, body = TaskResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse)),
    tag = "tasks"
)]
pub(super) async fn cancel_connection_task(
    State(state): State<HttpState>,
    path: Result<Path<ConnectionTaskPath>, PathRejection>,
) -> Result<Json<TaskResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let task = state
        .application
        .cancel_connection_task(
            path.connection_id,
            RequestTaskCancellation {
                task_id: path.task_id,
                requested_at: OffsetDateTime::now_utc(),
            },
        )
        .await?;
    Ok(Json(TaskResponse::from_snapshot(task)?))
}

fn adapter_settings(settings: Vec<AdapterSettingRequest>) -> Vec<AdapterSetting> {
    settings
        .into_iter()
        .map(|setting| AdapterSetting {
            key: setting.key,
            value: setting.value.into(),
        })
        .collect()
}

#[derive(Deserialize)]
pub(super) struct ConnectionPath {
    connection_id: Uuid,
}

#[derive(Deserialize)]
pub(super) struct ConnectionTaskPath {
    connection_id: Uuid,
    task_id: Uuid,
}

#[derive(Deserialize)]
pub(super) struct ConnectionAuthenticationPath {
    connection_id: Uuid,
    authentication_id: Uuid,
}
