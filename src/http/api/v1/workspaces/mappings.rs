use axum::{
    Json, Router,
    body::Body,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::{
        HeaderValue, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::Response,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use trakkin_mapping_contract::{PortableEndpoint, PortableSelector};
use uuid::Uuid;

use crate::{
    application::{
        facade::UpdateMappingDocumentResult,
        mapping_lifecycle::{
            ActivateWorkspaceMappingRevision, CreateWorkspaceMappingDocument,
            RetireWorkspaceMapping, UpdateWorkspaceMappingDocument,
        },
        mapping_queries::{QueryMappingHistory, QueryMappingRegistry},
        mapping_resolution::{ExactMappingRevision, ResolutionRequest},
    },
    http::{
        dto::mappings::{
            ActivateMappingRevisionRequest, CreateMappingDocumentRequest,
            MappingActivationResponse, MappingDetailResponse, MappingHeadMovedResponse,
            MappingHistoryPageResponse, MappingPageParameters, MappingRegistryPageResponse,
            MappingResolutionModeResponse, MappingResolutionPreviewRequest,
            MappingResolutionPreviewResponse, UpdateMappingDocumentRequest,
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::super::support::{HttpState, generate_uuid};
use super::WorkspacePath;

const DEFAULT_MAPPING_PAGE_SIZE: u32 = 25;
const MAX_MAPPING_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_MAPPING_RESOLUTION_REFERENCES: usize = 256;

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route(
            "/api/v1/workspaces/{workspace_id}/mappings",
            get(list_mappings).post(create_mapping),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}",
            get(get_mapping),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions",
            get(list_mapping_revisions).post(create_mapping_revision),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions/{content_hash}",
            get(get_mapping_revision),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions/{content_hash}/resolution-preview",
            post(preview_mapping_resolution),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/activation",
            post(activate_mapping_revision),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/retirement",
            post(retire_mapping),
        )
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/mappings", params(("workspace_id" = Uuid, Path), MappingPageParameters), responses((status = 200, body = MappingRegistryPageResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn list_mappings(
    State(state): State<HttpState>,
    path: Result<Path<WorkspacePath>, PathRejection>,
    query: Result<Query<MappingPageParameters>, QueryRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Query(query) =
        query.map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let page = state
        .application
        .query_mapping_registry(QueryMappingRegistry {
            workspace_id: path.workspace_id,
            limit: query.limit.unwrap_or(DEFAULT_MAPPING_PAGE_SIZE),
            cursor: query.cursor,
        })
        .await?;
    mapping_json_response(MappingRegistryPageResponse::from_page(page)?)
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/mappings", params(("workspace_id" = Uuid, Path)), request_body = CreateMappingDocumentRequest, responses((status = 201, body = MappingDetailResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn create_mapping(
    State(state): State<HttpState>,
    path: Result<Path<WorkspacePath>, PathRejection>,
    payload: Result<Json<CreateMappingDocumentRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let detail = state
        .application
        .create_mapping_document(CreateWorkspaceMappingDocument {
            mapping_id: generate_uuid()?,
            workspace_id: path.workspace_id,
            schema: payload.schema,
            mappings: payload.mappings,
            created_at: OffsetDateTime::now_utc(),
        })
        .await?;
    mapping_json_response_with_status(
        MappingDetailResponse::from_detail(detail)?,
        StatusCode::CREATED,
    )
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}", params(("workspace_id" = Uuid, Path), ("mapping_id" = Uuid, Path)), responses((status = 200, body = MappingDetailResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn get_mapping(
    State(state): State<HttpState>,
    path: Result<Path<MappingPath>, PathRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let detail = state
        .application
        .get_mapping_head(path.workspace_id, path.mapping_id)
        .await?;
    mapping_json_response(MappingDetailResponse::from_detail(detail)?)
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions", params(("workspace_id" = Uuid, Path), ("mapping_id" = Uuid, Path), MappingPageParameters), responses((status = 200, body = MappingHistoryPageResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn list_mapping_revisions(
    State(state): State<HttpState>,
    path: Result<Path<MappingPath>, PathRejection>,
    query: Result<Query<MappingPageParameters>, QueryRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Query(query) =
        query.map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let page = state
        .application
        .query_mapping_history(QueryMappingHistory {
            workspace_id: path.workspace_id,
            mapping_id: path.mapping_id,
            limit: query.limit.unwrap_or(DEFAULT_MAPPING_PAGE_SIZE),
            cursor: query.cursor,
        })
        .await?;
    mapping_json_response(MappingHistoryPageResponse::from_page(page)?)
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions", params(("workspace_id" = Uuid, Path), ("mapping_id" = Uuid, Path)), request_body = UpdateMappingDocumentRequest, responses((status = 201, body = MappingDetailResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = MappingHeadMovedResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn create_mapping_revision(
    State(state): State<HttpState>,
    path: Result<Path<MappingPath>, PathRejection>,
    payload: Result<Json<UpdateMappingDocumentRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    match state
        .application
        .update_mapping_document(UpdateWorkspaceMappingDocument {
            workspace_id: path.workspace_id,
            mapping_id: path.mapping_id,
            base_content_hash: payload.base_content_hash,
            mappings: payload.mappings,
            updated_at: OffsetDateTime::now_utc(),
        })
        .await?
    {
        UpdateMappingDocumentResult::Updated(detail) => mapping_json_response_with_status(
            MappingDetailResponse::from_detail(*detail)?,
            StatusCode::CREATED,
        ),
        UpdateMappingDocumentResult::HeadMoved { current_head, diff } => {
            mapping_json_response_with_status(
                MappingHeadMovedResponse {
                    code: "head_moved".to_owned(),
                    message: "mapping head moved since the selected base revision".to_owned(),
                    field_problems: Vec::new(),
                    current_head,
                    added: diff.added,
                    removed: diff.removed,
                },
                StatusCode::CONFLICT,
            )
        }
    }
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions/{content_hash}", params(("workspace_id" = Uuid, Path), ("mapping_id" = Uuid, Path), ("content_hash" = String, Path)), responses((status = 200, body = MappingDetailResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn get_mapping_revision(
    State(state): State<HttpState>,
    path: Result<Path<MappingRevisionPath>, PathRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let detail = state
        .application
        .get_mapping_revision(path.workspace_id, path.mapping_id, &path.content_hash)
        .await?;
    mapping_json_response(MappingDetailResponse::from_detail(detail)?)
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions/{content_hash}/resolution-preview", params(("workspace_id" = Uuid, Path), ("mapping_id" = Uuid, Path), ("content_hash" = String, Path)), request_body = MappingResolutionPreviewRequest, responses((status = 200, body = MappingResolutionPreviewResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn preview_mapping_resolution(
    State(state): State<HttpState>,
    path: Result<Path<MappingRevisionPath>, PathRejection>,
    payload: Result<Json<MappingResolutionPreviewRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let mode = MappingResolutionModeResponse::parse(&payload.mode).ok_or_else(|| {
        HttpError(crate::application::error::ApplicationError::field(
            crate::application::error::ApplicationErrorCode::InvalidField,
            "mapping resolution request is invalid",
            "mode",
            "invalid_mode",
            "mode must be direct or transitive",
        ))
    })?;
    if payload.references.is_empty() || payload.references.len() > MAX_MAPPING_RESOLUTION_REFERENCES
    {
        return Err(HttpError(
            crate::application::error::ApplicationError::field(
                crate::application::error::ApplicationErrorCode::InvalidField,
                "mapping resolution request is invalid",
                "references",
                "out_of_range",
                "references must contain between 1 and 256 portable endpoints",
            ),
        ));
    }
    let references = payload
        .references
        .iter()
        .enumerate()
        .map(|(index, reference)| {
            PortableEndpoint::parse(reference).map_err(|error| {
                HttpError(crate::application::error::ApplicationError::field(
                    crate::application::error::ApplicationErrorCode::InvalidField,
                    "mapping resolution request is invalid",
                    format!("references[{index}]"),
                    error.code(),
                    error.to_string(),
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let window = payload
        .window
        .as_deref()
        .map(|window| {
            PortableSelector::parse(window).map_err(|error| {
                HttpError(crate::application::error::ApplicationError::field(
                    crate::application::error::ApplicationErrorCode::InvalidField,
                    "mapping resolution request is invalid",
                    "window",
                    error.code(),
                    error.to_string(),
                ))
            })
        })
        .transpose()?;
    let resolved_at = OffsetDateTime::now_utc();
    let resolution = state
        .application
        .resolve_mapping_revision(
            ExactMappingRevision {
                mapping_id: path.mapping_id,
                content_hash: path.content_hash.clone(),
            },
            ResolutionRequest {
                workspace_id: path.workspace_id,
                mode: mode.into_domain(),
                references,
                window,
                resolved_at,
            },
        )
        .await?;
    mapping_json_response(MappingResolutionPreviewResponse::from_resolution(
        path.mapping_id,
        path.content_hash,
        mode,
        resolved_at,
        resolution,
    )?)
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/activation", params(("workspace_id" = Uuid, Path), ("mapping_id" = Uuid, Path)), request_body = ActivateMappingRevisionRequest, responses((status = 200, body = MappingActivationResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn activate_mapping_revision(
    State(state): State<HttpState>,
    path: Result<Path<MappingPath>, PathRejection>,
    payload: Result<Json<ActivateMappingRevisionRequest>, JsonRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let activation = state
        .application
        .activate_mapping_revision(ActivateWorkspaceMappingRevision {
            event_id: generate_uuid()?,
            workspace_id: path.workspace_id,
            mapping_id: path.mapping_id,
            content_hash: payload.content_hash,
            changed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    mapping_json_response(MappingActivationResponse::from_activation(activation)?)
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/retirement", params(("workspace_id" = Uuid, Path), ("mapping_id" = Uuid, Path)), responses((status = 200, body = MappingActivationResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 413, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(in crate::http::api::v1) async fn retire_mapping(
    State(state): State<HttpState>,
    path: Result<Path<MappingPath>, PathRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let activation = state
        .application
        .retire_mapping(RetireWorkspaceMapping {
            event_id: generate_uuid()?,
            workspace_id: path.workspace_id,
            mapping_id: path.mapping_id,
            changed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    mapping_json_response(MappingActivationResponse::from_activation(activation)?)
}

fn mapping_json_response<T>(payload: T) -> Result<Response, HttpError>
where
    T: Serialize,
{
    mapping_json_response_with_status(payload, StatusCode::OK)
}

fn mapping_json_response_with_status<T>(
    payload: T,
    status: StatusCode,
) -> Result<Response, HttpError>
where
    T: Serialize,
{
    let content = serde_json::to_vec(&payload).map_err(|_| {
        HttpError::from(crate::application::error::ApplicationError::new(
            crate::application::error::ApplicationErrorCode::Internal,
            "mapping response could not be serialized",
        ))
    })?;
    if content.len() > MAX_MAPPING_RESPONSE_BYTES {
        return Err(HttpError::from(
            crate::application::error::ApplicationError::new(
                crate::application::error::ApplicationErrorCode::MappingResponseTooLarge,
                "mapping response exceeds the 8 MiB limit",
            ),
        ));
    }
    let content_length = content.len();
    let mut response = Response::new(Body::from(content));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|_| HttpError::invalid_provider_data("mapping response length is invalid"))?,
    );
    *response.status_mut() = status;
    Ok(response)
}

#[derive(Deserialize)]
pub(in crate::http::api::v1) struct MappingPath {
    workspace_id: Uuid,
    mapping_id: Uuid,
}

#[derive(Deserialize)]
pub(in crate::http::api::v1) struct MappingRevisionPath {
    workspace_id: Uuid,
    mapping_id: Uuid,
    content_hash: String,
}

#[cfg(test)]
mod tests {
    use crate::application::error::ApplicationErrorCode;

    use super::{MAX_MAPPING_RESPONSE_BYTES, mapping_json_response};

    #[test]
    fn mapping_response_rejects_serialized_bytes_over_cap() {
        let error = match mapping_json_response("x".repeat(MAX_MAPPING_RESPONSE_BYTES)) {
            Err(error) => error,
            Ok(_) => panic!("JSON string delimiters should cross the response cap"),
        };

        assert_eq!(error.0.code, ApplicationErrorCode::MappingResponseTooLarge);
    }
}
