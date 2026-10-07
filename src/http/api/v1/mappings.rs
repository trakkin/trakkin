use super::support::HttpState;
use crate::{
    application::error::ApplicationError,
    application::mappings::{
        ChangeMappingSource, DecideMapping, DeleteMapping, QueryMappings, SaveMapping,
        SaveMappingSource,
    },
    http::{
        dto::mappings::{
            DecideMappingRequest, DeleteMappingRequest, MappingPageResponse,
            MappingQueryParameters, MappingResultResponse, MappingSourceResponse,
            MappingSourcesResponse, MappingStateResponse, PreviewMappingRequest,
            SaveMappingRequest, SaveMappingSourceRequest,
        },
        error::{ErrorResponse, HttpError},
    },
};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    routing::{delete, get, post},
};

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route("/api/v1/mappings/state", get(mapping_state))
        .route(
            "/api/v1/mappings/sources",
            get(mapping_sources).post(create_mapping_source),
        )
        .route(
            "/api/v1/mappings/sources/{source_id}",
            axum::routing::put(update_mapping_source).delete(delete_mapping_source),
        )
        .route(
            "/api/v1/mappings/sources/{source_id}/refresh",
            post(refresh_mapping_source),
        )
        .route("/api/v1/mappings", get(query_mappings).post(save_mapping))
        .route("/api/v1/mappings/preview", post(preview_mapping))
        .route("/api/v1/mappings/{claim_id}/decision", post(decide_mapping))
        .route("/api/v1/mappings/{claim_id}/revert", post(revert_mapping))
        .route("/api/v1/mappings/{claim_id}", delete(delete_mapping))
        .layer(DefaultBodyLimit::max(3 * 1024 * 1024))
}

#[utoipa::path(get, path = "/api/v1/mappings/state", responses((status = 200, body = MappingStateResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn mapping_state(
    State(state): State<HttpState>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    Ok(Json(MappingStateResponse::from(
        state.application.mapping_state().await?,
    )))
}

#[utoipa::path(get, path = "/api/v1/mappings/sources", responses((status = 200, body = MappingSourcesResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn mapping_sources(
    State(state): State<HttpState>,
) -> Result<Json<MappingSourcesResponse>, HttpError> {
    let snapshot = state.application.mapping_sources().await?;
    let sources = snapshot
        .sources
        .into_iter()
        .map(MappingSourceResponse::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ApplicationError::internal("mapping source timestamp is invalid"))?;
    Ok(Json(MappingSourcesResponse {
        fingerprint: snapshot.fingerprint,
        sources,
    }))
}

fn source_command(id: Option<String>, body: SaveMappingSourceRequest) -> SaveMappingSource {
    SaveMappingSource {
        id,
        expected_fingerprint: body.expected_fingerprint,
        display_name: body.display_name,
        url: body.url,
        branch: body.branch,
        poll_interval_seconds: body.poll_interval_seconds,
        enabled: body.enabled,
    }
}

#[utoipa::path(post, path = "/api/v1/mappings/sources", request_body = SaveMappingSourceRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn create_mapping_source(
    State(state): State<HttpState>,
    body: Result<Json<SaveMappingSourceRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state
            .application
            .save_mapping_source(source_command(None, body))
            .await?
            .into(),
    ))
}

#[utoipa::path(put, path = "/api/v1/mappings/sources/{source_id}", params(("source_id" = String, Path)), request_body = SaveMappingSourceRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn update_mapping_source(
    State(state): State<HttpState>,
    Path(source_id): Path<String>,
    body: Result<Json<SaveMappingSourceRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state
            .application
            .save_mapping_source(source_command(Some(source_id), body))
            .await?
            .into(),
    ))
}

#[utoipa::path(delete, path = "/api/v1/mappings/sources/{source_id}", params(("source_id" = String, Path)), request_body = DeleteMappingRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn delete_mapping_source(
    State(state): State<HttpState>,
    Path(source_id): Path<String>,
    body: Result<Json<DeleteMappingRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state
            .application
            .delete_mapping_source(ChangeMappingSource {
                id: source_id,
                expected_fingerprint: body.expected_fingerprint,
            })
            .await?
            .into(),
    ))
}

#[utoipa::path(post, path = "/api/v1/mappings/sources/{source_id}/refresh", params(("source_id" = String, Path)), request_body = DeleteMappingRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn refresh_mapping_source(
    State(state): State<HttpState>,
    Path(source_id): Path<String>,
    body: Result<Json<DeleteMappingRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state
            .application
            .refresh_mapping_source(ChangeMappingSource {
                id: source_id,
                expected_fingerprint: body.expected_fingerprint,
            })
            .await?
            .into(),
    ))
}

#[utoipa::path(post, path = "/api/v1/mappings/{claim_id}/revert", params(("claim_id" = String, Path)), request_body = DeleteMappingRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn revert_mapping(
    State(state): State<HttpState>,
    Path(claim_id): Path<String>,
    body: Result<Json<DeleteMappingRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state
            .application
            .revert_mapping(DeleteMapping {
                claim_id,
                expected_fingerprint: body.expected_fingerprint,
            })
            .await?
            .into(),
    ))
}

#[utoipa::path(get, path = "/api/v1/mappings", params(MappingQueryParameters), responses((status = 200, body = MappingPageResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn query_mappings(
    State(state): State<HttpState>,
    parameters: Result<Query<MappingQueryParameters>, QueryRejection>,
) -> Result<Json<MappingPageResponse>, HttpError> {
    let Query(parameters) =
        parameters.map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let page = state
        .application
        .query_mappings(QueryMappings {
            expected_fingerprint: parameters.expected_fingerprint,
            search: parameters.search,
            canonical_statement: parameters.canonical_statement,
            claim_id: parameters.claim_id,
            operator: parameters.operator.map(Into::into),
            endpoint: parameters.endpoint,
            annotation_name: parameters.annotation_name,
            annotation_value: parameters.annotation_value,
            limit: parameters.limit.unwrap_or(50),
            cursor: parameters.cursor,
        })
        .await?;
    Ok(Json(MappingPageResponse::from(page)))
}

#[utoipa::path(post, path = "/api/v1/mappings/preview", request_body = PreviewMappingRequest, responses((status = 200, body = MappingResultResponse), (status = 400, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn preview_mapping(
    State(state): State<HttpState>,
    body: Result<Json<PreviewMappingRequest>, JsonRejection>,
) -> Result<Json<MappingResultResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state.application.preview_mapping(body.record).await?.into(),
    ))
}

#[utoipa::path(post, path = "/api/v1/mappings", request_body = SaveMappingRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn save_mapping(
    State(state): State<HttpState>,
    body: Result<Json<SaveMappingRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state
            .application
            .save_mapping(SaveMapping {
                expected_fingerprint: body.expected_fingerprint,
                record: body.record,
                replaces: body.replaces,
            })
            .await?
            .into(),
    ))
}

#[utoipa::path(post, path = "/api/v1/mappings/{claim_id}/decision", params(("claim_id" = String, Path)), request_body = DecideMappingRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn decide_mapping(
    State(state): State<HttpState>,
    Path(claim_id): Path<String>,
    body: Result<Json<DecideMappingRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state
            .application
            .decide_mapping(DecideMapping {
                expected_fingerprint: body.expected_fingerprint,
                claim_id,
                rejected: body.rejected,
            })
            .await?
            .into(),
    ))
}

#[utoipa::path(delete, path = "/api/v1/mappings/{claim_id}", params(("claim_id" = String, Path)), request_body = DeleteMappingRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn delete_mapping(
    State(state): State<HttpState>,
    Path(claim_id): Path<String>,
    body: Result<Json<DeleteMappingRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(body) = body.map_err(invalid_body)?;
    Ok(Json(
        state
            .application
            .delete_mapping(DeleteMapping {
                expected_fingerprint: body.expected_fingerprint,
                claim_id,
            })
            .await?
            .into(),
    ))
}

fn invalid_body(_: JsonRejection) -> HttpError {
    HttpError::invalid_request("mapping request body is invalid")
}
