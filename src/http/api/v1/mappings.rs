use axum::{
    Json, Router,
    body::Body,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG},
    },
    response::{IntoResponse, Response},
    routing::{get, put},
};
use futures_util::TryStreamExt;
use time::OffsetDateTime;
use tokio_util::io::{ReaderStream, StreamReader};
use uuid::Uuid;

use crate::{
    application::mappings::{
        CreateLocalMapping, CreateRemoteMappingSource, CreateUploadedMappingSource,
        MappingContentReader, QueryMappings, RefreshRemoteMappingSource, RemoveLocalMapping,
        RemoveMappingSource, ReorderMappingSources, UpdateLocalMapping, UpdateMappingSource,
    },
    http::{
        dto::mappings::{
            CreateRemoteMappingSourceRequest, CreateUploadedMappingSourceParameters,
            MappingPageResponse, MappingQueryParameters, MappingStateResponse,
            ReorderMappingSourcesRequest, UpdateMappingSourceRequest, WriteLocalMappingRequest,
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::support::{HttpState, generate_uuid};

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route("/api/v1/mapping-sources", get(mapping_state))
        .route(
            "/api/v1/mapping-sources/uploads",
            axum::routing::post(create_uploaded_mapping_source),
        )
        .route(
            "/api/v1/mapping-sources/remotes",
            axum::routing::post(create_remote_mapping_source),
        )
        .route(
            "/api/v1/mapping-sources/order",
            put(reorder_mapping_sources),
        )
        .route(
            "/api/v1/mapping-sources/{source_id}",
            axum::routing::patch(update_mapping_source).delete(remove_mapping_source),
        )
        .route(
            "/api/v1/mapping-sources/{source_id}/content",
            get(read_mapping_source_content),
        )
        .route(
            "/api/v1/mapping-sources/{source_id}/refresh",
            axum::routing::post(refresh_remote_mapping_source),
        )
        .route(
            "/api/v1/mappings/local",
            axum::routing::post(create_local_mapping),
        )
        .route(
            "/api/v1/mappings/local/{statement_id}",
            put(update_local_mapping).delete(remove_local_mapping),
        )
        .route("/api/v1/mappings", get(query_mappings))
}

#[utoipa::path(get, path = "/api/v1/mapping-sources", responses((status = 200, body = MappingStateResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn mapping_state(
    State(state): State<HttpState>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    Ok(Json(MappingStateResponse::from_snapshot(
        state.application.mapping_state().await?,
    )?))
}

#[utoipa::path(post, path = "/api/v1/mapping-sources/uploads", params(CreateUploadedMappingSourceParameters), request_body(content = String, content_type = "text/plain"), responses((status = 201, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn create_uploaded_mapping_source(
    State(state): State<HttpState>,
    parameters: Result<Query<CreateUploadedMappingSourceParameters>, QueryRejection>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<MappingStateResponse>), HttpError> {
    let Query(parameters) =
        parameters.map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let snapshot = state
        .application
        .create_uploaded_mapping_source(
            CreateUploadedMappingSource {
                source_id: generate_uuid()?,
                source_key: parameters.source_key,
                display_name: parameters.display_name,
                enabled: parameters.enabled,
                created_at: OffsetDateTime::now_utc(),
            },
            mapping_content_reader(&headers, body)?,
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(MappingStateResponse::from_snapshot(snapshot)?),
    ))
}

#[utoipa::path(post, path = "/api/v1/mapping-sources/remotes", request_body = CreateRemoteMappingSourceRequest, responses((status = 201, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 502, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn create_remote_mapping_source(
    State(state): State<HttpState>,
    payload: Result<Json<CreateRemoteMappingSourceRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<MappingStateResponse>), HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let snapshot = state
        .application
        .create_remote_mapping_source(CreateRemoteMappingSource {
            source_id: generate_uuid()?,
            source_key: payload.source_key,
            display_name: payload.display_name,
            remote_url: payload.remote_url,
            enabled: payload.enabled,
            created_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(MappingStateResponse::from_snapshot(snapshot)?),
    ))
}

#[utoipa::path(patch, path = "/api/v1/mapping-sources/{source_id}", params(("source_id" = Uuid, Path)), request_body = UpdateMappingSourceRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn update_mapping_source(
    State(state): State<HttpState>,
    Path(source_id): Path<Uuid>,
    payload: Result<Json<UpdateMappingSourceRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let snapshot = state
        .application
        .update_mapping_source(UpdateMappingSource {
            source_id,
            display_name: payload.display_name,
            enabled: payload.enabled,
            updated_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(MappingStateResponse::from_snapshot(snapshot)?))
}

#[utoipa::path(post, path = "/api/v1/mappings/local", request_body = WriteLocalMappingRequest, responses((status = 201, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn create_local_mapping(
    State(state): State<HttpState>,
    payload: Result<Json<WriteLocalMappingRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<MappingStateResponse>), HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let snapshot = state
        .application
        .create_local_mapping(CreateLocalMapping {
            record: payload.record,
            created_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(MappingStateResponse::from_snapshot(snapshot)?),
    ))
}

#[utoipa::path(put, path = "/api/v1/mappings/local/{statement_id}", params(("statement_id" = String, Path)), request_body = WriteLocalMappingRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn update_local_mapping(
    State(state): State<HttpState>,
    Path(statement_id): Path<String>,
    payload: Result<Json<WriteLocalMappingRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let snapshot = state
        .application
        .update_local_mapping(UpdateLocalMapping {
            statement_id,
            record: payload.record,
            updated_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(MappingStateResponse::from_snapshot(snapshot)?))
}

#[utoipa::path(delete, path = "/api/v1/mappings/local/{statement_id}", params(("statement_id" = String, Path)), responses((status = 200, body = MappingStateResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn remove_local_mapping(
    State(state): State<HttpState>,
    Path(statement_id): Path<String>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let snapshot = state
        .application
        .remove_local_mapping(RemoveLocalMapping {
            statement_id,
            removed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(MappingStateResponse::from_snapshot(snapshot)?))
}

#[utoipa::path(put, path = "/api/v1/mapping-sources/order", request_body = ReorderMappingSourcesRequest, responses((status = 200, body = MappingStateResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn reorder_mapping_sources(
    State(state): State<HttpState>,
    payload: Result<Json<ReorderMappingSourcesRequest>, JsonRejection>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let snapshot = state
        .application
        .reorder_mapping_sources(ReorderMappingSources {
            source_ids: payload.source_ids,
            updated_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(MappingStateResponse::from_snapshot(snapshot)?))
}

#[utoipa::path(delete, path = "/api/v1/mapping-sources/{source_id}", params(("source_id" = Uuid, Path)), responses((status = 200, body = MappingStateResponse), (status = 404, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn remove_mapping_source(
    State(state): State<HttpState>,
    Path(source_id): Path<Uuid>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let snapshot = state
        .application
        .remove_mapping_source(RemoveMappingSource {
            source_id,
            removed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(MappingStateResponse::from_snapshot(snapshot)?))
}

#[utoipa::path(post, path = "/api/v1/mapping-sources/{source_id}/refresh", params(("source_id" = Uuid, Path)), responses((status = 200, body = MappingStateResponse), (status = 404, body = ErrorResponse), (status = 422, body = ErrorResponse), (status = 502, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn refresh_remote_mapping_source(
    State(state): State<HttpState>,
    Path(source_id): Path<Uuid>,
) -> Result<Json<MappingStateResponse>, HttpError> {
    let snapshot = state
        .application
        .refresh_remote_mapping_source(RefreshRemoteMappingSource {
            source_id,
            refreshed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(MappingStateResponse::from_snapshot(snapshot)?))
}

#[utoipa::path(get, path = "/api/v1/mapping-sources/{source_id}/content", params(("source_id" = Uuid, Path)), responses((status = 200, content_type = "text/plain", body = String), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "mappings")]
pub(super) async fn read_mapping_source_content(
    State(state): State<HttpState>,
    Path(source_id): Path<Uuid>,
) -> Result<Response, HttpError> {
    let content = state
        .application
        .read_mapping_source_content(source_id)
        .await?;
    let mut response = Body::from_stream(ReaderStream::new(content.reader)).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content.byte_length.to_string())
            .map_err(|_| HttpError::invalid_request("mapping content length is invalid"))?,
    );
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(&format!("\"{}\"", content.content_hash))
            .map_err(|_| HttpError::invalid_request("mapping content hash is invalid"))?,
    );
    Ok(response)
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
            expected_chain_fingerprint: parameters.expected_chain_fingerprint,
            search: parameters.search,
            canonical_statement: parameters.canonical_statement,
            statement_id: parameters.statement_id,
            relation_key: parameters.relation_key,
            operator: parameters.operator.map(Into::into),
            source_key: parameters.source_key,
            endpoint: parameters.endpoint,
            annotation_name: parameters.annotation_name,
            annotation_value: parameters.annotation_value,
            include_shadowed: parameters.include_shadowed,
            limit: parameters.limit.unwrap_or(50),
            cursor: parameters.cursor,
        })
        .await?;
    Ok(Json(MappingPageResponse::from(page)))
}

fn mapping_content_reader(
    headers: &HeaderMap,
    body: Body,
) -> Result<MappingContentReader, HttpError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type.eq_ignore_ascii_case("text/plain")
        && !content_type.to_ascii_lowercase().starts_with("text/plain;")
    {
        return Err(HttpError::invalid_field(
            "contentType",
            "unsupported_content_type",
            "mapping content must use text/plain",
        ));
    }
    let stream = body.into_data_stream().map_err(std::io::Error::other);
    Ok(Box::pin(StreamReader::new(stream)))
}
