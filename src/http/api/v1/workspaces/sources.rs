use axum::{
    Json, Router,
    body::Body,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::{
        HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG},
    },
    response::Response,
    routing::{get, post, put},
};
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::{
        assets::ReadPublishedBinaryAsset,
        catalog_queries::{InspectSourceState, QueryCatalogPage},
        reference_lookup::LookupPortableReferences,
        task_manager::{AdmitTask, SourceStream, TaskDefinition},
        workspaces::{
            AttachWorkspaceSource, DetachWorkspaceSource, UpdateWorkspaceSourceMappingRootPolicy,
        },
    },
    domain::acquisition::{
        BinaryAssetKey, CatalogRelationKey, PortableReference, ProviderItemKey, StateSubject,
    },
    http::{
        dto::{
            sources::{
                CatalogPageResponse, CatalogQueryParameters, LookupPortableReferencesRequest,
                PortableReferenceLookupResponse, SourceStateResponse, StartSourceReadRequest,
                StartSourceResetRequest, StateQueryParameters,
            },
            tasks::TaskResponse,
            workspaces::{
                MappingRootPolicyResponse, UpdateMappingRootPolicyRequest, WorkspaceResponse,
            },
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::super::support::{HttpState, generate_uuid};

const DEFAULT_CATALOG_PAGE_SIZE: u32 = 60;

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}",
            put(attach_source).delete(detach_source),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}/catalog",
            get(get_catalog),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}/mapping-root-policy",
            get(get_mapping_root_policy).put(update_mapping_root_policy),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}/catalog/{publication_id}/assets/{provider_item_namespace}/{provider_item_value}/{asset_namespace}/{asset_value}",
            get(get_binary_asset),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}/reference-lookups",
            post(lookup_portable_references),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}/state",
            get(get_state),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}/reads",
            post(start_source_read),
        )
        .route(
            "/api/v1/workspaces/{workspace_id}/sources/{source_id}/resets",
            post(start_source_reset),
        )
}

#[utoipa::path(put, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path)), responses((status = 200, body = WorkspaceResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse)), tag = "workspaces")]
pub(in crate::http::api::v1) async fn attach_source(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
) -> Result<Json<WorkspaceResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let workspace = state
        .application
        .attach_source(AttachWorkspaceSource {
            workspace_id: path.workspace_id,
            source_id: path.source_id,
            attached_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(WorkspaceResponse::from_view(workspace)?))
}

#[utoipa::path(delete, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path)), responses((status = 200, body = WorkspaceResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse)), tag = "workspaces")]
pub(in crate::http::api::v1) async fn detach_source(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
) -> Result<Json<WorkspaceResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let workspace = state
        .application
        .detach_source(DetachWorkspaceSource {
            workspace_id: path.workspace_id,
            source_id: path.source_id,
        })
        .await?;
    Ok(Json(WorkspaceResponse::from_view(workspace)?))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}/mapping-root-policy", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path)), responses((status = 200, body = MappingRootPolicyResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "workspaces")]
pub(in crate::http::api::v1) async fn get_mapping_root_policy(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
) -> Result<Json<MappingRootPolicyResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let policy = state
        .application
        .get_mapping_root_policy(path.workspace_id, path.source_id)
        .await?;
    Ok(Json(MappingRootPolicyResponse::from_policy(policy)))
}

#[utoipa::path(put, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}/mapping-root-policy", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path)), request_body = UpdateMappingRootPolicyRequest, responses((status = 200, body = MappingRootPolicyResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "workspaces")]
pub(in crate::http::api::v1) async fn update_mapping_root_policy(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
    payload: Result<Json<UpdateMappingRootPolicyRequest>, JsonRejection>,
) -> Result<Json<MappingRootPolicyResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let policy = state
        .application
        .update_mapping_root_policy(UpdateWorkspaceSourceMappingRootPolicy {
            workspace_id: path.workspace_id,
            source_id: path.source_id,
            namespaces: payload.namespaces,
        })
        .await?;
    Ok(Json(MappingRootPolicyResponse::from_policy(policy)))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}/catalog", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path), CatalogQueryParameters), responses((status = 200, body = CatalogPageResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "catalog")]
pub(in crate::http::api::v1) async fn get_catalog(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
    query: Result<Query<CatalogQueryParameters>, QueryRejection>,
) -> Result<Json<CatalogPageResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Query(query) =
        query.map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let parent = catalog_parent(&query)?;
    let page = state
        .application
        .query_catalog(QueryCatalogPage {
            workspace_id: path.workspace_id,
            source_id: path.source_id,
            parent,
            search: query.search,
            limit: query.limit.unwrap_or(DEFAULT_CATALOG_PAGE_SIZE),
            cursor: query.cursor,
        })
        .await?;
    Ok(Json(CatalogPageResponse::from_page(
        page,
        path.workspace_id,
        path.source_id,
    )))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}/catalog/{publication_id}/assets/{provider_item_namespace}/{provider_item_value}/{asset_namespace}/{asset_value}", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path), ("publication_id" = Uuid, Path), ("provider_item_namespace" = String, Path, description = "Hex-encoded provider-item namespace"), ("provider_item_value" = String, Path, description = "Hex-encoded provider-item value"), ("asset_namespace" = String, Path, description = "Hex-encoded binary-asset namespace"), ("asset_value" = String, Path, description = "Hex-encoded binary-asset value")), responses((status = 200, content_type = "*/*", body = Vec<u8>, headers(("content-type" = String, description = "Provider-declared binary asset content type"), ("content-length" = u64, description = "Complete binary asset length"), ("cache-control" = String, description = "Provider-declared cache policy"), ("etag" = String, description = "Quoted SHA-256 digest"), ("x-content-type-options" = String, description = "Always nosniff"))), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "catalog")]
pub(in crate::http::api::v1) async fn get_binary_asset(
    State(state): State<HttpState>,
    path: Result<Path<BinaryAssetPath>, PathRejection>,
) -> Result<Response, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let provider_item_key = ProviderItemKey::new(
        decode_hex_namespace(&path.provider_item_namespace, "providerItemNamespace")?,
        decode_hex_value(&path.provider_item_value, "providerItemValue")?,
    )
    .map_err(|_| invalid_binary_asset_key("providerItemValue"))?;
    let asset_key = BinaryAssetKey::new(
        decode_hex_namespace(&path.asset_namespace, "assetNamespace")?,
        decode_hex_value(&path.asset_value, "assetValue")?,
    )
    .map_err(|_| invalid_binary_asset_key("assetValue"))?;
    let asset = state
        .application
        .read_binary_asset(ReadPublishedBinaryAsset {
            operation_id: generate_uuid()?.as_bytes().to_vec(),
            workspace_id: path.workspace_id,
            source_id: path.source_id,
            publication_id: path.publication_id,
            provider_item_key,
            asset_key,
        })
        .await?;
    binary_asset_http_response(asset)
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}/reference-lookups", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path)), request_body = LookupPortableReferencesRequest, responses((status = 200, body = [PortableReferenceLookupResponse]), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "catalog")]
pub(in crate::http::api::v1) async fn lookup_portable_references(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
    payload: Result<Json<LookupPortableReferencesRequest>, JsonRejection>,
) -> Result<Json<Vec<PortableReferenceLookupResponse>>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let references = payload
        .references
        .into_iter()
        .enumerate()
        .map(|(index, reference)| {
            PortableReference::new(reference.namespace, reference.value).map_err(|_| {
                HttpError::invalid_field(
                    format!("references[{index}]"),
                    "invalid_reference",
                    "portable reference namespace and value must not be empty",
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let lookups = state
        .application
        .lookup_portable_references(LookupPortableReferences {
            operation_id: generate_uuid()?.as_bytes().to_vec(),
            workspace_id: path.workspace_id,
            source_id: path.source_id,
            references,
        })
        .await?;
    let policy = state
        .application
        .get_mapping_root_policy(path.workspace_id, path.source_id)
        .await?;
    let mapping_root_policy = crate::application::workspaces::MappingRootPolicy::from(
        policy.attachment.mapping_root_namespace_allowlist,
    );
    Ok(Json(
        lookups
            .into_iter()
            .map(|lookup| {
                PortableReferenceLookupResponse::from_lookup(lookup, &mapping_root_policy)
            })
            .collect::<Result<_, _>>()?,
    ))
}

#[utoipa::path(get, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}/state", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path), StateQueryParameters), responses((status = 200, body = SourceStateResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "catalog")]
pub(in crate::http::api::v1) async fn get_state(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
    query: Result<Query<StateQueryParameters>, QueryRejection>,
) -> Result<Json<SourceStateResponse>, HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Query(query) =
        query.map_err(|_| HttpError::invalid_request("query parameters are invalid"))?;
    let inspection = state
        .application
        .inspect_state(InspectSourceState {
            workspace_id: path.workspace_id,
            source_id: path.source_id,
            subject: state_subject(&query)?,
        })
        .await?;
    Ok(Json(SourceStateResponse::from_inspection(inspection)?))
}

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}/reads", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path)), request_body = StartSourceReadRequest, responses((status = 202, body = TaskResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "tasks")]
pub(in crate::http::api::v1) async fn start_source_read(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
    payload: Result<Json<StartSourceReadRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<TaskResponse>), HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let task = state
        .application
        .admit_task(AdmitTask {
            task_id: generate_uuid()?,
            workspace_id: Some(path.workspace_id),
            definition: TaskDefinition::Read {
                source_id: path.source_id,
                stream: parse_stream(&payload.stream)?,
                mode: parse_read_mode(&payload.mode)?,
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

#[utoipa::path(post, path = "/api/v1/workspaces/{workspace_id}/sources/{source_id}/resets", params(("workspace_id" = Uuid, Path), ("source_id" = Uuid, Path)), request_body = StartSourceResetRequest, responses((status = 202, body = TaskResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse)), tag = "tasks")]
pub(in crate::http::api::v1) async fn start_source_reset(
    State(state): State<HttpState>,
    path: Result<Path<SourcePath>, PathRejection>,
    payload: Result<Json<StartSourceResetRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<TaskResponse>), HttpError> {
    let Path(path) = path.map_err(|_| HttpError::invalid_request("path parameters are invalid"))?;
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let task = state
        .application
        .admit_task(AdmitTask {
            task_id: generate_uuid()?,
            workspace_id: Some(path.workspace_id),
            definition: TaskDefinition::Reset {
                source_id: path.source_id,
                stream: parse_stream(&payload.stream)?,
            },
            cancellation_supported: false,
            admitted_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(TaskResponse::from_snapshot(task)?),
    ))
}

fn catalog_parent(query: &CatalogQueryParameters) -> Result<Option<CatalogRelationKey>, HttpError> {
    match (&query.parent_namespace, &query.parent_value) {
        (None, None) => Ok(None),
        (Some(namespace), Some(value)) => {
            let value = hex::decode(value).map_err(|_| {
                HttpError::invalid_field(
                    "parentValue",
                    "invalid_hex",
                    "catalog parent value must be hexadecimal",
                )
            })?;
            CatalogRelationKey::new(namespace.clone(), value)
                .map(Some)
                .map_err(|_| {
                    HttpError::invalid_field(
                        "parentValue",
                        "invalid_key",
                        "catalog parent key must not be empty",
                    )
                })
        }
        _ => Err(HttpError::invalid_field(
            "parent",
            "incomplete_key",
            "catalog parent namespace and value must be supplied together",
        )),
    }
}

fn state_subject(query: &StateQueryParameters) -> Result<Option<StateSubject>, HttpError> {
    match (
        &query.subject_kind,
        &query.subject_namespace,
        &query.subject_value,
    ) {
        (None, None, None) => Ok(None),
        (Some(kind), Some(namespace), Some(value)) => {
            let value = hex::decode(value).map_err(|_| {
                HttpError::invalid_field(
                    "subjectValue",
                    "invalid_hex",
                    "state subject value must be hexadecimal",
                )
            })?;
            match kind.as_str() {
                "providerItem" => ProviderItemKey::new(namespace.clone(), value)
                    .map(StateSubject::ProviderItem)
                    .map(Some)
                    .map_err(|_| invalid_state_subject()),
                "catalogRelation" => CatalogRelationKey::new(namespace.clone(), value)
                    .map(StateSubject::CatalogRelation)
                    .map(Some)
                    .map_err(|_| invalid_state_subject()),
                _ => Err(HttpError::invalid_field(
                    "subjectKind",
                    "unsupported_value",
                    "state subject kind must be providerItem or catalogRelation",
                )),
            }
        }
        _ => Err(HttpError::invalid_field(
            "subject",
            "incomplete_key",
            "state subject kind, namespace, and value must be supplied together",
        )),
    }
}

fn invalid_state_subject() -> HttpError {
    HttpError::invalid_field(
        "subjectValue",
        "invalid_key",
        "state subject key must not be empty",
    )
}

fn decode_hex_namespace(value: &str, path: &str) -> Result<String, HttpError> {
    let bytes = decode_hex_value(value, path)?;
    String::from_utf8(bytes).map_err(|_| {
        HttpError::invalid_field(
            path,
            "invalid_namespace",
            "key namespace must be hexadecimal UTF-8",
        )
    })
}

fn decode_hex_value(value: &str, path: &str) -> Result<Vec<u8>, HttpError> {
    hex::decode(value)
        .map_err(|_| HttpError::invalid_field(path, "invalid_hex", "key part must be hexadecimal"))
}

fn invalid_binary_asset_key(path: &str) -> HttpError {
    HttpError::invalid_field(
        path,
        "invalid_key",
        "binary asset key parts must not be empty",
    )
}

fn binary_asset_http_response(
    asset: crate::application::adapter_runtime::AdapterBinaryAsset,
) -> Result<Response, HttpError> {
    let mut response = Response::new(Body::from(asset.content));
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        binary_asset_header(&asset.content_type, "binary asset content type is invalid")?,
    );
    headers.insert(
        CONTENT_LENGTH,
        binary_asset_header(
            &asset.full_length.to_string(),
            "binary asset length is invalid",
        )?,
    );
    headers.insert(
        CACHE_CONTROL,
        binary_asset_header(&asset.cache_control, "binary asset cache policy is invalid")?,
    );
    headers.insert(
        ETAG,
        binary_asset_header(
            &format!("\"{}\"", hex::encode(asset.sha256_digest)),
            "binary asset digest is invalid",
        )?,
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

fn binary_asset_header(value: &str, message: &'static str) -> Result<HeaderValue, HttpError> {
    HeaderValue::from_str(value).map_err(|_| HttpError::invalid_provider_data(message))
}

fn parse_stream(value: &str) -> Result<SourceStream, HttpError> {
    match value {
        "catalog" => Ok(SourceStream::Catalog),
        "state" => Ok(SourceStream::State),
        _ => Err(HttpError::invalid_field(
            "stream",
            "unsupported_value",
            "stream must be catalog or state",
        )),
    }
}

fn parse_read_mode(value: &str) -> Result<crate::application::acquisition::ReadMode, HttpError> {
    match value {
        "full" => Ok(crate::application::acquisition::ReadMode::Full),
        "incremental" => Ok(crate::application::acquisition::ReadMode::Incremental),
        _ => Err(HttpError::invalid_field(
            "mode",
            "unsupported_value",
            "read mode must be full or incremental",
        )),
    }
}

#[derive(Deserialize)]
pub(in crate::http::api::v1) struct SourcePath {
    workspace_id: Uuid,
    source_id: Uuid,
}

#[derive(Deserialize)]
pub(in crate::http::api::v1) struct BinaryAssetPath {
    workspace_id: Uuid,
    source_id: Uuid,
    publication_id: Uuid,
    provider_item_namespace: String,
    provider_item_value: String,
    asset_namespace: String,
    asset_value: String,
}
