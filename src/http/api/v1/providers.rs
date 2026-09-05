use axum::{
    Json, Router,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{get, post, put},
};
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::providers::{
        ChangeProviderUpdatePolicy, InstallProvider, RefreshProviderSource, RemoveProvider,
        RemoveProviderSource, TrustProviderSource, UpdateProvider,
    },
    http::{
        dto::providers::{
            ChangeProviderUpdatePolicyRequest, InstallProviderRequest, ProviderResponse,
            ProviderSourceResponse, TrustProviderSourceRequest, UpdateProviderRequest,
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::support::{HttpState, generate_uuid};

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route(
            "/api/v1/provider-sources",
            get(list_provider_sources).post(trust_provider_source),
        )
        .route(
            "/api/v1/provider-sources/{source_id}",
            axum::routing::delete(remove_provider_source),
        )
        .route(
            "/api/v1/provider-sources/{source_id}/refresh",
            post(refresh_provider_source),
        )
        .route(
            "/api/v1/providers",
            get(list_providers).post(install_provider),
        )
        .route(
            "/api/v1/providers/{provider_id}",
            axum::routing::delete(remove_provider),
        )
        .route(
            "/api/v1/providers/{provider_id}/update",
            post(update_provider),
        )
        .route(
            "/api/v1/providers/{provider_id}/update-policy",
            put(change_provider_update_policy),
        )
}

#[utoipa::path(get, path = "/api/v1/provider-sources", responses((status = 200, body = [ProviderSourceResponse]), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn list_provider_sources(
    State(state): State<HttpState>,
) -> Result<Json<Vec<ProviderSourceResponse>>, HttpError> {
    Ok(Json(
        state
            .application
            .list_provider_sources()
            .await?
            .into_iter()
            .map(ProviderSourceResponse::from_snapshot)
            .collect::<Result<_, _>>()?,
    ))
}

#[utoipa::path(post, path = "/api/v1/provider-sources", request_body = TrustProviderSourceRequest, responses((status = 201, body = ProviderSourceResponse), (status = 400, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn trust_provider_source(
    State(state): State<HttpState>,
    payload: Result<Json<TrustProviderSourceRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ProviderSourceResponse>), HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let source = state
        .application
        .trust_provider_source(TrustProviderSource {
            source_id: generate_uuid()?,
            repository_url: payload.repository_url,
            trusted_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ProviderSourceResponse::from_snapshot(source)?),
    ))
}

#[utoipa::path(post, path = "/api/v1/provider-sources/{source_id}/refresh", params(("source_id" = Uuid, Path)), responses((status = 200, body = ProviderSourceResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "providers")]
pub(super) async fn refresh_provider_source(
    State(state): State<HttpState>,
    Path(path): Path<SourcePath>,
) -> Result<Json<ProviderSourceResponse>, HttpError> {
    let source_id = parse_source_id(&path.source_id)?;
    let source = state
        .application
        .refresh_provider_source(RefreshProviderSource {
            source_id,
            checked_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(ProviderSourceResponse::from_snapshot(source)?))
}

#[utoipa::path(delete, path = "/api/v1/provider-sources/{source_id}", params(("source_id" = Uuid, Path)), responses((status = 204), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn remove_provider_source(
    State(state): State<HttpState>,
    Path(path): Path<SourcePath>,
) -> Result<StatusCode, HttpError> {
    let source_id = parse_source_id(&path.source_id)?;
    state
        .application
        .remove_provider_source(RemoveProviderSource { source_id })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path = "/api/v1/providers", responses((status = 200, body = [ProviderResponse]), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn list_providers(
    State(state): State<HttpState>,
) -> Result<Json<Vec<ProviderResponse>>, HttpError> {
    Ok(Json(
        state
            .application
            .list_providers()
            .await?
            .into_iter()
            .map(ProviderResponse::from_snapshot)
            .collect::<Result<_, _>>()?,
    ))
}

#[utoipa::path(post, path = "/api/v1/providers", request_body = InstallProviderRequest, responses((status = 201, body = ProviderResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "providers")]
pub(super) async fn install_provider(
    State(state): State<HttpState>,
    payload: Result<Json<InstallProviderRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ProviderResponse>), HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let provider = state
        .application
        .install_provider(InstallProvider {
            artifact_id: generate_uuid()?,
            source_id: payload.source_id,
            provider_id: payload.provider_id,
            version: payload.version,
            update_policy: payload.update_policy.into(),
            installed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ProviderResponse::from_snapshot(provider)?),
    ))
}

#[utoipa::path(post, path = "/api/v1/providers/{provider_id}/update", params(("provider_id" = String, Path)), request_body = UpdateProviderRequest, responses((status = 200, body = ProviderResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "providers")]
pub(super) async fn update_provider(
    State(state): State<HttpState>,
    Path(path): Path<ProviderPath>,
    payload: Result<Json<UpdateProviderRequest>, JsonRejection>,
) -> Result<Json<ProviderResponse>, HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let provider = state
        .application
        .update_provider(UpdateProvider {
            provider_id: path.provider_id,
            artifact_id: generate_uuid()?,
            version: payload.version,
            updated_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(ProviderResponse::from_snapshot(provider)?))
}

#[utoipa::path(put, path = "/api/v1/providers/{provider_id}/update-policy", params(("provider_id" = String, Path)), request_body = ChangeProviderUpdatePolicyRequest, responses((status = 200, body = ProviderResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "providers")]
pub(super) async fn change_provider_update_policy(
    State(state): State<HttpState>,
    Path(path): Path<ProviderPath>,
    payload: Result<Json<ChangeProviderUpdatePolicyRequest>, JsonRejection>,
) -> Result<Json<ProviderResponse>, HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let provider = state
        .application
        .change_provider_update_policy(ChangeProviderUpdatePolicy {
            provider_id: path.provider_id,
            update_policy: payload.update_policy.into(),
            updated_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(ProviderResponse::from_snapshot(provider)?))
}

#[utoipa::path(delete, path = "/api/v1/providers/{provider_id}", params(("provider_id" = String, Path)), responses((status = 204), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn remove_provider(
    State(state): State<HttpState>,
    Path(path): Path<ProviderPath>,
) -> Result<StatusCode, HttpError> {
    state
        .application
        .remove_provider(RemoveProvider {
            provider_id: path.provider_id,
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn parse_source_id(value: &str) -> Result<Uuid, HttpError> {
    Uuid::parse_str(value).map_err(|_| {
        HttpError::invalid_field(
            "sourceId",
            "invalid_uuid",
            "provider source ID must be a UUID",
        )
    })
}

#[derive(Deserialize)]
pub(super) struct ProviderPath {
    provider_id: String,
}

#[derive(Deserialize)]
pub(super) struct SourcePath {
    source_id: String,
}

#[cfg(test)]
mod tests {
    use crate::application::error::ApplicationErrorCode;

    use super::*;

    #[test]
    fn malformed_source_id_uses_structured_field_error() {
        let error = parse_source_id("not-a-uuid")
            .expect_err("malformed source ID is rejected")
            .0;

        assert_eq!(error.code, ApplicationErrorCode::InvalidField);
        assert_eq!(error.field_problems[0].path, "sourceId");
        assert_eq!(error.field_problems[0].code, "invalid_uuid");
    }
}
