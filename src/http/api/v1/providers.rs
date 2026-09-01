use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{get, post, put},
};
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    application::providers::{
        ActivateProviderInstallation, InstallProvider, RefreshProviderRepository,
        RegisterProviderRepository, RemoveProviderInstallation,
    },
    http::{
        dto::providers::{
            ActivateProviderRequest, InstallProviderRequest, ProviderInstallationResponse,
            ProviderReleaseResponse, ProviderRepositoryResponse, RegisterProviderRepositoryRequest,
        },
        error::{ErrorResponse, HttpError},
    },
};

use super::support::{HttpState, generate_uuid};

const MAXIMUM_REGISTER_REPOSITORY_REQUEST_BYTES: usize = 16 * 1024;

pub(super) fn router() -> Router<HttpState> {
    Router::new()
        .route(
            "/api/v1/provider-repositories",
            get(list_repositories)
                .post(register_repository)
                .layer(DefaultBodyLimit::max(
                    MAXIMUM_REGISTER_REPOSITORY_REQUEST_BYTES,
                )),
        )
        .route(
            "/api/v1/provider-repositories/{repository_id}/refresh",
            post(refresh_repository),
        )
        .route("/api/v1/providers", get(list_providers))
        .route(
            "/api/v1/provider-installations",
            get(list_installations).post(install_provider),
        )
        .route(
            "/api/v1/provider-installations/{installation_id}",
            axum::routing::delete(remove_provider_installation),
        )
        .route(
            "/api/v1/providers/{provider_id}/activation",
            put(activate_provider),
        )
}

#[utoipa::path(get, path = "/api/v1/provider-repositories", responses((status = 200, body = [ProviderRepositoryResponse]), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn list_repositories(
    State(state): State<HttpState>,
) -> Result<Json<Vec<ProviderRepositoryResponse>>, HttpError> {
    let repositories = state.application.list_provider_repositories().await?;
    Ok(Json(
        repositories
            .into_iter()
            .map(ProviderRepositoryResponse::from_snapshot)
            .collect::<Result<_, _>>()?,
    ))
}

#[utoipa::path(post, path = "/api/v1/provider-repositories", request_body = RegisterProviderRepositoryRequest, responses((status = 201, body = ProviderRepositoryResponse), (status = 400, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn register_repository(
    State(state): State<HttpState>,
    payload: Result<Json<RegisterProviderRepositoryRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ProviderRepositoryResponse>), HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let repository = state
        .application
        .register_provider_repository(RegisterProviderRepository {
            url: payload.url,
            registered_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ProviderRepositoryResponse::from_snapshot(repository)?),
    ))
}

#[utoipa::path(post, path = "/api/v1/provider-repositories/{repository_id}/refresh", params(("repository_id" = String, Path)), responses((status = 200, body = ProviderRepositoryResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "providers")]
pub(super) async fn refresh_repository(
    State(state): State<HttpState>,
    Path(path): Path<RepositoryPath>,
) -> Result<Json<ProviderRepositoryResponse>, HttpError> {
    let repository = state
        .application
        .refresh_provider_repository(RefreshProviderRepository {
            repository_id: path.repository_id,
            refreshed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(ProviderRepositoryResponse::from_snapshot(repository)?))
}

#[utoipa::path(get, path = "/api/v1/providers", responses((status = 200, body = [ProviderReleaseResponse]), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn list_providers(
    State(state): State<HttpState>,
) -> Result<Json<Vec<ProviderReleaseResponse>>, HttpError> {
    Ok(Json(
        state
            .application
            .list_provider_releases()
            .await?
            .into_iter()
            .map(ProviderReleaseResponse::from)
            .collect(),
    ))
}

#[utoipa::path(get, path = "/api/v1/provider-installations", responses((status = 200, body = [ProviderInstallationResponse]), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn list_installations(
    State(state): State<HttpState>,
) -> Result<Json<Vec<ProviderInstallationResponse>>, HttpError> {
    Ok(Json(
        state
            .application
            .list_provider_installations()
            .await?
            .into_iter()
            .map(ProviderInstallationResponse::from_snapshot)
            .collect::<Result<_, _>>()?,
    ))
}

#[utoipa::path(post, path = "/api/v1/provider-installations", request_body = InstallProviderRequest, responses((status = 201, body = ProviderInstallationResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "providers")]
pub(super) async fn install_provider(
    State(state): State<HttpState>,
    payload: Result<Json<InstallProviderRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ProviderInstallationResponse>), HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let installation = state
        .application
        .install_provider(InstallProvider {
            installation_id: generate_uuid()?,
            repository_id: payload.repository_id,
            provider_id: payload.provider_id,
            version: payload.version,
            target: payload.target,
            installed_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ProviderInstallationResponse::from_snapshot(installation)?),
    ))
}

#[utoipa::path(put, path = "/api/v1/providers/{provider_id}/activation", params(("provider_id" = String, Path)), request_body = ActivateProviderRequest, responses((status = 200, body = ProviderInstallationResponse), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 500, body = ErrorResponse), (status = 502, body = ErrorResponse)), tag = "providers")]
pub(super) async fn activate_provider(
    State(state): State<HttpState>,
    Path(path): Path<ProviderPath>,
    payload: Result<Json<ActivateProviderRequest>, JsonRejection>,
) -> Result<Json<ProviderInstallationResponse>, HttpError> {
    let Json(payload) =
        payload.map_err(|_| HttpError::invalid_request("request body is invalid"))?;
    let installation = state
        .application
        .activate_provider_installation(ActivateProviderInstallation {
            provider_id: path.provider_id,
            installation_id: payload.installation_id,
            generation: generate_uuid()?,
            activated_at: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(Json(ProviderInstallationResponse::from_snapshot(
        installation,
    )?))
}

#[utoipa::path(delete, path = "/api/v1/provider-installations/{installation_id}", params(("installation_id" = Uuid, Path)), responses((status = 204), (status = 400, body = ErrorResponse), (status = 404, body = ErrorResponse), (status = 409, body = ErrorResponse), (status = 500, body = ErrorResponse)), tag = "providers")]
pub(super) async fn remove_provider_installation(
    State(state): State<HttpState>,
    Path(path): Path<InstallationPath>,
) -> Result<StatusCode, HttpError> {
    let installation_id = parse_installation_id(&path.installation_id)?;
    state
        .application
        .remove_provider_installation(RemoveProviderInstallation { installation_id })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn parse_installation_id(value: &str) -> Result<Uuid, HttpError> {
    Uuid::parse_str(value).map_err(|_| {
        HttpError::invalid_field(
            "installationId",
            "invalid_uuid",
            "installation ID must be a UUID",
        )
    })
}

#[derive(Deserialize)]
pub(super) struct RepositoryPath {
    repository_id: String,
}

#[derive(Deserialize)]
pub(super) struct ProviderPath {
    provider_id: String,
}

#[derive(Deserialize)]
pub(super) struct InstallationPath {
    installation_id: String,
}

#[cfg(test)]
mod tests {
    use crate::application::error::ApplicationErrorCode;

    use super::*;

    #[test]
    fn malformed_installation_id_uses_structured_field_error() {
        let error = parse_installation_id("not-a-uuid")
            .expect_err("malformed installation ID is rejected")
            .0;

        assert_eq!(error.code, ApplicationErrorCode::InvalidField);
        assert_eq!(error.field_problems[0].path, "installationId");
        assert_eq!(error.field_problems[0].code, "invalid_uuid");
    }
}
