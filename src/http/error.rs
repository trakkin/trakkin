use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use utoipa::ToSchema;

use crate::application::error::{ApplicationError, ApplicationErrorCode, ApplicationFieldProblem};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub field_problems: Vec<FieldProblemResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct FieldProblemResponse {
    pub path: String,
    pub code: String,
    pub message: String,
}

pub struct HttpError(pub ApplicationError);

impl HttpError {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self(ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            message,
        ))
    }

    pub fn invalid_field(
        path: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self(ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "request contains invalid fields",
            path,
            code,
            message,
        ))
    }

    pub fn invalid_provider_data(message: impl Into<String>) -> Self {
        Self(ApplicationError::new(
            ApplicationErrorCode::InvalidProviderData,
            message,
        ))
    }

    pub fn not_found() -> Self {
        Self(ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            "route was not found",
        ))
    }

    pub fn method_not_allowed() -> Self {
        Self(ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            "method is not allowed for this route",
        ))
    }
}

impl From<ApplicationError> for HttpError {
    fn from(error: ApplicationError) -> Self {
        Self(error)
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = status(self.0.code);
        let response = ErrorResponse {
            code: self.0.code.as_str().to_owned(),
            message: self.0.message,
            field_problems: self
                .0
                .field_problems
                .into_iter()
                .map(FieldProblemResponse::from)
                .collect(),
        };
        (status, Json(response)).into_response()
    }
}

impl From<ApplicationFieldProblem> for FieldProblemResponse {
    fn from(problem: ApplicationFieldProblem) -> Self {
        Self {
            path: problem.path,
            code: problem.code,
            message: problem.message,
        }
    }
}

fn status(code: ApplicationErrorCode) -> StatusCode {
    match code {
        ApplicationErrorCode::InvalidRequest
        | ApplicationErrorCode::InvalidField
        | ApplicationErrorCode::CatalogCursorInvalid
        | ApplicationErrorCode::MappingCursorInvalid
        | ApplicationErrorCode::ActivityCursorInvalid => StatusCode::BAD_REQUEST,
        ApplicationErrorCode::WorkspaceNotFound
        | ApplicationErrorCode::WorkspaceOrSourceNotFound
        | ApplicationErrorCode::ConnectionNotFound
        | ApplicationErrorCode::AuthenticationNotFound
        | ApplicationErrorCode::BinaryAssetNotFound
        | ApplicationErrorCode::SourceNotAttached
        | ApplicationErrorCode::MappingNotFound
        | ApplicationErrorCode::MappingRevisionNotFound
        | ApplicationErrorCode::ActivityActionNotFound
        | ApplicationErrorCode::SyncPolicyNotFound
        | ApplicationErrorCode::SyncScheduleNotFound
        | ApplicationErrorCode::SyncPreviewNotFound
        | ApplicationErrorCode::StreamNotFound
        | ApplicationErrorCode::TaskNotFound => StatusCode::NOT_FOUND,
        ApplicationErrorCode::WorkspaceAlreadyExists
        | ApplicationErrorCode::ConnectionBusy
        | ApplicationErrorCode::ConnectionUnavailable
        | ApplicationErrorCode::AuthenticationNotActive
        | ApplicationErrorCode::AuthenticationSessionUnavailable
        | ApplicationErrorCode::CatalogPublicationExpired
        | ApplicationErrorCode::ProviderCapabilityUnavailable
        | ApplicationErrorCode::CatalogCursorExpired
        | ApplicationErrorCode::StreamUnavailable
        | ApplicationErrorCode::TaskResourceUnavailable
        | ApplicationErrorCode::TaskCancellationUnsupported
        | ApplicationErrorCode::SyncPolicyAlreadyExists
        | ApplicationErrorCode::SyncPolicyHeadMoved
        | ApplicationErrorCode::SyncPreviewExpired
        | ApplicationErrorCode::SyncPreviewStale
        | ApplicationErrorCode::SyncPreviewAlreadyAdmitted => StatusCode::CONFLICT,
        ApplicationErrorCode::MappingResponseTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        ApplicationErrorCode::InvalidProviderData => StatusCode::BAD_GATEWAY,
        ApplicationErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
