use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use utoipa::ToSchema;

use crate::application::error::{ApplicationError, ApplicationErrorCode, ApplicationFieldProblem};
use crate::application::error::{ApplicationRetryAdvice, ApplicationRetryAfter};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub field_problems: Vec<FieldProblemResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryAdviceResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct FieldProblemResponse {
    pub path: String,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetryAdviceResponse {
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<RetryAfterResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct RetryAfterResponse {
    pub seconds: i64,
    pub nanoseconds: i32,
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
        let mut error = self.0;
        let code = error.code;
        let status = status(code);
        let diagnostic_id = error
            .take_diagnostic_id()
            .or_else(|| (code == ApplicationErrorCode::Internal).then(generate_diagnostic_id));
        let provider_code = error.take_provider_code();
        let retry = error.take_retry();
        if code == ApplicationErrorCode::Internal {
            tracing::error!(
                event = "http.error.internal",
                error.code = code.as_str(),
                error.cause = error.internal_message().unwrap_or("unavailable"),
                diagnostic.id = diagnostic_id.as_deref().unwrap_or("unavailable"),
            );
        } else if let Some(provider_code) = provider_code.as_deref() {
            tracing::warn!(
                event = "http.error.provider",
                error.code = code.as_str(),
                provider.error.code = provider_code,
                diagnostic.id = diagnostic_id.as_deref().unwrap_or("unavailable"),
            );
        }
        let response = ErrorResponse {
            code: code.as_str().to_owned(),
            message: error.message,
            field_problems: error
                .field_problems
                .into_iter()
                .map(FieldProblemResponse::from)
                .collect(),
            provider_code,
            retry: retry.map(RetryAdviceResponse::from),
            diagnostic_id,
        };
        (status, Json(response)).into_response()
    }
}

impl From<ApplicationRetryAdvice> for RetryAdviceResponse {
    fn from(advice: ApplicationRetryAdvice) -> Self {
        Self {
            retryable: advice.retryable,
            after: advice.after.map(RetryAfterResponse::from),
        }
    }
}

impl From<ApplicationRetryAfter> for RetryAfterResponse {
    fn from(after: ApplicationRetryAfter) -> Self {
        Self {
            seconds: after.seconds,
            nanoseconds: after.nanoseconds,
        }
    }
}

fn generate_diagnostic_id() -> String {
    let mut bytes = [0_u8; 16];
    if getrandom::fill(&mut bytes).is_err() {
        return "diagnostic-id-unavailable".to_owned();
    }
    format!("host:{}", hex::encode(bytes))
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
        | ApplicationErrorCode::ActivityCursorInvalid
        | ApplicationErrorCode::ProviderInvalidInput => StatusCode::BAD_REQUEST,
        ApplicationErrorCode::WorkspaceNotFound
        | ApplicationErrorCode::WorkspaceOrSourceNotFound
        | ApplicationErrorCode::ConnectionNotFound
        | ApplicationErrorCode::ProviderRepositoryNotFound
        | ApplicationErrorCode::ProviderNotFound
        | ApplicationErrorCode::ProviderInstallationNotFound
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
        | ApplicationErrorCode::ProviderInstallationActive
        | ApplicationErrorCode::ProviderAlreadyInstalled
        | ApplicationErrorCode::CatalogCursorExpired
        | ApplicationErrorCode::StreamUnavailable
        | ApplicationErrorCode::TaskResourceUnavailable
        | ApplicationErrorCode::TaskCancellationUnsupported
        | ApplicationErrorCode::SyncPolicyAlreadyExists
        | ApplicationErrorCode::SyncPolicyHeadMoved
        | ApplicationErrorCode::SyncPreviewExpired
        | ApplicationErrorCode::SyncPreviewStale
        | ApplicationErrorCode::SyncPreviewAlreadyAdmitted => StatusCode::CONFLICT,
        ApplicationErrorCode::ProviderConflict => StatusCode::CONFLICT,
        ApplicationErrorCode::ProviderAuthenticationFailed
        | ApplicationErrorCode::ProviderAuthorizationFailed
        | ApplicationErrorCode::ProviderUnsupported => StatusCode::UNPROCESSABLE_ENTITY,
        ApplicationErrorCode::ProviderRateLimited => StatusCode::TOO_MANY_REQUESTS,
        ApplicationErrorCode::ProviderUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        ApplicationErrorCode::MappingResponseTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        ApplicationErrorCode::InvalidProviderData | ApplicationErrorCode::ProviderInternal => {
            StatusCode::BAD_GATEWAY
        }
        ApplicationErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    #[tokio::test]
    async fn internal_cause_is_not_serialized() {
        let response = HttpError(ApplicationError::internal(
            "request could not be completed",
            "database password=not-for-clients",
        ))
        .into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let body_text = String::from_utf8(body.to_vec()).unwrap();
        let body: serde_json::Value = serde_json::from_str(&body_text).unwrap();

        assert_eq!(body["message"], "request could not be completed");
        assert!(body["diagnosticId"].as_str().is_some());
        assert!(body.get("internalMessage").is_none());
        assert!(!body_text.contains("password"));
        assert!(!body_text.contains("not-for-clients"));
    }
}
