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

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpFailureContext {
    code: &'static str,
    message: String,
}

impl HttpFailureContext {
    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

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
        let provider_code = error.take_provider_code();
        let retry = error.take_retry();
        let failure_context = HttpFailureContext {
            code: code.as_str(),
            message: error.message.clone(),
        };
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
        };
        let mut response = (status, Json(response)).into_response();
        response.extensions_mut().insert(failure_context);
        response
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
        | ApplicationErrorCode::ProviderNotFound
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
    async fn internal_error_uses_safe_message() {
        let response =
            HttpError(ApplicationError::internal("request could not be completed")).into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.extensions().get::<HttpFailureContext>(),
            Some(&HttpFailureContext {
                code: "internal_error",
                message: "request could not be completed".to_owned(),
            })
        );
        let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let body_text = String::from_utf8(body.to_vec()).unwrap();
        let body: serde_json::Value = serde_json::from_str(&body_text).unwrap();

        assert_eq!(body["message"], "request could not be completed");
    }
}
