use std::{future::Future, pin::Pin};

pub type ApplicationFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ApplicationError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationErrorCode {
    InvalidRequest,
    InvalidField,
    WorkspaceNotFound,
    WorkspaceAlreadyExists,
    WorkspaceOrSourceNotFound,
    ConnectionNotFound,
    ConnectionBusy,
    ConnectionUnavailable,
    AuthenticationNotFound,
    AuthenticationNotActive,
    AuthenticationSessionUnavailable,
    BinaryAssetNotFound,
    CatalogPublicationExpired,
    ProviderCapabilityUnavailable,
    ProviderNotFound,
    ProviderAlreadyInstalled,
    SourceNotAttached,
    CatalogCursorInvalid,
    CatalogCursorExpired,
    StreamNotFound,
    StreamUnavailable,
    TaskNotFound,
    TaskResourceUnavailable,
    TaskCancellationUnsupported,
    ProviderInvalidInput,
    ProviderAuthenticationFailed,
    ProviderAuthorizationFailed,
    ProviderRateLimited,
    ProviderUnavailable,
    ProviderConflict,
    ProviderUnsupported,
    InvalidProviderData,
    ProviderInternal,
    MappingsChanged,
    InvalidMappingData,
    Internal,
}

impl ApplicationErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidField => "invalid_field",
            Self::WorkspaceNotFound => "workspace_not_found",
            Self::WorkspaceAlreadyExists => "workspace_already_exists",
            Self::WorkspaceOrSourceNotFound => "workspace_or_source_not_found",
            Self::ConnectionNotFound => "connection_not_found",
            Self::ConnectionBusy => "connection_busy",
            Self::ConnectionUnavailable => "connection_unavailable",
            Self::AuthenticationNotFound => "authentication_not_found",
            Self::AuthenticationNotActive => "authentication_not_active",
            Self::AuthenticationSessionUnavailable => "authentication_session_unavailable",
            Self::BinaryAssetNotFound => "binary_asset_not_found",
            Self::CatalogPublicationExpired => "catalog_publication_expired",
            Self::ProviderCapabilityUnavailable => "provider_capability_unavailable",
            Self::ProviderNotFound => "provider_not_found",
            Self::ProviderAlreadyInstalled => "provider_already_installed",
            Self::SourceNotAttached => "source_not_attached",
            Self::CatalogCursorInvalid => "catalog_cursor_invalid",
            Self::CatalogCursorExpired => "catalog_cursor_expired",
            Self::StreamNotFound => "stream_not_found",
            Self::StreamUnavailable => "stream_unavailable",
            Self::TaskNotFound => "task_not_found",
            Self::TaskResourceUnavailable => "task_resource_unavailable",
            Self::TaskCancellationUnsupported => "task_cancellation_unsupported",
            Self::ProviderInvalidInput => "provider_invalid_input",
            Self::ProviderAuthenticationFailed => "provider_authentication_failed",
            Self::ProviderAuthorizationFailed => "provider_authorization_failed",
            Self::ProviderRateLimited => "provider_rate_limited",
            Self::ProviderUnavailable => "provider_unavailable",
            Self::ProviderConflict => "provider_conflict",
            Self::ProviderUnsupported => "provider_unsupported",
            Self::InvalidProviderData => "invalid_provider_data",
            Self::ProviderInternal => "provider_internal_error",
            Self::MappingsChanged => "mappings_changed",
            Self::InvalidMappingData => "invalid_mapping_data",
            Self::Internal => "internal_error",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationFieldProblem {
    pub path: String,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationRetryAfter {
    pub seconds: i64,
    pub nanoseconds: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationRetryAdvice {
    pub retryable: bool,
    pub after: Option<ApplicationRetryAfter>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{message}")]
pub struct ApplicationError {
    pub code: ApplicationErrorCode,
    pub message: String,
    pub field_problems: Vec<ApplicationFieldProblem>,
    details: Box<ApplicationErrorDetails>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ApplicationErrorDetails {
    provider_code: Option<String>,
    retry: Option<ApplicationRetryAdvice>,
}

impl ApplicationError {
    pub fn new(code: ApplicationErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            field_problems: Vec::new(),
            details: Box::default(),
        }
    }

    pub fn field(
        code: ApplicationErrorCode,
        message: impl Into<String>,
        path: impl Into<String>,
        field_code: impl Into<String>,
        field_message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            field_problems: vec![ApplicationFieldProblem {
                path: path.into(),
                code: field_code.into(),
                message: field_message.into(),
            }],
            details: Box::default(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: ApplicationErrorCode::Internal,
            message: message.into(),
            field_problems: Vec::new(),
            details: Box::default(),
        }
    }

    pub fn provider(
        code: ApplicationErrorCode,
        message: impl Into<String>,
        provider_code: impl Into<String>,
        field_problems: Vec<ApplicationFieldProblem>,
        retry: ApplicationRetryAdvice,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            field_problems,
            details: Box::new(ApplicationErrorDetails {
                provider_code: Some(provider_code.into()),
                retry: Some(retry),
            }),
        }
    }

    pub fn provider_code(&self) -> Option<&str> {
        self.details.provider_code.as_deref()
    }

    pub fn retry(&self) -> Option<&ApplicationRetryAdvice> {
        self.details.retry.as_ref()
    }

    pub fn take_provider_code(&mut self) -> Option<String> {
        self.details.provider_code.take()
    }

    pub fn take_retry(&mut self) -> Option<ApplicationRetryAdvice> {
        self.details.retry.take()
    }
}

pub trait IntoApplicationError {
    fn into_application_error(self) -> ApplicationError;
}

impl IntoApplicationError for ApplicationError {
    fn into_application_error(self) -> ApplicationError {
        self
    }
}
