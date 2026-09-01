use std::sync::atomic::{AtomicU64, Ordering};

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
    ProviderRepositoryNotFound,
    ProviderNotFound,
    ProviderInstallationNotFound,
    ProviderInstallationActive,
    ProviderAlreadyInstalled,
    SourceNotAttached,
    CatalogCursorInvalid,
    CatalogCursorExpired,
    MappingNotFound,
    MappingRevisionNotFound,
    MappingCursorInvalid,
    MappingResponseTooLarge,
    ActivityCursorInvalid,
    ActivityActionNotFound,
    SyncPolicyNotFound,
    SyncPolicyAlreadyExists,
    SyncPolicyHeadMoved,
    SyncScheduleNotFound,
    SyncPreviewNotFound,
    SyncPreviewExpired,
    SyncPreviewStale,
    SyncPreviewAlreadyAdmitted,
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
            Self::ProviderRepositoryNotFound => "provider_repository_not_found",
            Self::ProviderNotFound => "provider_not_found",
            Self::ProviderInstallationNotFound => "provider_installation_not_found",
            Self::ProviderInstallationActive => "provider_installation_active",
            Self::ProviderAlreadyInstalled => "provider_already_installed",
            Self::SourceNotAttached => "source_not_attached",
            Self::CatalogCursorInvalid => "catalog_cursor_invalid",
            Self::CatalogCursorExpired => "catalog_cursor_expired",
            Self::MappingNotFound => "mapping_not_found",
            Self::MappingRevisionNotFound => "mapping_revision_not_found",
            Self::MappingCursorInvalid => "mapping_cursor_invalid",
            Self::MappingResponseTooLarge => "mapping_response_too_large",
            Self::ActivityCursorInvalid => "activity_cursor_invalid",
            Self::ActivityActionNotFound => "activity_action_not_found",
            Self::SyncPolicyNotFound => "sync_policy_not_found",
            Self::SyncPolicyAlreadyExists => "sync_policy_already_exists",
            Self::SyncPolicyHeadMoved => "sync_policy_head_moved",
            Self::SyncScheduleNotFound => "sync_schedule_not_found",
            Self::SyncPreviewNotFound => "sync_preview_not_found",
            Self::SyncPreviewExpired => "sync_preview_expired",
            Self::SyncPreviewStale => "sync_preview_stale",
            Self::SyncPreviewAlreadyAdmitted => "sync_preview_already_admitted",
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

#[derive(Clone, Debug, Eq, PartialEq)]
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
    diagnostic_id: Option<String>,
    internal_message: Option<String>,
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

    pub fn internal(message: impl Into<String>, cause: impl std::fmt::Display) -> Self {
        Self {
            code: ApplicationErrorCode::Internal,
            message: message.into(),
            field_problems: Vec::new(),
            details: Box::new(ApplicationErrorDetails {
                diagnostic_id: Some(generate_diagnostic_id()),
                internal_message: Some(cause.to_string()),
                ..ApplicationErrorDetails::default()
            }),
        }
    }

    pub fn provider(
        code: ApplicationErrorCode,
        message: impl Into<String>,
        provider_code: impl Into<String>,
        field_problems: Vec<ApplicationFieldProblem>,
        retry: ApplicationRetryAdvice,
        diagnostic_id: impl Into<String>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            field_problems,
            details: Box::new(ApplicationErrorDetails {
                provider_code: Some(provider_code.into()),
                retry: Some(retry),
                diagnostic_id: Some(diagnostic_id.into()),
                internal_message: None,
            }),
        }
    }

    pub fn provider_code(&self) -> Option<&str> {
        self.details.provider_code.as_deref()
    }

    pub fn retry(&self) -> Option<&ApplicationRetryAdvice> {
        self.details.retry.as_ref()
    }

    pub fn diagnostic_id(&self) -> Option<&str> {
        self.details.diagnostic_id.as_deref()
    }

    pub fn internal_message(&self) -> Option<&str> {
        self.details.internal_message.as_deref()
    }

    pub fn take_provider_code(&mut self) -> Option<String> {
        self.details.provider_code.take()
    }

    pub fn take_retry(&mut self) -> Option<ApplicationRetryAdvice> {
        self.details.retry.take()
    }

    pub fn take_diagnostic_id(&mut self) -> Option<String> {
        self.details.diagnostic_id.take()
    }
}

static DIAGNOSTIC_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn generate_diagnostic_id() -> String {
    let mut bytes = [0_u8; 16];
    if getrandom::fill(&mut bytes).is_ok() {
        return format!("host:{}", hex::encode(bytes));
    }
    format!(
        "host:fallback:{}",
        DIAGNOSTIC_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

pub trait IntoApplicationError {
    fn into_application_error(self) -> ApplicationError;
}

impl IntoApplicationError for ApplicationError {
    fn into_application_error(self) -> ApplicationError {
        self
    }
}
