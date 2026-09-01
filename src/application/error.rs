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
    InvalidProviderData,
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
            Self::InvalidProviderData => "invalid_provider_data",
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
pub struct ApplicationError {
    pub code: ApplicationErrorCode,
    pub message: String,
    pub field_problems: Vec<ApplicationFieldProblem>,
}

impl ApplicationError {
    pub fn new(code: ApplicationErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            field_problems: Vec::new(),
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
        }
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
