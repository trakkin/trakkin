use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    application::{
        adapter_runtime::{
            AdapterAuthenticationMethod, AdapterAuthenticationPrompt, AdapterAuthenticationStatus,
            AdapterAuthenticationStep, AdapterCallError, AdapterConfigurationField,
            AdapterConfigurationOption, AdapterFieldProblem, AdapterRetryAfter,
            AdapterSourceAvailability, AdapterValue, AdapterValueKind,
        },
        connection::{
            ConnectionDeletionImpact, ConnectionInspection, ConnectionSourceSummary,
            ConnectionStatus, ConnectionSummary, StartedConnectionAuthentication,
        },
        error::ApplicationError,
    },
    http::error::FieldProblemResponse,
};

use super::{
    sources::{KeyResponse, TermResponse, key_from_adapter, term_from_adapter},
    timestamp,
};

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateConnectionRequest {
    pub provider_id: String,
    pub display_name: String,
    pub settings: Vec<AdapterSettingRequest>,
    pub secrets: Vec<AdapterSecretRequest>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct AdapterSettingRequest {
    pub key: String,
    pub value: AdapterValueRequest,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct AdapterSecretRequest {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum AdapterValueRequest {
    Boolean(bool),
    Integer(i64),
    Decimal(String),
    Text(String),
    Bytes(Vec<u8>),
    TimestampMilliseconds(i64),
}

impl From<AdapterValueRequest> for AdapterValue {
    fn from(value: AdapterValueRequest) -> Self {
        match value {
            AdapterValueRequest::Boolean(value) => Self::Boolean(value),
            AdapterValueRequest::Integer(value) => Self::Integer(value),
            AdapterValueRequest::Decimal(value) => Self::Decimal(value),
            AdapterValueRequest::Text(value) => Self::Text(value),
            AdapterValueRequest::Bytes(value) => Self::Bytes(value),
            AdapterValueRequest::TimestampMilliseconds(value) => Self::TimestampMilliseconds(value),
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RenameConnectionRequest {
    pub display_name: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct DeleteConnectionRequest {
    pub confirmed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionDeletionImpactResponse {
    pub connection_id: String,
    pub source_count: i64,
    pub workspace_attachment_count: i64,
    pub task_count: i64,
    pub active_task_count: i64,
    pub authentication_session_count: i64,
}

impl From<ConnectionDeletionImpact> for ConnectionDeletionImpactResponse {
    fn from(impact: ConnectionDeletionImpact) -> Self {
        Self {
            connection_id: impact.connection_id.to_string(),
            source_count: impact.source_count,
            workspace_attachment_count: impact.workspace_attachment_count,
            task_count: impact.task_count,
            active_task_count: impact.active_task_count,
            authentication_session_count: impact.authentication_session_count,
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct StartAuthenticationRequest {
    pub method: String,
    pub values: Vec<AdapterSettingRequest>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct ContinueAuthenticationRequest {
    pub values: Vec<AdapterSettingRequest>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInspectionResponse {
    pub fields: Vec<AdapterConfigurationFieldResponse>,
    pub field_problems: Vec<FieldProblemResponse>,
    pub authentication_methods: Vec<AuthenticationMethodResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AdapterConfigurationFieldResponse {
    pub key: String,
    pub display_name: String,
    pub value_kind: AdapterValueKindResponse,
    pub required: bool,
    pub secret: bool,
    pub options: Vec<AdapterConfigurationOptionResponse>,
    pub default_value: Option<AdapterValueResponse>,
    pub placeholder: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum AdapterValueKindResponse {
    Text,
    Integer,
    Decimal,
    Boolean,
    Timestamp,
    Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AdapterConfigurationOptionResponse {
    pub value: AdapterValueResponse,
    pub display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum AdapterValueResponse {
    Boolean(bool),
    Integer(i64),
    Decimal(String),
    Text(String),
    Bytes(Vec<u8>),
    TimestampMilliseconds(i64),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationMethodResponse {
    pub key: String,
    pub display_name: String,
    pub interactions: Vec<TermResponse>,
    pub fields: Vec<AdapterConfigurationFieldResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationStepResponse {
    pub id: String,
    pub status: AuthenticationStatusResponse,
    pub prompt: Option<AuthenticationPromptResponse>,
    pub accounts: Vec<AuthenticationAccountResponse>,
    pub retry_after: Option<RetryAfterResponse>,
    pub error: Option<AdapterCallErrorResponse>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum AuthenticationStatusResponse {
    Waiting,
    InputRequired,
    Completed,
    Cancelled,
    Expired,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationPromptResponse {
    pub message: String,
    pub verification_url: String,
    pub user_code: String,
    pub fields: Vec<AdapterConfigurationFieldResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationAccountResponse {
    pub key: KeyResponse,
    pub display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct RetryAfterResponse {
    pub seconds: i64,
    pub nanoseconds: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AdapterCallErrorResponse {
    pub code: String,
    pub safe_message: String,
    pub retryable: bool,
    pub retry_after: Option<RetryAfterResponse>,
    pub field_problems: Vec<FieldProblemResponse>,
}

impl ConnectionInspectionResponse {
    pub fn from_inspection(inspection: ConnectionInspection) -> Self {
        Self {
            fields: inspection
                .description
                .fields
                .into_iter()
                .map(AdapterConfigurationFieldResponse::from)
                .collect(),
            field_problems: inspection
                .validation
                .field_problems
                .into_iter()
                .map(FieldProblemResponse::from)
                .collect(),
            authentication_methods: inspection
                .authentication_methods
                .into_iter()
                .map(AuthenticationMethodResponse::from)
                .collect(),
        }
    }
}

impl AuthenticationStepResponse {
    pub fn from_started(started: StartedConnectionAuthentication) -> Self {
        Self::from_step(started.authentication_id, started.step)
    }

    pub fn from_step(authentication_id: uuid::Uuid, step: AdapterAuthenticationStep) -> Self {
        Self {
            id: authentication_id.to_string(),
            status: AuthenticationStatusResponse::from(step.status),
            prompt: step.prompt.map(AuthenticationPromptResponse::from),
            accounts: step
                .accounts
                .into_iter()
                .map(|account| AuthenticationAccountResponse {
                    key: key_from_adapter(account.key),
                    display_name: account.display_name,
                })
                .collect(),
            retry_after: step.retry_after.map(RetryAfterResponse::from),
            error: step.error.map(AdapterCallErrorResponse::from),
        }
    }

    pub fn cancelled(authentication_id: uuid::Uuid) -> Self {
        Self {
            id: authentication_id.to_string(),
            status: AuthenticationStatusResponse::Cancelled,
            prompt: None,
            accounts: Vec::new(),
            retry_after: None,
            error: None,
        }
    }
}

impl From<AdapterConfigurationField> for AdapterConfigurationFieldResponse {
    fn from(field: AdapterConfigurationField) -> Self {
        let (options, default_value) = if field.secret {
            (Vec::new(), None)
        } else {
            (
                field
                    .options
                    .into_iter()
                    .map(AdapterConfigurationOptionResponse::from)
                    .collect(),
                field.default_value.map(AdapterValueResponse::from),
            )
        };
        Self {
            key: field.key,
            display_name: field.display_name,
            value_kind: AdapterValueKindResponse::from(field.value_kind),
            required: field.required,
            secret: field.secret,
            options,
            default_value,
            placeholder: field.placeholder,
        }
    }
}

impl From<AdapterConfigurationOption> for AdapterConfigurationOptionResponse {
    fn from(option: AdapterConfigurationOption) -> Self {
        Self {
            value: AdapterValueResponse::from(option.value),
            display_name: option.display_name,
        }
    }
}

impl From<AdapterValue> for AdapterValueResponse {
    fn from(value: AdapterValue) -> Self {
        match value {
            AdapterValue::Boolean(value) => Self::Boolean(value),
            AdapterValue::Integer(value) => Self::Integer(value),
            AdapterValue::Decimal(value) => Self::Decimal(value),
            AdapterValue::Text(value) => Self::Text(value),
            AdapterValue::Bytes(value) => Self::Bytes(value),
            AdapterValue::TimestampMilliseconds(value) => Self::TimestampMilliseconds(value),
        }
    }
}

impl From<AdapterAuthenticationMethod> for AuthenticationMethodResponse {
    fn from(method: AdapterAuthenticationMethod) -> Self {
        Self {
            key: method.key,
            display_name: method.display_name,
            interactions: method
                .interactions
                .into_iter()
                .map(term_from_adapter)
                .collect(),
            fields: method
                .fields
                .into_iter()
                .map(AdapterConfigurationFieldResponse::from)
                .collect(),
        }
    }
}

impl From<AdapterAuthenticationPrompt> for AuthenticationPromptResponse {
    fn from(prompt: AdapterAuthenticationPrompt) -> Self {
        Self {
            message: prompt.message,
            verification_url: prompt.verification_url,
            user_code: prompt.user_code,
            fields: prompt
                .fields
                .into_iter()
                .map(AdapterConfigurationFieldResponse::from)
                .collect(),
        }
    }
}

impl From<AdapterRetryAfter> for RetryAfterResponse {
    fn from(retry_after: AdapterRetryAfter) -> Self {
        Self {
            seconds: retry_after.seconds,
            nanoseconds: retry_after.nanoseconds,
        }
    }
}

impl From<AdapterCallError> for AdapterCallErrorResponse {
    fn from(error: AdapterCallError) -> Self {
        Self {
            code: error.code,
            safe_message: error.safe_message,
            retryable: error.retryable,
            retry_after: error.retry_after.map(RetryAfterResponse::from),
            field_problems: error
                .field_problems
                .into_iter()
                .map(FieldProblemResponse::from)
                .collect(),
        }
    }
}

impl From<AdapterFieldProblem> for FieldProblemResponse {
    fn from(problem: AdapterFieldProblem) -> Self {
        Self {
            path: problem.path,
            code: problem.code,
            message: problem.message,
        }
    }
}

impl From<AdapterAuthenticationStatus> for AuthenticationStatusResponse {
    fn from(status: AdapterAuthenticationStatus) -> Self {
        match status {
            AdapterAuthenticationStatus::Waiting => Self::Waiting,
            AdapterAuthenticationStatus::InputRequired => Self::InputRequired,
            AdapterAuthenticationStatus::Completed => Self::Completed,
            AdapterAuthenticationStatus::Cancelled => Self::Cancelled,
            AdapterAuthenticationStatus::Expired => Self::Expired,
            AdapterAuthenticationStatus::Failed => Self::Failed,
        }
    }
}

impl From<AdapterValueKind> for AdapterValueKindResponse {
    fn from(kind: AdapterValueKind) -> Self {
        match kind {
            AdapterValueKind::Text => Self::Text,
            AdapterValueKind::Integer => Self::Integer,
            AdapterValueKind::Decimal => Self::Decimal,
            AdapterValueKind::Boolean => Self::Boolean,
            AdapterValueKind::Timestamp => Self::Timestamp,
            AdapterValueKind::Bytes => Self::Bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionResponse {
    pub id: String,
    pub provider_id: String,
    pub display_name: String,
    pub status: ConnectionStatusResponse,
    pub capability_revision: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionStatusResponse {
    Pending,
    Ready,
    Unavailable,
    Disabled,
}

impl ConnectionResponse {
    pub fn from_summary(summary: ConnectionSummary) -> Result<Self, ApplicationError> {
        Ok(Self {
            id: summary.connection_id.to_string(),
            provider_id: summary.provider_id,
            display_name: summary.display_name,
            status: ConnectionStatusResponse::from(summary.status),
            capability_revision: summary.capability_revision,
            created_at: timestamp(summary.created_at)?,
            updated_at: timestamp(summary.updated_at)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSourceResponse {
    pub id: String,
    pub account_id: String,
    pub account_display_name: String,
    pub display_name: String,
    pub kind: TermResponse,
    pub availability: SourceAvailabilityResponse,
    pub capability_revision: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SourceAvailabilityResponse {
    Available,
    Unavailable,
    Removed,
}

impl From<ConnectionSourceSummary> for ConnectionSourceResponse {
    fn from(summary: ConnectionSourceSummary) -> Self {
        Self {
            id: summary.source_id.to_string(),
            account_id: summary.account_id.to_string(),
            account_display_name: summary.account_display_name,
            display_name: summary.display_name,
            kind: TermResponse {
                namespace: summary.kind.namespace,
                name: summary.kind.name,
            },
            availability: SourceAvailabilityResponse::from(summary.availability),
            capability_revision: summary.capability_revision,
        }
    }
}

impl From<ConnectionStatus> for ConnectionStatusResponse {
    fn from(status: ConnectionStatus) -> Self {
        match status {
            ConnectionStatus::Pending => Self::Pending,
            ConnectionStatus::Ready => Self::Ready,
            ConnectionStatus::Unavailable => Self::Unavailable,
            ConnectionStatus::Disabled => Self::Disabled,
        }
    }
}

impl From<AdapterSourceAvailability> for SourceAvailabilityResponse {
    fn from(availability: AdapterSourceAvailability) -> Self {
        match availability {
            AdapterSourceAvailability::Available => Self::Available,
            AdapterSourceAvailability::Unavailable => Self::Unavailable,
            AdapterSourceAvailability::Removed => Self::Removed,
        }
    }
}
