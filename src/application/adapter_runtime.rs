use time::OffsetDateTime;
use trakkin_mapping::PortableEndpoint;
use trakkin_provider_sdk::v1::{
    AuthenticationMethod, AuthenticationProgress, DescribeConnectionResult, DiscoverSourcesResult,
    HealthResponse, OpenConnectionResult, OperationFailure, OperationFailureCategory,
    RetryDisposition, ValidateConnectionResult,
};

use super::acquisition::ReadMode;
use super::error::{
    ApplicationError, ApplicationErrorCode, ApplicationFieldProblem, ApplicationRetryAdvice,
    ApplicationRetryAfter, IntoApplicationError,
};
use crate::domain::acquisition::{
    CatalogBatch, CoordinateBinding, PortableReference, ProviderItem, StateBatch, StateField,
    StateSubject, StateValue,
};

pub type AdapterOperationResult<T> = Result<T, AdapterCallError>;
pub type AdapterAuthenticationMethod = AuthenticationMethod;
pub type AdapterAuthenticationStep = AuthenticationProgress;
pub type AdapterConnectionDescription = DescribeConnectionResult;
pub type AdapterConnectionValidation = ValidateConnectionResult;
pub type DiscoveredAdapterSources = DiscoverSourcesResult;
pub type OpenedAdapterConnection = OpenConnectionResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchAdapter {
    pub provider_id: String,
    pub process_instance_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterIdentity {
    pub adapter_key: String,
    pub adapter_version: String,
    pub process_instance_id: String,
    pub protocol_major: u32,
    pub protocol_minor: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterFieldProblem {
    pub path: String,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterRetryAfter {
    pub seconds: i64,
    pub nanoseconds: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterFailureCategory {
    InvalidInput,
    Authentication,
    Authorization,
    RateLimited,
    Unavailable,
    Conflict,
    Unsupported,
    InvalidRemoteData,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterCallError {
    pub category: AdapterFailureCategory,
    pub code: String,
    pub safe_message: String,
    pub retryable: bool,
    pub retry_after: Option<AdapterRetryAfter>,
    pub field_problems: Vec<AdapterFieldProblem>,
}

impl IntoApplicationError for AdapterCallError {
    fn into_application_error(self) -> ApplicationError {
        let code = match self.category {
            AdapterFailureCategory::InvalidInput => ApplicationErrorCode::ProviderInvalidInput,
            AdapterFailureCategory::Authentication => {
                ApplicationErrorCode::ProviderAuthenticationFailed
            }
            AdapterFailureCategory::Authorization => {
                ApplicationErrorCode::ProviderAuthorizationFailed
            }
            AdapterFailureCategory::RateLimited => ApplicationErrorCode::ProviderRateLimited,
            AdapterFailureCategory::Unavailable => ApplicationErrorCode::ProviderUnavailable,
            AdapterFailureCategory::Conflict => ApplicationErrorCode::ProviderConflict,
            AdapterFailureCategory::Unsupported => ApplicationErrorCode::ProviderUnsupported,
            AdapterFailureCategory::InvalidRemoteData => ApplicationErrorCode::InvalidProviderData,
            AdapterFailureCategory::Internal => ApplicationErrorCode::ProviderInternal,
        };
        let retry_after = self.retry_after.map(|retry_after| ApplicationRetryAfter {
            seconds: retry_after.seconds,
            nanoseconds: retry_after.nanoseconds,
        });
        ApplicationError::provider(
            code,
            self.safe_message,
            self.code,
            self.field_problems
                .into_iter()
                .map(|problem| ApplicationFieldProblem {
                    path: problem.path,
                    code: problem.code,
                    message: problem.message,
                })
                .collect(),
            ApplicationRetryAdvice {
                retryable: self.retryable,
                after: retry_after,
            },
        )
    }
}

impl From<OperationFailure> for AdapterCallError {
    fn from(error: OperationFailure) -> Self {
        let category = match OperationFailureCategory::try_from(error.category)
            .expect("validated operation failure category")
        {
            OperationFailureCategory::InvalidInput => AdapterFailureCategory::InvalidInput,
            OperationFailureCategory::Authentication => AdapterFailureCategory::Authentication,
            OperationFailureCategory::Authorization => AdapterFailureCategory::Authorization,
            OperationFailureCategory::RateLimited => AdapterFailureCategory::RateLimited,
            OperationFailureCategory::Unavailable => AdapterFailureCategory::Unavailable,
            OperationFailureCategory::Conflict => AdapterFailureCategory::Conflict,
            OperationFailureCategory::Unsupported => AdapterFailureCategory::Unsupported,
            OperationFailureCategory::InvalidRemoteData => {
                AdapterFailureCategory::InvalidRemoteData
            }
            OperationFailureCategory::Internal => AdapterFailureCategory::Internal,
            OperationFailureCategory::Unspecified => {
                unreachable!("validated operation failure category")
            }
        };
        let retry = error
            .retry
            .expect("validated operation failure retry advice");
        let disposition =
            RetryDisposition::try_from(retry.disposition).expect("validated retry disposition");
        Self {
            category,
            code: error.code,
            safe_message: error.safe_message,
            retryable: matches!(
                disposition,
                RetryDisposition::Retryable | RetryDisposition::RetryAfter
            ),
            retry_after: retry.after.map(|retry_after| AdapterRetryAfter {
                seconds: retry_after.seconds,
                nanoseconds: retry_after.nanos,
            }),
            field_problems: error
                .field_problems
                .into_iter()
                .map(|problem| AdapterFieldProblem {
                    path: problem.path,
                    code: problem.code,
                    message: problem.message,
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterValue {
    Boolean(bool),
    Integer(i64),
    Decimal(String),
    Text(String),
    Bytes(Vec<u8>),
    TimestampMilliseconds(i64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterSetting {
    pub key: String,
    pub value: AdapterValue,
}

#[derive(Clone, Eq, PartialEq)]
pub struct AdapterSecret {
    pub key: String,
    pub value: Vec<u8>,
}

impl std::fmt::Debug for AdapterSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdapterSecret")
            .field("key", &self.key)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAdapterConnection {
    pub settings: Vec<AdapterSetting>,
    pub secrets: Vec<AdapterSecret>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidateAdapterConnection {
    pub settings: Vec<AdapterSetting>,
    pub secrets: Vec<AdapterSecret>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartAdapterAuthentication {
    pub method: String,
    pub values: Vec<AdapterSetting>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinueAdapterAuthentication {
    pub authentication_id: String,
    pub values: Vec<AdapterSetting>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterKey {
    pub namespace: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTerm {
    pub namespace: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterSecretPatch {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterValueKind {
    Text,
    Integer,
    Decimal,
    Boolean,
    Timestamp,
    Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTargetedStateReadCapability {
    pub maximum_fields: u32,
    pub maximum_response_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateWritePreconditionMode {
    ProviderToken,
    HostRecheckOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateWriteIdempotencyMode {
    NotSupported,
    StableKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTargetedStateFieldWriteCapability {
    pub field: StateField,
    pub set_supported: bool,
    pub clear_supported: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTargetedStateWriteCapability {
    pub fields: Vec<AdapterTargetedStateFieldWriteCapability>,
    pub may_create_source_membership: bool,
    pub precondition_mode: Option<AdapterTargetedStateWritePreconditionMode>,
    pub idempotency_mode: Option<AdapterTargetedStateWriteIdempotencyMode>,
    pub maximum_fields: u32,
    pub maximum_request_bytes: u64,
    pub maximum_response_bytes: u64,
    pub maximum_receipt_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterSourceAvailability {
    Available,
    Unavailable,
    Removed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAdapterSource {
    pub operation_id: Vec<u8>,
    pub source_key: AdapterKey,
    pub mode: ReadMode,
    pub prior_cursor: Option<Vec<u8>>,
    pub preferred_batch_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterReadCompletion {
    pub next_cursor: Vec<u8>,
    pub evidence_revision: Vec<u8>,
    pub observed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupAdapterPortableReferences {
    pub operation_id: Vec<u8>,
    pub source_key: AdapterKey,
    pub references: Vec<PortableReference>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdapterLookupEvidence {
    pub adapter_revision: Vec<u8>,
    pub observed_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
    pub matched_references: Vec<PortableReference>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdapterLookupCandidate {
    pub provider_item: ProviderItem,
    pub evidence: AdapterLookupEvidence,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AdapterLookupOutcome {
    Matched(Box<AdapterLookupCandidate>),
    NotFound,
    Ambiguous(Vec<AdapterLookupCandidate>),
    Unsupported,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdapterPortableReferenceLookup {
    pub requested: PortableReference,
    pub outcome: AdapterLookupOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveAdapterPortableEndpoints {
    pub operation_id: Vec<u8>,
    pub source_key: AdapterKey,
    pub endpoints: Vec<PortableEndpoint>,
    pub maximum_response_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAdapterTargetedState {
    pub operation_id: Vec<u8>,
    pub source_key: AdapterKey,
    pub subject: StateSubject,
    pub fields: Vec<StateField>,
    pub maximum_response_bytes: u64,
    pub capability: AdapterTargetedStateReadCapability,
    pub reconciliation_idempotency_key: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterSourceMembership {
    Present,
    Absent,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterStatePresence {
    Present,
    Deleted,
    Absent,
    Unknown,
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTargetedStateField {
    pub field: StateField,
    pub presence: AdapterStatePresence,
    pub value: Option<StateValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTargetedStateWriteCausation {
    pub idempotency_key: Vec<u8>,
    pub receipt: Vec<u8>,
    pub provider_revision: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTargetedStateMatch {
    pub membership: AdapterSourceMembership,
    pub fields: Vec<AdapterTargetedStateField>,
    pub provider_revision: Vec<u8>,
    pub observed_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
    pub precondition: Option<Vec<u8>>,
    pub write_causation: Option<AdapterTargetedStateWriteCausation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateReadOutcome {
    Matched(AdapterTargetedStateMatch),
    Unsupported,
    NotFound,
    Ambiguous(Vec<StateSubject>),
    Indeterminate(AdapterCallError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateWriteOperation {
    Set(StateValue),
    Clear,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTargetedStateWriteIntent {
    pub field: StateField,
    pub operation: AdapterTargetedStateWriteOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteAdapterTargetedState {
    pub operation_id: Vec<u8>,
    pub source_key: AdapterKey,
    pub subject: StateSubject,
    pub idempotency_key: Vec<u8>,
    pub expected_membership: AdapterSourceMembership,
    pub precondition: Option<Vec<u8>>,
    pub allow_create_membership: bool,
    pub intents: Vec<AdapterTargetedStateWriteIntent>,
    pub maximum_receipt_bytes: u64,
    pub maximum_response_bytes: u64,
    pub capability: AdapterTargetedStateWriteCapability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateWriteStatus {
    Applied,
    NoOp,
    Rejected,
    Failed,
    Indeterminate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateWriteCertainty {
    ConfirmedApplied,
    ConfirmedNotApplied,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateWriteRetryDisposition {
    NotRetryable,
    SafeSameKey,
    ReconcileFirst,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateMembershipEffect {
    Unchanged,
    Created,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterTargetedStateFieldEffectKind {
    Set,
    Cleared,
    Unchanged,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterTargetedStateFieldEffect {
    pub field: StateField,
    pub effect: AdapterTargetedStateFieldEffectKind,
    pub value: Option<StateValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdapterTargetedStateWriteOutcome {
    pub status: AdapterTargetedStateWriteStatus,
    pub certainty: AdapterTargetedStateWriteCertainty,
    pub retry_disposition: AdapterTargetedStateWriteRetryDisposition,
    pub membership_effect: AdapterTargetedStateMembershipEffect,
    pub field_effects: Vec<AdapterTargetedStateFieldEffect>,
    pub provider_revision: Vec<u8>,
    pub successor_precondition: Option<Vec<u8>>,
    pub receipt: Vec<u8>,
    pub error: Option<AdapterCallError>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdapterEndpointLookupCandidate {
    pub provider_item: ProviderItem,
    pub binding: Option<CoordinateBinding>,
    pub evidence: AdapterLookupEvidence,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AdapterEndpointLookupOutcome {
    Matched(Box<AdapterEndpointLookupCandidate>),
    NotFound,
    Ambiguous(Vec<AdapterEndpointLookupCandidate>),
    Unsupported,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdapterPortableEndpointResolution {
    pub requested: PortableEndpoint,
    pub outcome: AdapterEndpointLookupOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAdapterAsset {
    pub operation_id: Vec<u8>,
    pub source_key: AdapterKey,
    pub provider_item_key: AdapterKey,
    pub asset_key: AdapterKey,
    pub maximum_bytes: u64,
    pub allowed_content_types: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterBinaryAsset {
    pub content: Vec<u8>,
    pub content_type: String,
    pub full_length: u64,
    pub sha256_digest: Vec<u8>,
    pub cache_control: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AdapterReadEvent<Batch> {
    Heartbeat { records_emitted: u64 },
    Batch(Batch),
    Completed(AdapterReadCompletion),
    Failed(AdapterCallError),
    Cancelled,
}

#[trait_variant::make(Send)]
pub trait AdapterReadStream<Batch> {
    type Error;

    async fn next_event(&mut self) -> Result<Option<AdapterReadEvent<Batch>>, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterRetirement {
    Graceful,
    Forced,
}

#[trait_variant::make(Send)]
pub trait AdapterLauncher: Sync {
    type Session: AdapterSession<Error = Self::Error>;
    type Error;

    async fn launch(&self, command: LaunchAdapter) -> Result<Self::Session, Self::Error>;
}

#[trait_variant::make(Send)]
pub trait AdapterSession {
    type Error;
    type CatalogReadStream: AdapterReadStream<CatalogBatch, Error = Self::Error>;
    type StateReadStream: AdapterReadStream<StateBatch, Error = Self::Error>;

    fn identity(&self) -> &AdapterIdentity;

    async fn health(&mut self) -> Result<HealthResponse, Self::Error>;

    async fn describe_connection(
        &mut self,
    ) -> Result<AdapterOperationResult<AdapterConnectionDescription>, Self::Error>;

    async fn validate_connection(
        &mut self,
        command: ValidateAdapterConnection,
    ) -> Result<AdapterOperationResult<AdapterConnectionValidation>, Self::Error>;

    async fn list_authentication_methods(
        &mut self,
    ) -> Result<AdapterOperationResult<Vec<AdapterAuthenticationMethod>>, Self::Error>;

    async fn start_authentication(
        &mut self,
        command: StartAdapterAuthentication,
    ) -> Result<AdapterOperationResult<AdapterAuthenticationStep>, Self::Error>;

    async fn continue_authentication(
        &mut self,
        command: ContinueAdapterAuthentication,
    ) -> Result<AdapterOperationResult<AdapterAuthenticationStep>, Self::Error>;

    async fn cancel_authentication(
        &mut self,
        authentication_id: String,
    ) -> Result<AdapterOperationResult<()>, Self::Error>;

    async fn open_connection(
        &mut self,
        command: OpenAdapterConnection,
    ) -> Result<AdapterOperationResult<OpenedAdapterConnection>, Self::Error>;

    async fn discover_sources(
        &mut self,
    ) -> Result<AdapterOperationResult<DiscoveredAdapterSources>, Self::Error>;

    async fn read_catalog(
        &mut self,
        command: ReadAdapterSource,
    ) -> Result<Self::CatalogReadStream, Self::Error>;

    async fn read_state(
        &mut self,
        command: ReadAdapterSource,
    ) -> Result<Self::StateReadStream, Self::Error>;

    async fn read_targeted_state(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> Result<AdapterTargetedStateReadOutcome, Self::Error>;

    async fn write_targeted_state(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> Result<AdapterTargetedStateWriteOutcome, Self::Error>;

    async fn lookup_portable_references(
        &mut self,
        command: LookupAdapterPortableReferences,
    ) -> Result<Result<Vec<AdapterPortableReferenceLookup>, AdapterCallError>, Self::Error>;

    async fn resolve_portable_endpoints(
        &mut self,
        command: ResolveAdapterPortableEndpoints,
    ) -> Result<Result<Vec<AdapterPortableEndpointResolution>, AdapterCallError>, Self::Error>;

    async fn read_asset(
        &mut self,
        command: ReadAdapterAsset,
    ) -> Result<Result<AdapterBinaryAsset, AdapterCallError>, Self::Error>;

    async fn cancel_operation(&mut self, operation_id: Vec<u8>) -> Result<(), Self::Error>;

    async fn retire(self) -> Result<AdapterRetirement, Self::Error>
    where
        Self: Sized;
}
