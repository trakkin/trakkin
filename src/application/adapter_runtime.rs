use std::{future::Future, pin::Pin};

use time::OffsetDateTime;
use trakkin_mapping::PortableEndpoint;

use crate::domain::acquisition::{
    CatalogBatch, CoordinateBinding, PortableReference, ProviderItem, StateBatch, StateField,
    StateSubject, StateValue,
};
use crate::domain::synchronization::Quantizer;

use super::acquisition::ReadMode;
use super::error::{
    ApplicationError, ApplicationErrorCode, ApplicationFieldProblem, ApplicationRetryAdvice,
    ApplicationRetryAfter, IntoApplicationError,
};

pub type AdapterFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;
pub type AdapterRetirementFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;
pub type AdapterOperationResult<T> = Result<T, AdapterCallError>;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterHealthStatus {
    Ready,
    Degraded,
    NotReady,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterHealth {
    pub status: AdapterHealthStatus,
    pub error: Option<AdapterCallError>,
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
    pub diagnostic_id: String,
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
            self.diagnostic_id,
        )
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterConfigurationOption {
    pub value: AdapterValue,
    pub display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterConfigurationField {
    pub key: String,
    pub display_name: String,
    pub value_kind: AdapterValueKind,
    pub required: bool,
    pub secret: bool,
    pub options: Vec<AdapterConfigurationOption>,
    pub default_value: Option<AdapterValue>,
    pub placeholder: String,
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
pub struct AdapterConnectionDescription {
    pub fields: Vec<AdapterConfigurationField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidateAdapterConnection {
    pub settings: Vec<AdapterSetting>,
    pub secrets: Vec<AdapterSecret>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterConnectionValidation {
    pub field_problems: Vec<AdapterFieldProblem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterAuthenticationMethod {
    pub key: String,
    pub display_name: String,
    pub interactions: Vec<AdapterTerm>,
    pub fields: Vec<AdapterConfigurationField>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterAuthenticationStatus {
    Waiting,
    InputRequired,
    Completed,
    Cancelled,
    Expired,
    Failed,
}

impl AdapterAuthenticationStatus {
    pub fn active(self) -> bool {
        matches!(self, Self::Waiting | Self::InputRequired)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterAuthenticationPrompt {
    pub message: String,
    pub verification_url: String,
    pub user_code: String,
    pub fields: Vec<AdapterConfigurationField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterAuthenticationStep {
    pub authentication_id: String,
    pub status: AdapterAuthenticationStatus,
    pub prompt: Option<AdapterAuthenticationPrompt>,
    pub accounts: Vec<AdapterAccount>,
    pub secret_patches: Vec<AdapterSecretPatch>,
    pub retry_after: Option<AdapterRetryAfter>,
    pub error: Option<AdapterCallError>,
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
pub struct AdapterAccount {
    pub key: AdapterKey,
    pub display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterSecretPatch {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterReadCapability {
    pub full: bool,
    pub incremental: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterLookupCapability {
    pub reference_namespaces: Vec<String>,
    pub maximum_batch_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterEndpointLookupCapability {
    pub reference_namespaces: Vec<String>,
    pub coordinate_ids: Vec<String>,
    pub maximum_batch_size: u32,
    pub maximum_response_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterConnectionCapabilities {
    pub reference_lookup: Option<AdapterLookupCapability>,
    pub endpoint_lookup: Option<AdapterEndpointLookupCapability>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterEndpointResolutionOperation {
    ReferenceLookup,
    EndpointLookup,
    Unsupported,
}

pub fn endpoint_resolution_operation(
    capabilities: &AdapterConnectionCapabilities,
    endpoint: &PortableEndpoint,
) -> AdapterEndpointResolutionOperation {
    if endpoint.selector().is_none() {
        return capabilities
            .reference_lookup
            .as_ref()
            .filter(|lookup| {
                lookup.maximum_batch_size > 0
                    && lookup
                        .reference_namespaces
                        .iter()
                        .any(|namespace| namespace == endpoint.namespace())
            })
            .map_or(AdapterEndpointResolutionOperation::Unsupported, |_| {
                AdapterEndpointResolutionOperation::ReferenceLookup
            });
    }

    capabilities
        .endpoint_lookup
        .as_ref()
        .filter(|lookup| {
            lookup.maximum_batch_size > 0
                && lookup.maximum_response_bytes > 0
                && lookup
                    .reference_namespaces
                    .iter()
                    .any(|namespace| namespace == endpoint.namespace())
                && endpoint.selector().is_some_and(|selector| {
                    selector.as_str().split('/').all(|component| {
                        let coordinate_id = component
                            .split_once(':')
                            .map_or(component, |(coordinate_id, _)| coordinate_id);
                        lookup
                            .coordinate_ids
                            .iter()
                            .any(|supported| supported == coordinate_id)
                    })
                })
        })
        .map_or(AdapterEndpointResolutionOperation::Unsupported, |_| {
            AdapterEndpointResolutionOperation::EndpointLookup
        })
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
pub struct AdapterRatingScale {
    pub minimum: String,
    pub maximum: String,
    pub step: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterStateNumericRange {
    pub minimum: String,
    pub maximum: String,
    pub step: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterStateField {
    pub field: AdapterTerm,
    pub unit: Option<AdapterTerm>,
    pub value_kind: AdapterValueKind,
    pub rating_scale: Option<AdapterRatingScale>,
    pub numeric_range: Option<AdapterStateNumericRange>,
    pub quantizer: Option<Quantizer>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterAssetCapability {
    pub maximum_bytes: u64,
    pub content_types: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterSourceCapabilities {
    pub catalog: Option<AdapterReadCapability>,
    pub state: Option<AdapterReadCapability>,
    pub state_fields: Vec<AdapterStateField>,
    pub assets: Option<AdapterAssetCapability>,
    pub coordinate_ids: Vec<String>,
    pub targeted_state_read: Option<AdapterTargetedStateReadCapability>,
    pub targeted_state_write: Option<AdapterTargetedStateWriteCapability>,
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
pub struct AdapterSource {
    pub key: AdapterKey,
    pub account_key: AdapterKey,
    pub display_name: String,
    pub kind: AdapterTerm,
    pub availability: AdapterSourceAvailability,
    pub capabilities: AdapterSourceCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenedAdapterConnection {
    pub accounts: Vec<AdapterAccount>,
    pub capabilities: AdapterConnectionCapabilities,
    pub secret_patches: Vec<AdapterSecretPatch>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredAdapterSources {
    pub sources: Vec<AdapterSource>,
    pub secret_patches: Vec<AdapterSecretPatch>,
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

pub trait AdapterReadStream<Batch>: Send {
    type Error;

    fn next_event(&mut self) -> AdapterFuture<'_, Option<AdapterReadEvent<Batch>>, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterRetirement {
    Graceful,
    Forced,
}

pub trait AdapterLauncher: Send + Sync {
    type Session: AdapterSession<Error = Self::Error>;
    type Error;

    fn launch(&self, command: LaunchAdapter) -> AdapterFuture<'_, Self::Session, Self::Error>;
}

pub trait AdapterSession: Send {
    type Error;
    type CatalogReadStream: AdapterReadStream<CatalogBatch, Error = Self::Error>;
    type StateReadStream: AdapterReadStream<StateBatch, Error = Self::Error>;

    fn identity(&self) -> &AdapterIdentity;

    fn health(&mut self) -> AdapterFuture<'_, AdapterHealth, Self::Error>;

    fn describe_connection(
        &mut self,
    ) -> AdapterFuture<'_, AdapterOperationResult<AdapterConnectionDescription>, Self::Error>;

    fn validate_connection(
        &mut self,
        command: ValidateAdapterConnection,
    ) -> AdapterFuture<'_, AdapterOperationResult<AdapterConnectionValidation>, Self::Error>;

    fn list_authentication_methods(
        &mut self,
    ) -> AdapterFuture<'_, AdapterOperationResult<Vec<AdapterAuthenticationMethod>>, Self::Error>;

    fn start_authentication(
        &mut self,
        command: StartAdapterAuthentication,
    ) -> AdapterFuture<'_, AdapterOperationResult<AdapterAuthenticationStep>, Self::Error>;

    fn continue_authentication(
        &mut self,
        command: ContinueAdapterAuthentication,
    ) -> AdapterFuture<'_, AdapterOperationResult<AdapterAuthenticationStep>, Self::Error>;

    fn cancel_authentication(
        &mut self,
        authentication_id: String,
    ) -> AdapterFuture<'_, AdapterOperationResult<()>, Self::Error>;

    fn open_connection(
        &mut self,
        command: OpenAdapterConnection,
    ) -> AdapterFuture<'_, AdapterOperationResult<OpenedAdapterConnection>, Self::Error>;

    fn discover_sources(
        &mut self,
    ) -> AdapterFuture<'_, AdapterOperationResult<DiscoveredAdapterSources>, Self::Error>;

    fn read_catalog(
        &mut self,
        command: ReadAdapterSource,
    ) -> AdapterFuture<'_, Self::CatalogReadStream, Self::Error>;

    fn read_state(
        &mut self,
        command: ReadAdapterSource,
    ) -> AdapterFuture<'_, Self::StateReadStream, Self::Error>;

    fn read_targeted_state(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> AdapterFuture<'_, AdapterTargetedStateReadOutcome, Self::Error>;

    fn write_targeted_state(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> AdapterFuture<'_, AdapterTargetedStateWriteOutcome, Self::Error>;

    fn lookup_portable_references(
        &mut self,
        command: LookupAdapterPortableReferences,
    ) -> AdapterFuture<'_, Result<Vec<AdapterPortableReferenceLookup>, AdapterCallError>, Self::Error>;

    fn resolve_portable_endpoints(
        &mut self,
        command: ResolveAdapterPortableEndpoints,
    ) -> AdapterFuture<
        '_,
        Result<Vec<AdapterPortableEndpointResolution>, AdapterCallError>,
        Self::Error,
    >;

    fn read_asset(
        &mut self,
        command: ReadAdapterAsset,
    ) -> AdapterFuture<'_, Result<AdapterBinaryAsset, AdapterCallError>, Self::Error>;

    fn cancel_operation(&mut self, operation_id: Vec<u8>) -> AdapterFuture<'_, (), Self::Error>;

    fn retire(self) -> AdapterRetirementFuture<AdapterRetirement, Self::Error>
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::{
        AdapterConnectionCapabilities, AdapterEndpointLookupCapability,
        AdapterEndpointResolutionOperation, AdapterLookupCapability, endpoint_resolution_operation,
    };
    use trakkin_mapping::PortableEndpoint;

    #[test]
    fn endpoint_resolution_uses_selector_compatible_capability() {
        let capabilities = AdapterConnectionCapabilities {
            reference_lookup: Some(AdapterLookupCapability {
                reference_namespaces: vec!["example.media".to_owned()],
                maximum_batch_size: 50,
            }),
            endpoint_lookup: Some(AdapterEndpointLookupCapability {
                reference_namespaces: vec!["example.media".to_owned()],
                coordinate_ids: vec!["segment".to_owned()],
                maximum_batch_size: 50,
                maximum_response_bytes: 65_536,
            }),
        };
        let bare = PortableEndpoint::parse("example.media://media/quiet-signal")
            .expect("bare endpoint is valid");
        let selected = PortableEndpoint::parse("example.media://media/quiet-signal[segment:1..12]")
            .expect("selected endpoint is valid");

        assert_eq!(
            endpoint_resolution_operation(&capabilities, &bare),
            AdapterEndpointResolutionOperation::ReferenceLookup
        );
        assert_eq!(
            endpoint_resolution_operation(&capabilities, &selected),
            AdapterEndpointResolutionOperation::EndpointLookup
        );
    }
}
