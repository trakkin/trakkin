use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use serde_json::json;
use time::OffsetDateTime;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    time::timeout,
};
use tonic::{Request, transport::Channel};
use trakkin_adapter_contract::{
    current_protocol_version,
    v1::{
        AccountSnapshot as ProtoAccountSnapshot, AdapterError,
        AuthenticationPrompt as ProtoAuthenticationPrompt,
        AuthenticationStatus as ProtoAuthenticationStatus, CancelAuthenticationRequest,
        ConfigurationField, ConfigurationValue, ContinueAuthenticationRequest,
        ContinueAuthenticationResponse, CoordinateBacking as ProtoCoordinateBacking, DecimalValue,
        DescribeConnectionRequest, DiscoverSourcesRequest, HealthRequest, HealthStatus, Key,
        ListAuthenticationMethodsRequest, LookupPortableReferencesRequest, OpenConnectionRequest,
        PortableEndpoint as ProtoPortableEndpoint, ProtocolVersion, ReadAssetRequest,
        ReadCatalogRequest, ReadCatalogResponse as ProtoCatalogReadEvent,
        ReadMode as ProtoReadMode, ReadStateRequest, ReadStateResponse as ProtoStateReadEvent,
        ReadTargetedStateRequest, ResolvePortableEndpointsRequest, SecretPatch, SecretValue,
        ShutdownRequest, SourceAvailability, SourceMembership as ProtoSourceMembership,
        StartAuthenticationRequest, StartAuthenticationResponse, StateField as ProtoStateField,
        StateFieldQuantizer, StatePresence as ProtoStatePresence, TargetedStateClear,
        TargetedStateFieldEffectKind as ProtoTargetedStateFieldEffectKind,
        TargetedStateMembershipEffect as ProtoTargetedStateMembershipEffect,
        TargetedStateReadCapability as ProtoTargetedStateReadCapability,
        TargetedStateWriteCapability as ProtoTargetedStateWriteCapability,
        TargetedStateWriteCertainty as ProtoTargetedStateWriteCertainty,
        TargetedStateWriteIdempotencyMode as ProtoTargetedStateWriteIdempotencyMode,
        TargetedStateWriteIntent,
        TargetedStateWritePreconditionMode as ProtoTargetedStateWritePreconditionMode,
        TargetedStateWriteRetryDisposition as ProtoTargetedStateWriteRetryDisposition,
        TargetedStateWriteStatus as ProtoTargetedStateWriteStatus, TimestampValue,
        ValidateConnectionRequest, Value, WriteTargetedStateRequest,
        adapter_service_client::AdapterServiceClient, cancel_authentication_response,
        cancel_operation_response, describe_connection_response, discover_sources_response,
        list_authentication_methods_response, lookup_portable_references_response,
        open_connection_response, portable_endpoint_resolution, portable_reference_lookup_result,
        read_asset_response, read_catalog_response, read_state_response,
        read_targeted_state_response, resolve_portable_endpoints_response, secret_patch,
        state_observation, subject_reference, targeted_state_write_intent,
        validate_connection_response, value,
    },
    validation,
};
use trakkin_adapter_sdk::{
    BOOTSTRAP_VERSION, LaunchRequest, LaunchToken, ReadyMessage, supported_protocol_range,
};

use crate::application::adapter_runtime::{
    AdapterAccount, AdapterAssetCapability, AdapterAuthenticationMethod,
    AdapterAuthenticationPrompt, AdapterAuthenticationStatus, AdapterAuthenticationStep,
    AdapterBinaryAsset, AdapterCallError, AdapterConfigurationField, AdapterConfigurationOption,
    AdapterConnectionCapabilities, AdapterConnectionDescription, AdapterConnectionValidation,
    AdapterEndpointLookupCandidate, AdapterEndpointLookupCapability, AdapterEndpointLookupOutcome,
    AdapterEndpointResolutionOperation, AdapterFieldProblem, AdapterFuture, AdapterHealth,
    AdapterHealthStatus, AdapterIdentity, AdapterKey, AdapterLauncher, AdapterLookupCandidate,
    AdapterLookupCapability, AdapterLookupEvidence, AdapterLookupOutcome,
    AdapterPortableEndpointResolution, AdapterPortableReferenceLookup, AdapterRatingScale,
    AdapterReadCapability, AdapterReadCompletion, AdapterReadEvent, AdapterReadStream,
    AdapterRetirement, AdapterRetirementFuture, AdapterRetryAfter, AdapterSecretPatch,
    AdapterSession, AdapterSource, AdapterSourceAvailability, AdapterSourceCapabilities,
    AdapterSourceMembership, AdapterStateField, AdapterStateNumericRange, AdapterStatePresence,
    AdapterTargetedStateField, AdapterTargetedStateFieldEffect,
    AdapterTargetedStateFieldEffectKind, AdapterTargetedStateFieldWriteCapability,
    AdapterTargetedStateMatch, AdapterTargetedStateMembershipEffect,
    AdapterTargetedStateReadCapability, AdapterTargetedStateReadOutcome,
    AdapterTargetedStateWriteCapability, AdapterTargetedStateWriteCausation,
    AdapterTargetedStateWriteCertainty, AdapterTargetedStateWriteIdempotencyMode,
    AdapterTargetedStateWriteOperation, AdapterTargetedStateWriteOutcome,
    AdapterTargetedStateWritePreconditionMode, AdapterTargetedStateWriteRetryDisposition,
    AdapterTargetedStateWriteStatus, AdapterTerm, AdapterValue, AdapterValueKind,
    ContinueAdapterAuthentication, DiscoveredAdapterSources, LaunchAdapter,
    LookupAdapterPortableReferences, OpenAdapterConnection, OpenedAdapterConnection,
    ReadAdapterAsset, ReadAdapterSource, ReadAdapterTargetedState, ResolveAdapterPortableEndpoints,
    StartAdapterAuthentication, ValidateAdapterConnection, WriteAdapterTargetedState,
    endpoint_resolution_operation,
};
use crate::domain::synchronization::Quantizer;
use crate::{
    application::acquisition::ReadMode,
    domain::acquisition::{
        BinaryAssetKey, BinaryAssetReference, CatalogBatch, CatalogItem, CatalogRelation,
        CatalogRelationKey, CoordinateBacking, CoordinateBinding, CoordinateBindingKey,
        CoordinateSubject, PortableReference, ProviderItem, ProviderItemKey, StateBatch,
        StateField, StateObservation, StateSubject, StateValue, Term,
    },
};

const MAXIMUM_READY_MESSAGE_BYTES: u64 = 16 * 1024;

#[derive(Clone)]
pub struct AdapterProcessLauncher {
    executable: PathBuf,
    host_version: String,
    ready_timeout: Duration,
    rpc_timeout: Duration,
    shutdown_timeout: Duration,
}

impl AdapterProcessLauncher {
    pub fn new(executable: impl Into<PathBuf>, host_version: impl Into<String>) -> Self {
        Self {
            executable: executable.into(),
            host_version: host_version.into(),
            ready_timeout: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    pub fn with_timeouts(
        mut self,
        ready_timeout: Duration,
        rpc_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Self {
        self.ready_timeout = ready_timeout;
        self.rpc_timeout = rpc_timeout;
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    async fn launch_process(
        &self,
        command: LaunchAdapter,
    ) -> Result<AdapterProcess, AdapterProcessError> {
        validate_process_instance_id(&command.process_instance_id)?;
        let launch_token = generate_launch_token()?;
        let launch = LaunchRequest {
            bootstrap_version: BOOTSTRAP_VERSION,
            process_instance_id: command.process_instance_id.clone(),
            bind_address: "127.0.0.1:0".to_owned(),
            launch_token: launch_token.clone(),
        };
        let mut child = spawn_child(&self.executable)?;
        let initialization = initialize_child(
            &mut child,
            &launch,
            &self.host_version,
            self.ready_timeout,
            self.rpc_timeout,
        )
        .await;
        let (client, token, identity) = match initialization {
            Ok(initialized) => initialized,
            Err(error) => {
                force_stop(&mut child).await;
                return Err(error);
            }
        };
        Ok(AdapterProcess {
            child,
            client,
            token,
            identity,
            account_keys: Vec::new(),
            connection_capabilities: None,
            rpc_timeout: self.rpc_timeout,
            shutdown_timeout: self.shutdown_timeout,
        })
    }
}

impl AdapterLauncher for AdapterProcessLauncher {
    type Session = AdapterProcess;
    type Error = AdapterProcessError;

    fn launch(&self, command: LaunchAdapter) -> AdapterFuture<'_, Self::Session, Self::Error> {
        Box::pin(self.launch_process(command))
    }
}

pub struct AdapterProcess {
    child: Child,
    client: AdapterServiceClient<Channel>,
    token: LaunchToken,
    identity: AdapterIdentity,
    account_keys: Vec<Key>,
    connection_capabilities: Option<AdapterConnectionCapabilities>,
    rpc_timeout: Duration,
    shutdown_timeout: Duration,
}

pub struct AdapterCatalogReadStream {
    inner: tonic::Streaming<ProtoCatalogReadEvent>,
    validator: validation::CatalogStreamValidator,
    operation_id: Vec<u8>,
    finished: bool,
}

impl AdapterReadStream<CatalogBatch> for AdapterCatalogReadStream {
    type Error = AdapterProcessError;

    fn next_event(
        &mut self,
    ) -> AdapterFuture<'_, Option<AdapterReadEvent<CatalogBatch>>, Self::Error> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            let Some(event) = self.inner.message().await? else {
                self.finished = true;
                std::mem::take(&mut self.validator)
                    .finish()
                    .map_err(|_| AdapterProcessError::InvalidResponse("invalid catalog stream"))?;
                return Ok(None);
            };
            self.validator
                .accept(&event)
                .map_err(|_| AdapterProcessError::InvalidResponse("invalid catalog stream"))?;
            map_catalog_event(event, &self.operation_id).map(Some)
        })
    }
}

pub struct AdapterStateReadStream {
    inner: tonic::Streaming<ProtoStateReadEvent>,
    validator: validation::StateStreamValidator,
    operation_id: Vec<u8>,
    finished: bool,
}

impl AdapterReadStream<StateBatch> for AdapterStateReadStream {
    type Error = AdapterProcessError;

    fn next_event(
        &mut self,
    ) -> AdapterFuture<'_, Option<AdapterReadEvent<StateBatch>>, Self::Error> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            let Some(event) = self.inner.message().await? else {
                self.finished = true;
                std::mem::take(&mut self.validator)
                    .finish()
                    .map_err(|_| AdapterProcessError::InvalidResponse("invalid state stream"))?;
                return Ok(None);
            };
            self.validator
                .accept(&event)
                .map_err(|_| AdapterProcessError::InvalidResponse("invalid state stream"))?;
            map_state_event(event, &self.operation_id).map(Some)
        })
    }
}

impl AdapterSession for AdapterProcess {
    type Error = AdapterProcessError;
    type CatalogReadStream = AdapterCatalogReadStream;
    type StateReadStream = AdapterStateReadStream;

    fn identity(&self) -> &AdapterIdentity {
        &self.identity
    }

    fn health(&mut self) -> AdapterFuture<'_, AdapterHealth, Self::Error> {
        Box::pin(async move {
            let request = self.signed(HealthRequest {});
            let response = timeout(self.rpc_timeout, self.client.health(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("health"))??
                .into_inner();
            validation::health_response(&response)
                .map_err(|_| AdapterProcessError::InvalidResponse("invalid health response"))?;
            let status = match HealthStatus::try_from(response.status)
                .map_err(|_| AdapterProcessError::InvalidResponse("unknown health status"))?
            {
                HealthStatus::Ready => AdapterHealthStatus::Ready,
                HealthStatus::Degraded => AdapterHealthStatus::Degraded,
                HealthStatus::NotReady => AdapterHealthStatus::NotReady,
                HealthStatus::Unspecified => {
                    return Err(AdapterProcessError::InvalidResponse(
                        "health status is unspecified",
                    ));
                }
            };
            Ok(AdapterHealth {
                status,
                error: response.error.map(map_adapter_error),
            })
        })
    }

    fn describe_connection(
        &mut self,
    ) -> AdapterFuture<'_, AdapterConnectionDescription, Self::Error> {
        Box::pin(async move {
            let request = self.signed(DescribeConnectionRequest {});
            let response = timeout(self.rpc_timeout, self.client.describe_connection(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("describe connection"))??
                .into_inner();
            validation::describe_connection_response(&response).map_err(|_| {
                AdapterProcessError::InvalidResponse("invalid connection description response")
            })?;
            let result = match response
                .outcome
                .expect("validated connection description outcome")
            {
                describe_connection_response::Outcome::Result(result) => result,
                describe_connection_response::Outcome::Error(error) => {
                    return Err(AdapterProcessError::Adapter(map_adapter_error(error)));
                }
            };
            Ok(AdapterConnectionDescription {
                fields: map_configuration_fields(result.fields)?,
            })
        })
    }

    fn validate_connection(
        &mut self,
        command: ValidateAdapterConnection,
    ) -> AdapterFuture<'_, AdapterConnectionValidation, Self::Error> {
        Box::pin(async move {
            let request = self.signed(ValidateConnectionRequest {
                settings: map_settings(command.settings),
                secrets: map_secrets(command.secrets),
            });
            let response = timeout(self.rpc_timeout, self.client.validate_connection(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("validate connection"))??
                .into_inner();
            validation::validate_connection_response(&response).map_err(|_| {
                AdapterProcessError::InvalidResponse("invalid connection validation response")
            })?;
            let result = match response
                .outcome
                .expect("validated connection validation outcome")
            {
                validate_connection_response::Outcome::Result(result) => result,
                validate_connection_response::Outcome::Error(error) => {
                    return Err(AdapterProcessError::Adapter(map_adapter_error(error)));
                }
            };
            Ok(AdapterConnectionValidation {
                field_problems: result
                    .field_problems
                    .into_iter()
                    .map(map_field_problem)
                    .collect(),
            })
        })
    }

    fn list_authentication_methods(
        &mut self,
    ) -> AdapterFuture<'_, Vec<AdapterAuthenticationMethod>, Self::Error> {
        Box::pin(async move {
            let request = self.signed(ListAuthenticationMethodsRequest {});
            let response = timeout(
                self.rpc_timeout,
                self.client.list_authentication_methods(request),
            )
            .await
            .map_err(|_| AdapterProcessError::RpcTimeout("list authentication methods"))??
            .into_inner();
            validation::list_authentication_methods_response(&response).map_err(|_| {
                AdapterProcessError::InvalidResponse("invalid authentication methods response")
            })?;
            let result = match response
                .outcome
                .expect("validated authentication methods outcome")
            {
                list_authentication_methods_response::Outcome::Result(result) => result,
                list_authentication_methods_response::Outcome::Error(error) => {
                    return Err(AdapterProcessError::Adapter(map_adapter_error(error)));
                }
            };
            result
                .methods
                .into_iter()
                .map(|method| {
                    if method.key.is_empty() || method.display_name.is_empty() {
                        return Err(AdapterProcessError::InvalidResponse(
                            "invalid authentication method",
                        ));
                    }
                    Ok(AdapterAuthenticationMethod {
                        key: method.key,
                        display_name: method.display_name,
                        interactions: method
                            .interactions
                            .into_iter()
                            .map(|term| map_term(Some(term)))
                            .collect::<Result<_, _>>()?,
                        fields: map_configuration_fields(method.fields)?,
                    })
                })
                .collect()
        })
    }

    fn start_authentication(
        &mut self,
        command: StartAdapterAuthentication,
    ) -> AdapterFuture<'_, AdapterAuthenticationStep, Self::Error> {
        Box::pin(async move {
            if command.method.is_empty() {
                return Err(AdapterProcessError::InvalidAuthenticationRequest);
            }
            let request = self.signed(StartAuthenticationRequest {
                method: command.method,
                values: map_settings(command.values),
            });
            let response = timeout(self.rpc_timeout, self.client.start_authentication(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("start authentication"))??
                .into_inner();
            map_start_authentication_response(response)
        })
    }

    fn continue_authentication(
        &mut self,
        command: ContinueAdapterAuthentication,
    ) -> AdapterFuture<'_, AdapterAuthenticationStep, Self::Error> {
        Box::pin(async move {
            if command.authentication_id.is_empty() {
                return Err(AdapterProcessError::InvalidAuthenticationRequest);
            }
            let request = self.signed(ContinueAuthenticationRequest {
                authentication_id: command.authentication_id,
                values: map_settings(command.values),
            });
            let response = timeout(
                self.rpc_timeout,
                self.client.continue_authentication(request),
            )
            .await
            .map_err(|_| AdapterProcessError::RpcTimeout("continue authentication"))??
            .into_inner();
            map_continue_authentication_response(response)
        })
    }

    fn cancel_authentication(
        &mut self,
        authentication_id: String,
    ) -> AdapterFuture<'_, (), Self::Error> {
        Box::pin(async move {
            if authentication_id.is_empty() {
                return Err(AdapterProcessError::InvalidAuthenticationRequest);
            }
            let request = self.signed(CancelAuthenticationRequest { authentication_id });
            let response = timeout(self.rpc_timeout, self.client.cancel_authentication(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("cancel authentication"))??
                .into_inner();
            validation::cancel_authentication_response(&response).map_err(|_| {
                AdapterProcessError::InvalidResponse("invalid authentication cancellation response")
            })?;
            match response
                .outcome
                .expect("validated authentication cancellation outcome")
            {
                cancel_authentication_response::Outcome::Result(_) => Ok(()),
                cancel_authentication_response::Outcome::Error(error) => {
                    Err(AdapterProcessError::Adapter(map_adapter_error(error)))
                }
            }
        })
    }

    fn open_connection(
        &mut self,
        command: OpenAdapterConnection,
    ) -> AdapterFuture<'_, OpenedAdapterConnection, Self::Error> {
        Box::pin(async move {
            let request = self.signed(OpenConnectionRequest {
                settings: map_settings(command.settings),
                secrets: map_secrets(command.secrets),
            });
            let response = timeout(self.rpc_timeout, self.client.open_connection(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("open connection"))??
                .into_inner();
            validation::open_connection_response(&response)
                .map_err(|_| AdapterProcessError::InvalidResponse("invalid open response"))?;
            let result = match response.outcome.expect("validated open connection outcome") {
                open_connection_response::Outcome::Result(result) => result,
                open_connection_response::Outcome::Error(error) => {
                    return Err(AdapterProcessError::Adapter(map_adapter_error(error)));
                }
            };
            self.account_keys = result
                .accounts
                .iter()
                .map(|account| {
                    account
                        .key
                        .clone()
                        .ok_or(AdapterProcessError::InvalidResponse(
                            "account key is missing",
                        ))
                })
                .collect::<Result<_, _>>()?;
            let capabilities = map_connection_capabilities(result.capabilities);
            self.connection_capabilities = Some(capabilities.clone());
            Ok(OpenedAdapterConnection {
                accounts: result
                    .accounts
                    .into_iter()
                    .map(|account| {
                        Ok(AdapterAccount {
                            key: map_key(account.key)?,
                            display_name: account.display_name,
                        })
                    })
                    .collect::<Result<_, AdapterProcessError>>()?,
                capabilities,
                secret_patches: map_secret_patches(result.secret_patches)?,
            })
        })
    }

    fn discover_sources(&mut self) -> AdapterFuture<'_, DiscoveredAdapterSources, Self::Error> {
        Box::pin(async move {
            if self.account_keys.is_empty() {
                return Err(AdapterProcessError::ConnectionNotOpen);
            }
            let request = self.signed(DiscoverSourcesRequest {});
            let response = timeout(self.rpc_timeout, self.client.discover_sources(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("discover sources"))??
                .into_inner();
            validation::discover_sources_response(&self.account_keys, &response)
                .map_err(|_| AdapterProcessError::InvalidResponse("invalid discovery response"))?;
            let result = match response
                .outcome
                .expect("validated source discovery outcome")
            {
                discover_sources_response::Outcome::Result(result) => result,
                discover_sources_response::Outcome::Error(error) => {
                    return Err(AdapterProcessError::Adapter(map_adapter_error(error)));
                }
            };
            Ok(DiscoveredAdapterSources {
                sources: result
                    .sources
                    .into_iter()
                    .map(map_source)
                    .collect::<Result<_, _>>()?,
                secret_patches: map_secret_patches(result.secret_patches)?,
            })
        })
    }

    fn read_catalog(
        &mut self,
        command: ReadAdapterSource,
    ) -> AdapterFuture<'_, Self::CatalogReadStream, Self::Error> {
        Box::pin(async move {
            let operation_id = command.operation_id.clone();
            let request = self.signed(map_read_catalog_request(command)?);
            let response = timeout(self.rpc_timeout, self.client.read_catalog(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("read catalog"))??;
            Ok(AdapterCatalogReadStream {
                inner: response.into_inner(),
                validator: validation::CatalogStreamValidator::default(),
                operation_id,
                finished: false,
            })
        })
    }

    fn read_state(
        &mut self,
        command: ReadAdapterSource,
    ) -> AdapterFuture<'_, Self::StateReadStream, Self::Error> {
        Box::pin(async move {
            let operation_id = command.operation_id.clone();
            let request = self.signed(map_read_state_request(command)?);
            let response = timeout(self.rpc_timeout, self.client.read_state(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("read state"))??;
            Ok(AdapterStateReadStream {
                inner: response.into_inner(),
                validator: validation::StateStreamValidator::default(),
                operation_id,
                finished: false,
            })
        })
    }

    fn read_targeted_state(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> AdapterFuture<'_, AdapterTargetedStateReadOutcome, Self::Error> {
        Box::pin(async move {
            let capability = ProtoTargetedStateReadCapability {
                maximum_fields: command.capability.maximum_fields,
                maximum_response_bytes: command.capability.maximum_response_bytes,
            };
            let request = ReadTargetedStateRequest {
                operation_id: command.operation_id,
                source_key: Some(Key {
                    namespace: command.source_key.namespace,
                    value: command.source_key.value,
                }),
                subject: Some(map_state_subject(command.subject)),
                fields: command.fields.into_iter().map(map_state_field).collect(),
                maximum_response_bytes: command.maximum_response_bytes,
                reconciliation_idempotency_key: command
                    .reconciliation_idempotency_key
                    .unwrap_or_default(),
            };
            validation::targeted_state_read_request(&request, &capability)
                .map_err(|_| AdapterProcessError::InvalidTargetedStateReadRequest)?;
            let request_message = request.clone();
            let request = self.signed(request);
            let response = timeout(self.rpc_timeout, self.client.read_targeted_state(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("read targeted state"))??
                .into_inner();
            validation::read_targeted_state_response(&request_message, &response).map_err(
                |_| AdapterProcessError::InvalidResponse("invalid targeted state response"),
            )?;
            map_targeted_state_outcome(response)
        })
    }

    fn write_targeted_state(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> AdapterFuture<'_, AdapterTargetedStateWriteOutcome, Self::Error> {
        Box::pin(async move {
            let capability = map_targeted_state_write_capability_to_proto(&command.capability);
            let request = WriteTargetedStateRequest {
                operation_id: command.operation_id,
                source_key: Some(Key {
                    namespace: command.source_key.namespace,
                    value: command.source_key.value,
                }),
                subject: Some(map_state_subject(command.subject)),
                idempotency_key: command.idempotency_key,
                expected_membership: map_source_membership(command.expected_membership) as i32,
                precondition: command.precondition.unwrap_or_default(),
                allow_create_membership: command.allow_create_membership,
                intents: command
                    .intents
                    .into_iter()
                    .map(|intent| TargetedStateWriteIntent {
                        field: Some(map_state_field(intent.field)),
                        operation: Some(match intent.operation {
                            AdapterTargetedStateWriteOperation::Set(value) => {
                                targeted_state_write_intent::Operation::Set(map_domain_state_value(
                                    value,
                                ))
                            }
                            AdapterTargetedStateWriteOperation::Clear => {
                                targeted_state_write_intent::Operation::Clear(TargetedStateClear {})
                            }
                        }),
                    })
                    .collect(),
                maximum_receipt_bytes: command.maximum_receipt_bytes,
                maximum_response_bytes: command.maximum_response_bytes,
            };
            validation::targeted_state_write_request(&request, &capability)
                .map_err(|_| AdapterProcessError::InvalidTargetedStateWriteRequest)?;
            let rpc_request = self.signed(request.clone());
            let response = timeout(
                self.rpc_timeout,
                self.client.write_targeted_state(rpc_request),
            )
            .await
            .map_err(|_| AdapterProcessError::RpcTimeout("write targeted state"))??
            .into_inner();
            if validation::targeted_state_write_response(&request, &capability, &response).is_err()
            {
                force_stop(&mut self.child).await;
                return Err(AdapterProcessError::InvalidResponse(
                    "invalid targeted state write response",
                ));
            }
            map_targeted_state_write_outcome(response)
        })
    }

    fn lookup_portable_references(
        &mut self,
        command: LookupAdapterPortableReferences,
    ) -> AdapterFuture<'_, Result<Vec<AdapterPortableReferenceLookup>, AdapterCallError>, Self::Error>
    {
        Box::pin(async move {
            if command.operation_id.is_empty() || command.references.is_empty() {
                return Err(AdapterProcessError::InvalidLookupRequest);
            }
            let requested = command
                .references
                .iter()
                .map(map_portable_reference)
                .collect::<Vec<_>>();
            let request = self.signed(LookupPortableReferencesRequest {
                operation_id: command.operation_id,
                references: requested.clone(),
            });
            let response = timeout(
                self.rpc_timeout,
                self.client.lookup_portable_references(request),
            )
            .await
            .map_err(|_| AdapterProcessError::RpcTimeout("lookup portable references"))??
            .into_inner();
            validation::lookup_response(&requested, &response)
                .map_err(|_| AdapterProcessError::InvalidResponse("invalid lookup response"))?;
            let result = match response
                .outcome
                .expect("validated portable reference lookup outcome")
            {
                lookup_portable_references_response::Outcome::Result(result) => result,
                lookup_portable_references_response::Outcome::Error(error) => {
                    return Ok(Err(map_adapter_error(error)));
                }
            };
            Ok(Ok(result
                .results
                .into_iter()
                .map(map_lookup_result)
                .collect::<Result<_, _>>()?))
        })
    }

    fn resolve_portable_endpoints(
        &mut self,
        command: ResolveAdapterPortableEndpoints,
    ) -> AdapterFuture<
        '_,
        Result<Vec<AdapterPortableEndpointResolution>, AdapterCallError>,
        Self::Error,
    > {
        Box::pin(async move {
            if command.operation_id.is_empty()
                || command.endpoints.is_empty()
                || command.maximum_response_bytes == 0
            {
                return Err(AdapterProcessError::InvalidEndpointLookupRequest);
            }
            let capabilities = self
                .connection_capabilities
                .clone()
                .ok_or(AdapterProcessError::ConnectionNotOpen)?;
            let mut results = command
                .endpoints
                .iter()
                .cloned()
                .map(|requested| AdapterPortableEndpointResolution {
                    requested,
                    outcome: AdapterEndpointLookupOutcome::Unsupported,
                })
                .collect::<Vec<_>>();
            let mut reference_indexes = Vec::new();
            let mut references = Vec::new();
            let mut endpoint_indexes = Vec::new();
            let mut endpoints = Vec::new();
            for (index, endpoint) in command.endpoints.iter().enumerate() {
                match endpoint_resolution_operation(&capabilities, endpoint) {
                    AdapterEndpointResolutionOperation::ReferenceLookup => {
                        reference_indexes.push(index);
                        references.push(
                            PortableReference::new(endpoint.namespace(), endpoint.value().to_vec())
                                .map_err(|_| AdapterProcessError::InvalidEndpointLookupRequest)?,
                        );
                    }
                    AdapterEndpointResolutionOperation::EndpointLookup => {
                        endpoint_indexes.push(index);
                        endpoints.push(endpoint.clone());
                    }
                    AdapterEndpointResolutionOperation::Unsupported => {}
                }
            }

            if !references.is_empty() {
                let maximum_batch_size = capabilities
                    .reference_lookup
                    .as_ref()
                    .map_or(0, |lookup| lookup.maximum_batch_size);
                if references.len() > maximum_batch_size as usize {
                    return Err(AdapterProcessError::InvalidEndpointLookupRequest);
                }
                let lookups = match self
                    .lookup_portable_references(LookupAdapterPortableReferences {
                        operation_id: command.operation_id.clone(),
                        references,
                    })
                    .await?
                {
                    Ok(lookups) => lookups,
                    Err(error) => return Ok(Err(error)),
                };
                for (index, lookup) in reference_indexes.into_iter().zip(lookups) {
                    results[index] =
                        map_reference_endpoint_result(command.endpoints[index].clone(), lookup)?;
                }
            }

            if !endpoints.is_empty() {
                let capability = capabilities
                    .endpoint_lookup
                    .as_ref()
                    .ok_or(AdapterProcessError::InvalidEndpointLookupRequest)?;
                if endpoints.len() > capability.maximum_batch_size as usize {
                    return Err(AdapterProcessError::InvalidEndpointLookupRequest);
                }
                let maximum_response_bytes = command
                    .maximum_response_bytes
                    .min(capability.maximum_response_bytes);
                let requested = endpoints
                    .iter()
                    .map(map_portable_endpoint)
                    .collect::<Vec<_>>();
                let request = self.signed(ResolvePortableEndpointsRequest {
                    operation_id: command.operation_id,
                    endpoints: requested.clone(),
                    maximum_response_bytes,
                });
                let response = timeout(
                    self.rpc_timeout,
                    self.client.resolve_portable_endpoints(request),
                )
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("resolve portable endpoints"))??
                .into_inner();
                validation::resolve_endpoints_response(
                    &requested,
                    &response,
                    maximum_response_bytes,
                )
                .map_err(|_| {
                    AdapterProcessError::InvalidResponse("invalid endpoint resolution response")
                })?;
                let result = match response
                    .outcome
                    .expect("validated endpoint resolution outcome")
                {
                    resolve_portable_endpoints_response::Outcome::Result(result) => result,
                    resolve_portable_endpoints_response::Outcome::Error(error) => {
                        return Ok(Err(map_adapter_error(error)));
                    }
                };
                for (index, result) in endpoint_indexes.into_iter().zip(result.results) {
                    results[index] = map_endpoint_result(result)?;
                }
            }

            Ok(Ok(results))
        })
    }

    fn read_asset(
        &mut self,
        command: ReadAdapterAsset,
    ) -> AdapterFuture<'_, Result<AdapterBinaryAsset, AdapterCallError>, Self::Error> {
        Box::pin(async move {
            if command.operation_id.is_empty()
                || command.maximum_bytes == 0
                || command.allowed_content_types.is_empty()
            {
                return Err(AdapterProcessError::InvalidAssetRequest);
            }
            let request = self.signed(ReadAssetRequest {
                operation_id: command.operation_id,
                source_key: Some(map_adapter_key(command.source_key)?),
                provider_item_key: Some(map_adapter_key(command.provider_item_key)?),
                asset_key: Some(map_adapter_key(command.asset_key)?),
                maximum_bytes: command.maximum_bytes,
            });
            let response = timeout(self.rpc_timeout, self.client.read_asset(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("read asset"))??
                .into_inner();
            validation::asset_response(
                &response,
                command.maximum_bytes,
                &command.allowed_content_types,
            )
            .map_err(|_| AdapterProcessError::InvalidResponse("invalid asset response"))?;
            let result = match response.outcome.expect("validated asset read outcome") {
                read_asset_response::Outcome::Result(result) => result,
                read_asset_response::Outcome::Error(error) => {
                    return Ok(Err(map_adapter_error(error)));
                }
            };
            Ok(Ok(AdapterBinaryAsset {
                content: result.content,
                content_type: result.content_type,
                full_length: result.full_length,
                sha256_digest: result
                    .hash
                    .ok_or(AdapterProcessError::InvalidResponse(
                        "asset hash is missing",
                    ))?
                    .digest,
                cache_control: result.cache_control,
            }))
        })
    }

    fn cancel_operation(&mut self, operation_id: Vec<u8>) -> AdapterFuture<'_, (), Self::Error> {
        Box::pin(async move {
            if operation_id.is_empty() {
                return Err(AdapterProcessError::InvalidOperationId);
            }
            let request =
                self.signed(trakkin_adapter_contract::v1::CancelOperationRequest { operation_id });
            let response = timeout(self.rpc_timeout, self.client.cancel_operation(request))
                .await
                .map_err(|_| AdapterProcessError::RpcTimeout("cancel operation"))??
                .into_inner();
            validation::cancel_operation_response(&response).map_err(|_| {
                AdapterProcessError::InvalidResponse("invalid operation cancellation response")
            })?;
            match response
                .outcome
                .expect("validated operation cancellation outcome")
            {
                cancel_operation_response::Outcome::Result(_) => Ok(()),
                cancel_operation_response::Outcome::Error(error) => {
                    Err(AdapterProcessError::Adapter(map_adapter_error(error)))
                }
            }
        })
    }

    fn retire(mut self) -> AdapterRetirementFuture<AdapterRetirement, Self::Error> {
        Box::pin(async move {
            let request = self.signed(ShutdownRequest { grace_period: None });
            let shutdown = timeout(self.rpc_timeout, self.client.shutdown(request)).await;
            if matches!(shutdown, Ok(Ok(_))) {
                match timeout(self.shutdown_timeout, self.child.wait()).await {
                    Ok(Ok(status)) if status.success() => return Ok(AdapterRetirement::Graceful),
                    Ok(Ok(status)) => {
                        return Err(AdapterProcessError::UnexpectedExit(status.code()));
                    }
                    Ok(Err(error)) => return Err(error.into()),
                    Err(_) => {}
                }
            }
            force_stop(&mut self.child).await;
            Ok(AdapterRetirement::Forced)
        })
    }
}

impl AdapterProcess {
    fn signed<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        self.token.apply(&mut request);
        request
    }
}

async fn initialize_child(
    child: &mut Child,
    launch: &LaunchRequest,
    host_version: &str,
    ready_timeout: Duration,
    rpc_timeout: Duration,
) -> Result<(AdapterServiceClient<Channel>, LaunchToken, AdapterIdentity), AdapterProcessError> {
    let mut stdin = child
        .stdin
        .take()
        .ok_or(AdapterProcessError::MissingChildPipe("stdin"))?;
    let mut payload = serde_json::to_vec(launch)?;
    payload.push(b'\n');
    stdin.write_all(&payload).await?;
    stdin.shutdown().await?;
    drop(stdin);

    let stdout = child
        .stdout
        .take()
        .ok_or(AdapterProcessError::MissingChildPipe("stdout"))?;
    let mut reader = BufReader::new(stdout).take(MAXIMUM_READY_MESSAGE_BYTES);
    let mut line = String::new();
    let bytes = timeout(ready_timeout, reader.read_line(&mut line))
        .await
        .map_err(|_| AdapterProcessError::ReadyTimeout)??;
    if bytes == 0 {
        return Err(AdapterProcessError::MissingReadyMessage);
    }
    if !line.ends_with('\n') {
        return Err(AdapterProcessError::ReadyMessageTooLarge);
    }
    let ready = serde_json::from_str::<ReadyMessage>(&line)?;
    let address = validate_ready_message(&ready, launch)?;
    let endpoint = format!("http://{address}");
    let client = timeout(rpc_timeout, AdapterServiceClient::connect(endpoint))
        .await
        .map_err(|_| AdapterProcessError::RpcTimeout("connect"))??;
    let token = LaunchToken::new(&launch.launch_token)
        .map_err(|_| AdapterProcessError::InvalidLaunchToken)?;
    let mut handshake_request = Request::new(trakkin_adapter_contract::v1::HandshakeRequest {
        supported_protocol: Some(supported_protocol_range()),
        host_version: host_version.to_owned(),
        process_instance_id: launch.process_instance_id.clone(),
    });
    token.apply(&mut handshake_request);
    let handshake = timeout(rpc_timeout, client.clone().handshake(handshake_request))
        .await
        .map_err(|_| AdapterProcessError::RpcTimeout("handshake"))??
        .into_inner();
    let selected_protocol =
        handshake
            .selected_protocol
            .ok_or(AdapterProcessError::InvalidResponse(
                "selected protocol is missing",
            ))?;
    validate_handshake(&handshake, &selected_protocol, &launch.process_instance_id)?;
    Ok((
        client,
        token,
        AdapterIdentity {
            adapter_key: handshake.adapter_key,
            adapter_version: handshake.adapter_version,
            process_instance_id: handshake.process_instance_id,
            protocol_major: selected_protocol.major,
            protocol_minor: selected_protocol.minor,
        },
    ))
}

fn spawn_child(executable: &Path) -> Result<Child, AdapterProcessError> {
    Ok(Command::new(executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?)
}

fn validate_process_instance_id(process_instance_id: &str) -> Result<(), AdapterProcessError> {
    if process_instance_id.is_empty() || process_instance_id.len() > 256 {
        return Err(AdapterProcessError::InvalidProcessInstanceId);
    }
    Ok(())
}

fn generate_launch_token() -> Result<String, AdapterProcessError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)?;
    Ok(hex::encode(bytes))
}

fn validate_ready_message(
    ready: &ReadyMessage,
    launch: &LaunchRequest,
) -> Result<SocketAddr, AdapterProcessError> {
    if ready.bootstrap_version != BOOTSTRAP_VERSION
        || ready.process_instance_id != launch.process_instance_id
        || ready.launch_token != launch.launch_token
    {
        return Err(AdapterProcessError::InvalidReadyMessage);
    }
    let address = ready
        .address
        .parse::<SocketAddr>()
        .map_err(|_| AdapterProcessError::InvalidReadyMessage)?;
    if !address.ip().is_loopback() {
        return Err(AdapterProcessError::InvalidReadyMessage);
    }
    Ok(address)
}

fn validate_handshake(
    handshake: &trakkin_adapter_contract::v1::HandshakeResponse,
    selected_protocol: &ProtocolVersion,
    process_instance_id: &str,
) -> Result<(), AdapterProcessError> {
    if handshake.adapter_key.is_empty()
        || handshake.adapter_version.is_empty()
        || handshake.process_instance_id != process_instance_id
        || selected_protocol.major != current_protocol_version().major
        || selected_protocol.minor != current_protocol_version().minor
    {
        return Err(AdapterProcessError::InvalidHandshake);
    }
    Ok(())
}

fn map_value(value: AdapterValue) -> Value {
    let value = match value {
        AdapterValue::Boolean(value) => value::Value::Boolean(value),
        AdapterValue::Integer(value) => value::Value::Integer(value),
        AdapterValue::Decimal(value) => value::Value::Decimal(DecimalValue { value }),
        AdapterValue::Text(value) => value::Value::Text(value),
        AdapterValue::Bytes(value) => value::Value::Bytes(value),
        AdapterValue::TimestampMilliseconds(milliseconds) => {
            value::Value::Timestamp(TimestampValue { milliseconds })
        }
    };
    Value { value: Some(value) }
}

fn map_read_catalog_request(
    command: ReadAdapterSource,
) -> Result<ReadCatalogRequest, AdapterProcessError> {
    if command.operation_id.is_empty()
        || command.source_key.namespace.is_empty()
        || command.source_key.value.is_empty()
        || command.preferred_batch_size == 0
    {
        return Err(AdapterProcessError::InvalidReadRequest);
    }
    let (mode, prior_cursor) = match (command.mode, command.prior_cursor) {
        (ReadMode::Full, None) => (ProtoReadMode::Full, Vec::new()),
        (ReadMode::Incremental, Some(cursor)) if !cursor.is_empty() => {
            (ProtoReadMode::Incremental, cursor)
        }
        (ReadMode::Full | ReadMode::Incremental, _) => {
            return Err(AdapterProcessError::InvalidReadRequest);
        }
    };
    Ok(ReadCatalogRequest {
        operation_id: command.operation_id,
        source_key: Some(Key {
            namespace: command.source_key.namespace,
            value: command.source_key.value,
        }),
        mode: mode as i32,
        prior_cursor,
        preferred_batch_size: command.preferred_batch_size,
    })
}

fn map_read_state_request(
    command: ReadAdapterSource,
) -> Result<ReadStateRequest, AdapterProcessError> {
    let request = map_read_catalog_request(command)?;
    Ok(ReadStateRequest {
        operation_id: request.operation_id,
        source_key: request.source_key,
        mode: request.mode,
        prior_cursor: request.prior_cursor,
        preferred_batch_size: request.preferred_batch_size,
    })
}

fn map_catalog_event(
    event: ProtoCatalogReadEvent,
    operation_id: &[u8],
) -> Result<AdapterReadEvent<CatalogBatch>, AdapterProcessError> {
    match event.event.ok_or(AdapterProcessError::InvalidResponse(
        "catalog event is missing",
    ))? {
        read_catalog_response::Event::Heartbeat(heartbeat) => {
            if heartbeat.operation_id != operation_id {
                return Err(AdapterProcessError::InvalidResponse(
                    "catalog heartbeat operation ID does not match",
                ));
            }
            Ok(AdapterReadEvent::Heartbeat {
                records_emitted: heartbeat.records_emitted,
            })
        }
        read_catalog_response::Event::Batch(batch) => {
            Ok(AdapterReadEvent::Batch(map_catalog_batch(batch)?))
        }
        read_catalog_response::Event::Completed(completed) => {
            Ok(AdapterReadEvent::Completed(map_read_completion(completed)?))
        }
        read_catalog_response::Event::Failed(failed) => {
            Ok(AdapterReadEvent::Failed(map_read_failure(failed.error)?))
        }
        read_catalog_response::Event::Cancelled(_) => Ok(AdapterReadEvent::Cancelled),
    }
}

fn map_state_event(
    event: ProtoStateReadEvent,
    operation_id: &[u8],
) -> Result<AdapterReadEvent<StateBatch>, AdapterProcessError> {
    match event.event.ok_or(AdapterProcessError::InvalidResponse(
        "state event is missing",
    ))? {
        read_state_response::Event::Heartbeat(heartbeat) => {
            if heartbeat.operation_id != operation_id {
                return Err(AdapterProcessError::InvalidResponse(
                    "state heartbeat operation ID does not match",
                ));
            }
            Ok(AdapterReadEvent::Heartbeat {
                records_emitted: heartbeat.records_emitted,
            })
        }
        read_state_response::Event::Batch(batch) => {
            Ok(AdapterReadEvent::Batch(map_state_batch(batch)?))
        }
        read_state_response::Event::Completed(completed) => {
            Ok(AdapterReadEvent::Completed(map_read_completion(completed)?))
        }
        read_state_response::Event::Failed(failed) => {
            Ok(AdapterReadEvent::Failed(map_read_failure(failed.error)?))
        }
        read_state_response::Event::Cancelled(_) => Ok(AdapterReadEvent::Cancelled),
    }
}

fn map_read_completion(
    completed: trakkin_adapter_contract::v1::ReadCompleted,
) -> Result<AdapterReadCompletion, AdapterProcessError> {
    let observed_at = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(completed.observed_time_milliseconds) * 1_000_000,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("read observation time is invalid"))?;
    Ok(AdapterReadCompletion {
        next_cursor: completed.next_cursor,
        evidence_revision: completed.evidence_revision,
        observed_at,
    })
}

fn map_read_failure(error: Option<AdapterError>) -> Result<AdapterCallError, AdapterProcessError> {
    let error = error.ok_or(AdapterProcessError::InvalidResponse(
        "read failure error is missing",
    ))?;
    validate_adapter_error(&error)?;
    Ok(map_adapter_error(error))
}

fn map_catalog_batch(
    batch: trakkin_adapter_contract::v1::CatalogBatch,
) -> Result<CatalogBatch, AdapterProcessError> {
    let coordinate_binding_upserts = batch
        .coordinate_binding_upserts
        .into_iter()
        .map(map_coordinate_binding)
        .collect::<Result<_, _>>()?;
    let coordinate_binding_deletes = batch
        .coordinate_binding_deletes
        .into_iter()
        .map(map_coordinate_binding_key)
        .collect::<Result<_, _>>()?;
    Ok(CatalogBatch::new(
        batch.sequence,
        batch
            .item_upserts
            .into_iter()
            .map(map_catalog_item)
            .collect::<Result<_, _>>()?,
        batch
            .item_deletes
            .into_iter()
            .map(|key| map_provider_item_key(Some(key)))
            .collect::<Result<_, _>>()?,
        batch
            .relation_upserts
            .into_iter()
            .map(map_catalog_relation)
            .collect::<Result<_, _>>()?,
        batch
            .relation_deletes
            .into_iter()
            .map(|key| map_catalog_relation_key(Some(key)))
            .collect::<Result<_, _>>()?,
    )
    .with_coordinate_bindings(coordinate_binding_upserts, coordinate_binding_deletes))
}

fn map_coordinate_binding(
    binding: trakkin_adapter_contract::v1::CoordinateBinding,
) -> Result<CoordinateBinding, AdapterProcessError> {
    let backing = match ProtoCoordinateBacking::try_from(binding.backing)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid coordinate backing"))?
    {
        ProtoCoordinateBacking::Materialized => CoordinateBacking::Materialized,
        ProtoCoordinateBacking::Virtual => CoordinateBacking::Virtual,
        ProtoCoordinateBacking::Aggregate => CoordinateBacking::Aggregate,
        ProtoCoordinateBacking::Unspecified => {
            return Err(AdapterProcessError::InvalidResponse(
                "coordinate backing is unspecified",
            ));
        }
    };
    CoordinateBinding::new(
        map_coordinate_binding_parts(binding.endpoint, binding.subject)?,
        backing,
        binding.evidence_revision,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid coordinate binding"))
}

fn map_coordinate_binding_key(
    binding: trakkin_adapter_contract::v1::CoordinateBindingKey,
) -> Result<CoordinateBindingKey, AdapterProcessError> {
    map_coordinate_binding_parts(binding.endpoint, binding.subject)
}

fn map_coordinate_binding_parts(
    endpoint: Option<trakkin_adapter_contract::v1::PortableEndpoint>,
    subject: Option<trakkin_adapter_contract::v1::SubjectReference>,
) -> Result<CoordinateBindingKey, AdapterProcessError> {
    let endpoint = map_domain_portable_endpoint(endpoint)?;
    let subject = match subject.and_then(|subject| subject.subject).ok_or(
        AdapterProcessError::InvalidResponse("coordinate subject is missing"),
    )? {
        subject_reference::Subject::ProviderItemKey(key) => {
            CoordinateSubject::ProviderItem(map_provider_item_key(Some(key))?)
        }
        subject_reference::Subject::CatalogRelationKey(key) => {
            CoordinateSubject::CatalogRelation(map_catalog_relation_key(Some(key))?)
        }
    };
    Ok(CoordinateBindingKey::new(endpoint, subject))
}

fn map_state_batch(
    batch: trakkin_adapter_contract::v1::StateBatch,
) -> Result<StateBatch, AdapterProcessError> {
    Ok(StateBatch::new(
        batch.sequence,
        batch
            .observations
            .into_iter()
            .map(map_state_observation)
            .collect::<Result<_, _>>()?,
    ))
}

fn map_catalog_item(
    item: trakkin_adapter_contract::v1::ProviderItem,
) -> Result<CatalogItem, AdapterProcessError> {
    map_provider_item(item).map(CatalogItem::from)
}

fn map_provider_item(
    item: trakkin_adapter_contract::v1::ProviderItem,
) -> Result<ProviderItem, AdapterProcessError> {
    ProviderItem::new(
        map_provider_item_key(item.key)?,
        map_domain_term(item.kind)?,
        item.display_name,
        map_attributes(item.attributes)?,
        item.portable_references
            .into_iter()
            .map(|reference| {
                PortableReference::new(reference.namespace, reference.value)
                    .map_err(|_| AdapterProcessError::InvalidResponse("invalid portable reference"))
            })
            .collect::<Result<_, _>>()?,
        item.assets
            .into_iter()
            .map(|asset| {
                Ok(BinaryAssetReference::new(
                    map_binary_asset_key(asset.key)?,
                    map_domain_term(asset.kind)?,
                ))
            })
            .collect::<Result<_, AdapterProcessError>>()?,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid provider item"))
}

fn map_lookup_result(
    result: trakkin_adapter_contract::v1::PortableReferenceLookupResult,
) -> Result<AdapterPortableReferenceLookup, AdapterProcessError> {
    let requested = map_domain_portable_reference(result.requested)?;
    let outcome = match result.outcome.ok_or(AdapterProcessError::InvalidResponse(
        "lookup outcome is missing",
    ))? {
        portable_reference_lookup_result::Outcome::Matched(matched) => {
            AdapterLookupOutcome::Matched(Box::new(map_lookup_candidate(matched.candidate)?))
        }
        portable_reference_lookup_result::Outcome::NotFound(_) => AdapterLookupOutcome::NotFound,
        portable_reference_lookup_result::Outcome::Ambiguous(ambiguous) => {
            AdapterLookupOutcome::Ambiguous(
                ambiguous
                    .candidates
                    .into_iter()
                    .map(|candidate| map_lookup_candidate(Some(candidate)))
                    .collect::<Result<_, _>>()?,
            )
        }
        portable_reference_lookup_result::Outcome::Unsupported(_) => {
            AdapterLookupOutcome::Unsupported
        }
    };
    Ok(AdapterPortableReferenceLookup { requested, outcome })
}

fn map_lookup_candidate(
    candidate: Option<trakkin_adapter_contract::v1::LookupCandidate>,
) -> Result<AdapterLookupCandidate, AdapterProcessError> {
    let candidate = candidate.ok_or(AdapterProcessError::InvalidResponse(
        "lookup candidate is missing",
    ))?;
    Ok(AdapterLookupCandidate {
        provider_item: map_provider_item(candidate.provider_item.ok_or(
            AdapterProcessError::InvalidResponse("lookup provider item is missing"),
        )?)?,
        evidence: map_lookup_evidence(candidate.evidence)?,
    })
}

fn map_reference_endpoint_result(
    requested: trakkin_mapping_contract::PortableEndpoint,
    lookup: AdapterPortableReferenceLookup,
) -> Result<AdapterPortableEndpointResolution, AdapterProcessError> {
    if requested.namespace() != lookup.requested.namespace()
        || requested.value() != lookup.requested.value()
    {
        return Err(AdapterProcessError::InvalidResponse(
            "reference fallback endpoint does not match",
        ));
    }
    let outcome = match lookup.outcome {
        AdapterLookupOutcome::Matched(candidate) => {
            AdapterEndpointLookupOutcome::Matched(Box::new(AdapterEndpointLookupCandidate {
                provider_item: candidate.provider_item,
                binding: None,
                evidence: candidate.evidence,
            }))
        }
        AdapterLookupOutcome::NotFound => AdapterEndpointLookupOutcome::NotFound,
        AdapterLookupOutcome::Ambiguous(candidates) => AdapterEndpointLookupOutcome::Ambiguous(
            candidates
                .into_iter()
                .map(|candidate| AdapterEndpointLookupCandidate {
                    provider_item: candidate.provider_item,
                    binding: None,
                    evidence: candidate.evidence,
                })
                .collect(),
        ),
        AdapterLookupOutcome::Unsupported => AdapterEndpointLookupOutcome::Unsupported,
    };
    Ok(AdapterPortableEndpointResolution { requested, outcome })
}

fn map_endpoint_result(
    result: trakkin_adapter_contract::v1::PortableEndpointResolution,
) -> Result<AdapterPortableEndpointResolution, AdapterProcessError> {
    let requested = map_domain_portable_endpoint(result.requested)?;
    let outcome = match result.outcome.ok_or(AdapterProcessError::InvalidResponse(
        "endpoint resolution outcome is missing",
    ))? {
        portable_endpoint_resolution::Outcome::Matched(matched) => {
            AdapterEndpointLookupOutcome::Matched(Box::new(map_endpoint_candidate(
                matched.candidate,
            )?))
        }
        portable_endpoint_resolution::Outcome::NotFound(_) => {
            AdapterEndpointLookupOutcome::NotFound
        }
        portable_endpoint_resolution::Outcome::Ambiguous(ambiguous) => {
            AdapterEndpointLookupOutcome::Ambiguous(
                ambiguous
                    .candidates
                    .into_iter()
                    .map(|candidate| map_endpoint_candidate(Some(candidate)))
                    .collect::<Result<_, _>>()?,
            )
        }
        portable_endpoint_resolution::Outcome::Unsupported(_) => {
            AdapterEndpointLookupOutcome::Unsupported
        }
    };
    Ok(AdapterPortableEndpointResolution { requested, outcome })
}

fn map_endpoint_candidate(
    candidate: Option<trakkin_adapter_contract::v1::EndpointLookupCandidate>,
) -> Result<AdapterEndpointLookupCandidate, AdapterProcessError> {
    let candidate = candidate.ok_or(AdapterProcessError::InvalidResponse(
        "endpoint lookup candidate is missing",
    ))?;
    Ok(AdapterEndpointLookupCandidate {
        provider_item: map_provider_item(candidate.provider_item.ok_or(
            AdapterProcessError::InvalidResponse("endpoint lookup provider item is missing"),
        )?)?,
        binding: Some(map_coordinate_binding(candidate.binding.ok_or(
            AdapterProcessError::InvalidResponse("endpoint lookup binding is missing"),
        )?)?),
        evidence: map_lookup_evidence(candidate.evidence)?,
    })
}

fn map_lookup_evidence(
    evidence: Option<trakkin_adapter_contract::v1::LookupEvidence>,
) -> Result<AdapterLookupEvidence, AdapterProcessError> {
    let evidence = evidence.ok_or(AdapterProcessError::InvalidResponse(
        "lookup evidence is missing",
    ))?;
    Ok(AdapterLookupEvidence {
        adapter_revision: evidence.adapter_revision,
        observed_at: map_milliseconds(evidence.observed_time_milliseconds, "lookup time")?,
        expires_at: evidence
            .expires_time_milliseconds
            .map(|value| map_milliseconds(value, "lookup expiry"))
            .transpose()?,
        matched_references: evidence
            .matched_references
            .into_iter()
            .map(|reference| map_domain_portable_reference(Some(reference)))
            .collect::<Result<_, _>>()?,
    })
}

fn map_portable_reference(
    reference: &PortableReference,
) -> trakkin_adapter_contract::v1::PortableReference {
    trakkin_adapter_contract::v1::PortableReference {
        namespace: reference.namespace().to_owned(),
        value: reference.value().to_vec(),
    }
}

fn map_portable_endpoint(
    endpoint: &trakkin_mapping_contract::PortableEndpoint,
) -> ProtoPortableEndpoint {
    ProtoPortableEndpoint {
        reference: Some(trakkin_adapter_contract::v1::PortableReference {
            namespace: endpoint.namespace().to_owned(),
            value: endpoint.value().to_vec(),
        }),
        selector: endpoint
            .selector()
            .map(|selector| selector.as_str().to_owned())
            .unwrap_or_default(),
    }
}

fn map_domain_portable_reference(
    reference: Option<trakkin_adapter_contract::v1::PortableReference>,
) -> Result<PortableReference, AdapterProcessError> {
    let reference = reference.ok_or(AdapterProcessError::InvalidResponse(
        "portable reference is missing",
    ))?;
    PortableReference::new(reference.namespace, reference.value)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid portable reference"))
}

fn map_domain_portable_endpoint(
    endpoint: Option<trakkin_adapter_contract::v1::PortableEndpoint>,
) -> Result<trakkin_mapping_contract::PortableEndpoint, AdapterProcessError> {
    let endpoint = endpoint.ok_or(AdapterProcessError::InvalidResponse(
        "portable endpoint is missing",
    ))?;
    let reference = endpoint
        .reference
        .ok_or(AdapterProcessError::InvalidResponse(
            "portable endpoint reference is missing",
        ))?;
    trakkin_mapping_contract::PortableEndpoint::from_parts(
        &reference.namespace,
        &reference.value,
        Some(&endpoint.selector),
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid portable endpoint"))
}

fn map_adapter_key(key: AdapterKey) -> Result<Key, AdapterProcessError> {
    if key.namespace.is_empty() || key.value.is_empty() {
        return Err(AdapterProcessError::InvalidAssetRequest);
    }
    Ok(Key {
        namespace: key.namespace,
        value: key.value,
    })
}

fn map_milliseconds(
    milliseconds: i64,
    resource: &'static str,
) -> Result<OffsetDateTime, AdapterProcessError> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(milliseconds) * 1_000_000)
        .map_err(|_| AdapterProcessError::InvalidResponse(resource))
}

fn map_catalog_relation(
    relation: trakkin_adapter_contract::v1::CatalogRelation,
) -> Result<CatalogRelation, AdapterProcessError> {
    CatalogRelation::new(
        map_catalog_relation_key(relation.key)?,
        relation
            .parent_key
            .map(|key| map_catalog_relation_key(Some(key)))
            .transpose()?,
        relation
            .provider_item_key
            .map(|key| map_provider_item_key(Some(key)))
            .transpose()?,
        map_domain_term(relation.kind)?,
        relation.order,
        map_attributes(relation.attributes)?,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid catalog relation"))
}

fn map_state_observation(
    observation: trakkin_adapter_contract::v1::StateObservation,
) -> Result<StateObservation, AdapterProcessError> {
    let subject = match observation
        .subject
        .and_then(|subject| subject.subject)
        .ok_or(AdapterProcessError::InvalidResponse(
            "state observation subject is missing",
        ))? {
        subject_reference::Subject::ProviderItemKey(key) => {
            StateSubject::ProviderItem(map_provider_item_key(Some(key))?)
        }
        subject_reference::Subject::CatalogRelationKey(key) => {
            StateSubject::CatalogRelation(map_catalog_relation_key(Some(key))?)
        }
    };
    let field = observation
        .field
        .ok_or(AdapterProcessError::InvalidResponse(
            "state observation field is missing",
        ))?;
    let field = map_domain_state_field(field)?;
    let observed_at = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(observation.observed_time_milliseconds) * 1_000_000,
    )
    .map_err(|_| AdapterProcessError::InvalidResponse("state observation time is invalid"))?;
    match observation
        .observation
        .ok_or(AdapterProcessError::InvalidResponse(
            "state observation operation is missing",
        ))? {
        state_observation::Observation::Value(value) => StateObservation::set(
            subject,
            field,
            map_state_value(Some(value))?,
            observation.provider_revision,
            observed_at,
        ),
        state_observation::Observation::Deletion(_) => {
            StateObservation::delete(subject, field, observation.provider_revision, observed_at)
        }
    }
    .map_err(|_| AdapterProcessError::InvalidResponse("invalid state observation"))
}

fn map_state_subject(subject: StateSubject) -> trakkin_adapter_contract::v1::SubjectReference {
    let subject = match subject {
        StateSubject::ProviderItem(key) => subject_reference::Subject::ProviderItemKey(Key {
            namespace: key.namespace().to_owned(),
            value: key.value().to_vec(),
        }),
        StateSubject::CatalogRelation(key) => subject_reference::Subject::CatalogRelationKey(Key {
            namespace: key.namespace().to_owned(),
            value: key.value().to_vec(),
        }),
    };
    trakkin_adapter_contract::v1::SubjectReference {
        subject: Some(subject),
    }
}

fn map_domain_state_subject(
    subject: trakkin_adapter_contract::v1::SubjectReference,
) -> Result<StateSubject, AdapterProcessError> {
    match subject.subject.ok_or(AdapterProcessError::InvalidResponse(
        "targeted state subject is missing",
    ))? {
        subject_reference::Subject::ProviderItemKey(key) => Ok(StateSubject::ProviderItem(
            map_provider_item_key(Some(key))?,
        )),
        subject_reference::Subject::CatalogRelationKey(key) => Ok(StateSubject::CatalogRelation(
            map_catalog_relation_key(Some(key))?,
        )),
    }
}

fn map_state_field(field: StateField) -> ProtoStateField {
    ProtoStateField {
        field: Some(trakkin_adapter_contract::v1::Term {
            namespace: field.field().namespace().to_owned(),
            name: field.field().name().to_owned(),
        }),
        unit: field.unit().map(|unit| trakkin_adapter_contract::v1::Term {
            namespace: unit.namespace().to_owned(),
            name: unit.name().to_owned(),
        }),
    }
}

fn map_domain_state_value(value: StateValue) -> Value {
    map_value(match value {
        StateValue::Boolean(value) => AdapterValue::Boolean(value),
        StateValue::Integer(value) => AdapterValue::Integer(value),
        StateValue::Decimal(value) => AdapterValue::Decimal(value),
        StateValue::Text(value) => AdapterValue::Text(value),
        StateValue::Bytes(value) => AdapterValue::Bytes(value),
        StateValue::TimestampMilliseconds(value) => AdapterValue::TimestampMilliseconds(value),
    })
}

fn map_source_membership(membership: AdapterSourceMembership) -> ProtoSourceMembership {
    match membership {
        AdapterSourceMembership::Present => ProtoSourceMembership::Present,
        AdapterSourceMembership::Absent => ProtoSourceMembership::Absent,
        AdapterSourceMembership::Unknown => ProtoSourceMembership::Unknown,
    }
}

fn map_domain_state_field(field: ProtoStateField) -> Result<StateField, AdapterProcessError> {
    Ok(StateField::new(
        map_domain_term(field.field)?,
        field
            .unit
            .map(|unit| map_domain_term(Some(unit)))
            .transpose()?,
    ))
}

fn map_targeted_state_outcome(
    response: trakkin_adapter_contract::v1::ReadTargetedStateResponse,
) -> Result<AdapterTargetedStateReadOutcome, AdapterProcessError> {
    match response
        .outcome
        .ok_or(AdapterProcessError::InvalidResponse(
            "targeted state outcome is missing",
        ))? {
        read_targeted_state_response::Outcome::Matched(matched) => {
            let membership = match ProtoSourceMembership::try_from(matched.membership)
                .map_err(|_| AdapterProcessError::InvalidResponse("unknown source membership"))?
            {
                ProtoSourceMembership::Present => AdapterSourceMembership::Present,
                ProtoSourceMembership::Absent => AdapterSourceMembership::Absent,
                ProtoSourceMembership::Unknown => AdapterSourceMembership::Unknown,
                ProtoSourceMembership::Unspecified => {
                    return Err(AdapterProcessError::InvalidResponse(
                        "source membership is unspecified",
                    ));
                }
            };
            let fields = matched
                .fields
                .into_iter()
                .map(|observation| {
                    let presence = match ProtoStatePresence::try_from(observation.presence)
                        .map_err(|_| {
                            AdapterProcessError::InvalidResponse("unknown targeted state presence")
                        })? {
                        ProtoStatePresence::Present => AdapterStatePresence::Present,
                        ProtoStatePresence::Deleted => AdapterStatePresence::Deleted,
                        ProtoStatePresence::Absent => AdapterStatePresence::Absent,
                        ProtoStatePresence::Unknown => AdapterStatePresence::Unknown,
                        ProtoStatePresence::Stale => AdapterStatePresence::Stale,
                        ProtoStatePresence::Unspecified => {
                            return Err(AdapterProcessError::InvalidResponse(
                                "targeted state presence is unspecified",
                            ));
                        }
                    };
                    Ok(AdapterTargetedStateField {
                        field: map_domain_state_field(observation.field.ok_or(
                            AdapterProcessError::InvalidResponse("targeted state field is missing"),
                        )?)?,
                        presence,
                        value: observation
                            .value
                            .map(|value| map_state_value(Some(value)))
                            .transpose()?,
                    })
                })
                .collect::<Result<_, AdapterProcessError>>()?;
            Ok(AdapterTargetedStateReadOutcome::Matched(
                AdapterTargetedStateMatch {
                    membership,
                    fields,
                    provider_revision: matched.provider_revision,
                    observed_at: map_milliseconds(
                        matched.observed_time_milliseconds,
                        "targeted state observation time is invalid",
                    )?,
                    expires_at: matched
                        .expires_time_milliseconds
                        .map(|expires| {
                            map_milliseconds(expires, "targeted state expiry is invalid")
                        })
                        .transpose()?,
                    precondition: (!matched.precondition.is_empty())
                        .then_some(matched.precondition),
                    write_causation: matched.write_causation.map(|causation| {
                        AdapterTargetedStateWriteCausation {
                            idempotency_key: causation.idempotency_key,
                            receipt: causation.receipt,
                            provider_revision: causation.provider_revision,
                        }
                    }),
                },
            ))
        }
        read_targeted_state_response::Outcome::Unsupported(_) => {
            Ok(AdapterTargetedStateReadOutcome::Unsupported)
        }
        read_targeted_state_response::Outcome::NotFound(_) => {
            Ok(AdapterTargetedStateReadOutcome::NotFound)
        }
        read_targeted_state_response::Outcome::Ambiguous(ambiguous) => {
            Ok(AdapterTargetedStateReadOutcome::Ambiguous(
                ambiguous
                    .candidates
                    .into_iter()
                    .map(map_domain_state_subject)
                    .collect::<Result<_, _>>()?,
            ))
        }
        read_targeted_state_response::Outcome::Indeterminate(indeterminate) => {
            Ok(AdapterTargetedStateReadOutcome::Indeterminate(
                map_adapter_error(indeterminate.error.ok_or(
                    AdapterProcessError::InvalidResponse(
                        "targeted state indeterminate error is missing",
                    ),
                )?),
            ))
        }
    }
}

fn map_targeted_state_write_outcome(
    response: trakkin_adapter_contract::v1::WriteTargetedStateResponse,
) -> Result<AdapterTargetedStateWriteOutcome, AdapterProcessError> {
    let status = match ProtoTargetedStateWriteStatus::try_from(response.status)
        .map_err(|_| AdapterProcessError::InvalidResponse("unknown targeted state write status"))?
    {
        ProtoTargetedStateWriteStatus::Applied => AdapterTargetedStateWriteStatus::Applied,
        ProtoTargetedStateWriteStatus::NoOp => AdapterTargetedStateWriteStatus::NoOp,
        ProtoTargetedStateWriteStatus::Rejected => AdapterTargetedStateWriteStatus::Rejected,
        ProtoTargetedStateWriteStatus::Failed => AdapterTargetedStateWriteStatus::Failed,
        ProtoTargetedStateWriteStatus::Indeterminate => {
            AdapterTargetedStateWriteStatus::Indeterminate
        }
        ProtoTargetedStateWriteStatus::Unspecified => {
            return Err(AdapterProcessError::InvalidResponse(
                "targeted state write status is unspecified",
            ));
        }
    };
    let certainty =
        match ProtoTargetedStateWriteCertainty::try_from(response.certainty).map_err(|_| {
            AdapterProcessError::InvalidResponse("unknown targeted state write certainty")
        })? {
            ProtoTargetedStateWriteCertainty::ConfirmedApplied => {
                AdapterTargetedStateWriteCertainty::ConfirmedApplied
            }
            ProtoTargetedStateWriteCertainty::ConfirmedNotApplied => {
                AdapterTargetedStateWriteCertainty::ConfirmedNotApplied
            }
            ProtoTargetedStateWriteCertainty::Unknown => {
                AdapterTargetedStateWriteCertainty::Unknown
            }
            ProtoTargetedStateWriteCertainty::Unspecified => {
                return Err(AdapterProcessError::InvalidResponse(
                    "targeted state write certainty is unspecified",
                ));
            }
        };
    let retry_disposition =
        match ProtoTargetedStateWriteRetryDisposition::try_from(response.retry_disposition)
            .map_err(|_| {
                AdapterProcessError::InvalidResponse(
                    "unknown targeted state write retry disposition",
                )
            })? {
            ProtoTargetedStateWriteRetryDisposition::NotRetryable => {
                AdapterTargetedStateWriteRetryDisposition::NotRetryable
            }
            ProtoTargetedStateWriteRetryDisposition::SafeSameKey => {
                AdapterTargetedStateWriteRetryDisposition::SafeSameKey
            }
            ProtoTargetedStateWriteRetryDisposition::ReconcileFirst => {
                AdapterTargetedStateWriteRetryDisposition::ReconcileFirst
            }
            ProtoTargetedStateWriteRetryDisposition::Unspecified => {
                return Err(AdapterProcessError::InvalidResponse(
                    "targeted state write retry disposition is unspecified",
                ));
            }
        };
    let membership_effect =
        match ProtoTargetedStateMembershipEffect::try_from(response.membership_effect).map_err(
            |_| AdapterProcessError::InvalidResponse("unknown targeted state membership effect"),
        )? {
            ProtoTargetedStateMembershipEffect::Unchanged => {
                AdapterTargetedStateMembershipEffect::Unchanged
            }
            ProtoTargetedStateMembershipEffect::Created => {
                AdapterTargetedStateMembershipEffect::Created
            }
            ProtoTargetedStateMembershipEffect::Unknown => {
                AdapterTargetedStateMembershipEffect::Unknown
            }
            ProtoTargetedStateMembershipEffect::Unspecified => {
                return Err(AdapterProcessError::InvalidResponse(
                    "targeted state membership effect is unspecified",
                ));
            }
        };
    let field_effects = response
        .field_effects
        .into_iter()
        .map(|effect| {
            let effect_kind = match ProtoTargetedStateFieldEffectKind::try_from(effect.effect)
                .map_err(|_| {
                    AdapterProcessError::InvalidResponse("unknown targeted state field effect")
                })? {
                ProtoTargetedStateFieldEffectKind::Set => AdapterTargetedStateFieldEffectKind::Set,
                ProtoTargetedStateFieldEffectKind::Cleared => {
                    AdapterTargetedStateFieldEffectKind::Cleared
                }
                ProtoTargetedStateFieldEffectKind::Unchanged => {
                    AdapterTargetedStateFieldEffectKind::Unchanged
                }
                ProtoTargetedStateFieldEffectKind::Unknown => {
                    AdapterTargetedStateFieldEffectKind::Unknown
                }
                ProtoTargetedStateFieldEffectKind::Unspecified => {
                    return Err(AdapterProcessError::InvalidResponse(
                        "targeted state field effect is unspecified",
                    ));
                }
            };
            Ok(AdapterTargetedStateFieldEffect {
                field: map_domain_state_field(effect.field.ok_or(
                    AdapterProcessError::InvalidResponse(
                        "targeted state write effect field is missing",
                    ),
                )?)?,
                effect: effect_kind,
                value: effect
                    .value
                    .map(|value| map_state_value(Some(value)))
                    .transpose()?,
            })
        })
        .collect::<Result<_, AdapterProcessError>>()?;
    Ok(AdapterTargetedStateWriteOutcome {
        status,
        certainty,
        retry_disposition,
        membership_effect,
        field_effects,
        provider_revision: response.provider_revision,
        successor_precondition: (!response.successor_precondition.is_empty())
            .then_some(response.successor_precondition),
        receipt: response.receipt,
        error: response.error.map(map_adapter_error),
    })
}

fn map_attributes(
    attributes: Vec<trakkin_adapter_contract::v1::Attribute>,
) -> Result<serde_json::Value, AdapterProcessError> {
    let entries = attributes
        .into_iter()
        .map(|attribute| {
            let term = map_domain_term(attribute.term)?;
            Ok(json!({
                "term": {
                    "namespace": term.namespace(),
                    "name": term.name(),
                },
                "value": encode_adapter_value(map_contract_value(attribute.value)?),
            }))
        })
        .collect::<Result<Vec<_>, AdapterProcessError>>()?;
    Ok(json!({"entries": entries}))
}

fn encode_adapter_value(value: AdapterValue) -> serde_json::Value {
    match value {
        AdapterValue::Boolean(value) => json!({"kind": "boolean", "value": value}),
        AdapterValue::Integer(value) => json!({"kind": "integer", "value": value}),
        AdapterValue::Decimal(value) => json!({"kind": "decimal", "value": value}),
        AdapterValue::Text(value) => json!({"kind": "text", "value": value}),
        AdapterValue::Bytes(value) => json!({"kind": "bytes", "value": hex::encode(value)}),
        AdapterValue::TimestampMilliseconds(value) => {
            json!({"kind": "timestamp_milliseconds", "value": value})
        }
    }
}

fn map_provider_item_key(key: Option<Key>) -> Result<ProviderItemKey, AdapterProcessError> {
    let key = key.ok_or(AdapterProcessError::InvalidResponse(
        "provider item key is missing",
    ))?;
    ProviderItemKey::new(key.namespace, key.value)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid provider item key"))
}

fn map_catalog_relation_key(key: Option<Key>) -> Result<CatalogRelationKey, AdapterProcessError> {
    let key = key.ok_or(AdapterProcessError::InvalidResponse(
        "catalog relation key is missing",
    ))?;
    CatalogRelationKey::new(key.namespace, key.value)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid catalog relation key"))
}

fn map_binary_asset_key(key: Option<Key>) -> Result<BinaryAssetKey, AdapterProcessError> {
    let key = key.ok_or(AdapterProcessError::InvalidResponse(
        "binary asset key is missing",
    ))?;
    BinaryAssetKey::new(key.namespace, key.value)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid binary asset key"))
}

fn map_domain_term(
    term: Option<trakkin_adapter_contract::v1::Term>,
) -> Result<Term, AdapterProcessError> {
    let term = term.ok_or(AdapterProcessError::InvalidResponse("term is missing"))?;
    Term::new(term.namespace, term.name)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid term"))
}

fn map_state_value(value: Option<Value>) -> Result<StateValue, AdapterProcessError> {
    match map_contract_value(value)? {
        AdapterValue::Boolean(value) => Ok(StateValue::Boolean(value)),
        AdapterValue::Integer(value) => Ok(StateValue::Integer(value)),
        AdapterValue::Decimal(value) => Ok(StateValue::Decimal(value)),
        AdapterValue::Text(value) => Ok(StateValue::Text(value)),
        AdapterValue::Bytes(value) => Ok(StateValue::Bytes(value)),
        AdapterValue::TimestampMilliseconds(value) => Ok(StateValue::TimestampMilliseconds(value)),
    }
}

fn map_settings(
    settings: Vec<crate::application::adapter_runtime::AdapterSetting>,
) -> Vec<ConfigurationValue> {
    settings
        .into_iter()
        .map(|setting| ConfigurationValue {
            key: setting.key,
            value: Some(map_value(setting.value)),
        })
        .collect()
}

fn map_secrets(
    secrets: Vec<crate::application::adapter_runtime::AdapterSecret>,
) -> Vec<SecretValue> {
    secrets
        .into_iter()
        .map(|secret| SecretValue {
            key: secret.key,
            value: secret.value,
        })
        .collect()
}

fn map_contract_value(value: Option<Value>) -> Result<AdapterValue, AdapterProcessError> {
    match value.and_then(|value| value.value) {
        Some(value::Value::Boolean(value)) => Ok(AdapterValue::Boolean(value)),
        Some(value::Value::Integer(value)) => Ok(AdapterValue::Integer(value)),
        Some(value::Value::Decimal(value)) => Ok(AdapterValue::Decimal(value.value)),
        Some(value::Value::Text(value)) => Ok(AdapterValue::Text(value)),
        Some(value::Value::Bytes(value)) => Ok(AdapterValue::Bytes(value)),
        Some(value::Value::Timestamp(value)) => {
            Ok(AdapterValue::TimestampMilliseconds(value.milliseconds))
        }
        None => Err(AdapterProcessError::InvalidResponse("value is missing")),
    }
}

fn map_configuration_fields(
    fields: Vec<ConfigurationField>,
) -> Result<Vec<AdapterConfigurationField>, AdapterProcessError> {
    let mut keys = HashSet::new();
    fields
        .into_iter()
        .map(|field| {
            if field.key.is_empty()
                || field.display_name.is_empty()
                || !keys.insert(field.key.clone())
            {
                return Err(AdapterProcessError::InvalidResponse(
                    "invalid configuration field",
                ));
            }
            Ok(AdapterConfigurationField {
                key: field.key,
                display_name: field.display_name,
                value_kind: map_value_kind(field.value_kind)?,
                required: field.required,
                secret: field.secret,
                options: field
                    .options
                    .into_iter()
                    .map(|option| {
                        if option.display_name.is_empty() {
                            return Err(AdapterProcessError::InvalidResponse(
                                "invalid configuration option",
                            ));
                        }
                        Ok(AdapterConfigurationOption {
                            value: map_contract_value(option.value)?,
                            display_name: option.display_name,
                        })
                    })
                    .collect::<Result<_, _>>()?,
                default_value: field
                    .default_value
                    .map(|value| map_contract_value(Some(value)))
                    .transpose()?,
                placeholder: field.placeholder,
            })
        })
        .collect()
}

fn map_start_authentication_response(
    response: StartAuthenticationResponse,
) -> Result<AdapterAuthenticationStep, AdapterProcessError> {
    validation::start_authentication_response(&response)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid authentication step"))?;
    map_authentication_response(
        response.authentication_id,
        response.status,
        response.prompt,
        response.accounts,
        response.secret_patches,
        response.retry_after.map(|retry_after| AdapterRetryAfter {
            seconds: retry_after.seconds,
            nanoseconds: retry_after.nanos,
        }),
        response.error,
    )
}

fn map_continue_authentication_response(
    response: ContinueAuthenticationResponse,
) -> Result<AdapterAuthenticationStep, AdapterProcessError> {
    validation::continue_authentication_response(&response)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid authentication step"))?;
    map_authentication_response(
        response.authentication_id,
        response.status,
        response.prompt,
        response.accounts,
        response.secret_patches,
        response.retry_after.map(|retry_after| AdapterRetryAfter {
            seconds: retry_after.seconds,
            nanoseconds: retry_after.nanos,
        }),
        response.error,
    )
}

fn map_authentication_response(
    authentication_id: String,
    status: i32,
    prompt: Option<ProtoAuthenticationPrompt>,
    accounts: Vec<ProtoAccountSnapshot>,
    secret_patches: Vec<SecretPatch>,
    retry_after: Option<AdapterRetryAfter>,
    error: Option<AdapterError>,
) -> Result<AdapterAuthenticationStep, AdapterProcessError> {
    let status = match ProtoAuthenticationStatus::try_from(status)
        .map_err(|_| AdapterProcessError::InvalidResponse("unknown authentication status"))?
    {
        ProtoAuthenticationStatus::Waiting => AdapterAuthenticationStatus::Waiting,
        ProtoAuthenticationStatus::InputRequired => AdapterAuthenticationStatus::InputRequired,
        ProtoAuthenticationStatus::Completed => AdapterAuthenticationStatus::Completed,
        ProtoAuthenticationStatus::Cancelled => AdapterAuthenticationStatus::Cancelled,
        ProtoAuthenticationStatus::Expired => AdapterAuthenticationStatus::Expired,
        ProtoAuthenticationStatus::Failed => AdapterAuthenticationStatus::Failed,
        ProtoAuthenticationStatus::Unspecified => {
            return Err(AdapterProcessError::InvalidResponse(
                "authentication status is unspecified",
            ));
        }
    };
    Ok(AdapterAuthenticationStep {
        authentication_id,
        status,
        prompt: prompt
            .map(
                |prompt| -> Result<AdapterAuthenticationPrompt, AdapterProcessError> {
                    Ok(AdapterAuthenticationPrompt {
                        message: prompt.message,
                        verification_url: prompt.verification_url,
                        user_code: prompt.user_code,
                        fields: map_configuration_fields(prompt.fields)?,
                    })
                },
            )
            .transpose()?,
        accounts: accounts
            .into_iter()
            .map(|account| {
                Ok(AdapterAccount {
                    key: map_key(account.key)?,
                    display_name: account.display_name,
                })
            })
            .collect::<Result<_, AdapterProcessError>>()?,
        secret_patches: map_secret_patches(secret_patches)?,
        retry_after,
        error: error.map(map_adapter_error),
    })
}

fn validate_adapter_error(error: &AdapterError) -> Result<(), AdapterProcessError> {
    validation::adapter_error(error)
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid adapter error"))
}

fn map_field_problem(problem: trakkin_adapter_contract::v1::FieldProblem) -> AdapterFieldProblem {
    AdapterFieldProblem {
        path: problem.path,
        code: problem.code,
        message: problem.message,
    }
}

fn map_adapter_error(error: AdapterError) -> AdapterCallError {
    AdapterCallError {
        code: error.code,
        safe_message: error.safe_message,
        retryable: error.retryable,
        retry_after: error.retry_after.map(|retry_after| AdapterRetryAfter {
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

fn map_key(key: Option<Key>) -> Result<AdapterKey, AdapterProcessError> {
    let key = key.ok_or(AdapterProcessError::InvalidResponse("key is missing"))?;
    Ok(AdapterKey {
        namespace: key.namespace,
        value: key.value,
    })
}

fn map_term(
    term: Option<trakkin_adapter_contract::v1::Term>,
) -> Result<AdapterTerm, AdapterProcessError> {
    let term = term.ok_or(AdapterProcessError::InvalidResponse("term is missing"))?;
    Ok(AdapterTerm {
        namespace: term.namespace,
        name: term.name,
    })
}

fn map_connection_capabilities(
    capabilities: Option<trakkin_adapter_contract::v1::ConnectionCapabilities>,
) -> AdapterConnectionCapabilities {
    let capabilities = capabilities.unwrap_or_default();
    AdapterConnectionCapabilities {
        reference_lookup: capabilities
            .reference_lookup
            .map(|lookup| AdapterLookupCapability {
                reference_namespaces: lookup.reference_namespaces,
                maximum_batch_size: lookup.maximum_batch_size,
            }),
        endpoint_lookup: capabilities.endpoint_lookup.map(|lookup| {
            AdapterEndpointLookupCapability {
                reference_namespaces: lookup.reference_namespaces,
                coordinate_ids: lookup.coordinate_ids,
                maximum_batch_size: lookup.maximum_batch_size,
                maximum_response_bytes: lookup.maximum_response_bytes,
            }
        }),
    }
}

fn map_source(
    source: trakkin_adapter_contract::v1::SourceSnapshot,
) -> Result<AdapterSource, AdapterProcessError> {
    let availability = match SourceAvailability::try_from(source.availability)
        .map_err(|_| AdapterProcessError::InvalidResponse("unknown source availability"))?
    {
        SourceAvailability::Available => AdapterSourceAvailability::Available,
        SourceAvailability::Unavailable => AdapterSourceAvailability::Unavailable,
        SourceAvailability::Removed => AdapterSourceAvailability::Removed,
        SourceAvailability::Unspecified => {
            return Err(AdapterProcessError::InvalidResponse(
                "source availability is unspecified",
            ));
        }
    };
    let capabilities = source
        .capabilities
        .ok_or(AdapterProcessError::InvalidResponse(
            "source capabilities are missing",
        ))?;
    Ok(AdapterSource {
        key: map_key(source.key)?,
        account_key: map_key(source.account_key)?,
        display_name: source.display_name,
        kind: map_term(source.kind)?,
        availability,
        capabilities: AdapterSourceCapabilities {
            catalog: capabilities.catalog.map(map_read_capability),
            state: capabilities.state.map(map_read_capability),
            state_fields: capabilities
                .state_fields
                .into_iter()
                .map(|field| {
                    Ok(AdapterStateField {
                        field: map_term(field.field)?,
                        unit: field.unit.map(|unit| AdapterTerm {
                            namespace: unit.namespace,
                            name: unit.name,
                        }),
                        value_kind: map_value_kind(field.value_kind)?,
                        rating_scale: field.rating_scale.map(|scale| AdapterRatingScale {
                            minimum: scale.minimum,
                            maximum: scale.maximum,
                            step: scale.step,
                        }),
                        numeric_range: field.numeric_range.map(|range| AdapterStateNumericRange {
                            minimum: range.minimum,
                            maximum: range.maximum,
                            step: range.step,
                        }),
                        quantizer: match StateFieldQuantizer::try_from(field.quantizer).map_err(
                            |_| {
                                AdapterProcessError::InvalidResponse(
                                    "unknown state field quantizer",
                                )
                            },
                        )? {
                            StateFieldQuantizer::Unspecified => None,
                            StateFieldQuantizer::Exact => Some(Quantizer::Exact),
                            StateFieldQuantizer::Nearest => Some(Quantizer::Nearest),
                            StateFieldQuantizer::Floor => Some(Quantizer::Floor),
                            StateFieldQuantizer::Ceiling => Some(Quantizer::Ceiling),
                        },
                    })
                })
                .collect::<Result<_, AdapterProcessError>>()?,
            assets: capabilities.assets.map(|assets| AdapterAssetCapability {
                maximum_bytes: assets.maximum_bytes,
                content_types: assets.content_types,
            }),
            coordinate_ids: capabilities
                .coordinates
                .map(|coordinates| coordinates.coordinate_ids)
                .unwrap_or_default(),
            targeted_state_read: capabilities.targeted_state_read.map(|read| {
                AdapterTargetedStateReadCapability {
                    maximum_fields: read.maximum_fields,
                    maximum_response_bytes: read.maximum_response_bytes,
                }
            }),
            targeted_state_write: capabilities
                .targeted_state_write
                .map(map_targeted_state_write_capability)
                .transpose()?,
        },
    })
}

fn map_targeted_state_write_capability(
    capability: ProtoTargetedStateWriteCapability,
) -> Result<AdapterTargetedStateWriteCapability, AdapterProcessError> {
    let precondition_mode =
        match ProtoTargetedStateWritePreconditionMode::try_from(capability.precondition_mode)
            .map_err(|_| {
                AdapterProcessError::InvalidResponse(
                    "unknown targeted state write precondition mode",
                )
            })? {
            ProtoTargetedStateWritePreconditionMode::Unspecified => None,
            ProtoTargetedStateWritePreconditionMode::ProviderToken => {
                Some(AdapterTargetedStateWritePreconditionMode::ProviderToken)
            }
            ProtoTargetedStateWritePreconditionMode::HostRecheckOnly => {
                Some(AdapterTargetedStateWritePreconditionMode::HostRecheckOnly)
            }
        };
    let idempotency_mode =
        match ProtoTargetedStateWriteIdempotencyMode::try_from(capability.idempotency_mode)
            .map_err(|_| {
                AdapterProcessError::InvalidResponse(
                    "unknown targeted state write idempotency mode",
                )
            })? {
            ProtoTargetedStateWriteIdempotencyMode::Unspecified => None,
            ProtoTargetedStateWriteIdempotencyMode::NotSupported => {
                Some(AdapterTargetedStateWriteIdempotencyMode::NotSupported)
            }
            ProtoTargetedStateWriteIdempotencyMode::StableKey => {
                Some(AdapterTargetedStateWriteIdempotencyMode::StableKey)
            }
        };
    Ok(AdapterTargetedStateWriteCapability {
        fields: capability
            .fields
            .into_iter()
            .map(|field| {
                Ok(AdapterTargetedStateFieldWriteCapability {
                    field: map_domain_state_field(field.field.ok_or(
                        AdapterProcessError::InvalidResponse(
                            "targeted state write capability field is missing",
                        ),
                    )?)?,
                    set_supported: field.set_supported,
                    clear_supported: field.clear_supported,
                })
            })
            .collect::<Result<_, AdapterProcessError>>()?,
        may_create_source_membership: capability.may_create_source_membership,
        precondition_mode,
        idempotency_mode,
        maximum_fields: capability.maximum_fields,
        maximum_request_bytes: capability.maximum_request_bytes,
        maximum_response_bytes: capability.maximum_response_bytes,
        maximum_receipt_bytes: capability.maximum_receipt_bytes,
    })
}

fn map_targeted_state_write_capability_to_proto(
    capability: &AdapterTargetedStateWriteCapability,
) -> ProtoTargetedStateWriteCapability {
    ProtoTargetedStateWriteCapability {
        fields: capability
            .fields
            .iter()
            .map(
                |field| trakkin_adapter_contract::v1::TargetedStateFieldWriteCapability {
                    field: Some(map_state_field(field.field.clone())),
                    set_supported: field.set_supported,
                    clear_supported: field.clear_supported,
                },
            )
            .collect(),
        may_create_source_membership: capability.may_create_source_membership,
        precondition_mode: match capability.precondition_mode {
            None => ProtoTargetedStateWritePreconditionMode::Unspecified,
            Some(AdapterTargetedStateWritePreconditionMode::ProviderToken) => {
                ProtoTargetedStateWritePreconditionMode::ProviderToken
            }
            Some(AdapterTargetedStateWritePreconditionMode::HostRecheckOnly) => {
                ProtoTargetedStateWritePreconditionMode::HostRecheckOnly
            }
        } as i32,
        idempotency_mode: match capability.idempotency_mode {
            None => ProtoTargetedStateWriteIdempotencyMode::Unspecified,
            Some(AdapterTargetedStateWriteIdempotencyMode::NotSupported) => {
                ProtoTargetedStateWriteIdempotencyMode::NotSupported
            }
            Some(AdapterTargetedStateWriteIdempotencyMode::StableKey) => {
                ProtoTargetedStateWriteIdempotencyMode::StableKey
            }
        } as i32,
        maximum_fields: capability.maximum_fields,
        maximum_request_bytes: capability.maximum_request_bytes,
        maximum_response_bytes: capability.maximum_response_bytes,
        maximum_receipt_bytes: capability.maximum_receipt_bytes,
    }
}

fn map_read_capability(
    capability: trakkin_adapter_contract::v1::ReadCapability,
) -> AdapterReadCapability {
    AdapterReadCapability {
        full: capability.full,
        incremental: capability.incremental,
    }
}

fn map_value_kind(value: i32) -> Result<AdapterValueKind, AdapterProcessError> {
    match trakkin_adapter_contract::v1::ConfigurationValueKind::try_from(value)
        .map_err(|_| AdapterProcessError::InvalidResponse("unknown value kind"))?
    {
        trakkin_adapter_contract::v1::ConfigurationValueKind::Text => Ok(AdapterValueKind::Text),
        trakkin_adapter_contract::v1::ConfigurationValueKind::Integer => {
            Ok(AdapterValueKind::Integer)
        }
        trakkin_adapter_contract::v1::ConfigurationValueKind::Decimal => {
            Ok(AdapterValueKind::Decimal)
        }
        trakkin_adapter_contract::v1::ConfigurationValueKind::Boolean => {
            Ok(AdapterValueKind::Boolean)
        }
        trakkin_adapter_contract::v1::ConfigurationValueKind::Timestamp => {
            Ok(AdapterValueKind::Timestamp)
        }
        trakkin_adapter_contract::v1::ConfigurationValueKind::Bytes => Ok(AdapterValueKind::Bytes),
        trakkin_adapter_contract::v1::ConfigurationValueKind::Unspecified => Err(
            AdapterProcessError::InvalidResponse("value kind is unspecified"),
        ),
    }
}

fn map_secret_patches(
    patches: Vec<SecretPatch>,
) -> Result<Vec<AdapterSecretPatch>, AdapterProcessError> {
    patches
        .into_iter()
        .map(|patch| {
            let value = match patch.patch {
                Some(secret_patch::Patch::Set(value)) => Some(value),
                Some(secret_patch::Patch::Remove(_)) => None,
                None => {
                    return Err(AdapterProcessError::InvalidResponse(
                        "secret patch operation is missing",
                    ));
                }
            };
            Ok(AdapterSecretPatch {
                key: patch.key,
                value,
            })
        })
        .collect()
}

async fn force_stop(child: &mut Child) {
    if child.id().is_some() {
        let _ = child.kill().await;
    }
    let _ = child.wait().await;
}

#[derive(Debug, thiserror::Error)]
pub enum AdapterProcessError {
    #[error("adapter executable could not be started: {0}")]
    Io(#[from] std::io::Error),
    #[error("adapter bootstrap message is invalid: {0}")]
    BootstrapJson(#[from] serde_json::Error),
    #[error("adapter launch token generation failed: {0}")]
    Random(#[from] getrandom::Error),
    #[error("adapter process instance ID is invalid")]
    InvalidProcessInstanceId,
    #[error("adapter child {0} pipe is unavailable")]
    MissingChildPipe(&'static str),
    #[error("adapter did not become ready before the deadline")]
    ReadyTimeout,
    #[error("adapter exited without a ready message")]
    MissingReadyMessage,
    #[error("adapter ready message exceeds the size limit")]
    ReadyMessageTooLarge,
    #[error("adapter ready message failed validation")]
    InvalidReadyMessage,
    #[error("adapter launch token is invalid")]
    InvalidLaunchToken,
    #[error("adapter RPC {0} exceeded its deadline")]
    RpcTimeout(&'static str),
    #[error("adapter transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("adapter RPC failed with status {0}")]
    Rpc(tonic::Code),
    #[error("adapter handshake failed validation")]
    InvalidHandshake,
    #[error("adapter response failed validation: {0}")]
    InvalidResponse(&'static str),
    #[error("adapter rejected the request: {0:?}")]
    Adapter(AdapterCallError),
    #[error("adapter connection is not open")]
    ConnectionNotOpen,
    #[error("adapter operation ID is invalid")]
    InvalidOperationId,
    #[error("adapter read request is invalid")]
    InvalidReadRequest,
    #[error("adapter portable-reference lookup request is invalid")]
    InvalidLookupRequest,
    #[error("adapter portable-endpoint lookup request is invalid")]
    InvalidEndpointLookupRequest,
    #[error("adapter targeted state-read request is invalid")]
    InvalidTargetedStateReadRequest,
    #[error("adapter targeted state-write request is invalid")]
    InvalidTargetedStateWriteRequest,
    #[error("adapter binary-asset request is invalid")]
    InvalidAssetRequest,
    #[error("adapter authentication request is invalid")]
    InvalidAuthenticationRequest,
    #[error("adapter exited unsuccessfully with status {0:?}")]
    UnexpectedExit(Option<i32>),
}

impl From<tonic::Status> for AdapterProcessError {
    fn from(status: tonic::Status) -> Self {
        Self::Rpc(status.code())
    }
}
