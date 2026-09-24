use trakkin_provider_sdk::{
    v1::{
        CancelAuthenticationRequest, ContinueAuthenticationRequest, DescribeConnectionRequest,
        DiscoverSourcesRequest, HealthRequest, Key, ListAuthenticationMethodsRequest,
        LookupPortableReferencesRequest, OpenConnectionRequest,
        PortableEndpoint as ProtoPortableEndpoint, ReadAssetRequest,
        ReadCatalogResponse as ProtoCatalogReadEvent, ReadStateResponse as ProtoStateReadEvent,
        ReadTargetedStateRequest, ResolvePortableEndpointsRequest, StartAuthenticationRequest,
        TargetedStateClear, TargetedStateReadCapability as ProtoTargetedStateReadCapability,
        TargetedStateWriteIntent, ValidateConnectionRequest, WriteTargetedStateRequest,
        cancel_authentication_response, cancel_operation_response, describe_connection_response,
        discover_sources_response, list_authentication_methods_response,
        lookup_portable_references_response, open_connection_response, read_asset_response,
        resolve_portable_endpoints_response, targeted_state_write_intent,
        validate_connection_response,
    },
    validation,
};

use crate::{
    application::adapter_runtime::{
        AdapterAuthenticationMethod, AdapterAuthenticationStep, AdapterBinaryAsset,
        AdapterCallError, AdapterConnectionDescription, AdapterConnectionValidation,
        AdapterIdentity, AdapterOperationResult, AdapterPortableEndpointResolution,
        AdapterPortableReferenceLookup, AdapterReadEvent, AdapterReadStream, AdapterRetirement,
        AdapterSession, AdapterTargetedStateReadOutcome, AdapterTargetedStateWriteOperation,
        AdapterTargetedStateWriteOutcome, ContinueAdapterAuthentication, DiscoveredAdapterSources,
        LookupAdapterPortableReferences, OpenAdapterConnection, OpenedAdapterConnection,
        ReadAdapterAsset, ReadAdapterSource, ReadAdapterTargetedState,
        ResolveAdapterPortableEndpoints, StartAdapterAuthentication, ValidateAdapterConnection,
        WriteAdapterTargetedState,
    },
    domain::acquisition::{CatalogBatch, StateBatch},
};

use super::{
    AdapterProcessError,
    codec::{
        map_adapter_error, map_adapter_key, map_catalog_event,
        map_continue_authentication_response, map_domain_state_value, map_endpoint_result,
        map_lookup_result, map_portable_endpoint, map_portable_reference, map_read_catalog_request,
        map_read_state_request, map_secrets, map_settings, map_source_membership,
        map_start_authentication_response, map_state_event, map_state_field, map_state_subject,
        map_targeted_state_outcome, map_targeted_state_write_capability_to_proto,
        map_targeted_state_write_outcome,
    },
    process::{ProviderProcess, await_rpc},
};

macro_rules! call_rpc {
    ($process:expr, $method:ident, $operation:literal, $message:expr) => {{
        let request = $process.request($message);
        let deadline = $process.rpc_timeout();
        let provider_id = $process.identity().adapter_key.clone();
        let process_instance_id = $process.identity().process_instance_id.clone();
        await_rpc(
            deadline,
            $operation,
            &provider_id,
            &process_instance_id,
            $process.client().$method(request),
        )
        .await?
    }};
}

macro_rules! operation_result {
    ($response:expr, $outcome:ident) => {
        match $response
            .outcome
            .expect("validated adapter operation outcome")
        {
            $outcome::Outcome::Result(result) => result,
            $outcome::Outcome::Error(error) => return Ok(Err(map_adapter_error(error))),
        }
    };
}

pub struct AdapterProcess {
    process: ProviderProcess,
    account_keys: Vec<Key>,
}

impl AdapterProcess {
    pub(super) fn new(process: ProviderProcess) -> Self {
        Self {
            process,
            account_keys: Vec::new(),
        }
    }
}

pub struct AdapterCatalogReadStream {
    inner: tonic::Streaming<ProtoCatalogReadEvent>,
    validator: validation::CatalogStreamValidator,
    operation_id: Vec<u8>,
    finished: bool,
}

impl AdapterReadStream<CatalogBatch> for AdapterCatalogReadStream {
    type Error = AdapterProcessError;

    async fn next_event(&mut self) -> Result<Option<AdapterReadEvent<CatalogBatch>>, Self::Error> {
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

    async fn next_event(&mut self) -> Result<Option<AdapterReadEvent<StateBatch>>, Self::Error> {
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
    }
}

impl AdapterSession for AdapterProcess {
    type Error = AdapterProcessError;
    type CatalogReadStream = AdapterCatalogReadStream;
    type StateReadStream = AdapterStateReadStream;

    fn identity(&self) -> &AdapterIdentity {
        self.process.identity()
    }

    async fn health(&mut self) -> Result<trakkin_provider_sdk::v1::HealthResponse, Self::Error> {
        let response = call_rpc!(self.process, health, "Health", HealthRequest {});
        validation::health_response(&response)
            .map_err(|_| AdapterProcessError::InvalidResponse("invalid health response"))?;
        Ok(response)
    }

    async fn describe_connection(
        &mut self,
    ) -> Result<Result<AdapterConnectionDescription, AdapterCallError>, Self::Error> {
        let response = call_rpc!(
            self.process,
            describe_connection,
            "DescribeConnection",
            DescribeConnectionRequest {}
        );
        validation::describe_connection_response(&response).map_err(|_| {
            AdapterProcessError::InvalidResponse("invalid connection description response")
        })?;
        let result = operation_result!(response, describe_connection_response);
        Ok(Ok(result))
    }

    async fn validate_connection(
        &mut self,
        command: ValidateAdapterConnection,
    ) -> Result<Result<AdapterConnectionValidation, AdapterCallError>, Self::Error> {
        let response = call_rpc!(
            self.process,
            validate_connection,
            "ValidateConnection",
            ValidateConnectionRequest {
                settings: map_settings(command.settings),
                secrets: map_secrets(command.secrets),
            }
        );
        validation::validate_connection_response(&response).map_err(|_| {
            AdapterProcessError::InvalidResponse("invalid connection validation response")
        })?;
        let result = operation_result!(response, validate_connection_response);
        Ok(Ok(result))
    }

    async fn list_authentication_methods(
        &mut self,
    ) -> Result<Result<Vec<AdapterAuthenticationMethod>, AdapterCallError>, Self::Error> {
        let response = call_rpc!(
            self.process,
            list_authentication_methods,
            "ListAuthenticationMethods",
            ListAuthenticationMethodsRequest {}
        );
        validation::list_authentication_methods_response(&response).map_err(|_| {
            AdapterProcessError::InvalidResponse("invalid authentication methods response")
        })?;
        let result = operation_result!(response, list_authentication_methods_response);
        Ok(Ok(result.methods))
    }

    async fn start_authentication(
        &mut self,
        command: StartAdapterAuthentication,
    ) -> Result<AdapterOperationResult<AdapterAuthenticationStep>, Self::Error> {
        if command.method.is_empty() {
            return Err(AdapterProcessError::InvalidAuthenticationRequest);
        }
        let response = call_rpc!(
            self.process,
            start_authentication,
            "StartAuthentication",
            StartAuthenticationRequest {
                method: command.method,
                values: map_settings(command.values),
            }
        );
        map_start_authentication_response(response)
    }

    async fn continue_authentication(
        &mut self,
        command: ContinueAdapterAuthentication,
    ) -> Result<AdapterOperationResult<AdapterAuthenticationStep>, Self::Error> {
        if command.authentication_id.is_empty() {
            return Err(AdapterProcessError::InvalidAuthenticationRequest);
        }
        let response = call_rpc!(
            self.process,
            continue_authentication,
            "ContinueAuthentication",
            ContinueAuthenticationRequest {
                authentication_id: command.authentication_id,
                values: map_settings(command.values),
            }
        );
        map_continue_authentication_response(response)
    }

    async fn cancel_authentication(
        &mut self,
        authentication_id: String,
    ) -> Result<Result<(), AdapterCallError>, Self::Error> {
        if authentication_id.is_empty() {
            return Err(AdapterProcessError::InvalidAuthenticationRequest);
        }
        let response = call_rpc!(
            self.process,
            cancel_authentication,
            "CancelAuthentication",
            CancelAuthenticationRequest { authentication_id }
        );
        validation::cancel_authentication_response(&response).map_err(|_| {
            AdapterProcessError::InvalidResponse("invalid authentication cancellation response")
        })?;
        match response
            .outcome
            .expect("validated authentication cancellation outcome")
        {
            cancel_authentication_response::Outcome::Result(_) => Ok(Ok(())),
            cancel_authentication_response::Outcome::Error(error) => {
                Ok(Err(map_adapter_error(error)))
            }
        }
    }

    async fn open_connection(
        &mut self,
        command: OpenAdapterConnection,
    ) -> Result<Result<OpenedAdapterConnection, AdapterCallError>, Self::Error> {
        let response = call_rpc!(
            self.process,
            open_connection,
            "OpenConnection",
            OpenConnectionRequest {
                settings: map_settings(command.settings),
                secrets: map_secrets(command.secrets),
            }
        );
        validation::open_connection_response(&response)
            .map_err(|_| AdapterProcessError::InvalidResponse("invalid open response"))?;
        let result = operation_result!(response, open_connection_response);
        self.account_keys = result
            .accounts
            .iter()
            .map(|account| account.key.clone().expect("validated account key"))
            .collect();
        Ok(Ok(result))
    }

    async fn discover_sources(
        &mut self,
    ) -> Result<Result<DiscoveredAdapterSources, AdapterCallError>, Self::Error> {
        if self.account_keys.is_empty() {
            return Err(AdapterProcessError::ConnectionNotOpen);
        }
        let response = call_rpc!(
            self.process,
            discover_sources,
            "DiscoverSources",
            DiscoverSourcesRequest {}
        );
        validation::discover_sources_response(&self.account_keys, &response)
            .map_err(|_| AdapterProcessError::InvalidResponse("invalid discovery response"))?;
        let result = operation_result!(response, discover_sources_response);
        Ok(Ok(result))
    }

    async fn read_catalog(
        &mut self,
        command: ReadAdapterSource,
    ) -> Result<Self::CatalogReadStream, Self::Error> {
        let operation_id = command.operation_id.clone();
        let stream = call_rpc!(
            self.process,
            read_catalog,
            "ReadCatalog",
            map_read_catalog_request(command)?
        );
        Ok(AdapterCatalogReadStream {
            inner: stream,
            validator: validation::CatalogStreamValidator::default(),
            operation_id,
            finished: false,
        })
    }

    async fn read_state(
        &mut self,
        command: ReadAdapterSource,
    ) -> Result<Self::StateReadStream, Self::Error> {
        let operation_id = command.operation_id.clone();
        let stream = call_rpc!(
            self.process,
            read_state,
            "ReadState",
            map_read_state_request(command)?
        );
        Ok(AdapterStateReadStream {
            inner: stream,
            validator: validation::StateStreamValidator::default(),
            operation_id,
            finished: false,
        })
    }

    async fn read_targeted_state(
        &mut self,
        command: ReadAdapterTargetedState,
    ) -> Result<AdapterTargetedStateReadOutcome, Self::Error> {
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
        let response = call_rpc!(
            self.process,
            read_targeted_state,
            "ReadTargetedState",
            request
        );
        validation::read_targeted_state_response(&request_message, &response)
            .map_err(|_| AdapterProcessError::InvalidResponse("invalid targeted state response"))?;
        map_targeted_state_outcome(response)
    }

    async fn write_targeted_state(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> Result<AdapterTargetedStateWriteOutcome, Self::Error> {
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
        let response = call_rpc!(
            self.process,
            write_targeted_state,
            "WriteTargetedState",
            request.clone()
        );
        if validation::targeted_state_write_response(&request, &capability, &response).is_err() {
            tracing::warn!(
                provider.id = %self.process.identity().adapter_key,
                process.instance.id = %self.process.identity().process_instance_id,
                provider.operation = "WriteTargetedState",
                retirement.mode = "forced",
                retirement.reason = "invalid_response",
                "provider was forcefully stopped"
            );
            self.process.force_stop().await;
            return Err(AdapterProcessError::InvalidResponse(
                "invalid targeted state write response",
            ));
        }
        map_targeted_state_write_outcome(response)
    }

    async fn lookup_portable_references(
        &mut self,
        command: LookupAdapterPortableReferences,
    ) -> Result<Result<Vec<AdapterPortableReferenceLookup>, AdapterCallError>, Self::Error> {
        if command.operation_id.is_empty()
            || command.source_key.namespace.is_empty()
            || command.source_key.value.is_empty()
            || command.references.is_empty()
        {
            return Err(AdapterProcessError::InvalidLookupRequest);
        }
        let requested = command
            .references
            .iter()
            .map(map_portable_reference)
            .collect::<Vec<_>>();
        let response = call_rpc!(
            self.process,
            lookup_portable_references,
            "LookupPortableReferences",
            LookupPortableReferencesRequest {
                operation_id: command.operation_id,
                references: requested.clone(),
                source_key: Some(Key {
                    namespace: command.source_key.namespace,
                    value: command.source_key.value,
                }),
            }
        );
        validation::lookup_response(&requested, &response)
            .map_err(|_| AdapterProcessError::InvalidResponse("invalid lookup response"))?;
        let result = operation_result!(response, lookup_portable_references_response);
        Ok(Ok(result
            .results
            .into_iter()
            .map(map_lookup_result)
            .collect::<Result<_, _>>()?))
    }

    async fn resolve_portable_endpoints(
        &mut self,
        command: ResolveAdapterPortableEndpoints,
    ) -> Result<Result<Vec<AdapterPortableEndpointResolution>, AdapterCallError>, Self::Error> {
        if command.operation_id.is_empty()
            || command.source_key.namespace.is_empty()
            || command.source_key.value.is_empty()
            || command.endpoints.is_empty()
            || command.maximum_response_bytes == 0
            || command
                .endpoints
                .iter()
                .any(|endpoint| endpoint.selector().is_none())
        {
            return Err(AdapterProcessError::InvalidEndpointLookupRequest);
        }
        let requested = command
            .endpoints
            .iter()
            .map(map_portable_endpoint)
            .collect::<Vec<ProtoPortableEndpoint>>();
        let response = call_rpc!(
            self.process,
            resolve_portable_endpoints,
            "ResolvePortableEndpoints",
            ResolvePortableEndpointsRequest {
                operation_id: command.operation_id,
                endpoints: requested.clone(),
                maximum_response_bytes: command.maximum_response_bytes,
                source_key: Some(Key {
                    namespace: command.source_key.namespace,
                    value: command.source_key.value,
                }),
            }
        );
        validation::resolve_endpoints_response(
            &requested,
            &response,
            command.maximum_response_bytes,
        )
        .map_err(|_| {
            AdapterProcessError::InvalidResponse("invalid endpoint resolution response")
        })?;
        let result = operation_result!(response, resolve_portable_endpoints_response);
        Ok(Ok(result
            .results
            .into_iter()
            .map(map_endpoint_result)
            .collect::<Result<_, _>>()?))
    }

    async fn read_asset(
        &mut self,
        command: ReadAdapterAsset,
    ) -> Result<Result<AdapterBinaryAsset, AdapterCallError>, Self::Error> {
        if command.operation_id.is_empty()
            || command.maximum_bytes == 0
            || command.allowed_content_types.is_empty()
        {
            return Err(AdapterProcessError::InvalidAssetRequest);
        }
        let response = call_rpc!(
            self.process,
            read_asset,
            "ReadAsset",
            ReadAssetRequest {
                operation_id: command.operation_id,
                source_key: Some(map_adapter_key(command.source_key)?),
                provider_item_key: Some(map_adapter_key(command.provider_item_key)?),
                asset_key: Some(map_adapter_key(command.asset_key)?),
                maximum_bytes: command.maximum_bytes,
            }
        );
        validation::asset_response(
            &response,
            command.maximum_bytes,
            &command.allowed_content_types,
        )
        .map_err(|_| AdapterProcessError::InvalidResponse("invalid asset response"))?;
        let result = operation_result!(response, read_asset_response);
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
    }

    async fn cancel_operation(&mut self, operation_id: Vec<u8>) -> Result<(), Self::Error> {
        if operation_id.is_empty() {
            return Err(AdapterProcessError::InvalidOperationId);
        }
        let response = call_rpc!(
            self.process,
            cancel_operation,
            "CancelOperation",
            trakkin_provider_sdk::v1::CancelOperationRequest { operation_id }
        );
        validation::cancel_operation_response(&response).map_err(|_| {
            AdapterProcessError::InvalidResponse("invalid operation cancellation response")
        })?;
        match response
            .outcome
            .expect("validated operation cancellation outcome")
        {
            cancel_operation_response::Outcome::Result(_) => Ok(()),
            cancel_operation_response::Outcome::Error(error) => Err(AdapterProcessError::Adapter(
                Box::new(map_adapter_error(error)),
            )),
        }
    }

    async fn retire(self) -> Result<AdapterRetirement, Self::Error> {
        self.process.retire().await
    }
}
