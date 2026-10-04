use axum::Json;
use utoipa::OpenApi;

use crate::http::{
    dto::{
        connections::{
            AdapterConfigurationFieldResponse, AdapterConfigurationOptionResponse,
            AdapterSecretPatchRequest, AdapterSecretRequest, AdapterSettingRequest,
            AdapterSettingResponse, AdapterValueRequest, AdapterValueResponse,
            AuthenticationAccountResponse, AuthenticationMethodResponse,
            AuthenticationPromptResponse, AuthenticationStepResponse,
            ConnectionDeletionImpactResponse, ConnectionInspectionResponse, ConnectionResponse,
            ConnectionSourceResponse, ContinueAuthenticationRequest, CreateConnectionRequest,
            DeleteConnectionRequest, ReconfigureConnectionRequest, RenameConnectionRequest,
            RetryAfterResponse, StartAuthenticationRequest,
        },
        logs::{LogEntryResponse, LogPageResponse},
        mappings::{
            CreateRemoteMappingSourceRequest, MappingOperatorParameter, MappingPageResponse,
            MappingResultResponse, MappingSourceKindValue, MappingSourceResponse,
            MappingStateResponse, ReorderMappingSourcesRequest, UpdateMappingSourceRequest,
            WriteLocalMappingRequest,
        },
        providers::{
            AvailableProviderResponse, ChangeProviderUpdatePolicyRequest, InstallProviderRequest,
            ProviderMaintainerResponse, ProviderReleaseCompatibilityValue,
            ProviderReleaseInspectionResponse, ProviderResponse, ProviderSourceResponse,
            ProviderUpdatePolicyValue, ProviderUpdateResponse, TrustProviderSourceRequest,
            UpdateProviderRequest,
        },
        sources::{
            BinaryAssetReferenceResponse, CatalogEntryResponse, CatalogItemResponse,
            CatalogPageResponse, CatalogRelationResponse, KeyResponse, SourceStateResponse,
            StartSourceReadRequest, StartSourceResetRequest, StateFieldResponse,
            StateObservationResponse, StateSubjectResponse, StateValueResponse, TermResponse,
        },
        tasks::{TaskFailureResponse, TaskResponse},
        workspaces::{CreateWorkspaceRequest, WorkspaceResponse, WorkspaceSourceResponse},
    },
    error::{ErrorResponse, FieldProblemResponse},
};

use super::{connections, logs, mappings, providers, workspaces};

#[derive(OpenApi)]
#[openapi(
    paths(
        connections::list_connections,
        connections::create_connection,
        connections::rename_connection,
        connections::reconfigure_connection,
        connections::list_connection_sources,
        connections::get_connection_deletion_impact,
        connections::delete_connection,
        connections::inspect_connection,
        connections::start_authentication,
        connections::continue_authentication,
        connections::cancel_authentication,
        connections::start_source_discovery,
        connections::get_connection_task,
        connections::cancel_connection_task,
        logs::list_logs,
        logs::stream_logs,
        mappings::mapping_state,
        mappings::create_uploaded_mapping_source,
        mappings::create_remote_mapping_source,
        mappings::update_mapping_source,
        mappings::create_local_mapping,
        mappings::update_local_mapping,
        mappings::remove_local_mapping,
        mappings::reorder_mapping_sources,
        mappings::remove_mapping_source,
        mappings::refresh_remote_mapping_source,
        mappings::read_mapping_source_content,
        mappings::query_mappings,
        providers::list_provider_sources,
        providers::trust_provider_source,
        providers::refresh_provider_source,
        providers::remove_provider_source,
        providers::inspect_provider_release,
        providers::list_providers,
        providers::install_provider,
        providers::update_provider,
        providers::change_provider_update_policy,
        providers::remove_provider,
        workspaces::list_workspaces,
        workspaces::create_workspace,
        workspaces::get_workspace,
        workspaces::sources::attach_source,
        workspaces::sources::detach_source,
        workspaces::sources::get_catalog,
        workspaces::sources::get_binary_asset,
        workspaces::sources::get_state,
        workspaces::sources::start_source_read,
        workspaces::sources::start_source_reset,
        workspaces::tasks::list_tasks,
        workspaces::tasks::get_task,
        workspaces::tasks::cancel_task,
        get_openapi
    ),
    components(schemas(
        ErrorResponse,
        FieldProblemResponse,
        CreateConnectionRequest,
        RenameConnectionRequest,
        DeleteConnectionRequest,
        AdapterSettingRequest,
        AdapterSettingResponse,
        AdapterSecretRequest,
        ReconfigureConnectionRequest,
        AdapterSecretPatchRequest,
        AdapterValueRequest,
        ConnectionResponse,
        ConnectionSourceResponse,
        ConnectionDeletionImpactResponse,
        ConnectionInspectionResponse,
        StartAuthenticationRequest,
        ContinueAuthenticationRequest,
        AdapterConfigurationFieldResponse,
        AdapterConfigurationOptionResponse,
        AdapterValueResponse,
        AuthenticationMethodResponse,
        AuthenticationStepResponse,
        AuthenticationPromptResponse,
        AuthenticationAccountResponse,
        RetryAfterResponse,
        LogPageResponse,
        LogEntryResponse,
        UpdateMappingSourceRequest,
        CreateRemoteMappingSourceRequest,
        ReorderMappingSourcesRequest,
        WriteLocalMappingRequest,
        MappingSourceKindValue,
        MappingSourceResponse,
        MappingStateResponse,
        MappingResultResponse,
        MappingPageResponse,
        MappingOperatorParameter,
        TrustProviderSourceRequest,
        InstallProviderRequest,
        UpdateProviderRequest,
        ChangeProviderUpdatePolicyRequest,
        ProviderUpdatePolicyValue,
        AvailableProviderResponse,
        ProviderSourceResponse,
        ProviderReleaseCompatibilityValue,
        ProviderMaintainerResponse,
        ProviderReleaseInspectionResponse,
        ProviderUpdateResponse,
        ProviderResponse,
        CreateWorkspaceRequest,
        WorkspaceResponse,
        WorkspaceSourceResponse,
        CatalogPageResponse,
        CatalogEntryResponse,
        CatalogRelationResponse,
        CatalogItemResponse,
        KeyResponse,
        TermResponse,
        BinaryAssetReferenceResponse,
        SourceStateResponse,
        StateObservationResponse,
        StateSubjectResponse,
        StateFieldResponse,
        StateValueResponse,
        StartSourceReadRequest,
        StartSourceResetRequest,
        TaskResponse,
        TaskFailureResponse
    )),
    tags(
        (name = "connections"),
        (name = "providers"),
        (name = "workspaces"),
        (name = "catalog"),
        (name = "tasks"),
        (name = "logs"),
        (name = "mappings"),
        (name = "contract")
    )
)]
struct ApiDocument;

pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDocument::openapi()
}

#[utoipa::path(get, path = "/api/v1/openapi.json", responses((status = 200, description = "Generated OpenAPI contract", body = OpenApiDocumentResponse)), tag = "contract")]
pub(super) async fn get_openapi() -> Json<utoipa::openapi::OpenApi> {
    Json(openapi())
}

struct OpenApiDocumentResponse;

impl utoipa::PartialSchema for OpenApiDocumentResponse {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{AdditionalProperties, ObjectBuilder};

        ObjectBuilder::new()
            .additional_properties(Some(AdditionalProperties::FreeForm(true)))
            .into()
    }
}

impl utoipa::ToSchema for OpenApiDocumentResponse {}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    #[test]
    fn document_resolves_every_local_reference() {
        let document = serde_json::to_value(super::openapi()).expect("OpenAPI serializes");
        assert_references(&document, &document);
    }

    fn assert_references(value: &Value, document: &Value) {
        match value {
            Value::Object(fields) => {
                if let Some(reference) = fields.get("$ref").and_then(Value::as_str)
                    && let Some(pointer) = reference.strip_prefix('#')
                {
                    assert!(
                        document.pointer(pointer).is_some(),
                        "unresolved reference: {reference}"
                    );
                }
                for child in fields.values() {
                    assert_references(child, document);
                }
            }
            Value::Array(values) => {
                for child in values {
                    assert_references(child, document);
                }
            }
            _ => {}
        }
    }
}
