use std::sync::Arc;

use uuid::Uuid;

use super::{
    adapter_runtime::AdapterBinaryAsset,
    assets::{BinaryAssetInteraction, ReadPublishedBinaryAsset},
    catalog_queries::{CatalogPage, InspectSourceState, QueryCatalogPage, SourceStateInspection},
    connection::{
        BeginConnectionDeletion, CancelConnectionAuthentication, ConnectionDeletionImpact,
        ConnectionInspection, ConnectionInteraction, ConnectionSourceSummary, ConnectionSummary,
        ContinueConnectionAuthentication, CreateConnection, InspectConnection,
        ReconfigureConnection, RenameConnection, StartConnectionAuthentication,
        StartedConnectionAuthentication,
    },
    error::ApplicationFuture,
    mappings::{
        ChangeMappingSource, DecideMapping, DeleteMapping, MappingPageSnapshot, MappingResult,
        MappingSourcesSnapshot, MappingStateSnapshot, QueryMappings, SaveMapping,
        SaveMappingSource, SharedMappingApi,
    },
    providers::{
        ChangeProviderUpdatePolicy, InspectProviderRelease, InstallProvider,
        ProviderReleaseInspectionSnapshot, ProviderSnapshot, ProviderSourceSnapshot,
        RefreshProviderSource, RemoveProvider, RemoveProviderSource, SharedProviderRegistry,
        TrustProviderSource, UpdateProvider,
    },
    task_manager::{AdmitTask, RequestTaskCancellation, TaskSnapshot},
    workspaces::{AttachWorkspaceSource, CreateWorkspace, DetachWorkspaceSource},
};

use super::workspaces::WorkspaceView;

pub trait ApplicationApi: Send + Sync {
    fn create_workspace(&self, command: CreateWorkspace) -> ApplicationFuture<'_, WorkspaceView>;
    fn list_workspaces(&self) -> ApplicationFuture<'_, Vec<WorkspaceView>>;
    fn get_workspace(&self, workspace_id: Uuid) -> ApplicationFuture<'_, WorkspaceView>;
    fn attach_source(&self, command: AttachWorkspaceSource)
    -> ApplicationFuture<'_, WorkspaceView>;
    fn detach_source(&self, command: DetachWorkspaceSource)
    -> ApplicationFuture<'_, WorkspaceView>;
    fn query_catalog(&self, query: QueryCatalogPage) -> ApplicationFuture<'_, CatalogPage>;
    fn inspect_state(
        &self,
        query: InspectSourceState,
    ) -> ApplicationFuture<'_, SourceStateInspection>;
    fn admit_task(&self, command: AdmitTask) -> ApplicationFuture<'_, TaskSnapshot>;
    fn list_tasks(&self, workspace_id: Uuid) -> ApplicationFuture<'_, Vec<TaskSnapshot>>;
    fn get_task(&self, workspace_id: Uuid, task_id: Uuid) -> ApplicationFuture<'_, TaskSnapshot>;
    fn cancel_task(
        &self,
        workspace_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot>;
    fn create_connection(
        &self,
        command: CreateConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary>;
    fn list_connections(&self) -> ApplicationFuture<'_, Vec<ConnectionSummary>>;
    fn rename_connection(
        &self,
        command: RenameConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary>;
    fn reconfigure_connection(
        &self,
        command: ReconfigureConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary>;
    fn list_connection_sources(
        &self,
        connection_id: Uuid,
    ) -> ApplicationFuture<'_, Vec<ConnectionSourceSummary>>;
    fn get_connection_task(
        &self,
        connection_id: Uuid,
        task_id: Uuid,
    ) -> ApplicationFuture<'_, TaskSnapshot>;
    fn cancel_connection_task(
        &self,
        connection_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot>;
    fn connection_deletion_impact(
        &self,
        connection_id: Uuid,
    ) -> ApplicationFuture<'_, ConnectionDeletionImpact>;
    fn delete_connection(
        &self,
        command: BeginConnectionDeletion,
    ) -> ApplicationFuture<'_, ConnectionDeletionImpact>;
    fn inspect_connection(
        &self,
        command: InspectConnection,
    ) -> ApplicationFuture<'_, ConnectionInspection>;
    fn start_authentication(
        &self,
        command: StartConnectionAuthentication,
    ) -> ApplicationFuture<'_, StartedConnectionAuthentication>;
    fn continue_authentication(
        &self,
        command: ContinueConnectionAuthentication,
    ) -> ApplicationFuture<'_, super::adapter_runtime::AdapterAuthenticationStep>;
    fn cancel_authentication(
        &self,
        command: CancelConnectionAuthentication,
    ) -> ApplicationFuture<'_, ()>;
    fn read_binary_asset(
        &self,
        command: ReadPublishedBinaryAsset,
    ) -> ApplicationFuture<'_, AdapterBinaryAsset>;
    fn list_provider_sources(&self) -> ApplicationFuture<'_, Vec<ProviderSourceSnapshot>>;
    fn trust_provider_source(
        &self,
        command: TrustProviderSource,
    ) -> ApplicationFuture<'_, ProviderSourceSnapshot>;
    fn refresh_provider_source(
        &self,
        command: RefreshProviderSource,
    ) -> ApplicationFuture<'_, ProviderSourceSnapshot>;
    fn remove_provider_source(&self, command: RemoveProviderSource) -> ApplicationFuture<'_, ()>;
    fn inspect_provider_release(
        &self,
        command: InspectProviderRelease,
    ) -> ApplicationFuture<'_, ProviderReleaseInspectionSnapshot>;
    fn list_providers(&self) -> ApplicationFuture<'_, Vec<ProviderSnapshot>>;
    fn install_provider(&self, command: InstallProvider)
    -> ApplicationFuture<'_, ProviderSnapshot>;
    fn update_provider(&self, command: UpdateProvider) -> ApplicationFuture<'_, ProviderSnapshot>;
    fn change_provider_update_policy(
        &self,
        command: ChangeProviderUpdatePolicy,
    ) -> ApplicationFuture<'_, ProviderSnapshot>;
    fn remove_provider(&self, command: RemoveProvider) -> ApplicationFuture<'_, ()>;
    fn mapping_state(&self) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn query_mappings(&self, query: QueryMappings) -> ApplicationFuture<'_, MappingPageSnapshot>;
    fn preview_mapping(&self, record: String) -> ApplicationFuture<'_, MappingResult>;
    fn save_mapping(&self, command: SaveMapping) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn decide_mapping(&self, command: DecideMapping)
    -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn delete_mapping(&self, command: DeleteMapping)
    -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn mapping_sources(&self) -> ApplicationFuture<'_, MappingSourcesSnapshot>;
    fn save_mapping_source(
        &self,
        command: SaveMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn delete_mapping_source(
        &self,
        command: ChangeMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn refresh_mapping_source(
        &self,
        command: ChangeMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn revert_mapping(&self, command: DeleteMapping)
    -> ApplicationFuture<'_, MappingStateSnapshot>;
}

#[derive(Clone)]
pub struct ApplicationFacade {
    pub workspaces: Arc<dyn super::workspaces::WorkspaceApi>,
    pub catalog: Arc<dyn super::catalog_queries::CatalogApi>,
    pub tasks: Arc<dyn super::task_manager::TaskApi>,
    pub connections: Arc<dyn super::connection::ConnectionApi>,
    pub connection_interaction: Arc<dyn ConnectionInteraction>,
    pub binary_assets: Arc<dyn BinaryAssetInteraction>,
    pub providers: SharedProviderRegistry,
    pub mappings: SharedMappingApi,
}

impl ApplicationApi for ApplicationFacade {
    fn create_workspace(&self, command: CreateWorkspace) -> ApplicationFuture<'_, WorkspaceView> {
        self.workspaces.create_workspace(command)
    }

    fn list_workspaces(&self) -> ApplicationFuture<'_, Vec<WorkspaceView>> {
        self.workspaces.list_workspaces()
    }

    fn get_workspace(&self, workspace_id: Uuid) -> ApplicationFuture<'_, WorkspaceView> {
        self.workspaces.get_workspace(workspace_id)
    }

    fn attach_source(
        &self,
        command: AttachWorkspaceSource,
    ) -> ApplicationFuture<'_, WorkspaceView> {
        self.workspaces.attach_source(command)
    }

    fn detach_source(
        &self,
        command: DetachWorkspaceSource,
    ) -> ApplicationFuture<'_, WorkspaceView> {
        self.workspaces.detach_source(command)
    }

    fn query_catalog(&self, query: QueryCatalogPage) -> ApplicationFuture<'_, CatalogPage> {
        self.catalog.query_catalog(query)
    }

    fn inspect_state(
        &self,
        query: InspectSourceState,
    ) -> ApplicationFuture<'_, SourceStateInspection> {
        self.catalog.inspect_state(query)
    }

    fn admit_task(&self, command: AdmitTask) -> ApplicationFuture<'_, TaskSnapshot> {
        self.tasks.admit_task(command)
    }

    fn list_tasks(&self, workspace_id: Uuid) -> ApplicationFuture<'_, Vec<TaskSnapshot>> {
        self.tasks.list_tasks(workspace_id)
    }

    fn get_task(&self, workspace_id: Uuid, task_id: Uuid) -> ApplicationFuture<'_, TaskSnapshot> {
        self.tasks.get_task(workspace_id, task_id)
    }

    fn cancel_task(
        &self,
        workspace_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        self.tasks.cancel_task(workspace_id, command)
    }

    fn create_connection(
        &self,
        command: CreateConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary> {
        self.connections.create_connection(command)
    }

    fn list_connections(&self) -> ApplicationFuture<'_, Vec<ConnectionSummary>> {
        self.connections.list_connections()
    }

    fn rename_connection(
        &self,
        command: RenameConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary> {
        self.connections.rename_connection(command)
    }

    fn reconfigure_connection(
        &self,
        command: ReconfigureConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary> {
        self.connections.reconfigure_connection(command)
    }

    fn list_connection_sources(
        &self,
        connection_id: Uuid,
    ) -> ApplicationFuture<'_, Vec<ConnectionSourceSummary>> {
        self.connections.list_connection_sources(connection_id)
    }

    fn get_connection_task(
        &self,
        connection_id: Uuid,
        task_id: Uuid,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        self.tasks.get_connection_task(connection_id, task_id)
    }

    fn cancel_connection_task(
        &self,
        connection_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        self.tasks.cancel_connection_task(connection_id, command)
    }

    fn connection_deletion_impact(
        &self,
        connection_id: Uuid,
    ) -> ApplicationFuture<'_, ConnectionDeletionImpact> {
        self.connections.connection_deletion_impact(connection_id)
    }

    fn delete_connection(
        &self,
        command: BeginConnectionDeletion,
    ) -> ApplicationFuture<'_, ConnectionDeletionImpact> {
        self.connections.delete_connection(command)
    }

    fn inspect_connection(
        &self,
        command: InspectConnection,
    ) -> ApplicationFuture<'_, ConnectionInspection> {
        self.connection_interaction.inspect_connection(command)
    }

    fn start_authentication(
        &self,
        command: StartConnectionAuthentication,
    ) -> ApplicationFuture<'_, StartedConnectionAuthentication> {
        self.connection_interaction
            .start_connection_authentication(command)
    }

    fn continue_authentication(
        &self,
        command: ContinueConnectionAuthentication,
    ) -> ApplicationFuture<'_, super::adapter_runtime::AdapterAuthenticationStep> {
        self.connection_interaction
            .continue_connection_authentication(command)
    }

    fn cancel_authentication(
        &self,
        command: CancelConnectionAuthentication,
    ) -> ApplicationFuture<'_, ()> {
        self.connection_interaction
            .cancel_connection_authentication(command)
    }

    fn read_binary_asset(
        &self,
        command: ReadPublishedBinaryAsset,
    ) -> ApplicationFuture<'_, AdapterBinaryAsset> {
        self.binary_assets.read_published_binary_asset(command)
    }

    fn list_provider_sources(&self) -> ApplicationFuture<'_, Vec<ProviderSourceSnapshot>> {
        self.providers.list_provider_sources()
    }

    fn trust_provider_source(
        &self,
        command: TrustProviderSource,
    ) -> ApplicationFuture<'_, ProviderSourceSnapshot> {
        self.providers.trust_provider_source(command)
    }

    fn refresh_provider_source(
        &self,
        command: RefreshProviderSource,
    ) -> ApplicationFuture<'_, ProviderSourceSnapshot> {
        self.providers.refresh_provider_source(command)
    }

    fn remove_provider_source(&self, command: RemoveProviderSource) -> ApplicationFuture<'_, ()> {
        self.providers.remove_provider_source(command)
    }

    fn inspect_provider_release(
        &self,
        command: InspectProviderRelease,
    ) -> ApplicationFuture<'_, ProviderReleaseInspectionSnapshot> {
        self.providers.inspect_provider_release(command)
    }

    fn list_providers(&self) -> ApplicationFuture<'_, Vec<ProviderSnapshot>> {
        self.providers.list_providers()
    }

    fn install_provider(
        &self,
        command: InstallProvider,
    ) -> ApplicationFuture<'_, ProviderSnapshot> {
        self.providers.install_provider(command)
    }

    fn update_provider(&self, command: UpdateProvider) -> ApplicationFuture<'_, ProviderSnapshot> {
        self.providers.update_provider(command)
    }

    fn change_provider_update_policy(
        &self,
        command: ChangeProviderUpdatePolicy,
    ) -> ApplicationFuture<'_, ProviderSnapshot> {
        self.providers.change_provider_update_policy(command)
    }

    fn remove_provider(&self, command: RemoveProvider) -> ApplicationFuture<'_, ()> {
        self.providers.remove_provider(command)
    }

    fn mapping_state(&self) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.state()
    }

    fn query_mappings(&self, query: QueryMappings) -> ApplicationFuture<'_, MappingPageSnapshot> {
        self.mappings.query(query)
    }

    fn preview_mapping(&self, record: String) -> ApplicationFuture<'_, MappingResult> {
        self.mappings.preview(record)
    }

    fn save_mapping(&self, command: SaveMapping) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.save(command)
    }

    fn decide_mapping(
        &self,
        command: DecideMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.decide(command)
    }

    fn delete_mapping(
        &self,
        command: DeleteMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.delete(command)
    }

    fn mapping_sources(&self) -> ApplicationFuture<'_, MappingSourcesSnapshot> {
        self.mappings.sources()
    }
    fn save_mapping_source(
        &self,
        command: SaveMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.save_source(command)
    }
    fn delete_mapping_source(
        &self,
        command: ChangeMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.delete_source(command)
    }
    fn refresh_mapping_source(
        &self,
        command: ChangeMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.refresh_source(command)
    }
    fn revert_mapping(
        &self,
        command: DeleteMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.revert(command)
    }
}
