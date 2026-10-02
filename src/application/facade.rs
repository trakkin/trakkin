use std::{future::Future, pin::Pin, sync::Arc};

use uuid::Uuid;

use super::{
    adapter_runtime::AdapterBinaryAsset,
    assets::{BinaryAssetInteraction, ReadPublishedBinaryAsset},
    catalog_queries::{
        CatalogPage, CatalogQueryRepository, InspectSourceState, QueryCatalogPage,
        SourceStateInspection,
    },
    connection::{
        BeginConnectionDeletion, CancelConnectionAuthentication, ConnectionDeletionImpact,
        ConnectionInspection, ConnectionInteraction, ConnectionRepository, ConnectionRetirement,
        ConnectionSourceSummary, ConnectionSummary, ContinueConnectionAuthentication,
        CreateConnection, FinishConnectionDeletion, InspectConnection, ReconfigureConnection,
        RenameConnection, StartConnectionAuthentication, StartedConnectionAuthentication,
    },
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    mappings::{
        CreateLocalMapping, CreateRemoteMappingSource, CreateUploadedMappingSource,
        MappingContentReader, MappingPageSnapshot, MappingSourceContent, MappingStateSnapshot,
        QueryMappings, RefreshRemoteMappingSource, RemoveLocalMapping, RemoveMappingSource,
        ReorderMappingSources, SharedMappingApi, UpdateLocalMapping, UpdateMappingSource,
    },
    providers::{
        ChangeProviderUpdatePolicy, InspectProviderRelease, InstallProvider,
        ProviderReleaseInspectionSnapshot, ProviderSnapshot, ProviderSourceSnapshot,
        RefreshProviderSource, RemoveProvider, RemoveProviderSource, SharedProviderRegistry,
        TrustProviderSource, UpdateProvider,
    },
    task_manager::{AdmitTask, RequestTaskCancellation, TaskManager, TaskRepository, TaskSnapshot},
    workspaces::{
        AttachWorkspaceSource, CreateWorkspace, DetachWorkspaceSource, WorkspaceRepository,
        WorkspaceService, WorkspaceSnapshot, WorkspaceSourceAttachment,
    },
};

pub type ApplicationFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ApplicationError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceView {
    pub workspace: WorkspaceSnapshot,
    pub sources: Vec<WorkspaceSourceAttachment>,
}

pub struct ApplicationInteractions {
    pub connections: Arc<dyn ConnectionInteraction>,
    pub binary_assets: Arc<dyn BinaryAssetInteraction>,
    pub providers: SharedProviderRegistry,
    pub mappings: SharedMappingApi,
}

impl ApplicationInteractions {
    pub fn new(
        connections: Arc<dyn ConnectionInteraction>,
        binary_assets: Arc<dyn BinaryAssetInteraction>,
        providers: SharedProviderRegistry,
        mappings: SharedMappingApi,
    ) -> Self {
        Self {
            connections,
            binary_assets,
            providers,
            mappings,
        }
    }
}

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
    fn create_uploaded_mapping_source(
        &self,
        command: CreateUploadedMappingSource,
        content: MappingContentReader,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn create_remote_mapping_source(
        &self,
        command: CreateRemoteMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn update_mapping_source(
        &self,
        command: UpdateMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn create_local_mapping(
        &self,
        command: CreateLocalMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn update_local_mapping(
        &self,
        command: UpdateLocalMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn remove_local_mapping(
        &self,
        command: RemoveLocalMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn reorder_mapping_sources(
        &self,
        command: ReorderMappingSources,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn remove_mapping_source(
        &self,
        command: RemoveMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn refresh_remote_mapping_source(
        &self,
        command: RefreshRemoteMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot>;
    fn read_mapping_source_content(
        &self,
        source_id: Uuid,
    ) -> ApplicationFuture<'_, MappingSourceContent>;
    fn query_mappings(&self, query: QueryMappings) -> ApplicationFuture<'_, MappingPageSnapshot>;
}

#[derive(Clone)]
pub struct ApplicationFacade<W, C, T, R, D> {
    workspaces: WorkspaceService<W>,
    catalog: C,
    tasks: TaskManager<T>,
    connections: R,
    connection_retirement: D,
    connection_interaction: Arc<dyn ConnectionInteraction>,
    binary_assets: Arc<dyn BinaryAssetInteraction>,
    providers: SharedProviderRegistry,
    mappings: SharedMappingApi,
}

impl<W, C, T, R, D> ApplicationFacade<W, C, T, R, D>
where
    W: WorkspaceRepository,
    W::Error: IntoApplicationError,
    C: CatalogQueryRepository,
    C::Error: IntoApplicationError,
    T: TaskRepository,
    T::Error: IntoApplicationError,
    R: ConnectionRepository,
    R::Error: IntoApplicationError,
    D: ConnectionRetirement,
    D::Error: IntoApplicationError,
{
    pub fn new(
        workspaces: W,
        catalog: C,
        tasks: T,
        connections: R,
        connection_retirement: D,
        interactions: ApplicationInteractions,
    ) -> Self {
        Self {
            workspaces: WorkspaceService::new(workspaces),
            catalog,
            tasks: TaskManager::new(tasks),
            connections,
            connection_retirement,
            connection_interaction: interactions.connections,
            binary_assets: interactions.binary_assets,
            providers: interactions.providers,
            mappings: interactions.mappings,
        }
    }

    async fn workspace_view(&self, workspace_id: Uuid) -> Result<WorkspaceView, ApplicationError> {
        let workspace = self
            .workspaces
            .get(workspace_id)
            .await
            .map_err(IntoApplicationError::into_application_error)?
            .ok_or_else(|| {
                ApplicationError::new(
                    ApplicationErrorCode::WorkspaceNotFound,
                    "workspace was not found",
                )
            })?;
        let sources = self
            .workspaces
            .attachments(workspace_id)
            .await
            .map_err(IntoApplicationError::into_application_error)?;
        Ok(WorkspaceView { workspace, sources })
    }

    async fn workspace_task(
        &self,
        workspace_id: Uuid,
        task_id: Uuid,
    ) -> Result<TaskSnapshot, ApplicationError> {
        self.tasks
            .get(task_id)
            .await
            .map_err(IntoApplicationError::into_application_error)?
            .filter(|task| task.workspace_id == Some(workspace_id))
            .ok_or_else(|| {
                ApplicationError::new(ApplicationErrorCode::TaskNotFound, "task was not found")
            })
    }

    async fn connection_task(
        &self,
        connection_id: Uuid,
        task_id: Uuid,
    ) -> Result<TaskSnapshot, ApplicationError> {
        self.tasks
            .get(task_id)
            .await
            .map_err(IntoApplicationError::into_application_error)?
            .filter(|task| {
                task.workspace_id.is_none()
                    && task.connection_id == connection_id
                    && matches!(
                        task.definition,
                        super::task_manager::TaskDefinition::SourceDiscovery { .. }
                    )
            })
            .ok_or_else(|| {
                ApplicationError::new(ApplicationErrorCode::TaskNotFound, "task was not found")
            })
    }
}

impl<W, C, T, R, D> ApplicationApi for ApplicationFacade<W, C, T, R, D>
where
    W: WorkspaceRepository + 'static,
    W::Error: IntoApplicationError,
    C: CatalogQueryRepository + 'static,
    C::Error: IntoApplicationError,
    T: TaskRepository + 'static,
    T::Error: IntoApplicationError,
    R: ConnectionRepository + 'static,
    R::Error: IntoApplicationError,
    D: ConnectionRetirement + 'static,
    D::Error: IntoApplicationError,
{
    fn create_workspace(&self, command: CreateWorkspace) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(async move {
            let workspace_id = command.workspace_id;
            let workspace = self
                .workspaces
                .create(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            tracing::info!(workspace.id = %workspace_id, "workspace created");
            Ok(WorkspaceView {
                workspace,
                sources: Vec::new(),
            })
        })
    }

    fn list_workspaces(&self) -> ApplicationFuture<'_, Vec<WorkspaceView>> {
        Box::pin(async move {
            let workspaces = self
                .workspaces
                .list()
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            let mut views = Vec::with_capacity(workspaces.len());
            for workspace in workspaces {
                let sources = self
                    .workspaces
                    .attachments(workspace.id)
                    .await
                    .map_err(IntoApplicationError::into_application_error)?;
                views.push(WorkspaceView { workspace, sources });
            }
            Ok(views)
        })
    }

    fn get_workspace(&self, workspace_id: Uuid) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(self.workspace_view(workspace_id))
    }

    fn attach_source(
        &self,
        command: AttachWorkspaceSource,
    ) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(async move {
            let workspace_id = command.workspace_id;
            let source_id = command.source_id;
            self.workspaces
                .attach(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            tracing::info!(%workspace_id, %source_id, "workspace source attached");
            self.workspace_view(workspace_id).await
        })
    }

    fn detach_source(
        &self,
        command: DetachWorkspaceSource,
    ) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(async move {
            let workspace_id = command.workspace_id;
            let source_id = command.source_id;
            let detached = self
                .workspaces
                .detach(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            if detached {
                tracing::info!(%workspace_id, %source_id, "workspace source detached");
            }
            self.workspace_view(workspace_id).await
        })
    }

    fn query_catalog(&self, query: QueryCatalogPage) -> ApplicationFuture<'_, CatalogPage> {
        Box::pin(async move {
            self.catalog
                .query_catalog_page(query)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn inspect_state(
        &self,
        query: InspectSourceState,
    ) -> ApplicationFuture<'_, SourceStateInspection> {
        Box::pin(async move {
            self.catalog
                .inspect_source_state(query)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn admit_task(&self, command: AdmitTask) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(async move {
            self.tasks
                .admit(command)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn list_tasks(&self, workspace_id: Uuid) -> ApplicationFuture<'_, Vec<TaskSnapshot>> {
        Box::pin(async move {
            self.workspace_view(workspace_id).await?;
            self.tasks
                .list_workspace(workspace_id)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn get_task(&self, workspace_id: Uuid, task_id: Uuid) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(self.workspace_task(workspace_id, task_id))
    }

    fn cancel_task(
        &self,
        workspace_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(async move {
            self.workspace_task(workspace_id, command.task_id).await?;
            self.tasks
                .request_cancellation(command.clone())
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            self.workspace_task(workspace_id, command.task_id).await
        })
    }

    fn create_connection(
        &self,
        command: CreateConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary> {
        Box::pin(async move {
            let connection_id = command.connection_id;
            let provider_id = command.provider_id.clone();
            let setting_count = command.settings.len();
            let secret_count = command.secrets.len();
            self.providers
                .ensure_provider_installed(&command.provider_id)
                .await?;
            self.connections
                .store_connection(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            let connection = self
                .connections
                .get_connection_summary(connection_id)
                .await
                .map_err(IntoApplicationError::into_application_error)?
                .ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorCode::Internal,
                        "stored connection could not be loaded",
                    )
                })?;
            tracing::info!(
                %connection_id,
                %provider_id,
                setting_count,
                secret_count,
                status = ?connection.status,
                "connection created"
            );
            Ok(connection)
        })
    }

    fn list_connections(&self) -> ApplicationFuture<'_, Vec<ConnectionSummary>> {
        Box::pin(async move {
            self.connections
                .list_connections()
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn rename_connection(
        &self,
        command: RenameConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary> {
        Box::pin(async move {
            let connection_id = command.connection_id;
            let connection = self
                .connections
                .rename_connection(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            tracing::info!(%connection_id, "connection renamed");
            Ok(connection)
        })
    }

    fn reconfigure_connection(
        &self,
        command: ReconfigureConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary> {
        Box::pin(async move {
            let connection_id = command.connection_id;
            let setting_count = command.settings.len();
            let secret_patch_count = command.secret_patches.len();
            self.connection_retirement
                .retire_connection(connection_id)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            let connection = self
                .connections
                .reconfigure_connection(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            tracing::info!(
                %connection_id,
                setting_count,
                secret_patch_count,
                status = ?connection.status,
                "connection reconfigured"
            );
            Ok(connection)
        })
    }

    fn list_connection_sources(
        &self,
        connection_id: Uuid,
    ) -> ApplicationFuture<'_, Vec<ConnectionSourceSummary>> {
        Box::pin(async move {
            self.connections
                .list_connection_sources(connection_id)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn get_connection_task(
        &self,
        connection_id: Uuid,
        task_id: Uuid,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(self.connection_task(connection_id, task_id))
    }

    fn cancel_connection_task(
        &self,
        connection_id: Uuid,
        command: RequestTaskCancellation,
    ) -> ApplicationFuture<'_, TaskSnapshot> {
        Box::pin(async move {
            self.connection_task(connection_id, command.task_id).await?;
            self.tasks
                .request_cancellation(command.clone())
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            self.connection_task(connection_id, command.task_id).await
        })
    }

    fn connection_deletion_impact(
        &self,
        connection_id: Uuid,
    ) -> ApplicationFuture<'_, ConnectionDeletionImpact> {
        Box::pin(async move {
            self.connections
                .connection_deletion_impact(connection_id)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn delete_connection(
        &self,
        command: BeginConnectionDeletion,
    ) -> ApplicationFuture<'_, ConnectionDeletionImpact> {
        Box::pin(async move {
            let connection_id = command.connection_id;
            let impact = self
                .connections
                .begin_connection_deletion(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            self.connection_retirement
                .retire_connection(connection_id)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            self.connections
                .finish_connection_deletion(FinishConnectionDeletion { connection_id })
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            tracing::info!(
                %connection_id,
                source_count = impact.source_count,
                workspace_attachment_count = impact.workspace_attachment_count,
                task_count = impact.task_count,
                active_task_count = impact.active_task_count,
                authentication_session_count = impact.authentication_session_count,
                "connection deleted"
            );
            Ok(impact)
        })
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

    fn create_uploaded_mapping_source(
        &self,
        command: CreateUploadedMappingSource,
        content: MappingContentReader,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.create_uploaded_source(command, content)
    }

    fn create_remote_mapping_source(
        &self,
        command: CreateRemoteMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.create_remote_source(command)
    }

    fn update_mapping_source(
        &self,
        command: UpdateMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.update_source(command)
    }

    fn create_local_mapping(
        &self,
        command: CreateLocalMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.create_local_mapping(command)
    }

    fn update_local_mapping(
        &self,
        command: UpdateLocalMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.update_local_mapping(command)
    }

    fn remove_local_mapping(
        &self,
        command: RemoveLocalMapping,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.remove_local_mapping(command)
    }

    fn reorder_mapping_sources(
        &self,
        command: ReorderMappingSources,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.reorder_sources(command)
    }

    fn remove_mapping_source(
        &self,
        command: RemoveMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.remove_source(command)
    }

    fn refresh_remote_mapping_source(
        &self,
        command: RefreshRemoteMappingSource,
    ) -> ApplicationFuture<'_, MappingStateSnapshot> {
        self.mappings.refresh_remote_source(command)
    }

    fn read_mapping_source_content(
        &self,
        source_id: Uuid,
    ) -> ApplicationFuture<'_, MappingSourceContent> {
        self.mappings.read_source_content(source_id)
    }

    fn query_mappings(&self, query: QueryMappings) -> ApplicationFuture<'_, MappingPageSnapshot> {
        self.mappings.query(query)
    }
}
