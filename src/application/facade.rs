use std::{future::Future, pin::Pin, sync::Arc};

use uuid::Uuid;

use super::{
    adapter_runtime::{AdapterBinaryAsset, AdapterPortableReferenceLookup},
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
    mapping_lifecycle::{
        ActivateWorkspaceMappingRevision, CanonicalRowSetDiff, ChangeMappingActivation,
        CreateMapping, CreateWorkspaceMappingDocument, MappingActivationKind,
        MappingLifecycleError, MappingLifecycleRepository, MappingLifecycleService,
        RetireWorkspaceMapping, UpdateMapping, UpdateWorkspaceMappingDocument,
        WorkspaceMappingActivation,
    },
    mapping_queries::{
        MappingDetail, MappingHistoryPage, MappingQueryRepository, MappingRegistryPage,
        QueryMappingHistory, QueryMappingRegistry,
    },
    mapping_resolution::{
        ExactMappingRevision, MappingResolution, MappingResolutionInteraction, ResolutionRequest,
    },
    providers::{
        ActivateProviderInstallation, InstallProvider, ProviderInstallationSnapshot,
        ProviderReleaseSnapshot, ProviderRepositorySnapshot, RefreshProviderRepository,
        RegisterProviderRepository, RemoveProviderInstallation, SharedProviderRegistry,
    },
    reference_lookup::{LookupPortableReferences, ReferenceLookupInteraction},
    sync_activity::{
        QuerySyncActivity, SyncActivityActionDetail, SyncActivityApi, SyncActivityPage,
    },
    sync_policy::{
        CreateSyncPolicy, QuerySyncPolicies, SyncPolicyApi, SyncPolicyPage,
        SyncPolicyRevisionSnapshot, UpdateSyncPolicy,
    },
    sync_run::SyncRunSnapshot,
    sync_schedule::{EnableSyncSchedule, PauseSyncSchedule, SyncScheduleApi, SyncScheduleSnapshot},
    sync_workflow::{
        CreateWorkspaceSyncPreview, StartWorkspaceSyncRun, SyncPreviewMutationApi,
        SyncRunMutationApi, WorkspaceSyncPreview,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateMappingDocumentResult {
    Updated(Box<MappingDetail>),
    HeadMoved {
        current_head: String,
        diff: CanonicalRowSetDiff,
    },
}

pub struct ApplicationInteractions {
    pub connections: Arc<dyn ConnectionInteraction>,
    pub binary_assets: Arc<dyn BinaryAssetInteraction>,
    pub reference_lookup: Arc<dyn ReferenceLookupInteraction>,
    pub mapping_resolution: Arc<dyn MappingResolutionInteraction>,
    pub sync_policy: Arc<dyn SyncPolicyApi>,
    pub sync_preview_mutation: Option<Arc<dyn SyncPreviewMutationApi>>,
    pub sync_run_mutation: Option<Arc<dyn SyncRunMutationApi>>,
    pub sync_schedule: Option<Arc<dyn SyncScheduleApi>>,
    pub sync_activity: Arc<dyn SyncActivityApi>,
    pub providers: SharedProviderRegistry,
}

impl ApplicationInteractions {
    pub fn new(
        connections: Arc<dyn ConnectionInteraction>,
        binary_assets: Arc<dyn BinaryAssetInteraction>,
        reference_lookup: Arc<dyn ReferenceLookupInteraction>,
        mapping_resolution: Arc<dyn MappingResolutionInteraction>,
        sync_policy: Arc<dyn SyncPolicyApi>,
        sync_activity: Arc<dyn SyncActivityApi>,
        providers: SharedProviderRegistry,
    ) -> Self {
        Self {
            connections,
            binary_assets,
            reference_lookup,
            mapping_resolution,
            sync_policy,
            sync_preview_mutation: None,
            sync_run_mutation: None,
            sync_schedule: None,
            sync_activity,
            providers,
        }
    }

    pub fn with_sync_preview_mutation(
        mut self,
        sync_preview_mutation: Arc<dyn SyncPreviewMutationApi>,
    ) -> Self {
        self.sync_preview_mutation = Some(sync_preview_mutation);
        self
    }

    pub fn with_sync_run_mutation(
        mut self,
        sync_run_mutation: Arc<dyn SyncRunMutationApi>,
    ) -> Self {
        self.sync_run_mutation = Some(sync_run_mutation);
        self
    }

    pub fn with_sync_schedule(mut self, sync_schedule: Arc<dyn SyncScheduleApi>) -> Self {
        self.sync_schedule = Some(sync_schedule);
        self
    }
}

pub struct ApplicationMappingRepositories<M, L> {
    pub queries: M,
    pub lifecycle: L,
}

impl<M, L> ApplicationMappingRepositories<M, L> {
    pub fn new(queries: M, lifecycle: L) -> Self {
        Self { queries, lifecycle }
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
    fn query_mapping_registry(
        &self,
        query: QueryMappingRegistry,
    ) -> ApplicationFuture<'_, MappingRegistryPage>;
    fn get_mapping_head(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
    ) -> ApplicationFuture<'_, MappingDetail>;
    fn query_mapping_history(
        &self,
        query: QueryMappingHistory,
    ) -> ApplicationFuture<'_, MappingHistoryPage>;
    fn get_mapping_revision(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> ApplicationFuture<'_, MappingDetail>;
    fn create_mapping_document(
        &self,
        command: CreateWorkspaceMappingDocument,
    ) -> ApplicationFuture<'_, MappingDetail>;
    fn update_mapping_document(
        &self,
        command: UpdateWorkspaceMappingDocument,
    ) -> ApplicationFuture<'_, UpdateMappingDocumentResult>;
    fn activate_mapping_revision(
        &self,
        command: ActivateWorkspaceMappingRevision,
    ) -> ApplicationFuture<'_, WorkspaceMappingActivation>;
    fn retire_mapping(
        &self,
        command: RetireWorkspaceMapping,
    ) -> ApplicationFuture<'_, WorkspaceMappingActivation>;
    fn resolve_mapping_revision(
        &self,
        revision: ExactMappingRevision,
        request: ResolutionRequest,
    ) -> ApplicationFuture<'_, MappingResolution>;
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
    fn lookup_portable_references(
        &self,
        command: LookupPortableReferences,
    ) -> ApplicationFuture<'_, Vec<AdapterPortableReferenceLookup>>;
    fn list_sync_policies(&self, query: QuerySyncPolicies)
    -> ApplicationFuture<'_, SyncPolicyPage>;
    fn get_sync_policy(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot>;
    fn create_sync_policy(
        &self,
        command: CreateSyncPolicy,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot>;
    fn update_sync_policy(
        &self,
        command: UpdateSyncPolicy,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot>;
    fn enable_sync_schedule(
        &self,
        command: EnableSyncSchedule,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot>;
    fn get_sync_schedule(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot>;
    fn pause_sync_schedule(
        &self,
        command: PauseSyncSchedule,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot>;
    fn create_workspace_sync_preview(
        &self,
        command: CreateWorkspaceSyncPreview,
    ) -> ApplicationFuture<'_, WorkspaceSyncPreview>;
    fn start_workspace_sync_run(
        &self,
        command: StartWorkspaceSyncRun,
    ) -> ApplicationFuture<'_, SyncRunSnapshot>;
    fn list_sync_activity(
        &self,
        query: QuerySyncActivity,
    ) -> ApplicationFuture<'_, SyncActivityPage>;
    fn get_sync_activity_action(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> ApplicationFuture<'_, SyncActivityActionDetail>;
    fn list_provider_repositories(&self) -> ApplicationFuture<'_, Vec<ProviderRepositorySnapshot>>;
    fn register_provider_repository(
        &self,
        command: RegisterProviderRepository,
    ) -> ApplicationFuture<'_, ProviderRepositorySnapshot>;
    fn refresh_provider_repository(
        &self,
        command: RefreshProviderRepository,
    ) -> ApplicationFuture<'_, ProviderRepositorySnapshot>;
    fn list_provider_releases(&self) -> ApplicationFuture<'_, Vec<ProviderReleaseSnapshot>>;
    fn list_provider_installations(
        &self,
    ) -> ApplicationFuture<'_, Vec<ProviderInstallationSnapshot>>;
    fn install_provider(
        &self,
        command: InstallProvider,
    ) -> ApplicationFuture<'_, ProviderInstallationSnapshot>;
    fn activate_provider_installation(
        &self,
        command: ActivateProviderInstallation,
    ) -> ApplicationFuture<'_, ProviderInstallationSnapshot>;
    fn remove_provider_installation(
        &self,
        command: RemoveProviderInstallation,
    ) -> ApplicationFuture<'_, ()>;
}

#[derive(Clone)]
pub struct ApplicationFacade<W, C, M, L, T, R, D> {
    workspaces: WorkspaceService<W>,
    catalog: C,
    mapping_queries: M,
    mapping_lifecycle: MappingLifecycleService<L>,
    tasks: TaskManager<T>,
    connections: R,
    connection_retirement: D,
    connection_interaction: Arc<dyn ConnectionInteraction>,
    binary_assets: Arc<dyn BinaryAssetInteraction>,
    reference_lookup: Arc<dyn ReferenceLookupInteraction>,
    mapping_resolution: Arc<dyn MappingResolutionInteraction>,
    sync_policy: Arc<dyn SyncPolicyApi>,
    sync_preview_mutation: Option<Arc<dyn SyncPreviewMutationApi>>,
    sync_run_mutation: Option<Arc<dyn SyncRunMutationApi>>,
    sync_schedule: Option<Arc<dyn SyncScheduleApi>>,
    sync_activity: Arc<dyn SyncActivityApi>,
    providers: SharedProviderRegistry,
}

impl<W, C, M, L, T, R, D> ApplicationFacade<W, C, M, L, T, R, D>
where
    W: WorkspaceRepository,
    W::Error: IntoApplicationError,
    C: CatalogQueryRepository,
    C::Error: IntoApplicationError,
    M: MappingQueryRepository,
    M::Error: IntoApplicationError,
    L: MappingLifecycleRepository,
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
        mappings: ApplicationMappingRepositories<M, L>,
        tasks: T,
        connections: R,
        connection_retirement: D,
        interactions: ApplicationInteractions,
    ) -> Self {
        Self {
            workspaces: WorkspaceService::new(workspaces),
            catalog,
            mapping_queries: mappings.queries,
            mapping_lifecycle: MappingLifecycleService::new(mappings.lifecycle),
            tasks: TaskManager::new(tasks),
            connections,
            connection_retirement,
            connection_interaction: interactions.connections,
            binary_assets: interactions.binary_assets,
            reference_lookup: interactions.reference_lookup,
            mapping_resolution: interactions.mapping_resolution,
            sync_policy: interactions.sync_policy,
            sync_preview_mutation: interactions.sync_preview_mutation,
            sync_run_mutation: interactions.sync_run_mutation,
            sync_schedule: interactions.sync_schedule,
            sync_activity: interactions.sync_activity,
            providers: interactions.providers,
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

impl<W, C, M, L, T, R, D> ApplicationApi for ApplicationFacade<W, C, M, L, T, R, D>
where
    W: WorkspaceRepository + 'static,
    W::Error: IntoApplicationError,
    C: CatalogQueryRepository + 'static,
    C::Error: IntoApplicationError,
    M: MappingQueryRepository + 'static,
    M::Error: IntoApplicationError,
    L: MappingLifecycleRepository + 'static,
    T: TaskRepository + 'static,
    T::Error: IntoApplicationError,
    R: ConnectionRepository + 'static,
    R::Error: IntoApplicationError,
    D: ConnectionRetirement + 'static,
    D::Error: IntoApplicationError,
{
    fn create_workspace(&self, command: CreateWorkspace) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(async move {
            let workspace = self
                .workspaces
                .create(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
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
            self.workspaces
                .attach(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            self.workspace_view(workspace_id).await
        })
    }

    fn detach_source(
        &self,
        command: DetachWorkspaceSource,
    ) -> ApplicationFuture<'_, WorkspaceView> {
        Box::pin(async move {
            let workspace_id = command.workspace_id;
            self.workspaces
                .detach(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
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

    fn query_mapping_registry(
        &self,
        query: QueryMappingRegistry,
    ) -> ApplicationFuture<'_, MappingRegistryPage> {
        Box::pin(async move {
            self.mapping_queries
                .query_registry(query)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn get_mapping_head(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
    ) -> ApplicationFuture<'_, MappingDetail> {
        Box::pin(async move {
            self.mapping_queries
                .query_head(workspace_id, mapping_id)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn query_mapping_history(
        &self,
        query: QueryMappingHistory,
    ) -> ApplicationFuture<'_, MappingHistoryPage> {
        Box::pin(async move {
            self.mapping_queries
                .query_history(query)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn get_mapping_revision(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> ApplicationFuture<'_, MappingDetail> {
        let content_hash = content_hash.to_owned();
        Box::pin(async move {
            self.mapping_queries
                .query_revision(workspace_id, mapping_id, &content_hash)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn create_mapping_document(
        &self,
        command: CreateWorkspaceMappingDocument,
    ) -> ApplicationFuture<'_, MappingDetail> {
        Box::pin(async move {
            if command.schema != "trakkin.mapping/v1" {
                return Err(ApplicationError::field(
                    ApplicationErrorCode::InvalidField,
                    "mapping document is invalid",
                    "schema",
                    "unsupported_schema",
                    "mapping document schema is unsupported",
                ));
            }
            self.workspace_view(command.workspace_id).await?;
            let revision = self
                .mapping_lifecycle
                .create(CreateMapping {
                    mapping_id: command.mapping_id,
                    mappings: command.mappings,
                    created_at: command.created_at,
                })
                .await
                .map_err(|error| mapping_lifecycle_error(error, "contentHash"))?;
            self.mapping_queries
                .query_revision(
                    command.workspace_id,
                    command.mapping_id,
                    &revision.content_hash,
                )
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn update_mapping_document(
        &self,
        command: UpdateWorkspaceMappingDocument,
    ) -> ApplicationFuture<'_, UpdateMappingDocumentResult> {
        Box::pin(async move {
            self.workspace_view(command.workspace_id).await?;
            let revision = match self
                .mapping_lifecycle
                .update(UpdateMapping {
                    mapping_id: command.mapping_id,
                    base_content_hash: command.base_content_hash,
                    mappings: command.mappings,
                    updated_at: command.updated_at,
                })
                .await
            {
                Ok(revision) => revision,
                Err(MappingLifecycleError::HeadMoved { current_head, diff }) => {
                    return Ok(UpdateMappingDocumentResult::HeadMoved { current_head, diff });
                }
                Err(error) => return Err(mapping_lifecycle_error(error, "baseContentHash")),
            };
            let detail = self
                .mapping_queries
                .query_revision(
                    command.workspace_id,
                    command.mapping_id,
                    &revision.content_hash,
                )
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            Ok(UpdateMappingDocumentResult::Updated(Box::new(detail)))
        })
    }

    fn activate_mapping_revision(
        &self,
        command: ActivateWorkspaceMappingRevision,
    ) -> ApplicationFuture<'_, WorkspaceMappingActivation> {
        Box::pin(async move {
            self.workspace_view(command.workspace_id).await?;
            let head = self
                .mapping_lifecycle
                .head(command.mapping_id)
                .await
                .map_err(|error| mapping_lifecycle_error(error, "contentHash"))?;
            let kind = if head.content_hash == command.content_hash {
                MappingActivationKind::Activate
            } else {
                MappingActivationKind::Rollback
            };
            self.mapping_lifecycle
                .change_activation(ChangeMappingActivation {
                    event_id: command.event_id,
                    workspace_id: command.workspace_id,
                    mapping_id: command.mapping_id,
                    content_hash: Some(command.content_hash),
                    kind,
                    changed_at: command.changed_at,
                })
                .await
                .map_err(|error| mapping_lifecycle_error(error, "contentHash"))
        })
    }

    fn retire_mapping(
        &self,
        command: RetireWorkspaceMapping,
    ) -> ApplicationFuture<'_, WorkspaceMappingActivation> {
        Box::pin(async move {
            self.workspace_view(command.workspace_id).await?;
            self.mapping_lifecycle
                .change_activation(ChangeMappingActivation {
                    event_id: command.event_id,
                    workspace_id: command.workspace_id,
                    mapping_id: command.mapping_id,
                    content_hash: None,
                    kind: MappingActivationKind::Retire,
                    changed_at: command.changed_at,
                })
                .await
                .map_err(|error| mapping_lifecycle_error(error, "contentHash"))
        })
    }

    fn resolve_mapping_revision(
        &self,
        revision: ExactMappingRevision,
        request: ResolutionRequest,
    ) -> ApplicationFuture<'_, MappingResolution> {
        Box::pin(async move {
            self.mapping_queries
                .query_revision(
                    request.workspace_id,
                    revision.mapping_id,
                    &revision.content_hash,
                )
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            self.mapping_resolution
                .resolve_mapping_revision(revision, request)
                .await
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
            self.providers
                .ensure_provider_active(&command.provider_id)
                .await?;
            self.connections
                .store_connection(command)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            self.connections
                .get_connection_summary(connection_id)
                .await
                .map_err(IntoApplicationError::into_application_error)?
                .ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorCode::Internal,
                        "stored connection could not be loaded",
                    )
                })
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
            self.connections
                .rename_connection(command)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn reconfigure_connection(
        &self,
        command: ReconfigureConnection,
    ) -> ApplicationFuture<'_, ConnectionSummary> {
        Box::pin(async move {
            self.connection_retirement
                .retire_connection(command.connection_id)
                .await
                .map_err(IntoApplicationError::into_application_error)?;
            self.connections
                .reconfigure_connection(command)
                .await
                .map_err(IntoApplicationError::into_application_error)
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

    fn lookup_portable_references(
        &self,
        command: LookupPortableReferences,
    ) -> ApplicationFuture<'_, Vec<AdapterPortableReferenceLookup>> {
        self.reference_lookup.lookup_portable_references(command)
    }

    fn list_sync_activity(
        &self,
        query: QuerySyncActivity,
    ) -> ApplicationFuture<'_, SyncActivityPage> {
        self.sync_activity.list_activity(query)
    }

    fn list_sync_policies(
        &self,
        query: QuerySyncPolicies,
    ) -> ApplicationFuture<'_, SyncPolicyPage> {
        self.sync_policy.list_policies(query)
    }

    fn get_sync_policy(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot> {
        self.sync_policy.get_policy(workspace_id, policy_id)
    }

    fn create_sync_policy(
        &self,
        command: CreateSyncPolicy,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot> {
        self.sync_policy.create_policy(command)
    }

    fn update_sync_policy(
        &self,
        command: UpdateSyncPolicy,
    ) -> ApplicationFuture<'_, SyncPolicyRevisionSnapshot> {
        self.sync_policy.update_policy(command)
    }

    fn enable_sync_schedule(
        &self,
        command: EnableSyncSchedule,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot> {
        match &self.sync_schedule {
            Some(sync_schedule) => sync_schedule.enable_schedule(command),
            None => Box::pin(async move {
                Err(ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "synchronization schedules are not configured",
                ))
            }),
        }
    }

    fn get_sync_schedule(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot> {
        match &self.sync_schedule {
            Some(sync_schedule) => sync_schedule.get_schedule(workspace_id, policy_id),
            None => Box::pin(async move {
                Err(ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "synchronization schedules are not configured",
                ))
            }),
        }
    }

    fn pause_sync_schedule(
        &self,
        command: PauseSyncSchedule,
    ) -> ApplicationFuture<'_, SyncScheduleSnapshot> {
        match &self.sync_schedule {
            Some(sync_schedule) => sync_schedule.pause_schedule(command),
            None => Box::pin(async move {
                Err(ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "synchronization schedules are not configured",
                ))
            }),
        }
    }

    fn create_workspace_sync_preview(
        &self,
        command: CreateWorkspaceSyncPreview,
    ) -> ApplicationFuture<'_, WorkspaceSyncPreview> {
        match &self.sync_preview_mutation {
            Some(sync_preview_mutation) => sync_preview_mutation.create_workspace_preview(command),
            None => Box::pin(async move {
                Err(ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "synchronization preview creation is not configured",
                ))
            }),
        }
    }

    fn start_workspace_sync_run(
        &self,
        command: StartWorkspaceSyncRun,
    ) -> ApplicationFuture<'_, SyncRunSnapshot> {
        match &self.sync_run_mutation {
            Some(sync_run_mutation) => sync_run_mutation.start_workspace_sync_run(command),
            None => Box::pin(async move {
                Err(ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "synchronization run admission is not configured",
                ))
            }),
        }
    }

    fn get_sync_activity_action(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> ApplicationFuture<'_, SyncActivityActionDetail> {
        self.sync_activity
            .get_activity_action(workspace_id, action_id)
    }

    fn list_provider_repositories(&self) -> ApplicationFuture<'_, Vec<ProviderRepositorySnapshot>> {
        self.providers.list_provider_repositories()
    }

    fn register_provider_repository(
        &self,
        command: RegisterProviderRepository,
    ) -> ApplicationFuture<'_, ProviderRepositorySnapshot> {
        self.providers.register_provider_repository(command)
    }

    fn refresh_provider_repository(
        &self,
        command: RefreshProviderRepository,
    ) -> ApplicationFuture<'_, ProviderRepositorySnapshot> {
        self.providers.refresh_provider_repository(command)
    }

    fn list_provider_releases(&self) -> ApplicationFuture<'_, Vec<ProviderReleaseSnapshot>> {
        self.providers.list_provider_releases()
    }

    fn list_provider_installations(
        &self,
    ) -> ApplicationFuture<'_, Vec<ProviderInstallationSnapshot>> {
        self.providers.list_provider_installations()
    }

    fn install_provider(
        &self,
        command: InstallProvider,
    ) -> ApplicationFuture<'_, ProviderInstallationSnapshot> {
        self.providers.install_provider(command)
    }

    fn activate_provider_installation(
        &self,
        command: ActivateProviderInstallation,
    ) -> ApplicationFuture<'_, ProviderInstallationSnapshot> {
        self.providers.activate_provider_installation(command)
    }

    fn remove_provider_installation(
        &self,
        command: RemoveProviderInstallation,
    ) -> ApplicationFuture<'_, ()> {
        self.providers.remove_provider_installation(command)
    }
}

fn mapping_lifecycle_error<E>(
    error: MappingLifecycleError<E>,
    content_hash_path: &'static str,
) -> ApplicationError {
    match error {
        MappingLifecycleError::InvalidDocument(error) => {
            let path = if error.code() == "invalid_content_hash" {
                content_hash_path
            } else {
                "mappings"
            };
            ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "mapping command is invalid",
                path,
                error.code(),
                error.to_string(),
            )
        }
        MappingLifecycleError::MappingNotFound => ApplicationError::new(
            ApplicationErrorCode::MappingNotFound,
            "mapping was not found",
        ),
        MappingLifecycleError::RevisionNotFound => ApplicationError::new(
            ApplicationErrorCode::MappingRevisionNotFound,
            "mapping revision was not found",
        ),
        MappingLifecycleError::WorkspaceNotFound => ApplicationError::new(
            ApplicationErrorCode::WorkspaceNotFound,
            "workspace was not found",
        ),
        MappingLifecycleError::LineageCycle => ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "mapping command is invalid",
            "mappings",
            "lineage_cycle",
            "mapping revision lineage contains a cycle",
        ),
        MappingLifecycleError::PackageDocumentSet
        | MappingLifecycleError::RevisionOverflow
        | MappingLifecycleError::HeadMoved { .. }
        | MappingLifecycleError::Repository(_) => ApplicationError::new(
            ApplicationErrorCode::Internal,
            "mapping lifecycle operation failed",
        ),
    }
}
