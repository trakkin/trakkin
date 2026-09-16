#[cfg(feature = "embedded-database")]
use std::path::Path;
use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{sync::watch, task::JoinHandle, time::sleep};
use uuid::Uuid;

use crate::{
    application::task_lifecycle::{
        InterruptActiveTasks, InterruptionSummary, TaskLifecycleRepository,
    },
    application::task_manager::{TaskManager, TaskRepository, TaskStatus},
    application::{
        assets::BinaryAssetInteractionService,
        connection::{
            ConnectionCoordinator, ConnectionRepository, ConnectionStatus, OpenConnection,
            SourceDiscoveryTaskHandler,
        },
        facade::{
            ApplicationApi, ApplicationFacade, ApplicationInteractions,
            ApplicationMappingRepositories,
        },
        mapping_resolution::{AdapterMappingEndpointLookupService, MappingResolutionService},
        reference_lookup::ReferenceLookupInteractionService,
        sync_activity::SyncActivityService,
        sync_policy::SyncPolicyService,
        sync_retention::{RetainSyncEvidence, SyncRetentionPolicy, SyncRetentionService},
        sync_schedule::SyncScheduleService,
        sync_schedule::{SyncScheduleRunner, SyncScheduleTickResult, TickSyncSchedules},
        sync_workflow::{
            ProductionSyncPreviewInputCollector, SyncRunWorkflowService, SyncWorkflowService,
        },
        task_dispatcher::{DispatchNextTask, TaskDispatcher},
    },
    database::{DatabaseError, DatabaseRuntime},
    domain::synchronization::StateRegistry,
    infrastructure::task_lifecycle::{PostgresTaskLifecycleRepository, TaskLifecycleError},
    infrastructure::task_manager::PostgresTaskRepository,
    infrastructure::{
        acquisition_dispatch::AcquisitionTaskHandler,
        adapter_process::{AdapterProcess, AdapterProcessLauncher},
        catalog_publication::PostgresPublicationRepository,
        catalog_queries::PostgresCatalogQueryRepository,
        connection::{
            ConnectionMasterKey, ConnectionSessionRetirementError, PostgresConnectionRepository,
            ProcessSessionRegistry,
        },
        mapping_lifecycle::PostgresMappingLifecycleRepository,
        mapping_queries::PostgresMappingQueryRepository,
        mapping_resolution::PostgresMappingResolutionRepository,
        provider_access::PostgresProviderAccessRepository,
        provider_registry::{PostgresProviderRegistry, ProbedProviderRegistry},
        sync_action_attempt::PostgresSyncActionAttemptRepository,
        sync_action_dispatch::SyncActionTaskHandler,
        sync_action_reconciliation::PostgresSyncActionReconciliationRepository,
        sync_action_reconciliation_dispatch::SyncActionReconciliationTaskHandler,
        sync_activity::PostgresSyncActivityRepository,
        sync_policy::PostgresSyncPolicyRepository,
        sync_preview::PostgresSyncPreviewRepository,
        sync_preview_collection::PostgresSyncReceiverPreflightService,
        sync_retention::PostgresSyncRetentionRepository,
        sync_run::PostgresSyncRunRepository,
        sync_schedule::PostgresSyncScheduleRepository,
        sync_state_evidence::PostgresSyncStateEvidenceRepository,
        workspaces::PostgresWorkspaceRepository,
    },
};

const HOST_MAXIMUM_ASSET_BYTES: u64 = 16 * 1024 * 1024;
const TASK_WORKERS_PER_KIND: usize = 2;
const TASK_IDLE_INTERVAL: Duration = Duration::from_millis(50);
const SCHEDULE_IDLE_INTERVAL: Duration = Duration::from_secs(30);
const SCHEDULE_PREVIEW_LIFETIME: TimeDuration = TimeDuration::minutes(5);
const PROVIDER_UPDATE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const RETENTION_IDLE_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RETENTION_PASSES_PER_SWEEP: usize = 16;

pub struct Host {
    database: DatabaseRuntime,
    startup_recovery: InterruptionSummary,
    task_manager: TaskManager<PostgresTaskRepository>,
}

impl Host {
    #[cfg(feature = "embedded-database")]
    pub async fn start(
        data_directory: &Path,
        started_at: OffsetDateTime,
    ) -> Result<Self, HostError> {
        let database = DatabaseRuntime::start(data_directory).await?;
        Self::compose(database, started_at).await
    }

    pub async fn connect(
        database_url: &str,
        started_at: OffsetDateTime,
    ) -> Result<Self, HostError> {
        let database = DatabaseRuntime::connect(database_url).await?;
        Self::compose(database, started_at).await
    }

    pub fn startup_recovery(&self) -> &InterruptionSummary {
        &self.startup_recovery
    }

    pub fn task_manager(&self) -> &TaskManager<PostgresTaskRepository> {
        &self.task_manager
    }

    pub async fn into_runtime(
        self,
        data_directory: impl Into<PathBuf>,
        provider_runtime_directory: impl Into<PathBuf>,
        connection_master_key: ConnectionMasterKey,
    ) -> Result<RunningHost, HostError> {
        let pool = self.database.pool();
        let provider_registry = PostgresProviderRegistry::new(
            pool.clone(),
            data_directory.into().join("providers"),
            provider_runtime_directory,
        );
        provider_registry
            .reconcile(OffsetDateTime::now_utc())
            .await
            .map_err(|error| HostError::ProviderRegistry(error.message))?;
        let connection_repository =
            PostgresConnectionRepository::new(pool.clone(), connection_master_key);
        let sessions = ProcessSessionRegistry::<AdapterProcess>::default();
        let launcher =
            AdapterProcessLauncher::new(provider_registry.clone(), env!("CARGO_PKG_VERSION"));
        let recoverable_connections = connection_repository
            .list_connections()
            .await
            .map_err(|error| HostError::ConnectionRecovery(format!("{error:?}")))?;
        let recovery = ConnectionCoordinator::new(
            connection_repository.clone(),
            launcher.clone(),
            sessions.clone(),
        );
        for connection in recoverable_connections
            .into_iter()
            .filter(|connection| connection.status == ConnectionStatus::Ready)
        {
            let connection_id = connection.connection_id;
            let process_instance_id = format!("startup-recovery-{}", random_uuid()?);
            match recovery
                .open(OpenConnection {
                    connection_id,
                    process_instance_id,
                    opened_at: OffsetDateTime::now_utc(),
                })
                .await
            {
                Ok(opened) => tracing::info!(
                    event = "connection.session.recovered",
                    connection.id = %connection_id,
                    connection.accounts = opened.account_count,
                    connection.sources = opened.source_count,
                ),
                Err(error) => tracing::error!(
                    event = "connection.session.recovery.failed",
                    connection.id = %connection_id,
                    error = ?error,
                ),
            }
        }
        let provider_management =
            ProbedProviderRegistry::new(provider_registry.clone(), env!("CARGO_PKG_VERSION"));
        let provider_access = PostgresProviderAccessRepository::new(pool.clone());
        let sync_preview_collector = Arc::new(ProductionSyncPreviewInputCollector::new(
            PostgresSyncStateEvidenceRepository::new(pool.clone()),
            MappingResolutionService::with_endpoint_lookup(
                PostgresMappingResolutionRepository::new(pool.clone()),
                AdapterMappingEndpointLookupService::<_, _, AdapterProcess>::new(
                    provider_access.clone(),
                    sessions.clone(),
                ),
            ),
            PostgresSyncReceiverPreflightService::<_, AdapterProcess>::new(
                pool.clone(),
                sessions.clone(),
            ),
            StateRegistry::v1(),
        ));
        let sync_workflow = Arc::new(SyncWorkflowService::new(
            PostgresSyncPolicyRepository::new(pool.clone()),
            sync_preview_collector.clone(),
            PostgresSyncPreviewRepository::new(pool.clone()),
            StateRegistry::v1(),
        ));
        let sync_run_workflow = Arc::new(SyncRunWorkflowService::new(
            PostgresSyncPolicyRepository::new(pool.clone()),
            sync_preview_collector.clone(),
            PostgresSyncPreviewRepository::new(pool.clone()),
            PostgresSyncRunRepository::new(pool.clone()),
        ));
        let sync_schedule_runner = SyncScheduleRunner::new(
            PostgresSyncScheduleRepository::new(pool.clone()),
            PostgresSyncPolicyRepository::new(pool.clone()),
            sync_preview_collector.clone(),
            PostgresSyncPreviewRepository::new(pool.clone()),
            PostgresSyncRunRepository::new(pool.clone()),
        );
        let application: Arc<dyn ApplicationApi> = Arc::new(ApplicationFacade::new(
            PostgresWorkspaceRepository::new(pool.clone()),
            PostgresCatalogQueryRepository::new(pool.clone()),
            ApplicationMappingRepositories::new(
                PostgresMappingQueryRepository::new(pool.clone()),
                PostgresMappingLifecycleRepository::new(pool.clone()),
            ),
            PostgresTaskRepository::new(pool.clone()),
            connection_repository.clone(),
            sessions.clone(),
            ApplicationInteractions::new(
                Arc::new(ConnectionCoordinator::new(
                    connection_repository.clone(),
                    launcher.clone(),
                    sessions.clone(),
                )),
                Arc::new(BinaryAssetInteractionService::<_, _, AdapterProcess>::new(
                    provider_access.clone(),
                    sessions.clone(),
                    HOST_MAXIMUM_ASSET_BYTES,
                )),
                Arc::new(
                    ReferenceLookupInteractionService::<_, _, AdapterProcess>::new(
                        provider_access.clone(),
                        sessions.clone(),
                    ),
                ),
                Arc::new(MappingResolutionService::with_endpoint_lookup(
                    PostgresMappingResolutionRepository::new(pool.clone()),
                    AdapterMappingEndpointLookupService::<_, _, AdapterProcess>::new(
                        provider_access,
                        sessions.clone(),
                    ),
                )),
                Arc::new(SyncPolicyService::new(PostgresSyncPolicyRepository::new(
                    pool.clone(),
                ))),
                Arc::new(SyncActivityService::new(
                    PostgresSyncActivityRepository::new(pool.clone()),
                )),
                Arc::new(provider_management.clone()),
            )
            .with_sync_preview_mutation(sync_workflow)
            .with_sync_run_mutation(sync_run_workflow)
            .with_sync_schedule(Arc::new(SyncScheduleService::new(
                PostgresSyncScheduleRepository::new(pool.clone()),
            ))),
        ));
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let mut workers = Vec::with_capacity(TASK_WORKERS_PER_KIND * 4 + 3);
        for _ in 0..TASK_WORKERS_PER_KIND {
            let discovery = TaskDispatcher::new(
                self.task_manager.clone(),
                SourceDiscoveryTaskHandler::new(ConnectionCoordinator::new(
                    connection_repository.clone(),
                    launcher.clone(),
                    sessions.clone(),
                )),
            );
            workers.push(spawn_dispatcher(
                "source-discovery",
                discovery,
                shutdown_receiver.clone(),
            )?);

            let acquisition = TaskDispatcher::new(
                self.task_manager.clone(),
                AcquisitionTaskHandler::new(
                    PostgresPublicationRepository::new(pool.clone()),
                    self.task_manager.clone(),
                    sessions.clone(),
                ),
            );
            workers.push(spawn_dispatcher(
                "acquisition",
                acquisition,
                shutdown_receiver.clone(),
            )?);

            let synchronization = TaskDispatcher::new(
                self.task_manager.clone(),
                SyncActionTaskHandler::new(
                    PostgresSyncActionAttemptRepository::new(pool.clone()),
                    sessions.clone(),
                ),
            );
            workers.push(spawn_dispatcher(
                "synchronization",
                synchronization,
                shutdown_receiver.clone(),
            )?);

            let reconciliation = TaskDispatcher::new(
                self.task_manager.clone(),
                SyncActionReconciliationTaskHandler::new(
                    PostgresSyncActionReconciliationRepository::new(pool.clone()),
                    sessions.clone(),
                ),
            );
            workers.push(spawn_dispatcher(
                "reconciliation",
                reconciliation,
                shutdown_receiver.clone(),
            )?);
        }
        workers.push(spawn_supervised(
            "schedules",
            run_schedules(sync_schedule_runner, shutdown_receiver.clone()),
        ));
        workers.push(spawn_supervised(
            "provider-updates",
            run_provider_updates(provider_management, shutdown_receiver.clone()),
        ));
        workers.push(spawn_supervised(
            "retention",
            run_retention(
                PostgresSyncRetentionRepository::new(pool),
                shutdown_receiver.clone(),
            ),
        ));
        Ok(RunningHost {
            host: self,
            application,
            sessions,
            shutdown_sender,
            workers,
        })
    }

    pub async fn shutdown(self) -> Result<(), HostError> {
        self.database.shutdown().await?;
        Ok(())
    }

    async fn compose(
        database: DatabaseRuntime,
        started_at: OffsetDateTime,
    ) -> Result<Self, HostError> {
        let lifecycle = PostgresTaskLifecycleRepository::new(database.pool());
        let startup_recovery = lifecycle
            .interrupt_active_tasks(InterruptActiveTasks {
                interrupted_at: started_at,
            })
            .await?;
        let task_manager = TaskManager::new(PostgresTaskRepository::new(database.pool()));
        Ok(Self {
            database,
            startup_recovery,
            task_manager,
        })
    }
}

pub struct RunningHost {
    host: Host,
    application: Arc<dyn ApplicationApi>,
    sessions: ProcessSessionRegistry<AdapterProcess>,
    shutdown_sender: watch::Sender<bool>,
    workers: Vec<JoinHandle<()>>,
}

impl RunningHost {
    pub fn application(&self) -> Arc<dyn ApplicationApi> {
        Arc::clone(&self.application)
    }

    pub async fn shutdown(self) -> Result<(), HostError> {
        let _ = self.shutdown_sender.send(true);
        for worker in self.workers {
            if let Err(error) = worker.await {
                tracing::error!(event = "worker.join.failed", error = ?error);
            }
        }
        if let Err(error) = self.sessions.retire_all().await {
            tracing::error!(event = "host.shutdown.providers.failed", error = ?error);
            return Err(error.into());
        }
        let shutdown = self.host.shutdown().await;
        if let Err(error) = &shutdown {
            tracing::error!(event = "host.shutdown.database.failed", error = ?error);
        }
        shutdown
    }
}

fn spawn_dispatcher<R, H>(
    name: &'static str,
    dispatcher: TaskDispatcher<R, H>,
    shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<()>, getrandom::Error>
where
    R: TaskRepository + Send + Sync + 'static,
    R::Error: Send + std::fmt::Debug,
    H: crate::application::task_dispatcher::TaskHandler + Send + Sync + 'static,
    H::Error: Send + std::fmt::Debug,
{
    let owner_id = random_uuid()?;
    Ok(spawn_supervised(
        name,
        run_dispatcher(dispatcher, owner_id, shutdown),
    ))
}

fn spawn_supervised<F>(name: &'static str, worker: F) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let task = tokio::spawn(worker);
        match task.await {
            Ok(()) => tracing::info!(event = "worker.exited", worker.name = name),
            Err(error) => tracing::error!(
                event = "worker.panicked",
                worker.name = name,
                error = ?error,
            ),
        }
    })
}

async fn run_dispatcher<R, H>(
    dispatcher: TaskDispatcher<R, H>,
    owner_id: Uuid,
    mut shutdown: watch::Receiver<bool>,
) where
    R: TaskRepository,
    R::Error: std::fmt::Debug,
    H: crate::application::task_dispatcher::TaskHandler,
    H::Error: std::fmt::Debug,
{
    while !*shutdown.borrow() {
        let Ok(attempt_id) = random_uuid() else {
            tracing::error!(event = "task.dispatcher.random_id.failed", %owner_id);
            return;
        };
        let now = OffsetDateTime::now_utc();
        let dispatch = dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id,
                attempt_id,
                claimed_at: now,
                completed_at: now,
            })
            .await;
        match dispatch {
            Ok(Some(task)) => {
                match &task.status {
                    TaskStatus::Queued => tracing::warn!(
                        event = "task.retry.queued",
                        %owner_id,
                        %attempt_id,
                        task.id = %task.id,
                    ),
                    TaskStatus::Failed => tracing::error!(
                        event = "task.failed",
                        %owner_id,
                        %attempt_id,
                        task.id = %task.id,
                        failure.code = task
                            .failure
                            .as_ref()
                            .map(|failure| failure.code.as_str())
                            .unwrap_or("unavailable"),
                        failure.message = task
                            .failure
                            .as_ref()
                            .map(|failure| failure.message.as_str())
                            .unwrap_or("unavailable"),
                        failure.retryable = task
                            .failure
                            .as_ref()
                            .is_some_and(|failure| failure.retryable),
                        diagnostic.id = task
                            .failure
                            .as_ref()
                            .and_then(|failure| failure.diagnostic_id.as_deref())
                            .unwrap_or("unavailable"),
                    ),
                    _ => tracing::info!(
                        event = "task.dispatched",
                        %owner_id,
                        %attempt_id,
                        task.id = %task.id,
                        task.status = ?task.status,
                    ),
                }
                continue;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(
                    event = "task.dispatch.failed",
                    %owner_id,
                    %attempt_id,
                    error = ?error,
                );
            }
        }
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() {
                    return;
                }
            }
            () = sleep(TASK_IDLE_INTERVAL) => {}
        }
    }
}

async fn run_schedules<C>(
    runner: SyncScheduleRunner<
        PostgresSyncScheduleRepository,
        PostgresSyncPolicyRepository,
        C,
        PostgresSyncPreviewRepository,
        PostgresSyncRunRepository,
    >,
    mut shutdown: watch::Receiver<bool>,
) where
    C: crate::application::sync_workflow::SyncPreviewInputCollector + Clone,
    C::Error: std::fmt::Debug,
{
    while !*shutdown.borrow() {
        let Ok(occurrence_id) = random_uuid() else {
            tracing::error!(event = "schedule.random_id.failed", id.kind = "occurrence");
            return;
        };
        let Ok(preview_id) = random_uuid() else {
            tracing::error!(event = "schedule.random_id.failed", id.kind = "preview");
            return;
        };
        let Ok(run_id) = random_uuid() else {
            tracing::error!(event = "schedule.random_id.failed", id.kind = "run");
            return;
        };
        let observed_at = OffsetDateTime::now_utc();
        let result = runner
            .tick(TickSyncSchedules {
                occurrence_id,
                preview_id,
                run_id,
                observed_at,
                preview_expires_at: observed_at + SCHEDULE_PREVIEW_LIFETIME,
            })
            .await;
        match result {
            Ok(
                SyncScheduleTickResult::NoChanges { .. }
                | SyncScheduleTickResult::RunStarted { .. }
                | SyncScheduleTickResult::RunAlreadyStarted { .. },
            ) => continue,
            Ok(SyncScheduleTickResult::Idle) => {}
            Err(error) => tracing::error!(
                event = "schedule.tick.failed",
                %occurrence_id,
                %preview_id,
                %run_id,
                error = ?error,
            ),
        }
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() {
                    return;
                }
            }
            () = sleep(SCHEDULE_IDLE_INTERVAL) => {}
        }
    }
}

async fn run_retention(
    repository: PostgresSyncRetentionRepository,
    mut shutdown: watch::Receiver<bool>,
) {
    while !*shutdown.borrow() {
        match repository.workspace_ids().await {
            Ok(workspace_ids) => {
                let service = SyncRetentionService::new(repository.clone());
                for workspace_id in workspace_ids {
                    let mut policy = SyncRetentionPolicy::default();
                    for _ in 0..RETENTION_PASSES_PER_SWEEP {
                        let result = service
                            .retain(RetainSyncEvidence {
                                workspace_id,
                                retained_at: OffsetDateTime::now_utc(),
                                policy,
                            })
                            .await;
                        let report = match result {
                            Ok(report) => report,
                            Err(error) => {
                                tracing::error!(
                                    event = "retention.workspace.failed",
                                    %workspace_id,
                                    error = ?error,
                                );
                                break;
                            }
                        };
                        if !report.deferred {
                            break;
                        }
                        if let Some(minimum) = report.next_batch_minimum_rows {
                            policy.maximum_deleted_rows = policy.maximum_deleted_rows.max(minimum);
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }
            Err(error) => tracing::error!(event = "retention.workspaces.failed", error = ?error),
        }
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() {
                    return;
                }
            }
            () = sleep(RETENTION_IDLE_INTERVAL) => {}
        }
    }
}

async fn run_provider_updates(
    registry: ProbedProviderRegistry,
    mut shutdown: watch::Receiver<bool>,
) {
    while !*shutdown.borrow() {
        registry
            .refresh_all_provider_sources(OffsetDateTime::now_utc())
            .await;
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() {
                    return;
                }
            }
            () = sleep(PROVIDER_UPDATE_INTERVAL) => {}
        }
    }
}

fn random_uuid() -> Result<Uuid, getrandom::Error> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

#[derive(Debug, Error)]
pub enum HostError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    TaskLifecycle(#[from] TaskLifecycleError),
    #[error(transparent)]
    Random(#[from] getrandom::Error),
    #[error("provider registry startup failed: {0}")]
    ProviderRegistry(String),
    #[error("connection session recovery failed: {0}")]
    ConnectionRecovery(String),
    #[error("one or more adapter processes could not be retired")]
    ConnectionRetirement(#[from] ConnectionSessionRetirementError),
}
