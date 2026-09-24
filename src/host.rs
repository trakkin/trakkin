#[cfg(feature = "embedded-database")]
use std::path::Path;
use std::{path::PathBuf, sync::Arc, time::Duration};

use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep, timeout_at},
};
use uuid::Uuid;

use crate::{
    application::task_lifecycle::{InterruptActiveTasks, TaskLifecycleRepository},
    application::task_manager::{TaskManager, TaskRepository},
    application::{
        assets::BinaryAssetInteractionService,
        connection::{
            ConnectionCoordinator, ConnectionRepository, ConnectionStatus, OpenConnection,
            SourceDiscoveryTaskHandler,
        },
        error::ApplicationError,
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
            ConnectionMasterKey, ConnectionPersistenceError, ConnectionSessionRetirementError,
            PostgresConnectionRepository, ProcessSessionRegistry,
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
const WORKER_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(5);
const SCHEDULE_IDLE_INTERVAL: Duration = Duration::from_secs(30);
const SCHEDULE_PREVIEW_LIFETIME: TimeDuration = TimeDuration::minutes(5);
const PROVIDER_UPDATE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const RETENTION_IDLE_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RETENTION_PASSES_PER_SWEEP: usize = 16;

pub struct Host {
    database: DatabaseRuntime,
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
            .map_err(HostError::ProviderRegistry)?;
        let connection_repository =
            PostgresConnectionRepository::new(pool.clone(), connection_master_key);
        let sessions = ProcessSessionRegistry::<AdapterProcess>::default();
        let launcher =
            AdapterProcessLauncher::new(provider_registry.clone(), env!("CARGO_PKG_VERSION"));
        let recoverable_connections = connection_repository
            .list_connections()
            .await
            .map_err(HostError::ConnectionRecovery)?;
        let recovery = ConnectionCoordinator::new(
            connection_repository.clone(),
            launcher.clone(),
            sessions.clone(),
        );
        let mut recovery_attempted = 0_u64;
        let mut recovery_succeeded = 0_u64;
        for connection in recoverable_connections
            .into_iter()
            .filter(|connection| connection.status == ConnectionStatus::Ready)
        {
            recovery_attempted += 1;
            let connection_id = connection.connection_id;
            let process_instance_id = format!("startup-recovery-{}", random_uuid()?);
            if recovery
                .open(OpenConnection {
                    connection_id,
                    process_instance_id,
                    opened_at: OffsetDateTime::now_utc(),
                })
                .await
                .is_ok()
            {
                recovery_succeeded += 1;
            } else {
                tracing::warn!(%connection_id, "connection recovery failed");
            }
        }
        if recovery_attempted > 0 {
            tracing::info!(
                recovery.attempted = recovery_attempted,
                recovery.succeeded = recovery_succeeded,
                recovery.failed = recovery_attempted - recovery_succeeded,
                "connection recovery completed"
            );
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
            workers.push(spawn_dispatcher(discovery, shutdown_receiver.clone())?);

            let acquisition = TaskDispatcher::new(
                self.task_manager.clone(),
                AcquisitionTaskHandler::new(
                    PostgresPublicationRepository::new(pool.clone()),
                    self.task_manager.clone(),
                    sessions.clone(),
                )
                .with_shutdown(shutdown_receiver.clone()),
            );
            workers.push(spawn_dispatcher(acquisition, shutdown_receiver.clone())?);

            let synchronization = TaskDispatcher::new(
                self.task_manager.clone(),
                SyncActionTaskHandler::new(
                    PostgresSyncActionAttemptRepository::new(pool.clone()),
                    sessions.clone(),
                ),
            );
            workers.push(spawn_dispatcher(
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
            workers.push(spawn_dispatcher(reconciliation, shutdown_receiver.clone())?);
        }
        workers.push(tokio::spawn(run_schedules(
            sync_schedule_runner,
            shutdown_receiver.clone(),
        )));
        workers.push(tokio::spawn(run_provider_updates(
            provider_management,
            shutdown_receiver.clone(),
        )));
        workers.push(tokio::spawn(run_retention(
            PostgresSyncRetentionRepository::new(pool),
            shutdown_receiver.clone(),
        )));
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

    async fn interrupt_active_tasks(
        &self,
        interrupted_at: OffsetDateTime,
    ) -> Result<(), HostError> {
        PostgresTaskLifecycleRepository::new(self.database.pool())
            .interrupt_active_tasks(InterruptActiveTasks { interrupted_at })
            .await?;
        Ok(())
    }

    async fn compose(
        database: DatabaseRuntime,
        started_at: OffsetDateTime,
    ) -> Result<Self, HostError> {
        let lifecycle = PostgresTaskLifecycleRepository::new(database.pool());
        lifecycle
            .interrupt_active_tasks(InterruptActiveTasks {
                interrupted_at: started_at,
            })
            .await?;
        let task_manager = TaskManager::new(PostgresTaskRepository::new(database.pool()));
        Ok(Self {
            database,
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
        let worker_count = self.workers.len();
        let session_count = self.sessions.len();
        tracing::info!(worker_count, session_count, "shutting down host runtime");
        let _ = self.shutdown_sender.send(true);
        let worker_shutdown = finish_workers(self.workers, WORKER_SHUTDOWN_GRACE_PERIOD).await;
        if worker_shutdown.failed > 0 || worker_shutdown.aborted > 0 {
            tracing::warn!(
                failed_workers = worker_shutdown.failed,
                aborted_workers = worker_shutdown.aborted,
                "background workers did not stop cleanly"
            );
        }
        self.host
            .interrupt_active_tasks(OffsetDateTime::now_utc())
            .await?;
        self.sessions.retire_all().await.inspect_err(|_| {
            tracing::error!("provider sessions could not be retired");
        })?;
        self.host.shutdown().await.inspect_err(|_| {
            tracing::error!("database shutdown failed");
        })?;
        tracing::info!(worker_count, session_count, "host runtime stopped");
        Ok(())
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct WorkerShutdown {
    failed: usize,
    aborted: usize,
}

async fn finish_workers(
    mut workers: Vec<JoinHandle<()>>,
    grace_period: Duration,
) -> WorkerShutdown {
    let deadline = Instant::now() + grace_period;
    let mut result = WorkerShutdown::default();
    while let Some(mut worker) = workers.pop() {
        match timeout_at(deadline, &mut worker).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => result.failed += 1,
            Err(_) => {
                worker.abort();
                classify_aborted_worker(worker.await, &mut result);
                for worker in workers {
                    worker.abort();
                    classify_aborted_worker(worker.await, &mut result);
                }
                break;
            }
        }
    }
    result
}

fn classify_aborted_worker(
    result: Result<(), tokio::task::JoinError>,
    shutdown: &mut WorkerShutdown,
) {
    match result {
        Ok(()) => {}
        Err(error) if error.is_cancelled() => shutdown.aborted += 1,
        Err(_) => shutdown.failed += 1,
    }
}

fn spawn_dispatcher<R, H>(
    dispatcher: TaskDispatcher<R, H>,
    shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<()>, getrandom::Error>
where
    R: TaskRepository + Send + Sync + 'static,
    R::Error: Send + std::fmt::Debug,
    H: crate::application::task_dispatcher::TaskHandler + Send + Sync + 'static,
    H::Error: Send + std::fmt::Debug + std::fmt::Display,
{
    let owner_id = random_uuid()?;
    Ok(tokio::spawn(run_dispatcher(dispatcher, owner_id, shutdown)))
}

async fn run_dispatcher<R, H>(
    dispatcher: TaskDispatcher<R, H>,
    owner_id: Uuid,
    mut shutdown: watch::Receiver<bool>,
) where
    R: TaskRepository,
    R::Error: std::fmt::Debug,
    H: crate::application::task_dispatcher::TaskHandler,
    H::Error: std::fmt::Debug + std::fmt::Display,
{
    let accepted_kinds = dispatcher.accepted_kinds();
    let mut healthy = true;
    while !*shutdown.borrow() {
        let Ok(attempt_id) = random_uuid() else {
            tracing::error!(?accepted_kinds, "task worker stopped");
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
            Ok(result) => {
                if !healthy {
                    tracing::info!(?accepted_kinds, "task worker recovered");
                    healthy = true;
                }
                if result.is_some() {
                    continue;
                }
            }
            Err(_) => {
                if healthy {
                    tracing::warn!(?accepted_kinds, "task worker dispatch failed");
                    healthy = false;
                }
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
    let mut healthy = true;
    while !*shutdown.borrow() {
        let Ok(occurrence_id) = random_uuid() else {
            tracing::error!(worker.name = "sync_schedule", "background worker stopped");
            return;
        };
        let Ok(preview_id) = random_uuid() else {
            tracing::error!(worker.name = "sync_schedule", "background worker stopped");
            return;
        };
        let Ok(run_id) = random_uuid() else {
            tracing::error!(worker.name = "sync_schedule", "background worker stopped");
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
            Ok(result) => {
                if !healthy {
                    tracing::info!(worker.name = "sync_schedule", "background worker recovered");
                    healthy = true;
                }
                match result {
                    SyncScheduleTickResult::RunStarted { occurrence_id, run } => {
                        tracing::info!(
                            %occurrence_id,
                            sync.run.id = %run.run_id,
                            workspace.id = %run.workspace_id,
                            action_count = run.actions.len(),
                            "scheduled synchronization started"
                        );
                        continue;
                    }
                    SyncScheduleTickResult::NoChanges { .. }
                    | SyncScheduleTickResult::RunAlreadyStarted { .. } => continue,
                    SyncScheduleTickResult::Idle => {}
                }
            }
            Err(_) => {
                if healthy {
                    tracing::warn!(worker.name = "sync_schedule", "background worker failed");
                    healthy = false;
                }
            }
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
                    let mut deleted_rows = 0_u32;
                    let mut deferred = false;
                    for _ in 0..RETENTION_PASSES_PER_SWEEP {
                        let report = match service
                            .retain(RetainSyncEvidence {
                                workspace_id,
                                retained_at: OffsetDateTime::now_utc(),
                                policy,
                            })
                            .await
                        {
                            Ok(report) => report,
                            Err(_) => {
                                tracing::warn!(
                                    workspace.id = %workspace_id,
                                    "synchronization retention failed"
                                );
                                break;
                            }
                        };
                        deleted_rows = deleted_rows.saturating_add(report.deleted_rows());
                        deferred |= report.deferred;
                        if !report.deferred {
                            break;
                        }
                        if let Some(minimum) = report.next_batch_minimum_rows {
                            policy.maximum_deleted_rows = policy.maximum_deleted_rows.max(minimum);
                        }
                        tokio::task::yield_now().await;
                    }
                    if deleted_rows > 0 || deferred {
                        tracing::info!(
                            workspace.id = %workspace_id,
                            deleted_rows,
                            deferred,
                            "synchronization retention completed"
                        );
                    }
                }
            }
            Err(_) => tracing::warn!("could not enumerate workspaces for retention"),
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
    ProviderRegistry(#[source] ApplicationError),
    #[error("connection session recovery failed: {0}")]
    ConnectionRecovery(#[source] ConnectionPersistenceError),
    #[error("one or more adapter processes could not be retired")]
    ConnectionRetirement(#[from] ConnectionSessionRetirementError),
}

#[cfg(test)]
mod tests {
    use std::{future::pending, time::Duration};

    use super::{WorkerShutdown, finish_workers};

    #[tokio::test]
    async fn worker_shutdown_aborts_tasks_that_miss_the_deadline() {
        let completed = tokio::spawn(async {});
        let stalled = tokio::spawn(pending());

        let result = finish_workers(vec![stalled, completed], Duration::from_millis(10)).await;

        assert_eq!(
            result,
            WorkerShutdown {
                failed: 0,
                aborted: 1,
            }
        );
    }
}
