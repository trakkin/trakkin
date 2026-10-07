#[cfg(feature = "embedded-database")]
use std::path::Path;
use std::{path::PathBuf, sync::Arc, time::Duration};

use thiserror::Error;
use time::OffsetDateTime;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep, timeout_at},
};
use trakkin_mappings_corpus::snapshots::SnapshotSet;
use uuid::Uuid;

use crate::{
    application::task_lifecycle::{InterruptActiveTasks, TaskLifecycleRepository},
    application::task_manager::TaskRepository,
    application::{
        assets::BinaryAssetInteractionService,
        connection::{
            ConnectionCoordinator, ConnectionRepository, ConnectionService, ConnectionStatus,
            OpenConnection, SourceDiscoveryTaskHandler,
        },
        error::ApplicationError,
        facade::{ApplicationApi, ApplicationFacade},
        task_dispatcher::{DispatchNextTask, TaskDispatcher},
        task_manager::TaskService,
        workspaces::WorkspaceService,
    },
    database::{DatabaseError, DatabaseRuntime},
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
        mapping_updates::MappingUpdates,
        mappings::MappingStore,
        provider_access::PostgresProviderAccessRepository,
        provider_registry::{PostgresProviderRegistry, ProbedProviderRegistry},
        workspaces::PostgresWorkspaceRepository,
    },
};

const HOST_MAXIMUM_ASSET_BYTES: u64 = 16 * 1024 * 1024;
const TASK_WORKERS_PER_KIND: usize = 2;
const TASK_IDLE_INTERVAL: Duration = Duration::from_millis(50);
const WORKER_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(5);
const PROVIDER_UPDATE_INTERVAL: Duration = Duration::from_secs(15 * 60);

pub struct Host {
    database: DatabaseRuntime,
    task_repository: PostgresTaskRepository,
    mapping_metadata: Option<PathBuf>,
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

    pub fn task_repository(&self) -> &PostgresTaskRepository {
        &self.task_repository
    }

    pub fn with_mapping_metadata(mut self, path: impl Into<PathBuf>) -> Self {
        self.mapping_metadata = Some(path.into());
        self
    }

    pub async fn into_runtime(
        self,
        data_directory: impl Into<PathBuf>,
        provider_runtime_directory: impl Into<PathBuf>,
        connection_master_key: ConnectionMasterKey,
    ) -> Result<RunningHost, HostError> {
        let pool = self.database.pool();
        let data_directory = data_directory.into();
        let snapshots = if let Some(path) = self.mapping_metadata.clone() {
            let snapshots = tokio::task::spawn_blocking(move || SnapshotSet::load(&path))
                .await
                .map_err(|error| {
                    HostError::Mappings(ApplicationError::internal(format!(
                        "mapping metadata load task failed: {error}"
                    )))
                })?
                .map_err(|error| {
                    HostError::Mappings(ApplicationError::new(
                        crate::application::error::ApplicationErrorCode::InvalidMappingData,
                        format!("invalid mapping metadata: {error}"),
                    ))
                })?;
            Some(snapshots)
        } else {
            None
        };
        let mappings = Arc::new(
            MappingStore::open(
                pool.clone(),
                &data_directory.join("mapping-repositories"),
                snapshots,
            )
            .await
            .map_err(HostError::Mappings)?,
        );
        let mapping_updates = MappingUpdates::new(mappings.clone());
        let provider_registry = PostgresProviderRegistry::new(
            pool.clone(),
            data_directory.join("providers"),
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
        let workspaces = Arc::new(WorkspaceService::new(PostgresWorkspaceRepository::new(
            pool.clone(),
        )));
        let providers = Arc::new(provider_management.clone());
        let application: Arc<dyn ApplicationApi> = Arc::new(ApplicationFacade {
            workspaces: workspaces.clone(),
            catalog: Arc::new(PostgresCatalogQueryRepository::new(pool.clone())),
            tasks: Arc::new(TaskService::new(
                PostgresTaskRepository::new(pool.clone()),
                workspaces,
            )),
            connections: Arc::new(ConnectionService::new(
                connection_repository.clone(),
                sessions.clone(),
                providers.clone(),
            )),
            connection_interaction: Arc::new(ConnectionCoordinator::new(
                connection_repository.clone(),
                launcher.clone(),
                sessions.clone(),
            )),
            binary_assets: Arc::new(BinaryAssetInteractionService::<_, _, AdapterProcess>::new(
                provider_access.clone(),
                sessions.clone(),
                HOST_MAXIMUM_ASSET_BYTES,
            )),
            providers,
            mappings,
        });
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let execution = crate::application::task_dispatcher::ConnectionExecutionLocks::default();
        let mut workers = Vec::with_capacity(TASK_WORKERS_PER_KIND * 2 + 2);
        for _ in 0..TASK_WORKERS_PER_KIND {
            let discovery = TaskDispatcher::new(
                self.task_repository.clone(),
                SourceDiscoveryTaskHandler::new(ConnectionCoordinator::new(
                    connection_repository.clone(),
                    launcher.clone(),
                    sessions.clone(),
                )),
            );
            workers.push(spawn_dispatcher(
                discovery.with_execution_locks(execution.clone()),
                shutdown_receiver.clone(),
            )?);

            let acquisition = TaskDispatcher::new(
                self.task_repository.clone(),
                AcquisitionTaskHandler::new(
                    PostgresPublicationRepository::new(pool.clone()),
                    self.task_repository.clone(),
                    sessions.clone(),
                )
                .with_shutdown(shutdown_receiver.clone()),
            );
            workers.push(spawn_dispatcher(
                acquisition.with_execution_locks(execution.clone()),
                shutdown_receiver.clone(),
            )?);
        }
        workers.push(tokio::spawn(run_provider_updates(
            provider_management,
            shutdown_receiver.clone(),
        )));
        workers.push(tokio::spawn(mapping_updates.run(shutdown_receiver.clone())));
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
        let task_repository = PostgresTaskRepository::new(database.pool());
        Ok(Self {
            database,
            task_repository,
            mapping_metadata: None,
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
    #[error("mapping startup recovery failed: {0}")]
    Mappings(#[source] ApplicationError),
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
