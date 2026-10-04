use std::{convert::Infallible, future::Future, pin::Pin};

use time::OffsetDateTime;
use trakkin_provider_sdk::v1::{AuthenticationStatus, HealthStatus};
use uuid::Uuid;

use super::adapter_runtime::{
    AdapterAuthenticationMethod, AdapterAuthenticationStep, AdapterCallError,
    AdapterConnectionDescription, AdapterConnectionValidation, AdapterIdentity, AdapterLauncher,
    AdapterRetirement, AdapterSecret, AdapterSession, AdapterSetting,
    ContinueAdapterAuthentication, DiscoveredAdapterSources, LaunchAdapter, OpenAdapterConnection,
    OpenedAdapterConnection, StartAdapterAuthentication, ValidateAdapterConnection,
};
use super::{
    error::{ApplicationError, ApplicationErrorCode, ApplicationFuture, IntoApplicationError},
    providers::SharedProviderRegistry,
    task_dispatcher::{TaskHandler, TaskHandlerFuture, TaskHandlerOutcome},
    task_manager::{ClaimedTask, TaskCompletion, TaskDefinition, TaskFailure, TaskKind},
};

pub type ConnectionFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;
pub type ConnectionSessionFuture<'a, S> = Pin<Box<dyn Future<Output = S> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateConnection {
    pub connection_id: Uuid,
    pub provider_id: String,
    pub display_name: String,
    pub settings: Vec<AdapterSetting>,
    pub secrets: Vec<AdapterSecret>,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredConnection {
    pub connection_id: Uuid,
    pub provider_id: String,
    pub settings: Vec<AdapterSetting>,
    pub secrets: Vec<AdapterSecret>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionStatus {
    Pending,
    Ready,
    Unavailable,
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionSummary {
    pub connection_id: Uuid,
    pub provider_id: String,
    pub display_name: String,
    pub status: ConnectionStatus,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameConnection {
    pub connection_id: Uuid,
    pub display_name: String,
    pub renamed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconfigureConnection {
    pub connection_id: Uuid,
    pub settings: Vec<AdapterSetting>,
    pub secret_patches: Vec<super::adapter_runtime::AdapterSecretPatch>,
    pub reconfigured_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConnectionConfigurationError {
    #[error("connection {0} key is empty")]
    EmptyKey(&'static str),
    #[error("connection {0} key is duplicated")]
    DuplicateKey(&'static str),
}

impl IntoApplicationError for ConnectionConfigurationError {
    fn into_application_error(self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            "connection configuration is invalid",
        )
    }
}

pub fn validate_named_values<'a>(
    values: impl IntoIterator<Item = &'a str>,
    resource: &'static str,
) -> Result<(), ConnectionConfigurationError> {
    let mut seen = std::collections::HashSet::new();
    for value in values {
        if value.is_empty() {
            return Err(ConnectionConfigurationError::EmptyKey(resource));
        }
        if !seen.insert(value) {
            return Err(ConnectionConfigurationError::DuplicateKey(resource));
        }
    }
    Ok(())
}

impl ReconfigureConnection {
    pub fn validate(&self) -> Result<(), ConnectionConfigurationError> {
        validate_named_values(
            self.settings.iter().map(|setting| setting.key.as_str()),
            "setting",
        )?;
        validate_named_values(
            self.secret_patches.iter().map(|patch| patch.key.as_str()),
            "secret patch",
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionSourceSummary {
    pub source_id: Uuid,
    pub account_id: Uuid,
    pub account_display_name: String,
    pub display_name: String,
    pub kind: super::adapter_runtime::AdapterTerm,
    pub availability: super::adapter_runtime::AdapterSourceAvailability,
    pub capability_revision: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionDeletionImpact {
    pub connection_id: Uuid,
    pub source_count: i64,
    pub workspace_attachment_count: i64,
    pub task_count: i64,
    pub active_task_count: i64,
    pub authentication_session_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginConnectionDeletion {
    pub connection_id: Uuid,
    pub disabled_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinishConnectionDeletion {
    pub connection_id: Uuid,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishConnectionSnapshot {
    pub connection_id: Uuid,
    pub opened: OpenedAdapterConnection,
    pub discovered: DiscoveredAdapterSources,
    pub published_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkConnectionUnavailable {
    pub connection_id: Uuid,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SaveAuthenticationStep {
    pub authentication_id: Uuid,
    pub connection_id: Uuid,
    pub method: String,
    pub step: AdapterAuthenticationStep,
    pub expires_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredAuthenticationSession {
    pub authentication_id: Uuid,
    pub connection_id: Uuid,
    pub provider_authentication_id: String,
    pub method: String,
    pub status: i32,
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkAuthenticationCancelled {
    pub authentication_id: Uuid,
    pub cancelled_at: OffsetDateTime,
}

pub trait ConnectionRepository: Send + Sync {
    type Error;

    fn store_connection(&self, command: CreateConnection) -> ConnectionFuture<'_, (), Self::Error>;

    fn load_connection(
        &self,
        connection_id: Uuid,
    ) -> ConnectionFuture<'_, Option<StoredConnection>, Self::Error>;

    fn publish_connection_snapshot(
        &self,
        command: PublishConnectionSnapshot,
    ) -> ConnectionFuture<'_, (), Self::Error>;

    fn mark_connection_unavailable(
        &self,
        command: MarkConnectionUnavailable,
    ) -> ConnectionFuture<'_, (), Self::Error>;

    fn save_authentication_step(
        &self,
        command: SaveAuthenticationStep,
    ) -> ConnectionFuture<'_, (), Self::Error>;

    fn load_authentication_session(
        &self,
        authentication_id: Uuid,
    ) -> ConnectionFuture<'_, Option<StoredAuthenticationSession>, Self::Error>;

    fn mark_authentication_cancelled(
        &self,
        command: MarkAuthenticationCancelled,
    ) -> ConnectionFuture<'_, (), Self::Error>;

    fn list_connections(&self) -> ConnectionFuture<'_, Vec<ConnectionSummary>, Self::Error>;

    fn get_connection_summary(
        &self,
        connection_id: Uuid,
    ) -> ConnectionFuture<'_, Option<ConnectionSummary>, Self::Error>;

    fn rename_connection(
        &self,
        command: RenameConnection,
    ) -> ConnectionFuture<'_, ConnectionSummary, Self::Error>;

    fn reconfigure_connection(
        &self,
        command: ReconfigureConnection,
    ) -> ConnectionFuture<'_, ConnectionSummary, Self::Error>;

    fn list_connection_sources(
        &self,
        connection_id: Uuid,
    ) -> ConnectionFuture<'_, Vec<ConnectionSourceSummary>, Self::Error>;

    fn connection_deletion_impact(
        &self,
        connection_id: Uuid,
    ) -> ConnectionFuture<'_, ConnectionDeletionImpact, Self::Error>;

    fn begin_connection_deletion(
        &self,
        command: BeginConnectionDeletion,
    ) -> ConnectionFuture<'_, ConnectionDeletionImpact, Self::Error>;

    fn finish_connection_deletion(
        &self,
        command: FinishConnectionDeletion,
    ) -> ConnectionFuture<'_, (), Self::Error>;
}

pub trait ConnectionRetirement: Send + Sync {
    type Error;

    fn retire_connection(&self, connection_id: Uuid) -> ConnectionFuture<'_, (), Self::Error>;
}

pub trait ConnectionSessionRegistry<S>: Send + Sync {
    fn replace(&self, connection_id: Uuid, session: S) -> Option<S>;
    fn remove(&self, connection_id: Uuid) -> Option<S>;
    fn identity(&self, connection_id: Uuid) -> Option<AdapterIdentity>;
}

pub trait WaitForConnectionSession<S>: ConnectionSessionRegistry<S> {
    fn wait_for_session(&self, connection_id: Uuid) -> ConnectionSessionFuture<'_, S>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenConnection {
    pub connection_id: Uuid,
    pub process_instance_id: String,
    pub opened_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectConnection {
    pub connection_id: Uuid,
    pub process_instance_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionInspection {
    pub description: AdapterConnectionDescription,
    pub validation: AdapterConnectionValidation,
    pub authentication_methods: Vec<AdapterAuthenticationMethod>,
    pub settings: Vec<AdapterSetting>,
    pub configured_secret_keys: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartConnectionAuthentication {
    pub authentication_id: Uuid,
    pub connection_id: Uuid,
    pub process_instance_id: String,
    pub method: String,
    pub values: Vec<AdapterSetting>,
    pub expires_at: OffsetDateTime,
    pub started_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartedConnectionAuthentication {
    pub authentication_id: Uuid,
    pub step: AdapterAuthenticationStep,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinueConnectionAuthentication {
    pub authentication_id: Uuid,
    pub connection_id: Uuid,
    pub values: Vec<AdapterSetting>,
    pub expires_at: OffsetDateTime,
    pub continued_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelConnectionAuthentication {
    pub authentication_id: Uuid,
    pub connection_id: Uuid,
    pub cancelled_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenedConnection {
    pub connection_id: Uuid,
    pub account_count: usize,
    pub source_count: usize,
    pub replaced_session: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopConnection {
    pub connection_id: Uuid,
    pub stopped_at: OffsetDateTime,
}

pub trait ConnectionInteraction: Send + Sync {
    fn inspect_connection(
        &self,
        command: InspectConnection,
    ) -> ConnectionFuture<'_, ConnectionInspection, ApplicationError>;

    fn start_connection_authentication(
        &self,
        command: StartConnectionAuthentication,
    ) -> ConnectionFuture<'_, StartedConnectionAuthentication, ApplicationError>;

    fn continue_connection_authentication(
        &self,
        command: ContinueConnectionAuthentication,
    ) -> ConnectionFuture<'_, AdapterAuthenticationStep, ApplicationError>;

    fn cancel_connection_authentication(
        &self,
        command: CancelConnectionAuthentication,
    ) -> ConnectionFuture<'_, (), ApplicationError>;
}

pub struct ConnectionCoordinator<R, L, G> {
    repository: R,
    launcher: L,
    sessions: G,
}

pub struct SourceDiscoveryTaskHandler<R, L, G> {
    coordinator: ConnectionCoordinator<R, L, G>,
}

impl<R, L, G> SourceDiscoveryTaskHandler<R, L, G> {
    pub fn new(coordinator: ConnectionCoordinator<R, L, G>) -> Self {
        Self { coordinator }
    }
}

impl<R, L, G> TaskHandler for SourceDiscoveryTaskHandler<R, L, G>
where
    R: ConnectionRepository,
    R::Error: Send,
    L: AdapterLauncher,
    L::Error: Send,
    G: ConnectionSessionRegistry<L::Session>,
{
    type Error = Infallible;

    fn accepted_kinds(&self) -> &'static [TaskKind] {
        &[TaskKind::SourceDiscovery]
    }

    fn execute<'a>(&'a self, claimed: &'a ClaimedTask) -> TaskHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            let TaskDefinition::SourceDiscovery { connection_id } = claimed.task.definition else {
                return Ok(TaskHandlerOutcome::Complete(TaskCompletion::Failed(
                    TaskFailure::coded("invalid_discovery_task", false),
                )));
            };
            let opened_at = claimed.task.started_at.unwrap_or(claimed.task.updated_at);
            let completion = match self
                .coordinator
                .open(OpenConnection {
                    connection_id,
                    process_instance_id: format!("discovery-task-{}", claimed.task.id),
                    opened_at,
                })
                .await
            {
                Ok(_) => TaskCompletion::Succeeded,
                Err(error) => TaskCompletion::Failed(source_discovery_failure(error)),
            };
            Ok(TaskHandlerOutcome::Complete(completion))
        })
    }
}

fn source_discovery_failure<RepositoryError, AdapterError>(
    error: ConnectionCoordinatorError<RepositoryError, AdapterError>,
) -> TaskFailure {
    match error {
        ConnectionCoordinatorError::Rejected(error)
        | ConnectionCoordinatorError::AdapterNotReady(Some(error)) => {
            TaskFailure::from_adapter(&error)
        }
        _ => TaskFailure::coded("connection_discovery_failed", false),
    }
}

impl<R, L, G> ConnectionCoordinator<R, L, G>
where
    R: ConnectionRepository,
    L: AdapterLauncher,
    G: ConnectionSessionRegistry<L::Session>,
{
    pub fn new(repository: R, launcher: L, sessions: G) -> Self {
        Self {
            repository,
            launcher,
            sessions,
        }
    }

    pub async fn inspect(
        &self,
        command: InspectConnection,
    ) -> Result<ConnectionInspection, ConnectionCoordinatorError<R::Error, L::Error>> {
        let (stored, mut session) = self
            .launch_stored(command.connection_id, command.process_instance_id)
            .await?;
        let settings = stored.settings.clone();
        let configured_secret_keys = stored
            .secrets
            .iter()
            .map(|secret| secret.key.clone())
            .collect();
        let inspection = async {
            let description = session
                .describe_connection()
                .await
                .map_err(ConnectionCoordinatorError::Adapter)?
                .map_err(ConnectionCoordinatorError::Rejected)?;
            let validation = session
                .validate_connection(ValidateAdapterConnection {
                    settings: stored.settings,
                    secrets: stored.secrets,
                })
                .await
                .map_err(ConnectionCoordinatorError::Adapter)?
                .map_err(ConnectionCoordinatorError::Rejected)?;
            let authentication_methods = session
                .list_authentication_methods()
                .await
                .map_err(ConnectionCoordinatorError::Adapter)?
                .map_err(ConnectionCoordinatorError::Rejected)?;
            Ok(ConnectionInspection {
                description,
                validation,
                authentication_methods,
                settings,
                configured_secret_keys,
            })
        }
        .await;
        let retirement = session.retire().await;
        match (inspection, retirement) {
            (Ok(inspection), Ok(_)) => Ok(inspection),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(ConnectionCoordinatorError::Adapter(error)),
        }
    }

    pub async fn start_authentication(
        &self,
        command: StartConnectionAuthentication,
    ) -> Result<StartedConnectionAuthentication, ConnectionCoordinatorError<R::Error, L::Error>>
    {
        let (_, mut session) = self
            .launch_stored(command.connection_id, command.process_instance_id)
            .await?;
        let step = match session
            .start_authentication(StartAdapterAuthentication {
                method: command.method.clone(),
                values: command.values,
            })
            .await
        {
            Ok(Ok(step)) => step,
            Ok(Err(error)) => {
                let _ = session.retire().await;
                return Err(ConnectionCoordinatorError::Rejected(error));
            }
            Err(error) => {
                let _ = session.retire().await;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
        };
        if let Err(error) = self
            .repository
            .save_authentication_step(SaveAuthenticationStep {
                authentication_id: command.authentication_id,
                connection_id: command.connection_id,
                method: command.method,
                step: step.clone(),
                expires_at: command.expires_at,
                updated_at: command.started_at,
            })
            .await
        {
            let _ = session.retire().await;
            return Err(ConnectionCoordinatorError::Repository(error));
        }
        let active = authentication_status_active(step.status);
        if active {
            self.install_authentication_session(command.connection_id, session)
                .await?;
        } else {
            session
                .retire()
                .await
                .map_err(ConnectionCoordinatorError::Adapter)?;
        }
        if active {
            tracing::info!(
                connection.id = %command.connection_id,
                authentication.id = %command.authentication_id,
                authentication.status = %authentication_status_name(step.status),
                "connection authentication started"
            );
        } else {
            tracing::info!(
                connection.id = %command.connection_id,
                authentication.id = %command.authentication_id,
                authentication.status = %authentication_status_name(step.status),
                "connection authentication completed"
            );
        }
        Ok(StartedConnectionAuthentication {
            authentication_id: command.authentication_id,
            step,
        })
    }

    pub async fn continue_authentication(
        &self,
        command: ContinueConnectionAuthentication,
    ) -> Result<AdapterAuthenticationStep, ConnectionCoordinatorError<R::Error, L::Error>> {
        let stored = self
            .repository
            .load_authentication_session(command.authentication_id)
            .await
            .map_err(ConnectionCoordinatorError::Repository)?
            .ok_or(ConnectionCoordinatorError::AuthenticationNotFound)?;
        if stored.connection_id != command.connection_id {
            return Err(ConnectionCoordinatorError::AuthenticationNotFound);
        }
        if !authentication_status_active(stored.status) || stored.expires_at <= command.continued_at
        {
            return Err(ConnectionCoordinatorError::AuthenticationNotActive);
        }
        let mut session = self
            .sessions
            .remove(stored.connection_id)
            .ok_or(ConnectionCoordinatorError::AuthenticationSessionUnavailable)?;
        let step = match session
            .continue_authentication(ContinueAdapterAuthentication {
                authentication_id: stored.provider_authentication_id,
                values: command.values,
            })
            .await
        {
            Ok(Ok(step)) => step,
            Ok(Err(error)) => {
                self.restore_session(stored.connection_id, session).await;
                return Err(ConnectionCoordinatorError::Rejected(error));
            }
            Err(error) => {
                self.restore_session(stored.connection_id, session).await;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
        };
        if let Err(error) = self
            .repository
            .save_authentication_step(SaveAuthenticationStep {
                authentication_id: stored.authentication_id,
                connection_id: stored.connection_id,
                method: stored.method,
                step: step.clone(),
                expires_at: command.expires_at,
                updated_at: command.continued_at,
            })
            .await
        {
            let _ = session.retire().await;
            return Err(ConnectionCoordinatorError::Repository(error));
        }
        if authentication_status_active(step.status) {
            self.install_authentication_session(stored.connection_id, session)
                .await?;
        } else {
            session
                .retire()
                .await
                .map_err(ConnectionCoordinatorError::Adapter)?;
        }
        if !authentication_status_active(step.status) {
            tracing::info!(
                connection.id = %stored.connection_id,
                authentication.id = %stored.authentication_id,
                authentication.status = %authentication_status_name(step.status),
                "connection authentication completed"
            );
        }
        Ok(step)
    }

    pub async fn cancel_authentication(
        &self,
        command: CancelConnectionAuthentication,
    ) -> Result<(), ConnectionCoordinatorError<R::Error, L::Error>> {
        let stored = self
            .repository
            .load_authentication_session(command.authentication_id)
            .await
            .map_err(ConnectionCoordinatorError::Repository)?
            .ok_or(ConnectionCoordinatorError::AuthenticationNotFound)?;
        if stored.connection_id != command.connection_id {
            return Err(ConnectionCoordinatorError::AuthenticationNotFound);
        }
        if !authentication_status_active(stored.status) {
            return Err(ConnectionCoordinatorError::AuthenticationNotActive);
        }
        let mut session = self
            .sessions
            .remove(stored.connection_id)
            .ok_or(ConnectionCoordinatorError::AuthenticationSessionUnavailable)?;
        let cancellation = session
            .cancel_authentication(stored.provider_authentication_id)
            .await;
        match cancellation {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.restore_session(stored.connection_id, session).await;
                return Err(ConnectionCoordinatorError::Rejected(error));
            }
            Err(error) => {
                self.restore_session(stored.connection_id, session).await;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
        }
        if let Err(error) = self
            .repository
            .mark_authentication_cancelled(MarkAuthenticationCancelled {
                authentication_id: stored.authentication_id,
                cancelled_at: command.cancelled_at,
            })
            .await
        {
            let _ = session.retire().await;
            return Err(ConnectionCoordinatorError::Repository(error));
        }
        session
            .retire()
            .await
            .map_err(ConnectionCoordinatorError::Adapter)?;
        tracing::info!(
            connection.id = %stored.connection_id,
            authentication.id = %stored.authentication_id,
            "connection authentication cancelled"
        );
        Ok(())
    }

    pub async fn open(
        &self,
        command: OpenConnection,
    ) -> Result<OpenedConnection, ConnectionCoordinatorError<R::Error, L::Error>> {
        let stored = self
            .repository
            .load_connection(command.connection_id)
            .await
            .map_err(ConnectionCoordinatorError::Repository)?
            .ok_or(ConnectionCoordinatorError::ConnectionNotFound)?;
        let replaced_session = if let Some(previous) = self.sessions.remove(command.connection_id) {
            if let Err(error) = previous.retire().await {
                self.mark_unavailable(command.connection_id, command.opened_at)
                    .await?;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
            true
        } else {
            false
        };
        let mut session = match self
            .launcher
            .launch(LaunchAdapter {
                provider_id: stored.provider_id.clone(),
                process_instance_id: command.process_instance_id,
            })
            .await
        {
            Ok(session) => session,
            Err(error) => {
                self.mark_unavailable(command.connection_id, command.opened_at)
                    .await?;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
        };
        if session.identity().adapter_key != stored.provider_id {
            let _ = session.retire().await;
            self.mark_unavailable(command.connection_id, command.opened_at)
                .await?;
            return Err(ConnectionCoordinatorError::AdapterIdentityMismatch);
        }
        let health = match session.health().await {
            Ok(health) => health,
            Err(error) => {
                let _ = session.retire().await;
                self.mark_unavailable(command.connection_id, command.opened_at)
                    .await?;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
        };
        if HealthStatus::try_from(health.status) != Ok(HealthStatus::Ready) {
            let error = health.error.map(AdapterCallError::from);
            let _ = session.retire().await;
            self.mark_unavailable(command.connection_id, command.opened_at)
                .await?;
            return Err(ConnectionCoordinatorError::AdapterNotReady(error));
        }
        let opened = match session
            .open_connection(OpenAdapterConnection {
                settings: stored.settings,
                secrets: stored.secrets,
            })
            .await
        {
            Ok(Ok(opened)) => opened,
            Ok(Err(error)) => {
                let _ = session.retire().await;
                self.mark_unavailable(command.connection_id, command.opened_at)
                    .await?;
                return Err(ConnectionCoordinatorError::Rejected(error));
            }
            Err(error) => {
                let _ = session.retire().await;
                self.mark_unavailable(command.connection_id, command.opened_at)
                    .await?;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
        };
        let discovered = match session.discover_sources().await {
            Ok(Ok(discovered)) => discovered,
            Ok(Err(error)) => {
                let _ = session.retire().await;
                self.mark_unavailable(command.connection_id, command.opened_at)
                    .await?;
                return Err(ConnectionCoordinatorError::Rejected(error));
            }
            Err(error) => {
                let _ = session.retire().await;
                self.mark_unavailable(command.connection_id, command.opened_at)
                    .await?;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
        };
        let result = OpenedConnection {
            connection_id: command.connection_id,
            account_count: opened.accounts.len(),
            source_count: discovered.sources.len(),
            replaced_session,
        };
        if let Err(error) = self
            .repository
            .publish_connection_snapshot(PublishConnectionSnapshot {
                connection_id: command.connection_id,
                opened,
                discovered,
                published_at: command.opened_at,
            })
            .await
        {
            let _ = session.retire().await;
            let _ = self
                .mark_unavailable(command.connection_id, command.opened_at)
                .await;
            return Err(ConnectionCoordinatorError::Repository(error));
        }
        if let Some(unexpected) = self.sessions.replace(command.connection_id, session) {
            if let Some(installed) = self.sessions.remove(command.connection_id) {
                let _ = installed.retire().await;
            }
            let _ = unexpected.retire().await;
            self.mark_unavailable(command.connection_id, command.opened_at)
                .await?;
            return Err(ConnectionCoordinatorError::ConcurrentSession);
        }
        tracing::info!(
            connection.id = %result.connection_id,
            account_count = result.account_count,
            source_count = result.source_count,
            replaced_session = result.replaced_session,
            "connection opened"
        );
        Ok(result)
    }

    pub async fn stop(
        &self,
        command: StopConnection,
    ) -> Result<Option<AdapterRetirement>, ConnectionCoordinatorError<R::Error, L::Error>> {
        let retirement = if let Some(session) = self.sessions.remove(command.connection_id) {
            match session.retire().await {
                Ok(retirement) => Some(retirement),
                Err(error) => {
                    self.mark_unavailable(command.connection_id, command.stopped_at)
                        .await?;
                    return Err(ConnectionCoordinatorError::Adapter(error));
                }
            }
        } else {
            None
        };
        self.mark_unavailable(command.connection_id, command.stopped_at)
            .await?;
        tracing::info!(
            connection.id = %command.connection_id,
            retired_session = retirement.is_some(),
            retirement = ?retirement,
            "connection stopped"
        );
        Ok(retirement)
    }

    async fn mark_unavailable(
        &self,
        connection_id: Uuid,
        updated_at: OffsetDateTime,
    ) -> Result<(), ConnectionCoordinatorError<R::Error, L::Error>> {
        self.repository
            .mark_connection_unavailable(MarkConnectionUnavailable {
                connection_id,
                updated_at,
            })
            .await
            .map_err(ConnectionCoordinatorError::Repository)
    }

    async fn launch_stored(
        &self,
        connection_id: Uuid,
        process_instance_id: String,
    ) -> Result<(StoredConnection, L::Session), ConnectionCoordinatorError<R::Error, L::Error>>
    {
        let stored = self
            .repository
            .load_connection(connection_id)
            .await
            .map_err(ConnectionCoordinatorError::Repository)?
            .ok_or(ConnectionCoordinatorError::ConnectionNotFound)?;
        let mut session = self
            .launcher
            .launch(LaunchAdapter {
                provider_id: stored.provider_id.clone(),
                process_instance_id,
            })
            .await
            .map_err(ConnectionCoordinatorError::Adapter)?;
        if session.identity().adapter_key != stored.provider_id {
            let _ = session.retire().await;
            return Err(ConnectionCoordinatorError::AdapterIdentityMismatch);
        }
        let health = match session.health().await {
            Ok(health) => health,
            Err(error) => {
                let _ = session.retire().await;
                return Err(ConnectionCoordinatorError::Adapter(error));
            }
        };
        if HealthStatus::try_from(health.status) != Ok(HealthStatus::Ready) {
            let error = health.error.map(AdapterCallError::from);
            let _ = session.retire().await;
            return Err(ConnectionCoordinatorError::AdapterNotReady(error));
        }
        Ok((stored, session))
    }

    async fn install_authentication_session(
        &self,
        connection_id: Uuid,
        session: L::Session,
    ) -> Result<(), ConnectionCoordinatorError<R::Error, L::Error>> {
        if let Some(unexpected) = self.sessions.replace(connection_id, session) {
            if let Some(installed) = self.sessions.remove(connection_id) {
                let _ = installed.retire().await;
            }
            let _ = unexpected.retire().await;
            return Err(ConnectionCoordinatorError::ConcurrentSession);
        }
        Ok(())
    }

    async fn restore_session(&self, connection_id: Uuid, session: L::Session) {
        if let Some(unexpected) = self.sessions.replace(connection_id, session) {
            if let Some(installed) = self.sessions.remove(connection_id) {
                let _ = installed.retire().await;
            }
            let _ = unexpected.retire().await;
        }
    }
}

fn authentication_status_active(status: i32) -> bool {
    matches!(
        AuthenticationStatus::try_from(status),
        Ok(AuthenticationStatus::Waiting | AuthenticationStatus::InputRequired)
    )
}

fn authentication_status_name(status: i32) -> &'static str {
    AuthenticationStatus::try_from(status).map_or("UNKNOWN", |status| status.as_str_name())
}

#[derive(Debug)]
pub enum ConnectionCoordinatorError<RepositoryError, AdapterError> {
    ConnectionNotFound,
    AdapterIdentityMismatch,
    AdapterNotReady(Option<AdapterCallError>),
    Rejected(AdapterCallError),
    ConcurrentSession,
    AuthenticationNotFound,
    AuthenticationNotActive,
    AuthenticationSessionUnavailable,
    Repository(RepositoryError),
    Adapter(AdapterError),
}

impl<RepositoryError, AdapterError> IntoApplicationError
    for ConnectionCoordinatorError<RepositoryError, AdapterError>
where
    RepositoryError: IntoApplicationError,
    AdapterError: std::fmt::Debug,
{
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::ConnectionNotFound => ApplicationError::new(
                ApplicationErrorCode::ConnectionNotFound,
                "connection was not found",
            ),
            Self::AdapterNotReady(Some(error)) | Self::Rejected(error) => {
                error.into_application_error()
            }
            Self::AdapterNotReady(None) => ApplicationError::new(
                ApplicationErrorCode::ConnectionUnavailable,
                "connection adapter is unavailable",
            ),
            Self::ConcurrentSession => ApplicationError::new(
                ApplicationErrorCode::ConnectionBusy,
                "connection already has an active operation",
            ),
            Self::AuthenticationNotFound => ApplicationError::new(
                ApplicationErrorCode::AuthenticationNotFound,
                "authentication was not found",
            ),
            Self::AuthenticationNotActive => ApplicationError::new(
                ApplicationErrorCode::AuthenticationNotActive,
                "authentication is not active",
            ),
            Self::AuthenticationSessionUnavailable => ApplicationError::new(
                ApplicationErrorCode::AuthenticationSessionUnavailable,
                "authentication process is unavailable",
            ),
            Self::Repository(error) => error.into_application_error(),
            Self::AdapterIdentityMismatch | Self::Adapter(_) => {
                ApplicationError::internal("connection adapter operation failed")
            }
        }
    }
}

impl<R, L, G> ConnectionInteraction for ConnectionCoordinator<R, L, G>
where
    R: ConnectionRepository,
    R::Error: IntoApplicationError + Send,
    L: AdapterLauncher,
    L::Error: Send + std::fmt::Debug,
    G: ConnectionSessionRegistry<L::Session>,
{
    fn inspect_connection(
        &self,
        command: InspectConnection,
    ) -> ConnectionFuture<'_, ConnectionInspection, ApplicationError> {
        Box::pin(async move {
            let connection_id = command.connection_id;
            let result = self.inspect(command).await;
            if let Err(ConnectionCoordinatorError::Rejected(error)) = &result {
                tracing::warn!(
                    connection.id = %connection_id,
                    provider.operation = "InspectConnection",
                    failure.category = ?error.category,
                    failure.retryable = error.retryable,
                    "provider rejected connection operation"
                );
            }
            result.map_err(IntoApplicationError::into_application_error)
        })
    }

    fn start_connection_authentication(
        &self,
        command: StartConnectionAuthentication,
    ) -> ConnectionFuture<'_, StartedConnectionAuthentication, ApplicationError> {
        Box::pin(async move {
            let connection_id = command.connection_id;
            let authentication_id = command.authentication_id;
            let result = self.start_authentication(command).await;
            if let Err(ConnectionCoordinatorError::Rejected(error)) = &result {
                tracing::warn!(
                    connection.id = %connection_id,
                    authentication.id = %authentication_id,
                    provider.operation = "StartAuthentication",
                    failure.category = ?error.category,
                    failure.retryable = error.retryable,
                    "provider rejected connection operation"
                );
            }
            result.map_err(IntoApplicationError::into_application_error)
        })
    }

    fn continue_connection_authentication(
        &self,
        command: ContinueConnectionAuthentication,
    ) -> ConnectionFuture<'_, AdapterAuthenticationStep, ApplicationError> {
        Box::pin(async move {
            let connection_id = command.connection_id;
            let authentication_id = command.authentication_id;
            let result = self.continue_authentication(command).await;
            if let Err(ConnectionCoordinatorError::Rejected(error)) = &result {
                tracing::warn!(
                    connection.id = %connection_id,
                    authentication.id = %authentication_id,
                    provider.operation = "ContinueAuthentication",
                    failure.category = ?error.category,
                    failure.retryable = error.retryable,
                    "provider rejected connection operation"
                );
            }
            result.map_err(IntoApplicationError::into_application_error)
        })
    }

    fn cancel_connection_authentication(
        &self,
        command: CancelConnectionAuthentication,
    ) -> ConnectionFuture<'_, (), ApplicationError> {
        Box::pin(async move {
            let connection_id = command.connection_id;
            let authentication_id = command.authentication_id;
            let result = self.cancel_authentication(command).await;
            if let Err(ConnectionCoordinatorError::Rejected(error)) = &result {
                tracing::warn!(
                    connection.id = %connection_id,
                    authentication.id = %authentication_id,
                    provider.operation = "CancelAuthentication",
                    failure.category = ?error.category,
                    failure.retryable = error.retryable,
                    "provider rejected connection operation"
                );
            }
            result.map_err(IntoApplicationError::into_application_error)
        })
    }
}

pub trait ConnectionApi: Send + Sync {
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
    fn connection_deletion_impact(
        &self,
        connection_id: Uuid,
    ) -> ApplicationFuture<'_, ConnectionDeletionImpact>;
    fn delete_connection(
        &self,
        command: BeginConnectionDeletion,
    ) -> ApplicationFuture<'_, ConnectionDeletionImpact>;
}

pub struct ConnectionService<R, D> {
    connections: R,
    connection_retirement: D,
    providers: SharedProviderRegistry,
}

impl<R, D> ConnectionService<R, D> {
    pub fn new(
        connections: R,
        connection_retirement: D,
        providers: SharedProviderRegistry,
    ) -> Self {
        Self {
            connections,
            connection_retirement,
            providers,
        }
    }
}

impl<R: ConnectionRepository, D: ConnectionRetirement> ConnectionApi for ConnectionService<R, D>
where
    R::Error: IntoApplicationError,
    D::Error: IntoApplicationError,
{
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
            command
                .validate()
                .map_err(IntoApplicationError::into_application_error)?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::adapter_runtime::{
        AdapterFailureCategory, AdapterFieldProblem, AdapterRetryAfter,
    };

    #[test]
    fn provider_rejection_preserves_public_details() {
        let rejection = AdapterCallError {
            category: AdapterFailureCategory::RateLimited,
            code: "remote_rate_limit".to_owned(),
            safe_message: "provider request limit reached".to_owned(),
            retryable: true,
            retry_after: Some(AdapterRetryAfter {
                seconds: 30,
                nanoseconds: 0,
            }),
            field_problems: vec![AdapterFieldProblem {
                path: "account".to_owned(),
                code: "temporarily_limited".to_owned(),
                message: "account is temporarily limited".to_owned(),
            }],
        };

        let error = ConnectionCoordinatorError::<ApplicationError, String>::Rejected(rejection)
            .into_application_error();

        assert_eq!(error.code, ApplicationErrorCode::ProviderRateLimited);
        assert_eq!(error.provider_code(), Some("remote_rate_limit"));
        assert_eq!(error.field_problems.len(), 1);
        assert!(error.retry().is_some_and(|retry| retry.retryable));
    }

    #[test]
    fn discovery_task_preserves_provider_rejection() {
        let rejection = AdapterCallError {
            category: AdapterFailureCategory::Authentication,
            code: "remote_authentication_failed".to_owned(),
            safe_message: "remote authentication failed".to_owned(),
            retryable: false,
            retry_after: None,
            field_problems: Vec::new(),
        };

        let failure =
            source_discovery_failure(ConnectionCoordinatorError::<(), ()>::Rejected(rejection));

        assert_eq!(failure.code, "remote_authentication_failed");
        assert_eq!(failure.message, "remote authentication failed");
        assert!(!failure.retryable);
    }

    #[test]
    fn transport_failure_keeps_cause_private() {
        let error = ConnectionCoordinatorError::<ApplicationError, String>::Adapter(
            "socket closed".to_owned(),
        )
        .into_application_error();

        assert_eq!(error.code, ApplicationErrorCode::Internal);
        assert_eq!(error.message, "connection adapter operation failed");
    }
}
