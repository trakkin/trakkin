use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};

use tokio::{sync::Mutex as AsyncMutex, time::timeout};
use uuid::Uuid;

use crate::domain::acquisition::{BinaryAssetKey, ProviderItemKey};

use super::{
    adapter_runtime::{
        AdapterBinaryAsset, AdapterCallError, AdapterKey, AdapterSession, ReadAdapterAsset,
    },
    connection::{ConnectionSessionRegistry, WaitForConnectionSession},
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
};

pub type AssetFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

const SESSION_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveBinaryAsset {
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub publication_id: Uuid,
    pub provider_item_key: ProviderItemKey,
    pub asset_key: BinaryAssetKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryAssetTarget {
    pub connection_id: Uuid,
    pub source_key: AdapterKey,
    pub provider_item_key: AdapterKey,
    pub asset_key: AdapterKey,
    pub maximum_bytes: u64,
    pub content_types: Vec<String>,
}

pub trait BinaryAssetRepository: Send + Sync {
    type Error;

    fn resolve_binary_asset(
        &self,
        command: ResolveBinaryAsset,
    ) -> AssetFuture<'_, BinaryAssetTarget, Self::Error>;

    fn confirm_binary_asset_publication(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
        publication_id: Uuid,
    ) -> AssetFuture<'_, (), Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadPublishedBinaryAsset {
    pub operation_id: Vec<u8>,
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub publication_id: Uuid,
    pub provider_item_key: ProviderItemKey,
    pub asset_key: BinaryAssetKey,
}

pub struct BinaryAssetService<R, G> {
    repository: R,
    sessions: G,
    host_maximum_bytes: u64,
    connection_locks: Mutex<HashMap<Uuid, Weak<AsyncMutex<()>>>>,
}

pub trait BinaryAssetInteraction: Send + Sync {
    fn read_published_binary_asset(
        &self,
        command: ReadPublishedBinaryAsset,
    ) -> AssetFuture<'_, AdapterBinaryAsset, ApplicationError>;
}

pub struct BinaryAssetInteractionService<R, G, S> {
    service: BinaryAssetService<R, G>,
    session: PhantomData<fn() -> S>,
}

impl<R, G, S> BinaryAssetInteractionService<R, G, S> {
    pub fn new(repository: R, sessions: G, host_maximum_bytes: u64) -> Self {
        Self {
            service: BinaryAssetService::new(repository, sessions, host_maximum_bytes),
            session: PhantomData,
        }
    }
}

impl<R, G, S> BinaryAssetInteraction for BinaryAssetInteractionService<R, G, S>
where
    R: BinaryAssetRepository,
    R::Error: IntoApplicationError + Send,
    G: WaitForConnectionSession<S>,
    S: AdapterSession + 'static,
    S::Error: Send + std::fmt::Debug,
{
    fn read_published_binary_asset(
        &self,
        command: ReadPublishedBinaryAsset,
    ) -> AssetFuture<'_, AdapterBinaryAsset, ApplicationError> {
        Box::pin(async move {
            self.service
                .read(command)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }
}

impl<R, G> BinaryAssetService<R, G> {
    pub fn new(repository: R, sessions: G, host_maximum_bytes: u64) -> Self {
        Self {
            repository,
            sessions,
            host_maximum_bytes,
            connection_locks: Mutex::new(HashMap::new()),
        }
    }

    pub async fn read<S>(
        &self,
        command: ReadPublishedBinaryAsset,
    ) -> Result<AdapterBinaryAsset, BinaryAssetServiceError<R::Error, S::Error>>
    where
        R: BinaryAssetRepository,
        G: WaitForConnectionSession<S>,
        S: AdapterSession,
    {
        if command.operation_id.is_empty() || self.host_maximum_bytes == 0 {
            return Err(BinaryAssetServiceError::InvalidRequest);
        }
        let workspace_id = command.workspace_id;
        let source_id = command.source_id;
        let publication_id = command.publication_id;
        let target = self
            .repository
            .resolve_binary_asset(ResolveBinaryAsset {
                workspace_id: command.workspace_id,
                source_id: command.source_id,
                publication_id: command.publication_id,
                provider_item_key: command.provider_item_key,
                asset_key: command.asset_key,
            })
            .await
            .map_err(BinaryAssetServiceError::Repository)?;
        let maximum_bytes = self.host_maximum_bytes.min(target.maximum_bytes);
        if maximum_bytes == 0 || target.content_types.is_empty() {
            return Err(BinaryAssetServiceError::InvalidTarget);
        }
        let connection_lock = self.connection_lock(target.connection_id);
        let _connection_guard = connection_lock.lock().await;
        let mut session = timeout(
            SESSION_WAIT_TIMEOUT,
            self.sessions.wait_for_session(target.connection_id),
        )
        .await
        .map_err(|_| BinaryAssetServiceError::SessionUnavailable)?;
        let result = session
            .read_asset(ReadAdapterAsset {
                operation_id: command.operation_id,
                source_key: target.source_key,
                provider_item_key: target.provider_item_key,
                asset_key: target.asset_key,
                maximum_bytes,
                allowed_content_types: target.content_types,
            })
            .await;
        match result {
            Ok(result) => {
                restore_session(&self.sessions, target.connection_id, session).await?;
                let asset = result.map_err(BinaryAssetServiceError::Rejected)?;
                self.repository
                    .confirm_binary_asset_publication(workspace_id, source_id, publication_id)
                    .await
                    .map_err(BinaryAssetServiceError::Repository)?;
                Ok(asset)
            }
            Err(error) => {
                let _ = session.retire().await;
                Err(BinaryAssetServiceError::Adapter(error))
            }
        }
    }

    fn connection_lock(&self, connection_id: Uuid) -> Arc<AsyncMutex<()>> {
        let mut connection_locks = self
            .connection_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection_locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = connection_locks.get(&connection_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        connection_locks.insert(connection_id, Arc::downgrade(&lock));
        lock
    }
}

async fn restore_session<G, S, RepositoryError>(
    sessions: &G,
    connection_id: Uuid,
    session: S,
) -> Result<(), BinaryAssetServiceError<RepositoryError, S::Error>>
where
    G: ConnectionSessionRegistry<S>,
    S: AdapterSession,
{
    if let Some(unexpected) = sessions.replace(connection_id, session) {
        if let Some(installed) = sessions.remove(connection_id) {
            let _ = installed.retire().await;
        }
        let _ = unexpected.retire().await;
        return Err(BinaryAssetServiceError::ConcurrentSession);
    }
    Ok(())
}

#[derive(Debug)]
pub enum BinaryAssetServiceError<RepositoryError, AdapterError> {
    InvalidRequest,
    InvalidTarget,
    SessionUnavailable,
    ConcurrentSession,
    Repository(RepositoryError),
    Rejected(AdapterCallError),
    Adapter(AdapterError),
}

impl<RepositoryError, AdapterError> IntoApplicationError
    for BinaryAssetServiceError<RepositoryError, AdapterError>
where
    RepositoryError: IntoApplicationError,
    AdapterError: std::fmt::Debug,
{
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::InvalidRequest => ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "binary asset request is invalid",
            ),
            Self::InvalidTarget => ApplicationError::new(
                ApplicationErrorCode::InvalidProviderData,
                "binary asset capability is invalid",
            ),
            Self::SessionUnavailable => ApplicationError::new(
                ApplicationErrorCode::ConnectionUnavailable,
                "connection process is unavailable",
            ),
            Self::ConcurrentSession => ApplicationError::new(
                ApplicationErrorCode::ConnectionBusy,
                "connection process changed during asset read",
            ),
            Self::Repository(error) => error.into_application_error(),
            Self::Rejected(error) => error.into_application_error(),
            Self::Adapter(error) => ApplicationError::internal(
                "binary asset adapter operation failed",
                format_args!("{error:?}"),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::BinaryAssetService;

    #[tokio::test]
    async fn binary_asset_reads_are_serialized_per_connection() {
        let service = BinaryAssetService::new((), (), 1);
        let connection_id = Uuid::new_v4();
        let first_lock = service.connection_lock(connection_id);
        let first_guard = first_lock.lock().await;
        let second_lock = service.connection_lock(connection_id);

        assert!(second_lock.try_lock().is_err());
        assert!(service.connection_lock(Uuid::new_v4()).try_lock().is_ok());

        drop(first_guard);
        assert!(second_lock.try_lock().is_ok());
    }
}
