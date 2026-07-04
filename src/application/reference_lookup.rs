use std::{future::Future, marker::PhantomData, pin::Pin};

use uuid::Uuid;

use crate::domain::acquisition::PortableReference;

use super::{
    adapter_runtime::{
        AdapterCallError, AdapterPortableReferenceLookup, AdapterSession,
        LookupAdapterPortableReferences,
    },
    connection::ConnectionSessionRegistry,
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
};

pub type ReferenceLookupFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceLookupTarget {
    pub connection_id: Uuid,
    pub reference_namespaces: Vec<String>,
    pub maximum_batch_size: u32,
}

pub trait ReferenceLookupRepository: Send + Sync {
    type Error;

    fn resolve_reference_lookup(
        &self,
        workspace_id: Uuid,
        source_id: Uuid,
    ) -> ReferenceLookupFuture<'_, ReferenceLookupTarget, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LookupPortableReferences {
    pub operation_id: Vec<u8>,
    pub workspace_id: Uuid,
    pub source_id: Uuid,
    pub references: Vec<PortableReference>,
}

pub struct ReferenceLookupService<R, G> {
    repository: R,
    sessions: G,
}

pub trait ReferenceLookupInteraction: Send + Sync {
    fn lookup_portable_references(
        &self,
        command: LookupPortableReferences,
    ) -> ReferenceLookupFuture<'_, Vec<AdapterPortableReferenceLookup>, ApplicationError>;
}

pub struct ReferenceLookupInteractionService<R, G, S> {
    service: ReferenceLookupService<R, G>,
    session: PhantomData<fn() -> S>,
}

impl<R, G, S> ReferenceLookupInteractionService<R, G, S> {
    pub fn new(repository: R, sessions: G) -> Self {
        Self {
            service: ReferenceLookupService::new(repository, sessions),
            session: PhantomData,
        }
    }
}

impl<R, G, S> ReferenceLookupInteraction for ReferenceLookupInteractionService<R, G, S>
where
    R: ReferenceLookupRepository,
    R::Error: IntoApplicationError + Send,
    G: ConnectionSessionRegistry<S>,
    S: AdapterSession + 'static,
    S::Error: Send,
{
    fn lookup_portable_references(
        &self,
        command: LookupPortableReferences,
    ) -> ReferenceLookupFuture<'_, Vec<AdapterPortableReferenceLookup>, ApplicationError> {
        Box::pin(async move {
            self.service
                .lookup(command)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }
}

impl<R, G> ReferenceLookupService<R, G> {
    pub fn new(repository: R, sessions: G) -> Self {
        Self {
            repository,
            sessions,
        }
    }

    pub async fn lookup<S>(
        &self,
        command: LookupPortableReferences,
    ) -> Result<Vec<AdapterPortableReferenceLookup>, ReferenceLookupServiceError<R::Error, S::Error>>
    where
        R: ReferenceLookupRepository,
        G: ConnectionSessionRegistry<S>,
        S: AdapterSession,
    {
        if command.operation_id.is_empty() || command.references.is_empty() {
            return Err(ReferenceLookupServiceError::InvalidRequest);
        }
        let target = self
            .repository
            .resolve_reference_lookup(command.workspace_id, command.source_id)
            .await
            .map_err(ReferenceLookupServiceError::Repository)?;
        if target.maximum_batch_size == 0
            || command.references.len() > target.maximum_batch_size as usize
        {
            return Err(ReferenceLookupServiceError::BatchTooLarge);
        }
        if command.references.iter().any(|reference| {
            !target
                .reference_namespaces
                .iter()
                .any(|namespace| namespace == reference.namespace())
        }) {
            return Err(ReferenceLookupServiceError::NamespaceUnsupported);
        }
        let mut session = self
            .sessions
            .remove(target.connection_id)
            .ok_or(ReferenceLookupServiceError::SessionUnavailable)?;
        let result = session
            .lookup_portable_references(LookupAdapterPortableReferences {
                operation_id: command.operation_id,
                references: command.references,
            })
            .await;
        match result {
            Ok(result) => {
                restore_session(&self.sessions, target.connection_id, session).await?;
                result.map_err(ReferenceLookupServiceError::Rejected)
            }
            Err(error) => {
                let _ = session.retire().await;
                Err(ReferenceLookupServiceError::Adapter(error))
            }
        }
    }
}

async fn restore_session<G, S, RepositoryError>(
    sessions: &G,
    connection_id: Uuid,
    session: S,
) -> Result<(), ReferenceLookupServiceError<RepositoryError, S::Error>>
where
    G: ConnectionSessionRegistry<S>,
    S: AdapterSession,
{
    if let Some(unexpected) = sessions.replace(connection_id, session) {
        if let Some(installed) = sessions.remove(connection_id) {
            let _ = installed.retire().await;
        }
        let _ = unexpected.retire().await;
        return Err(ReferenceLookupServiceError::ConcurrentSession);
    }
    Ok(())
}

#[derive(Debug)]
pub enum ReferenceLookupServiceError<RepositoryError, AdapterError> {
    InvalidRequest,
    BatchTooLarge,
    NamespaceUnsupported,
    SessionUnavailable,
    ConcurrentSession,
    Repository(RepositoryError),
    Rejected(AdapterCallError),
    Adapter(AdapterError),
}

impl<RepositoryError, AdapterError> IntoApplicationError
    for ReferenceLookupServiceError<RepositoryError, AdapterError>
where
    RepositoryError: IntoApplicationError,
{
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::InvalidRequest => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "reference lookup request is invalid",
                "references",
                "required",
                "at least one portable reference is required",
            ),
            Self::BatchTooLarge => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "reference lookup request is invalid",
                "references",
                "batch_too_large",
                "portable reference batch exceeds the provider limit",
            ),
            Self::NamespaceUnsupported => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "reference lookup request is invalid",
                "references",
                "namespace_unsupported",
                "portable reference namespace is not supported",
            ),
            Self::SessionUnavailable => ApplicationError::new(
                ApplicationErrorCode::ConnectionUnavailable,
                "connection process is unavailable",
            ),
            Self::ConcurrentSession => ApplicationError::new(
                ApplicationErrorCode::ConnectionBusy,
                "connection process changed during reference lookup",
            ),
            Self::Repository(error) => error.into_application_error(),
            Self::Rejected(error) => error.into_application_error(),
            Self::Adapter(_) => ApplicationError::new(
                ApplicationErrorCode::Internal,
                "reference lookup adapter operation failed",
            ),
        }
    }
}
