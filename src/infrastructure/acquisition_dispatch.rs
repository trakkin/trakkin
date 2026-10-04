use std::{future::pending, marker::PhantomData, time::Duration};

use time::OffsetDateTime;
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior, interval, timeout_at},
};
use uuid::Uuid;

use crate::application::{
    acquisition::{
        AbandonRead, BeginRead, PublicationRepository, PublishRead, ReadAbandonment, ReadFailure,
        ReadFence, ReadMode, ResetSourceStream, StageCatalogBatch, StageStateBatch,
    },
    adapter_runtime::{
        AdapterKey, AdapterReadEvent, AdapterReadStream, AdapterSession, ReadAdapterSource,
    },
    connection::ConnectionSessionRegistry,
    task_dispatcher::{TaskHandler, TaskHandlerFuture, TaskHandlerOutcome},
    task_manager::{
        ClaimedTask, SourceStream, TaskCompletion, TaskDefinition, TaskFailure, TaskKind,
        TaskRepository, TaskStatus,
    },
};
use crate::domain::acquisition::{CatalogBatch, StateBatch};
use crate::infrastructure::connection::ProcessSessionRegistry;

const READ_TASK_KINDS: [TaskKind; 4] = [
    TaskKind::CatalogRead,
    TaskKind::StateRead,
    TaskKind::CatalogReset,
    TaskKind::StateReset,
];

pub struct AcquisitionTaskHandler<P, T, S> {
    publication: P,
    tasks: T,
    sessions: ProcessSessionRegistry<S>,
    preferred_batch_size: u32,
    cancellation_poll_interval: Duration,
    cancellation_grace_period: Duration,
    shutdown: Option<watch::Receiver<bool>>,
    session: PhantomData<fn() -> S>,
}

impl<P, T, S> AcquisitionTaskHandler<P, T, S> {
    pub fn new(publication: P, tasks: T, sessions: ProcessSessionRegistry<S>) -> Self {
        Self {
            publication,
            tasks,
            sessions,
            preferred_batch_size: 100,
            cancellation_poll_interval: Duration::from_millis(50),
            cancellation_grace_period: Duration::from_secs(5),
            shutdown: None,
            session: PhantomData,
        }
    }

    pub fn with_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    pub fn with_cancellation_timing(
        mut self,
        poll_interval: Duration,
        grace_period: Duration,
    ) -> Self {
        self.cancellation_poll_interval = poll_interval;
        self.cancellation_grace_period = grace_period;
        self
    }
}

impl<P, T, S> TaskHandler for AcquisitionTaskHandler<P, T, S>
where
    P: PublicationRepository,
    P::Error: Send + std::fmt::Display,
    T: TaskRepository,
    T::Error: Send + std::fmt::Display,
    S: AdapterSession,
    S::Error: Send + std::fmt::Display,
{
    type Error = AcquisitionDispatchError<P::Error, T::Error, S::Error>;

    fn accepted_kinds(&self) -> &'static [TaskKind] {
        &READ_TASK_KINDS
    }

    fn execute<'a>(&'a self, claimed: &'a ClaimedTask) -> TaskHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            match &claimed.task.definition {
                TaskDefinition::Reset { .. } => {
                    let outcome = match self
                        .publication
                        .reset_source_stream(ResetSourceStream {
                            task_id: claimed.task.id,
                            task_attempt_id: claimed.attempt_id,
                            owner_id: claimed.owner_id,
                            completed_at: OffsetDateTime::now_utc(),
                        })
                        .await
                    {
                        Ok(()) => TaskHandlerOutcome::Terminalized,
                        Err(error) => {
                            failed_completion_with_error("source_reset_failed", error, false)
                        }
                    };
                    Ok(outcome)
                }
                TaskDefinition::Read { stream, .. } => {
                    let Some(mut session) = self.sessions.remove(claimed.task.connection_id) else {
                        return Ok(failed_completion("connection_session_unavailable", true));
                    };
                    let mut shutdown = self.shutdown.clone();
                    let attempt_id = match generate_uuid() {
                        Ok(attempt_id) => attempt_id,
                        Err(error) => {
                            let _ = self
                                .restore_session(claimed.task.connection_id, session)
                                .await;
                            return Ok(failed_completion_with_error(
                                "operation_id_generation_failed",
                                error,
                                false,
                            ));
                        }
                    };
                    let publication_id = match generate_uuid() {
                        Ok(publication_id) => publication_id,
                        Err(error) => {
                            let _ = self
                                .restore_session(claimed.task.connection_id, session)
                                .await;
                            return Ok(failed_completion_with_error(
                                "publication_id_generation_failed",
                                error,
                                false,
                            ));
                        }
                    };
                    let begin = BeginRead {
                        attempt_id,
                        task_id: claimed.task.id,
                        task_attempt_id: claimed.attempt_id,
                        owner_id: claimed.owner_id,
                        started_at: claimed.task.started_at.unwrap_or(claimed.task.updated_at),
                    };
                    let fence = match stream {
                        crate::application::task_manager::SourceStream::Catalog => {
                            self.publication.begin_catalog_read(begin).await
                        }
                        crate::application::task_manager::SourceStream::State => {
                            self.publication.begin_state_read(begin).await
                        }
                    };
                    let fence = match fence {
                        Ok(fence) => fence,
                        Err(error) => {
                            let restored = self
                                .restore_session(claimed.task.connection_id, session)
                                .await;
                            let code = if restored.is_ok() {
                                "source_read_not_started"
                            } else {
                                "connection_session_conflict"
                            };
                            return Ok(failed_completion_with_error(code, error, false));
                        }
                    };
                    let read_result = self
                        .run_read(
                            claimed,
                            *stream,
                            ReadAttempt {
                                publication_id,
                                attempt_id,
                            },
                            fence,
                            &mut session,
                            &mut shutdown,
                        )
                        .await;
                    let disposition = match read_result {
                        Ok(disposition) => disposition,
                        Err(error) => {
                            let _ = session.retire().await;
                            return Err(error);
                        }
                    };
                    match disposition {
                        ReadDisposition::Retire => {
                            session
                                .retire()
                                .await
                                .map_err(AcquisitionDispatchError::Adapter)?;
                        }
                        ReadDisposition::Restore => {
                            self.restore_session(claimed.task.connection_id, session)
                                .await?;
                        }
                        ReadDisposition::Shutdown => {
                            self.restore_session(claimed.task.connection_id, session)
                                .await?;
                            return Ok(TaskHandlerOutcome::Abandoned);
                        }
                    }
                    Ok(TaskHandlerOutcome::Terminalized)
                }
                TaskDefinition::SourceDiscovery { .. } => {
                    Ok(failed_completion("invalid_acquisition_task", false))
                }
            }
        })
    }
}

impl<P, T, S> AcquisitionTaskHandler<P, T, S>
where
    P: PublicationRepository,
    T: TaskRepository,
    S: AdapterSession,
    P::Error: std::fmt::Display,
    T::Error: std::fmt::Display,
    S::Error: std::fmt::Display,
{
    async fn run_read(
        &self,
        claimed: &ClaimedTask,
        source_stream: SourceStream,
        attempt: ReadAttempt,
        fence: ReadFence,
        session: &mut S,
        shutdown: &mut Option<watch::Receiver<bool>>,
    ) -> Result<ReadDisposition, AcquisitionDispatchError<P::Error, T::Error, S::Error>> {
        let ReadAttempt {
            publication_id,
            attempt_id,
        } = attempt;
        let operation_id = attempt_id.as_bytes().to_vec();
        let command = read_command(&fence, operation_id.clone(), self.preferred_batch_size);
        let started = match source_stream {
            SourceStream::Catalog => session.read_catalog(command).await.map(ReadStream::Catalog),
            SourceStream::State => session.read_state(command).await.map(ReadStream::State),
        };
        let codes = ReadFailureCodes::for_stream(source_stream);
        let mut stream: ReadStream<S> = match started {
            Ok(stream) => stream,
            Err(error) => {
                self.abandon_current(
                    claimed.task.id,
                    attempt_id,
                    ReadFailure::new(codes.start, error.to_string(), true),
                )
                .await?;
                return Ok(ReadDisposition::Retire);
            }
        };
        let mut cancellation = CancellationMonitor::new(self.cancellation_poll_interval);
        loop {
            let next = self
                .next_event(
                    claimed.task.id,
                    &operation_id,
                    session,
                    &mut stream,
                    &mut cancellation,
                    shutdown,
                )
                .await;
            let event = match next {
                Ok(Some(event)) => event,
                Ok(None) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        ReadFailure::coded(codes.ended, true),
                    )
                    .await?;
                    return Ok(ReadDisposition::Retire);
                }
                Err(NextEventError::CancellationTimeout) => {
                    self.abandon_cancelled(attempt_id).await?;
                    return Ok(ReadDisposition::Retire);
                }
                Err(NextEventError::Task(error)) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        ReadFailure::new("task_state_unavailable", error.to_string(), true),
                    )
                    .await?;
                    return Ok(ReadDisposition::Retire);
                }
                Err(NextEventError::TaskMissing) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        ReadFailure::coded("task_state_unavailable", true),
                    )
                    .await?;
                    return Ok(ReadDisposition::Retire);
                }
                Err(NextEventError::Adapter(error)) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        ReadFailure::new(codes.stream, error.to_string(), true),
                    )
                    .await?;
                    return Ok(ReadDisposition::Retire);
                }
                Err(NextEventError::Shutdown) => return Ok(ReadDisposition::Shutdown),
            };
            if cancellation.requested {
                match event {
                    AdapterReadEvent::Heartbeat { .. } | AdapterReadEvent::Batch(_) => continue,
                    AdapterReadEvent::Completed(_)
                    | AdapterReadEvent::Failed(_)
                    | AdapterReadEvent::Cancelled => {
                        self.abandon_cancelled(attempt_id).await?;
                        return Ok(ReadDisposition::Restore);
                    }
                }
            }
            match event {
                AdapterReadEvent::Heartbeat { .. } => {}
                AdapterReadEvent::Batch(batch) => {
                    let staged = match batch {
                        ReadBatch::Catalog(batch) => {
                            self.publication
                                .stage_catalog_batch(StageCatalogBatch { attempt_id, batch })
                                .await
                        }
                        ReadBatch::State(batch) => {
                            self.publication
                                .stage_state_batch(StageStateBatch { attempt_id, batch })
                                .await
                        }
                    };
                    if let Err(error) = staged {
                        self.abandon_current(
                            claimed.task.id,
                            attempt_id,
                            ReadFailure::new(codes.staging, error.to_string(), false),
                        )
                        .await?;
                        return Ok(ReadDisposition::Retire);
                    }
                }
                AdapterReadEvent::Completed(completed) => {
                    let command = publish_command(attempt_id, publication_id, completed);
                    let published = match (source_stream, fence.read_mode) {
                        (SourceStream::Catalog, ReadMode::Full) => {
                            self.publication.publish_full_catalog(command).await
                        }
                        (SourceStream::Catalog, ReadMode::Incremental) => {
                            self.publication.publish_incremental_catalog(command).await
                        }
                        (SourceStream::State, ReadMode::Full) => {
                            self.publication.publish_full_state(command).await
                        }
                        (SourceStream::State, ReadMode::Incremental) => {
                            self.publication.publish_incremental_state(command).await
                        }
                    };
                    if let Err(error) = published {
                        self.abandon_current(
                            claimed.task.id,
                            attempt_id,
                            ReadFailure::new(codes.publication, error.to_string(), false),
                        )
                        .await?;
                    }
                    return Ok(ReadDisposition::Restore);
                }
                AdapterReadEvent::Failed(error) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        ReadFailure::new(error.code, error.safe_message, error.retryable),
                    )
                    .await?;
                    return Ok(ReadDisposition::Restore);
                }
                AdapterReadEvent::Cancelled => {
                    self.abandon_cancelled(attempt_id).await?;
                    return Ok(ReadDisposition::Restore);
                }
            }
        }
    }

    async fn next_event<B, R>(
        &self,
        task_id: Uuid,
        operation_id: &[u8],
        session: &mut S,
        stream: &mut R,
        cancellation: &mut CancellationMonitor,
        shutdown: &mut Option<watch::Receiver<bool>>,
    ) -> Result<Option<AdapterReadEvent<B>>, NextEventError<T::Error, S::Error>>
    where
        R: AdapterReadStream<B, Error = S::Error>,
    {
        loop {
            if let Some(deadline) = cancellation.deadline {
                return next_cancelled_event(deadline, stream, shutdown).await;
            }
            tokio::select! {
                event = stream.next_event() => {
                    return event.map_err(NextEventError::Adapter);
                }
                _ = cancellation.interval.tick() => {
                    let status = self.tasks
                        .get_task(task_id)
                        .await
                        .map_err(NextEventError::Task)?
                        .map(|task| task.status)
                        .ok_or(NextEventError::TaskMissing)?;
                    if status == TaskStatus::Cancelling {
                        let deadline = Instant::now() + self.cancellation_grace_period;
                        cancellation.requested = true;
                        cancellation.deadline = Some(deadline);
                        cancel_before_deadline(
                            session.cancel_operation(operation_id.to_vec()),
                            deadline,
                            shutdown,
                        ).await?;
                    }
                }
                () = wait_for_shutdown(shutdown) => {
                    return Err(NextEventError::Shutdown);
                }
            }
        }
    }

    async fn abandon_current(
        &self,
        _task_id: Uuid,
        attempt_id: Uuid,
        failure: ReadFailure,
    ) -> Result<(), AcquisitionDispatchError<P::Error, T::Error, S::Error>> {
        self.publication
            .abandon_read(AbandonRead {
                attempt_id,
                outcome: ReadAbandonment::Failed(failure),
                completed_at: OffsetDateTime::now_utc(),
            })
            .await
            .map_err(AcquisitionDispatchError::Publication)
    }

    async fn abandon_cancelled(
        &self,
        attempt_id: Uuid,
    ) -> Result<(), AcquisitionDispatchError<P::Error, T::Error, S::Error>> {
        self.publication
            .abandon_read(AbandonRead {
                attempt_id,
                outcome: ReadAbandonment::Cancelled,
                completed_at: OffsetDateTime::now_utc(),
            })
            .await
            .map_err(AcquisitionDispatchError::Publication)
    }

    async fn restore_session(
        &self,
        connection_id: Uuid,
        session: S,
    ) -> Result<(), AcquisitionDispatchError<P::Error, T::Error, S::Error>> {
        if let Some(unexpected) = self.sessions.replace(connection_id, session) {
            if let Some(installed) = self.sessions.remove(connection_id) {
                let _ = installed.retire().await;
            }
            let _ = unexpected.retire().await;
            return Err(AcquisitionDispatchError::ConcurrentSession);
        }
        Ok(())
    }
}

struct CancellationMonitor {
    interval: tokio::time::Interval,
    requested: bool,
    deadline: Option<Instant>,
}

enum ReadBatch {
    Catalog(CatalogBatch),
    State(StateBatch),
}

enum ReadStream<S: AdapterSession> {
    Catalog(S::CatalogReadStream),
    State(S::StateReadStream),
}

impl<S: AdapterSession> AdapterReadStream<ReadBatch> for ReadStream<S> {
    type Error = S::Error;

    async fn next_event(&mut self) -> Result<Option<AdapterReadEvent<ReadBatch>>, Self::Error> {
        match self {
            Self::Catalog(stream) => stream
                .next_event()
                .await
                .map(|event| event.map(|event| map_batch(event, ReadBatch::Catalog))),
            Self::State(stream) => stream
                .next_event()
                .await
                .map(|event| event.map(|event| map_batch(event, ReadBatch::State))),
        }
    }
}

fn map_batch<B>(
    event: AdapterReadEvent<B>,
    wrap: impl FnOnce(B) -> ReadBatch,
) -> AdapterReadEvent<ReadBatch> {
    match event {
        AdapterReadEvent::Heartbeat { records_emitted } => {
            AdapterReadEvent::Heartbeat { records_emitted }
        }
        AdapterReadEvent::Batch(batch) => AdapterReadEvent::Batch(wrap(batch)),
        AdapterReadEvent::Completed(completed) => AdapterReadEvent::Completed(completed),
        AdapterReadEvent::Failed(error) => AdapterReadEvent::Failed(error),
        AdapterReadEvent::Cancelled => AdapterReadEvent::Cancelled,
    }
}

struct ReadFailureCodes {
    start: &'static str,
    ended: &'static str,
    stream: &'static str,
    staging: &'static str,
    publication: &'static str,
}

impl ReadFailureCodes {
    fn for_stream(stream: SourceStream) -> Self {
        match stream {
            SourceStream::Catalog => Self {
                start: "adapter_catalog_read_failed",
                ended: "adapter_catalog_stream_ended",
                stream: "adapter_catalog_stream_failed",
                staging: "catalog_staging_failed",
                publication: "catalog_publication_failed",
            },
            SourceStream::State => Self {
                start: "adapter_state_read_failed",
                ended: "adapter_state_stream_ended",
                stream: "adapter_state_stream_failed",
                staging: "state_staging_failed",
                publication: "state_publication_failed",
            },
        }
    }
}

#[derive(Clone, Copy)]
struct ReadAttempt {
    publication_id: Uuid,
    attempt_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadDisposition {
    Retire,
    Restore,
    Shutdown,
}

async fn cancel_before_deadline<T, E>(
    cancellation: impl std::future::Future<Output = Result<(), E>>,
    deadline: Instant,
    shutdown: &mut Option<watch::Receiver<bool>>,
) -> Result<(), NextEventError<T, E>> {
    tokio::select! {
        result = timeout_at(deadline, cancellation) => {
            result.map_err(|_| NextEventError::CancellationTimeout)?
                .map_err(NextEventError::Adapter)
        }
        () = wait_for_shutdown(shutdown) => Err(NextEventError::Shutdown),
    }
}

async fn next_cancelled_event<B, R, T>(
    deadline: Instant,
    stream: &mut R,
    shutdown: &mut Option<watch::Receiver<bool>>,
) -> Result<Option<AdapterReadEvent<B>>, NextEventError<T, R::Error>>
where
    R: AdapterReadStream<B>,
{
    tokio::select! {
        result = timeout_at(deadline, stream.next_event()) => {
            result
                .map_err(|_| NextEventError::CancellationTimeout)?
                .map_err(NextEventError::Adapter)
        }
        () = wait_for_shutdown(shutdown) => Err(NextEventError::Shutdown),
    }
}

async fn wait_for_shutdown(shutdown: &mut Option<watch::Receiver<bool>>) {
    let Some(shutdown) = shutdown else {
        pending().await
    };
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.wait_for(|requested| *requested).await;
}

impl CancellationMonitor {
    fn new(poll_interval: Duration) -> Self {
        let mut interval = interval(poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self {
            interval,
            requested: false,
            deadline: None,
        }
    }
}

fn read_command(fence: &ReadFence, operation_id: Vec<u8>, batch_size: u32) -> ReadAdapterSource {
    ReadAdapterSource {
        operation_id,
        source_key: AdapterKey {
            namespace: fence.provider_source_key.namespace.clone(),
            value: fence.provider_source_key.value.clone(),
        },
        mode: fence.read_mode,
        prior_cursor: match fence.read_mode {
            ReadMode::Full => None,
            ReadMode::Incremental => fence.expected_cursor.clone(),
        },
        preferred_batch_size: batch_size,
    }
}

fn publish_command(
    attempt_id: Uuid,
    publication_id: Uuid,
    completed: crate::application::adapter_runtime::AdapterReadCompletion,
) -> PublishRead {
    PublishRead {
        attempt_id,
        publication_id,
        next_cursor: Some(completed.next_cursor),
        evidence_revision: completed.evidence_revision,
        observed_at: completed.observed_at,
        published_at: OffsetDateTime::now_utc(),
    }
}

fn failed_completion(code: &str, retryable: bool) -> TaskHandlerOutcome {
    TaskHandlerOutcome::Complete(TaskCompletion::Failed(TaskFailure::coded(code, retryable)))
}

fn failed_completion_with_error(
    code: &str,
    error: impl std::fmt::Display,
    retryable: bool,
) -> TaskHandlerOutcome {
    TaskHandlerOutcome::Complete(TaskCompletion::Failed(TaskFailure::new(
        code,
        error.to_string(),
        retryable,
    )))
}

fn generate_uuid() -> Result<Uuid, getrandom::Error> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

#[derive(Debug, thiserror::Error)]
pub enum AcquisitionDispatchError<PublicationError, TaskError, AdapterError> {
    #[error("acquisition publication operation failed: {0}")]
    Publication(#[source] PublicationError),
    #[error("acquisition task operation failed: {0}")]
    Task(#[source] TaskError),
    #[error("acquisition adapter operation failed: {0}")]
    Adapter(#[source] AdapterError),
    #[error("acquisition session was replaced concurrently")]
    ConcurrentSession,
}

enum NextEventError<TaskError, AdapterError> {
    Task(TaskError),
    TaskMissing,
    Adapter(AdapterError),
    CancellationTimeout,
    Shutdown,
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, future::pending};

    use tokio::sync::watch;

    use super::*;
    use crate::application::adapter_runtime::{
        AdapterCallError, AdapterFailureCategory, AdapterReadCompletion,
    };

    struct SilentStream;

    impl AdapterReadStream<()> for SilentStream {
        type Error = Infallible;

        async fn next_event(&mut self) -> Result<Option<AdapterReadEvent<()>>, Self::Error> {
            pending().await
        }
    }

    #[tokio::test]
    async fn stalled_cancellation_rpc_expires_at_grace_deadline() {
        let result =
            cancel_before_deadline::<Infallible, Infallible>(pending(), Instant::now(), &mut None)
                .await;
        assert!(matches!(result, Err(NextEventError::CancellationTimeout)));
    }

    #[tokio::test]
    async fn shutdown_interrupts_stalled_cancellation_rpc() {
        let (_sender, receiver) = watch::channel(true);
        let result = cancel_before_deadline::<Infallible, Infallible>(
            pending(),
            Instant::now() + Duration::from_secs(60),
            &mut Some(receiver),
        )
        .await;
        assert!(matches!(result, Err(NextEventError::Shutdown)));
    }

    #[tokio::test]
    async fn cancelled_silent_read_expires_at_grace_deadline() {
        let mut stream = SilentStream;
        let result =
            next_cancelled_event::<(), _, Infallible>(Instant::now(), &mut stream, &mut None).await;
        assert!(matches!(result, Err(NextEventError::CancellationTimeout)));
    }

    #[tokio::test]
    async fn shutdown_interrupts_cancellation_grace_wait() {
        let (sender, receiver) = watch::channel(true);
        let mut stream = SilentStream;
        let result = next_cancelled_event::<(), _, Infallible>(
            Instant::now() + Duration::from_secs(60),
            &mut stream,
            &mut Some(receiver),
        )
        .await;
        assert!(matches!(result, Err(NextEventError::Shutdown)));
        drop(sender);
    }

    fn completion() -> AdapterReadCompletion {
        AdapterReadCompletion {
            next_cursor: vec![1, 2],
            evidence_revision: vec![3, 4],
            observed_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn publication_uses_current_time_and_preserves_adapter_evidence() {
        let before = OffsetDateTime::now_utc();
        let attempt_id = Uuid::from_u128(1);
        let publication_id = Uuid::from_u128(2);
        let completed = completion();
        let command = publish_command(attempt_id, publication_id, completed.clone());
        assert!(command.published_at >= before);
        assert!(command.published_at <= OffsetDateTime::now_utc());
        assert_eq!(command.attempt_id, attempt_id);
        assert_eq!(command.publication_id, publication_id);
        assert_eq!(command.next_cursor, Some(completed.next_cursor));
        assert_eq!(command.evidence_revision, completed.evidence_revision);
        assert_eq!(command.observed_at, completed.observed_at);
    }

    #[test]
    fn shared_events_preserve_typed_batches() {
        let catalog = CatalogBatch::new(7, vec![], vec![], vec![], vec![], vec![], vec![]);
        let state = StateBatch::new(8, vec![]);
        assert!(matches!(
            map_batch(AdapterReadEvent::Batch(catalog.clone()), ReadBatch::Catalog),
            AdapterReadEvent::Batch(ReadBatch::Catalog(batch)) if batch == catalog
        ));
        assert!(matches!(
            map_batch(AdapterReadEvent::Batch(state.clone()), ReadBatch::State),
            AdapterReadEvent::Batch(ReadBatch::State(batch)) if batch == state
        ));
    }

    #[test]
    fn shared_events_preserve_control_events_for_both_streams() {
        let failure = AdapterCallError {
            category: AdapterFailureCategory::Unavailable,
            code: "unavailable".to_owned(),
            safe_message: "provider unavailable".to_owned(),
            retryable: true,
            retry_after: None,
            field_problems: vec![],
        };
        for catalog in [true, false] {
            let wrap = |()| {
                if catalog {
                    ReadBatch::Catalog(CatalogBatch::new(
                        1,
                        vec![],
                        vec![],
                        vec![],
                        vec![],
                        vec![],
                        vec![],
                    ))
                } else {
                    ReadBatch::State(StateBatch::new(1, vec![]))
                }
            };
            assert!(matches!(
                map_batch(
                    AdapterReadEvent::Heartbeat {
                        records_emitted: 42
                    },
                    wrap
                ),
                AdapterReadEvent::Heartbeat {
                    records_emitted: 42
                }
            ));
            assert!(matches!(
                map_batch(AdapterReadEvent::Completed(completion()), wrap),
                AdapterReadEvent::Completed(completed) if completed == completion()
            ));
            assert!(matches!(
                map_batch(AdapterReadEvent::Failed(failure.clone()), wrap),
                AdapterReadEvent::Failed(error) if error == failure
            ));
            assert!(matches!(
                map_batch(AdapterReadEvent::Cancelled, wrap),
                AdapterReadEvent::Cancelled
            ));
        }
    }

    #[tokio::test]
    async fn shutdown_interrupts_a_pending_read() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let waiter = tokio::spawn(async move {
            let mut shutdown = Some(shutdown_receiver);
            tokio::select! {
                () = pending() => false,
                () = wait_for_shutdown(&mut shutdown) => true,
            }
        });

        shutdown_sender
            .send(true)
            .expect("shutdown receiver exists");

        assert!(waiter.await.expect("shutdown waiter completes"));
    }
}
