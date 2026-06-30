use std::{marker::PhantomData, time::Duration};

use time::OffsetDateTime;
use tokio::time::{Instant, MissedTickBehavior, interval, timeout_at};
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
        ClaimedTask, TaskCompletion, TaskDefinition, TaskFailure, TaskKind, TaskManager,
        TaskRepository, TaskStatus,
    },
};
use crate::infrastructure::connection::ProcessSessionRegistry;

const READ_TASK_KINDS: [TaskKind; 4] = [
    TaskKind::CatalogRead,
    TaskKind::StateRead,
    TaskKind::CatalogReset,
    TaskKind::StateReset,
];

pub struct AcquisitionTaskHandler<P, T, S> {
    publication: P,
    tasks: TaskManager<T>,
    sessions: ProcessSessionRegistry<S>,
    preferred_batch_size: u32,
    cancellation_poll_interval: Duration,
    cancellation_grace_period: Duration,
    session: PhantomData<fn() -> S>,
}

impl<P, T, S> AcquisitionTaskHandler<P, T, S> {
    pub fn new(publication: P, tasks: TaskManager<T>, sessions: ProcessSessionRegistry<S>) -> Self {
        Self {
            publication,
            tasks,
            sessions,
            preferred_batch_size: 100,
            cancellation_poll_interval: Duration::from_millis(50),
            cancellation_grace_period: Duration::from_secs(5),
            session: PhantomData,
        }
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
    P::Error: Send,
    T: TaskRepository,
    T::Error: Send,
    S: AdapterSession,
    S::Error: Send,
{
    type Error = AcquisitionDispatchError<P::Error, T::Error, S::Error>;

    fn accepted_kinds(&self) -> &'static [TaskKind] {
        &READ_TASK_KINDS
    }

    fn execute<'a>(
        &'a self,
        claimed: &'a ClaimedTask,
        completed_at: OffsetDateTime,
    ) -> TaskHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            match &claimed.task.definition {
                TaskDefinition::Reset { .. } => {
                    let outcome = match self
                        .publication
                        .reset_source_stream(ResetSourceStream {
                            task_id: claimed.task.id,
                            task_attempt_id: claimed.attempt_id,
                            owner_id: claimed.owner_id,
                            completed_at,
                        })
                        .await
                    {
                        Ok(()) => TaskHandlerOutcome::Terminalized,
                        Err(_) => failed_completion("source_reset_failed", false),
                    };
                    Ok(outcome)
                }
                TaskDefinition::Read { stream, .. } => {
                    let Some(mut session) = self.sessions.remove(claimed.task.connection_id) else {
                        return Ok(failed_completion("connection_session_unavailable", true));
                    };
                    let attempt_id = match generate_uuid() {
                        Ok(attempt_id) => attempt_id,
                        Err(_) => {
                            let _ = self
                                .restore_session(claimed.task.connection_id, session)
                                .await;
                            return Ok(failed_completion("operation_id_generation_failed", false));
                        }
                    };
                    let publication_id = match generate_uuid() {
                        Ok(publication_id) => publication_id,
                        Err(_) => {
                            let _ = self
                                .restore_session(claimed.task.connection_id, session)
                                .await;
                            return Ok(failed_completion(
                                "publication_id_generation_failed",
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
                        Err(_) => {
                            let restored = self
                                .restore_session(claimed.task.connection_id, session)
                                .await;
                            let code = if restored.is_ok() {
                                "source_read_not_started"
                            } else {
                                "connection_session_conflict"
                            };
                            return Ok(failed_completion(code, false));
                        }
                    };
                    let read_result = match stream {
                        crate::application::task_manager::SourceStream::Catalog => {
                            self.run_catalog_read(
                                claimed,
                                completed_at,
                                publication_id,
                                attempt_id,
                                fence,
                                &mut session,
                            )
                            .await
                        }
                        crate::application::task_manager::SourceStream::State => {
                            self.run_state_read(
                                claimed,
                                completed_at,
                                publication_id,
                                attempt_id,
                                fence,
                                &mut session,
                            )
                            .await
                        }
                    };
                    let retire = match read_result {
                        Ok(retire) => retire,
                        Err(error) => {
                            let _ = session.retire().await;
                            return Err(error);
                        }
                    };
                    if retire {
                        session
                            .retire()
                            .await
                            .map_err(AcquisitionDispatchError::Adapter)?;
                    } else {
                        self.restore_session(claimed.task.connection_id, session)
                            .await?;
                    }
                    Ok(TaskHandlerOutcome::Terminalized)
                }
                TaskDefinition::SourceDiscovery { .. }
                | TaskDefinition::SyncAction { .. }
                | TaskDefinition::SyncActionReconciliation { .. } => {
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
{
    async fn run_catalog_read(
        &self,
        claimed: &ClaimedTask,
        completed_at: OffsetDateTime,
        publication_id: Uuid,
        attempt_id: Uuid,
        fence: ReadFence,
        session: &mut S,
    ) -> Result<bool, AcquisitionDispatchError<P::Error, T::Error, S::Error>> {
        let operation_id = attempt_id.as_bytes().to_vec();
        let mut stream = match session
            .read_catalog(read_command(
                &fence,
                operation_id.clone(),
                self.preferred_batch_size,
            ))
            .await
        {
            Ok(stream) => stream,
            Err(_) => {
                self.abandon_current(
                    claimed.task.id,
                    attempt_id,
                    completed_at,
                    ReadFailure {
                        code: "adapter_catalog_read_failed".to_owned(),
                        retryable: true,
                    },
                )
                .await?;
                return Ok(true);
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
                )
                .await;
            let event = match next {
                Ok(Some(event)) => event,
                Ok(None) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        completed_at,
                        ReadFailure {
                            code: "adapter_catalog_stream_ended".to_owned(),
                            retryable: true,
                        },
                    )
                    .await?;
                    return Ok(true);
                }
                Err(NextEventError::CancellationTimeout) => {
                    self.abandon_cancelled(attempt_id, completed_at).await?;
                    return Ok(true);
                }
                Err(NextEventError::Task(_)) | Err(NextEventError::TaskMissing) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        completed_at,
                        ReadFailure {
                            code: "task_state_unavailable".to_owned(),
                            retryable: true,
                        },
                    )
                    .await?;
                    return Ok(true);
                }
                Err(NextEventError::Adapter(_)) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        completed_at,
                        ReadFailure {
                            code: "adapter_catalog_stream_failed".to_owned(),
                            retryable: true,
                        },
                    )
                    .await?;
                    return Ok(true);
                }
            };
            if cancellation.requested {
                match event {
                    AdapterReadEvent::Heartbeat { .. } | AdapterReadEvent::Batch(_) => continue,
                    AdapterReadEvent::Completed(_)
                    | AdapterReadEvent::Failed(_)
                    | AdapterReadEvent::Cancelled => {
                        self.abandon_cancelled(attempt_id, completed_at).await?;
                        return Ok(false);
                    }
                }
            }
            match event {
                AdapterReadEvent::Heartbeat { .. } => {}
                AdapterReadEvent::Batch(batch) => {
                    if self
                        .publication
                        .stage_catalog_batch(StageCatalogBatch { attempt_id, batch })
                        .await
                        .is_err()
                    {
                        self.abandon_current(
                            claimed.task.id,
                            attempt_id,
                            completed_at,
                            ReadFailure {
                                code: "catalog_staging_failed".to_owned(),
                                retryable: false,
                            },
                        )
                        .await?;
                        return Ok(false);
                    }
                }
                AdapterReadEvent::Completed(completed) => {
                    let command =
                        publish_command(attempt_id, publication_id, completed, completed_at);
                    let published = match fence.read_mode {
                        ReadMode::Full => self.publication.publish_full_catalog(command).await,
                        ReadMode::Incremental => {
                            self.publication.publish_incremental_catalog(command).await
                        }
                    };
                    if published.is_err() {
                        self.abandon_current(
                            claimed.task.id,
                            attempt_id,
                            completed_at,
                            ReadFailure {
                                code: "catalog_publication_failed".to_owned(),
                                retryable: false,
                            },
                        )
                        .await?;
                    }
                    return Ok(false);
                }
                AdapterReadEvent::Failed(error) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        completed_at,
                        ReadFailure {
                            code: error.code,
                            retryable: error.retryable,
                        },
                    )
                    .await?;
                    return Ok(false);
                }
                AdapterReadEvent::Cancelled => {
                    self.abandon_cancelled(attempt_id, completed_at).await?;
                    return Ok(false);
                }
            }
        }
    }

    async fn run_state_read(
        &self,
        claimed: &ClaimedTask,
        completed_at: OffsetDateTime,
        publication_id: Uuid,
        attempt_id: Uuid,
        fence: ReadFence,
        session: &mut S,
    ) -> Result<bool, AcquisitionDispatchError<P::Error, T::Error, S::Error>> {
        let operation_id = attempt_id.as_bytes().to_vec();
        let mut stream = match session
            .read_state(read_command(
                &fence,
                operation_id.clone(),
                self.preferred_batch_size,
            ))
            .await
        {
            Ok(stream) => stream,
            Err(_) => {
                self.abandon_current(
                    claimed.task.id,
                    attempt_id,
                    completed_at,
                    ReadFailure {
                        code: "adapter_state_read_failed".to_owned(),
                        retryable: true,
                    },
                )
                .await?;
                return Ok(true);
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
                )
                .await;
            let event = match next {
                Ok(Some(event)) => event,
                Ok(None) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        completed_at,
                        ReadFailure {
                            code: "adapter_state_stream_ended".to_owned(),
                            retryable: true,
                        },
                    )
                    .await?;
                    return Ok(true);
                }
                Err(NextEventError::CancellationTimeout) => {
                    self.abandon_cancelled(attempt_id, completed_at).await?;
                    return Ok(true);
                }
                Err(NextEventError::Task(_)) | Err(NextEventError::TaskMissing) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        completed_at,
                        ReadFailure {
                            code: "task_state_unavailable".to_owned(),
                            retryable: true,
                        },
                    )
                    .await?;
                    return Ok(true);
                }
                Err(NextEventError::Adapter(_)) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        completed_at,
                        ReadFailure {
                            code: "adapter_state_stream_failed".to_owned(),
                            retryable: true,
                        },
                    )
                    .await?;
                    return Ok(true);
                }
            };
            if cancellation.requested {
                match event {
                    AdapterReadEvent::Heartbeat { .. } | AdapterReadEvent::Batch(_) => continue,
                    AdapterReadEvent::Completed(_)
                    | AdapterReadEvent::Failed(_)
                    | AdapterReadEvent::Cancelled => {
                        self.abandon_cancelled(attempt_id, completed_at).await?;
                        return Ok(false);
                    }
                }
            }
            match event {
                AdapterReadEvent::Heartbeat { .. } => {}
                AdapterReadEvent::Batch(batch) => {
                    if self
                        .publication
                        .stage_state_batch(StageStateBatch { attempt_id, batch })
                        .await
                        .is_err()
                    {
                        self.abandon_current(
                            claimed.task.id,
                            attempt_id,
                            completed_at,
                            ReadFailure {
                                code: "state_staging_failed".to_owned(),
                                retryable: false,
                            },
                        )
                        .await?;
                        return Ok(false);
                    }
                }
                AdapterReadEvent::Completed(completed) => {
                    let command =
                        publish_command(attempt_id, publication_id, completed, completed_at);
                    let published = match fence.read_mode {
                        ReadMode::Full => self.publication.publish_full_state(command).await,
                        ReadMode::Incremental => {
                            self.publication.publish_incremental_state(command).await
                        }
                    };
                    if published.is_err() {
                        self.abandon_current(
                            claimed.task.id,
                            attempt_id,
                            completed_at,
                            ReadFailure {
                                code: "state_publication_failed".to_owned(),
                                retryable: false,
                            },
                        )
                        .await?;
                    }
                    return Ok(false);
                }
                AdapterReadEvent::Failed(error) => {
                    self.abandon_current(
                        claimed.task.id,
                        attempt_id,
                        completed_at,
                        ReadFailure {
                            code: error.code,
                            retryable: error.retryable,
                        },
                    )
                    .await?;
                    return Ok(false);
                }
                AdapterReadEvent::Cancelled => {
                    self.abandon_cancelled(attempt_id, completed_at).await?;
                    return Ok(false);
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
    ) -> Result<Option<AdapterReadEvent<B>>, NextEventError<T::Error, S::Error>>
    where
        R: AdapterReadStream<B, Error = S::Error>,
    {
        if let Some(deadline) = cancellation.deadline {
            return timeout_at(deadline, stream.next_event())
                .await
                .map_err(|_| NextEventError::CancellationTimeout)?
                .map_err(NextEventError::Adapter);
        }
        loop {
            tokio::select! {
                event = stream.next_event() => {
                    return event.map_err(NextEventError::Adapter);
                }
                _ = cancellation.interval.tick() => {
                    let status = self.tasks
                        .get(task_id)
                        .await
                        .map_err(NextEventError::Task)?
                        .map(|task| task.status)
                        .ok_or(NextEventError::TaskMissing)?;
                    if status == TaskStatus::Cancelling {
                        if let Err(error) = session.cancel_operation(operation_id.to_vec()).await {
                            return Err(NextEventError::Adapter(error));
                        }
                        cancellation.requested = true;
                        cancellation.deadline = Some(Instant::now() + self.cancellation_grace_period);
                    }
                }
            }
        }
    }

    async fn abandon_current(
        &self,
        _task_id: Uuid,
        attempt_id: Uuid,
        completed_at: OffsetDateTime,
        failure: ReadFailure,
    ) -> Result<(), AcquisitionDispatchError<P::Error, T::Error, S::Error>> {
        self.publication
            .abandon_read(AbandonRead {
                attempt_id,
                outcome: ReadAbandonment::Failed(failure),
                completed_at,
            })
            .await
            .map_err(AcquisitionDispatchError::Publication)
    }

    async fn abandon_cancelled(
        &self,
        attempt_id: Uuid,
        completed_at: OffsetDateTime,
    ) -> Result<(), AcquisitionDispatchError<P::Error, T::Error, S::Error>> {
        self.publication
            .abandon_read(AbandonRead {
                attempt_id,
                outcome: ReadAbandonment::Cancelled,
                completed_at,
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
    published_at: OffsetDateTime,
) -> PublishRead {
    PublishRead {
        attempt_id,
        publication_id,
        next_cursor: Some(completed.next_cursor),
        evidence_revision: completed.evidence_revision,
        observed_at: completed.observed_at,
        published_at,
    }
}

fn failed_completion(code: &str, retryable: bool) -> TaskHandlerOutcome {
    TaskHandlerOutcome::Complete(TaskCompletion::Failed(TaskFailure {
        code: code.to_owned(),
        retryable,
    }))
}

fn generate_uuid() -> Result<Uuid, getrandom::Error> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

#[derive(Debug)]
pub enum AcquisitionDispatchError<PublicationError, TaskError, AdapterError> {
    Publication(PublicationError),
    Task(TaskError),
    Adapter(AdapterError),
    ConcurrentSession,
}

enum NextEventError<TaskError, AdapterError> {
    Task(TaskError),
    TaskMissing,
    Adapter(AdapterError),
    CancellationTimeout,
}
