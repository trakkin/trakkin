use std::{future::Future, pin::Pin};

use time::OffsetDateTime;

pub type TaskRepositoryFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InterruptActiveTasks {
    pub interrupted_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InterruptionSummary {
    pub tasks: u64,
    pub task_attempts: u64,
    pub read_attempts: u64,
    pub sync_actions_confirmed_not_applied: u64,
    pub sync_actions_indeterminate: u64,
    pub claims: u64,
}

pub trait TaskLifecycleRepository: Send + Sync {
    type Error;

    fn interrupt_active_tasks(
        &self,
        command: InterruptActiveTasks,
    ) -> TaskRepositoryFuture<'_, InterruptionSummary, Self::Error>;
}
