use std::{future::Future, pin::Pin};

use time::OffsetDateTime;

pub type TaskRepositoryFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InterruptActiveTasks {
    pub interrupted_at: OffsetDateTime,
}

pub trait TaskLifecycleRepository: Send + Sync {
    type Error;

    fn interrupt_active_tasks(
        &self,
        command: InterruptActiveTasks,
    ) -> TaskRepositoryFuture<'_, (), Self::Error>;
}
