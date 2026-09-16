use std::{env, error::Error, future::Future, io};

use tracing_subscriber::filter::LevelFilter;
use trakkin_provider_sdk::InvocationContext;
use uuid::Uuid;

pub(crate) const LOG_LEVEL_ENV: &str = "TRAKKIN_LOG";
const DEFAULT_LOG_LEVEL: &str = "info";

tokio::task_local! {
    static ACTIVE_INVOCATION_CONTEXT: InvocationContext;
}

pub async fn in_invocation_context<F>(context: InvocationContext, future: F) -> F::Output
where
    F: Future,
{
    ACTIVE_INVOCATION_CONTEXT.scope(context, future).await
}

pub fn current_invocation_context() -> Option<InvocationContext> {
    ACTIVE_INVOCATION_CONTEXT.try_with(Clone::clone).ok()
}

pub fn task_invocation_context(task_id: Uuid) -> InvocationContext {
    current_invocation_context().unwrap_or_else(|| {
        InvocationContext::new(&format!("task:{task_id}"), None, None)
            .expect("task UUID correlation ID is valid")
    })
}

pub fn generate_correlation_id(prefix: &str) -> String {
    let mut bytes = [0_u8; 16];
    if getrandom::fill(&mut bytes).is_ok() {
        return format!("{prefix}:{}", hex::encode(bytes));
    }
    format!("{prefix}:unavailable")
}

pub(crate) fn configured_log_level() -> String {
    env::var(LOG_LEVEL_ENV).unwrap_or_else(|_| DEFAULT_LOG_LEVEL.to_owned())
}

pub fn init() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_max_level(configured_log_level().parse::<LevelFilter>()?)
        .with_writer(io::stderr)
        .try_init()?;
    Ok(())
}
