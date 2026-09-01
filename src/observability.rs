use std::{env, error::Error, future::Future, io};

use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};
use trakkin_provider_sdk::InvocationContext;
use uuid::Uuid;

pub const LOG_FILTER_ENV: &str = "TRAKKIN_LOG";
pub const LOG_FORMAT_ENV: &str = "TRAKKIN_LOG_FORMAT";

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

pub fn init() -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter =
        env::var(LOG_FILTER_ENV).unwrap_or_else(|_| "trakkin=info,tower_http=info".to_owned());
    let filter = EnvFilter::try_new(filter)?;
    let format = env::var(LOG_FORMAT_ENV).unwrap_or_else(|_| "json".to_owned());
    if format.eq_ignore_ascii_case("pretty") {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_span_events(FmtSpan::CLOSE)
                    .with_writer(io::stderr),
            )
            .try_init()?;
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .flatten_event(true)
                    .with_ansi(false)
                    .with_span_events(FmtSpan::CLOSE)
                    .with_writer(io::stderr),
            )
            .try_init()?;
    }
    tracing::info!(event = "host.logging.initialized");
    Ok(())
}
