use std::{sync::Arc, time::Duration};

use futures_util::StreamExt;
use sqlx::FromRow;
use time::OffsetDateTime;
use tokio::sync::watch;
use trakkin_mappings_corpus::git::Revision;

use super::mappings::MappingStore;
use crate::application::error::ApplicationError;

#[derive(Clone, FromRow)]
pub(super) struct DueSource {
    pub id: String,
    pub generation: i64,
    pub url: String,
    pub branch: String,
    pub poll_interval_seconds: i64,
    pub revision: Option<String>,
    pub consecutive_failures: i32,
}

pub(super) enum CheckResult {
    Unchanged,
    Updated(Revision),
    Failed(String),
}

pub struct MappingUpdates {
    store: Arc<MappingStore>,
}

impl MappingUpdates {
    pub fn new(store: Arc<MappingStore>) -> Self {
        Self { store }
    }

    pub async fn poll_due(&self, now: OffsetDateTime) -> Result<(), ApplicationError> {
        let sources = sqlx::query_as::<_, DueSource>(
            "SELECT\n    id,\n    generation,\n    url,\n    branch,\n    poll_interval_seconds,\n    revision,\n    consecutive_failures\nFROM mapping_sources\nWHERE enabled AND kind = 'upstream' AND next_check_at <= $1\nORDER BY next_check_at, id\nLIMIT 32\n",
        )
        .bind(now)
        .fetch_all(&self.store.pool)
        .await
        .map_err(|error| ApplicationError::internal(format!("mapping scheduling failed: {error}")))?;
        let mut checks = futures_util::stream::iter(sources.into_iter().map(|source| async move {
            let response = match self.store.repositories.upstream(&source.id) {
                Ok(repository) => match repository.fetch(&source.url, &source.branch).await {
                    Ok(revision) if source.revision.as_deref() == Some(&revision.commit) => {
                        CheckResult::Unchanged
                    }
                    Ok(revision) => CheckResult::Updated(revision),
                    Err(error) => CheckResult::Failed(error.to_string()),
                },
                Err(error) => CheckResult::Failed(error.to_string()),
            };
            self.store.complete_check(&source, response, now).await
        }))
        .buffer_unordered(4);
        while let Some(result) = checks.next().await {
            result?;
        }
        Ok(())
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !*shutdown.borrow() {
            tokio::select! {
                result = shutdown.changed() => { if result.is_err() || *shutdown.borrow() { return; } }
                _ = interval.tick() => {
                    tokio::select! {
                        result = shutdown.changed() => { if result.is_err() || *shutdown.borrow() { return; } }
                        result = self.poll_due(OffsetDateTime::now_utc()) => {
                            if let Err(error) = result { tracing::warn!(error = %error, "mapping update scheduling failed"); }
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn retry_delay(interval: i64, failures: i32) -> i64 {
    if failures == 0 {
        interval
    } else {
        interval
            .saturating_mul(1_i64 << (failures - 1).min(16))
            .min(604800)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_back_off_without_overflow() {
        assert_eq!(retry_delay(60, 0), 60);
        assert_eq!(retry_delay(60, 1), 60);
        assert_eq!(retry_delay(60, 2), 120);
        assert_eq!(retry_delay(60, 100), 604800);
    }
}
