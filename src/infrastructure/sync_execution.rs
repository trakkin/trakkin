use sqlx::{FromRow, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

pub(crate) async fn reduce_sync_run(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    updated_at: OffsetDateTime,
) -> Result<SyncRunReduction, sqlx::Error> {
    let Some(previous_status) = sqlx::query_scalar::<_, String>(
        "SELECT status FROM sync_runs WHERE id = $1 FOR UPDATE
",
    )
    .bind(run_id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(SyncRunReduction::not_updated());
    };
    let reduction = sqlx::query_as::<_, RunReductionRow>(
        "SELECT
    COUNT(*) FILTER (WHERE status = 'queued') AS queued,
    COUNT(*) FILTER (
        WHERE status IN ('queued', 'running', 'retryable_failure')
    ) AS active,
    COUNT(*) FILTER (WHERE status IN ('applied', 'no_op')) AS succeeded,
    COUNT(*) FILTER (WHERE status = 'indeterminate') AS indeterminate,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
    COUNT(*) AS total
FROM sync_actions
WHERE run_id = $1
",
    )
    .bind(run_id)
    .fetch_one(&mut **transaction)
    .await?;
    if reduction.total == 0 {
        return Ok(SyncRunReduction::not_updated());
    }
    let (status, finished_at) = if reduction.queued == reduction.total {
        ("queued", None)
    } else if reduction.active > 0 {
        ("running", None)
    } else if reduction.indeterminate > 0 {
        ("indeterminate", Some(updated_at))
    } else if reduction.succeeded == reduction.total {
        ("succeeded", Some(updated_at))
    } else if reduction.succeeded > 0 {
        ("partial", Some(updated_at))
    } else if reduction.cancelled == reduction.total {
        ("cancelled", Some(updated_at))
    } else {
        ("failed", Some(updated_at))
    };
    let run = sqlx::query(
        "UPDATE sync_runs
SET status = $1, finished_at = $2, updated_at = $3
WHERE id = $4
",
    )
    .bind(status)
    .bind(finished_at)
    .bind(updated_at)
    .bind(run_id)
    .execute(&mut **transaction)
    .await?;
    let updated = run.rows_affected() == 1;
    let outcome =
        (updated && reduction.active == 0 && previous_status != status).then(|| SyncRunOutcome {
            run_id,
            status,
            total: reduction.total,
            succeeded: reduction.succeeded,
            indeterminate: reduction.indeterminate,
            cancelled: reduction.cancelled,
            failed: reduction
                .total
                .saturating_sub(reduction.succeeded)
                .saturating_sub(reduction.indeterminate)
                .saturating_sub(reduction.cancelled),
        });
    Ok(SyncRunReduction { updated, outcome })
}

pub(crate) struct SyncRunReduction {
    pub(crate) updated: bool,
    outcome: Option<SyncRunOutcome>,
}

impl SyncRunReduction {
    fn not_updated() -> Self {
        Self {
            updated: false,
            outcome: None,
        }
    }

    pub(crate) fn record_outcome(self) {
        let Some(outcome) = self.outcome else {
            return;
        };
        if outcome.status == "indeterminate" {
            tracing::warn!(
                sync.run.id = %outcome.run_id,
                run.status = outcome.status,
                actions.total = outcome.total,
                actions.succeeded = outcome.succeeded,
                actions.indeterminate = outcome.indeterminate,
                actions.cancelled = outcome.cancelled,
                actions.failed = outcome.failed,
                "synchronization run outcome is indeterminate"
            );
        } else if matches!(outcome.status, "succeeded" | "cancelled") {
            tracing::info!(
                sync.run.id = %outcome.run_id,
                run.status = outcome.status,
                actions.total = outcome.total,
                actions.succeeded = outcome.succeeded,
                actions.indeterminate = outcome.indeterminate,
                actions.cancelled = outcome.cancelled,
                actions.failed = outcome.failed,
                "synchronization run completed"
            );
        } else {
            tracing::warn!(
                sync.run.id = %outcome.run_id,
                run.status = outcome.status,
                actions.total = outcome.total,
                actions.succeeded = outcome.succeeded,
                actions.indeterminate = outcome.indeterminate,
                actions.cancelled = outcome.cancelled,
                actions.failed = outcome.failed,
                "synchronization run completed"
            );
        }
    }
}

struct SyncRunOutcome {
    run_id: Uuid,
    status: &'static str,
    total: i64,
    succeeded: i64,
    indeterminate: i64,
    cancelled: i64,
    failed: i64,
}

#[derive(FromRow)]
struct RunReductionRow {
    queued: i64,
    active: i64,
    succeeded: i64,
    indeterminate: i64,
    cancelled: i64,
    total: i64,
}
