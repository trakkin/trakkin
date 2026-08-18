use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::application::sync_retention::{
    RetainSyncEvidence, SyncRetentionClass, SyncRetentionClassResult, SyncRetentionError,
    SyncRetentionFuture, SyncRetentionReport, SyncRetentionRepository,
};

#[derive(Clone)]
pub struct PostgresSyncRetentionRepository {
    pool: PgPool,
}

impl PostgresSyncRetentionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn workspace_ids(&self) -> Result<Vec<Uuid>, PostgresSyncRetentionRepositoryError> {
        Ok(sqlx::query_scalar(
            "SELECT id FROM workspaces WHERE status = 'active' ORDER BY id
",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn retain(
        &self,
        command: RetainSyncEvidence,
    ) -> Result<SyncRetentionReport, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended($1::TEXT, 0))
",
        )
        .bind(command.workspace_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "SELECT set_config('trakkin.sync_retention', 'enabled', TRUE)
    ",
        )
        .execute(&mut *transaction)
        .await?;

        let audit_cutoff = command
            .retained_at
            .checked_sub(command.policy.audit_retention)
            .ok_or(SyncRetentionError::InvalidPolicy)?;
        let mut remaining = command.policy.maximum_deleted_rows;
        let mut unused = ClassAccumulator::default();
        let mut runs = ClassAccumulator::default();
        let mut policies = ClassAccumulator::default();
        let mut halted = false;
        let mut next_batch_minimum_rows = None;

        let preview_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT previews.id
FROM sync_previews AS previews
WHERE
    previews.workspace_id = $1
    AND previews.expires_at <= $2
    AND NOT EXISTS (
        SELECT 1 FROM sync_runs AS runs WHERE runs.preview_id = previews.id
    )
ORDER BY previews.expires_at, previews.id
FOR UPDATE OF previews SKIP LOCKED
LIMIT $3
",
        )
        .bind(command.workspace_id)
        .bind(command.retained_at)
        .bind(i64::from(command.policy.maximum_deleted_rows))
        .fetch_all(&mut *transaction)
        .await?;
        for preview_id in preview_ids {
            let rows = unused_preview_rows(&mut transaction, preview_id).await?;
            if rows > remaining {
                halted = true;
                next_batch_minimum_rows = Some(rows);
                break;
            }
            let deleted = sqlx::query(
                "DELETE FROM sync_previews
WHERE id = $1 AND workspace_id = $2
",
            )
            .bind(preview_id)
            .bind(command.workspace_id)
            .execute(&mut *transaction)
            .await?;
            if deleted.rows_affected() != 1 {
                return Err(SyncRetentionError::RepositoryInvariant);
            }
            unused.record(rows)?;
            remaining -= rows;
        }

        if !halted && remaining > 0 {
            let candidates = sqlx::query_as::<_, RunCandidate>(
                "SELECT id, preview_id
FROM sync_runs
WHERE
    workspace_id = $1
    AND finished_at < $2
    AND status NOT IN ('queued', 'running')
    AND NOT EXISTS (
        SELECT 1
        FROM sync_action_tasks AS action_tasks
        INNER JOIN
            sync_actions AS actions
            ON actions.id = action_tasks.action_id
        INNER JOIN tasks ON tasks.id = action_tasks.task_id
        WHERE
            actions.run_id = sync_runs.id
            AND tasks.status IN ('queued', 'running', 'cancelling')
    )
    AND NOT EXISTS (
        SELECT 1
        FROM sync_action_reconciliation_tasks AS reconciliation_tasks
        INNER JOIN sync_actions AS actions
            ON actions.id = reconciliation_tasks.action_id
        INNER JOIN tasks ON tasks.id = reconciliation_tasks.task_id
        WHERE
            actions.run_id = sync_runs.id
            AND tasks.status IN ('queued', 'running', 'cancelling')
    )
ORDER BY finished_at, id
FOR UPDATE SKIP LOCKED
LIMIT $3
",
            )
            .bind(command.workspace_id)
            .bind(audit_cutoff)
            .bind(i64::from(remaining))
            .fetch_all(&mut *transaction)
            .await?;
            for candidate in candidates {
                let rows = run_bundle_rows(&mut transaction, candidate.id).await?;
                if rows > remaining {
                    halted = true;
                    next_batch_minimum_rows = Some(rows);
                    break;
                }
                let deleted = delete_run_bundle(&mut transaction, &candidate).await?;
                if deleted != rows {
                    return Err(SyncRetentionError::RepositoryInvariant);
                }
                runs.record(rows)?;
                remaining -= rows;
            }
        }

        if !halted && remaining > 0 {
            let revisions = sqlx::query_as::<_, PolicyRevisionCandidate>(
                "SELECT revisions.policy_id, revisions.revision
FROM sync_policy_revisions AS revisions
INNER JOIN sync_policies AS policies
    ON
        policies.id = revisions.policy_id
        AND policies.workspace_id = revisions.workspace_id
WHERE
    revisions.workspace_id = $1
    AND revisions.created_at < $2
    AND revisions.revision <> policies.head_revision
    AND NOT EXISTS (
        SELECT 1
        FROM sync_previews AS previews
        WHERE
            previews.policy_id = revisions.policy_id
            AND previews.policy_revision = revisions.revision
    )
ORDER BY revisions.created_at, revisions.policy_id, revisions.revision
FOR UPDATE OF revisions SKIP LOCKED
LIMIT $3
",
            )
            .bind(command.workspace_id)
            .bind(audit_cutoff)
            .bind(i64::from(remaining))
            .fetch_all(&mut *transaction)
            .await?;
            for revision in revisions {
                let rows = policy_revision_rows(&mut transaction, &revision).await?;
                if rows > remaining {
                    halted = true;
                    next_batch_minimum_rows = Some(rows);
                    break;
                }
                let deleted = sqlx::query(
                    "DELETE FROM sync_policy_revisions
WHERE policy_id = $1 AND revision = $2
",
                )
                .bind(revision.policy_id)
                .bind(revision.revision)
                .execute(&mut *transaction)
                .await?;
                if deleted.rows_affected() != 1 {
                    return Err(SyncRetentionError::RepositoryInvariant);
                }
                policies.record(rows)?;
                remaining -= rows;
            }
        }

        let deferred = halted
            || eligible_records_exist(
                &mut transaction,
                command.workspace_id,
                command.retained_at,
                audit_cutoff,
            )
            .await?;
        transaction.commit().await?;
        Ok(SyncRetentionReport {
            workspace_id: command.workspace_id,
            classes: [
                unused.result(SyncRetentionClass::UnusedPreview),
                runs.result(SyncRetentionClass::RunBundle),
                policies.result(SyncRetentionClass::UnreferencedPolicy),
            ],
            deferred,
            next_batch_minimum_rows,
        })
    }
}

impl SyncRetentionRepository for PostgresSyncRetentionRepository {
    type Error = PostgresSyncRetentionRepositoryError;

    fn retain_sync_evidence(
        &self,
        command: RetainSyncEvidence,
    ) -> SyncRetentionFuture<'_, SyncRetentionReport, Self::Error> {
        Box::pin(self.retain(command))
    }
}

#[derive(Default)]
struct ClassAccumulator {
    deleted_roots: u32,
    deleted_rows: u32,
}

impl ClassAccumulator {
    fn record(
        &mut self,
        rows: u32,
    ) -> Result<(), SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
        self.deleted_roots = self
            .deleted_roots
            .checked_add(1)
            .ok_or(SyncRetentionError::RepositoryInvariant)?;
        self.deleted_rows = self
            .deleted_rows
            .checked_add(rows)
            .ok_or(SyncRetentionError::RepositoryInvariant)?;
        Ok(())
    }

    fn result(self, class: SyncRetentionClass) -> SyncRetentionClassResult {
        SyncRetentionClassResult {
            class,
            deleted_roots: self.deleted_roots,
            deleted_rows: self.deleted_rows,
        }
    }
}

#[derive(FromRow)]
struct RunCandidate {
    id: Uuid,
    preview_id: Uuid,
}

#[derive(FromRow)]
struct PolicyRevisionCandidate {
    policy_id: Uuid,
    revision: i64,
}

async fn unused_preview_rows(
    transaction: &mut Transaction<'_, Postgres>,
    preview_id: Uuid,
) -> Result<u32, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    let rows = sqlx::query_scalar::<_, i64>(
        "SELECT
    1 + (SELECT COUNT(*) FROM sync_preview_actions WHERE preview_id = $1)
",
    )
    .bind(preview_id)
    .fetch_one(&mut **transaction)
    .await?;
    row_count(rows)
}

async fn run_bundle_rows(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
) -> Result<u32, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    let rows = sqlx::query_scalar::<_, i64>(
        "WITH action_ids AS (
    SELECT id FROM sync_actions WHERE run_id = $1
),

task_ids AS (
    SELECT task_id
    FROM sync_action_tasks
    WHERE action_id IN (SELECT id FROM action_ids)
    UNION
    SELECT task_id
    FROM sync_action_reconciliation_tasks
    WHERE action_id IN (SELECT id FROM action_ids)
),

run_record AS (
    SELECT preview_id FROM sync_runs WHERE id = $1
)

SELECT
    (SELECT COUNT(*) FROM run_record)
    + (SELECT COUNT(*) FROM action_ids)
    + (
        SELECT COUNT(*) FROM sync_action_provenance
        WHERE action_id IN (SELECT id FROM action_ids)
    )
    + (
        SELECT COUNT(*) FROM sync_action_reconciliation_attempts
        WHERE action_id IN (SELECT id FROM action_ids)
    )
    + (
        SELECT COUNT(*) FROM sync_action_reconciliation_tasks
        WHERE action_id IN (SELECT id FROM action_ids)
    )
    + (
        SELECT COUNT(*) FROM sync_action_attempt_field_effects
        WHERE task_attempt_id IN (
            SELECT task_attempt_id FROM sync_action_attempts
            WHERE action_id IN (SELECT id FROM action_ids)
        )
    )
    + (
        SELECT COUNT(*) FROM sync_action_attempt_error_field_problems
        WHERE task_attempt_id IN (
            SELECT task_attempt_id FROM sync_action_attempts
            WHERE action_id IN (SELECT id FROM action_ids)
        )
    )
    + (
        SELECT COUNT(*) FROM sync_action_attempts
        WHERE action_id IN (SELECT id FROM action_ids)
    )
    + (
        SELECT COUNT(*) FROM sync_action_tasks
        WHERE action_id IN (SELECT id FROM action_ids)
    )
    + (
        SELECT COUNT(*) FROM task_claims
        WHERE task_id IN (SELECT task_id FROM task_ids)
    )
    + (
        SELECT COUNT(*) FROM task_attempts
        WHERE task_id IN (SELECT task_id FROM task_ids)
    )
    + (
        SELECT COUNT(*) FROM tasks
        WHERE id IN (SELECT task_id FROM task_ids)
    )
    + (
        SELECT COUNT(*) FROM sync_preview_actions
        WHERE preview_id = (SELECT preview_id FROM run_record)
    )
    + (
        SELECT COUNT(*) FROM sync_previews
        WHERE id = (SELECT preview_id FROM run_record)
    )
",
    )
    .bind(run_id)
    .fetch_one(&mut **transaction)
    .await?;
    row_count(rows)
}

async fn delete_run_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    candidate: &RunCandidate,
) -> Result<u32, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    let task_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT task_id
FROM sync_action_tasks
WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1)
UNION
SELECT task_id
FROM sync_action_reconciliation_tasks
WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1)
ORDER BY task_id
",
    )
    .bind(candidate.id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut deleted = 0_u32;
    for statement in [
        "DELETE FROM sync_action_provenance WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1)",
        "DELETE FROM sync_action_reconciliation_attempts WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1)",
        "DELETE FROM sync_action_reconciliation_tasks WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1)",
        "DELETE FROM sync_action_attempt_field_effects WHERE task_attempt_id IN (SELECT task_attempt_id FROM sync_action_attempts WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1))",
        "DELETE FROM sync_action_attempt_error_field_problems WHERE task_attempt_id IN (SELECT task_attempt_id FROM sync_action_attempts WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1))",
        "DELETE FROM sync_action_attempts WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1)",
        "DELETE FROM sync_action_tasks WHERE action_id IN (SELECT id FROM sync_actions WHERE run_id = $1)",
    ] {
        deleted = add_deleted(
            deleted,
            delete_run_rows(transaction, statement, candidate.id).await?,
        )?;
    }
    for statement in [
        "DELETE FROM task_claims WHERE task_id = ANY($1)",
        "DELETE FROM task_attempts WHERE task_id = ANY($1)",
        "DELETE FROM tasks WHERE id = ANY($1)",
    ] {
        let rows = sqlx::query(statement)
            .bind(&task_ids)
            .execute(&mut **transaction)
            .await?
            .rows_affected();
        deleted = add_deleted(deleted, affected_rows(rows)?)?;
    }
    deleted = add_deleted(
        deleted,
        delete_run_rows(
            transaction,
            "DELETE FROM sync_actions WHERE run_id = $1",
            candidate.id,
        )
        .await?,
    )?;
    deleted = add_deleted(
        deleted,
        delete_run_rows(
            transaction,
            "DELETE FROM sync_runs WHERE id = $1",
            candidate.id,
        )
        .await?,
    )?;
    deleted = add_deleted(
        deleted,
        delete_run_rows(
            transaction,
            "DELETE FROM sync_preview_actions WHERE preview_id = $1",
            candidate.preview_id,
        )
        .await?,
    )?;
    deleted = add_deleted(
        deleted,
        delete_run_rows(
            transaction,
            "DELETE FROM sync_previews WHERE id = $1",
            candidate.preview_id,
        )
        .await?,
    )?;
    Ok(deleted)
}

async fn delete_run_rows(
    transaction: &mut Transaction<'_, Postgres>,
    statement: &'static str,
    id: Uuid,
) -> Result<u32, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    affected_rows(
        sqlx::query(statement)
            .bind(id)
            .execute(&mut **transaction)
            .await?
            .rows_affected(),
    )
}

async fn policy_revision_rows(
    transaction: &mut Transaction<'_, Postgres>,
    revision: &PolicyRevisionCandidate,
) -> Result<u32, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    let rows = sqlx::query_scalar::<_, i64>(
        "SELECT 1 + COUNT(*)
FROM sync_policy_source_roles
WHERE policy_id = $1 AND revision = $2
",
    )
    .bind(revision.policy_id)
    .bind(revision.revision)
    .fetch_one(&mut **transaction)
    .await?;
    row_count(rows)
}

async fn eligible_records_exist(
    transaction: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    retained_at: time::OffsetDateTime,
    audit_cutoff: time::OffsetDateTime,
) -> Result<bool, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT
    EXISTS (
        SELECT 1
        FROM sync_previews AS previews
        WHERE
            previews.workspace_id = $1
            AND previews.expires_at <= $2
            AND NOT EXISTS (
                SELECT 1 FROM sync_runs WHERE preview_id = previews.id
            )
    )
    OR EXISTS (
        SELECT 1
        FROM sync_runs
        WHERE
            workspace_id = $1
            AND finished_at < $3
            AND status NOT IN ('queued', 'running')
            AND NOT EXISTS (
                SELECT 1
                FROM sync_action_tasks AS action_tasks
                INNER JOIN sync_actions AS actions
                    ON actions.id = action_tasks.action_id
                INNER JOIN tasks ON tasks.id = action_tasks.task_id
                WHERE
                    actions.run_id = sync_runs.id
                    AND tasks.status IN ('queued', 'running', 'cancelling')
            )
            AND NOT EXISTS (
                SELECT 1
                FROM sync_action_reconciliation_tasks AS reconciliation_tasks
                INNER JOIN sync_actions AS actions
                    ON actions.id = reconciliation_tasks.action_id
                INNER JOIN tasks ON tasks.id = reconciliation_tasks.task_id
                WHERE
                    actions.run_id = sync_runs.id
                    AND tasks.status IN ('queued', 'running', 'cancelling')
            )
    )
    OR EXISTS (
        SELECT 1
        FROM sync_policy_revisions AS revisions
        INNER JOIN sync_policies AS policies
            ON
                policies.id = revisions.policy_id
                AND policies.workspace_id = revisions.workspace_id
        WHERE
            revisions.workspace_id = $1
            AND revisions.created_at < $3
            AND revisions.revision <> policies.head_revision
            AND NOT EXISTS (
                SELECT 1 FROM sync_previews AS previews
                WHERE
                    previews.policy_id = revisions.policy_id
                    AND previews.policy_revision = revisions.revision
            )
    )
",
    )
    .bind(workspace_id)
    .bind(retained_at)
    .bind(audit_cutoff)
    .fetch_one(&mut **transaction)
    .await?)
}

fn row_count(rows: i64) -> Result<u32, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    u32::try_from(rows).map_err(|_| SyncRetentionError::RepositoryInvariant)
}

fn affected_rows(
    rows: u64,
) -> Result<u32, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    u32::try_from(rows).map_err(|_| SyncRetentionError::RepositoryInvariant)
}

fn add_deleted(
    deleted: u32,
    rows: u32,
) -> Result<u32, SyncRetentionError<PostgresSyncRetentionRepositoryError>> {
    deleted
        .checked_add(rows)
        .ok_or(SyncRetentionError::RepositoryInvariant)
}

#[derive(Debug, Error)]
pub enum PostgresSyncRetentionRepositoryError {
    #[error("sync retention database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl From<sqlx::Error> for SyncRetentionError<PostgresSyncRetentionRepositoryError> {
    fn from(error: sqlx::Error) -> Self {
        Self::Repository(PostgresSyncRetentionRepositoryError::Database(error))
    }
}
