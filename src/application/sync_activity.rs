use std::{collections::BTreeSet, future::Future, pin::Pin};

use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    error::{ApplicationError, IntoApplicationError},
    sync_action_reconciliation::SyncActionReconciliationResult,
    sync_provenance::SyncActionProvenanceSnapshot,
    task_manager::TaskStatus,
};

pub const MAX_SYNC_ACTIVITY_PAGE_SIZE: u32 = 100;

pub type SyncActivityFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;
pub type SyncActivityApiFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ApplicationError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SyncActivityRunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Partial,
    Indeterminate,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncActivityActionStatus {
    Queued,
    Running,
    Applied,
    NoOp,
    PreconditionFailed,
    RetryableFailure,
    Failed,
    Indeterminate,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySyncActivity {
    pub workspace_id: Uuid,
    pub statuses: Vec<SyncActivityRunStatus>,
    pub receiver_source_id: Option<Uuid>,
    pub semantic_field: Option<String>,
    pub occurred_at_or_after: Option<OffsetDateTime>,
    pub occurred_before: Option<OffsetDateTime>,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActivityTaskSummary {
    pub task_id: Uuid,
    pub status: TaskStatus,
    pub created_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActivityActionSummary {
    pub action_id: Uuid,
    pub run_id: Uuid,
    pub workspace_id: Uuid,
    pub action_hash: String,
    pub receiver_source_id: Uuid,
    pub semantic_field: String,
    pub status: SyncActivityActionStatus,
    pub write_task: SyncActivityTaskSummary,
    pub reconciliation_task: Option<SyncActivityTaskSummary>,
    pub created_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActivityRunSummary {
    pub run_id: Uuid,
    pub workspace_id: Uuid,
    pub preview_id: Uuid,
    pub status: SyncActivityRunStatus,
    pub actions: Vec<SyncActivityActionSummary>,
    pub created_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncActivityPage {
    pub runs: Vec<SyncActivityRunSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncActivityReconciliationGuidance {
    NotRequired,
    Required,
    ConfirmedApplied,
    ConfirmedNotApplied,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncActivityActionRecord {
    pub summary: SyncActivityActionSummary,
    pub provenance: Option<SyncActionProvenanceSnapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncActivityActionDetail {
    pub summary: SyncActivityActionSummary,
    pub provenance: Option<SyncActionProvenanceSnapshot>,
    pub reconciliation_guidance: SyncActivityReconciliationGuidance,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncActivityError<E> {
    InvalidQuery,
    NotFound,
    RepositoryInvariant,
    Repository(E),
}

pub trait SyncActivityRepository: Send + Sync {
    type Error;

    fn query_activity(
        &self,
        query: QuerySyncActivity,
    ) -> SyncActivityFuture<'_, SyncActivityPage, Self::Error>;

    fn get_action_record(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> SyncActivityFuture<'_, Option<SyncActivityActionRecord>, Self::Error>;
}

pub trait SyncActivityApi: Send + Sync {
    fn list_activity(
        &self,
        query: QuerySyncActivity,
    ) -> SyncActivityApiFuture<'_, SyncActivityPage>;

    fn get_activity_action(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> SyncActivityApiFuture<'_, SyncActivityActionDetail>;
}

#[derive(Clone)]
pub struct SyncActivityService<R> {
    repository: R,
}

impl<R> SyncActivityService<R>
where
    R: SyncActivityRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn list(
        &self,
        mut query: QuerySyncActivity,
    ) -> Result<SyncActivityPage, SyncActivityError<R::Error>> {
        validate_query(&mut query)?;
        let expected = query.clone();
        let page = self
            .repository
            .query_activity(query)
            .await
            .map_err(SyncActivityError::Repository)?;
        validate_page(&page, &expected)?;
        Ok(page)
    }

    pub async fn get_action(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> Result<SyncActivityActionDetail, SyncActivityError<R::Error>> {
        if workspace_id.is_nil() || action_id.is_nil() {
            return Err(SyncActivityError::InvalidQuery);
        }
        let record = self
            .repository
            .get_action_record(workspace_id, action_id)
            .await
            .map_err(SyncActivityError::Repository)?
            .ok_or(SyncActivityError::NotFound)?;
        validate_action(&record.summary, workspace_id, None)?;
        if record.summary.action_id != action_id
            || record.provenance.as_ref().is_some_and(|provenance| {
                provenance.workspace_id != workspace_id || provenance.action_id != action_id
            })
            || record
                .provenance
                .as_ref()
                .and_then(|provenance| provenance.reconciliation_edge())
                .is_some()
                && record.summary.reconciliation_task.is_none()
        {
            return Err(SyncActivityError::RepositoryInvariant);
        }
        let reconciliation_guidance = reconciliation_guidance(&record);
        Ok(SyncActivityActionDetail {
            summary: record.summary,
            provenance: record.provenance,
            reconciliation_guidance,
        })
    }
}

impl<R> SyncActivityApi for SyncActivityService<R>
where
    R: SyncActivityRepository,
    SyncActivityError<R::Error>: IntoApplicationError,
{
    fn list_activity(
        &self,
        query: QuerySyncActivity,
    ) -> SyncActivityApiFuture<'_, SyncActivityPage> {
        Box::pin(async move {
            self.list(query)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }

    fn get_activity_action(
        &self,
        workspace_id: Uuid,
        action_id: Uuid,
    ) -> SyncActivityApiFuture<'_, SyncActivityActionDetail> {
        Box::pin(async move {
            self.get_action(workspace_id, action_id)
                .await
                .map_err(IntoApplicationError::into_application_error)
        })
    }
}

fn validate_query<E>(query: &mut QuerySyncActivity) -> Result<(), SyncActivityError<E>> {
    if query.workspace_id.is_nil()
        || query.limit == 0
        || query.limit > MAX_SYNC_ACTIVITY_PAGE_SIZE
        || query
            .receiver_source_id
            .is_some_and(|source_id| source_id.is_nil())
        || query
            .semantic_field
            .as_ref()
            .is_some_and(|field| field.is_empty())
        || matches!(
            (query.occurred_at_or_after, query.occurred_before),
            (Some(start), Some(end)) if start >= end
        )
        || query.cursor.as_ref().is_some_and(String::is_empty)
    {
        return Err(SyncActivityError::InvalidQuery);
    }
    query.statuses.sort_unstable();
    query.statuses.dedup();
    Ok(())
}

fn validate_page<E>(
    page: &SyncActivityPage,
    query: &QuerySyncActivity,
) -> Result<(), SyncActivityError<E>> {
    if page.runs.len() > query.limit as usize
        || (page.runs.len() < query.limit as usize && page.next_cursor.is_some())
        || page.next_cursor.as_ref().is_some_and(String::is_empty)
    {
        return Err(SyncActivityError::RepositoryInvariant);
    }
    let mut run_ids = BTreeSet::new();
    for (index, run) in page.runs.iter().enumerate() {
        if !run_ids.insert(run.run_id)
            || run.workspace_id != query.workspace_id
            || run.actions.is_empty()
            || (!query.statuses.is_empty() && !query.statuses.contains(&run.status))
            || query
                .occurred_at_or_after
                .is_some_and(|start| run.created_at < start)
            || query
                .occurred_before
                .is_some_and(|end| run.created_at >= end)
            || !run.actions.iter().any(|action| {
                query
                    .receiver_source_id
                    .is_none_or(|source_id| action.receiver_source_id == source_id)
                    && query
                        .semantic_field
                        .as_ref()
                        .is_none_or(|field| action.semantic_field == *field)
            })
            || index > 0 && !run_precedes(&page.runs[index - 1], run)
        {
            return Err(SyncActivityError::RepositoryInvariant);
        }
        for action in &run.actions {
            validate_action(action, query.workspace_id, Some(run.run_id))?;
        }
    }
    Ok(())
}

fn validate_action<E>(
    action: &SyncActivityActionSummary,
    workspace_id: Uuid,
    run_id: Option<Uuid>,
) -> Result<(), SyncActivityError<E>> {
    if action.action_id.is_nil()
        || action.run_id.is_nil()
        || action.workspace_id != workspace_id
        || run_id.is_some_and(|run_id| action.run_id != run_id)
        || action.action_hash.is_empty()
        || action.receiver_source_id.is_nil()
        || action.semantic_field.is_empty()
        || action.write_task.task_id.is_nil()
        || action
            .reconciliation_task
            .as_ref()
            .is_some_and(|task| task.task_id.is_nil())
    {
        return Err(SyncActivityError::RepositoryInvariant);
    }
    Ok(())
}

fn run_precedes(left: &SyncActivityRunSummary, right: &SyncActivityRunSummary) -> bool {
    left.created_at > right.created_at
        || (left.created_at == right.created_at && left.run_id < right.run_id)
}

fn reconciliation_guidance(
    record: &SyncActivityActionRecord,
) -> SyncActivityReconciliationGuidance {
    match record
        .provenance
        .as_ref()
        .and_then(|provenance| provenance.reconciliation_edge())
        .map(|reconciliation| reconciliation.result)
    {
        Some(SyncActionReconciliationResult::ConfirmedApplied) => {
            SyncActivityReconciliationGuidance::ConfirmedApplied
        }
        Some(SyncActionReconciliationResult::ConfirmedNotApplied) => {
            SyncActivityReconciliationGuidance::ConfirmedNotApplied
        }
        Some(
            SyncActionReconciliationResult::StateMatchesCausationUnknown
            | SyncActionReconciliationResult::Unresolved,
        )
        | None
            if record.summary.status == SyncActivityActionStatus::Indeterminate =>
        {
            SyncActivityReconciliationGuidance::Required
        }
        _ => SyncActivityReconciliationGuidance::NotRequired,
    }
}
