mod index;
mod records;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use serde::Serialize;
use sqlx::{FromRow, PgConnection, PgPool, Postgres, Transaction};
use tokio::sync::{Mutex, MutexGuard};
use trakkin_mappings_corpus::{
    git::{Repository, RepositoryStore, WriteError},
    snapshots::SnapshotSet,
};
use trakkin_mappings_language::{
    Record, ResolvedSelection, Resolver, Selection, SemanticError, SemanticErrorKind, evaluate,
    sha256,
};

use self::{
    index::{MappingIndex, compile},
    records::{StoredRecord, import_records, read_record, save_record},
};
use crate::application::{
    error::{ApplicationError, ApplicationErrorCode},
    mappings::*,
};

const MAXIMUM_BATCH: usize = 1000;
pub(super) const MAXIMUM_RECORDS: usize = 100_000;
const MAXIMUM_CONTENT_BYTES: usize = 2 * 1024 * 1024;
const WRITE_LOCK: i64 = 0x7472616b6d617073;

pub struct MappingStore {
    pub(super) pool: PgPool,
    pub(super) repositories: RepositoryStore,
    local: Repository,
    resolver: MappingResolver,
    index: Mutex<MappingIndex>,
}

struct MappingResolver {
    snapshots: Option<SnapshotSet>,
}

impl Resolver for MappingResolver {
    fn resolve(&self, selection: &Selection) -> Result<ResolvedSelection, SemanticError> {
        if selection.selector.is_none() {
            let key = trakkin_provider_sdk::mapping::MappingUnitKey::parse(&selection.reference)
                .map_err(|error| SemanticError::invalid(error.to_string()))?;
            return Ok(ResolvedSelection {
                units: vec![key.as_str().to_owned()],
                ordered: true,
                coordinates: None,
            });
        }
        self.snapshots
            .as_ref()
            .ok_or_else(|| SemanticError::unresolved("selector requires native catalog metadata"))?
            .resolve(selection)
    }
}

#[derive(FromRow)]
struct SourceRow {
    id: String,
    display_name: String,
    kind: String,
    enabled: bool,
    url: Option<String>,
    branch: Option<String>,
    poll_interval_seconds: Option<i64>,
    revision: Option<String>,
    last_checked_at: Option<time::OffsetDateTime>,
    last_updated_at: Option<time::OffsetDateTime>,
    next_check_at: Option<time::OffsetDateTime>,
    consecutive_failures: i32,
    last_error: Option<String>,
}

#[derive(FromRow, Serialize)]
struct SourceConfiguration {
    id: String,
    display_name: String,
    url: Option<String>,
    branch: Option<String>,
    revision: Option<String>,
    poll_interval_seconds: Option<i64>,
    enabled: bool,
    generation: i64,
}

impl MappingStore {
    pub async fn sources(&self) -> Result<MappingSourcesSnapshot, ApplicationError> {
        let index = self.index.lock().await;
        let rows = sqlx::query_as::<_, SourceRow>(
            "SELECT
    source.id,
    source.display_name,
    source.kind,
    source.enabled,
    source.url,
    source.branch,
    source.poll_interval_seconds,
    source.revision,
    source.last_checked_at,
    source.last_updated_at,
    source.next_check_at,
    source.consecutive_failures,
    source.last_error
FROM mapping_sources AS source
ORDER BY source.kind, source.display_name, source.id
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let sources = rows
            .into_iter()
            .map(|row| {
                let cached_records = index
                    .records
                    .values()
                    .filter(|record| record.origins.contains_key(&row.id))
                    .count() as u64;
                MappingSource {
                    id: row.id,
                    display_name: row.display_name,
                    kind: if row.kind == "local" {
                        MappingSourceKind::Local
                    } else {
                        MappingSourceKind::Upstream
                    },
                    enabled: row.enabled,
                    url: row.url,
                    branch: row.branch,
                    repository_path: (row.kind == "local")
                        .then(|| self.local.path().display().to_string()),
                    poll_interval_seconds: row
                        .poll_interval_seconds
                        .map(|interval| interval as u64),
                    cached_records,
                    revision: row.revision,
                    last_checked_at: row.last_checked_at,
                    last_updated_at: row.last_updated_at,
                    next_check_at: row.next_check_at,
                    consecutive_failures: row.consecutive_failures as u32,
                    last_error: row.last_error,
                }
            })
            .collect();
        Ok(MappingSourcesSnapshot {
            fingerprint: index.fingerprint.clone(),
            sources,
        })
    }

    pub async fn save_source(
        &self,
        command: SaveMappingSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let name = command.display_name.trim();
        if name.is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
            return Err(invalid_request(
                "source name must contain 1 to 128 bytes without control characters",
            ));
        }
        let url = self
            .repositories
            .validate_upstream(&command.url, &command.branch)
            .await
            .map_err(|error| invalid_request(&error.to_string()))?;
        if !(60..=604800).contains(&command.poll_interval_seconds) {
            return Err(invalid_request(
                "polling interval must be between 60 seconds and 7 days",
            ));
        }
        let (index, mut transaction) = self
            .begin_write(Some(&command.expected_fingerprint))
            .await?;
        if let Some(id) = &command.id {
            require_upstream(&mut transaction, id).await?;
            sqlx::query(
                "UPDATE mapping_sources
SET
    display_name = $2,
    url = $3,
    branch = $6,
    poll_interval_seconds = $4,
    enabled = $5,
    generation = generation + 1,
    consecutive_failures = 0,
    last_error = NULL,
    next_check_at = NOW()
WHERE id = $1
",
            )
            .bind(id)
            .bind(name)
            .bind(&url)
            .bind(command.poll_interval_seconds as i64)
            .bind(command.enabled)
            .bind(&command.branch)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        } else {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM mapping_sources WHERE kind = 'upstream'\n",
            )
            .fetch_one(&mut *transaction)
            .await
            .map_err(database_error)?;
            if count >= 32 {
                return Err(invalid_request(
                    "at most 32 upstream mapping sources are supported",
                ));
            }
            let mut bytes = [0_u8; 16];
            getrandom::fill(&mut bytes)
                .map_err(|_| ApplicationError::internal("mapping source ID generation failed"))?;
            bytes[6] = (bytes[6] & 0x0f) | 0x40;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            let id = uuid::Uuid::from_bytes(bytes).to_string();
            sqlx::query(
                "INSERT INTO mapping_sources\n(\n    id,\n    display_name,\n    kind,\n    url,\n    poll_interval_seconds,\n    enabled,\n    branch,\n    next_check_at\n)\nVALUES ($1, $2, 'upstream', $3, $4, $5, $6, NOW())\n",
            )
            .bind(id)
            .bind(name)
            .bind(&url)
            .bind(command.poll_interval_seconds as i64)
            .bind(command.enabled)
            .bind(&command.branch)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        }
        let records = self.committed_records(&mut transaction).await?;
        self.commit_write(index, transaction, records).await
    }

    pub async fn delete_source(
        &self,
        command: ChangeMappingSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let (index, mut transaction) = self
            .begin_write(Some(&command.expected_fingerprint))
            .await?;
        require_upstream(&mut transaction, &command.id).await?;
        sqlx::query("DELETE FROM mapping_sources WHERE id = $1\n")
            .bind(&command.id)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        let records = self.committed_records(&mut transaction).await?;
        let state = self.commit_write(index, transaction, records).await?;
        if let Err(error) = self.repositories.remove_upstream(&command.id) {
            tracing::warn!(source = %command.id, error = %error, "mapping cache cleanup deferred until the active fetch completes");
        }
        Ok(state)
    }

    pub async fn refresh_source(
        &self,
        command: ChangeMappingSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let (index, mut transaction) = self
            .begin_write((command.id != "local").then_some(command.expected_fingerprint.as_str()))
            .await?;
        if command.id == "local" {
            let records = index.records.clone();
            return self.commit_write(index, transaction, records).await;
        }
        require_upstream(&mut transaction, &command.id).await?;
        let enabled: bool =
            sqlx::query_scalar("SELECT enabled FROM mapping_sources WHERE id = $1\n")
                .bind(&command.id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(database_error)?;
        if !enabled {
            return Err(invalid_request("enable the source before refreshing it"));
        }
        sqlx::query(
            "UPDATE mapping_sources
SET
    next_check_at = NOW(),
    generation = generation + 1,
    consecutive_failures = 0,
    last_error = NULL
WHERE id = $1
",
        )
        .bind(&command.id)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        let records = self.committed_records(&mut transaction).await?;
        self.commit_write(index, transaction, records).await
    }

    pub async fn revert(
        &self,
        command: DeleteMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.update(Some(&command.expected_fingerprint), move |records| {
            let stored = records
                .get_mut(&command.claim_id)
                .ok_or_else(|| invalid_request("mapping claim does not exist"))?;
            if stored.origins.remove("local").is_none() {
                return Err(invalid_request("mapping has no local decision"));
            }
            records.retain(|_, stored| !stored.origins.is_empty());
            Ok(())
        })
        .await
    }

    pub async fn open(
        pool: PgPool,
        directory: &Path,
        snapshots: Option<SnapshotSet>,
    ) -> Result<Self, ApplicationError> {
        let resolver = MappingResolver { snapshots };
        let repositories = RepositoryStore::open(directory).map_err(repository_error)?;
        let local = repositories.local().await.map_err(repository_error)?;
        let index = MappingIndex::build(BTreeMap::new(), &resolver, &BTreeSet::new())?;
        let store = Self {
            pool,
            repositories,
            local,
            resolver,
            index: Mutex::new(index),
        };
        let (index, transaction) = store.begin_write(None).await?;
        let records = index.records.clone();
        store.commit_write(index, transaction, records).await?;
        Ok(store)
    }

    pub async fn preview(&self, text: &str) -> Result<MappingResult, ApplicationError> {
        let record = read_record(text)?;
        Ok(compile(&record, false, vec!["local".into()], &self.resolver).row)
    }

    pub async fn save(
        &self,
        command: SaveMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let record = read_record(&command.record)?;
        validate_evaluation(&record, &self.resolver)?;
        self.update(Some(&command.expected_fingerprint), move |records| {
            save_record(records, record, command.replaces.as_deref())
        })
        .await
    }

    pub async fn decide(
        &self,
        command: DecideMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.update(Some(&command.expected_fingerprint), move |records| {
            let record = records
                .get_mut(&command.claim_id)
                .ok_or_else(|| invalid_request("mapping claim does not exist"))?;
            record.decide(command.rejected)
        })
        .await
    }

    pub async fn delete(
        &self,
        command: DeleteMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.update(Some(&command.expected_fingerprint), move |records| {
            let record = records
                .get(&command.claim_id)
                .ok_or_else(|| invalid_request("mapping claim does not exist"))?;
            if record.origins.keys().any(|source| source != "local") {
                return Err(invalid_request(
                    "imported mappings must be suppressed rather than deleted",
                ));
            }
            records
                .remove(&command.claim_id)
                .ok_or_else(|| invalid_request("mapping claim does not exist"))?;
            Ok(())
        })
        .await
    }

    pub(super) async fn complete_check(
        &self,
        source: &super::mapping_updates::DueSource,
        response: super::mapping_updates::CheckResult,
        now: time::OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        use super::mapping_updates::{CheckResult, retry_delay};
        let (index, mut transaction) = self.begin_write(None).await?;
        let current: bool = sqlx::query_scalar(
            "SELECT EXISTS(\n    SELECT 1\n    FROM mapping_sources\n    WHERE id = $1 AND generation = $2 AND enabled AND kind = 'upstream'\n)\n",
        )
        .bind(&source.id)
        .bind(source.generation)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        if !current {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mapping_sources WHERE id = $1)\n")
                    .bind(&source.id)
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(database_error)?;
            if !exists {
                self.repositories
                    .remove_upstream(&source.id)
                    .map_err(repository_error)?;
            }
            return Ok(());
        }
        let mut records = index.records.clone();
        let response = match response {
            CheckResult::Updated(revision) => {
                let validation = (|| {
                    let decisions = revision
                        .corpus
                        .decisions()
                        .map_err(|error| invalid_request(&error.to_string()))?;
                    let mut accepted = Vec::new();
                    let mut rejected = Vec::new();
                    for (_, decision) in decisions {
                        if decision.rejected {
                            rejected.push(decision.record);
                        } else {
                            accepted.push(decision.record);
                        }
                    }
                    for record in &accepted {
                        validate_evaluation(record, &self.resolver)?;
                    }
                    import_records(&mut records, &source.id, accepted, rejected)?;
                    if records.len() > MAXIMUM_RECORDS {
                        return Err(invalid_request(
                            "mapping store is limited to 100000 records",
                        ));
                    }
                    MappingIndex::build(records.clone(), &self.resolver, &BTreeSet::new())?;
                    Ok::<_, ApplicationError>(())
                })();
                match validation {
                    Ok(()) => CheckResult::Updated(revision),
                    Err(error) => CheckResult::Failed(error.to_string()),
                }
            }
            response => response,
        };
        let (revision, updated, failures, error) = match response {
            CheckResult::Updated(revision) => {
                self.repositories
                    .upstream(&source.id)
                    .map_err(repository_error)?
                    .pin(&revision.commit)
                    .await
                    .map_err(repository_error)?;
                (Some(revision.commit), true, 0, None)
            }
            CheckResult::Unchanged => (source.revision.clone(), false, 0, None),
            CheckResult::Failed(error) => {
                let error: String = error.chars().take(2048).collect();
                tracing::warn!(source = %source.id, error, "mapping Git fetch failed; keeping last valid mappings");
                (
                    source.revision.clone(),
                    false,
                    source.consecutive_failures.saturating_add(1),
                    Some(error),
                )
            }
        };
        let next =
            now + time::Duration::seconds(retry_delay(source.poll_interval_seconds, failures));
        sqlx::query(
            "UPDATE mapping_sources
SET
    revision = $2,
    last_checked_at = $3,
    last_updated_at = CASE WHEN $4 THEN $3 ELSE last_updated_at END,
    next_check_at = $5,
    consecutive_failures = $6,
    last_error = $7
WHERE id = $1
",
        )
        .bind(&source.id)
        .bind(revision)
        .bind(now)
        .bind(updated)
        .bind(next)
        .bind(failures)
        .bind(error)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        if updated {
            self.commit_write(index, transaction, records).await?;
            tracing::info!(source = %source.id, "mapping Git revision applied");
        } else {
            transaction.commit().await.map_err(database_error)?;
        }
        Ok(())
    }

    async fn committed_records(
        &self,
        connection: &mut PgConnection,
    ) -> Result<BTreeMap<String, StoredRecord>, ApplicationError> {
        let sources = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT id, revision FROM mapping_sources ORDER BY id\n",
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(database_error)?;
        let mut records = BTreeMap::new();
        for (id, commit) in sources {
            let revision = if id == "local" {
                self.local.current().await.map_err(repository_error)?
            } else if let Some(commit) = commit {
                self.repositories
                    .upstream(&id)
                    .map_err(repository_error)?
                    .read(&commit)
                    .await
                    .map_err(repository_error)?
            } else {
                continue;
            };
            let mut accepted = Vec::new();
            let mut rejected = Vec::new();
            for (_, decision) in revision.corpus.decisions().map_err(repository_error)? {
                if decision.rejected {
                    rejected.push(decision.record);
                } else {
                    validate_evaluation(&decision.record, &self.resolver)?;
                    accepted.push(decision.record);
                }
            }
            import_records(&mut records, &id, accepted, rejected)?;
            if id == "local" {
                sqlx::query("UPDATE mapping_sources SET\n    last_updated_at\n    = CASE\n        WHEN revision IS DISTINCT FROM $1 THEN NOW() ELSE last_updated_at\n    END,\n    revision = $1\nWHERE id = 'local'\n")
                    .bind(&revision.commit).execute(&mut *connection).await.map_err(database_error)?;
            }
        }
        Ok(records)
    }

    async fn update(
        &self,
        expected: Option<&str>,
        operation: impl FnOnce(&mut BTreeMap<String, StoredRecord>) -> Result<(), ApplicationError>
        + Send,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let (index, mut transaction) = self.begin_write(expected).await?;
        let mut records = index.records.clone();
        operation(&mut records)?;
        let decisions = records::local_decisions(&mut records)?;
        if records.len() > MAXIMUM_RECORDS {
            return Err(invalid_request(
                "mapping store is limited to 100000 records",
            ));
        }
        build_index(records.clone(), &mut transaction, &self.resolver).await?;
        let expected: String =
            sqlx::query_scalar("SELECT revision FROM mapping_sources WHERE id = 'local'\n")
                .fetch_one(&mut *transaction)
                .await
                .map_err(database_error)?;
        let revision = self
            .local
            .commit(&expected, decisions, "Update Local mapping decisions")
            .await
            .map_err(|error| match error {
                WriteError::RevisionChanged => ApplicationError::new(
                    ApplicationErrorCode::MappingsChanged,
                    "Local repository changed; reload mappings before continuing",
                ),
                WriteError::DirtyWorktree => invalid_request(&error.to_string()),
                _ => ApplicationError::internal(error.to_string()),
            })?;
        sqlx::query(
            "UPDATE mapping_sources SET revision = $1, last_updated_at = NOW()\nWHERE id = 'local'\n",
        )
        .bind(&revision.commit)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        self.commit_write(index, transaction, records).await
    }

    async fn begin_write(
        &self,
        expected: Option<&str>,
    ) -> Result<(MutexGuard<'_, MappingIndex>, Transaction<'_, Postgres>), ApplicationError> {
        let mut index = self.index.lock().await;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)\n")
            .bind(WRITE_LOCK)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        let current = build_index(
            self.committed_records(&mut transaction).await?,
            &mut transaction,
            &self.resolver,
        )
        .await?;
        *index = current;
        if let Err(error) = index.check_fingerprint(expected) {
            let records = index.records.clone();
            self.commit_write(index, transaction, records).await?;
            return Err(error);
        }
        Ok((index, transaction))
    }

    async fn commit_write(
        &self,
        mut index: MutexGuard<'_, MappingIndex>,
        mut transaction: Transaction<'_, Postgres>,
        records: BTreeMap<String, StoredRecord>,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        if records.len() > MAXIMUM_RECORDS {
            return Err(invalid_request(
                "mapping store is limited to 100000 records",
            ));
        }
        let replacement = build_index(records, &mut transaction, &self.resolver).await?;
        transaction.commit().await.map_err(database_error)?;
        *index = replacement;
        Ok(index.state())
    }
}

async fn build_index(
    records: BTreeMap<String, StoredRecord>,
    connection: &mut PgConnection,
    resolver: &MappingResolver,
) -> Result<MappingIndex, ApplicationError> {
    let configuration = sqlx::query_as::<_, SourceConfiguration>(
        "SELECT\n    id,\n    display_name,\n    url,\n    branch,\n    revision,\n    poll_interval_seconds,\n    enabled,\n    generation
FROM mapping_sources
ORDER BY id
",
    )
    .fetch_all(connection)
    .await
    .map_err(database_error)?;
    let disabled = configuration
        .iter()
        .filter(|source| !source.enabled)
        .map(|source| source.id.clone())
        .collect();
    let mut index = MappingIndex::build(records, resolver, &disabled)?;
    index.fingerprint = sha256(
        &serde_json::to_vec(&(&index.fingerprint, configuration))
            .map_err(|error| ApplicationError::internal(error.to_string()))?,
    );
    Ok(index)
}

fn repository_error(error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::internal(format!("mapping Git repository failed: {error}"))
}

async fn require_upstream(connection: &mut PgConnection, id: &str) -> Result<(), ApplicationError> {
    let kind: Option<String> =
        sqlx::query_scalar("SELECT kind FROM mapping_sources WHERE id = $1\n")
            .bind(id)
            .fetch_optional(connection)
            .await
            .map_err(database_error)?;
    match kind.as_deref() {
        Some("upstream") => Ok(()),
        Some(_) => Err(invalid_request(
            "the Local source cannot be configured or removed",
        )),
        None => Err(invalid_request("mapping source does not exist")),
    }
}

fn validate_evaluation(
    record: &Record,
    resolver: &MappingResolver,
) -> Result<(), ApplicationError> {
    if let Err(error) = evaluate(&record.statement, resolver)
        && error.kind == SemanticErrorKind::Invalid
    {
        return Err(invalid_request(&error.message));
    }
    Ok(())
}

fn invalid_request(message: &str) -> ApplicationError {
    ApplicationError::new(ApplicationErrorCode::InvalidRequest, message)
}

fn database_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::internal(format!("mapping storage failed: {error}"))
}

impl MappingApi for MappingStore {
    fn sources(&self) -> MappingFuture<'_, MappingSourcesSnapshot> {
        Box::pin(MappingStore::sources(self))
    }
    fn save_source(&self, command: SaveMappingSource) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(MappingStore::save_source(self, command))
    }
    fn delete_source(
        &self,
        command: ChangeMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(MappingStore::delete_source(self, command))
    }
    fn refresh_source(
        &self,
        command: ChangeMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(MappingStore::refresh_source(self, command))
    }
    fn revert(&self, command: DeleteMapping) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(MappingStore::revert(self, command))
    }

    fn state(&self) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(async { Ok(self.index.lock().await.state()) })
    }
    fn query(&self, query: QueryMappings) -> MappingFuture<'_, MappingPageSnapshot> {
        Box::pin(async move { self.index.lock().await.query(query) })
    }
    fn preview(&self, record: String) -> MappingFuture<'_, MappingResult> {
        Box::pin(async move { MappingStore::preview(self, &record).await })
    }
    fn save(&self, command: SaveMapping) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(async move { MappingStore::save(self, command).await })
    }
    fn decide(&self, command: DecideMapping) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(async move { MappingStore::decide(self, command).await })
    }
    fn delete(&self, command: DeleteMapping) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(async move { MappingStore::delete(self, command).await })
    }
    fn resolve_units(
        &self,
        command: ResolveMappingUnits,
    ) -> MappingFuture<'_, MappingUnitResolutionBatchSnapshot> {
        Box::pin(async move { self.index.lock().await.resolve(command) })
    }
}

#[cfg(test)]
mod tests;
