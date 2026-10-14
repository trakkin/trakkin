mod index;
mod records;
mod updates;

pub use updates::MappingUpdates;

use self::{
    index::{MappingIndex, compile},
    records::{OriginRecord, StoredRecord, read_record, save_record},
};
use crate::application::{
    error::{ApplicationError, ApplicationErrorCode},
    mappings::*,
};
use serde::Serialize;
use sqlx::{Acquire, FromRow, PgConnection, PgPool, Postgres, Transaction};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use tokio::sync::{Mutex, MutexGuard};
use trakkin_mappings_corpus::{
    Artifact, Bindings, ConflictPolicy, POLICY, claim_path,
    git::{Repository, RepositoryStore, WriteError},
};
use trakkin_mappings_language::{Record, SemanticErrorKind, evaluate, sha256};

const MAXIMUM_BATCH: usize = 1000;
const MAXIMUM_CONTENT_BYTES: usize = 2 * 1024 * 1024;
const WRITE_LOCK: i64 = 0x7472616b6d617073;

pub struct MappingStore {
    pool: PgPool,
    repositories: RepositoryStore,
    local: Repository,
    index: Mutex<MappingIndex>,
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
    cached_records: i64,
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
    conflict_policy: serde_json::Value,
}

impl MappingStore {
    pub async fn open(pool: PgPool, directory: &Path) -> Result<Self, ApplicationError> {
        let repositories = RepositoryStore::open(directory).map_err(repository_error)?;
        let local = repositories.local().await.map_err(repository_error)?;
        let index = MappingIndex::build(BTreeMap::new(), &BTreeSet::new())?;
        let store = Self {
            pool,
            repositories,
            local,
            index: Mutex::new(index),
        };
        let (index, mut transaction) = store.begin_write(None).await?;
        let head = store.local.head().await.map_err(repository_error)?;
        store
            .import_revision(&mut transaction, "local", &store.local, &head)
            .await?;
        store.commit_write(index, transaction).await?;
        Ok(store)
    }

    pub async fn sources(&self) -> Result<MappingSourcesSnapshot, ApplicationError> {
        let (index, mut transaction) = self.begin_read().await?;
        let rows = sqlx::query_as::<_, SourceRow>(
            "SELECT
    s.id,
    s.display_name,
    s.kind,
    s.enabled,
    s.url,
    s.branch,
    s.poll_interval_seconds,
    s.revision,
    s.last_checked_at,
    s.last_updated_at,
    s.next_check_at,
    s.consecutive_failures,
    s.last_error,
    (SELECT count(*) FROM mapping_origins o WHERE o.source_id = s.id
    ) AS cached_records
FROM mapping_sources s
ORDER BY s.kind, s.display_name, s.id
",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        Ok(MappingSourcesSnapshot {
            fingerprint: index.fingerprint.clone(),
            sources: rows
                .into_iter()
                .map(|row| MappingSource {
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
                    poll_interval_seconds: row
                        .poll_interval_seconds
                        .map(|interval| interval as u64),
                    repository_path: (row.kind == "local")
                        .then(|| self.local.path().display().to_string()),
                    cached_records: row.cached_records as u64,
                    revision: row.revision,
                    last_checked_at: row.last_checked_at,
                    last_updated_at: row.last_updated_at,
                    next_check_at: row.next_check_at,
                    consecutive_failures: row.consecutive_failures as u32,
                    last_error: row.last_error,
                })
                .collect(),
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
        if !(60..=604800).contains(&command.poll_interval_seconds) {
            return Err(invalid_request(
                "polling interval must be between 60 seconds and 7 days",
            ));
        }
        let url = self
            .repositories
            .validate_upstream(&command.url, &command.branch)
            .await
            .map_err(|error| invalid_request(&error.to_string()))?;
        let (index, mut transaction) = self
            .begin_write(Some(&command.expected_fingerprint))
            .await?;
        if let Some(id) = &command.id {
            require_upstream(&mut transaction, id).await?;
            sqlx::query(
                "UPDATE mapping_sources SET
    display_name = $2,
    url = $3,
    branch = $4,
    poll_interval_seconds = $5,
    enabled = $6,
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
            .bind(&command.branch)
            .bind(command.poll_interval_seconds as i64)
            .bind(command.enabled)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            invalidate_source(&mut transaction, id).await?;
        } else {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM mapping_sources WHERE kind = 'upstream'
",
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
                "INSERT INTO mapping_sources (
    id,
    display_name,
    kind,
    url,
    branch,
    poll_interval_seconds,
    enabled,
    next_check_at
) VALUES ($1, $2, 'upstream', $3, $4, $5, $6, NOW())
",
            )
            .bind(id)
            .bind(name)
            .bind(url)
            .bind(command.branch)
            .bind(command.poll_interval_seconds as i64)
            .bind(command.enabled)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        }
        self.commit_write(index, transaction).await
    }

    pub async fn delete_source(
        &self,
        command: ChangeMappingSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let (index, mut transaction) = self
            .begin_write(Some(&command.expected_fingerprint))
            .await?;
        require_upstream(&mut transaction, &command.id).await?;
        invalidate_source(&mut transaction, &command.id).await?;
        sqlx::query(
            "DELETE FROM mapping_sources WHERE id = $1
",
        )
        .bind(&command.id)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        let state = self.commit_write(index, transaction).await?;
        if let Err(error) = self.repositories.remove_upstream(&command.id) {
            tracing::warn!(source=%command.id,error=%error,"mapping cache cleanup deferred");
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
            let head = self.local.head().await.map_err(repository_error)?;
            self.import_revision(&mut transaction, "local", &self.local, &head)
                .await?;
        } else {
            require_upstream(&mut transaction, &command.id).await?;
            let enabled: bool = sqlx::query_scalar(
                "SELECT enabled FROM mapping_sources WHERE id = $1
",
            )
            .bind(&command.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(database_error)?;
            if !enabled {
                return Err(invalid_request("enable the source before refreshing it"));
            }
            sqlx::query(
                "UPDATE mapping_sources SET
    generation = generation + 1,
    next_check_at = NOW(),
    consecutive_failures = 0,
    last_error = NULL
WHERE id = $1
",
            )
            .bind(command.id)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        }
        self.commit_write(index, transaction).await
    }

    pub async fn preview(&self, text: &str) -> Result<MappingResult, ApplicationError> {
        Ok(compile(&read_record(text)?, false, vec!["local".into()]).row)
    }

    pub async fn save(
        &self,
        command: SaveMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let record = read_record(&command.record)?;
        let mut ids = vec![record.statement.claim_id().to_string()];
        ids.extend(command.replaces.clone());
        self.update(
            &command.expected_fingerprint,
            ids,
            move |records, disabled| {
                save_record(records, record, command.replaces.as_deref(), disabled)
            },
        )
        .await
    }
    pub async fn decide(
        &self,
        command: DecideMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.update(
            &command.expected_fingerprint,
            vec![command.claim_id.clone()],
            move |records, disabled| {
                records
                    .get_mut(&command.claim_id)
                    .ok_or_else(|| invalid_request("mapping claim does not exist"))?
                    .decide(command.rejected, disabled)
            },
        )
        .await
    }
    pub async fn delete(
        &self,
        command: DeleteMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.update(
            &command.expected_fingerprint,
            vec![command.claim_id.clone()],
            move |records, _disabled| {
                let record = records
                    .get(&command.claim_id)
                    .ok_or_else(|| invalid_request("mapping claim does not exist"))?;
                if record.origins.keys().any(|source| source != "local") {
                    return Err(invalid_request(
                        "imported mappings must be suppressed rather than deleted",
                    ));
                }
                records.remove(&command.claim_id);
                Ok(())
            },
        )
        .await
    }
    pub async fn revert(
        &self,
        command: DeleteMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.update(
            &command.expected_fingerprint,
            vec![command.claim_id.clone()],
            move |records, _disabled| {
                let record = records
                    .get_mut(&command.claim_id)
                    .ok_or_else(|| invalid_request("mapping claim does not exist"))?;
                if record.origins.remove("local").is_none() {
                    return Err(invalid_request("mapping has no local decision"));
                }
                Ok(())
            },
        )
        .await
    }

    async fn update(
        &self,
        expected: &str,
        ids: Vec<String>,
        operation: impl FnOnce(
            &mut BTreeMap<String, StoredRecord>,
            &BTreeSet<String>,
        ) -> Result<(), ApplicationError>
        + Send,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let (index, mut transaction) = self.begin_write(Some(expected)).await?;
        let mut records = ids
            .iter()
            .filter_map(|id| {
                index
                    .records
                    .get(id)
                    .map(|record| (id.clone(), record.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        operation(&mut records, &index.disabled)?;
        let mut changes = BTreeMap::new();
        for id in ids {
            let claim_id = id
                .parse::<trakkin_mappings_language::ClaimId>()
                .map_err(|error| invalid_request(&error.to_string()))?;
            let text = if let Some(origin) = records
                .get(&id)
                .and_then(|record| record.origins.get("local"))
            {
                if !origin.rejected {
                    validate_evaluation(&origin.record)?;
                }
                Some(
                    Artifact {
                        record: origin.record.clone(),
                        rejected: origin.rejected,
                    }
                    .text(),
                )
            } else {
                None
            };
            changes.insert(claim_path(&claim_id), text);
        }
        let base: Option<String> = sqlx::query_scalar(
            "SELECT revision FROM mapping_sources WHERE id = 'local'
",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        let base =
            base.ok_or_else(|| ApplicationError::internal("local mapping revision is missing"))?;
        let prepared = self
            .local
            .prepare(&base, &changes, "Update Local mapping decisions")
            .await
            .map_err(write_error)?;
        self.local.apply(&prepared).await.map_err(write_error)?;
        self.import_revision(&mut transaction, "local", &self.local, &prepared.commit)
            .await?;
        self.commit_write(index, transaction).await
    }

    async fn complete_check(
        &self,
        source: &updates::DueSource,
        response: updates::CheckResult,
        now: time::OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        use self::updates::{CheckResult, retry_delay};
        let (index, mut transaction) = self.begin_write(None).await?;
        let current: bool = sqlx::query_scalar(
            "SELECT
    EXISTS(
        SELECT 1
        FROM mapping_sources
        WHERE
            id = $1
            AND generation = $2
            AND revision IS NOT DISTINCT FROM $3
            AND enabled
            AND kind = 'upstream'
    )
",
        )
        .bind(&source.id)
        .bind(source.generation)
        .bind(&source.revision)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        if !current {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM mapping_sources WHERE id = $1)
",
            )
            .bind(&source.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(database_error)?;
            if !exists && let Err(error) = self.repositories.remove_upstream(&source.id) {
                tracing::warn!(source=%source.id,error=%error,"mapping cache cleanup deferred");
            }
            return Ok(());
        }
        let mut response = response;
        if let CheckResult::Updated(commit) = &response {
            let repository = self
                .repositories
                .upstream(&source.id)
                .map_err(repository_error)?;
            let mut savepoint = transaction.begin().await.map_err(database_error)?;
            match self
                .import_revision(&mut savepoint, &source.id, &repository, commit)
                .await
            {
                Ok(()) => {
                    savepoint.commit().await.map_err(database_error)?;
                }
                Err(error) => {
                    savepoint.rollback().await.map_err(database_error)?;
                    response = CheckResult::Failed(error.to_string());
                }
            }
        }
        let (updated, failures, error) = match response {
            CheckResult::Updated(_) => (true, 0, None),
            CheckResult::Unchanged => (false, 0, None),
            CheckResult::Failed(error) => {
                tracing::warn!(source=%source.id,error=%error,"mapping Git update failed; retaining last valid mappings");
                (
                    false,
                    source.consecutive_failures.saturating_add(1),
                    Some(error.chars().take(2048).collect::<String>()),
                )
            }
        };
        sqlx::query(
            "UPDATE mapping_sources SET
    last_checked_at = $2,
    next_check_at = $3,
    consecutive_failures = $4,
    last_error = $5
WHERE id = $1
",
        )
        .bind(&source.id)
        .bind(now)
        .bind(now + time::Duration::seconds(retry_delay(source.poll_interval_seconds, failures)))
        .bind(failures)
        .bind(error)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        self.commit_write(index, transaction).await?;
        if updated {
            tracing::info!(source=%source.id,"mapping Git delta applied");
        }
        Ok(())
    }

    async fn import_revision(
        &self,
        connection: &mut PgConnection,
        source: &str,
        repository: &Repository,
        commit: &str,
    ) -> Result<(), ApplicationError> {
        let base: Option<String> = sqlx::query_scalar(
            "SELECT revision FROM mapping_sources WHERE id = $1
",
        )
        .bind(source)
        .fetch_one(&mut *connection)
        .await
        .map_err(database_error)?;
        if base.as_deref() == Some(commit) {
            return Ok(());
        }
        let base = if let Some(base) = base {
            if repository
                .contains_commit(&base)
                .await
                .map_err(repository_error)?
            {
                Some(base)
            } else {
                if source == "local" {
                    return Err(invalid_request(
                        "local mapping history is missing; restore the local Git repository before continuing",
                    ));
                }
                invalidate_source(connection, source).await?;
                sqlx::query(
                    "DELETE FROM mapping_origins WHERE source_id = $1
",
                )
                .bind(source)
                .execute(&mut *connection)
                .await
                .map_err(database_error)?;
                None
            }
        } else {
            None
        };
        let mut after = None;
        loop {
            let page = repository
                .changes(base.as_deref(), commit, after.as_deref(), MAXIMUM_BATCH)
                .await
                .map_err(repository_error)?;
            let mut upserts = Vec::new();
            let mut deletions = Vec::new();
            let mut ids = Vec::new();
            for change in page.changes {
                if change.path == POLICY {
                    continue;
                }
                if let Some(text) = change.text {
                    let artifact =
                        Artifact::parse(&change.path, &text).map_err(repository_error)?;
                    if !artifact.rejected {
                        validate_evaluation(&artifact.record)?;
                    }
                    let id = artifact.record.statement.claim_id().to_string();
                    upserts.push(serde_json::json!({"claim_id":id,"record":artifact.record,"rejected":artifact.rejected}));
                    ids.push(id);
                } else {
                    let hash = change
                        .path
                        .rsplit('/')
                        .next()
                        .and_then(|name| name.strip_suffix(".trk"))
                        .ok_or_else(|| invalid_request("invalid deleted claim path"))?;
                    let id = format!("v1:{hash}");
                    deletions.push(id.clone());
                    ids.push(id);
                }
            }
            if !deletions.is_empty() {
                sqlx::query(
                    "DELETE FROM mapping_origins WHERE source_id = $1 AND claim_id = ANY($2)
",
                )
                .bind(source)
                .bind(&deletions)
                .execute(&mut *connection)
                .await
                .map_err(database_error)?;
            }
            if !upserts.is_empty() {
                sqlx::query(
                    "INSERT INTO mapping_origins (source_id, claim_id, record, rejected)
SELECT $1 AS source_id, r.claim_id, r.record, r.rejected
FROM
    jsonb_to_recordset($2::jsonb) r (
        claim_id text, record jsonb, rejected boolean
    )
ON CONFLICT (source_id, claim_id) DO UPDATE SET
record = excluded.record,
rejected = excluded.rejected
WHERE
mapping_origins.record IS DISTINCT FROM excluded.record
OR mapping_origins.rejected <> excluded.rejected
",
                )
                .bind(source)
                .bind(serde_json::json!(upserts))
                .execute(&mut *connection)
                .await
                .map_err(database_error)?;
            }
            if !ids.is_empty() {
                sqlx::query(
                    "INSERT INTO mapping_invalidations (claim_id)
SELECT DISTINCT changed.claim_id
FROM UNNEST($1::text []) AS changed (claim_id)
WHERE TRUE
ON CONFLICT (claim_id) DO UPDATE SET sequence = DEFAULT
",
                )
                .bind(ids)
                .execute(&mut *connection)
                .await
                .map_err(database_error)?;
            }
            after = page.next;
            if after.is_none() {
                break;
            }
        }
        let policy = repository.policy(commit).await.map_err(repository_error)?;
        repository.pin(commit).await.map_err(repository_error)?;
        sqlx::query(
            "UPDATE mapping_sources SET
    revision = $2, conflict_policy = $3, last_updated_at = NOW()
WHERE id = $1
",
        )
        .bind(source)
        .bind(commit)
        .bind(serde_json::to_value(policy).map_err(repository_error)?)
        .execute(&mut *connection)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    async fn begin_read(
        &self,
    ) -> Result<(MutexGuard<'_, MappingIndex>, Transaction<'_, Postgres>), ApplicationError> {
        let mut index = self.index.lock().await;
        let mut transaction = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .await
            .map_err(database_error)?;
        self.catch_up(&mut index, &mut transaction).await?;
        Ok((index, transaction))
    }

    async fn read_index(&self) -> Result<MutexGuard<'_, MappingIndex>, ApplicationError> {
        let (index, transaction) = self.begin_read().await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(index)
    }

    async fn begin_write(
        &self,
        expected: Option<&str>,
    ) -> Result<(MutexGuard<'_, MappingIndex>, Transaction<'_, Postgres>), ApplicationError> {
        let mut index = self.index.lock().await;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock($1)
",
        )
        .bind(WRITE_LOCK)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        self.catch_up(&mut index, &mut transaction).await?;
        index.check_fingerprint(expected)?;
        Ok((index, transaction))
    }
    async fn commit_write(
        &self,
        mut index: MutexGuard<'_, MappingIndex>,
        transaction: Transaction<'_, Postgres>,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        transaction.commit().await.map_err(database_error)?;
        let mut refresh = self.pool.begin().await.map_err(database_error)?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock($1)
",
        )
        .bind(WRITE_LOCK)
        .execute(&mut *refresh)
        .await
        .map_err(database_error)?;
        self.catch_up(&mut index, &mut refresh).await?;
        refresh.commit().await.map_err(database_error)?;
        Ok(index.state())
    }
    async fn catch_up(
        &self,
        index: &mut MappingIndex,
        connection: &mut PgConnection,
    ) -> Result<(), ApplicationError> {
        let configuration = sqlx::query_as::<_, SourceConfiguration>(
            "SELECT
    id,
    display_name,
    url,
    branch,
    revision,
    poll_interval_seconds,
    enabled,
    generation,
    conflict_policy
FROM mapping_sources
ORDER BY id
",
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(database_error)?;
        let disabled = configuration
            .iter()
            .filter(|source| !source.enabled)
            .map(|source| source.id.clone())
            .collect::<BTreeSet<_>>();
        let mut policy = ConflictPolicy::default();
        for source in configuration.iter().filter(|source| source.enabled) {
            let incoming = ConflictPolicy::parse(&source.conflict_policy.to_string())
                .map_err(repository_error)?;
            policy
                .exclusive_source_pairs
                .extend(incoming.exclusive_source_pairs);
        }
        policy.exclusive_source_pairs.sort();
        policy.exclusive_source_pairs.dedup();
        if index.position == 0 && index.records.is_empty() {
            let through: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(sequence), 0) FROM mapping_invalidations
",
            )
            .fetch_one(&mut *connection)
            .await
            .map_err(database_error)?;
            let mut after = String::new();
            loop {
                let ids = sqlx::query_scalar::<_, String>(
                    "SELECT DISTINCT claim_id
FROM mapping_origins
WHERE claim_id > $1
ORDER BY claim_id
LIMIT 1000
",
                )
                .bind(&after)
                .fetch_all(&mut *connection)
                .await
                .map_err(database_error)?;
                if ids.is_empty() {
                    break;
                }
                let replacements = load_origins(connection, &ids).await?;
                index.apply(replacements, &disabled, policy.clone())?;
                after = ids.last().expect("nonempty origin page").clone();
            }
            index.position = through;
        }
        loop {
            let invalidations = sqlx::query_as::<_, (i64, String)>(
                "SELECT sequence, claim_id
FROM mapping_invalidations
WHERE sequence > $1
ORDER BY sequence
LIMIT 1000
",
            )
            .bind(index.position)
            .fetch_all(&mut *connection)
            .await
            .map_err(database_error)?;
            if invalidations.is_empty() {
                break;
            }
            let ids = invalidations
                .iter()
                .map(|(_, id)| id.clone())
                .collect::<Vec<_>>();
            let replacements = load_origins(connection, &ids).await?;
            index.apply(replacements, &disabled, policy.clone())?;
            index.position = invalidations.last().expect("nonempty invalidation page").0;
        }
        index.apply(BTreeMap::new(), &disabled, policy)?;
        index.fingerprint = sha256(
            &serde_json::to_vec(&(
                trakkin_mappings_language::semantic_revision(),
                index.position,
                configuration,
            ))
            .map_err(repository_error)?,
        );
        Ok(())
    }
}

async fn load_origins(
    connection: &mut PgConnection,
    ids: &[String],
) -> Result<BTreeMap<String, Option<StoredRecord>>, ApplicationError> {
    let rows = sqlx::query_as::<_, (String, String, serde_json::Value, bool)>(
        "SELECT claim_id, source_id, record, rejected
FROM mapping_origins
WHERE claim_id = ANY($1)
ORDER BY claim_id, source_id
",
    )
    .bind(ids)
    .fetch_all(connection)
    .await
    .map_err(database_error)?;
    let mut replacements = ids
        .iter()
        .map(|id| (id.clone(), None))
        .collect::<BTreeMap<_, _>>();
    for (id, source, record, rejected) in rows {
        let origin = OriginRecord {
            record: serde_json::from_value(record).map_err(repository_error)?,
            rejected,
        };
        let stored = replacements
            .entry(id.clone())
            .or_default()
            .get_or_insert_with(|| StoredRecord {
                claim_id: id,
                origins: BTreeMap::new(),
            });
        stored.origins.insert(source, origin);
    }
    Ok(replacements)
}

async fn invalidate_source(
    connection: &mut PgConnection,
    source: &str,
) -> Result<(), ApplicationError> {
    sqlx::query(
        "INSERT INTO mapping_invalidations (claim_id)
SELECT claim_id
FROM mapping_origins
WHERE source_id = $1
ON CONFLICT (claim_id) DO UPDATE SET sequence
= DEFAULT
",
    )
    .bind(source)
    .execute(connection)
    .await
    .map_err(database_error)?;
    Ok(())
}
async fn require_upstream(connection: &mut PgConnection, id: &str) -> Result<(), ApplicationError> {
    let kind: Option<String> = sqlx::query_scalar(
        "SELECT kind FROM mapping_sources WHERE id = $1
",
    )
    .bind(id)
    .fetch_optional(connection)
    .await
    .map_err(database_error)?;
    if kind.as_deref() != Some("upstream") {
        return Err(invalid_request(
            "mapping source must be an existing upstream",
        ));
    }
    Ok(())
}
fn validate_evaluation(record: &Record) -> Result<(), ApplicationError> {
    let bindings = Bindings::read(record).map_err(|error| invalid_request(&error.to_string()))?;
    if let Err(error) = evaluate(&record.statement, &bindings)
        && error.kind == SemanticErrorKind::Invalid
    {
        return Err(invalid_request(&error.message));
    }
    Ok(())
}
fn repository_error(error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::internal(format!("mapping Git repository failed: {error}"))
}
fn write_error(error: WriteError) -> ApplicationError {
    match error {
        WriteError::RevisionChanged => ApplicationError::new(
            ApplicationErrorCode::MappingsChanged,
            "Local repository changed; refresh it before editing",
        ),
        WriteError::DirtyWorktree => invalid_request(&error.to_string()),
        _ => repository_error(error),
    }
}
fn database_error(error: sqlx::Error) -> ApplicationError {
    ApplicationError::internal(format!("mapping storage failed: {error}"))
}
fn invalid_request(message: &str) -> ApplicationError {
    ApplicationError::new(ApplicationErrorCode::InvalidRequest, message)
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
        Box::pin(async { Ok(self.read_index().await?.state()) })
    }
    fn query(&self, query: QueryMappings) -> MappingFuture<'_, MappingPageSnapshot> {
        Box::pin(async move { self.read_index().await?.query(query) })
    }
    fn preview(&self, record: String) -> MappingFuture<'_, MappingResult> {
        Box::pin(async move { MappingStore::preview(self, &record).await })
    }
    fn save(&self, command: SaveMapping) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(MappingStore::save(self, command))
    }
    fn decide(&self, command: DecideMapping) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(MappingStore::decide(self, command))
    }
    fn delete(&self, command: DeleteMapping) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(MappingStore::delete(self, command))
    }
    fn resolve_units(
        &self,
        command: ResolveMappingUnits,
    ) -> MappingFuture<'_, MappingUnitResolutionBatchSnapshot> {
        Box::pin(async move { self.read_index().await?.resolve(command) })
    }
}

#[cfg(test)]
mod tests;
