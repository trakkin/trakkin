use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::OffsetDateTime;
use trakkin_mapping::{MappingDocument, Selection};
use uuid::Uuid;

use crate::application::mapping_lifecycle::{
    AppendMappingRevision, ChangeMappingActivation, MappingActivationKind, MappingLifecycleError,
    MappingLifecycleFuture, MappingLifecycleRepository, MappingRevisionOrigin,
    MappingRevisionParent, MappingRevisionSnapshot, PackageImportMetadata, StoreMappingRevision,
    WorkspaceMappingActivation, canonical_row_set_diff,
};

type LifecycleResult<T> = Result<T, MappingLifecycleError<PostgresMappingLifecycleRepositoryError>>;

#[derive(Clone)]
pub struct PostgresMappingLifecycleRepository {
    pool: PgPool,
}

impl PostgresMappingLifecycleRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn head(&self, mapping_id: Uuid) -> LifecycleResult<Option<MappingRevisionSnapshot>> {
        let content_hash = sqlx::query_scalar::<_, String>(
            "SELECT head_content_hash
FROM mappings
WHERE id = $1
",
        )
        .bind(mapping_id)
        .fetch_optional(&self.pool)
        .await?;
        match content_hash {
            Some(content_hash) => self.revision(mapping_id, &content_hash).await,
            None => Ok(None),
        }
    }

    async fn revision(
        &self,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> LifecycleResult<Option<MappingRevisionSnapshot>> {
        let row = sqlx::query_as::<_, RevisionRow>(
            "SELECT
    record.mapping_id,
    revision.content_hash,
    revision.semantic_fingerprint,
    revision.canonical_json,
    record.origin,
    record.created_at
FROM mapping_record_revisions AS record
INNER JOIN mapping_revisions AS revision
    ON revision.content_hash = record.content_hash
WHERE record.mapping_id = $1 AND record.content_hash = $2
",
        )
        .bind(mapping_id)
        .bind(content_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(self.snapshot(row).await?))
    }

    async fn snapshot(&self, row: RevisionRow) -> LifecycleResult<MappingRevisionSnapshot> {
        let document = parse_stored_document(&row)?;
        let parents = sqlx::query_as::<_, ParentRow>(
            "SELECT parent_hash, verified
FROM mapping_revision_parents
WHERE mapping_id = $1 AND child_hash = $2
ORDER BY parent_ordinal
",
        )
        .bind(row.mapping_id)
        .bind(&row.content_hash)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|parent| MappingRevisionParent {
            content_hash: parent.parent_hash,
            verified: parent.verified,
        })
        .collect();
        let package = sqlx::query_as::<_, PackageRow>(
            "SELECT
    package_id,
    document_path,
    roots,
    license_expression,
    attribution,
    signatures,
    forge_commit,
    required_mapping_schema,
    required_resolver,
    required_coordinate_registry_digest
FROM mapping_package_provenance
WHERE mapping_id = $1 AND content_hash = $2
",
        )
        .bind(row.mapping_id)
        .bind(&row.content_hash)
        .fetch_optional(&self.pool)
        .await?
        .map(PackageRow::into_metadata)
        .transpose()
        .map_err(repository_error)?;

        Ok(MappingRevisionSnapshot {
            mapping_id: row.mapping_id,
            document,
            content_hash: row.content_hash,
            parents,
            origin: parse_origin(&row.origin).map_err(repository_error)?,
            package,
            content_reused: false,
            created_at: row.created_at,
        })
    }

    async fn create_revision(
        &self,
        command: StoreMappingRevision,
    ) -> LifecycleResult<MappingRevisionSnapshot> {
        let mapping_id = command.mapping_id;
        let content_hash = command.document.content_hash();
        let mut transaction = self.pool.begin().await?;
        let content_reused = create_mapping_in_transaction(&mut transaction, &command).await?;
        transaction.commit().await?;
        let mut snapshot = self
            .revision(mapping_id, &content_hash)
            .await?
            .ok_or_else(|| {
                repository_error(PostgresMappingLifecycleRepositoryError::MissingWrite)
            })?;
        snapshot.content_reused = content_reused;
        Ok(snapshot)
    }

    async fn append(
        &self,
        command: AppendMappingRevision,
    ) -> LifecycleResult<MappingRevisionSnapshot> {
        let mapping_id = command.revision.mapping_id;
        let content_hash = command.revision.document.content_hash();
        let mut transaction = self.pool.begin().await?;
        let current_head = sqlx::query_scalar::<_, String>(
            "SELECT head_content_hash
FROM mappings
WHERE id = $1
FOR UPDATE
",
        )
        .bind(mapping_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(MappingLifecycleError::MappingNotFound)?;
        if current_head != command.expected_head {
            let current_json = sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT canonical_json
FROM mapping_revisions
WHERE content_hash = $1
",
            )
            .bind(&current_head)
            .fetch_one(&mut *transaction)
            .await?;
            let current =
                MappingDocument::from_json(std::str::from_utf8(&current_json).map_err(|_| {
                    repository_error(PostgresMappingLifecycleRepositoryError::CorruptDocument)
                })?)
                .map_err(|_| {
                    repository_error(PostgresMappingLifecycleRepositoryError::CorruptDocument)
                })?;
            return Err(MappingLifecycleError::HeadMoved {
                current_head,
                diff: canonical_row_set_diff(&current, &command.revision.document),
            });
        }

        let current_revision = sqlx::query_scalar::<_, i64>(
            "SELECT portable_revision
FROM mapping_revisions
WHERE content_hash = $1
",
        )
        .bind(&current_head)
        .fetch_one(&mut *transaction)
        .await?;
        let expected_revision = current_revision.checked_add(1).ok_or_else(|| {
            repository_error(PostgresMappingLifecycleRepositoryError::RevisionOverflow)
        })?;
        let proposed_revision =
            i64::try_from(command.revision.document.revision()).map_err(|_| {
                repository_error(PostgresMappingLifecycleRepositoryError::RevisionOverflow)
            })?;
        if proposed_revision != expected_revision {
            return Err(repository_error(
                PostgresMappingLifecycleRepositoryError::InvalidRevisionSequence,
            ));
        }

        let content_reused = insert_revision_content(&mut transaction, &command.revision).await?;
        let lineage_ordinal = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(lineage_ordinal), 0) + 1
FROM mapping_record_revisions
WHERE mapping_id = $1
",
        )
        .bind(mapping_id)
        .fetch_one(&mut *transaction)
        .await?;
        insert_occurrence(&mut transaction, &command.revision, lineage_ordinal).await?;
        sqlx::query(
            "UPDATE mappings
SET head_content_hash = $1, updated_at = $2
WHERE id = $3
",
        )
        .bind(&content_hash)
        .bind(command.revision.created_at)
        .bind(mapping_id)
        .execute(&mut *transaction)
        .await?;
        ensure_acyclic(&mut transaction, mapping_id, &content_hash).await?;
        transaction.commit().await?;

        let mut snapshot = self
            .revision(mapping_id, &content_hash)
            .await?
            .ok_or_else(|| {
                repository_error(PostgresMappingLifecycleRepositoryError::MissingWrite)
            })?;
        snapshot.content_reused = content_reused;
        Ok(snapshot)
    }

    async fn import_revisions(
        &self,
        revisions: Vec<StoreMappingRevision>,
    ) -> LifecycleResult<Vec<MappingRevisionSnapshot>> {
        let identities = revisions
            .iter()
            .map(|revision| (revision.mapping_id, revision.document.content_hash()))
            .collect::<Vec<_>>();
        let mut transaction = self.pool.begin().await?;
        let mut reused = Vec::with_capacity(revisions.len());
        for revision in &revisions {
            reused.push(create_mapping_in_transaction(&mut transaction, revision).await?);
        }
        transaction.commit().await?;

        let mut snapshots = Vec::with_capacity(identities.len());
        for ((mapping_id, content_hash), content_reused) in identities.into_iter().zip(reused) {
            let mut snapshot =
                self.revision(mapping_id, &content_hash)
                    .await?
                    .ok_or_else(|| {
                        repository_error(PostgresMappingLifecycleRepositoryError::MissingWrite)
                    })?;
            snapshot.content_reused = content_reused;
            snapshots.push(snapshot);
        }
        Ok(snapshots)
    }

    async fn change(
        &self,
        command: ChangeMappingActivation,
    ) -> LifecycleResult<WorkspaceMappingActivation> {
        let mut transaction = self.pool.begin().await?;
        let workspace_exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1::BIGINT
FROM workspaces
WHERE id = $1 AND status = 'active'
FOR UPDATE
",
        )
        .bind(command.workspace_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if !workspace_exists {
            return Err(MappingLifecycleError::WorkspaceNotFound);
        }
        let mapping_exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1::BIGINT
FROM mappings
WHERE id = $1
FOR UPDATE
",
        )
        .bind(command.mapping_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if !mapping_exists {
            return Err(MappingLifecycleError::MappingNotFound);
        }
        if let Some(content_hash) = &command.content_hash {
            let revision_exists = sqlx::query_scalar::<_, i64>(
                "SELECT 1::BIGINT
FROM mapping_record_revisions
WHERE mapping_id = $1 AND content_hash = $2
",
            )
            .bind(command.mapping_id)
            .bind(content_hash)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !revision_exists {
                return Err(MappingLifecycleError::RevisionNotFound);
            }
        }

        let existing = sqlx::query_as::<_, ActivationRow>(
            "SELECT active_content_hash, generation
FROM workspace_mapping_activations
WHERE workspace_id = $1 AND mapping_id = $2
FOR UPDATE
",
        )
        .bind(command.workspace_id)
        .bind(command.mapping_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let generation = existing.as_ref().map_or(Ok(1), |activation| {
            activation.generation.checked_add(1).ok_or_else(|| {
                repository_error(PostgresMappingLifecycleRepositoryError::GenerationOverflow)
            })
        })?;
        let previous_content_hash = existing.and_then(|activation| activation.active_content_hash);
        sqlx::query(
            "INSERT INTO workspace_mapping_activations (
    workspace_id, mapping_id, active_content_hash, generation, updated_at
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (workspace_id, mapping_id) DO UPDATE
SET
active_content_hash = excluded.active_content_hash,
generation = excluded.generation,
updated_at = excluded.updated_at
",
        )
        .bind(command.workspace_id)
        .bind(command.mapping_id)
        .bind(&command.content_hash)
        .bind(generation)
        .bind(command.changed_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO workspace_mapping_activation_events (
    id,
    workspace_id,
    mapping_id,
    generation,
    kind,
    previous_content_hash,
    selected_content_hash,
    changed_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
",
        )
        .bind(command.event_id)
        .bind(command.workspace_id)
        .bind(command.mapping_id)
        .bind(generation)
        .bind(activation_kind(command.kind))
        .bind(previous_content_hash)
        .bind(&command.content_hash)
        .bind(command.changed_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(WorkspaceMappingActivation {
            workspace_id: command.workspace_id,
            mapping_id: command.mapping_id,
            active_content_hash: command.content_hash,
            generation,
            updated_at: command.changed_at,
        })
    }
}

impl MappingLifecycleRepository for PostgresMappingLifecycleRepository {
    type Error = PostgresMappingLifecycleRepositoryError;

    fn get_head(
        &self,
        mapping_id: Uuid,
    ) -> MappingLifecycleFuture<'_, Option<MappingRevisionSnapshot>, Self::Error> {
        Box::pin(self.head(mapping_id))
    }

    fn get_revision(
        &self,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> MappingLifecycleFuture<'_, Option<MappingRevisionSnapshot>, Self::Error> {
        let content_hash = content_hash.to_owned();
        Box::pin(async move { self.revision(mapping_id, &content_hash).await })
    }

    fn create_mapping(
        &self,
        command: StoreMappingRevision,
    ) -> MappingLifecycleFuture<'_, MappingRevisionSnapshot, Self::Error> {
        Box::pin(self.create_revision(command))
    }

    fn append_revision(
        &self,
        command: AppendMappingRevision,
    ) -> MappingLifecycleFuture<'_, MappingRevisionSnapshot, Self::Error> {
        Box::pin(self.append(command))
    }

    fn import_package(
        &self,
        revisions: Vec<StoreMappingRevision>,
    ) -> MappingLifecycleFuture<'_, Vec<MappingRevisionSnapshot>, Self::Error> {
        Box::pin(self.import_revisions(revisions))
    }

    fn change_activation(
        &self,
        command: ChangeMappingActivation,
    ) -> MappingLifecycleFuture<'_, WorkspaceMappingActivation, Self::Error> {
        Box::pin(self.change(command))
    }
}

async fn create_mapping_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StoreMappingRevision,
) -> LifecycleResult<bool> {
    let content_hash = command.document.content_hash();
    let content_reused = insert_revision_content(transaction, command).await?;
    sqlx::query(
        "INSERT INTO mappings (id, head_content_hash, created_at, updated_at)
VALUES ($1, $2, $3, $3)
",
    )
    .bind(command.mapping_id)
    .bind(&content_hash)
    .bind(command.created_at)
    .execute(&mut **transaction)
    .await?;
    insert_occurrence(transaction, command, 1).await?;
    ensure_acyclic(transaction, command.mapping_id, &content_hash).await?;
    Ok(content_reused)
}

async fn insert_revision_content(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StoreMappingRevision,
) -> LifecycleResult<bool> {
    let content_hash = command.document.content_hash();
    let semantic_fingerprint = command.document.semantic_fingerprint();
    let canonical_json = command.document.canonical_json();
    let portable_revision = i64::try_from(command.document.revision())
        .map_err(|_| repository_error(PostgresMappingLifecycleRepositoryError::RevisionOverflow))?;
    let result = sqlx::query(
        "INSERT INTO mapping_revisions (
    content_hash,
    semantic_fingerprint,
    schema_name,
    portable_revision,
    canonical_json,
    created_at
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (content_hash) DO NOTHING
",
    )
    .bind(&content_hash)
    .bind(&semantic_fingerprint)
    .bind(command.document.schema())
    .bind(portable_revision)
    .bind(&canonical_json)
    .bind(command.created_at)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() == 0 {
        let stored = sqlx::query_as::<_, ExistingRevisionRow>(
            "SELECT semantic_fingerprint, portable_revision, canonical_json
FROM mapping_revisions
WHERE content_hash = $1
",
        )
        .bind(&content_hash)
        .fetch_one(&mut **transaction)
        .await?;
        if stored.semantic_fingerprint != semantic_fingerprint
            || stored.portable_revision != portable_revision
            || stored.canonical_json != canonical_json
        {
            return Err(repository_error(
                PostgresMappingLifecycleRepositoryError::HashCollision,
            ));
        }
        return Ok(true);
    }

    for (row_ordinal, row) in command.document.mappings().iter().enumerate() {
        let canonical_bytes =
            serde_json::to_vec(&row.iter().map(ToString::to_string).collect::<Vec<_>>())
                .map_err(PostgresMappingLifecycleRepositoryError::from)
                .map_err(repository_error)?;
        let canonical_row = serde_json::from_slice::<serde_json::Value>(&canonical_bytes)
            .map_err(PostgresMappingLifecycleRepositoryError::from)
            .map_err(repository_error)?;
        let row_hash = hash(&canonical_bytes);
        sqlx::query(
            "INSERT INTO mapping_rows (
    content_hash, row_ordinal, row_hash, canonical_row
) VALUES ($1, $2, $3, $4)
",
        )
        .bind(&content_hash)
        .bind(i32::try_from(row_ordinal).expect("mapping row bound fits i32"))
        .bind(&row_hash)
        .bind(canonical_row)
        .execute(&mut **transaction)
        .await?;
        for (endpoint_ordinal, endpoint) in row.iter().enumerate() {
            let coordinate_ids = endpoint
                .selector()
                .map(|selector| {
                    selector
                        .components()
                        .iter()
                        .map(|component| component.coordinate().to_owned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let finite_ordinal = endpoint.selector().and_then(|selector| {
                let [component] = selector.components() else {
                    return None;
                };
                let (start, end) = match component.selection() {
                    Selection::Ordinal(value) => (*value, *value),
                    Selection::Range { start, end } => (*start, *end),
                    _ => return None,
                };
                Some((component.coordinate(), start, end))
            });
            sqlx::query(
                "INSERT INTO mapping_row_endpoints (
    content_hash,
    row_hash,
    endpoint_ordinal,
    endpoint_hash,
    reference_namespace,
    reference_value,
    selector,
    coordinate_ids,
    ordinal_coordinate,
    ordinal_start,
    ordinal_end
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
",
            )
            .bind(&content_hash)
            .bind(&row_hash)
            .bind(i32::try_from(endpoint_ordinal).expect("endpoint bound fits i32"))
            .bind(hash(endpoint.as_str().as_bytes()))
            .bind(endpoint.namespace())
            .bind(endpoint.value())
            .bind(endpoint.selector().map(|selector| selector.as_str()))
            .bind(coordinate_ids)
            .bind(finite_ordinal.map(|(coordinate, _, _)| coordinate))
            .bind(
                finite_ordinal
                    .map(|(_, start, _)| i64::try_from(start).expect("portable ordinal fits i64")),
            )
            .bind(
                finite_ordinal
                    .map(|(_, _, end)| i64::try_from(end).expect("portable ordinal fits i64")),
            )
            .execute(&mut **transaction)
            .await?;
        }
    }
    Ok(false)
}

async fn insert_occurrence(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StoreMappingRevision,
    lineage_ordinal: i64,
) -> LifecycleResult<()> {
    let content_hash = command.document.content_hash();
    sqlx::query(
        "INSERT INTO mapping_record_revisions (
    mapping_id, content_hash, lineage_ordinal, origin, created_at
) VALUES ($1, $2, $3, $4, $5)
",
    )
    .bind(command.mapping_id)
    .bind(&content_hash)
    .bind(lineage_ordinal)
    .bind(origin(command.origin))
    .bind(command.created_at)
    .execute(&mut **transaction)
    .await?;
    for (parent_ordinal, parent) in command.parents.iter().enumerate() {
        let known = sqlx::query_scalar::<_, i64>(
            "SELECT 1::BIGINT
FROM mapping_revisions
WHERE content_hash = $1
",
        )
        .bind(&parent.content_hash)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
        sqlx::query(
            "INSERT INTO mapping_revision_parents (
    mapping_id, child_hash, parent_hash, parent_ordinal, verified
) VALUES ($1, $2, $3, $4, $5)
",
        )
        .bind(command.mapping_id)
        .bind(&content_hash)
        .bind(&parent.content_hash)
        .bind(i32::try_from(parent_ordinal).expect("parent bound fits i32"))
        .bind(parent.verified || known)
        .execute(&mut **transaction)
        .await?;
    }
    if let Some(package) = &command.package {
        insert_package(transaction, command, package).await?;
    }
    Ok(())
}

async fn insert_package(
    transaction: &mut Transaction<'_, Postgres>,
    command: &StoreMappingRevision,
    package: &PackageImportMetadata,
) -> LifecycleResult<()> {
    sqlx::query(
        "INSERT INTO mapping_package_provenance (
    package_id,
    document_path,
    mapping_id,
    content_hash,
    roots,
    license_expression,
    attribution,
    signatures,
    forge_commit,
    required_mapping_schema,
    required_resolver,
    required_coordinate_registry_digest,
    imported_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
",
    )
    .bind(package.package_id)
    .bind(&package.document_path)
    .bind(command.mapping_id)
    .bind(command.document.content_hash())
    .bind(json!(package.roots))
    .bind(&package.license_expression)
    .bind(json!(package.attribution))
    .bind(json!(package.signatures))
    .bind(&package.forge_commit)
    .bind(&package.required_mapping_schema)
    .bind(&package.required_resolver)
    .bind(&package.required_coordinate_registry_digest)
    .bind(command.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn ensure_acyclic(
    transaction: &mut Transaction<'_, Postgres>,
    mapping_id: Uuid,
    child_hash: &str,
) -> LifecycleResult<()> {
    let cyclic = sqlx::query_scalar::<_, bool>(
        "WITH RECURSIVE ancestors (content_hash) AS (
    SELECT parent_hash
    FROM mapping_revision_parents
    WHERE mapping_id = $1 AND child_hash = $2

    UNION

    SELECT parent.parent_hash
    FROM mapping_revision_parents AS parent
    INNER JOIN ancestors
        ON ancestors.content_hash = parent.child_hash
    WHERE parent.mapping_id = $1
)

SELECT EXISTS(
    SELECT 1
    FROM ancestors
    WHERE content_hash = $2
)
",
    )
    .bind(mapping_id)
    .bind(child_hash)
    .fetch_one(&mut **transaction)
    .await?;
    if cyclic {
        return Err(MappingLifecycleError::LineageCycle);
    }
    Ok(())
}

fn parse_stored_document(
    row: &RevisionRow,
) -> Result<MappingDocument, MappingLifecycleError<PostgresMappingLifecycleRepositoryError>> {
    let json = std::str::from_utf8(&row.canonical_json)
        .map_err(|_| repository_error(PostgresMappingLifecycleRepositoryError::CorruptDocument))?;
    let document = MappingDocument::from_json(json)
        .map_err(|_| repository_error(PostgresMappingLifecycleRepositoryError::CorruptDocument))?;
    if document.content_hash() != row.content_hash
        || document.semantic_fingerprint() != row.semantic_fingerprint
    {
        return Err(repository_error(
            PostgresMappingLifecycleRepositoryError::CorruptDocument,
        ));
    }
    Ok(document)
}

fn origin(origin: MappingRevisionOrigin) -> &'static str {
    match origin {
        MappingRevisionOrigin::Created => "created",
        MappingRevisionOrigin::Updated => "updated",
        MappingRevisionOrigin::Imported => "imported",
        MappingRevisionOrigin::Forked => "forked",
    }
}

fn parse_origin(
    value: &str,
) -> Result<MappingRevisionOrigin, PostgresMappingLifecycleRepositoryError> {
    match value {
        "created" => Ok(MappingRevisionOrigin::Created),
        "updated" => Ok(MappingRevisionOrigin::Updated),
        "imported" => Ok(MappingRevisionOrigin::Imported),
        "forked" => Ok(MappingRevisionOrigin::Forked),
        _ => Err(PostgresMappingLifecycleRepositoryError::CorruptOrigin),
    }
}

fn activation_kind(kind: MappingActivationKind) -> &'static str {
    match kind {
        MappingActivationKind::Activate => "activate",
        MappingActivationKind::Rollback => "rollback",
        MappingActivationKind::Retire => "retire",
    }
}

fn hash(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn repository_error(
    error: PostgresMappingLifecycleRepositoryError,
) -> MappingLifecycleError<PostgresMappingLifecycleRepositoryError> {
    MappingLifecycleError::Repository(error)
}

impl From<sqlx::Error> for MappingLifecycleError<PostgresMappingLifecycleRepositoryError> {
    fn from(error: sqlx::Error) -> Self {
        repository_error(error.into())
    }
}

#[derive(Debug, Error)]
pub enum PostgresMappingLifecycleRepositoryError {
    #[error("mapping lifecycle database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("mapping lifecycle JSON operation failed")]
    Json(#[from] serde_json::Error),
    #[error("stored mapping document is corrupt")]
    CorruptDocument,
    #[error("stored mapping revision origin is corrupt")]
    CorruptOrigin,
    #[error("stored mapping package provenance is corrupt")]
    CorruptPackage,
    #[error("mapping content hash collision detected")]
    HashCollision,
    #[error("mapping revision sequence is invalid")]
    InvalidRevisionSequence,
    #[error("mapping revision overflow")]
    RevisionOverflow,
    #[error("mapping activation generation overflow")]
    GenerationOverflow,
    #[error("mapping lifecycle write could not be reloaded")]
    MissingWrite,
}

#[derive(FromRow)]
struct RevisionRow {
    mapping_id: Uuid,
    content_hash: String,
    semantic_fingerprint: String,
    canonical_json: Vec<u8>,
    origin: String,
    created_at: OffsetDateTime,
}

#[derive(FromRow)]
struct ExistingRevisionRow {
    semantic_fingerprint: String,
    portable_revision: i64,
    canonical_json: Vec<u8>,
}

#[derive(FromRow)]
struct ParentRow {
    parent_hash: String,
    verified: bool,
}

#[derive(FromRow)]
struct PackageRow {
    package_id: Uuid,
    document_path: String,
    roots: Value,
    license_expression: String,
    attribution: Value,
    signatures: Value,
    forge_commit: Option<String>,
    required_mapping_schema: String,
    required_resolver: String,
    required_coordinate_registry_digest: String,
}

impl PackageRow {
    fn into_metadata(
        self,
    ) -> Result<PackageImportMetadata, PostgresMappingLifecycleRepositoryError> {
        Ok(PackageImportMetadata {
            package_id: self.package_id,
            document_path: self.document_path,
            roots: serde_json::from_value(self.roots)
                .map_err(|_| PostgresMappingLifecycleRepositoryError::CorruptPackage)?,
            license_expression: self.license_expression,
            attribution: serde_json::from_value(self.attribution)
                .map_err(|_| PostgresMappingLifecycleRepositoryError::CorruptPackage)?,
            signatures: serde_json::from_value(self.signatures)
                .map_err(|_| PostgresMappingLifecycleRepositoryError::CorruptPackage)?,
            forge_commit: self.forge_commit,
            required_mapping_schema: self.required_mapping_schema,
            required_resolver: self.required_resolver,
            required_coordinate_registry_digest: self.required_coordinate_registry_digest,
        })
    }
}

#[derive(FromRow)]
struct ActivationRow {
    active_content_hash: Option<String>,
    generation: i64,
}
