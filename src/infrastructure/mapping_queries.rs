use serde_json::{Value, json};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use trakkin_mapping::validate_content_hash;
use uuid::Uuid;

use crate::application::{
    error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    mapping_lifecycle::{MappingLifecycleError, MappingLifecycleRepository, MappingRevisionOrigin},
    mapping_queries::{
        MappingActivationView, MappingDetail, MappingHistoryPage, MappingQueryFuture,
        MappingQueryRepository, MappingRegistryPage, MappingRevisionSummary, MappingSummary,
        QueryMappingHistory, QueryMappingRegistry,
    },
};

use super::mapping_lifecycle::{
    PostgresMappingLifecycleRepository, PostgresMappingLifecycleRepositoryError,
};

const MAX_PAGE_SIZE: u32 = 100;
const CURSOR_VERSION: i64 = 1;

#[derive(Clone)]
pub struct PostgresMappingQueryRepository {
    pool: PgPool,
    lifecycle: PostgresMappingLifecycleRepository,
}

impl PostgresMappingQueryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            lifecycle: PostgresMappingLifecycleRepository::new(pool.clone()),
            pool,
        }
    }

    async fn registry(
        &self,
        query: QueryMappingRegistry,
    ) -> Result<MappingRegistryPage, PostgresMappingQueryError> {
        validate_limit(query.limit)?;
        ensure_workspace(&self.pool, query.workspace_id).await?;
        let cursor = query
            .cursor
            .as_deref()
            .map(decode_registry_cursor)
            .transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.workspace_id != query.workspace_id)
        {
            return Err(PostgresMappingQueryError::InvalidCursor);
        }
        let after_updated_at = cursor.as_ref().map(|cursor| cursor.updated_at);
        let after_mapping_id = cursor.as_ref().map(|cursor| cursor.mapping_id);
        let row_limit = i64::from(query.limit) + 1;
        let mut rows = sqlx::query_as::<_, RegistryRow>(
            "SELECT
    mapping.id AS mapping_id,
    mapping.head_content_hash,
    revision.portable_revision,
    (
        SELECT COUNT(*)
        FROM mapping_rows AS mapping_row
        WHERE mapping_row.content_hash = mapping.head_content_hash
    ) AS row_count,
    mapping.created_at,
    mapping.updated_at,
    activation.active_content_hash,
    activation.generation AS activation_generation,
    activation.updated_at AS activation_updated_at
FROM mappings AS mapping
INNER JOIN mapping_revisions AS revision
    ON revision.content_hash = mapping.head_content_hash
LEFT JOIN workspace_mapping_activations AS activation
    ON activation.workspace_id = $1 AND activation.mapping_id = mapping.id
WHERE
    $2::TIMESTAMPTZ IS NULL
    OR mapping.updated_at < $2
    OR (mapping.updated_at = $2 AND mapping.id > $3::UUID)
ORDER BY mapping.updated_at DESC, mapping.id
LIMIT $4
",
        )
        .bind(query.workspace_id)
        .bind(after_updated_at)
        .bind(after_mapping_id)
        .bind(row_limit)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > query.limit as usize;
        if has_more {
            rows.truncate(query.limit as usize);
        }
        let next_cursor = if has_more {
            rows.last()
                .map(|row| encode_registry_cursor(query.workspace_id, row))
                .transpose()?
        } else {
            None
        };
        let mappings = rows
            .into_iter()
            .map(RegistryRow::into_summary)
            .collect::<Result<_, _>>()?;
        Ok(MappingRegistryPage {
            mappings,
            next_cursor,
        })
    }

    async fn head(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
    ) -> Result<MappingDetail, PostgresMappingQueryError> {
        let record = self.mapping_record(workspace_id, mapping_id).await?;
        let revision = self
            .lifecycle
            .get_head(mapping_id)
            .await
            .map_err(PostgresMappingQueryError::Lifecycle)?
            .ok_or(PostgresMappingQueryError::MappingNotFound)?;
        self.detail(workspace_id, record, revision).await
    }

    async fn history(
        &self,
        query: QueryMappingHistory,
    ) -> Result<MappingHistoryPage, PostgresMappingQueryError> {
        validate_limit(query.limit)?;
        let record = self
            .mapping_record(query.workspace_id, query.mapping_id)
            .await?;
        let cursor = query
            .cursor
            .as_deref()
            .map(decode_history_cursor)
            .transpose()?;
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.workspace_id != query.workspace_id || cursor.mapping_id != query.mapping_id
        }) {
            return Err(PostgresMappingQueryError::InvalidCursor);
        }
        let after_ordinal = cursor.as_ref().map(|cursor| cursor.lineage_ordinal);
        let row_limit = i64::from(query.limit) + 1;
        let mut rows = sqlx::query_as::<_, HistoryRow>(
            "SELECT
    record.content_hash,
    revision.portable_revision,
    (
        SELECT COUNT(*)
        FROM mapping_rows AS mapping_row
        WHERE mapping_row.content_hash = record.content_hash
    ) AS row_count,
    record.origin,
    record.created_at,
    record.lineage_ordinal
FROM mapping_record_revisions AS record
INNER JOIN mapping_revisions AS revision
    ON revision.content_hash = record.content_hash
WHERE
    record.mapping_id = $1
    AND ($2::BIGINT IS NULL OR record.lineage_ordinal < $2)
ORDER BY record.lineage_ordinal DESC
LIMIT $3
",
        )
        .bind(query.mapping_id)
        .bind(after_ordinal)
        .bind(row_limit)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > query.limit as usize;
        if has_more {
            rows.truncate(query.limit as usize);
        }
        let next_cursor = if has_more {
            rows.last()
                .map(|row| {
                    encode_history_cursor(query.workspace_id, query.mapping_id, row.lineage_ordinal)
                })
                .transpose()?
        } else {
            None
        };
        let revisions = rows
            .into_iter()
            .map(HistoryRow::into_summary)
            .collect::<Result<_, _>>()?;
        Ok(MappingHistoryPage {
            mapping_id: query.mapping_id,
            head_content_hash: record.head_content_hash,
            revisions,
            next_cursor,
        })
    }

    async fn revision(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> Result<MappingDetail, PostgresMappingQueryError> {
        validate_content_hash(content_hash)
            .map_err(|_| PostgresMappingQueryError::InvalidContentHash)?;
        let record = self.mapping_record(workspace_id, mapping_id).await?;
        let revision = self
            .lifecycle
            .get_revision(mapping_id, content_hash)
            .await
            .map_err(PostgresMappingQueryError::Lifecycle)?
            .ok_or(PostgresMappingQueryError::RevisionNotFound)?;
        self.detail(workspace_id, record, revision).await
    }

    async fn mapping_record(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
    ) -> Result<MappingRecordRow, PostgresMappingQueryError> {
        ensure_workspace(&self.pool, workspace_id).await?;
        sqlx::query_as::<_, MappingRecordRow>(
            "SELECT head_content_hash, created_at, updated_at
FROM mappings
WHERE id = $1
",
        )
        .bind(mapping_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresMappingQueryError::MappingNotFound)
    }

    async fn detail(
        &self,
        workspace_id: Uuid,
        record: MappingRecordRow,
        revision: crate::application::mapping_lifecycle::MappingRevisionSnapshot,
    ) -> Result<MappingDetail, PostgresMappingQueryError> {
        let activation = sqlx::query_as::<_, ActivationRow>(
            "SELECT active_content_hash, generation, updated_at
FROM workspace_mapping_activations
WHERE workspace_id = $1 AND mapping_id = $2
",
        )
        .bind(workspace_id)
        .bind(revision.mapping_id)
        .fetch_optional(&self.pool)
        .await?
        .map(ActivationRow::into_view);
        Ok(MappingDetail {
            revision,
            record_created_at: record.created_at,
            record_updated_at: record.updated_at,
            activation,
        })
    }
}

impl MappingQueryRepository for PostgresMappingQueryRepository {
    type Error = PostgresMappingQueryError;

    fn query_registry(
        &self,
        query: QueryMappingRegistry,
    ) -> MappingQueryFuture<'_, MappingRegistryPage, Self::Error> {
        Box::pin(self.registry(query))
    }

    fn query_head(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
    ) -> MappingQueryFuture<'_, MappingDetail, Self::Error> {
        Box::pin(self.head(workspace_id, mapping_id))
    }

    fn query_history(
        &self,
        query: QueryMappingHistory,
    ) -> MappingQueryFuture<'_, MappingHistoryPage, Self::Error> {
        Box::pin(self.history(query))
    }

    fn query_revision(
        &self,
        workspace_id: Uuid,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> MappingQueryFuture<'_, MappingDetail, Self::Error> {
        let content_hash = content_hash.to_owned();
        Box::pin(async move { self.revision(workspace_id, mapping_id, &content_hash).await })
    }
}

async fn ensure_workspace(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<(), PostgresMappingQueryError> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM workspaces WHERE id = $1)
",
    )
    .bind(workspace_id)
    .fetch_one(pool)
    .await?;
    if !exists {
        return Err(PostgresMappingQueryError::WorkspaceNotFound);
    }
    Ok(())
}

fn validate_limit(limit: u32) -> Result<(), PostgresMappingQueryError> {
    if limit == 0 || limit > MAX_PAGE_SIZE {
        return Err(PostgresMappingQueryError::InvalidLimit);
    }
    Ok(())
}

fn encode_registry_cursor(
    workspace_id: Uuid,
    row: &RegistryRow,
) -> Result<String, PostgresMappingQueryError> {
    encode_cursor(json!({
        "version": CURSOR_VERSION,
        "kind": "registry",
        "workspaceId": workspace_id.to_string(),
        "updatedSeconds": row.updated_at.unix_timestamp(),
        "updatedNanoseconds": row.updated_at.nanosecond(),
        "mappingId": row.mapping_id.to_string(),
    }))
}

fn encode_history_cursor(
    workspace_id: Uuid,
    mapping_id: Uuid,
    lineage_ordinal: i64,
) -> Result<String, PostgresMappingQueryError> {
    encode_cursor(json!({
        "version": CURSOR_VERSION,
        "kind": "history",
        "workspaceId": workspace_id.to_string(),
        "mappingId": mapping_id.to_string(),
        "lineageOrdinal": lineage_ordinal,
    }))
}

fn encode_cursor(value: Value) -> Result<String, PostgresMappingQueryError> {
    serde_json::to_vec(&value)
        .map(hex::encode)
        .map_err(|_| PostgresMappingQueryError::CursorEncoding)
}

fn decode_registry_cursor(cursor: &str) -> Result<RegistryCursor, PostgresMappingQueryError> {
    let value = decode_cursor(cursor, "registry")?;
    let object = value
        .as_object()
        .ok_or(PostgresMappingQueryError::InvalidCursor)?;
    let seconds = required_i64(object.get("updatedSeconds"))?;
    let nanoseconds = required_i64(object.get("updatedNanoseconds"))?;
    let nanoseconds = u32::try_from(nanoseconds)
        .ok()
        .filter(|value| *value < 1_000_000_000)
        .ok_or(PostgresMappingQueryError::InvalidCursor)?;
    let updated_at = OffsetDateTime::from_unix_timestamp(seconds)
        .map_err(|_| PostgresMappingQueryError::InvalidCursor)?
        + Duration::nanoseconds(i64::from(nanoseconds));
    Ok(RegistryCursor {
        workspace_id: required_uuid(object.get("workspaceId"))?,
        updated_at,
        mapping_id: required_uuid(object.get("mappingId"))?,
    })
}

fn decode_history_cursor(cursor: &str) -> Result<HistoryCursor, PostgresMappingQueryError> {
    let value = decode_cursor(cursor, "history")?;
    let object = value
        .as_object()
        .ok_or(PostgresMappingQueryError::InvalidCursor)?;
    let lineage_ordinal = required_i64(object.get("lineageOrdinal"))?;
    if lineage_ordinal <= 0 {
        return Err(PostgresMappingQueryError::InvalidCursor);
    }
    Ok(HistoryCursor {
        workspace_id: required_uuid(object.get("workspaceId"))?,
        mapping_id: required_uuid(object.get("mappingId"))?,
        lineage_ordinal,
    })
}

fn decode_cursor(cursor: &str, kind: &str) -> Result<Value, PostgresMappingQueryError> {
    let bytes = hex::decode(cursor).map_err(|_| PostgresMappingQueryError::InvalidCursor)?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| PostgresMappingQueryError::InvalidCursor)?;
    let object = value
        .as_object()
        .ok_or(PostgresMappingQueryError::InvalidCursor)?;
    if object.get("version").and_then(Value::as_i64) != Some(CURSOR_VERSION)
        || object.get("kind").and_then(Value::as_str) != Some(kind)
    {
        return Err(PostgresMappingQueryError::InvalidCursor);
    }
    Ok(value)
}

fn required_i64(value: Option<&Value>) -> Result<i64, PostgresMappingQueryError> {
    value
        .and_then(Value::as_i64)
        .ok_or(PostgresMappingQueryError::InvalidCursor)
}

fn required_uuid(value: Option<&Value>) -> Result<Uuid, PostgresMappingQueryError> {
    value
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(PostgresMappingQueryError::InvalidCursor)
}

fn parse_origin(value: &str) -> Result<MappingRevisionOrigin, PostgresMappingQueryError> {
    match value {
        "created" => Ok(MappingRevisionOrigin::Created),
        "updated" => Ok(MappingRevisionOrigin::Updated),
        "imported" => Ok(MappingRevisionOrigin::Imported),
        "forked" => Ok(MappingRevisionOrigin::Forked),
        _ => Err(PostgresMappingQueryError::InvalidStoredData(
            "revision origin",
        )),
    }
}

fn unsigned(value: i64, field: &'static str) -> Result<u64, PostgresMappingQueryError> {
    u64::try_from(value).map_err(|_| PostgresMappingQueryError::InvalidStoredData(field))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegistryCursor {
    workspace_id: Uuid,
    updated_at: OffsetDateTime,
    mapping_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HistoryCursor {
    workspace_id: Uuid,
    mapping_id: Uuid,
    lineage_ordinal: i64,
}

#[derive(FromRow)]
struct RegistryRow {
    mapping_id: Uuid,
    head_content_hash: String,
    portable_revision: i64,
    row_count: i64,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    active_content_hash: Option<String>,
    activation_generation: Option<i64>,
    activation_updated_at: Option<OffsetDateTime>,
}

impl RegistryRow {
    fn into_summary(self) -> Result<MappingSummary, PostgresMappingQueryError> {
        let activation = match (self.activation_generation, self.activation_updated_at) {
            (Some(generation), Some(updated_at)) => Some(MappingActivationView {
                active_content_hash: self.active_content_hash,
                generation,
                updated_at,
            }),
            (None, None) if self.active_content_hash.is_none() => None,
            _ => {
                return Err(PostgresMappingQueryError::InvalidStoredData(
                    "workspace activation",
                ));
            }
        };
        Ok(MappingSummary {
            mapping_id: self.mapping_id,
            head_content_hash: self.head_content_hash,
            portable_revision: unsigned(self.portable_revision, "portable revision")?,
            row_count: unsigned(self.row_count, "row count")?,
            created_at: self.created_at,
            updated_at: self.updated_at,
            activation,
        })
    }
}

#[derive(FromRow)]
struct MappingRecordRow {
    head_content_hash: String,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[derive(FromRow)]
struct HistoryRow {
    content_hash: String,
    portable_revision: i64,
    row_count: i64,
    origin: String,
    created_at: OffsetDateTime,
    lineage_ordinal: i64,
}

impl HistoryRow {
    fn into_summary(self) -> Result<MappingRevisionSummary, PostgresMappingQueryError> {
        Ok(MappingRevisionSummary {
            content_hash: self.content_hash,
            portable_revision: unsigned(self.portable_revision, "portable revision")?,
            row_count: unsigned(self.row_count, "row count")?,
            origin: parse_origin(&self.origin)?,
            created_at: self.created_at,
        })
    }
}

#[derive(FromRow)]
struct ActivationRow {
    active_content_hash: Option<String>,
    generation: i64,
    updated_at: OffsetDateTime,
}

impl ActivationRow {
    fn into_view(self) -> MappingActivationView {
        MappingActivationView {
            active_content_hash: self.active_content_hash,
            generation: self.generation,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, Error)]
pub enum PostgresMappingQueryError {
    #[error("mapping page limit must be between 1 and {MAX_PAGE_SIZE}")]
    InvalidLimit,
    #[error("mapping cursor is invalid for this query")]
    InvalidCursor,
    #[error("mapping cursor could not be encoded")]
    CursorEncoding,
    #[error("workspace was not found")]
    WorkspaceNotFound,
    #[error("mapping was not found")]
    MappingNotFound,
    #[error("mapping revision was not found")]
    RevisionNotFound,
    #[error("mapping content hash is invalid")]
    InvalidContentHash,
    #[error("stored mapping data is invalid: {0}")]
    InvalidStoredData(&'static str),
    #[error("mapping lifecycle read failed")]
    Lifecycle(MappingLifecycleError<PostgresMappingLifecycleRepositoryError>),
    #[error("mapping query database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoApplicationError for PostgresMappingQueryError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::InvalidLimit => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "mapping query is invalid",
                "limit",
                "out_of_range",
                "mapping page limit must be between 1 and 100",
            ),
            Self::InvalidCursor => ApplicationError::new(
                ApplicationErrorCode::MappingCursorInvalid,
                "mapping cursor is invalid for this query",
            ),
            Self::WorkspaceNotFound => ApplicationError::new(
                ApplicationErrorCode::WorkspaceNotFound,
                "workspace was not found",
            ),
            Self::MappingNotFound => ApplicationError::new(
                ApplicationErrorCode::MappingNotFound,
                "mapping was not found",
            ),
            Self::RevisionNotFound => ApplicationError::new(
                ApplicationErrorCode::MappingRevisionNotFound,
                "mapping revision was not found",
            ),
            Self::InvalidContentHash => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "mapping revision query is invalid",
                "contentHash",
                "invalid_content_hash",
                "mapping content hash must be a SHA-256 content hash",
            ),
            Self::CursorEncoding
            | Self::InvalidStoredData(_)
            | Self::Lifecycle(_)
            | Self::Database(_) => {
                ApplicationError::new(ApplicationErrorCode::Internal, "mapping query failed")
            }
        }
    }
}
