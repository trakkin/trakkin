use std::collections::BTreeSet;

use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    application::error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    application::sync_preview::{
        SYNC_PREVIEW_SCHEMA, SyncPreviewFingerprint, SyncPreviewFuture, SyncPreviewLifecycleError,
        SyncPreviewRepository, SyncPreviewSnapshot, SyncPreviewSnapshotAction,
        sync_preview_policy_revision_pin,
    },
    domain::{
        acquisition::{CatalogRelationKey, ProviderItemKey, StateSubject},
        synchronization::OrdinalSpan,
    },
};

type PreviewResult<T> = Result<T, SyncPreviewLifecycleError<PostgresSyncPreviewRepositoryError>>;

#[derive(Clone)]
pub struct PostgresSyncPreviewRepository {
    pool: PgPool,
}

impl PostgresSyncPreviewRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn get(
        &self,
        workspace_id: Uuid,
        preview_id: Uuid,
    ) -> PreviewResult<Option<SyncPreviewSnapshot>> {
        let row = sqlx::query_as::<_, PreviewRow>(
            "SELECT
    id,
    workspace_id,
    policy_id,
    policy_revision,
    policy_content_hash,
    content_hash,
    schema_name,
    canonical_json,
    fingerprint_hash,
    fingerprint_json,
    fresh_until,
    created_at,
    expires_at
FROM sync_previews
WHERE id = $1 AND workspace_id = $2
",
        )
        .bind(preview_id)
        .bind(workspace_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let action_rows = sqlx::query_as::<_, ActionRow>(
            "SELECT
    ordinal,
    action_hash,
    receiver_source_id,
    receiver_subject_kind,
    receiver_subject_namespace,
    receiver_subject_value,
    semantic_field,
    segment_coordinate,
    segment_start,
    segment_end,
    canonical_json
FROM sync_preview_actions
WHERE preview_id = $1
ORDER BY ordinal
",
        )
        .bind(preview_id)
        .fetch_all(&self.pool)
        .await?;
        self.snapshot(row, action_rows)
    }

    fn snapshot(
        &self,
        row: PreviewRow,
        action_rows: Vec<ActionRow>,
    ) -> PreviewResult<Option<SyncPreviewSnapshot>> {
        let actions = action_rows
            .into_iter()
            .map(snapshot_action)
            .collect::<PreviewResult<Vec<_>>>()?;
        validate_stored_preview(&row, &actions)?;
        Ok(Some(SyncPreviewSnapshot {
            preview_id: row.id,
            workspace_id: row.workspace_id,
            policy_id: row.policy_id,
            policy_revision: u64::try_from(row.policy_revision).map_err(|_| corrupt_preview())?,
            policy_content_hash: row.policy_content_hash,
            content_hash: row.content_hash,
            canonical_json: row.canonical_json,
            fingerprint_hash: row.fingerprint_hash,
            fingerprint_json: row.fingerprint_json,
            fresh_until: row.fresh_until,
            actions,
            created_at: row.created_at,
            expires_at: row.expires_at,
        }))
    }

    async fn create(&self, snapshot: SyncPreviewSnapshot) -> PreviewResult<SyncPreviewSnapshot> {
        validate_snapshot_before_insert(&snapshot)?;
        let mut transaction = self.pool.begin().await?;
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM sync_previews WHERE id = $1)
",
        )
        .bind(snapshot.preview_id)
        .fetch_one(&mut *transaction)
        .await?
        {
            return Err(repository_error(
                PostgresSyncPreviewRepositoryError::DuplicatePreview,
            ));
        }
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1
    FROM sync_previews
    WHERE workspace_id = $1 AND content_hash = $2
)
",
        )
        .bind(snapshot.workspace_id)
        .bind(&snapshot.content_hash)
        .fetch_one(&mut *transaction)
        .await?
        {
            return Err(repository_error(
                PostgresSyncPreviewRepositoryError::DuplicatePreviewContent,
            ));
        }
        ensure_sources_attached(&mut transaction, &snapshot).await?;
        let policy_revision = i64::try_from(snapshot.policy_revision)
            .map_err(|_| repository_error(PostgresSyncPreviewRepositoryError::NumericOverflow))?;
        sqlx::query(
            "INSERT INTO sync_previews (
    id,
    workspace_id,
    policy_id,
    policy_revision,
    policy_content_hash,
    content_hash,
    schema_name,
    canonical_json,
    fingerprint_hash,
    fingerprint_json,
    fresh_until,
    created_at,
    expires_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
",
        )
        .bind(snapshot.preview_id)
        .bind(snapshot.workspace_id)
        .bind(snapshot.policy_id)
        .bind(policy_revision)
        .bind(&snapshot.policy_content_hash)
        .bind(&snapshot.content_hash)
        .bind(SYNC_PREVIEW_SCHEMA)
        .bind(&snapshot.canonical_json)
        .bind(&snapshot.fingerprint_hash)
        .bind(&snapshot.fingerprint_json)
        .bind(snapshot.fresh_until)
        .bind(snapshot.created_at)
        .bind(snapshot.expires_at)
        .execute(&mut *transaction)
        .await?;
        for action in &snapshot.actions {
            insert_action(&mut transaction, &snapshot, action).await?;
        }
        transaction.commit().await?;
        self.get(snapshot.workspace_id, snapshot.preview_id)
            .await?
            .ok_or_else(|| repository_error(PostgresSyncPreviewRepositoryError::MissingWrite))
    }
}

impl SyncPreviewRepository for PostgresSyncPreviewRepository {
    type Error = PostgresSyncPreviewRepositoryError;

    fn get_preview(
        &self,
        workspace_id: Uuid,
        preview_id: Uuid,
    ) -> SyncPreviewFuture<'_, Option<SyncPreviewSnapshot>, Self::Error> {
        Box::pin(self.get(workspace_id, preview_id))
    }

    fn create_preview(
        &self,
        snapshot: SyncPreviewSnapshot,
    ) -> SyncPreviewFuture<'_, SyncPreviewSnapshot, Self::Error> {
        Box::pin(self.create(snapshot))
    }
}

async fn ensure_sources_attached(
    transaction: &mut Transaction<'_, Postgres>,
    snapshot: &SyncPreviewSnapshot,
) -> PreviewResult<()> {
    let policy_revision = i64::try_from(snapshot.policy_revision)
        .map_err(|_| repository_error(PostgresSyncPreviewRepositoryError::NumericOverflow))?;
    let roles = sqlx::query_as::<_, SourceRoleRow>(
        "SELECT
    roles.source_id,
    roles.role,
    workspace_sources.source_id IS NOT NULL AS attached
FROM sync_policy_source_roles AS roles
LEFT JOIN workspace_sources
    ON
        workspace_sources.workspace_id = roles.workspace_id
        AND workspace_sources.source_id = roles.source_id
WHERE
    roles.policy_id = $1
    AND roles.workspace_id = $2
    AND roles.revision = $3
ORDER BY roles.role, roles.source_id
",
    )
    .bind(snapshot.policy_id)
    .bind(snapshot.workspace_id)
    .bind(policy_revision)
    .fetch_all(&mut **transaction)
    .await?;
    if let Some(role) = roles.iter().find(|role| !role.attached) {
        return Err(repository_error(
            PostgresSyncPreviewRepositoryError::SourceNotAttached {
                source_id: role.source_id,
            },
        ));
    }
    let receiver_sources = roles
        .iter()
        .filter(|role| role.role == "receiver")
        .map(|role| role.source_id)
        .collect::<BTreeSet<_>>();
    if let Some(action) = snapshot
        .actions
        .iter()
        .find(|action| !receiver_sources.contains(&action.receiver_source_id))
    {
        return Err(repository_error(
            PostgresSyncPreviewRepositoryError::ReceiverOutsidePolicy {
                source_id: action.receiver_source_id,
            },
        ));
    }
    Ok(())
}

async fn insert_action(
    transaction: &mut Transaction<'_, Postgres>,
    snapshot: &SyncPreviewSnapshot,
    action: &SyncPreviewSnapshotAction,
) -> PreviewResult<()> {
    let ordinal = i32::try_from(action.ordinal)
        .map_err(|_| repository_error(PostgresSyncPreviewRepositoryError::NumericOverflow))?;
    let segment_start = i64::try_from(action.segment.start())
        .map_err(|_| repository_error(PostgresSyncPreviewRepositoryError::NumericOverflow))?;
    let segment_end = i64::try_from(action.segment.end())
        .map_err(|_| repository_error(PostgresSyncPreviewRepositoryError::NumericOverflow))?;
    let (subject_kind, subject_namespace, subject_value) = subject_parts(&action.receiver_subject);
    sqlx::query(
        "INSERT INTO sync_preview_actions (
    preview_id,
    workspace_id,
    preview_hash,
    ordinal,
    action_hash,
    receiver_source_id,
    receiver_subject_kind,
    receiver_subject_namespace,
    receiver_subject_value,
    semantic_field,
    segment_coordinate,
    segment_start,
    segment_end,
    canonical_json
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
)
",
    )
    .bind(snapshot.preview_id)
    .bind(snapshot.workspace_id)
    .bind(&snapshot.content_hash)
    .bind(ordinal)
    .bind(&action.action_hash)
    .bind(action.receiver_source_id)
    .bind(subject_kind)
    .bind(subject_namespace)
    .bind(subject_value)
    .bind(&action.semantic_field)
    .bind(action.segment.coordinate())
    .bind(segment_start)
    .bind(segment_end)
    .bind(&action.canonical_json)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn snapshot_action(row: ActionRow) -> PreviewResult<SyncPreviewSnapshotAction> {
    let receiver_subject = match row.receiver_subject_kind.as_str() {
        "provider_item" => StateSubject::ProviderItem(
            ProviderItemKey::new(row.receiver_subject_namespace, row.receiver_subject_value)
                .map_err(|_| corrupt_preview())?,
        ),
        "catalog_relation" => StateSubject::CatalogRelation(
            CatalogRelationKey::new(row.receiver_subject_namespace, row.receiver_subject_value)
                .map_err(|_| corrupt_preview())?,
        ),
        _ => return Err(corrupt_preview()),
    };
    Ok(SyncPreviewSnapshotAction {
        ordinal: u32::try_from(row.ordinal).map_err(|_| corrupt_preview())?,
        action_hash: row.action_hash,
        receiver_source_id: row.receiver_source_id,
        receiver_subject,
        semantic_field: row.semantic_field,
        segment: OrdinalSpan::new(
            row.segment_coordinate,
            u64::try_from(row.segment_start).map_err(|_| corrupt_preview())?,
            u64::try_from(row.segment_end).map_err(|_| corrupt_preview())?,
        )
        .map_err(|_| corrupt_preview())?,
        canonical_json: row.canonical_json,
    })
}

fn validate_stored_preview(
    row: &PreviewRow,
    actions: &[SyncPreviewSnapshotAction],
) -> PreviewResult<()> {
    let fingerprint = SyncPreviewFingerprint::from_canonical_json(&row.fingerprint_json)
        .map_err(|_| corrupt_preview())?;
    let policy_revision = u64::try_from(row.policy_revision).map_err(|_| corrupt_preview())?;
    if row.schema_name != SYNC_PREVIEW_SCHEMA
        || row.content_hash != sha256_hash(&row.canonical_json)
        || row.fingerprint_hash != sha256_hash(&row.fingerprint_json)
        || fingerprint.policy_revision
            != sync_preview_policy_revision_pin(policy_revision, &row.policy_content_hash)
        || fingerprint.preview_time != row.created_at
        || fingerprint.preview_expiry != row.expires_at
        || fingerprint.fresh_until() != row.fresh_until
        || row.fresh_until <= row.created_at
        || row.fresh_until > row.expires_at
        || row.expires_at <= row.created_at
    {
        return Err(corrupt_preview());
    }
    let document: Value =
        serde_json::from_slice(&row.canonical_json).map_err(|_| corrupt_preview())?;
    if serde_json::to_vec(&document).map_err(|_| corrupt_preview())? != row.canonical_json
        || !has_exact_keys(
            &document,
            &[
                "actions",
                "candidates",
                "createdAt",
                "expiresAt",
                "fingerprint",
                "fingerprintHash",
                "freshUntil",
                "policyContentHash",
                "policyId",
                "policyRevision",
                "schema",
                "workspaceId",
            ],
        )
        || document["schema"] != SYNC_PREVIEW_SCHEMA
        || document["workspaceId"] != row.workspace_id.to_string()
        || document["policyId"] != row.policy_id.to_string()
        || document["policyRevision"] != row.policy_revision
        || document["policyContentHash"] != row.policy_content_hash
        || document["fingerprintHash"] != row.fingerprint_hash
        || serde_json::to_vec(&document["fingerprint"]).map_err(|_| corrupt_preview())?
            != row.fingerprint_json
        || parse_time(&document["freshUntil"])? != row.fresh_until
        || parse_time(&document["createdAt"])? != row.created_at
        || parse_time(&document["expiresAt"])? != row.expires_at
    {
        return Err(corrupt_preview());
    }
    if !document["candidates"]
        .as_array()
        .is_some_and(|candidates| candidates.iter().all(valid_candidate_shape))
    {
        return Err(corrupt_preview());
    }
    let document_actions = document["actions"].as_array().ok_or_else(corrupt_preview)?;
    if document_actions.len() != actions.len() {
        return Err(corrupt_preview());
    }
    for (ordinal, action) in actions.iter().enumerate() {
        if action.ordinal != u32::try_from(ordinal).map_err(|_| corrupt_preview())?
            || action.action_hash != sha256_hash(&action.canonical_json)
        {
            return Err(corrupt_preview());
        }
        let canonical_action: Value =
            serde_json::from_slice(&action.canonical_json).map_err(|_| corrupt_preview())?;
        if serde_json::to_vec(&canonical_action).map_err(|_| corrupt_preview())?
            != action.canonical_json
            || !valid_action_shape(&canonical_action)
            || canonical_action["receiverSourceId"] != action.receiver_source_id.to_string()
            || canonical_action["semanticField"] != action.semantic_field
            || canonical_action["segment"]["coordinate"] != action.segment.coordinate()
            || canonical_action["segment"]["start"] != action.segment.start()
            || canonical_action["segment"]["end"] != action.segment.end()
            || !subject_matches(
                &canonical_action["receiverSubject"],
                &action.receiver_subject,
            )
        {
            return Err(corrupt_preview());
        }
        let mut preview_action = document_actions[ordinal].clone();
        let preview_object = preview_action.as_object_mut().ok_or_else(corrupt_preview)?;
        if preview_object.remove("actionHash") != Some(Value::String(action.action_hash.clone()))
            || preview_object.remove("ordinal") != Some(Value::from(action.ordinal))
            || preview_action != canonical_action
        {
            return Err(corrupt_preview());
        }
    }
    Ok(())
}

fn validate_snapshot_before_insert(snapshot: &SyncPreviewSnapshot) -> PreviewResult<()> {
    let row = PreviewRow {
        id: snapshot.preview_id,
        workspace_id: snapshot.workspace_id,
        policy_id: snapshot.policy_id,
        policy_revision: i64::try_from(snapshot.policy_revision)
            .map_err(|_| repository_error(PostgresSyncPreviewRepositoryError::NumericOverflow))?,
        policy_content_hash: snapshot.policy_content_hash.clone(),
        content_hash: snapshot.content_hash.clone(),
        schema_name: SYNC_PREVIEW_SCHEMA.to_owned(),
        canonical_json: snapshot.canonical_json.clone(),
        fingerprint_hash: snapshot.fingerprint_hash.clone(),
        fingerprint_json: snapshot.fingerprint_json.clone(),
        fresh_until: snapshot.fresh_until,
        created_at: snapshot.created_at,
        expires_at: snapshot.expires_at,
    };
    validate_stored_preview(&row, &snapshot.actions)
}

fn valid_candidate_shape(value: &Value) -> bool {
    has_exact_keys(value, &["aggregation", "segment", "semanticField", "value"])
        && valid_aggregation_shape(&value["aggregation"])
        && value["segment"].is_object()
        && value["semanticField"].is_string()
        && value["value"].is_object()
}

fn valid_action_shape(value: &Value) -> bool {
    has_exact_keys(
        value,
        &[
            "aggregation",
            "candidate",
            "intendedValue",
            "lossy",
            "membershipEffect",
            "receiverCapability",
            "receiverSourceId",
            "receiverSubject",
            "segment",
            "semanticField",
            "targetedReadCapability",
            "targetedState",
            "translation",
        ],
    ) && valid_aggregation_shape(&value["aggregation"])
        && value["candidate"].is_object()
        && value["intendedValue"].is_u64()
        && value["lossy"].is_boolean()
        && value["membershipEffect"].is_string()
        && value["receiverCapability"].is_object()
        && value["receiverSourceId"].is_string()
        && value["receiverSubject"].is_object()
        && value["segment"].is_object()
        && value["semanticField"].is_string()
        && value["targetedReadCapability"].is_object()
        && valid_targeted_state_shape(&value["targetedState"])
        && value["translation"].is_object()
}

fn valid_aggregation_shape(value: &Value) -> bool {
    has_exact_keys(value, &["contributors", "mappingProofs", "reducer"])
        && value["contributors"].is_array()
        && value["mappingProofs"].is_array()
        && value["reducer"].is_string()
}

fn valid_targeted_state_shape(value: &Value) -> bool {
    has_exact_keys(
        value,
        &[
            "expiresAt",
            "fields",
            "membership",
            "observedAt",
            "precondition",
            "providerRevision",
        ],
    ) && value["fields"].as_array().is_some_and(|fields| {
        fields.iter().all(|field| {
            has_exact_keys(field, &["field", "presence", "value"])
                && field["field"].is_object()
                && field["presence"].is_string()
        })
    }) && value["membership"].is_string()
        && value["observedAt"].is_string()
        && value["providerRevision"].is_array()
}

fn has_exact_keys(value: &Value, keys: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
    })
}

fn parse_time(value: &Value) -> PreviewResult<OffsetDateTime> {
    value
        .as_str()
        .ok_or_else(corrupt_preview)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).map_err(|_| corrupt_preview()))
}

fn subject_matches(value: &Value, subject: &StateSubject) -> bool {
    let (kind, namespace, bytes) = subject_parts(subject);
    value["kind"] == kind
        && value["namespace"] == namespace
        && value["value"].as_array().is_some_and(|values| {
            values.len() == bytes.len()
                && values.iter().zip(bytes).all(|(value, byte)| {
                    value.as_u64().and_then(|value| u8::try_from(value).ok()) == Some(*byte)
                })
        })
}

fn subject_parts(subject: &StateSubject) -> (&'static str, &str, &[u8]) {
    match subject {
        StateSubject::ProviderItem(key) => ("provider_item", key.namespace(), key.value()),
        StateSubject::CatalogRelation(key) => ("catalog_relation", key.namespace(), key.value()),
    }
}

fn sha256_hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn corrupt_preview() -> SyncPreviewLifecycleError<PostgresSyncPreviewRepositoryError> {
    repository_error(PostgresSyncPreviewRepositoryError::CorruptPreview)
}

fn repository_error(
    error: PostgresSyncPreviewRepositoryError,
) -> SyncPreviewLifecycleError<PostgresSyncPreviewRepositoryError> {
    SyncPreviewLifecycleError::Repository(error)
}

impl From<sqlx::Error> for SyncPreviewLifecycleError<PostgresSyncPreviewRepositoryError> {
    fn from(error: sqlx::Error) -> Self {
        repository_error(error.into())
    }
}

#[derive(Debug, Error)]
pub enum PostgresSyncPreviewRepositoryError {
    #[error("synchronization preview database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("stored synchronization preview is corrupt")]
    CorruptPreview,
    #[error("synchronization preview identity already exists")]
    DuplicatePreview,
    #[error("synchronization preview content already exists in the workspace")]
    DuplicatePreviewContent,
    #[error("synchronization preview source {source_id} is not attached")]
    SourceNotAttached { source_id: Uuid },
    #[error("synchronization preview receiver {source_id} is outside the policy")]
    ReceiverOutsidePolicy { source_id: Uuid },
    #[error("synchronization preview numeric value exceeds PostgreSQL bounds")]
    NumericOverflow,
    #[error("synchronization preview write could not be reloaded")]
    MissingWrite,
}

impl IntoApplicationError for PostgresSyncPreviewRepositoryError {
    fn into_application_error(self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::Internal,
            "synchronization preview database operation failed",
        )
    }
}

#[derive(FromRow)]
struct PreviewRow {
    id: Uuid,
    workspace_id: Uuid,
    policy_id: Uuid,
    policy_revision: i64,
    policy_content_hash: String,
    content_hash: String,
    schema_name: String,
    canonical_json: Vec<u8>,
    fingerprint_hash: String,
    fingerprint_json: Vec<u8>,
    fresh_until: OffsetDateTime,
    created_at: OffsetDateTime,
    expires_at: OffsetDateTime,
}

#[derive(FromRow)]
struct ActionRow {
    ordinal: i32,
    action_hash: String,
    receiver_source_id: Uuid,
    receiver_subject_kind: String,
    receiver_subject_namespace: String,
    receiver_subject_value: Vec<u8>,
    semantic_field: String,
    segment_coordinate: String,
    segment_start: i64,
    segment_end: i64,
    canonical_json: Vec<u8>,
}

#[derive(FromRow)]
struct SourceRoleRow {
    source_id: Uuid,
    role: String,
    attached: bool,
}
