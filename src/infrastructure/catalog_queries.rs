use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    application::catalog_queries::{
        CatalogEntry, CatalogPage, CatalogQueryFuture, CatalogQueryRepository, InspectSourceState,
        QueryCatalogPage, ResolveSubjectUnits, SourceStateInspection, SubjectUnitCandidate,
        SubjectUnitCandidates,
    },
    application::error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
    domain::acquisition::{
        BinaryAssetKey, BinaryAssetReference, CatalogItem, CatalogRelation, CatalogRelationKey,
        ProviderItemKey, StateField, StateObservation, StateSubject, StateValue, Term,
    },
};

const MAX_PAGE_SIZE: u32 = 100;
const CURSOR_VERSION: i64 = 1;

#[derive(Clone)]
pub struct PostgresCatalogQueryRepository {
    pool: PgPool,
}

impl PostgresCatalogQueryRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn query_page(&self, query: QueryCatalogPage) -> Result<CatalogPage, CatalogQueryError> {
        if query.limit == 0 || query.limit > MAX_PAGE_SIZE {
            return Err(CatalogQueryError::InvalidLimit);
        }
        let search = normalize_search(query.search.as_deref());
        let mut transaction = begin_snapshot(&self.pool).await?;
        ensure_attachment(&mut transaction, query.workspace_id, query.source_id).await?;
        let stream = load_stream(&mut transaction, query.source_id, "catalog").await?;
        let publication_id = available_publication(&stream, "catalog")?;

        let decoded = query.cursor.as_deref().map(decode_cursor).transpose()?;
        if let Some(cursor) = &decoded {
            cursor.validate_scope(&query, search.as_deref())?;
            if cursor.publication_id != publication_id {
                return Err(CatalogQueryError::CursorExpired);
            }
        }

        let parent_namespace = query
            .parent
            .as_ref()
            .map(|parent| parent.namespace().to_owned());
        let parent_value = query.parent.as_ref().map(|parent| parent.value().to_vec());
        let after_order = decoded.as_ref().map(|cursor| cursor.order.clone());
        let after_namespace = decoded
            .as_ref()
            .map(|cursor| cursor.relation_namespace.clone());
        let after_value = decoded.as_ref().map(|cursor| cursor.relation_value.clone());
        let row_limit = i64::from(query.limit) + 1;
        let mut rows = sqlx::query_as::<_, CatalogRow>(
            r#"SELECT
    relation.key_namespace AS relation_key_namespace,
    relation.key_value AS relation_key_value,
    relation.parent_key_namespace,
    relation.parent_key_value,
    relation.item_key_namespace,
    relation.item_key_value,
    relation.kind_namespace AS relation_kind_namespace,
    relation.kind_name AS relation_kind_name,
    relation.order_parts,
    relation.attributes AS relation_attributes,
    EXISTS(
        SELECT 1
        FROM catalog_relations AS child
        WHERE
            child.source_id = relation.source_id
            AND child.parent_key_namespace = relation.key_namespace
            AND child.parent_key_value = relation.key_value
    ) AS has_children,
    item.kind_namespace AS item_kind_namespace,
    item.kind_name AS item_kind_name,
    item.display_name AS item_display_name,
    item.attributes AS item_attributes
FROM catalog_relations AS relation
LEFT JOIN catalog_items AS item
    ON
        item.source_id = relation.source_id
        AND item.key_namespace = relation.item_key_namespace
        AND item.key_value = relation.item_key_value
WHERE
    relation.source_id = $1
    AND (
        ($2::TEXT IS NULL AND relation.parent_key_namespace IS NULL)
        OR (
            relation.parent_key_namespace = $2
            AND relation.parent_key_value = $3::BYTEA
        )
    )
    AND (
        $4::TEXT IS NULL
        OR LOWER(item.display_name) LIKE '%' || $4 || '%'
    )
    AND (
        $5 IS NULL
        OR relation.order_parts > $5
        OR (
            relation.order_parts = $5
            AND relation.key_namespace > $6
        )
        OR (
            relation.order_parts = $5
            AND relation.key_namespace = $6
            AND relation.key_value > $7
        )
    )
ORDER BY
    relation.order_parts,
    relation.key_namespace,
    relation.key_value
LIMIT $8
"#,
        )
        .bind(query.source_id)
        .bind(&parent_namespace)
        .bind(&parent_value)
        .bind(&search)
        .bind(&after_order)
        .bind(&after_namespace)
        .bind(&after_value)
        .bind(row_limit)
        .fetch_all(&mut *transaction)
        .await?;

        let has_more = rows.len() > query.limit as usize;
        if has_more {
            rows.truncate(query.limit as usize);
        }
        let next_cursor = if has_more {
            rows.last()
                .map(|row| encode_cursor(&query, search.as_deref(), publication_id, row))
                .transpose()?
        } else {
            None
        };
        let assets = load_assets(&mut transaction, query.source_id, &rows).await?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            entries.push(map_catalog_entry(row, &assets)?);
        }
        transaction.commit().await?;
        Ok(CatalogPage {
            publication_id,
            entries,
            next_cursor,
        })
    }

    async fn inspect_state(
        &self,
        query: InspectSourceState,
    ) -> Result<SourceStateInspection, CatalogQueryError> {
        let mut transaction = begin_snapshot(&self.pool).await?;
        ensure_attachment(&mut transaction, query.workspace_id, query.source_id).await?;
        let stream = load_stream(&mut transaction, query.source_id, "state").await?;
        let publication_id = available_publication(&stream, "state")?;
        let (subject_kind, subject_namespace, subject_value) =
            subject_filter(query.subject.as_ref());
        let rows = sqlx::query_as::<_, StateRow>(
            "SELECT
    subject_kind,
    subject_key_namespace,
    subject_key_value,
    field_namespace,
    field_name,
    unit_namespace,
    unit_name,
    value,
    provider_revision,
    observed_at
FROM state_observations
WHERE
    source_id = $1
    AND (
        $2::TEXT IS NULL
        OR (
            subject_kind = $2
            AND subject_key_namespace = $3::TEXT
            AND subject_key_value = $4::BYTEA
        )
    )
ORDER BY
    subject_kind,
    subject_key_namespace,
    subject_key_value,
    field_namespace,
    field_name,
    unit_namespace,
    unit_name
",
        )
        .bind(query.source_id)
        .bind(&subject_kind)
        .bind(&subject_namespace)
        .bind(&subject_value)
        .fetch_all(&mut *transaction)
        .await?;
        let observations = rows
            .into_iter()
            .map(StateRow::into_observation)
            .collect::<Result<_, _>>()?;
        transaction.commit().await?;
        Ok(SourceStateInspection {
            publication_id,
            observations,
        })
    }

    async fn resolve_subject_unit_candidates(
        &self,
        query: ResolveSubjectUnits,
    ) -> Result<SubjectUnitCandidates, CatalogQueryError> {
        let mut transaction = begin_snapshot(&self.pool).await?;
        ensure_attachment(&mut transaction, query.workspace_id, query.source_id).await?;
        let stream = load_stream(&mut transaction, query.source_id, "catalog").await?;
        let publication_id = available_publication(&stream, "catalog")?;
        let (subject_kind, subject_namespace, subject_value) = subject_filter(Some(&query.subject));
        let rows = sqlx::query_as::<_, (String, i64)>(
            "SELECT unit_key, relevance_rank
FROM subject_unit_bindings
WHERE
    source_id = $1
    AND subject_kind = $2
    AND subject_key_namespace = $3
    AND subject_key_value = $4
ORDER BY relevance_rank, unit_key
",
        )
        .bind(query.source_id)
        .bind(subject_kind)
        .bind(subject_namespace)
        .bind(subject_value)
        .fetch_all(&mut *transaction)
        .await?;
        let candidates = rows
            .into_iter()
            .map(|(unit_key, relevance_rank)| {
                Ok(SubjectUnitCandidate {
                    unit_key,
                    relevance_rank: u32::try_from(relevance_rank).map_err(|_| {
                        CatalogQueryError::InvalidStoredData("subject unit relevance rank")
                    })?,
                })
            })
            .collect::<Result<_, CatalogQueryError>>()?;
        transaction.commit().await?;
        Ok(SubjectUnitCandidates {
            publication_id,
            candidates,
        })
    }
}

impl CatalogQueryRepository for PostgresCatalogQueryRepository {
    type Error = CatalogQueryError;

    fn query_catalog_page(
        &self,
        query: QueryCatalogPage,
    ) -> CatalogQueryFuture<'_, CatalogPage, Self::Error> {
        Box::pin(self.query_page(query))
    }

    fn inspect_source_state(
        &self,
        query: InspectSourceState,
    ) -> CatalogQueryFuture<'_, SourceStateInspection, Self::Error> {
        Box::pin(self.inspect_state(query))
    }

    fn resolve_subject_units(
        &self,
        query: ResolveSubjectUnits,
    ) -> CatalogQueryFuture<'_, SubjectUnitCandidates, Self::Error> {
        Box::pin(self.resolve_subject_unit_candidates(query))
    }
}

async fn begin_snapshot(pool: &PgPool) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    pool.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n")
        .await
}

async fn ensure_attachment(
    transaction: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    source_id: Uuid,
) -> Result<(), CatalogQueryError> {
    let attached = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
    SELECT 1 FROM workspace_sources
    WHERE workspace_id = $1 AND source_id = $2
)
",
    )
    .bind(workspace_id)
    .bind(source_id)
    .fetch_one(&mut **transaction)
    .await?;
    if attached {
        Ok(())
    } else {
        Err(CatalogQueryError::SourceNotAttached)
    }
}

async fn load_stream(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    stream_kind: &'static str,
) -> Result<StreamRow, CatalogQueryError> {
    sqlx::query_as::<_, StreamRow>(
        "SELECT status, publication_id
FROM source_streams
WHERE source_id = $1 AND stream_kind = $2
",
    )
    .bind(source_id)
    .bind(stream_kind)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(CatalogQueryError::StreamNotFound(stream_kind))
}

fn available_publication(
    stream: &StreamRow,
    stream_kind: &'static str,
) -> Result<Uuid, CatalogQueryError> {
    if stream.status != "available" {
        return Err(CatalogQueryError::StreamUnavailable(stream_kind));
    }
    stream
        .publication_id
        .ok_or(CatalogQueryError::InvalidStoredData(
            "available publication",
        ))
}

type ItemAssets = BTreeMap<(String, Vec<u8>), Vec<BinaryAssetReference>>;

fn map_catalog_entry(
    row: CatalogRow,
    assets: &ItemAssets,
) -> Result<CatalogEntry, CatalogQueryError> {
    let relation = row.relation()?;
    let has_children = row.has_children;
    let item = if let Some(item_key_namespace) = &row.item_key_namespace {
        let item_key_value = row
            .item_key_value
            .as_ref()
            .ok_or(CatalogQueryError::InvalidStoredData("catalog item key"))?;
        let item_assets = assets
            .get(&(item_key_namespace.clone(), item_key_value.clone()))
            .cloned()
            .unwrap_or_default();
        Some(row.item(item_assets)?)
    } else {
        None
    };
    Ok(CatalogEntry {
        relation,
        item,
        has_children,
    })
}

async fn load_assets(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    rows: &[CatalogRow],
) -> Result<ItemAssets, CatalogQueryError> {
    let keys = rows
        .iter()
        .filter_map(|row| {
            row.item_key_namespace
                .as_ref()
                .zip(row.item_key_value.as_ref())
                .map(|(namespace, value)| (namespace.clone(), value.clone()))
        })
        .collect::<BTreeSet<_>>();
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let (namespaces, values): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
    let asset_rows = sqlx::query_as::<_, AssetRow>(
        "SELECT
    asset.item_key_namespace,
    asset.item_key_value,
    asset_key_namespace,
    asset_key_value,
    kind_namespace,
    kind_name
FROM catalog_assets AS asset
JOIN UNNEST($2::TEXT [], $3::BYTEA []) AS requested (namespace, value)
    ON
        asset.item_key_namespace = requested.namespace
        AND asset.item_key_value = requested.value
WHERE asset.source_id = $1
ORDER BY asset_key_namespace, asset_key_value
",
    )
    .bind(source_id)
    .bind(&namespaces)
    .bind(&values)
    .fetch_all(&mut **transaction)
    .await?;
    let mut assets = ItemAssets::new();
    for row in asset_rows {
        let key = BinaryAssetKey::new(row.asset_key_namespace, row.asset_key_value)
            .map_err(|_| CatalogQueryError::InvalidStoredData("binary asset key"))?;
        let kind = Term::new(row.kind_namespace, row.kind_name)
            .map_err(|_| CatalogQueryError::InvalidStoredData("binary asset kind"))?;
        assets
            .entry((row.item_key_namespace, row.item_key_value))
            .or_default()
            .push(BinaryAssetReference::new(key, kind));
    }
    Ok(assets)
}

fn normalize_search(search: Option<&str>) -> Option<String> {
    search
        .map(str::trim)
        .filter(|search| !search.is_empty())
        .map(str::to_lowercase)
}

fn subject_filter(
    subject: Option<&StateSubject>,
) -> (Option<String>, Option<String>, Option<Vec<u8>>) {
    match subject {
        Some(StateSubject::ProviderItem(key)) => (
            Some("provider_item".to_owned()),
            Some(key.namespace().to_owned()),
            Some(key.value().to_vec()),
        ),
        Some(StateSubject::CatalogRelation(key)) => (
            Some("catalog_relation".to_owned()),
            Some(key.namespace().to_owned()),
            Some(key.value().to_vec()),
        ),
        None => (None, None, None),
    }
}

fn encode_cursor(
    query: &QueryCatalogPage,
    search: Option<&str>,
    publication_id: Uuid,
    row: &CatalogRow,
) -> Result<String, CatalogQueryError> {
    let parent = query.parent.as_ref().map(|parent| {
        json!({
            "namespace": parent.namespace(),
            "value": hex::encode(parent.value()),
        })
    });
    let value = json!({
        "version": CURSOR_VERSION,
        "workspaceId": query.workspace_id.to_string(),
        "sourceId": query.source_id.to_string(),
        "publicationId": publication_id.to_string(),
        "parent": parent,
        "search": search,
        "order": row.order_parts,
        "relationNamespace": row.relation_key_namespace,
        "relationValue": hex::encode(&row.relation_key_value),
    });
    serde_json::to_vec(&value)
        .map(hex::encode)
        .map_err(|_| CatalogQueryError::CursorEncoding)
}

fn decode_cursor(cursor: &str) -> Result<DecodedCursor, CatalogQueryError> {
    let bytes = hex::decode(cursor).map_err(|_| CatalogQueryError::InvalidCursor)?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| CatalogQueryError::InvalidCursor)?;
    let object = value.as_object().ok_or(CatalogQueryError::InvalidCursor)?;
    if object.get("version").and_then(Value::as_i64) != Some(CURSOR_VERSION) {
        return Err(CatalogQueryError::InvalidCursor);
    }
    let parent = match object.get("parent") {
        Some(Value::Null) | None => None,
        Some(Value::Object(parent)) => Some(CursorKey {
            namespace: required_string(parent.get("namespace"))?,
            value: decode_cursor_bytes(parent.get("value"))?,
        }),
        _ => return Err(CatalogQueryError::InvalidCursor),
    };
    let search = match object.get("search") {
        Some(Value::Null) | None => None,
        Some(value) => Some(required_string(Some(value))?),
    };
    let order = object
        .get("order")
        .and_then(Value::as_array)
        .ok_or(CatalogQueryError::InvalidCursor)?
        .iter()
        .map(|part| part.as_i64().ok_or(CatalogQueryError::InvalidCursor))
        .collect::<Result<_, _>>()?;
    Ok(DecodedCursor {
        workspace_id: required_uuid(object.get("workspaceId"))?,
        source_id: required_uuid(object.get("sourceId"))?,
        publication_id: required_uuid(object.get("publicationId"))?,
        parent,
        search,
        order,
        relation_namespace: required_string(object.get("relationNamespace"))?,
        relation_value: decode_cursor_bytes(object.get("relationValue"))?,
    })
}

fn required_string(value: Option<&Value>) -> Result<String, CatalogQueryError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or(CatalogQueryError::InvalidCursor)
}

fn required_uuid(value: Option<&Value>) -> Result<Uuid, CatalogQueryError> {
    required_string(value)
        .and_then(|value| Uuid::parse_str(&value).map_err(|_| CatalogQueryError::InvalidCursor))
}

fn decode_cursor_bytes(value: Option<&Value>) -> Result<Vec<u8>, CatalogQueryError> {
    let value = required_string(value)?;
    let bytes = hex::decode(value).map_err(|_| CatalogQueryError::InvalidCursor)?;
    if bytes.is_empty() {
        return Err(CatalogQueryError::InvalidCursor);
    }
    Ok(bytes)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CursorKey {
    namespace: String,
    value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecodedCursor {
    workspace_id: Uuid,
    source_id: Uuid,
    publication_id: Uuid,
    parent: Option<CursorKey>,
    search: Option<String>,
    order: Vec<i64>,
    relation_namespace: String,
    relation_value: Vec<u8>,
}

impl DecodedCursor {
    fn validate_scope(
        &self,
        query: &QueryCatalogPage,
        search: Option<&str>,
    ) -> Result<(), CatalogQueryError> {
        let parent = query.parent.as_ref().map(|parent| CursorKey {
            namespace: parent.namespace().to_owned(),
            value: parent.value().to_vec(),
        });
        if self.workspace_id != query.workspace_id
            || self.source_id != query.source_id
            || self.parent != parent
            || self.search.as_deref() != search
        {
            return Err(CatalogQueryError::InvalidCursor);
        }
        Ok(())
    }
}

#[derive(FromRow)]
struct CatalogRow {
    relation_key_namespace: String,
    relation_key_value: Vec<u8>,
    parent_key_namespace: Option<String>,
    parent_key_value: Option<Vec<u8>>,
    item_key_namespace: Option<String>,
    item_key_value: Option<Vec<u8>>,
    relation_kind_namespace: String,
    relation_kind_name: String,
    order_parts: Vec<i64>,
    relation_attributes: Value,
    has_children: bool,
    item_kind_namespace: Option<String>,
    item_kind_name: Option<String>,
    item_display_name: Option<String>,
    item_attributes: Option<Value>,
}

impl CatalogRow {
    fn relation(&self) -> Result<CatalogRelation, CatalogQueryError> {
        CatalogRelation::new(
            CatalogRelationKey::new(
                self.relation_key_namespace.clone(),
                self.relation_key_value.clone(),
            )
            .map_err(|_| CatalogQueryError::InvalidStoredData("catalog relation key"))?,
            match (&self.parent_key_namespace, &self.parent_key_value) {
                (Some(namespace), Some(value)) => Some(
                    CatalogRelationKey::new(namespace.clone(), value.clone())
                        .map_err(|_| CatalogQueryError::InvalidStoredData("catalog parent key"))?,
                ),
                (None, None) => None,
                _ => return Err(CatalogQueryError::InvalidStoredData("catalog parent key")),
            },
            match (&self.item_key_namespace, &self.item_key_value) {
                (Some(namespace), Some(value)) => Some(
                    ProviderItemKey::new(namespace.clone(), value.clone())
                        .map_err(|_| CatalogQueryError::InvalidStoredData("catalog item key"))?,
                ),
                (None, None) => None,
                _ => return Err(CatalogQueryError::InvalidStoredData("catalog item key")),
            },
            Term::new(
                self.relation_kind_namespace.clone(),
                self.relation_kind_name.clone(),
            )
            .map_err(|_| CatalogQueryError::InvalidStoredData("catalog relation kind"))?,
            self.order_parts.clone(),
            self.relation_attributes.clone(),
        )
        .map_err(|_| CatalogQueryError::InvalidStoredData("catalog relation"))
    }

    fn item(self, assets: Vec<BinaryAssetReference>) -> Result<CatalogItem, CatalogQueryError> {
        let key_namespace = self
            .item_key_namespace
            .ok_or(CatalogQueryError::InvalidStoredData("catalog item key"))?;
        let key_value = self
            .item_key_value
            .ok_or(CatalogQueryError::InvalidStoredData("catalog item key"))?;
        CatalogItem::new(
            ProviderItemKey::new(key_namespace, key_value)
                .map_err(|_| CatalogQueryError::InvalidStoredData("catalog item key"))?,
            Term::new(
                self.item_kind_namespace
                    .ok_or(CatalogQueryError::InvalidStoredData("catalog item kind"))?,
                self.item_kind_name
                    .ok_or(CatalogQueryError::InvalidStoredData("catalog item kind"))?,
            )
            .map_err(|_| CatalogQueryError::InvalidStoredData("catalog item kind"))?,
            self.item_display_name
                .ok_or(CatalogQueryError::InvalidStoredData("catalog item name"))?,
            self.item_attributes
                .ok_or(CatalogQueryError::InvalidStoredData(
                    "catalog item attributes",
                ))?,
            assets,
        )
        .map_err(|_| CatalogQueryError::InvalidStoredData("catalog item"))
    }
}

#[derive(FromRow)]
struct AssetRow {
    item_key_namespace: String,
    item_key_value: Vec<u8>,
    asset_key_namespace: String,
    asset_key_value: Vec<u8>,
    kind_namespace: String,
    kind_name: String,
}

#[derive(FromRow)]
struct StateRow {
    subject_kind: String,
    subject_key_namespace: String,
    subject_key_value: Vec<u8>,
    field_namespace: String,
    field_name: String,
    unit_namespace: String,
    unit_name: String,
    value: Value,
    provider_revision: Vec<u8>,
    observed_at: time::OffsetDateTime,
}

impl StateRow {
    fn into_observation(self) -> Result<StateObservation, CatalogQueryError> {
        let subject = match self.subject_kind.as_str() {
            "provider_item" => StateSubject::ProviderItem(
                ProviderItemKey::new(self.subject_key_namespace, self.subject_key_value)
                    .map_err(|_| CatalogQueryError::InvalidStoredData("state subject"))?,
            ),
            "catalog_relation" => StateSubject::CatalogRelation(
                CatalogRelationKey::new(self.subject_key_namespace, self.subject_key_value)
                    .map_err(|_| CatalogQueryError::InvalidStoredData("state subject"))?,
            ),
            _ => return Err(CatalogQueryError::InvalidStoredData("state subject kind")),
        };
        let unit = if self.unit_namespace.is_empty() && self.unit_name.is_empty() {
            None
        } else {
            Some(
                Term::new(self.unit_namespace, self.unit_name)
                    .map_err(|_| CatalogQueryError::InvalidStoredData("state unit"))?,
            )
        };
        StateObservation::set(
            subject,
            StateField::new(
                Term::new(self.field_namespace, self.field_name)
                    .map_err(|_| CatalogQueryError::InvalidStoredData("state field"))?,
                unit,
            ),
            decode_state_value(&self.value)?,
            self.provider_revision,
            self.observed_at,
        )
        .map_err(|_| CatalogQueryError::InvalidStoredData("state observation"))
    }
}

fn decode_state_value(value: &Value) -> Result<StateValue, CatalogQueryError> {
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or(CatalogQueryError::InvalidStoredData("state value kind"))?;
    let value = value
        .get("value")
        .ok_or(CatalogQueryError::InvalidStoredData("state value"))?;
    match kind {
        "boolean" => value
            .as_bool()
            .map(StateValue::Boolean)
            .ok_or(CatalogQueryError::InvalidStoredData("boolean state value")),
        "integer" => value
            .as_i64()
            .map(StateValue::Integer)
            .ok_or(CatalogQueryError::InvalidStoredData("integer state value")),
        "decimal" => value
            .as_str()
            .map(|value| StateValue::Decimal(value.to_owned()))
            .ok_or(CatalogQueryError::InvalidStoredData("decimal state value")),
        "text" => value
            .as_str()
            .map(|value| StateValue::Text(value.to_owned()))
            .ok_or(CatalogQueryError::InvalidStoredData("text state value")),
        "bytes" => value
            .as_str()
            .ok_or(CatalogQueryError::InvalidStoredData("bytes state value"))
            .and_then(|value| {
                hex::decode(value)
                    .map(StateValue::Bytes)
                    .map_err(|_| CatalogQueryError::InvalidStoredData("bytes state value"))
            }),
        "timestamp_milliseconds" => value.as_i64().map(StateValue::TimestampMilliseconds).ok_or(
            CatalogQueryError::InvalidStoredData("timestamp state value"),
        ),
        _ => Err(CatalogQueryError::InvalidStoredData("state value kind")),
    }
}

#[derive(FromRow)]
struct StreamRow {
    status: String,
    publication_id: Option<Uuid>,
}

#[derive(Debug, Error)]
pub enum CatalogQueryError {
    #[error("catalog page limit must be between 1 and {MAX_PAGE_SIZE}")]
    InvalidLimit,
    #[error("catalog cursor is invalid for this query")]
    InvalidCursor,
    #[error("catalog cursor expired after publication changed")]
    CursorExpired,
    #[error("catalog cursor could not be encoded")]
    CursorEncoding,
    #[error("source is not attached to the workspace")]
    SourceNotAttached,
    #[error("{0} stream was not found")]
    StreamNotFound(&'static str),
    #[error("{0} stream has no available publication")]
    StreamUnavailable(&'static str),
    #[error("published provider data is invalid: {0}")]
    InvalidStoredData(&'static str),
    #[error("catalog query database operation failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl IntoApplicationError for CatalogQueryError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::InvalidLimit => ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "catalog query is invalid",
                "limit",
                "out_of_range",
                "catalog page limit must be between 1 and 100",
            ),
            Self::InvalidCursor => ApplicationError::new(
                ApplicationErrorCode::CatalogCursorInvalid,
                "catalog cursor is invalid for this query",
            ),
            Self::CursorExpired => ApplicationError::new(
                ApplicationErrorCode::CatalogCursorExpired,
                "catalog cursor expired after publication changed",
            ),
            Self::CursorEncoding => ApplicationError::new(
                ApplicationErrorCode::Internal,
                "catalog cursor could not be created",
            ),
            Self::SourceNotAttached => ApplicationError::new(
                ApplicationErrorCode::SourceNotAttached,
                "source is not attached to the workspace",
            ),
            Self::StreamNotFound(_) => ApplicationError::new(
                ApplicationErrorCode::StreamNotFound,
                "published source stream was not found",
            ),
            Self::StreamUnavailable(_) => ApplicationError::new(
                ApplicationErrorCode::StreamUnavailable,
                "published source stream is unavailable",
            ),
            Self::InvalidStoredData(_) => ApplicationError::new(
                ApplicationErrorCode::InvalidProviderData,
                "published provider data is invalid",
            ),
            Self::Database(_) => ApplicationError::new(
                ApplicationErrorCode::Internal,
                "published source query failed",
            ),
        }
    }
}
