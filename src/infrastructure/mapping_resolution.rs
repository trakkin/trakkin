use serde_json::Value;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use trakkin_mapping::{MappingError, PortableEndpoint};
use uuid::Uuid;

use crate::application::error::{ApplicationError, ApplicationErrorCode, IntoApplicationError};
use crate::application::mapping_resolution::{
    CoordinateBacking, DirectFrontierRow, DirectFrontierSnapshot, DirectResolutionPins,
    ExactMappingRevision, FrontierEndpoint, LoadDirectFrontier, MappingResolutionFuture,
    MappingResolutionRepository, PublishedReferenceMatch, ResolutionRevisionPin,
    ResolutionSourcePin,
};

#[derive(Clone)]
pub struct PostgresMappingResolutionRepository {
    pool: PgPool,
}

impl PostgresMappingResolutionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn load(
        &self,
        request: LoadDirectFrontier,
    ) -> Result<DirectFrontierSnapshot, PostgresMappingResolutionRepositoryError> {
        let namespaces = request
            .references
            .iter()
            .map(|endpoint| endpoint.namespace().to_owned())
            .collect::<Vec<_>>();
        let values = request
            .references
            .iter()
            .map(|endpoint| endpoint.value().to_vec())
            .collect::<Vec<_>>();
        let selectors = request
            .references
            .iter()
            .map(|endpoint| {
                endpoint
                    .selector()
                    .map(|selector| selector.as_str().to_owned())
            })
            .collect::<Vec<_>>();
        let window_coordinate = request
            .window
            .as_ref()
            .map(|window| window.coordinate.as_str());
        let window_start = request
            .window
            .as_ref()
            .map(|window| i64::try_from(window.start).expect("portable ordinal window fits i64"));
        let window_end = request
            .window
            .as_ref()
            .map(|window| i64::try_from(window.end).expect("portable ordinal window fits i64"));
        let mapping_id = request
            .revision
            .as_ref()
            .map(|revision| revision.mapping_id);
        let content_hash = request
            .revision
            .as_ref()
            .map(|revision| revision.content_hash.as_str());
        let mut transaction = self.pool.begin().await?;
        sqlx::query(READ_ONLY_TRANSACTION_SQL)
            .execute(&mut *transaction)
            .await?;

        let records = sqlx::query_as::<_, DirectFrontierRecord>(DIRECT_FRONTIER_SQL)
            .bind(request.workspace_id)
            .bind(&namespaces)
            .bind(&values)
            .bind(&selectors)
            .bind(window_coordinate)
            .bind(window_start)
            .bind(window_end)
            .bind(mapping_id)
            .bind(content_hash)
            .fetch_all(&mut *transaction)
            .await?;
        let mut rows = Vec::with_capacity(records.len());
        for record in records {
            let endpoint_text = serde_json::from_value::<Vec<String>>(record.canonical_row)?;
            if endpoint_text.len() != record.endpoint_hashes.len() {
                return Err(PostgresMappingResolutionRepositoryError::StoredRowShape);
            }
            let endpoints = endpoint_text
                .into_iter()
                .zip(record.endpoint_hashes)
                .enumerate()
                .map(|(endpoint_ordinal, (endpoint, endpoint_hash))| {
                    Ok(FrontierEndpoint {
                        endpoint: PortableEndpoint::parse(&endpoint)?,
                        endpoint_hash,
                        endpoint_ordinal: i32::try_from(endpoint_ordinal)
                            .expect("mapping endpoint bound fits i32"),
                    })
                })
                .collect::<Result<Vec<_>, MappingError>>()?;
            rows.push(DirectFrontierRow {
                pin: ResolutionRevisionPin {
                    mapping_id: record.mapping_id,
                    content_hash: record.content_hash,
                    activation_generation: record.activation_generation,
                },
                row_hash: record.row_hash,
                row_ordinal: record.row_ordinal,
                requested_endpoint: request
                    .references
                    .get(usize::try_from(record.request_ordinal - 1).map_err(|_| {
                        PostgresMappingResolutionRepositoryError::StoredRequestOrdinal
                    })?)
                    .cloned()
                    .ok_or(PostgresMappingResolutionRepositoryError::StoredRequestOrdinal)?,
                matched_endpoint_hash: record.matched_endpoint_hash,
                endpoints,
            });
        }

        let published_matches = sqlx::query_as::<_, PublishedMatchRecord>(PUBLISHED_MATCH_SQL)
            .bind(request.workspace_id)
            .bind(&namespaces)
            .bind(&values)
            .bind(&selectors)
            .fetch_all(&mut *transaction)
            .await?
            .into_iter()
            .map(PublishedMatchRecord::into_match)
            .collect::<Result<Vec<_>, _>>()?;
        let pins = load_resolution_pins(
            &mut transaction,
            request.workspace_id,
            request.revision.as_ref(),
        )
        .await?;
        transaction.commit().await?;

        Ok(DirectFrontierSnapshot {
            rows,
            published_matches,
            pins,
        })
    }

    async fn recheck(
        &self,
        workspace_id: Uuid,
        revision: Option<ExactMappingRevision>,
        expected: DirectResolutionPins,
    ) -> Result<bool, PostgresMappingResolutionRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(READ_ONLY_TRANSACTION_SQL)
            .execute(&mut *transaction)
            .await?;
        let current =
            load_resolution_pins(&mut transaction, workspace_id, revision.as_ref()).await?;
        transaction.commit().await?;
        Ok(current == expected)
    }
}

impl MappingResolutionRepository for PostgresMappingResolutionRepository {
    type Error = PostgresMappingResolutionRepositoryError;

    fn load_direct_frontier(
        &self,
        request: LoadDirectFrontier,
    ) -> MappingResolutionFuture<'_, DirectFrontierSnapshot, Self::Error> {
        Box::pin(self.load(request))
    }

    fn recheck_direct_resolution(
        &self,
        workspace_id: Uuid,
        revision: Option<ExactMappingRevision>,
        expected: DirectResolutionPins,
    ) -> MappingResolutionFuture<'_, bool, Self::Error> {
        Box::pin(self.recheck(workspace_id, revision, expected))
    }
}

#[derive(Debug, Error)]
pub enum PostgresMappingResolutionRepositoryError {
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("stored mapping endpoint is invalid")]
    StoredEndpoint(#[from] MappingError),
    #[error("stored mapping row is invalid")]
    StoredRow(#[from] serde_json::Error),
    #[error("stored mapping row endpoint hashes do not match its endpoints")]
    StoredRowShape,
    #[error("stored direct frontier request ordinal is invalid")]
    StoredRequestOrdinal,
    #[error("stored coordinate backing is invalid")]
    StoredCoordinateBacking,
}

impl IntoApplicationError for PostgresMappingResolutionRepositoryError {
    fn into_application_error(self) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorCode::Internal,
            "mapping resolution operation failed",
        )
    }
}

#[derive(FromRow)]
struct DirectFrontierRecord {
    mapping_id: Uuid,
    content_hash: String,
    activation_generation: Option<i64>,
    row_hash: String,
    row_ordinal: i32,
    request_ordinal: i64,
    matched_endpoint_hash: String,
    canonical_row: Value,
    endpoint_hashes: Vec<String>,
}

#[derive(FromRow)]
struct PublishedMatchRecord {
    reference_namespace: String,
    reference_value: Vec<u8>,
    selector: Option<String>,
    source_id: Uuid,
    item_key_namespace: String,
    item_key_value: Vec<u8>,
    catalog_publication_id: Uuid,
    coordinate_publication_id: Option<Uuid>,
    coordinate_evidence_revision: Option<Vec<u8>>,
    coordinate_observed_at: Option<time::OffsetDateTime>,
    coordinate_expires_at: Option<time::OffsetDateTime>,
    backing: Option<String>,
}

#[derive(FromRow)]
struct ResolutionSourcePinRecord {
    source_id: Uuid,
    connection_id: Uuid,
    attached_at: time::OffsetDateTime,
    source_removed: bool,
    source_capability_revision: i64,
    connection_ready: bool,
    connection_deleted: bool,
    connection_capability_revision: i64,
    catalog_available: bool,
    catalog_publication_id: Option<Uuid>,
    catalog_evidence_revision: Option<Vec<u8>>,
    coordinate_publication_id: Option<Uuid>,
    coordinate_evidence_revision: Option<Vec<u8>>,
    coordinate_expires_at: Option<time::OffsetDateTime>,
}

impl From<ResolutionSourcePinRecord> for ResolutionSourcePin {
    fn from(record: ResolutionSourcePinRecord) -> Self {
        Self {
            source_id: record.source_id,
            connection_id: record.connection_id,
            attached_at: record.attached_at,
            source_removed: record.source_removed,
            source_capability_revision: record.source_capability_revision,
            connection_ready: record.connection_ready,
            connection_deleted: record.connection_deleted,
            connection_capability_revision: record.connection_capability_revision,
            catalog_available: record.catalog_available,
            catalog_publication_id: record.catalog_publication_id,
            catalog_evidence_revision: record.catalog_evidence_revision,
            coordinate_publication_id: record.coordinate_publication_id,
            coordinate_evidence_revision: record.coordinate_evidence_revision,
            coordinate_expires_at: record.coordinate_expires_at,
        }
    }
}

impl PublishedMatchRecord {
    fn into_match(
        self,
    ) -> Result<PublishedReferenceMatch, PostgresMappingResolutionRepositoryError> {
        let endpoint = PortableEndpoint::from_parts(
            &self.reference_namespace,
            &self.reference_value,
            self.selector.as_deref(),
        )?;
        let backing = self
            .backing
            .map(|backing| match backing.as_str() {
                "materialized" => Ok(CoordinateBacking::Materialized),
                "virtual" => Ok(CoordinateBacking::Virtual),
                "aggregate" => Ok(CoordinateBacking::Aggregate),
                _ => Err(PostgresMappingResolutionRepositoryError::StoredCoordinateBacking),
            })
            .transpose()?;
        Ok(PublishedReferenceMatch {
            endpoint,
            source_id: self.source_id,
            item_key_namespace: self.item_key_namespace,
            item_key_value: self.item_key_value,
            catalog_publication_id: self.catalog_publication_id,
            coordinate_publication_id: self.coordinate_publication_id,
            coordinate_evidence_revision: self.coordinate_evidence_revision,
            coordinate_observed_at: self.coordinate_observed_at,
            coordinate_expires_at: self.coordinate_expires_at,
            backing,
        })
    }
}

const READ_ONLY_TRANSACTION_SQL: &str =
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY";

async fn load_resolution_pins(
    transaction: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    revision: Option<&ExactMappingRevision>,
) -> Result<DirectResolutionPins, sqlx::Error> {
    let revisions = if let Some(revision) = revision {
        sqlx::query_as::<_, (Uuid, String, Option<i64>)>(
            "SELECT mapping_id, content_hash, NULL::BIGINT
FROM mapping_record_revisions
WHERE mapping_id = $1 AND content_hash = $2
",
        )
        .bind(revision.mapping_id)
        .bind(&revision.content_hash)
        .fetch_all(&mut **transaction)
        .await?
    } else {
        sqlx::query_as::<_, (Uuid, String, Option<i64>)>(
            "SELECT mapping_id, active_content_hash, generation
FROM workspace_mapping_activations
WHERE workspace_id = $1 AND active_content_hash IS NOT NULL
ORDER BY mapping_id
",
        )
        .bind(workspace_id)
        .fetch_all(&mut **transaction)
        .await?
    }
    .into_iter()
    .map(
        |(mapping_id, content_hash, activation_generation)| ResolutionRevisionPin {
            mapping_id,
            content_hash,
            activation_generation,
        },
    )
    .collect();
    let sources = sqlx::query_as::<_, ResolutionSourcePinRecord>(
        "SELECT
    source.id AS source_id,
    source.connection_id,
    attachment.attached_at,
    source.availability = 'removed' AS source_removed,
    source.capability_revision AS source_capability_revision,
    connection.status = 'ready' AS connection_ready,
    connection.deleted_at IS NOT NULL AS connection_deleted,
    connection.capability_revision AS connection_capability_revision,
    COALESCE(catalog.status = 'available', FALSE) AS catalog_available,
    catalog.publication_id AS catalog_publication_id,
    catalog.evidence_revision AS catalog_evidence_revision,
    coordinate.publication_id AS coordinate_publication_id,
    coordinate.evidence_revision AS coordinate_evidence_revision,
    coordinate.expires_at AS coordinate_expires_at
FROM workspace_sources AS attachment
INNER JOIN sources AS source ON source.id = attachment.source_id
INNER JOIN connections AS connection ON connection.id = source.connection_id
LEFT JOIN source_streams AS catalog
    ON catalog.source_id = source.id AND catalog.stream_kind = 'catalog'
LEFT JOIN source_coordinate_publications AS coordinate
    ON coordinate.source_id = source.id
WHERE attachment.workspace_id = $1
ORDER BY source.id
",
    )
    .bind(workspace_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(ResolutionSourcePin::from)
    .collect();
    Ok(DirectResolutionPins { revisions, sources })
}

const DIRECT_FRONTIER_SQL: &str = r#"
WITH requested AS (
    SELECT reference_namespace, reference_value, selector, request_ordinal
    FROM UNNEST($2::TEXT[], $3::BYTEA[], $4::TEXT[]) WITH ORDINALITY
        AS input(reference_namespace, reference_value, selector, request_ordinal)
), matched_endpoints AS (
    SELECT
        requested.request_ordinal,
        matching.content_hash,
        matching.row_hash,
        matching.endpoint_hash
    FROM requested
    INNER JOIN mapping_row_endpoints AS matching
        ON matching.reference_namespace = requested.reference_namespace
        AND matching.reference_value = requested.reference_value
        AND matching.selector IS NOT DISTINCT FROM requested.selector

    UNION ALL

    SELECT
        requested.request_ordinal,
        matching.content_hash,
        matching.row_hash,
        matching.endpoint_hash
    FROM requested
    INNER JOIN mapping_row_endpoints AS matching
        ON matching.reference_namespace = requested.reference_namespace
        AND matching.reference_value = requested.reference_value
        AND matching.ordinal_coordinate IS NOT NULL
        AND (
            $5::TEXT IS NULL
            OR (
                matching.ordinal_coordinate = $5
                AND matching.ordinal_start <= $7
                AND matching.ordinal_end >= $6
            )
        )
    WHERE requested.selector IS NULL
), matched_rows AS (
    SELECT
        selected.mapping_id,
        selected.content_hash,
        selected.activation_generation,
        rows.row_hash,
        rows.row_ordinal,
        matching.request_ordinal,
        matching.endpoint_hash AS matched_endpoint_hash,
        rows.canonical_row
    FROM matched_endpoints AS matching
    INNER JOIN (
        SELECT
            activation.mapping_id,
            activation.active_content_hash AS content_hash,
            activation.generation AS activation_generation
        FROM workspace_mapping_activations AS activation
        WHERE activation.workspace_id = $1
            AND activation.active_content_hash IS NOT NULL
            AND $8::UUID IS NULL

        UNION ALL

        SELECT
            record.mapping_id,
            record.content_hash,
            NULL::BIGINT AS activation_generation
        FROM mapping_record_revisions AS record
        WHERE record.mapping_id = $8
            AND record.content_hash = $9
    ) AS selected ON selected.content_hash = matching.content_hash
    INNER JOIN mapping_rows AS rows
        ON rows.content_hash = matching.content_hash
        AND rows.row_hash = matching.row_hash
)
SELECT
    matched.mapping_id,
    matched.content_hash,
    matched.activation_generation,
    matched.row_hash,
    matched.row_ordinal,
    matched.request_ordinal,
    matched.matched_endpoint_hash,
    matched.canonical_row,
    ARRAY_AGG(endpoint.endpoint_hash ORDER BY endpoint.endpoint_ordinal) AS endpoint_hashes
FROM matched_rows AS matched
INNER JOIN mapping_row_endpoints AS endpoint
    ON endpoint.content_hash = matched.content_hash
    AND endpoint.row_hash = matched.row_hash
GROUP BY
    matched.mapping_id,
    matched.content_hash,
    matched.activation_generation,
    matched.row_hash,
    matched.row_ordinal,
    matched.request_ordinal,
    matched.matched_endpoint_hash,
    matched.canonical_row
ORDER BY
    matched.content_hash,
    matched.row_hash,
    matched.request_ordinal,
    matched.matched_endpoint_hash
"#;

const PUBLISHED_MATCH_SQL: &str = r#"
WITH requested(reference_namespace, reference_value, selector) AS (
    SELECT *
    FROM UNNEST($2::TEXT[], $3::BYTEA[], $4::TEXT[])
)
SELECT
    requested.reference_namespace,
    requested.reference_value,
    requested.selector,
    item.source_id,
    item.key_namespace AS item_key_namespace,
    item.key_value AS item_key_value,
    item.publication_id AS catalog_publication_id,
    coordinate.publication_id AS coordinate_publication_id,
    coordinate_publication.evidence_revision AS coordinate_evidence_revision,
    coordinate.observed_at AS coordinate_observed_at,
    LEAST(coordinate.expires_at, coordinate_publication.expires_at)
        AS coordinate_expires_at,
    coordinate.backing
FROM requested
INNER JOIN catalog_item_references AS reference
    ON reference.reference_namespace = requested.reference_namespace
    AND reference.reference_value = requested.reference_value
INNER JOIN catalog_items AS item
    ON item.source_id = reference.source_id
    AND item.key_namespace = reference.item_key_namespace
    AND item.key_value = reference.item_key_value
INNER JOIN workspace_sources AS attached
    ON attached.workspace_id = $1
    AND attached.source_id = item.source_id
LEFT JOIN source_coordinate_bindings AS coordinate
    ON coordinate.source_id = item.source_id
    AND coordinate.item_key_namespace = item.key_namespace
    AND coordinate.item_key_value = item.key_value
    AND coordinate.reference_namespace = requested.reference_namespace
    AND coordinate.reference_value = requested.reference_value
    AND coordinate.selector = COALESCE(requested.selector, '')
LEFT JOIN source_coordinate_publications AS coordinate_publication
    ON coordinate_publication.source_id = coordinate.source_id
    AND coordinate_publication.publication_id = coordinate.publication_id
ORDER BY
    requested.reference_namespace,
    requested.reference_value,
    requested.selector,
    item.publication_id,
    coordinate.publication_id,
    item.source_id,
    item.key_namespace,
    item.key_value
"#;

#[cfg(all(test, feature = "embedded-database"))]
mod tests {
    use serde_json::Value;
    use sqlx::AssertSqlSafe;
    use tempfile::tempdir;
    use time::OffsetDateTime;

    use super::DIRECT_FRONTIER_SQL;
    use crate::database::DatabaseRuntime;

    #[tokio::test]
    async fn direct_frontier_plan_uses_endpoint_indexes() {
        let directory = tempdir().expect("temporary directory is created");
        let database = DatabaseRuntime::start(directory.path())
            .await
            .expect("fresh database starts");
        let pool = database.pool();
        let workspace_id = uuid::Uuid::from_u128(1);
        let mapping_id = uuid::Uuid::from_u128(2);
        let now = OffsetDateTime::from_unix_timestamp(1_893_456_245).expect("fixture time");
        let content_hash = format!("sha256:{}", "a".repeat(64));
        let semantic_fingerprint = format!("sha256:{}", "b".repeat(64));

        let mut transaction = pool.begin().await.expect("seed transaction starts");
        sqlx::query(
            "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES ($1, 'Resolution plan', 'active', $2, $2)",
        )
        .bind(workspace_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .expect("workspace is seeded");
        sqlx::query(
            "INSERT INTO mapping_revisions (
    content_hash, semantic_fingerprint, schema_name, portable_revision,
    canonical_json, created_at
) VALUES ($1, $2, 'trakkin.mapping/v1', 1, $3, $4)",
        )
        .bind(&content_hash)
        .bind(&semantic_fingerprint)
        .bind(b"{}".as_slice())
        .bind(now)
        .execute(&mut *transaction)
        .await
        .expect("mapping revision is seeded");
        sqlx::query(
            "INSERT INTO mappings (id, head_content_hash, created_at, updated_at)
VALUES ($1, $2, $3, $3)",
        )
        .bind(mapping_id)
        .bind(&content_hash)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .expect("mapping is seeded");
        sqlx::query(
            "INSERT INTO mapping_record_revisions (
    mapping_id, content_hash, lineage_ordinal, origin, created_at
) VALUES ($1, $2, 1, 'created', $3)",
        )
        .bind(mapping_id)
        .bind(&content_hash)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .expect("mapping lineage is seeded");
        sqlx::query(
            "INSERT INTO workspace_mapping_activations (
    workspace_id, mapping_id, active_content_hash, generation, updated_at
) VALUES ($1, $2, $3, 1, $4)",
        )
        .bind(workspace_id)
        .bind(mapping_id)
        .bind(&content_hash)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .expect("mapping activation is seeded");
        sqlx::query(
            "INSERT INTO mapping_rows (
    content_hash, row_ordinal, row_hash, canonical_row
)
SELECT
    $1,
    ordinal - 1,
    'sha256:' || LPAD(TO_HEX(ordinal), 64, '0'),
    JSONB_BUILD_ARRAY(
        'example.media:node-' || ordinal,
        'example.media:peer-' || ordinal
    )
FROM GENERATE_SERIES(1, 10000) AS ordinal",
        )
        .bind(&content_hash)
        .execute(&mut *transaction)
        .await
        .expect("mapping rows are seeded");
        sqlx::query(
            "INSERT INTO mapping_row_endpoints (
    content_hash, row_hash, endpoint_ordinal, endpoint_hash,
    reference_namespace, reference_value, selector, coordinate_ids,
    ordinal_coordinate, ordinal_start, ordinal_end
)
SELECT
    $1,
    'sha256:' || LPAD(TO_HEX(ordinal), 64, '0'),
    endpoint_ordinal,
    'sha256:' || LPAD(TO_HEX((ordinal * 2) + endpoint_ordinal), 64, '0'),
    'example.media',
    CASE
        WHEN endpoint_ordinal = 0 THEN CONVERT_TO('node-' || ordinal, 'UTF8')
        ELSE CONVERT_TO('dense', 'UTF8')
    END,
    CASE
        WHEN endpoint_ordinal = 0 THEN NULL
        ELSE 'episode:' || ordinal || '..' || (ordinal + 100)
    END,
    CASE
        WHEN endpoint_ordinal = 0 THEN ARRAY[]::TEXT[]
        ELSE ARRAY['episode']::TEXT[]
    END,
    CASE WHEN endpoint_ordinal = 1 THEN 'episode' ELSE NULL END,
    CASE WHEN endpoint_ordinal = 1 THEN ordinal ELSE NULL END,
    CASE WHEN endpoint_ordinal = 1 THEN ordinal + 100 ELSE NULL END
FROM GENERATE_SERIES(1, 10000) AS ordinal
CROSS JOIN (VALUES (0), (1)) AS endpoint(endpoint_ordinal)",
        )
        .bind(&content_hash)
        .execute(&mut *transaction)
        .await
        .expect("mapping endpoints are seeded");
        transaction
            .commit()
            .await
            .expect("seed transaction commits");
        sqlx::query("ANALYZE mapping_row_endpoints")
            .execute(&pool)
            .await
            .expect("endpoint statistics are current");

        let statement = format!("EXPLAIN (FORMAT JSON) {DIRECT_FRONTIER_SQL}");
        let plan = sqlx::query_scalar::<_, Value>(AssertSqlSafe(statement))
            .bind(workspace_id)
            .bind(vec!["example.media"])
            .bind(vec![b"node-5000".to_vec()])
            .bind(vec![Option::<String>::None])
            .bind(Option::<String>::None)
            .bind(Option::<i64>::None)
            .bind(Option::<i64>::None)
            .bind(Option::<uuid::Uuid>::None)
            .bind(Option::<String>::None)
            .fetch_one(&pool)
            .await
            .expect("frontier query plan is available");
        let mut endpoint_scans = Vec::new();
        collect_endpoint_scans(&plan, &mut endpoint_scans);

        assert!(!endpoint_scans.is_empty(), "endpoint scans are visible");
        assert!(
            endpoint_scans
                .iter()
                .all(|scan| scan.node_type != "Seq Scan")
        );
        assert!(
            endpoint_scans.iter().any(|scan| {
                scan.alias.starts_with("matching")
                    && scan.index_name.as_deref() == Some("mapping_row_endpoints_reference_idx")
            }),
            "exact endpoint scans: {endpoint_scans:#?}"
        );

        let statement = format!("EXPLAIN (FORMAT JSON) {DIRECT_FRONTIER_SQL}");
        let finite_plan = sqlx::query_scalar::<_, Value>(AssertSqlSafe(statement))
            .bind(workspace_id)
            .bind(vec!["example.media"])
            .bind(vec![b"dense".to_vec()])
            .bind(vec![Option::<String>::None])
            .bind(Some("episode"))
            .bind(Some(5_000_i64))
            .bind(Some(5_100_i64))
            .bind(Option::<uuid::Uuid>::None)
            .bind(Option::<String>::None)
            .fetch_one(&pool)
            .await
            .expect("finite frontier query plan is available");
        let mut finite_endpoint_scans = Vec::new();
        collect_endpoint_scans(&finite_plan, &mut finite_endpoint_scans);

        assert!(
            finite_endpoint_scans
                .iter()
                .all(|scan| scan.node_type != "Seq Scan")
        );
        assert!(
            finite_endpoint_scans.iter().any(|scan| {
                scan.alias.starts_with("matching")
                    && scan.index_name.as_deref()
                        == Some("mapping_row_endpoints_finite_ordinal_idx")
            }),
            "finite endpoint scans: {finite_endpoint_scans:#?}"
        );

        database
            .shutdown()
            .await
            .expect("fresh database shuts down");
    }

    #[derive(Debug)]
    struct RelationScan {
        node_type: String,
        alias: String,
        index_name: Option<String>,
    }

    fn collect_endpoint_scans(value: &Value, scans: &mut Vec<RelationScan>) {
        match value {
            Value::Array(values) => {
                for value in values {
                    collect_endpoint_scans(value, scans);
                }
            }
            Value::Object(object) => {
                if object.get("Relation Name").and_then(Value::as_str)
                    == Some("mapping_row_endpoints")
                {
                    scans.push(RelationScan {
                        node_type: object["Node Type"]
                            .as_str()
                            .expect("plan node type is a string")
                            .to_owned(),
                        alias: object["Alias"]
                            .as_str()
                            .expect("plan alias is a string")
                            .to_owned(),
                        index_name: object
                            .get("Index Name")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    });
                }
                for value in object.values() {
                    collect_endpoint_scans(value, scans);
                }
            }
            _ => {}
        }
    }
}
