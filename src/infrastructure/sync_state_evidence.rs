use std::collections::BTreeMap;

use serde_json::Value;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use trakkin_mapping::PortableEndpoint;
use uuid::Uuid;

use crate::{
    application::{
        catalog_queries::PublishedStreamStatus,
        error::{ApplicationError, ApplicationErrorCode, IntoApplicationError},
        sync_state_evidence::{
            DiscoverSyncContributorRoots, MAX_SYNC_CONTRIBUTOR_ROOTS,
            MAX_SYNC_STATE_EVIDENCE_SLOTS, QuerySyncStateEvidence, SyncContributorRoot,
            SyncContributorRootFuture, SyncContributorRootProjection, SyncStateEvidence,
            SyncStateEvidenceFuture, SyncStateEvidencePresence, SyncStateEvidenceProjection,
            SyncStateEvidenceRepository, SyncStateEvidenceSlot, SyncStatePublicationCoverage,
            SyncStatePublicationPin,
        },
    },
    domain::acquisition::{
        CatalogRelationKey, ProviderItemKey, StateField, StateSubject, StateValue, Term,
    },
};

const READ_ONLY_TRANSACTION_SQL: &str =
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY";

#[derive(Clone)]
pub struct PostgresSyncStateEvidenceRepository {
    pool: PgPool,
}

impl PostgresSyncStateEvidenceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn discover(
        &self,
        query: DiscoverSyncContributorRoots,
    ) -> Result<SyncContributorRootProjection, SyncStateEvidenceError> {
        let revision = validate_revision(query.policy_revision)?;
        let fields = encode_fields(query.semantic_fields)?;
        let field_namespaces = fields
            .iter()
            .map(|field| field.namespace().to_owned())
            .collect::<Vec<_>>();
        let field_names = fields
            .iter()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();
        let row_limit = i64::try_from(MAX_SYNC_CONTRIBUTOR_ROOTS + 1)
            .map_err(|_| SyncStateEvidenceError::InvalidSelection)?;

        let mut transaction = self.pool.begin().await?;
        sqlx::query(READ_ONLY_TRANSACTION_SQL)
            .execute(&mut *transaction)
            .await?;
        let publications = load_contributor_publications(
            &mut transaction,
            query.workspace_id,
            query.policy_id,
            revision,
        )
        .await?;
        let rows = sqlx::query_as::<_, ContributorRootRow>(
            r#"WITH requested_fields AS (
    SELECT *
    FROM UNNEST($4::TEXT [], $5::TEXT []) AS requested_fields (
        field_namespace,
        field_name
    )
),

current_evidence AS (
    SELECT
        observation.source_id,
        observation.subject_kind,
        observation.subject_key_namespace,
        observation.subject_key_value,
        observation.field_namespace,
        observation.field_name,
        observation.unit_namespace,
        observation.unit_name,
        'present'::TEXT AS presence,
        observation.value,
        observation.provider_revision,
        observation.observed_at,
        observation.publication_id
    FROM state_observations AS observation
    UNION ALL
    SELECT
        tombstone.source_id,
        tombstone.subject_kind,
        tombstone.subject_key_namespace,
        tombstone.subject_key_value,
        tombstone.field_namespace,
        tombstone.field_name,
        tombstone.unit_namespace,
        tombstone.unit_name,
        'deleted'::TEXT AS presence,
        NULL::JSONB AS value,
        tombstone.provider_revision,
        tombstone.observed_at,
        tombstone.publication_id
    FROM state_observation_tombstones AS tombstone
    WHERE NOT EXISTS (
        SELECT 1
        FROM state_observations AS observation
        WHERE
            observation.source_id = tombstone.source_id
            AND observation.subject_kind = tombstone.subject_kind
            AND observation.subject_key_namespace
            = tombstone.subject_key_namespace
            AND observation.subject_key_value = tombstone.subject_key_value
            AND observation.field_namespace = tombstone.field_namespace
            AND observation.field_name = tombstone.field_name
            AND observation.unit_namespace = tombstone.unit_namespace
            AND observation.unit_name = tombstone.unit_name
    )
)

SELECT
    evidence.source_id,
    evidence.subject_kind,
    evidence.subject_key_namespace,
    evidence.subject_key_value,
    evidence.field_namespace,
    evidence.field_name,
    evidence.unit_namespace,
    evidence.unit_name,
    evidence.presence,
    evidence.value,
    evidence.provider_revision,
    evidence.observed_at,
    reference.reference_namespace,
    reference.reference_value
FROM current_evidence AS evidence
INNER JOIN requested_fields
    ON
        requested_fields.field_namespace = evidence.field_namespace
        AND requested_fields.field_name = evidence.field_name
INNER JOIN sync_policy_source_roles AS role
    ON
        role.policy_id = $1
        AND role.workspace_id = $2
        AND role.revision = $3
        AND role.source_id = evidence.source_id
        AND role.role = 'contributor'
INNER JOIN workspace_sources AS attachment
    ON
        attachment.workspace_id = $2
        AND attachment.source_id = evidence.source_id
INNER JOIN source_streams AS state_stream
    ON
        state_stream.source_id = evidence.source_id
        AND state_stream.stream_kind = 'state'
        AND state_stream.status = 'available'
        AND state_stream.publication_id = evidence.publication_id
LEFT JOIN source_streams AS catalog_stream
    ON
        catalog_stream.source_id = evidence.source_id
        AND catalog_stream.stream_kind = 'catalog'
        AND catalog_stream.status = 'available'
LEFT JOIN catalog_relations AS relation
    ON
        evidence.subject_kind = 'catalog_relation'
        AND relation.source_id = evidence.source_id
        AND relation.key_namespace = evidence.subject_key_namespace
        AND relation.key_value = evidence.subject_key_value
        AND relation.publication_id = catalog_stream.publication_id
LEFT JOIN catalog_items AS item
    ON
        item.source_id = evidence.source_id
        AND item.publication_id = catalog_stream.publication_id
        AND (
            (
                evidence.subject_kind = 'provider_item'
                AND item.key_namespace = evidence.subject_key_namespace
                AND item.key_value = evidence.subject_key_value
            )
            OR (
                evidence.subject_kind = 'catalog_relation'
                AND item.key_namespace = relation.item_key_namespace
                AND item.key_value = relation.item_key_value
            )
        )
LEFT JOIN catalog_item_references AS reference
    ON
        reference.source_id = item.source_id
        AND reference.item_key_namespace = item.key_namespace
        AND reference.item_key_value = item.key_value
ORDER BY
    evidence.source_id,
    evidence.subject_kind,
    evidence.subject_key_namespace,
    evidence.subject_key_value,
    evidence.field_namespace,
    evidence.field_name,
    evidence.unit_namespace,
    evidence.unit_name,
    reference.reference_namespace,
    reference.reference_value
LIMIT $6
"#,
        )
        .bind(query.policy_id)
        .bind(query.workspace_id)
        .bind(revision)
        .bind(field_namespaces)
        .bind(field_names)
        .bind(row_limit)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        project_contributor_roots(publications, rows)
    }

    async fn query(
        &self,
        query: QuerySyncStateEvidence,
    ) -> Result<SyncStateEvidenceProjection, SyncStateEvidenceError> {
        let revision = i64::try_from(query.policy_revision)
            .ok()
            .filter(|revision| *revision > 0)
            .ok_or(SyncStateEvidenceError::InvalidSelection)?;
        let slots = encode_slots(query.slots)?;
        let source_ids = slots
            .iter()
            .map(|slot| slot.slot.source_id)
            .collect::<Vec<_>>();
        let subject_kinds = slots
            .iter()
            .map(|slot| slot.subject_kind.clone())
            .collect::<Vec<_>>();
        let subject_namespaces = slots
            .iter()
            .map(|slot| slot.subject_namespace.clone())
            .collect::<Vec<_>>();
        let subject_values = slots
            .iter()
            .map(|slot| slot.subject_value.clone())
            .collect::<Vec<_>>();
        let field_namespaces = slots
            .iter()
            .map(|slot| slot.field_namespace.clone())
            .collect::<Vec<_>>();
        let field_names = slots
            .iter()
            .map(|slot| slot.field_name.clone())
            .collect::<Vec<_>>();
        let unit_namespaces = slots
            .iter()
            .map(|slot| slot.unit_namespace.clone())
            .collect::<Vec<_>>();
        let unit_names = slots
            .iter()
            .map(|slot| slot.unit_name.clone())
            .collect::<Vec<_>>();

        let mut transaction = self.pool.begin().await?;
        sqlx::query(READ_ONLY_TRANSACTION_SQL)
            .execute(&mut *transaction)
            .await?;
        let rows = sqlx::query_as::<_, EvidenceRow>(
            r#"WITH requested AS (
    SELECT *
    FROM UNNEST(
        $4::UUID [],
        $5::TEXT [],
        $6::TEXT [],
        $7::BYTEA [],
        $8::TEXT [],
        $9::TEXT [],
        $10::TEXT [],
        $11::TEXT []
    ) WITH ORDINALITY AS requested (
        source_id,
        subject_kind,
        subject_namespace,
        subject_value,
        field_namespace,
        field_name,
        unit_namespace,
        unit_name,
        ordinal
    )
)

SELECT
    requested.ordinal,
    role.source_id AS contributor_source_id,
    attachment.source_id AS attached_source_id,
    stream.status AS stream_status,
    stream.publication_id AS stream_publication_id,
    stream.evidence_revision AS stream_evidence_revision,
    stream.last_success_at AS stream_last_success_at,
    coverage.baseline_publication_id AS coverage_baseline_publication_id,
    coverage.publication_id AS coverage_publication_id,
    coverage.evidence_revision AS coverage_evidence_revision,
    coverage.observed_at AS coverage_observed_at,
    observation.value AS observation_value,
    observation.provider_revision AS observation_provider_revision,
    observation.observed_at AS observation_observed_at,
    observation.publication_id AS observation_publication_id,
    tombstone.provider_revision AS tombstone_provider_revision,
    tombstone.observed_at AS tombstone_observed_at,
    tombstone.publication_id AS tombstone_publication_id
FROM requested
LEFT JOIN sync_policy_source_roles AS role
    ON
        role.policy_id = $1
        AND role.workspace_id = $2
        AND role.revision = $3
        AND role.source_id = requested.source_id
        AND role.role = 'contributor'
LEFT JOIN workspace_sources AS attachment
    ON
        attachment.workspace_id = $2
        AND attachment.source_id = requested.source_id
LEFT JOIN source_streams AS stream
    ON
        stream.source_id = requested.source_id
        AND stream.stream_kind = 'state'
LEFT JOIN state_publication_coverage AS coverage
    ON coverage.source_id = requested.source_id
LEFT JOIN state_observations AS observation
    ON
        observation.source_id = requested.source_id
        AND observation.subject_kind = requested.subject_kind
        AND observation.subject_key_namespace = requested.subject_namespace
        AND observation.subject_key_value = requested.subject_value
        AND observation.field_namespace = requested.field_namespace
        AND observation.field_name = requested.field_name
        AND observation.unit_namespace = requested.unit_namespace
        AND observation.unit_name = requested.unit_name
LEFT JOIN state_observation_tombstones AS tombstone
    ON
        tombstone.source_id = requested.source_id
        AND tombstone.subject_kind = requested.subject_kind
        AND tombstone.subject_key_namespace = requested.subject_namespace
        AND tombstone.subject_key_value = requested.subject_value
        AND tombstone.field_namespace = requested.field_namespace
        AND tombstone.field_name = requested.field_name
        AND tombstone.unit_namespace = requested.unit_namespace
        AND tombstone.unit_name = requested.unit_name
ORDER BY requested.ordinal
"#,
        )
        .bind(query.policy_id)
        .bind(query.workspace_id)
        .bind(revision)
        .bind(source_ids)
        .bind(subject_kinds)
        .bind(subject_namespaces)
        .bind(subject_values)
        .bind(field_namespaces)
        .bind(field_names)
        .bind(unit_namespaces)
        .bind(unit_names)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        project_rows(slots, rows)
    }
}

impl SyncStateEvidenceRepository for PostgresSyncStateEvidenceRepository {
    type Error = SyncStateEvidenceError;

    fn discover_sync_contributor_roots(
        &self,
        query: DiscoverSyncContributorRoots,
    ) -> SyncContributorRootFuture<'_, Self::Error> {
        Box::pin(self.discover(query))
    }

    fn query_sync_state_evidence(
        &self,
        query: QuerySyncStateEvidence,
    ) -> SyncStateEvidenceFuture<'_, Self::Error> {
        Box::pin(self.query(query))
    }
}

#[derive(Debug, Error)]
pub enum SyncStateEvidenceError {
    #[error("synchronization state evidence selection is invalid")]
    InvalidSelection,
    #[error("source {source_id} is not an attached contributor for the policy revision")]
    SourceNotContributor { source_id: Uuid },
    #[error("source {source_id} has no state stream")]
    StateStreamNotFound { source_id: Uuid },
    #[error("stored synchronization state evidence is invalid: {0}")]
    InvalidStoredData(&'static str),
    #[error("synchronization contributor root discovery exceeded its bound")]
    DiscoveryLimitExceeded,
    #[error("synchronization state evidence repository invariant failed")]
    RepositoryInvariant,
    #[error("synchronization state evidence query failed")]
    Sql(#[from] sqlx::Error),
}

impl IntoApplicationError for SyncStateEvidenceError {
    fn into_application_error(self) -> ApplicationError {
        match self {
            Self::SourceNotContributor { .. } => ApplicationError::new(
                ApplicationErrorCode::SourceNotAttached,
                "synchronization contributor source is not attached",
            ),
            Self::StateStreamNotFound { .. } => ApplicationError::new(
                ApplicationErrorCode::StreamNotFound,
                "synchronization contributor state stream was not found",
            ),
            Self::InvalidSelection | Self::InvalidStoredData(_) | Self::DiscoveryLimitExceeded => {
                ApplicationError::new(
                    ApplicationErrorCode::InvalidProviderData,
                    "synchronization state evidence is invalid",
                )
            }
            Self::RepositoryInvariant | Self::Sql(_) => ApplicationError::new(
                ApplicationErrorCode::Internal,
                "synchronization state evidence query failed",
            ),
        }
    }
}

fn validate_revision(revision: u64) -> Result<i64, SyncStateEvidenceError> {
    i64::try_from(revision)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or(SyncStateEvidenceError::InvalidSelection)
}

fn encode_fields(mut fields: Vec<Term>) -> Result<Vec<Term>, SyncStateEvidenceError> {
    if fields.is_empty() || fields.len() > MAX_SYNC_STATE_EVIDENCE_SLOTS {
        return Err(SyncStateEvidenceError::InvalidSelection);
    }
    fields.sort_unstable_by(|left, right| {
        (left.namespace(), left.name()).cmp(&(right.namespace(), right.name()))
    });
    if fields.windows(2).any(|fields| fields[0] == fields[1]) {
        return Err(SyncStateEvidenceError::InvalidSelection);
    }
    Ok(fields)
}

#[derive(FromRow)]
struct ContributorPublicationRow {
    source_id: Uuid,
    attached_source_id: Option<Uuid>,
    stream_status: Option<String>,
    stream_publication_id: Option<Uuid>,
    stream_evidence_revision: Option<Vec<u8>>,
    stream_last_success_at: Option<time::OffsetDateTime>,
    coverage_baseline_publication_id: Option<Uuid>,
    coverage_publication_id: Option<Uuid>,
    coverage_evidence_revision: Option<Vec<u8>>,
    coverage_observed_at: Option<time::OffsetDateTime>,
}

async fn load_contributor_publications(
    transaction: &mut Transaction<'_, Postgres>,
    workspace_id: Uuid,
    policy_id: Uuid,
    revision: i64,
) -> Result<Vec<SyncStatePublicationPin>, SyncStateEvidenceError> {
    let rows = sqlx::query_as::<_, ContributorPublicationRow>(
        "SELECT
    role.source_id,
    attachment.source_id AS attached_source_id,
    stream.status AS stream_status,
    stream.publication_id AS stream_publication_id,
    stream.evidence_revision AS stream_evidence_revision,
    stream.last_success_at AS stream_last_success_at,
    coverage.baseline_publication_id AS coverage_baseline_publication_id,
    coverage.publication_id AS coverage_publication_id,
    coverage.evidence_revision AS coverage_evidence_revision,
    coverage.observed_at AS coverage_observed_at
FROM sync_policy_source_roles AS role
LEFT JOIN workspace_sources AS attachment
    ON
        attachment.workspace_id = role.workspace_id
        AND attachment.source_id = role.source_id
LEFT JOIN source_streams AS stream
    ON
        stream.source_id = role.source_id
        AND stream.stream_kind = 'state'
LEFT JOIN state_publication_coverage AS coverage
    ON coverage.source_id = role.source_id
WHERE
    role.policy_id = $1
    AND role.workspace_id = $2
    AND role.revision = $3
    AND role.role = 'contributor'
ORDER BY role.source_id
",
    )
    .bind(policy_id)
    .bind(workspace_id)
    .bind(revision)
    .fetch_all(&mut **transaction)
    .await?;
    let mut publications = Vec::with_capacity(rows.len());
    for row in rows {
        if row.attached_source_id != Some(row.source_id) {
            return Err(SyncStateEvidenceError::SourceNotContributor {
                source_id: row.source_id,
            });
        }
        let status = parse_stream_status(row.stream_status.as_deref().ok_or(
            SyncStateEvidenceError::StateStreamNotFound {
                source_id: row.source_id,
            },
        )?)?;
        publications.push(SyncStatePublicationPin {
            source_id: row.source_id,
            status,
            publication_id: row.stream_publication_id,
            evidence_revision: row.stream_evidence_revision,
            last_success_at: row.stream_last_success_at,
            coverage: project_publication_coverage(
                row.coverage_baseline_publication_id,
                row.coverage_publication_id,
                row.coverage_evidence_revision,
                row.coverage_observed_at,
            )?,
        });
    }
    Ok(publications)
}

#[derive(FromRow)]
struct ContributorRootRow {
    source_id: Uuid,
    subject_kind: String,
    subject_key_namespace: String,
    subject_key_value: Vec<u8>,
    field_namespace: String,
    field_name: String,
    unit_namespace: String,
    unit_name: String,
    presence: String,
    value: Option<Value>,
    provider_revision: Vec<u8>,
    observed_at: time::OffsetDateTime,
    reference_namespace: Option<String>,
    reference_value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ContributorRootKey {
    source_id: Uuid,
    subject_kind: String,
    subject_key_namespace: String,
    subject_key_value: Vec<u8>,
    field_namespace: String,
    field_name: String,
    unit_namespace: String,
    unit_name: String,
}

fn project_contributor_roots(
    publications: Vec<SyncStatePublicationPin>,
    rows: Vec<ContributorRootRow>,
) -> Result<SyncContributorRootProjection, SyncStateEvidenceError> {
    if rows.len() > MAX_SYNC_CONTRIBUTOR_ROOTS {
        return Err(SyncStateEvidenceError::DiscoveryLimitExceeded);
    }
    let mut contributors = BTreeMap::<ContributorRootKey, SyncContributorRoot>::new();
    for row in rows {
        let key = ContributorRootKey {
            source_id: row.source_id,
            subject_kind: row.subject_kind.clone(),
            subject_key_namespace: row.subject_key_namespace.clone(),
            subject_key_value: row.subject_key_value.clone(),
            field_namespace: row.field_namespace.clone(),
            field_name: row.field_name.clone(),
            unit_namespace: row.unit_namespace.clone(),
            unit_name: row.unit_name.clone(),
        };
        let contributor = if let Some(contributor) = contributors.get_mut(&key) {
            contributor
        } else {
            if contributors.len() == MAX_SYNC_STATE_EVIDENCE_SLOTS {
                return Err(SyncStateEvidenceError::DiscoveryLimitExceeded);
            }
            let evidence = contributor_evidence(&row)?;
            contributors.entry(key).or_insert(SyncContributorRoot {
                evidence,
                roots: Vec::new(),
            })
        };
        match (row.reference_namespace, row.reference_value) {
            (Some(namespace), Some(value)) => contributor.roots.push(
                PortableEndpoint::from_parts(&namespace, &value, None)
                    .map_err(|_| SyncStateEvidenceError::InvalidStoredData("portable root"))?,
            ),
            (None, None) => {}
            _ => {
                return Err(SyncStateEvidenceError::InvalidStoredData("portable root"));
            }
        }
    }
    for contributor in contributors.values_mut() {
        contributor.roots.sort_unstable();
        contributor.roots.dedup();
    }
    Ok(SyncContributorRootProjection {
        publications,
        contributors: contributors.into_values().collect(),
    })
}

fn contributor_evidence(
    row: &ContributorRootRow,
) -> Result<SyncStateEvidence, SyncStateEvidenceError> {
    let subject = match row.subject_kind.as_str() {
        "provider_item" => StateSubject::ProviderItem(
            ProviderItemKey::new(
                row.subject_key_namespace.clone(),
                row.subject_key_value.clone(),
            )
            .map_err(|_| SyncStateEvidenceError::InvalidStoredData("state subject"))?,
        ),
        "catalog_relation" => StateSubject::CatalogRelation(
            CatalogRelationKey::new(
                row.subject_key_namespace.clone(),
                row.subject_key_value.clone(),
            )
            .map_err(|_| SyncStateEvidenceError::InvalidStoredData("state subject"))?,
        ),
        _ => {
            return Err(SyncStateEvidenceError::InvalidStoredData(
                "state subject kind",
            ));
        }
    };
    let unit = if row.unit_namespace.is_empty() && row.unit_name.is_empty() {
        None
    } else {
        Some(
            Term::new(row.unit_namespace.clone(), row.unit_name.clone())
                .map_err(|_| SyncStateEvidenceError::InvalidStoredData("state unit"))?,
        )
    };
    let (presence, value) = match row.presence.as_str() {
        "present" => (
            SyncStateEvidencePresence::Present,
            Some(decode_state_value(row.value.as_ref().ok_or(
                SyncStateEvidenceError::InvalidStoredData("state value"),
            )?)?),
        ),
        "deleted" if row.value.is_none() => (SyncStateEvidencePresence::Deleted, None),
        _ => {
            return Err(SyncStateEvidenceError::InvalidStoredData("state presence"));
        }
    };
    Ok(SyncStateEvidence {
        slot: SyncStateEvidenceSlot {
            source_id: row.source_id,
            subject,
            field: StateField::new(
                Term::new(row.field_namespace.clone(), row.field_name.clone())
                    .map_err(|_| SyncStateEvidenceError::InvalidStoredData("state field"))?,
                unit,
            ),
        },
        presence,
        value,
        provider_revision: Some(row.provider_revision.clone()),
        observed_at: Some(row.observed_at),
    })
}

struct EncodedSlot {
    slot: SyncStateEvidenceSlot,
    subject_kind: String,
    subject_namespace: String,
    subject_value: Vec<u8>,
    field_namespace: String,
    field_name: String,
    unit_namespace: String,
    unit_name: String,
}

fn encode_slots(
    slots: Vec<SyncStateEvidenceSlot>,
) -> Result<Vec<EncodedSlot>, SyncStateEvidenceError> {
    if slots.is_empty() || slots.len() > MAX_SYNC_STATE_EVIDENCE_SLOTS {
        return Err(SyncStateEvidenceError::InvalidSelection);
    }
    let mut encoded = slots.into_iter().map(EncodedSlot::new).collect::<Vec<_>>();
    encoded.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
    if encoded
        .windows(2)
        .any(|slots| slots[0].key() == slots[1].key())
    {
        return Err(SyncStateEvidenceError::InvalidSelection);
    }
    Ok(encoded)
}

impl EncodedSlot {
    fn new(slot: SyncStateEvidenceSlot) -> Self {
        let (subject_kind, subject_namespace, subject_value) = match &slot.subject {
            StateSubject::ProviderItem(key) => (
                "provider_item".to_owned(),
                key.namespace().to_owned(),
                key.value().to_vec(),
            ),
            StateSubject::CatalogRelation(key) => (
                "catalog_relation".to_owned(),
                key.namespace().to_owned(),
                key.value().to_vec(),
            ),
        };
        let unit_namespace = slot
            .field
            .unit()
            .map_or_else(String::new, |unit| unit.namespace().to_owned());
        let unit_name = slot
            .field
            .unit()
            .map_or_else(String::new, |unit| unit.name().to_owned());
        Self {
            subject_kind,
            subject_namespace,
            subject_value,
            field_namespace: slot.field.field().namespace().to_owned(),
            field_name: slot.field.field().name().to_owned(),
            unit_namespace,
            unit_name,
            slot,
        }
    }

    fn key(&self) -> (Uuid, &str, &str, &[u8], &str, &str, &str, &str) {
        (
            self.slot.source_id,
            &self.subject_kind,
            &self.subject_namespace,
            &self.subject_value,
            &self.field_namespace,
            &self.field_name,
            &self.unit_namespace,
            &self.unit_name,
        )
    }
}

#[derive(FromRow)]
struct EvidenceRow {
    ordinal: i64,
    contributor_source_id: Option<Uuid>,
    attached_source_id: Option<Uuid>,
    stream_status: Option<String>,
    stream_publication_id: Option<Uuid>,
    stream_evidence_revision: Option<Vec<u8>>,
    stream_last_success_at: Option<time::OffsetDateTime>,
    coverage_baseline_publication_id: Option<Uuid>,
    coverage_publication_id: Option<Uuid>,
    coverage_evidence_revision: Option<Vec<u8>>,
    coverage_observed_at: Option<time::OffsetDateTime>,
    observation_value: Option<Value>,
    observation_provider_revision: Option<Vec<u8>>,
    observation_observed_at: Option<time::OffsetDateTime>,
    observation_publication_id: Option<Uuid>,
    tombstone_provider_revision: Option<Vec<u8>>,
    tombstone_observed_at: Option<time::OffsetDateTime>,
    tombstone_publication_id: Option<Uuid>,
}

fn project_rows(
    slots: Vec<EncodedSlot>,
    rows: Vec<EvidenceRow>,
) -> Result<SyncStateEvidenceProjection, SyncStateEvidenceError> {
    if slots.len() != rows.len() {
        return Err(SyncStateEvidenceError::RepositoryInvariant);
    }
    let mut publications = BTreeMap::new();
    let mut evidence = Vec::with_capacity(rows.len());
    for (index, (slot, row)) in slots.into_iter().zip(rows).enumerate() {
        if row.ordinal != i64::try_from(index + 1).unwrap_or(i64::MAX) {
            return Err(SyncStateEvidenceError::RepositoryInvariant);
        }
        let source_id = slot.slot.source_id;
        if row.contributor_source_id != Some(source_id) || row.attached_source_id != Some(source_id)
        {
            return Err(SyncStateEvidenceError::SourceNotContributor { source_id });
        }
        let status = parse_stream_status(
            row.stream_status
                .as_deref()
                .ok_or(SyncStateEvidenceError::StateStreamNotFound { source_id })?,
        )?;
        let coverage = project_coverage(&row)?;
        let pin = SyncStatePublicationPin {
            source_id,
            status,
            publication_id: row.stream_publication_id,
            evidence_revision: row.stream_evidence_revision.clone(),
            last_success_at: row.stream_last_success_at,
            coverage,
        };
        if publications
            .insert(source_id, pin.clone())
            .is_some_and(|stored| stored != pin)
        {
            return Err(SyncStateEvidenceError::RepositoryInvariant);
        }
        let current_publication = (status == PublishedStreamStatus::Available)
            .then_some(row.stream_publication_id)
            .flatten();
        let observation_current =
            current_publication.is_some() && row.observation_publication_id == current_publication;
        let tombstone_current =
            current_publication.is_some() && row.tombstone_publication_id == current_publication;
        let coverage_current =
            current_publication.is_some() && row.coverage_publication_id == current_publication;
        let (presence, value, provider_revision, observed_at) = if observation_current {
            (
                SyncStateEvidencePresence::Present,
                Some(decode_state_value(row.observation_value.as_ref().ok_or(
                    SyncStateEvidenceError::InvalidStoredData("state value"),
                )?)?),
                row.observation_provider_revision,
                row.observation_observed_at,
            )
        } else if tombstone_current {
            (
                SyncStateEvidencePresence::Deleted,
                None,
                row.tombstone_provider_revision,
                row.tombstone_observed_at,
            )
        } else if coverage_current {
            (SyncStateEvidencePresence::Absent, None, None, None)
        } else {
            (SyncStateEvidencePresence::Unknown, None, None, None)
        };
        evidence.push(SyncStateEvidence {
            slot: slot.slot,
            presence,
            value,
            provider_revision,
            observed_at,
        });
    }
    Ok(SyncStateEvidenceProjection {
        publications: publications.into_values().collect(),
        evidence,
    })
}

fn project_coverage(
    row: &EvidenceRow,
) -> Result<Option<SyncStatePublicationCoverage>, SyncStateEvidenceError> {
    project_publication_coverage(
        row.coverage_baseline_publication_id,
        row.coverage_publication_id,
        row.coverage_evidence_revision.clone(),
        row.coverage_observed_at,
    )
}

fn project_publication_coverage(
    baseline_publication_id: Option<Uuid>,
    publication_id: Option<Uuid>,
    evidence_revision: Option<Vec<u8>>,
    observed_at: Option<time::OffsetDateTime>,
) -> Result<Option<SyncStatePublicationCoverage>, SyncStateEvidenceError> {
    match (
        baseline_publication_id,
        publication_id,
        evidence_revision,
        observed_at,
    ) {
        (None, None, None, None) => Ok(None),
        (Some(baseline), Some(publication), Some(evidence_revision), Some(observed_at)) => {
            Ok(Some(SyncStatePublicationCoverage {
                baseline_publication_id: baseline,
                publication_id: publication,
                evidence_revision,
                observed_at,
            }))
        }
        _ => Err(SyncStateEvidenceError::InvalidStoredData("state coverage")),
    }
}

fn parse_stream_status(status: &str) -> Result<PublishedStreamStatus, SyncStateEvidenceError> {
    match status {
        "unavailable" => Ok(PublishedStreamStatus::Unavailable),
        "available" => Ok(PublishedStreamStatus::Available),
        "failed" => Ok(PublishedStreamStatus::Failed),
        _ => Err(SyncStateEvidenceError::InvalidStoredData(
            "state stream status",
        )),
    }
}

fn decode_state_value(value: &Value) -> Result<StateValue, SyncStateEvidenceError> {
    let kind = value.get("kind").and_then(Value::as_str).ok_or(
        SyncStateEvidenceError::InvalidStoredData("state value kind"),
    )?;
    let value = value
        .get("value")
        .ok_or(SyncStateEvidenceError::InvalidStoredData("state value"))?;
    match kind {
        "boolean" => value.as_bool().map(StateValue::Boolean).ok_or(
            SyncStateEvidenceError::InvalidStoredData("boolean state value"),
        ),
        "integer" => value.as_i64().map(StateValue::Integer).ok_or(
            SyncStateEvidenceError::InvalidStoredData("integer state value"),
        ),
        "decimal" => value
            .as_str()
            .map(|value| StateValue::Decimal(value.to_owned()))
            .ok_or(SyncStateEvidenceError::InvalidStoredData(
                "decimal state value",
            )),
        "text" => value
            .as_str()
            .map(|value| StateValue::Text(value.to_owned()))
            .ok_or(SyncStateEvidenceError::InvalidStoredData(
                "text state value",
            )),
        "bytes" => value
            .as_str()
            .ok_or(SyncStateEvidenceError::InvalidStoredData(
                "bytes state value",
            ))
            .and_then(|value| {
                hex::decode(value)
                    .map(StateValue::Bytes)
                    .map_err(|_| SyncStateEvidenceError::InvalidStoredData("bytes state value"))
            }),
        "timestamp_milliseconds" => value.as_i64().map(StateValue::TimestampMilliseconds).ok_or(
            SyncStateEvidenceError::InvalidStoredData("timestamp state value"),
        ),
        _ => Err(SyncStateEvidenceError::InvalidStoredData(
            "state value kind",
        )),
    }
}
