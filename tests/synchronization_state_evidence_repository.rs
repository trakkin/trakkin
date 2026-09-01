#![cfg(feature = "embedded-database")]

use serde_json::json;
use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::{
        catalog_queries::PublishedStreamStatus,
        sync_state_evidence::{
            DiscoverSyncContributorRoots, QuerySyncStateEvidence, SyncStateEvidencePresence,
            SyncStateEvidenceRepository, SyncStateEvidenceSlot,
        },
    },
    database::DatabaseRuntime,
    domain::acquisition::{ProviderItemKey, StateField, StateSubject, StateValue, Term},
    infrastructure::sync_state_evidence::{
        PostgresSyncStateEvidenceRepository, SyncStateEvidenceError,
    },
};
use uuid::Uuid;

const CONNECTION_ID: Uuid = Uuid::from_u128(1);
const ACCOUNT_ID: Uuid = Uuid::from_u128(2);
const COVERED_SOURCE_ID: Uuid = Uuid::from_u128(3);
const UNCOVERED_SOURCE_ID: Uuid = Uuid::from_u128(4);
const OUTSIDE_SOURCE_ID: Uuid = Uuid::from_u128(5);
const WORKSPACE_ID: Uuid = Uuid::from_u128(6);
const POLICY_ID: Uuid = Uuid::from_u128(7);
const BASELINE_PUBLICATION_ID: Uuid = Uuid::from_u128(8);
const COVERED_PUBLICATION_ID: Uuid = Uuid::from_u128(9);
const UNCOVERED_PUBLICATION_ID: Uuid = Uuid::from_u128(10);
const CATALOG_PUBLICATION_ID: Uuid = Uuid::from_u128(11);

#[tokio::test]
async fn postgres_sync_state_evidence_preserves_presence_and_publication_pins() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_state_evidence(&database.pool()).await;
    let repository = PostgresSyncStateEvidenceRepository::new(database.pool());

    let roots = repository
        .discover_sync_contributor_roots(DiscoverSyncContributorRoots {
            workspace_id: WORKSPACE_ID,
            policy_id: POLICY_ID,
            policy_revision: 1,
            semantic_fields: vec![
                Term::new("trakkin.state", "progress").expect("semantic field is valid"),
            ],
        })
        .await
        .expect("synchronization contributor roots are discovered");
    assert_eq!(roots.publications.len(), 2);
    assert_eq!(roots.publications[0].source_id, COVERED_SOURCE_ID);
    assert_eq!(roots.publications[1].source_id, UNCOVERED_SOURCE_ID);
    assert_eq!(roots.contributors.len(), 1);
    assert_eq!(
        roots.contributors[0].evidence.slot.source_id,
        COVERED_SOURCE_ID
    );
    assert_eq!(
        roots.contributors[0].evidence.value,
        Some(StateValue::Integer(8))
    );
    assert_eq!(roots.contributors[0].roots.len(), 1);
    assert_eq!(
        roots.contributors[0].roots[0].as_str(),
        "example.media://media/quiet-signal"
    );

    let projection = repository
        .query_sync_state_evidence(QuerySyncStateEvidence {
            workspace_id: WORKSPACE_ID,
            policy_id: POLICY_ID,
            policy_revision: 1,
            slots: vec![
                slot(UNCOVERED_SOURCE_ID, "missing"),
                slot(COVERED_SOURCE_ID, "watched"),
                slot(COVERED_SOURCE_ID, "progress"),
                slot(COVERED_SOURCE_ID, "rating"),
            ],
        })
        .await
        .expect("synchronization state evidence is projected");

    assert_eq!(projection.publications.len(), 2);
    assert_eq!(projection.publications[0].source_id, COVERED_SOURCE_ID);
    assert_eq!(
        projection.publications[0].status,
        PublishedStreamStatus::Available
    );
    assert_eq!(
        projection.publications[0].publication_id,
        Some(COVERED_PUBLICATION_ID)
    );
    let coverage = projection.publications[0]
        .coverage
        .as_ref()
        .expect("full publication coverage is preserved");
    assert_eq!(coverage.baseline_publication_id, BASELINE_PUBLICATION_ID);
    assert_eq!(coverage.publication_id, COVERED_PUBLICATION_ID);
    assert_eq!(coverage.evidence_revision, b"covered-state-r2");
    assert!(projection.publications[1].coverage.is_none());
    assert_eq!(
        projection
            .evidence
            .iter()
            .map(|evidence| evidence.slot.field.field().name())
            .collect::<Vec<_>>(),
        ["progress", "rating", "watched", "missing"]
    );
    assert_eq!(
        projection
            .evidence
            .iter()
            .map(|evidence| evidence.presence)
            .collect::<Vec<_>>(),
        [
            SyncStateEvidencePresence::Present,
            SyncStateEvidencePresence::Absent,
            SyncStateEvidencePresence::Deleted,
            SyncStateEvidencePresence::Unknown,
        ]
    );
    assert_eq!(projection.evidence[0].value, Some(StateValue::Integer(8)));
    assert_eq!(
        projection.evidence[0].provider_revision.as_deref(),
        Some(b"progress-r2".as_slice())
    );
    assert_eq!(projection.evidence[1].value, None);
    assert_eq!(
        projection.evidence[2].provider_revision.as_deref(),
        Some(b"watched-r2".as_slice())
    );
    assert_eq!(projection.evidence[3].value, None);

    let error = repository
        .query_sync_state_evidence(QuerySyncStateEvidence {
            workspace_id: WORKSPACE_ID,
            policy_id: POLICY_ID,
            policy_revision: 1,
            slots: vec![slot(OUTSIDE_SOURCE_ID, "progress")],
        })
        .await
        .expect_err("a source outside the policy is rejected");
    assert!(matches!(
        error,
        SyncStateEvidenceError::SourceNotContributor {
            source_id: OUTSIDE_SOURCE_ID
        }
    ));

    database.shutdown().await.expect("database shuts down");
}

fn slot(source_id: Uuid, field_name: &str) -> SyncStateEvidenceSlot {
    SyncStateEvidenceSlot {
        source_id,
        subject: StateSubject::ProviderItem(
            ProviderItemKey::new("fixture.item", b"quiet-signal".to_vec())
                .expect("provider item key is valid"),
        ),
        field: StateField::new(
            Term::new("trakkin.state", field_name).expect("state field is valid"),
            Some(Term::new("trakkin.unit", "episode").expect("state unit is valid")),
        ),
    }
}

async fn seed_state_evidence(pool: &sqlx::PgPool) {
    let timestamp = timestamp(0);
    sqlx::query(
        "INSERT INTO connections (
    id, provider_id, display_name, status, capabilities,
    capability_revision, created_at, updated_at
) VALUES ($1, 'fixture', 'Fixture', 'ready', '{}'::JSONB, 1, $2, $2)",
    )
    .bind(CONNECTION_ID)
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("connection is seeded");
    sqlx::query(
        "INSERT INTO accounts (
    id, connection_id, provider_key_namespace, provider_key_value,
    display_name, first_seen_at, last_seen_at
) VALUES ($1, $2, 'fixture.account', 'account'::BYTEA, 'Fixture', $3, $3)",
    )
    .bind(ACCOUNT_ID)
    .bind(CONNECTION_ID)
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("account is seeded");
    for (source_id, source_key) in [
        (COVERED_SOURCE_ID, "covered"),
        (UNCOVERED_SOURCE_ID, "uncovered"),
        (OUTSIDE_SOURCE_ID, "outside"),
    ] {
        sqlx::query(
            "INSERT INTO sources (
    id, connection_id, account_id, provider_key_namespace, provider_key_value,
    display_name, kind_namespace, kind_name, availability, capabilities,
    capability_revision, first_seen_at, last_seen_at
) VALUES (
    $1, $2, $3, 'fixture.source', $4::BYTEA,
    $4, 'trakkin', 'media', 'available', '{}'::JSONB, 1, $5, $5
)",
        )
        .bind(source_id)
        .bind(CONNECTION_ID)
        .bind(ACCOUNT_ID)
        .bind(source_key.as_bytes())
        .bind(timestamp)
        .execute(pool)
        .await
        .expect("source is seeded");
    }
    sqlx::query(
        "INSERT INTO workspaces (id, name, created_at, updated_at)
VALUES ($1, 'Synchronization evidence', $2, $2)",
    )
    .bind(WORKSPACE_ID)
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("workspace is seeded");
    for source_id in [COVERED_SOURCE_ID, UNCOVERED_SOURCE_ID, OUTSIDE_SOURCE_ID] {
        sqlx::query(
            "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
VALUES ($1, $2, $3)",
        )
        .bind(WORKSPACE_ID)
        .bind(source_id)
        .bind(timestamp)
        .execute(pool)
        .await
        .expect("workspace source is seeded");
    }
    sqlx::query(
        "INSERT INTO sync_policies (
    id, workspace_id, head_revision, head_content_hash, created_at, updated_at
) VALUES ($1, $2, 1, $3, $4, $4)",
    )
    .bind(POLICY_ID)
    .bind(WORKSPACE_ID)
    .bind(valid_hash())
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("policy is seeded");
    sqlx::query(
        "INSERT INTO sync_policy_revisions (
    policy_id, workspace_id, revision, content_hash, schema_name,
    canonical_json, created_at
) VALUES ($1, $2, 1, $3, 'trakkin.sync-policy/v1', '{}'::BYTEA, $4)",
    )
    .bind(POLICY_ID)
    .bind(WORKSPACE_ID)
    .bind(valid_hash())
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("policy revision is seeded");
    for source_id in [COVERED_SOURCE_ID, UNCOVERED_SOURCE_ID] {
        sqlx::query(
            "INSERT INTO sync_policy_source_roles (
    policy_id, workspace_id, revision, source_id, role
) VALUES ($1, $2, 1, $3, 'contributor')",
        )
        .bind(POLICY_ID)
        .bind(WORKSPACE_ID)
        .bind(source_id)
        .execute(pool)
        .await
        .expect("policy contributor is seeded");
    }
    for (source_id, publication_id, evidence_revision) in [
        (
            COVERED_SOURCE_ID,
            COVERED_PUBLICATION_ID,
            b"covered-state-r2".as_slice(),
        ),
        (
            UNCOVERED_SOURCE_ID,
            UNCOVERED_PUBLICATION_ID,
            b"uncovered-state-r1".as_slice(),
        ),
    ] {
        sqlx::query(
            "INSERT INTO source_streams (
    source_id, stream_kind, status, publication_id, evidence_revision,
    last_success_at, updated_at
) VALUES ($1, 'state', 'available', $2, $3, $4, $4)",
        )
        .bind(source_id)
        .bind(publication_id)
        .bind(evidence_revision)
        .bind(timestamp)
        .execute(pool)
        .await
        .expect("state stream is seeded");
    }
    sqlx::query(
        "INSERT INTO source_streams (
    source_id, stream_kind, status, publication_id, evidence_revision,
    last_success_at, updated_at
) VALUES ($1, 'catalog', 'available', $2, 'covered-catalog-r1'::BYTEA, $3, $3)",
    )
    .bind(COVERED_SOURCE_ID)
    .bind(CATALOG_PUBLICATION_ID)
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("catalog stream is seeded");
    sqlx::query(
        "INSERT INTO catalog_items (
    source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, publication_id, updated_at
) VALUES (
    $1, 'fixture.item', 'quiet-signal'::BYTEA, 'trakkin', 'media',
    'Display names are not roots', '{}'::JSONB, $2, $3
)",
    )
    .bind(COVERED_SOURCE_ID)
    .bind(CATALOG_PUBLICATION_ID)
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("catalog item is seeded");
    sqlx::query(
        "INSERT INTO catalog_item_references (
    source_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value
) VALUES (
    $1, 'fixture.item', 'quiet-signal'::BYTEA,
    'example.media', 'media/quiet-signal'::BYTEA
)",
    )
    .bind(COVERED_SOURCE_ID)
    .execute(pool)
    .await
    .expect("portable reference is seeded");
    sqlx::query(
        "INSERT INTO state_publication_coverage (
    source_id, baseline_publication_id, publication_id, evidence_revision,
    observed_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $5)",
    )
    .bind(COVERED_SOURCE_ID)
    .bind(BASELINE_PUBLICATION_ID)
    .bind(COVERED_PUBLICATION_ID)
    .bind(b"covered-state-r2".as_slice())
    .bind(timestamp)
    .execute(pool)
    .await
    .expect("state coverage is seeded");
    sqlx::query(
        "INSERT INTO state_observations (
    source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    value, provider_revision, observed_at, publication_id, updated_at
) VALUES (
    $1, 'provider_item', 'fixture.item', 'quiet-signal'::BYTEA,
    'trakkin.state', 'progress', 'trakkin.unit', 'episode',
    $2, 'progress-r2'::BYTEA, $3, $4, $3
)",
    )
    .bind(COVERED_SOURCE_ID)
    .bind(json!({"kind": "integer", "value": 8}))
    .bind(timestamp)
    .bind(COVERED_PUBLICATION_ID)
    .execute(pool)
    .await
    .expect("present state observation is seeded");
    sqlx::query(
        "INSERT INTO state_observation_tombstones (
    source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    provider_revision, observed_at, publication_id, updated_at
) VALUES (
    $1, 'provider_item', 'fixture.item', 'quiet-signal'::BYTEA,
    'trakkin.state', 'watched', 'trakkin.unit', 'episode',
    'watched-r2'::BYTEA, $2, $3, $2
)",
    )
    .bind(COVERED_SOURCE_ID)
    .bind(timestamp)
    .bind(COVERED_PUBLICATION_ID)
    .execute(pool)
    .await
    .expect("deleted state observation is seeded");
}

fn valid_hash() -> String {
    format!("sha256:{}", "a".repeat(64))
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_900_000_000 + seconds).expect("timestamp is valid")
}
