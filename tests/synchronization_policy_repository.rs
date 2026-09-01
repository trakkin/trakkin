#![cfg(feature = "embedded-database")]

use serde_json::json;
use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::sync_policy::{
        CreateSyncPolicy, SyncPolicyLifecycleError, SyncPolicyService, UpdateSyncPolicy,
    },
    database::DatabaseRuntime,
    domain::synchronization::{SYNC_POLICY_SCHEMA, StateRegistry, SyncPolicyDocument},
    infrastructure::sync_policy::{
        PostgresSyncPolicyRepository, PostgresSyncPolicyRepositoryError,
    },
};
use uuid::Uuid;

const WORKSPACE_ID: Uuid = Uuid::from_u128(1);
const OTHER_WORKSPACE_ID: Uuid = Uuid::from_u128(2);
const CONNECTION_ID: Uuid = Uuid::from_u128(3);
const ACCOUNT_ID: Uuid = Uuid::from_u128(4);
const POLICY_ID: Uuid = Uuid::from_u128(10);
const OTHER_POLICY_ID: Uuid = Uuid::from_u128(11);
const CONTRIBUTOR_ID: Uuid = Uuid::from_u128(0xa1);
const DUAL_ROLE_ID: Uuid = Uuid::from_u128(0xb2);
const OTHER_SOURCE_ID: Uuid = Uuid::from_u128(0xc3);

#[tokio::test]
async fn postgres_sync_policy_round_trips_immutable_revisions_and_local_roles() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    let now = OffsetDateTime::from_unix_timestamp(1_893_456_245).expect("fixture time");
    seed_sources(&pool, now).await;
    let repository = PostgresSyncPolicyRepository::new(pool.clone());
    let service = SyncPolicyService::new(repository);

    let first_document =
        policy_document(&[DUAL_ROLE_ID, CONTRIBUTOR_ID], &[DUAL_ROLE_ID], "direct");
    let created = service
        .create(CreateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            document: first_document.clone(),
            created_at: now,
        })
        .await
        .expect("first policy revision is stored");
    assert_eq!(created.revision, 1);
    assert_eq!(created.document, first_document);
    assert_eq!(created.content_hash, first_document.content_hash());
    assert_eq!(
        service.head(WORKSPACE_ID, POLICY_ID).await.unwrap(),
        created
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT canonical_json
FROM sync_policy_revisions
WHERE policy_id = $1 AND revision = 1
",
        )
        .bind(POLICY_ID)
        .fetch_one(&pool)
        .await
        .expect("canonical policy bytes are queryable"),
        first_document.canonical_json()
    );
    assert_eq!(
        stored_roles(&pool, 1).await,
        [
            (CONTRIBUTOR_ID, "contributor".to_owned()),
            (DUAL_ROLE_ID, "contributor".to_owned()),
            (DUAL_ROLE_ID, "receiver".to_owned()),
        ]
    );

    let second_document = policy_document(&[CONTRIBUTOR_ID], &[DUAL_ROLE_ID], "transitive");
    let updated = service
        .update(UpdateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            base_content_hash: created.content_hash.clone(),
            document: second_document.clone(),
            updated_at: now,
        })
        .await
        .expect("optimistic successor is appended");
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.document, second_document);
    assert_eq!(
        service.head(WORKSPACE_ID, POLICY_ID).await.unwrap(),
        updated
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT canonical_json
FROM sync_policy_revisions
WHERE policy_id = $1 AND revision = 1
",
        )
        .bind(POLICY_ID)
        .fetch_one(&pool)
        .await
        .expect("first policy revision remains queryable"),
        first_document.canonical_json()
    );

    let writes_before = revision_count(&pool).await;
    let stale = service
        .update(UpdateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            base_content_hash: created.content_hash.clone(),
            document: policy_document(&[DUAL_ROLE_ID], &[CONTRIBUTOR_ID], "direct"),
            updated_at: now,
        })
        .await
        .expect_err("stale policy base is rejected");
    assert!(matches!(
        stale,
        SyncPolicyLifecycleError::HeadMoved { current_head }
            if current_head == updated.content_hash
    ));

    let duplicate_revision = service
        .update(UpdateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            base_content_hash: updated.content_hash.clone(),
            document: first_document.clone(),
            updated_at: now,
        })
        .await
        .expect_err("duplicate revision content is rejected");
    assert!(matches!(
        duplicate_revision,
        SyncPolicyLifecycleError::Repository(PostgresSyncPolicyRepositoryError::DuplicateRevision)
    ));

    let duplicate_policy = service
        .create(CreateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            document: first_document,
            created_at: now,
        })
        .await
        .expect_err("duplicate policy identity is rejected");
    assert!(matches!(
        duplicate_policy,
        SyncPolicyLifecycleError::Repository(PostgresSyncPolicyRepositoryError::DuplicatePolicy)
    ));

    let cross_workspace_source = service
        .create(CreateSyncPolicy {
            policy_id: OTHER_POLICY_ID,
            workspace_id: WORKSPACE_ID,
            document: policy_document(&[CONTRIBUTOR_ID], &[OTHER_SOURCE_ID], "direct"),
            created_at: now,
        })
        .await
        .expect_err("source from another workspace is rejected");
    assert!(matches!(
        cross_workspace_source,
        SyncPolicyLifecycleError::Repository(
            PostgresSyncPolicyRepositoryError::SourceNotAttached {
                source_id: OTHER_SOURCE_ID
            }
        )
    ));
    assert!(matches!(
        service.head(OTHER_WORKSPACE_ID, POLICY_ID).await,
        Err(SyncPolicyLifecycleError::PolicyNotFound)
    ));
    assert_eq!(revision_count(&pool).await, writes_before);

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

fn policy_document(
    contributors: &[Uuid],
    receivers: &[Uuid],
    resolution_mode: &str,
) -> SyncPolicyDocument {
    SyncPolicyDocument::from_json(
        &json!({
            "contributors": contributors,
            "receivers": receivers,
            "resolutionMode": resolution_mode,
            "rules": [{
                "semanticField": "trakkin.state:progress",
                "presence": {
                    "deleted": "ignore",
                    "absent": "clear",
                    "unknown": "block",
                    "stale": "block"
                },
                "conflict": "require_reducer",
                "reducer": "coverage_contiguous_prefix",
                "translation": {"quantizer": "nearest", "allowLossy": false},
                "writeConstraints": {
                    "requireStrongPrecondition": true,
                    "allowCreateMembership": false,
                    "allowClear": true
                }
            }],
            "schema": SYNC_POLICY_SCHEMA
        })
        .to_string(),
        StateRegistry::v1(),
    )
    .expect("test policy is valid")
}

async fn stored_roles(pool: &sqlx::PgPool, revision: i64) -> Vec<(Uuid, String)> {
    sqlx::query_as::<_, (Uuid, String)>(
        "SELECT source_id, role
FROM sync_policy_source_roles
WHERE policy_id = $1 AND revision = $2
ORDER BY role, source_id
",
    )
    .bind(POLICY_ID)
    .bind(revision)
    .fetch_all(pool)
    .await
    .expect("stored policy roles are queryable")
}

async fn revision_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM sync_policy_revisions")
        .fetch_one(pool)
        .await
        .expect("policy revision count is queryable")
}

async fn seed_sources(pool: &sqlx::PgPool, now: OffsetDateTime) {
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES
    ($1, 'Policies', 'active', $3, $3),
    ($2, 'Other policies', 'active', $3, $3)
",
    )
    .bind(WORKSPACE_ID)
    .bind(OTHER_WORKSPACE_ID)
    .bind(now)
    .execute(pool)
    .await
    .expect("workspaces are seeded");
    sqlx::query(
        "INSERT INTO connections (
    id, provider_id, display_name, status, capabilities,
    capability_revision, created_at, updated_at
) VALUES ($1, 'fixture', 'Fixture', 'ready', '{}'::JSONB, 1, $2, $2)
",
    )
    .bind(CONNECTION_ID)
    .bind(now)
    .execute(pool)
    .await
    .expect("connection is seeded");
    sqlx::query(
        "INSERT INTO accounts (
    id, connection_id, provider_key_namespace, provider_key_value,
    display_name, first_seen_at, last_seen_at
) VALUES ($1, $2, 'fixture.account', $3, 'Fixture account', $4, $4)
",
    )
    .bind(ACCOUNT_ID)
    .bind(CONNECTION_ID)
    .bind(b"account".as_slice())
    .bind(now)
    .execute(pool)
    .await
    .expect("account is seeded");
    for (source_id, key) in [
        (CONTRIBUTOR_ID, b"contributor".as_slice()),
        (DUAL_ROLE_ID, b"dual-role".as_slice()),
        (OTHER_SOURCE_ID, b"other".as_slice()),
    ] {
        sqlx::query(
            "INSERT INTO sources (
    id, connection_id, account_id, provider_key_namespace, provider_key_value,
    display_name, kind_namespace, kind_name, availability, capabilities,
    capability_revision, first_seen_at, last_seen_at
) VALUES (
    $1, $2, $3, 'fixture.source', $4, 'Fixture source',
    'fixture', 'state', 'available', '{}'::JSONB, 1, $5, $5
)
",
        )
        .bind(source_id)
        .bind(CONNECTION_ID)
        .bind(ACCOUNT_ID)
        .bind(key)
        .bind(now)
        .execute(pool)
        .await
        .expect("source is seeded");
    }
    for source_id in [CONTRIBUTOR_ID, DUAL_ROLE_ID] {
        sqlx::query(
            "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
VALUES ($1, $2, $3)
",
        )
        .bind(WORKSPACE_ID)
        .bind(source_id)
        .bind(now)
        .execute(pool)
        .await
        .expect("policy workspace source is attached");
    }
    sqlx::query(
        "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
VALUES ($1, $2, $3)
",
    )
    .bind(OTHER_WORKSPACE_ID)
    .bind(OTHER_SOURCE_ID)
    .bind(now)
    .execute(pool)
    .await
    .expect("other workspace source is attached");
}
