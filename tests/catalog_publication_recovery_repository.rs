#![cfg(feature = "embedded-database")]

#[path = "support/catalog_publication.rs"]
mod catalog_publication_support;

use catalog_publication_support::{
    NEW_PUBLICATION_ID, OLD_PUBLICATION_ID, READ_ATTEMPT_ID, SOURCE_ID, STATE_PUBLICATION_ID,
    TASK_ATTEMPT_ID, TASK_ID, TASK_OWNER_ID, WORKSPACE_ID, seed_catalog_read, timestamp,
};
use serde_json::json;
use sqlx::PgPool;
use tempfile::tempdir;
use trakkin::{
    application::acquisition::{
        AbandonRead, BeginRead, PublicationRepository, PublishRead, ReadAbandonment, ReadFailure,
        ReadMode, ResetSourceStream, StageCatalogBatch, StageStateBatch,
    },
    application::task_lifecycle::{
        InterruptActiveTasks, InterruptionSummary, TaskLifecycleRepository,
    },
    database::DatabaseRuntime,
    domain::acquisition::{
        BinaryAssetKey, BinaryAssetReference, CatalogBatch, CatalogItem, CatalogRelation,
        CatalogRelationKey, PortableReference, ProviderItemKey, StateBatch, StateField,
        StateObservation, StateSubject, StateValue, Term,
    },
    infrastructure::{
        catalog_publication::{PostgresPublicationRepository, PublicationError},
        task_lifecycle::PostgresTaskLifecycleRepository,
    },
};
use uuid::Uuid;

const CONNECTION_ID: Uuid = Uuid::from_u128(1);
const REFETCH_TASK_ID: Uuid = Uuid::from_u128(12);
const REFETCH_TASK_ATTEMPT_ID: Uuid = Uuid::from_u128(13);
const REFETCH_TASK_OWNER_ID: Uuid = Uuid::from_u128(14);
const REFETCH_READ_ATTEMPT_ID: Uuid = Uuid::from_u128(15);
const SECOND_TASK_ID: Uuid = Uuid::from_u128(16);
const SECOND_TASK_ATTEMPT_ID: Uuid = Uuid::from_u128(17);
const SECOND_TASK_OWNER_ID: Uuid = Uuid::from_u128(18);
const SECOND_READ_ATTEMPT_ID: Uuid = Uuid::from_u128(19);
const QUEUED_TASK_ID: Uuid = Uuid::from_u128(20);

#[tokio::test]
async fn restart_interrupts_active_tasks_and_abandons_staging_without_replay() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_incremental_catalog_read(database.pool()).await;
    seed_concurrent_state_read_and_queued_task(database.pool()).await;
    let publication_repository = PostgresPublicationRepository::new(database.pool());
    let lifecycle_repository = PostgresTaskLifecycleRepository::new(database.pool());
    let catalog_before = catalog_stream(&database.pool()).await;
    let state_before = state_stream(&database.pool()).await;
    let items_before = published_item_rows(&database.pool()).await;
    let state_rows_before = published_state_rows(&database.pool()).await;

    publication_repository
        .begin_catalog_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("catalog read attempt starts before restart");
    publication_repository
        .stage_catalog_batch(StageCatalogBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: incremental_batch(),
        })
        .await
        .expect("catalog batch stages before restart");
    publication_repository
        .begin_state_read(BeginRead {
            attempt_id: SECOND_READ_ATTEMPT_ID,
            task_id: SECOND_TASK_ID,
            task_attempt_id: SECOND_TASK_ATTEMPT_ID,
            owner_id: SECOND_TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("state read attempt starts before restart");
    publication_repository
        .stage_state_batch(StageStateBatch {
            attempt_id: SECOND_READ_ATTEMPT_ID,
            batch: replacement_state_batch(),
        })
        .await
        .expect("state batch stages before restart");

    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        [
            (
                b"item.new".to_vec(),
                "upsert".to_owned(),
                Some("Added item".to_owned())
            ),
            (
                b"item.old".to_vec(),
                "upsert".to_owned(),
                Some("Updated item".to_owned())
            ),
            (b"item.stale".to_vec(), "delete".to_owned(), None),
        ]
    );
    assert_eq!(
        staged_state_rows(&database.pool(), SECOND_READ_ATTEMPT_ID).await,
        [
            ("completed".to_owned(), "set".to_owned()),
            ("progress".to_owned(), "set".to_owned()),
        ]
    );

    let summary = lifecycle_repository
        .interrupt_active_tasks(InterruptActiveTasks {
            interrupted_at: timestamp(12),
        })
        .await
        .expect("startup recovery interrupts active tasks");
    assert_eq!(
        summary,
        InterruptionSummary {
            tasks: 3,
            task_attempts: 2,
            read_attempts: 2,
            sync_actions_confirmed_not_applied: 0,
            sync_actions_indeterminate: 0,
            claims: 2,
        }
    );

    assert_eq!(catalog_stream(&database.pool()).await, catalog_before);
    assert_eq!(state_stream(&database.pool()).await, state_before);
    assert_eq!(published_item_rows(&database.pool()).await, items_before);
    assert_eq!(
        published_state_rows(&database.pool()).await,
        state_rows_before
    );
    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        []
    );
    assert_eq!(
        staged_catalog_relations(&database.pool(), READ_ATTEMPT_ID).await,
        []
    );
    assert_eq!(
        staged_state_rows(&database.pool(), SECOND_READ_ATTEMPT_ID).await,
        []
    );
    assert_eq!(task_claim_owner(&database.pool(), TASK_ID).await, None);
    assert_eq!(
        task_claim_owner(&database.pool(), SECOND_TASK_ID).await,
        None
    );
    assert_eq!(
        task_statuses(&database.pool()).await,
        [
            (TASK_ID, "interrupted".to_owned()),
            (SECOND_TASK_ID, "interrupted".to_owned()),
            (QUEUED_TASK_ID, "interrupted".to_owned()),
        ]
    );
    assert_eq!(
        read_attempt_status_for(&database.pool(), READ_ATTEMPT_ID).await,
        "abandoned"
    );
    assert_eq!(
        read_attempt_status_for(&database.pool(), SECOND_READ_ATTEMPT_ID).await,
        "abandoned"
    );
    assert_eq!(
        lifecycle_repository
            .interrupt_active_tasks(InterruptActiveTasks {
                interrupted_at: timestamp(13),
            })
            .await
            .expect("repeated startup recovery is a no-op"),
        InterruptionSummary::default()
    );

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn catalog_reset_is_selective_and_full_refetch_publishes_only_successor() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_catalog_reset(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());
    let state_before = state_stream(&database.pool()).await;
    let state_rows_before = published_state_rows(&database.pool()).await;

    repository
        .reset_source_stream(ResetSourceStream {
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            completed_at: timestamp(0),
        })
        .await
        .expect("catalog stream resets atomically");

    assert_eq!(published_item_rows(&database.pool()).await, []);
    assert!(
        published_reference_values(&database.pool())
            .await
            .is_empty()
    );
    assert!(published_asset_keys(&database.pool()).await.is_empty());
    assert!(published_relation_keys(&database.pool()).await.is_empty());
    assert_eq!(
        catalog_stream(&database.pool()).await,
        ("unavailable".to_owned(), None, None, None)
    );
    assert_eq!(state_stream(&database.pool()).await, state_before);
    assert_eq!(
        published_state_rows(&database.pool()).await,
        state_rows_before
    );
    assert_eq!(
        task_status_for(&database.pool(), TASK_ID).await,
        "succeeded"
    );
    assert_eq!(
        task_attempt_status_for(&database.pool(), TASK_ATTEMPT_ID).await,
        "succeeded"
    );
    assert_eq!(task_claim_owner(&database.pool(), TASK_ID).await, None);

    seed_catalog_refetch(database.pool()).await;
    let fence = repository
        .begin_catalog_read(BeginRead {
            attempt_id: REFETCH_READ_ATTEMPT_ID,
            task_id: REFETCH_TASK_ID,
            task_attempt_id: REFETCH_TASK_ATTEMPT_ID,
            owner_id: REFETCH_TASK_OWNER_ID,
            started_at: timestamp(1),
        })
        .await
        .expect("full refetch starts from unavailable stream");
    assert_eq!(fence.read_mode, ReadMode::Full);
    assert_eq!(fence.expected_cursor, None);
    assert_eq!(fence.expected_publication_id, None);
    repository
        .stage_catalog_batch(StageCatalogBatch {
            attempt_id: REFETCH_READ_ATTEMPT_ID,
            batch: replacement_batch(),
        })
        .await
        .expect("successor catalog stages");
    repository
        .publish_full_catalog(PublishRead {
            attempt_id: REFETCH_READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"catalog/v2".to_vec()),
            evidence_revision: b"fixture/catalog/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect("successor catalog publishes after reset");

    assert_eq!(
        published_item_rows(&database.pool()).await,
        successor_item_rows()
    );
    assert_eq!(
        published_reference_values(&database.pool()).await,
        [b"new".to_vec()]
    );
    assert_eq!(
        published_asset_keys(&database.pool()).await,
        [b"asset.new".to_vec()]
    );
    assert_eq!(
        published_relation_keys(&database.pool()).await,
        [b"group.root".to_vec(), b"relation.new".to_vec()]
    );
    assert_eq!(
        catalog_stream(&database.pool()).await,
        (
            "available".to_owned(),
            Some(b"catalog/v2".to_vec()),
            Some(NEW_PUBLICATION_ID),
            Some(b"fixture/catalog/v2".to_vec()),
        )
    );
    assert_eq!(state_stream(&database.pool()).await, state_before);
    assert_eq!(
        published_state_rows(&database.pool()).await,
        state_rows_before
    );
    assert_eq!(
        task_status_for(&database.pool(), REFETCH_TASK_ID).await,
        "succeeded"
    );
    assert_eq!(
        staged_catalog_items(&database.pool(), REFETCH_READ_ATTEMPT_ID).await,
        []
    );

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn reset_rejects_active_read_then_succeeds_after_abandonment() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_catalog_read(database.pool()).await;
    seed_concurrent_catalog_reset_task(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());
    let stream_before = catalog_stream(&database.pool()).await;
    let items_before = published_item_rows(&database.pool()).await;

    repository
        .begin_catalog_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("catalog read attempt starts before reset");
    repository
        .stage_catalog_batch(StageCatalogBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: replacement_batch(),
        })
        .await
        .expect("catalog batch stages before reset");

    let error = repository
        .reset_source_stream(ResetSourceStream {
            task_id: REFETCH_TASK_ID,
            task_attempt_id: REFETCH_TASK_ATTEMPT_ID,
            owner_id: REFETCH_TASK_OWNER_ID,
            completed_at: timestamp(10),
        })
        .await
        .expect_err("reset rejects an active staged read");
    assert!(matches!(error, PublicationError::SourceStreamBusy));
    assert_eq!(catalog_stream(&database.pool()).await, stream_before);
    assert_eq!(published_item_rows(&database.pool()).await, items_before);
    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        [(
            b"item.new".to_vec(),
            "upsert".to_owned(),
            Some("New item".to_owned())
        )]
    );
    assert_eq!(
        read_attempt_status_for(&database.pool(), READ_ATTEMPT_ID).await,
        "staging"
    );
    assert_eq!(
        task_status_for(&database.pool(), REFETCH_TASK_ID).await,
        "running"
    );

    repository
        .abandon_read(AbandonRead {
            attempt_id: READ_ATTEMPT_ID,
            outcome: ReadAbandonment::Failed(ReadFailure {
                code: "superseded_by_reset".to_owned(),
                retryable: false,
            }),
            completed_at: timestamp(11),
        })
        .await
        .expect("active read is abandoned before retrying reset");
    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        []
    );
    assert_eq!(
        staged_catalog_relations(&database.pool(), READ_ATTEMPT_ID).await,
        []
    );
    repository
        .reset_source_stream(ResetSourceStream {
            task_id: REFETCH_TASK_ID,
            task_attempt_id: REFETCH_TASK_ATTEMPT_ID,
            owner_id: REFETCH_TASK_OWNER_ID,
            completed_at: timestamp(12),
        })
        .await
        .expect("reset succeeds after active read abandonment");

    assert_eq!(published_item_rows(&database.pool()).await, []);
    assert!(
        published_reference_values(&database.pool())
            .await
            .is_empty()
    );
    assert!(published_asset_keys(&database.pool()).await.is_empty());
    assert!(published_relation_keys(&database.pool()).await.is_empty());
    assert_eq!(
        catalog_stream(&database.pool()).await,
        ("unavailable".to_owned(), None, None, None)
    );
    assert_eq!(
        task_status_for(&database.pool(), REFETCH_TASK_ID).await,
        "succeeded"
    );
    assert_eq!(
        task_claim_owner(&database.pool(), REFETCH_TASK_ID).await,
        None
    );

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn adapter_failure_abandons_staged_catalog_atomically() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_catalog_read(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());
    let stream_before = catalog_stream(&database.pool()).await;
    let items_before = published_item_rows(&database.pool()).await;

    repository
        .begin_catalog_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("catalog read attempt starts");
    repository
        .stage_catalog_batch(StageCatalogBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: replacement_batch(),
        })
        .await
        .expect("catalog batch stages before adapter failure");
    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        [(
            b"item.new".to_vec(),
            "upsert".to_owned(),
            Some("New item".to_owned())
        )]
    );
    assert_eq!(
        staged_catalog_relations(&database.pool(), READ_ATTEMPT_ID).await,
        [
            (b"group.root".to_vec(), "upsert".to_owned()),
            (b"relation.new".to_vec(), "upsert".to_owned()),
        ]
    );

    let failure = ReadFailure {
        code: "adapter_unavailable".to_owned(),
        retryable: true,
    };
    repository
        .abandon_read(AbandonRead {
            attempt_id: READ_ATTEMPT_ID,
            outcome: ReadAbandonment::Failed(failure.clone()),
            completed_at: timestamp(12),
        })
        .await
        .expect("adapter failure abandons catalog read");

    assert_eq!(catalog_stream(&database.pool()).await, stream_before);
    assert_eq!(published_item_rows(&database.pool()).await, items_before);
    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        []
    );
    assert_eq!(
        staged_catalog_relations(&database.pool(), READ_ATTEMPT_ID).await,
        []
    );
    assert_eq!(task_status_for(&database.pool(), TASK_ID).await, "failed");
    assert_eq!(
        task_attempt_status_for(&database.pool(), TASK_ATTEMPT_ID).await,
        "failed"
    );
    assert_eq!(
        read_attempt_status_for(&database.pool(), READ_ATTEMPT_ID).await,
        "abandoned"
    );
    assert_eq!(task_claim_owner(&database.pool(), TASK_ID).await, None);
    assert_eq!(
        task_failures(&database.pool()).await,
        failure_json(&failure)
    );

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn staged_full_catalog_is_invisible_until_fenced_publication() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_catalog_read(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());

    let fence = repository
        .begin_catalog_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("full catalog read attempt starts");
    assert_eq!(fence.source_id, SOURCE_ID);
    assert_eq!(fence.expected_cursor, Some(b"catalog/v1".to_vec()));
    assert_eq!(fence.expected_publication_id, Some(OLD_PUBLICATION_ID));
    assert_eq!(fence.expected_capability_revision, 4);

    repository
        .stage_catalog_batch(StageCatalogBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: replacement_batch(),
        })
        .await
        .expect("normalized catalog batch stages");

    assert_eq!(published_item_rows(&database.pool()).await, old_item_rows());
    assert_eq!(
        published_reference_values(&database.pool()).await,
        [b"old".to_vec()]
    );
    assert_eq!(
        published_asset_keys(&database.pool()).await,
        [b"asset.old".to_vec()]
    );
    assert_eq!(
        published_relation_keys(&database.pool()).await,
        [b"relation.old".to_vec()]
    );
    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        [(
            b"item.new".to_vec(),
            "upsert".to_owned(),
            Some("New item".to_owned())
        )]
    );
    assert_eq!(
        staged_catalog_relations(&database.pool(), READ_ATTEMPT_ID).await,
        [
            (b"group.root".to_vec(), "upsert".to_owned()),
            (b"relation.new".to_vec(), "upsert".to_owned()),
        ]
    );

    sqlx::query("UPDATE task_claims SET owner_id = $1 WHERE task_id = $2")
        .bind(SECOND_TASK_OWNER_ID)
        .bind(TASK_ID)
        .execute(&database.pool())
        .await
        .expect("claim owner changes");
    let ownership_error = repository
        .publish_full_catalog(PublishRead {
            attempt_id: READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"catalog/v2".to_vec()),
            evidence_revision: b"fixture/catalog/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect_err("a different claim owner cannot publish");
    assert!(matches!(
        ownership_error,
        PublicationError::InvalidTask { .. }
    ));
    assert_eq!(published_item_rows(&database.pool()).await, old_item_rows());
    assert_eq!(
        task_claim_owner(&database.pool(), TASK_ID).await,
        Some(SECOND_TASK_OWNER_ID)
    );
    sqlx::query("UPDATE task_claims SET owner_id = $1 WHERE task_id = $2")
        .bind(TASK_OWNER_ID)
        .bind(TASK_ID)
        .execute(&database.pool())
        .await
        .expect("original claim owner is restored");

    repository
        .publish_full_catalog(PublishRead {
            attempt_id: READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"catalog/v2".to_vec()),
            evidence_revision: b"fixture/catalog/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect("full catalog publishes atomically");

    assert_eq!(
        published_item_rows(&database.pool()).await,
        successor_item_rows()
    );
    assert_eq!(
        published_reference_values(&database.pool()).await,
        [b"new".to_vec()]
    );
    assert_eq!(
        published_asset_keys(&database.pool()).await,
        [b"asset.new".to_vec()]
    );
    assert_eq!(
        published_relation_keys(&database.pool()).await,
        [b"group.root".to_vec(), b"relation.new".to_vec()]
    );
    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        []
    );
    assert_eq!(
        staged_catalog_relations(&database.pool(), READ_ATTEMPT_ID).await,
        []
    );
    assert_eq!(
        catalog_stream(&database.pool()).await,
        (
            "available".to_owned(),
            Some(b"catalog/v2".to_vec()),
            Some(NEW_PUBLICATION_ID),
            Some(b"fixture/catalog/v2".to_vec()),
        )
    );
    assert_eq!(
        task_status_for(&database.pool(), TASK_ID).await,
        "succeeded"
    );
    assert_eq!(
        task_attempt_status_for(&database.pool(), TASK_ATTEMPT_ID).await,
        "succeeded"
    );
    assert_eq!(
        read_attempt_status_for(&database.pool(), READ_ATTEMPT_ID).await,
        "published"
    );
    assert_eq!(task_claim_owner(&database.pool(), TASK_ID).await, None);

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn stale_cursor_fence_preserves_prior_catalog_and_staging() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_catalog_read(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());

    repository
        .begin_catalog_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("full catalog read attempt starts");
    repository
        .stage_catalog_batch(StageCatalogBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: replacement_batch(),
        })
        .await
        .expect("normalized catalog batch stages");

    sqlx::query(
        "UPDATE source_streams
         SET cursor = $1
         WHERE source_id = $2 AND stream_kind = 'catalog'",
    )
    .bind(b"catalog/concurrent".as_slice())
    .bind(SOURCE_ID)
    .execute(&database.pool())
    .await
    .expect("concurrent cursor change is simulated");

    let error = repository
        .publish_full_catalog(PublishRead {
            attempt_id: READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"catalog/v2".to_vec()),
            evidence_revision: b"fixture/catalog/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect_err("stale cursor fence rejects publication");
    assert!(matches!(
        error,
        PublicationError::FenceChanged { field: "cursor" }
    ));

    assert_eq!(published_item_rows(&database.pool()).await, old_item_rows());
    assert_eq!(
        published_reference_values(&database.pool()).await,
        [b"old".to_vec()]
    );
    assert_eq!(
        published_asset_keys(&database.pool()).await,
        [b"asset.old".to_vec()]
    );
    assert_eq!(
        published_relation_keys(&database.pool()).await,
        [b"relation.old".to_vec()]
    );
    assert_eq!(
        catalog_stream(&database.pool()).await,
        (
            "available".to_owned(),
            Some(b"catalog/concurrent".to_vec()),
            Some(OLD_PUBLICATION_ID),
            Some(b"fixture/catalog/v1".to_vec()),
        )
    );
    assert_eq!(
        staged_catalog_items(&database.pool(), READ_ATTEMPT_ID).await,
        [(
            b"item.new".to_vec(),
            "upsert".to_owned(),
            Some("New item".to_owned())
        )]
    );
    assert_eq!(
        staged_catalog_relations(&database.pool(), READ_ATTEMPT_ID).await,
        [
            (b"group.root".to_vec(), "upsert".to_owned()),
            (b"relation.new".to_vec(), "upsert".to_owned()),
        ]
    );
    assert_eq!(task_status_for(&database.pool(), TASK_ID).await, "running");
    assert_eq!(
        task_attempt_status_for(&database.pool(), TASK_ATTEMPT_ID).await,
        "running"
    );
    assert_eq!(
        read_attempt_status_for(&database.pool(), READ_ATTEMPT_ID).await,
        "staging"
    );
    assert_eq!(
        task_claim_owner(&database.pool(), TASK_ID).await,
        Some(TASK_OWNER_ID)
    );

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

async fn seed_incremental_catalog_read(pool: PgPool) {
    seed_catalog_read(pool.clone()).await;
    let now = timestamp(-10);
    sqlx::query("UPDATE read_tasks SET read_mode = 'incremental' WHERE task_id = $1")
        .bind(TASK_ID)
        .execute(&pool)
        .await
        .expect("incremental read mode is seeded");
    sqlx::query(
        "INSERT INTO source_streams (
             source_id, stream_kind, status, cursor, publication_id,
             evidence_revision, last_success_at, updated_at
         ) VALUES ($1, 'state', 'available', $2, $3, $4, $5, $5)",
    )
    .bind(SOURCE_ID)
    .bind(b"state/v1".as_slice())
    .bind(STATE_PUBLICATION_ID)
    .bind(b"fixture/state/v1".as_slice())
    .bind(now)
    .execute(&pool)
    .await
    .expect("independent state stream is seeded");
    sqlx::query(
        "INSERT INTO state_observations (
             source_id, subject_kind, subject_key_namespace, subject_key_value,
             field_namespace, field_name, unit_namespace, unit_name,
             value, provider_revision, observed_at, publication_id, updated_at
         ) VALUES (
             $1, 'provider_item', 'fixture.item', $2,
             'fixture.state', 'progress', 'fixture.unit', 'count',
             $3, $4, $5, $6, $5
         )",
    )
    .bind(SOURCE_ID)
    .bind(b"item.old".as_slice())
    .bind(json!({"kind": "integer", "value": 1}))
    .bind(b"state/item.old/v1".as_slice())
    .bind(now)
    .bind(STATE_PUBLICATION_ID)
    .execute(&pool)
    .await
    .expect("state observation is seeded");
    sqlx::query(
        "INSERT INTO catalog_items (
             source_id, key_namespace, key_value, kind_namespace, kind_name,
             display_name, attributes, publication_id, updated_at
         ) VALUES ($1, 'fixture.item', $2, 'fixture', 'episode', 'Stale item', $3, $4, $5)",
    )
    .bind(SOURCE_ID)
    .bind(b"item.stale".as_slice())
    .bind(json!({}))
    .bind(OLD_PUBLICATION_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("stale catalog item is seeded");
    sqlx::query(
        "INSERT INTO catalog_item_references (
             source_id, item_key_namespace, item_key_value,
             reference_namespace, reference_value, recommended_mapping_root
         ) VALUES ($1, 'fixture.item', $2, 'example.media', $3, TRUE)",
    )
    .bind(SOURCE_ID)
    .bind(b"item.stale".as_slice())
    .bind(b"stale".as_slice())
    .execute(&pool)
    .await
    .expect("stale portable reference is seeded");
    sqlx::query(
        "INSERT INTO catalog_assets (
             source_id, item_key_namespace, item_key_value,
             asset_key_namespace, asset_key_value,
             kind_namespace, kind_name, publication_id
         ) VALUES ($1, 'fixture.item', $2, 'fixture.asset', $3, 'fixture', 'poster', $4)",
    )
    .bind(SOURCE_ID)
    .bind(b"item.stale".as_slice())
    .bind(b"asset.stale".as_slice())
    .bind(OLD_PUBLICATION_ID)
    .execute(&pool)
    .await
    .expect("stale asset locator is seeded");
    sqlx::query(
        "INSERT INTO catalog_relations (
             source_id, key_namespace, key_value,
             item_key_namespace, item_key_value,
             kind_namespace, kind_name, order_parts, attributes,
             publication_id, updated_at
         ) VALUES (
             $1, 'fixture.relation', $2,
             'fixture.item', $3,
             'fixture', 'member', $4, $5, $6, $7
         )",
    )
    .bind(SOURCE_ID)
    .bind(b"relation.stale".as_slice())
    .bind(b"item.stale".as_slice())
    .bind(vec![20_i64])
    .bind(json!({}))
    .bind(OLD_PUBLICATION_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("stale catalog relation is seeded");
}

async fn seed_catalog_reset(pool: PgPool) {
    seed_incremental_catalog_read(pool.clone()).await;
    sqlx::query("DELETE FROM read_tasks WHERE task_id = $1")
        .bind(TASK_ID)
        .execute(&pool)
        .await
        .expect("read task detail is removed");
    sqlx::query("UPDATE tasks SET kind = 'catalog_reset' WHERE id = $1")
        .bind(TASK_ID)
        .execute(&pool)
        .await
        .expect("catalog reset task kind is seeded");
    sqlx::query(
        "INSERT INTO reset_tasks (task_id, source_id, stream_kind)
         VALUES ($1, $2, 'catalog')",
    )
    .bind(TASK_ID)
    .bind(SOURCE_ID)
    .execute(&pool)
    .await
    .expect("catalog reset task detail is seeded");
}

async fn seed_catalog_refetch(pool: PgPool) {
    let now = timestamp(1);
    sqlx::query(
        "INSERT INTO tasks (
             id, workspace_id, connection_id, kind, status,
             cancellation_supported, invocation_context, created_at, started_at, updated_at
         ) VALUES (
             $1, $2, $3, 'catalog_read', 'running', TRUE,
             jsonb_build_object('correlationId', 'request:test', 'traceparent', NULL, 'tracestate', NULL),
             $4, $4, $4
         )",
    )
    .bind(REFETCH_TASK_ID)
    .bind(WORKSPACE_ID)
    .bind(CONNECTION_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("refetch task is seeded");
    sqlx::query(
        "INSERT INTO task_claims (task_id, owner_id, claimed_at, heartbeat_at)
         VALUES ($1, $2, $3, $3)",
    )
    .bind(REFETCH_TASK_ID)
    .bind(REFETCH_TASK_OWNER_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("refetch task claim is seeded");
    sqlx::query(
        "INSERT INTO task_attempts (id, task_id, ordinal, status, started_at)
         VALUES ($1, $2, 0, 'running', $3)",
    )
    .bind(REFETCH_TASK_ATTEMPT_ID)
    .bind(REFETCH_TASK_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("refetch task attempt is seeded");
    sqlx::query(
        "INSERT INTO read_tasks (task_id, source_id, stream_kind, read_mode)
         VALUES ($1, $2, 'catalog', 'full')",
    )
    .bind(REFETCH_TASK_ID)
    .bind(SOURCE_ID)
    .execute(&pool)
    .await
    .expect("refetch read task detail is seeded");
}

async fn seed_concurrent_catalog_reset_task(pool: PgPool) {
    let now = timestamp(-10);
    sqlx::query(
        "INSERT INTO tasks (
             id, workspace_id, connection_id, kind, status,
             cancellation_supported, invocation_context, created_at, started_at, updated_at
         ) VALUES (
             $1, $2, $3, 'catalog_reset', 'running', TRUE,
             jsonb_build_object('correlationId', 'request:test', 'traceparent', NULL, 'tracestate', NULL),
             $4, $4, $4
         )",
    )
    .bind(REFETCH_TASK_ID)
    .bind(WORKSPACE_ID)
    .bind(CONNECTION_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("concurrent reset task is seeded");
    sqlx::query(
        "INSERT INTO task_claims (task_id, owner_id, claimed_at, heartbeat_at)
         VALUES ($1, $2, $3, $3)",
    )
    .bind(REFETCH_TASK_ID)
    .bind(REFETCH_TASK_OWNER_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("concurrent reset task claim is seeded");
    sqlx::query(
        "INSERT INTO task_attempts (id, task_id, ordinal, status, started_at)
         VALUES ($1, $2, 0, 'running', $3)",
    )
    .bind(REFETCH_TASK_ATTEMPT_ID)
    .bind(REFETCH_TASK_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("concurrent reset task attempt is seeded");
    sqlx::query(
        "INSERT INTO reset_tasks (task_id, source_id, stream_kind)
         VALUES ($1, $2, 'catalog')",
    )
    .bind(REFETCH_TASK_ID)
    .bind(SOURCE_ID)
    .execute(&pool)
    .await
    .expect("concurrent reset task detail is seeded");
}

async fn seed_concurrent_state_read_and_queued_task(pool: PgPool) {
    let now = timestamp(-10);
    sqlx::query(
        "INSERT INTO tasks (
             id, workspace_id, connection_id, kind, status,
             cancellation_supported, invocation_context, created_at, started_at, updated_at
         ) VALUES
             (
                 $1, $2, $3, 'state_read', 'running', TRUE,
                 jsonb_build_object('correlationId', 'request:test', 'traceparent', NULL, 'tracestate', NULL),
                 $4, $4, $4
             ),
             (
                 $5, $2, $3, 'catalog_read', 'queued', TRUE,
                 jsonb_build_object('correlationId', 'request:test', 'traceparent', NULL, 'tracestate', NULL),
                 $4, NULL, $4
             )",
    )
    .bind(SECOND_TASK_ID)
    .bind(WORKSPACE_ID)
    .bind(CONNECTION_ID)
    .bind(now)
    .bind(QUEUED_TASK_ID)
    .execute(&pool)
    .await
    .expect("concurrent and queued tasks are seeded");
    sqlx::query(
        "INSERT INTO task_claims (task_id, owner_id, claimed_at, heartbeat_at)
         VALUES ($1, $2, $3, $3)",
    )
    .bind(SECOND_TASK_ID)
    .bind(SECOND_TASK_OWNER_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("concurrent state task claim is seeded");
    sqlx::query(
        "INSERT INTO task_attempts (id, task_id, ordinal, status, started_at)
         VALUES ($1, $2, 0, 'running', $3)",
    )
    .bind(SECOND_TASK_ATTEMPT_ID)
    .bind(SECOND_TASK_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("concurrent state task attempt is seeded");
    sqlx::query(
        "INSERT INTO read_tasks (task_id, source_id, stream_kind, read_mode)
         VALUES
             ($1, $2, 'state', 'full'),
             ($3, $2, 'catalog', 'full')",
    )
    .bind(SECOND_TASK_ID)
    .bind(SOURCE_ID)
    .bind(QUEUED_TASK_ID)
    .execute(&pool)
    .await
    .expect("concurrent and queued read task details are seeded");
}

fn replacement_batch() -> CatalogBatch {
    let item_key = ProviderItemKey::new("fixture.item", b"item.new".to_vec())
        .expect("provider item key is valid");
    let reference = PortableReference::new("example.media", b"new".to_vec())
        .expect("portable reference is valid");
    let item = CatalogItem::new(
        item_key.clone(),
        Term::new("fixture", "episode").expect("item kind is valid"),
        "New item",
        json!({"season": 1}),
        vec![reference.clone()],
        vec![reference],
        vec![BinaryAssetReference::new(
            BinaryAssetKey::new("fixture.asset", b"asset.new".to_vec())
                .expect("binary asset key is valid"),
            Term::new("fixture", "poster").expect("asset kind is valid"),
        )],
    )
    .expect("catalog item is valid");
    let root_key = CatalogRelationKey::new("fixture.relation", b"group.root".to_vec())
        .expect("root relation key is valid");
    let root = CatalogRelation::new(
        root_key.clone(),
        None,
        None,
        Term::new("fixture", "virtual_group").expect("root relation kind is valid"),
        Vec::new(),
        json!({}),
    )
    .expect("virtual root relation is valid");
    let child = CatalogRelation::new(
        CatalogRelationKey::new("fixture.relation", b"relation.new".to_vec())
            .expect("child relation key is valid"),
        Some(root_key),
        Some(item_key),
        Term::new("fixture", "member").expect("child relation kind is valid"),
        vec![10],
        json!({}),
    )
    .expect("child catalog relation is valid");
    CatalogBatch::new(0, vec![item], Vec::new(), vec![root, child], Vec::new())
}

fn incremental_batch() -> CatalogBatch {
    let updated_key = ProviderItemKey::new("fixture.item", b"item.old".to_vec())
        .expect("updated provider item key is valid");
    let added_key = ProviderItemKey::new("fixture.item", b"item.new".to_vec())
        .expect("added provider item key is valid");
    let updated_reference = PortableReference::new("example.media", b"old.updated".to_vec())
        .expect("updated portable reference is valid");
    let updated = CatalogItem::new(
        updated_key.clone(),
        Term::new("fixture", "episode").expect("updated item kind is valid"),
        "Updated item",
        json!({"revision": 2}),
        vec![updated_reference.clone()],
        vec![updated_reference],
        vec![BinaryAssetReference::new(
            BinaryAssetKey::new("fixture.asset", b"asset.updated".to_vec())
                .expect("updated binary asset key is valid"),
            Term::new("fixture", "poster").expect("updated asset kind is valid"),
        )],
    )
    .expect("updated catalog item is valid");
    let added_reference = PortableReference::new("example.media", b"new".to_vec())
        .expect("added portable reference is valid");
    let added = CatalogItem::new(
        added_key.clone(),
        Term::new("fixture", "episode").expect("added item kind is valid"),
        "Added item",
        json!({}),
        vec![added_reference.clone()],
        vec![added_reference],
        vec![BinaryAssetReference::new(
            BinaryAssetKey::new("fixture.asset", b"asset.new".to_vec())
                .expect("added binary asset key is valid"),
            Term::new("fixture", "poster").expect("added asset kind is valid"),
        )],
    )
    .expect("added catalog item is valid");
    let root_key = CatalogRelationKey::new("fixture.relation", b"group.root".to_vec())
        .expect("incremental root relation key is valid");
    let root = CatalogRelation::new(
        root_key.clone(),
        None,
        None,
        Term::new("fixture", "virtual_group").expect("incremental root kind is valid"),
        Vec::new(),
        json!({}),
    )
    .expect("incremental virtual root is valid");
    let updated_relation = CatalogRelation::new(
        CatalogRelationKey::new("fixture.relation", b"relation.old".to_vec())
            .expect("updated relation key is valid"),
        Some(root_key.clone()),
        Some(updated_key),
        Term::new("fixture", "member").expect("updated relation kind is valid"),
        vec![10],
        json!({}),
    )
    .expect("updated catalog relation is valid");
    let added_relation = CatalogRelation::new(
        CatalogRelationKey::new("fixture.relation", b"relation.new".to_vec())
            .expect("added relation key is valid"),
        Some(root_key),
        Some(added_key),
        Term::new("fixture", "member").expect("added relation kind is valid"),
        vec![20],
        json!({}),
    )
    .expect("added catalog relation is valid");
    CatalogBatch::new(
        0,
        vec![updated, added],
        vec![
            ProviderItemKey::new("fixture.item", b"item.stale".to_vec())
                .expect("deleted provider item key is valid"),
        ],
        vec![root, updated_relation, added_relation],
        vec![
            CatalogRelationKey::new("fixture.relation", b"relation.stale".to_vec())
                .expect("deleted relation key is valid"),
        ],
    )
}

fn replacement_state_batch() -> StateBatch {
    let progress = StateObservation::set(
        StateSubject::ProviderItem(
            ProviderItemKey::new("fixture.item", b"item.new".to_vec())
                .expect("state provider item key is valid"),
        ),
        StateField::new(
            Term::new("fixture.state", "progress").expect("progress field is valid"),
            Some(Term::new("fixture.unit", "count").expect("progress unit is valid")),
        ),
        StateValue::Integer(8),
        b"state/item/v2".to_vec(),
        timestamp(8),
    )
    .expect("progress observation is valid");
    let completed = StateObservation::set(
        StateSubject::CatalogRelation(
            CatalogRelationKey::new("fixture.relation", b"relation.old".to_vec())
                .expect("state catalog relation key is valid"),
        ),
        StateField::new(
            Term::new("fixture.state", "completed").expect("completed field is valid"),
            Some(Term::new("fixture.unit", "flag").expect("completed unit is valid")),
        ),
        StateValue::Boolean(true),
        b"state/relation/v2".to_vec(),
        timestamp(9),
    )
    .expect("completed observation is valid");
    StateBatch::new(0, vec![progress, completed])
}

fn old_item_rows() -> Vec<(Vec<u8>, String, Uuid)> {
    vec![(
        b"item.old".to_vec(),
        "Old item".to_owned(),
        OLD_PUBLICATION_ID,
    )]
}

fn successor_item_rows() -> Vec<(Vec<u8>, String, Uuid)> {
    vec![(
        b"item.new".to_vec(),
        "New item".to_owned(),
        NEW_PUBLICATION_ID,
    )]
}

async fn published_item_rows(pool: &PgPool) -> Vec<(Vec<u8>, String, Uuid)> {
    sqlx::query_as::<_, (Vec<u8>, String, Uuid)>(
        "SELECT key_value, display_name, publication_id
         FROM catalog_items
         WHERE source_id = $1
         ORDER BY key_value",
    )
    .bind(SOURCE_ID)
    .fetch_all(pool)
    .await
    .expect("published catalog items are queryable")
}

async fn published_reference_values(pool: &PgPool) -> Vec<Vec<u8>> {
    sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT reference_value
         FROM catalog_item_references
         WHERE source_id = $1
         ORDER BY reference_value",
    )
    .bind(SOURCE_ID)
    .fetch_all(pool)
    .await
    .expect("published portable references are queryable")
}

async fn published_asset_keys(pool: &PgPool) -> Vec<Vec<u8>> {
    sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT asset_key_value
         FROM catalog_assets
         WHERE source_id = $1
         ORDER BY asset_key_value",
    )
    .bind(SOURCE_ID)
    .fetch_all(pool)
    .await
    .expect("published asset keys are queryable")
}

async fn published_relation_keys(pool: &PgPool) -> Vec<Vec<u8>> {
    sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT key_value
         FROM catalog_relations
         WHERE source_id = $1
         ORDER BY key_value",
    )
    .bind(SOURCE_ID)
    .fetch_all(pool)
    .await
    .expect("published relation keys are queryable")
}

async fn published_state_rows(pool: &PgPool) -> Vec<(String, serde_json::Value, Uuid)> {
    sqlx::query_as::<_, (String, serde_json::Value, Uuid)>(
        "SELECT field_name, value, publication_id
         FROM state_observations
         WHERE source_id = $1
         ORDER BY field_name",
    )
    .bind(SOURCE_ID)
    .fetch_all(pool)
    .await
    .expect("published state observations are queryable")
}

async fn staged_catalog_items(
    pool: &PgPool,
    attempt_id: Uuid,
) -> Vec<(Vec<u8>, String, Option<String>)> {
    sqlx::query_as::<_, (Vec<u8>, String, Option<String>)>(
        "SELECT key_value, operation, display_name
         FROM staged_catalog_items
         WHERE attempt_id = $1
         ORDER BY key_value",
    )
    .bind(attempt_id)
    .fetch_all(pool)
    .await
    .expect("staged catalog items are queryable")
}

async fn staged_catalog_relations(pool: &PgPool, attempt_id: Uuid) -> Vec<(Vec<u8>, String)> {
    sqlx::query_as::<_, (Vec<u8>, String)>(
        "SELECT key_value, operation
         FROM staged_catalog_relations
         WHERE attempt_id = $1
         ORDER BY key_value",
    )
    .bind(attempt_id)
    .fetch_all(pool)
    .await
    .expect("staged catalog relations are queryable")
}

async fn staged_state_rows(pool: &PgPool, attempt_id: Uuid) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT field_name, operation
         FROM staged_state_observations
         WHERE attempt_id = $1
         ORDER BY field_name",
    )
    .bind(attempt_id)
    .fetch_all(pool)
    .await
    .expect("staged state observations are queryable")
}

async fn state_stream(pool: &PgPool) -> (String, Option<Vec<u8>>, Option<Uuid>, Option<Vec<u8>>) {
    sqlx::query_as::<_, (String, Option<Vec<u8>>, Option<Uuid>, Option<Vec<u8>>)>(
        "SELECT status, cursor, publication_id, evidence_revision
         FROM source_streams
         WHERE source_id = $1 AND stream_kind = 'state'",
    )
    .bind(SOURCE_ID)
    .fetch_one(pool)
    .await
    .expect("state stream is queryable")
}

async fn catalog_stream(pool: &PgPool) -> (String, Option<Vec<u8>>, Option<Uuid>, Option<Vec<u8>>) {
    sqlx::query_as::<_, (String, Option<Vec<u8>>, Option<Uuid>, Option<Vec<u8>>)>(
        "SELECT status, cursor, publication_id, evidence_revision
         FROM source_streams
         WHERE source_id = $1 AND stream_kind = 'catalog'",
    )
    .bind(SOURCE_ID)
    .fetch_one(pool)
    .await
    .expect("catalog stream is queryable")
}

async fn task_status_for(pool: &PgPool, task_id: Uuid) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("task status is queryable")
}

async fn task_attempt_status_for(pool: &PgPool, task_attempt_id: Uuid) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM task_attempts WHERE id = $1")
        .bind(task_attempt_id)
        .fetch_one(pool)
        .await
        .expect("task attempt status is queryable")
}

async fn read_attempt_status_for(pool: &PgPool, attempt_id: Uuid) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM read_attempts WHERE id = $1")
        .bind(attempt_id)
        .fetch_one(pool)
        .await
        .expect("read attempt status is queryable")
}

async fn task_claim_owner(pool: &PgPool, task_id: Uuid) -> Option<Uuid> {
    sqlx::query_scalar::<_, Uuid>("SELECT owner_id FROM task_claims WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("task claim owner is queryable")
}

async fn task_statuses(pool: &PgPool) -> Vec<(Uuid, String)> {
    sqlx::query_as::<_, (Uuid, String)>(
        "SELECT id, status
         FROM tasks
         WHERE id = ANY($1)
         ORDER BY id",
    )
    .bind([TASK_ID, SECOND_TASK_ID, QUEUED_TASK_ID].as_slice())
    .fetch_all(pool)
    .await
    .expect("task statuses are queryable")
}

async fn task_failures(pool: &PgPool) -> serde_json::Value {
    let (task_failure, attempt_failure) =
        sqlx::query_as::<_, (Option<serde_json::Value>, Option<serde_json::Value>)>(
            "SELECT tasks.failure, task_attempts.failure
             FROM tasks
             JOIN task_attempts ON task_attempts.task_id = tasks.id
             WHERE tasks.id = $1",
        )
        .bind(TASK_ID)
        .fetch_one(pool)
        .await
        .expect("task failures are queryable");
    assert_eq!(task_failure, attempt_failure);
    task_failure.expect("failed task records failure evidence")
}

fn failure_json(failure: &ReadFailure) -> serde_json::Value {
    json!({
        "code": failure.code,
        "retryable": failure.retryable,
    })
}
