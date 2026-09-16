#![cfg(feature = "embedded-database")]

#[path = "support/catalog_publication.rs"]
mod catalog_publication_support;

use catalog_publication_support::{
    NEW_PUBLICATION_ID, READ_ATTEMPT_ID, SOURCE_ID, STATE_PUBLICATION_ID, TASK_ATTEMPT_ID, TASK_ID,
    TASK_OWNER_ID, seed_catalog_read, timestamp,
};
use serde_json::json;
use sqlx::PgPool;
use tempfile::tempdir;
use trakkin::{
    application::acquisition::{
        AbandonRead, BeginRead, PublicationRepository, PublishRead, ReadAbandonment, ReadFailure,
        ReadMode, ResetSourceStream, StageStateBatch,
    },
    database::DatabaseRuntime,
    domain::acquisition::{
        CatalogRelationKey, ProviderItemKey, StateBatch, StateField, StateObservation,
        StateSubject, StateValue, Term,
    },
    infrastructure::catalog_publication::{PostgresPublicationRepository, PublicationError},
};
use uuid::Uuid;

#[tokio::test]
async fn state_reset_clears_only_state_projection_and_stream() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_state_reset(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());
    let catalog_before = catalog_stream(&database.pool()).await;
    let items_before = published_item_names(&database.pool()).await;

    repository
        .reset_source_stream(ResetSourceStream {
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            completed_at: timestamp(0),
        })
        .await
        .expect("state stream resets atomically");

    assert_eq!(published_state_rows(&database.pool()).await, []);
    assert_eq!(state_tombstones(&database.pool()).await, []);
    assert_eq!(state_coverage(&database.pool()).await, None);
    assert_eq!(
        state_presence(&database.pool(), b"item.old", "old", "", "").await,
        "unknown"
    );
    assert_eq!(
        state_stream(&database.pool()).await,
        ("unavailable".to_owned(), None, None, None)
    );
    assert_eq!(catalog_stream(&database.pool()).await, catalog_before);
    assert_eq!(published_item_names(&database.pool()).await, items_before);
    assert_eq!(task_status(&database.pool()).await, "succeeded");
    assert_eq!(task_attempt_status(&database.pool()).await, "succeeded");
    assert_eq!(task_claim_owner(&database.pool()).await, None);

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn cancellation_wins_over_failed_staged_state_abandonment_atomically() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_full_state_read(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());
    let stream_before = state_stream(&database.pool()).await;
    let rows_before = published_state_rows(&database.pool()).await;

    repository
        .begin_state_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("state read attempt starts");
    repository
        .stage_state_batch(StageStateBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: replacement_state_batch(),
        })
        .await
        .expect("state batch stages before cancellation");
    sqlx::query(
        "UPDATE tasks
         SET status = 'cancelling', cancellation_requested_at = $1, updated_at = $1
         WHERE id = $2",
    )
    .bind(timestamp(10))
    .bind(TASK_ID)
    .execute(&database.pool())
    .await
    .expect("task cancellation is requested");

    let error = repository
        .publish_full_state(PublishRead {
            attempt_id: READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"state/v2".to_vec()),
            evidence_revision: b"fixture/state/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect_err("final cancellation fence rejects publication");
    assert!(matches!(error, PublicationError::TaskNotPublishable));
    assert_eq!(state_stream(&database.pool()).await, stream_before);
    assert_eq!(published_state_rows(&database.pool()).await, rows_before);
    assert_eq!(
        staged_state_rows(&database.pool()).await,
        [
            (
                "completed".to_owned(),
                "set".to_owned(),
                Some(json!({"kind": "boolean", "value": true})),
                b"state/relation/v2".to_vec(),
            ),
            (
                "progress".to_owned(),
                "set".to_owned(),
                Some(json!({"kind": "integer", "value": 8})),
                b"state/item/v2".to_vec(),
            ),
        ]
    );

    repository
        .abandon_read(AbandonRead {
            attempt_id: READ_ATTEMPT_ID,
            outcome: ReadAbandonment::Failed(ReadFailure {
                code: "adapter_state_stream_failed".to_owned(),
                retryable: true,
            }),
            completed_at: timestamp(12),
        })
        .await
        .expect("cancellation wins over failed abandonment atomically");

    assert_eq!(state_stream(&database.pool()).await, stream_before);
    assert_eq!(published_state_rows(&database.pool()).await, rows_before);
    assert_eq!(staged_state_rows(&database.pool()).await, []);
    assert_eq!(task_status(&database.pool()).await, "cancelled");
    assert_eq!(task_attempt_status(&database.pool()).await, "cancelled");
    assert_eq!(read_attempt_status(&database.pool()).await, "abandoned");
    assert_eq!(task_claim_owner(&database.pool()).await, None);

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn malformed_full_state_is_rejected_then_abandoned_as_failed() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_full_state_read(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());
    let stream_before = state_stream(&database.pool()).await;
    let rows_before = published_state_rows(&database.pool()).await;

    repository
        .begin_state_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("full state read attempt starts");
    repository
        .stage_state_batch(StageStateBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: malformed_full_state_batch(),
        })
        .await
        .expect("malformed full-state deletion stages before final validation");

    let error = repository
        .publish_full_state(PublishRead {
            attempt_id: READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"state/v2".to_vec()),
            evidence_revision: b"fixture/state/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect_err("full state publication rejects deletion observations");
    assert!(matches!(error, PublicationError::InvalidStaging { .. }));
    assert_eq!(state_stream(&database.pool()).await, stream_before);
    assert_eq!(published_state_rows(&database.pool()).await, rows_before);
    assert_eq!(
        staged_state_rows(&database.pool()).await,
        [(
            "old".to_owned(),
            "delete".to_owned(),
            None,
            b"state/old/v2".to_vec(),
        )]
    );

    let failure = ReadFailure {
        code: "malformed_state".to_owned(),
        retryable: false,
    };
    repository
        .abandon_read(AbandonRead {
            attempt_id: READ_ATTEMPT_ID,
            outcome: ReadAbandonment::Failed(failure.clone()),
            completed_at: timestamp(12),
        })
        .await
        .expect("malformed state read is abandoned as failed");

    assert_eq!(state_stream(&database.pool()).await, stream_before);
    assert_eq!(published_state_rows(&database.pool()).await, rows_before);
    assert_eq!(staged_state_rows(&database.pool()).await, []);
    assert_eq!(task_status(&database.pool()).await, "failed");
    assert_eq!(task_attempt_status(&database.pool()).await, "failed");
    assert_eq!(read_attempt_status(&database.pool()).await, "abandoned");
    assert_eq!(task_claim_owner(&database.pool()).await, None);
    assert_task_failure(task_failures(&database.pool()).await, &failure);

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn staged_full_state_is_invisible_until_fenced_publication() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_full_state_read(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());
    let catalog_before = catalog_stream(&database.pool()).await;

    let fence = repository
        .begin_state_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("full state read attempt starts");
    assert_eq!(fence.source_id, SOURCE_ID);
    assert_eq!(fence.read_mode, ReadMode::Full);
    assert_eq!(fence.expected_cursor, Some(b"state/v1".to_vec()));
    assert_eq!(fence.expected_publication_id, Some(STATE_PUBLICATION_ID));
    assert_eq!(fence.expected_capability_revision, 4);

    repository
        .stage_state_batch(StageStateBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: replacement_state_batch(),
        })
        .await
        .expect("normalized state batch stages");

    assert_eq!(
        published_state_rows(&database.pool()).await,
        [(
            "old".to_owned(),
            json!({"kind": "text", "value": "stale"}),
            b"state/old/v1".to_vec(),
            STATE_PUBLICATION_ID,
        )]
    );
    assert_eq!(
        staged_state_rows(&database.pool()).await,
        [
            (
                "completed".to_owned(),
                "set".to_owned(),
                Some(json!({"kind": "boolean", "value": true})),
                b"state/relation/v2".to_vec(),
            ),
            (
                "progress".to_owned(),
                "set".to_owned(),
                Some(json!({"kind": "integer", "value": 8})),
                b"state/item/v2".to_vec(),
            ),
        ]
    );

    repository
        .publish_full_state(PublishRead {
            attempt_id: READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"state/v2".to_vec()),
            evidence_revision: b"fixture/state/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect("full state publishes atomically");

    assert_eq!(
        published_state_rows(&database.pool()).await,
        [
            (
                "completed".to_owned(),
                json!({"kind": "boolean", "value": true}),
                b"state/relation/v2".to_vec(),
                NEW_PUBLICATION_ID,
            ),
            (
                "progress".to_owned(),
                json!({"kind": "integer", "value": 8}),
                b"state/item/v2".to_vec(),
                NEW_PUBLICATION_ID,
            ),
        ]
    );
    assert_eq!(state_tombstones(&database.pool()).await, []);
    assert_eq!(
        state_coverage(&database.pool()).await,
        Some((
            NEW_PUBLICATION_ID,
            NEW_PUBLICATION_ID,
            b"fixture/state/v2".to_vec(),
        ))
    );
    assert_eq!(
        state_presence(
            &database.pool(),
            b"item.new",
            "progress",
            "fixture.unit",
            "count",
        )
        .await,
        "present"
    );
    assert_eq!(
        state_presence(&database.pool(), b"item.old", "old", "", "").await,
        "absent"
    );
    assert_eq!(staged_state_rows(&database.pool()).await, []);
    assert_eq!(catalog_stream(&database.pool()).await, catalog_before);
    assert_eq!(published_item_names(&database.pool()).await, ["Old item"]);
    assert_eq!(task_status(&database.pool()).await, "succeeded");
    assert_eq!(task_attempt_status(&database.pool()).await, "succeeded");
    assert_eq!(read_attempt_status(&database.pool()).await, "published");
    assert_eq!(task_claim_owner(&database.pool()).await, None);

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

async fn seed_full_state_read(pool: PgPool) {
    seed_catalog_read(pool.clone()).await;
    let now = timestamp(-10);
    sqlx::query("UPDATE tasks SET kind = 'state_read' WHERE id = $1")
        .bind(TASK_ID)
        .execute(&pool)
        .await
        .expect("state read task kind is seeded");
    sqlx::query("UPDATE read_tasks SET stream_kind = 'state' WHERE task_id = $1")
        .bind(TASK_ID)
        .execute(&pool)
        .await
        .expect("state read task detail is seeded");
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
    .expect("state stream is seeded");
    sqlx::query(
        "INSERT INTO state_observations (
             source_id, subject_kind, subject_key_namespace, subject_key_value,
             field_namespace, field_name, unit_namespace, unit_name,
             value, provider_revision, observed_at, publication_id, updated_at
         ) VALUES (
             $1, 'provider_item', 'fixture.item', $2,
             'fixture.state', 'old', '', '',
             $3, $4, $5, $6, $5
         )",
    )
    .bind(SOURCE_ID)
    .bind(b"item.old".as_slice())
    .bind(json!({"kind": "text", "value": "stale"}))
    .bind(b"state/old/v1".as_slice())
    .bind(now)
    .bind(STATE_PUBLICATION_ID)
    .execute(&pool)
    .await
    .expect("old state observation is seeded");
    sqlx::query(
        "INSERT INTO state_observation_tombstones (
             source_id, subject_kind, subject_key_namespace, subject_key_value,
             field_namespace, field_name, unit_namespace, unit_name,
             provider_revision, observed_at, publication_id, updated_at
         ) VALUES
             (
                 $1, 'provider_item', 'fixture.item', $2,
                 'fixture.state', 'rating', 'fixture.unit', 'score',
                 $3, $4, $5, $4
             ),
             (
                 $1, 'provider_item', 'fixture.item', $2,
                 'fixture.state', 'removed', '', '',
                 $6, $4, $5, $4
             )",
    )
    .bind(SOURCE_ID)
    .bind(b"item.old".as_slice())
    .bind(b"state/rating/deleted/v1".as_slice())
    .bind(now)
    .bind(STATE_PUBLICATION_ID)
    .bind(b"state/removed/v1".as_slice())
    .execute(&pool)
    .await
    .expect("state tombstones are seeded");
    sqlx::query(
        "INSERT INTO state_publication_coverage (
             source_id, baseline_publication_id, publication_id,
             evidence_revision, observed_at, updated_at
         ) VALUES ($1, $2, $2, $3, $4, $4)",
    )
    .bind(SOURCE_ID)
    .bind(STATE_PUBLICATION_ID)
    .bind(b"fixture/state/v1".as_slice())
    .bind(now)
    .execute(&pool)
    .await
    .expect("state coverage is seeded");
}

async fn seed_state_reset(pool: PgPool) {
    seed_full_state_read(pool.clone()).await;
    sqlx::query("DELETE FROM read_tasks WHERE task_id = $1")
        .bind(TASK_ID)
        .execute(&pool)
        .await
        .expect("read task detail is removed");
    sqlx::query("UPDATE tasks SET kind = 'state_reset' WHERE id = $1")
        .bind(TASK_ID)
        .execute(&pool)
        .await
        .expect("state reset task kind is seeded");
    sqlx::query(
        "INSERT INTO reset_tasks (task_id, source_id, stream_kind)
         VALUES ($1, $2, 'state')",
    )
    .bind(TASK_ID)
    .bind(SOURCE_ID)
    .execute(&pool)
    .await
    .expect("state reset task detail is seeded");
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

fn malformed_full_state_batch() -> StateBatch {
    let deleted = StateObservation::delete(
        StateSubject::ProviderItem(
            ProviderItemKey::new("fixture.item", b"item.old".to_vec())
                .expect("malformed state provider item key is valid"),
        ),
        StateField::new(
            Term::new("fixture.state", "old").expect("malformed state field is valid"),
            None,
        ),
        b"state/old/v2".to_vec(),
        timestamp(9),
    )
    .expect("state deletion is valid incremental data");
    StateBatch::new(0, vec![deleted])
}

async fn published_item_names(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT display_name
         FROM catalog_items
         WHERE source_id = $1
         ORDER BY display_name",
    )
    .bind(SOURCE_ID)
    .fetch_all(pool)
    .await
    .expect("published item names are queryable")
}

async fn published_state_rows(pool: &PgPool) -> Vec<(String, serde_json::Value, Vec<u8>, Uuid)> {
    sqlx::query_as::<_, (String, serde_json::Value, Vec<u8>, Uuid)>(
        "SELECT field_name, value, provider_revision, publication_id
         FROM state_observations
         WHERE source_id = $1
         ORDER BY field_name",
    )
    .bind(SOURCE_ID)
    .fetch_all(pool)
    .await
    .expect("published state observations are queryable")
}

async fn staged_state_rows(
    pool: &PgPool,
) -> Vec<(String, String, Option<serde_json::Value>, Vec<u8>)> {
    sqlx::query_as::<_, (String, String, Option<serde_json::Value>, Vec<u8>)>(
        "SELECT field_name, operation, value, provider_revision
         FROM staged_state_observations
         WHERE attempt_id = $1
         ORDER BY field_name",
    )
    .bind(READ_ATTEMPT_ID)
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

async fn state_tombstones(pool: &PgPool) -> Vec<(String, Vec<u8>, Uuid)> {
    sqlx::query_as::<_, (String, Vec<u8>, Uuid)>(
        "SELECT field_name, provider_revision, publication_id
         FROM state_observation_tombstones
         WHERE source_id = $1
         ORDER BY field_name",
    )
    .bind(SOURCE_ID)
    .fetch_all(pool)
    .await
    .expect("state tombstones are queryable")
}

async fn state_coverage(pool: &PgPool) -> Option<(Uuid, Uuid, Vec<u8>)> {
    sqlx::query_as::<_, (Uuid, Uuid, Vec<u8>)>(
        "SELECT baseline_publication_id, publication_id, evidence_revision
         FROM state_publication_coverage
         WHERE source_id = $1",
    )
    .bind(SOURCE_ID)
    .fetch_optional(pool)
    .await
    .expect("state publication coverage is queryable")
}

async fn state_presence(
    pool: &PgPool,
    subject_key_value: &[u8],
    field_name: &str,
    unit_namespace: &str,
    unit_name: &str,
) -> String {
    sqlx::query_scalar::<_, String>(
        "SELECT CASE
             WHEN EXISTS(
                 SELECT 1 FROM state_observations
                 WHERE source_id = $1
                   AND subject_kind = 'provider_item'
                   AND subject_key_namespace = 'fixture.item'
                   AND subject_key_value = $2
                   AND field_namespace = 'fixture.state'
                   AND field_name = $3
                   AND unit_namespace = $4
                   AND unit_name = $5
             ) THEN 'present'
             WHEN EXISTS(
                 SELECT 1 FROM state_observation_tombstones
                 WHERE source_id = $1
                   AND subject_kind = 'provider_item'
                   AND subject_key_namespace = 'fixture.item'
                   AND subject_key_value = $2
                   AND field_namespace = 'fixture.state'
                   AND field_name = $3
                   AND unit_namespace = $4
                   AND unit_name = $5
             ) THEN 'deleted'
             WHEN EXISTS(
                 SELECT 1 FROM state_publication_coverage WHERE source_id = $1
             ) THEN 'absent'
             ELSE 'unknown'
         END::TEXT",
    )
    .bind(SOURCE_ID)
    .bind(subject_key_value)
    .bind(field_name)
    .bind(unit_namespace)
    .bind(unit_name)
    .fetch_one(pool)
    .await
    .expect("state presence is classifiable")
}

async fn task_status(pool: &PgPool) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM tasks WHERE id = $1")
        .bind(TASK_ID)
        .fetch_one(pool)
        .await
        .expect("task status is queryable")
}

async fn task_attempt_status(pool: &PgPool) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM task_attempts WHERE id = $1")
        .bind(TASK_ATTEMPT_ID)
        .fetch_one(pool)
        .await
        .expect("task attempt status is queryable")
}

async fn read_attempt_status(pool: &PgPool) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM read_attempts WHERE id = $1")
        .bind(READ_ATTEMPT_ID)
        .fetch_one(pool)
        .await
        .expect("read attempt status is queryable")
}

async fn task_claim_owner(pool: &PgPool) -> Option<Uuid> {
    sqlx::query_scalar::<_, Uuid>("SELECT owner_id FROM task_claims WHERE task_id = $1")
        .bind(TASK_ID)
        .fetch_optional(pool)
        .await
        .expect("task claim owner is queryable")
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

fn assert_task_failure(value: serde_json::Value, failure: &ReadFailure) {
    assert_eq!(value["code"], failure.code);
    assert_eq!(value["message"], failure.code.replace('_', " "));
    assert_eq!(value["retryable"], failure.retryable);
    assert!(value["id"].as_str().is_some_and(|id| !id.is_empty()));
}
