#![cfg(feature = "embedded-database")]

#[path = "support/catalog_publication.rs"]
mod catalog_publication_support;

use catalog_publication_support::{
    NEW_PUBLICATION_ID, OLD_PUBLICATION_ID, READ_ATTEMPT_ID, SOURCE_ID, STATE_PUBLICATION_ID,
    TASK_ATTEMPT_ID, TASK_ID, TASK_OWNER_ID, seed_catalog_read, timestamp,
};
use serde_json::json;
use sqlx::PgPool;
use tempfile::tempdir;
use trakkin::{
    application::acquisition::{
        BeginRead, PublicationRepository, PublishRead, ReadMode, StageCatalogBatch, StageStateBatch,
    },
    application::catalog_queries::{CatalogQueryRepository, ResolveSubjectUnits},
    database::DatabaseRuntime,
    domain::acquisition::{
        BinaryAssetKey, BinaryAssetReference, CatalogBatch, CatalogItem, CatalogRelation,
        CatalogRelationKey, ProviderItemKey, StateBatch, StateField, StateObservation,
        StateSubject, StateValue, SubjectUnitBinding, SubjectUnitBindingKey, Term,
    },
    infrastructure::{
        catalog_publication::PostgresPublicationRepository,
        catalog_queries::PostgresCatalogQueryRepository,
    },
};
use uuid::Uuid;

#[tokio::test]
async fn incremental_state_applies_exact_delta_without_advancing_catalog() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_incremental_state_read(database.pool()).await;
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
        .expect("incremental state read attempt starts");
    assert_eq!(fence.read_mode, ReadMode::Incremental);
    assert_eq!(fence.expected_cursor, Some(b"state/v1".to_vec()));
    assert_eq!(fence.expected_publication_id, Some(STATE_PUBLICATION_ID));

    repository
        .stage_state_batch(StageStateBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: incremental_state_batch(),
        })
        .await
        .expect("incremental state batch stages");
    assert_eq!(
        published_state_rows(&database.pool()).await,
        [
            (
                "old".to_owned(),
                json!({"kind": "text", "value": "stale"}),
                b"state/old/v1".to_vec(),
                STATE_PUBLICATION_ID,
            ),
            (
                "progress".to_owned(),
                json!({"kind": "integer", "value": 1}),
                b"state/progress/v1".to_vec(),
                STATE_PUBLICATION_ID,
            ),
            (
                "untouched".to_owned(),
                json!({"kind": "boolean", "value": true}),
                b"state/untouched/v1".to_vec(),
                STATE_PUBLICATION_ID,
            ),
        ]
    );
    assert_eq!(
        staged_state_rows(&database.pool()).await,
        [
            (
                "old".to_owned(),
                "delete".to_owned(),
                None,
                b"state/old/v2".to_vec(),
            ),
            (
                "progress".to_owned(),
                "set".to_owned(),
                Some(json!({"kind": "integer", "value": 8})),
                b"state/progress/v2".to_vec(),
            ),
            (
                "rating".to_owned(),
                "set".to_owned(),
                Some(json!({"kind": "decimal", "value": "8.5"})),
                b"state/rating/v1".to_vec(),
            ),
        ]
    );

    repository
        .publish_incremental_state(PublishRead {
            attempt_id: READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"state/v2".to_vec()),
            evidence_revision: b"fixture/state/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect("incremental state publishes atomically");

    assert_eq!(
        published_state_rows(&database.pool()).await,
        [
            (
                "progress".to_owned(),
                json!({"kind": "integer", "value": 8}),
                b"state/progress/v2".to_vec(),
                NEW_PUBLICATION_ID,
            ),
            (
                "rating".to_owned(),
                json!({"kind": "decimal", "value": "8.5"}),
                b"state/rating/v1".to_vec(),
                NEW_PUBLICATION_ID,
            ),
            (
                "untouched".to_owned(),
                json!({"kind": "boolean", "value": true}),
                b"state/untouched/v1".to_vec(),
                NEW_PUBLICATION_ID,
            ),
        ]
    );
    assert_eq!(
        state_tombstones(&database.pool()).await,
        [
            (
                "old".to_owned(),
                b"state/old/v2".to_vec(),
                NEW_PUBLICATION_ID,
            ),
            (
                "removed".to_owned(),
                b"state/removed/v1".to_vec(),
                NEW_PUBLICATION_ID,
            ),
        ]
    );
    assert_eq!(
        state_coverage(&database.pool()).await,
        Some((
            STATE_PUBLICATION_ID,
            NEW_PUBLICATION_ID,
            b"fixture/state/v2".to_vec(),
        ))
    );
    assert_eq!(
        state_presence(
            &database.pool(),
            b"item.old",
            "rating",
            "fixture.unit",
            "score",
        )
        .await,
        "present"
    );
    assert_eq!(
        state_presence(&database.pool(), b"item.old", "old", "", "").await,
        "deleted"
    );
    assert_eq!(
        state_presence(&database.pool(), b"item.old", "missing", "", "").await,
        "absent"
    );
    assert_eq!(
        state_stream(&database.pool()).await,
        (
            "available".to_owned(),
            Some(b"state/v2".to_vec()),
            Some(NEW_PUBLICATION_ID),
            Some(b"fixture/state/v2".to_vec()),
        )
    );
    assert_eq!(catalog_stream(&database.pool()).await, catalog_before);
    assert_eq!(
        published_item_rows(&database.pool()).await,
        old_catalog_rows()
    );
    assert_eq!(staged_state_rows(&database.pool()).await, []);
    assert_task_completed(&database.pool()).await;

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn incremental_catalog_applies_exact_delta_without_advancing_state() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_incremental_catalog_read(database.pool()).await;
    let repository = PostgresPublicationRepository::new(database.pool());
    let state_before = state_stream(&database.pool()).await;
    let state_rows_before = published_state_rows(&database.pool()).await;

    let fence = repository
        .begin_catalog_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("incremental catalog read attempt starts");
    assert_eq!(fence.read_mode, ReadMode::Incremental);
    assert_eq!(fence.expected_cursor, Some(b"catalog/v1".to_vec()));
    assert_eq!(fence.expected_publication_id, Some(OLD_PUBLICATION_ID));

    repository
        .stage_catalog_batch(StageCatalogBatch {
            attempt_id: READ_ATTEMPT_ID,
            batch: incremental_batch(),
        })
        .await
        .expect("incremental catalog batch stages");
    assert_eq!(
        published_item_rows(&database.pool()).await,
        [
            (
                b"item.old".to_vec(),
                "Old item".to_owned(),
                OLD_PUBLICATION_ID,
            ),
            (
                b"item.stale".to_vec(),
                "Stale item".to_owned(),
                OLD_PUBLICATION_ID,
            ),
        ]
    );
    assert_eq!(
        staged_catalog_items(&database.pool()).await,
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
        staged_catalog_relations(&database.pool()).await,
        [
            (b"group.root".to_vec(), "upsert".to_owned()),
            (b"relation.new".to_vec(), "upsert".to_owned()),
            (b"relation.old".to_vec(), "upsert".to_owned()),
            (b"relation.stale".to_vec(), "delete".to_owned()),
        ]
    );
    assert_eq!(
        staged_unit_bindings(&database.pool()).await,
        [
            (
                "fixture://item/new".to_owned(),
                "upsert".to_owned(),
                Some(0)
            ),
            (
                "fixture://item/old".to_owned(),
                "upsert".to_owned(),
                Some(4)
            ),
            ("fixture://item/stale".to_owned(), "delete".to_owned(), None),
            (
                "fixture://relation/root".to_owned(),
                "upsert".to_owned(),
                Some(1)
            ),
        ]
    );

    repository
        .publish_incremental_catalog(PublishRead {
            attempt_id: READ_ATTEMPT_ID,
            publication_id: NEW_PUBLICATION_ID,
            next_cursor: Some(b"catalog/v2".to_vec()),
            evidence_revision: b"fixture/catalog/v2".to_vec(),
            observed_at: timestamp(10),
            published_at: timestamp(11),
        })
        .await
        .expect("incremental catalog publishes atomically");

    assert_eq!(
        published_item_rows(&database.pool()).await,
        [
            (
                b"item.new".to_vec(),
                "Added item".to_owned(),
                NEW_PUBLICATION_ID,
            ),
            (
                b"item.old".to_vec(),
                "Updated item".to_owned(),
                NEW_PUBLICATION_ID,
            ),
        ]
    );
    assert_eq!(
        published_asset_keys(&database.pool()).await,
        [b"asset.new".to_vec(), b"asset.updated".to_vec()]
    );
    assert_eq!(
        published_relation_keys(&database.pool()).await,
        [
            b"group.root".to_vec(),
            b"relation.new".to_vec(),
            b"relation.old".to_vec(),
        ]
    );
    assert_eq!(
        published_unit_bindings(&database.pool()).await,
        [
            (
                "fixture://item/new".to_owned(),
                "provider_item".to_owned(),
                b"item.new".to_vec(),
                0,
                NEW_PUBLICATION_ID,
            ),
            (
                "fixture://item/old".to_owned(),
                "provider_item".to_owned(),
                b"item.old".to_vec(),
                4,
                NEW_PUBLICATION_ID,
            ),
            (
                "fixture://relation/root".to_owned(),
                "catalog_relation".to_owned(),
                b"group.root".to_vec(),
                1,
                NEW_PUBLICATION_ID,
            ),
        ]
    );
    assert_eq!(state_stream(&database.pool()).await, state_before);
    assert_eq!(
        published_state_rows(&database.pool()).await,
        state_rows_before
    );
    assert_eq!(staged_catalog_items(&database.pool()).await, []);
    assert_eq!(staged_catalog_relations(&database.pool()).await, []);
    let candidates = PostgresCatalogQueryRepository::new(database.pool())
        .resolve_subject_units(ResolveSubjectUnits {
            workspace_id: catalog_publication_support::WORKSPACE_ID,
            source_id: SOURCE_ID,
            subject: StateSubject::ProviderItem(
                ProviderItemKey::new("fixture.item", b"item.old".to_vec())
                    .expect("published binding subject is valid"),
            ),
        })
        .await
        .expect("published subject unit bindings are queryable");
    assert_eq!(candidates.publication_id, NEW_PUBLICATION_ID);
    assert_eq!(candidates.candidates.len(), 1);
    assert_eq!(candidates.candidates[0].unit_key, "fixture://item/old");
    assert_eq!(candidates.candidates[0].relevance_rank, 4);
    assert_task_completed(&database.pool()).await;

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
    seed_state_stream(pool.clone(), now).await;
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
    sqlx::query(
        "INSERT INTO subject_unit_bindings (
               source_id, unit_key, subject_kind,
             subject_key_namespace, subject_key_value,
             relevance_rank, publication_id, updated_at
         ) VALUES
             ($1, 'fixture://item/old', 'provider_item', 'fixture.item', $2, 9, $4, $5),
             ($1, 'fixture://item/stale', 'provider_item', 'fixture.item', $3, 2, $4, $5)",
    )
    .bind(SOURCE_ID)
    .bind(b"item.old".as_slice())
    .bind(b"item.stale".as_slice())
    .bind(OLD_PUBLICATION_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("subject unit bindings are seeded");
}

async fn seed_incremental_state_read(pool: PgPool) {
    seed_catalog_read(pool.clone()).await;
    let now = timestamp(-10);
    sqlx::query("UPDATE tasks SET kind = 'state_read' WHERE id = $1")
        .bind(TASK_ID)
        .execute(&pool)
        .await
        .expect("state read task kind is seeded");
    sqlx::query(
        "UPDATE read_tasks
         SET stream_kind = 'state', read_mode = 'incremental'
         WHERE task_id = $1",
    )
    .bind(TASK_ID)
    .execute(&pool)
    .await
    .expect("incremental state read task detail is seeded");
    seed_state_stream(pool.clone(), now).await;
    sqlx::query(
        "INSERT INTO state_observations (
             source_id, subject_kind, subject_key_namespace, subject_key_value,
             field_namespace, field_name, unit_namespace, unit_name,
             value, provider_revision, observed_at, publication_id, updated_at
         ) VALUES
             (
                 $1, 'provider_item', 'fixture.item', $2,
                 'fixture.state', 'old', '', '',
                 $3, $4, $5, $6, $5
             ),
             (
                 $1, 'provider_item', 'fixture.item', $2,
                 'fixture.state', 'progress', 'fixture.unit', 'count',
                 $7, $8, $5, $6, $5
             ),
             (
                 $1, 'provider_item', 'fixture.item', $2,
                 'fixture.state', 'untouched', 'fixture.unit', 'flag',
                 $9, $10, $5, $6, $5
             )",
    )
    .bind(SOURCE_ID)
    .bind(b"item.old".as_slice())
    .bind(json!({"kind": "text", "value": "stale"}))
    .bind(b"state/old/v1".as_slice())
    .bind(now)
    .bind(STATE_PUBLICATION_ID)
    .bind(json!({"kind": "integer", "value": 1}))
    .bind(b"state/progress/v1".as_slice())
    .bind(json!({"kind": "boolean", "value": true}))
    .bind(b"state/untouched/v1".as_slice())
    .execute(&pool)
    .await
    .expect("incremental state observations are seeded");
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

async fn seed_state_stream(pool: PgPool, now: time::OffsetDateTime) {
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
}

fn incremental_batch() -> CatalogBatch {
    let updated_key = ProviderItemKey::new("fixture.item", b"item.old".to_vec())
        .expect("updated provider item key is valid");
    let added_key = ProviderItemKey::new("fixture.item", b"item.new".to_vec())
        .expect("added provider item key is valid");
    let updated = CatalogItem::new(
        updated_key.clone(),
        Term::new("fixture", "episode").expect("updated item kind is valid"),
        "Updated item",
        json!({"revision": 2}),
        vec![BinaryAssetReference::new(
            BinaryAssetKey::new("fixture.asset", b"asset.updated".to_vec())
                .expect("updated binary asset key is valid"),
            Term::new("fixture", "poster").expect("updated asset kind is valid"),
        )],
    )
    .expect("updated catalog item is valid");
    let added = CatalogItem::new(
        added_key.clone(),
        Term::new("fixture", "episode").expect("added item kind is valid"),
        "Added item",
        json!({}),
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
        Some(updated_key.clone()),
        Term::new("fixture", "member").expect("updated relation kind is valid"),
        vec![10],
        json!({}),
    )
    .expect("updated catalog relation is valid");
    let added_relation = CatalogRelation::new(
        CatalogRelationKey::new("fixture.relation", b"relation.new".to_vec())
            .expect("added relation key is valid"),
        Some(root_key.clone()),
        Some(added_key.clone()),
        Term::new("fixture", "member").expect("added relation kind is valid"),
        vec![20],
        json!({}),
    )
    .expect("added catalog relation is valid");
    let unit_binding_upserts = vec![
        unit_binding(
            "fixture://item/old",
            StateSubject::ProviderItem(updated_key.clone()),
            4,
        ),
        unit_binding(
            "fixture://item/new",
            StateSubject::ProviderItem(added_key.clone()),
            0,
        ),
        unit_binding(
            "fixture://relation/root",
            StateSubject::CatalogRelation(root_key.clone()),
            1,
        ),
    ];
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
        unit_binding_upserts,
        vec![unit_binding_key(
            "fixture://item/stale",
            StateSubject::ProviderItem(
                ProviderItemKey::new("fixture.item", b"item.stale".to_vec())
                    .expect("deleted mapping subject is valid"),
            ),
        )],
    )
}

fn unit_binding(unit_key: &str, subject: StateSubject, relevance_rank: u32) -> SubjectUnitBinding {
    SubjectUnitBinding::new(unit_binding_key(unit_key, subject), relevance_rank)
}

fn unit_binding_key(unit_key: &str, subject: StateSubject) -> SubjectUnitBindingKey {
    SubjectUnitBindingKey::new(unit_key, subject).expect("mapping unit binding key is valid")
}

fn incremental_state_batch() -> StateBatch {
    let subject = StateSubject::ProviderItem(
        ProviderItemKey::new("fixture.item", b"item.old".to_vec())
            .expect("incremental state provider item key is valid"),
    );
    let progress = StateObservation::set(
        subject.clone(),
        StateField::new(
            Term::new("fixture.state", "progress").expect("progress field is valid"),
            Some(Term::new("fixture.unit", "count").expect("progress unit is valid")),
        ),
        StateValue::Integer(8),
        b"state/progress/v2".to_vec(),
        timestamp(8),
    )
    .expect("updated progress observation is valid");
    let rating = StateObservation::set(
        subject.clone(),
        StateField::new(
            Term::new("fixture.state", "rating").expect("rating field is valid"),
            Some(Term::new("fixture.unit", "score").expect("rating unit is valid")),
        ),
        StateValue::Decimal("8.5".to_owned()),
        b"state/rating/v1".to_vec(),
        timestamp(9),
    )
    .expect("added rating observation is valid");
    let deleted = StateObservation::delete(
        subject,
        StateField::new(
            Term::new("fixture.state", "old").expect("deleted field is valid"),
            None,
        ),
        b"state/old/v2".to_vec(),
        timestamp(9),
    )
    .expect("deleted state observation is valid");
    StateBatch::new(0, vec![progress, rating, deleted])
}

fn old_catalog_rows() -> Vec<(Vec<u8>, String, Uuid)> {
    vec![(
        b"item.old".to_vec(),
        "Old item".to_owned(),
        OLD_PUBLICATION_ID,
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

async fn staged_catalog_items(pool: &PgPool) -> Vec<(Vec<u8>, String, Option<String>)> {
    sqlx::query_as::<_, (Vec<u8>, String, Option<String>)>(
        "SELECT key_value, operation, display_name
         FROM staged_catalog_items
         WHERE attempt_id = $1
         ORDER BY key_value",
    )
    .bind(READ_ATTEMPT_ID)
    .fetch_all(pool)
    .await
    .expect("staged catalog items are queryable")
}

async fn staged_catalog_relations(pool: &PgPool) -> Vec<(Vec<u8>, String)> {
    sqlx::query_as::<_, (Vec<u8>, String)>(
        "SELECT key_value, operation
         FROM staged_catalog_relations
         WHERE attempt_id = $1
         ORDER BY key_value",
    )
    .bind(READ_ATTEMPT_ID)
    .fetch_all(pool)
    .await
    .expect("staged catalog relations are queryable")
}

async fn staged_unit_bindings(pool: &PgPool) -> Vec<(String, String, Option<i64>)> {
    sqlx::query_as(
        "SELECT unit_key, operation, relevance_rank
         FROM staged_subject_unit_bindings
         ORDER BY unit_key",
    )
    .fetch_all(pool)
    .await
    .expect("staged subject unit bindings are readable")
}

async fn published_unit_bindings(pool: &PgPool) -> Vec<(String, String, Vec<u8>, i64, Uuid)> {
    sqlx::query_as(
        "SELECT unit_key, subject_kind, subject_key_value, relevance_rank, publication_id
         FROM subject_unit_bindings
         ORDER BY unit_key",
    )
    .fetch_all(pool)
    .await
    .expect("published subject unit bindings are readable")
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

async fn assert_task_completed(pool: &PgPool) {
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM tasks WHERE id = $1")
            .bind(TASK_ID)
            .fetch_one(pool)
            .await
            .expect("task status is queryable"),
        "succeeded"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM task_attempts WHERE id = $1")
            .bind(TASK_ATTEMPT_ID)
            .fetch_one(pool)
            .await
            .expect("task attempt status is queryable"),
        "succeeded"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM read_attempts WHERE id = $1")
            .bind(READ_ATTEMPT_ID)
            .fetch_one(pool)
            .await
            .expect("read attempt status is queryable"),
        "published"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>("SELECT owner_id FROM task_claims WHERE task_id = $1")
            .bind(TASK_ID)
            .fetch_optional(pool)
            .await
            .expect("task claim owner is queryable"),
        None
    );
}
