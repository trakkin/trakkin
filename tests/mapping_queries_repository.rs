#![cfg(feature = "embedded-database")]

use tempfile::tempdir;
use time::{Duration, OffsetDateTime};
use trakkin::{
    application::{
        mapping_lifecycle::{
            ChangeMappingActivation, CreateMapping, MappingActivationKind, MappingLifecycleService,
            UpdateMapping,
        },
        mapping_queries::{MappingQueryRepository, QueryMappingHistory, QueryMappingRegistry},
    },
    database::DatabaseRuntime,
    infrastructure::{
        mapping_lifecycle::PostgresMappingLifecycleRepository,
        mapping_queries::{PostgresMappingQueryError, PostgresMappingQueryRepository},
    },
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_mapping_queries_are_bounded_scoped_and_hash_native() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let queries = PostgresMappingQueryRepository::new(pool.clone());
    let now = OffsetDateTime::from_unix_timestamp(1_893_456_245).expect("fixture time");
    let active_workspace_id = id(1);
    let retired_workspace_id = id(2);
    for (workspace_id, name) in [
        (active_workspace_id, "Active mappings"),
        (retired_workspace_id, "Retired mappings"),
    ] {
        sqlx::query(
            "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES ($1, $2, 'active', $3, $3)
",
        )
        .bind(workspace_id)
        .bind(name)
        .bind(now)
        .execute(&pool)
        .await
        .expect("workspace should be inserted");
    }

    let mut mapping_ids = Vec::new();
    let mut first_hash = None;
    let mut head_hash = None;
    for index in 0..27_u128 {
        let mapping_id = id(1_000 + index);
        mapping_ids.push(mapping_id);
        let created_at =
            now + Duration::seconds(i64::try_from(index).expect("small fixture index"));
        let created = lifecycle
            .create(CreateMapping {
                mapping_id,
                mappings: vec![vec![
                    format!("example.media://item-{index}"),
                    format!("example.alt://item-{index}"),
                ]],
                created_at,
            })
            .await
            .expect("mapping should be created");
        if index == 0 {
            first_hash = Some(created.content_hash.clone());
            let updated = lifecycle
                .update(UpdateMapping {
                    mapping_id,
                    base_content_hash: created.content_hash,
                    mappings: vec![
                        vec![
                            "example.media://item-0".to_owned(),
                            "example.alt://item-0".to_owned(),
                        ],
                        vec![
                            "example.media://bonus-0".to_owned(),
                            "example.alt://bonus-0".to_owned(),
                        ],
                    ],
                    updated_at: created_at + Duration::milliseconds(1),
                })
                .await
                .expect("mapping should be updated");
            head_hash = Some(updated.content_hash);
        }
    }
    let mapping_id = mapping_ids[0];
    let first_hash = first_hash.expect("first hash should be recorded");
    let head_hash = head_hash.expect("head hash should be recorded");

    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(10_000),
            workspace_id: active_workspace_id,
            mapping_id,
            content_hash: Some(head_hash.clone()),
            kind: MappingActivationKind::Activate,
            changed_at: now + Duration::minutes(1),
        })
        .await
        .expect("head should activate in the first workspace");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(10_001),
            workspace_id: retired_workspace_id,
            mapping_id,
            content_hash: Some(head_hash.clone()),
            kind: MappingActivationKind::Activate,
            changed_at: now + Duration::minutes(1),
        })
        .await
        .expect("head should activate in the second workspace");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(10_002),
            workspace_id: retired_workspace_id,
            mapping_id,
            content_hash: None,
            kind: MappingActivationKind::Retire,
            changed_at: now + Duration::minutes(2),
        })
        .await
        .expect("mapping should retire in the second workspace");

    let first_page = queries
        .query_registry(QueryMappingRegistry {
            workspace_id: active_workspace_id,
            limit: 25,
            cursor: None,
        })
        .await
        .expect("first registry page should load");
    assert_eq!(first_page.mappings.len(), 25);
    let next_cursor = first_page
        .next_cursor
        .expect("the 25-item page should have a cursor");
    let second_page = queries
        .query_registry(QueryMappingRegistry {
            workspace_id: active_workspace_id,
            limit: 25,
            cursor: Some(next_cursor.clone()),
        })
        .await
        .expect("second registry page should load");
    assert_eq!(second_page.mappings.len(), 2);
    assert!(second_page.next_cursor.is_none());
    assert!(matches!(
        queries
            .query_registry(QueryMappingRegistry {
                workspace_id: retired_workspace_id,
                limit: 25,
                cursor: Some(next_cursor),
            })
            .await,
        Err(PostgresMappingQueryError::InvalidCursor)
    ));

    let retired_registry = queries
        .query_registry(QueryMappingRegistry {
            workspace_id: retired_workspace_id,
            limit: 100,
            cursor: None,
        })
        .await
        .expect("100-item registry page should load");
    assert_eq!(retired_registry.mappings.len(), 27);
    let retired_summary = retired_registry
        .mappings
        .iter()
        .find(|summary| summary.mapping_id == mapping_id)
        .expect("global mapping should be visible in the second workspace");
    let retired_activation = retired_summary
        .activation
        .as_ref()
        .expect("retired activation should remain visible");
    assert_eq!(retired_activation.generation, 2);
    assert!(retired_activation.active_content_hash.is_none());
    assert!(matches!(
        queries
            .query_registry(QueryMappingRegistry {
                workspace_id: active_workspace_id,
                limit: 101,
                cursor: None,
            })
            .await,
        Err(PostgresMappingQueryError::InvalidLimit)
    ));

    let head = queries
        .query_head(active_workspace_id, mapping_id)
        .await
        .expect("head detail should load");
    assert_eq!(head.revision.content_hash, head_hash);
    assert_eq!(head.revision.document.revision(), 2);
    assert_eq!(head.revision.document.mappings().len(), 2);
    let active_activation = head.activation.expect("active annotation should load");
    assert_eq!(
        active_activation.active_content_hash.as_deref(),
        Some(head_hash.as_str())
    );

    let first_history_page = queries
        .query_history(QueryMappingHistory {
            workspace_id: active_workspace_id,
            mapping_id,
            limit: 1,
            cursor: None,
        })
        .await
        .expect("first history page should load");
    assert_eq!(first_history_page.head_content_hash, head_hash);
    assert_eq!(first_history_page.revisions[0].portable_revision, 2);
    let history_cursor = first_history_page
        .next_cursor
        .expect("one-item history page should have a cursor");
    assert!(matches!(
        queries
            .query_history(QueryMappingHistory {
                workspace_id: active_workspace_id,
                mapping_id: mapping_ids[1],
                limit: 1,
                cursor: Some(history_cursor.clone()),
            })
            .await,
        Err(PostgresMappingQueryError::InvalidCursor)
    ));
    let second_history_page = queries
        .query_history(QueryMappingHistory {
            workspace_id: active_workspace_id,
            mapping_id,
            limit: 1,
            cursor: Some(history_cursor),
        })
        .await
        .expect("second history page should load");
    assert_eq!(second_history_page.revisions[0].portable_revision, 1);
    assert!(second_history_page.next_cursor.is_none());

    let exact = queries
        .query_revision(active_workspace_id, mapping_id, &first_hash)
        .await
        .expect("exact historical revision should load");
    assert_eq!(exact.revision.content_hash, first_hash);
    assert_eq!(exact.revision.document.revision(), 1);
    assert_eq!(exact.revision.document.mappings().len(), 1);

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

const fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
