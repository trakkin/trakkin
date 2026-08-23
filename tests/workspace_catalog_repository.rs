#![cfg(feature = "embedded-database")]

use serde_json::json;
use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::{
        catalog_queries::{CatalogQueryRepository, InspectSourceState, QueryCatalogPage},
        workspaces::{
            AttachWorkspaceSource, CreateWorkspace, DetachWorkspaceSource, WorkspaceRepository,
            WorkspaceService,
        },
    },
    database::DatabaseRuntime,
    domain::acquisition::{CatalogRelationKey, ProviderItemKey, StateSubject, StateValue},
    infrastructure::{
        catalog_queries::{CatalogQueryError, PostgresCatalogQueryRepository},
        workspaces::{PostgresWorkspaceRepository, WorkspaceError},
    },
};
use uuid::Uuid;

const CONNECTION_ID: Uuid = Uuid::from_u128(1);
const ACCOUNT_ID: Uuid = Uuid::from_u128(2);
const SOURCE_ID: Uuid = Uuid::from_u128(3);
const WORKSPACE_ID: Uuid = Uuid::from_u128(4);
const SECOND_WORKSPACE_ID: Uuid = Uuid::from_u128(5);
const CATALOG_PUBLICATION_ID: Uuid = Uuid::from_u128(6);
const NEXT_CATALOG_PUBLICATION_ID: Uuid = Uuid::from_u128(7);
const STATE_PUBLICATION_ID: Uuid = Uuid::from_u128(8);

#[tokio::test]
async fn workspace_crud_and_source_attachments_are_idempotent() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_source(&database.pool()).await;
    let repository = PostgresWorkspaceRepository::new(database.pool());
    let service = WorkspaceService::new(repository.clone());

    let workspace = service
        .create(CreateWorkspace {
            workspace_id: WORKSPACE_ID,
            name: "  Main workspace  ".to_owned(),
            created_at: timestamp(0),
        })
        .await
        .expect("workspace is created");
    assert_eq!(workspace.name, "Main workspace");
    service
        .create(CreateWorkspace {
            workspace_id: SECOND_WORKSPACE_ID,
            name: "Second workspace".to_owned(),
            created_at: timestamp(1),
        })
        .await
        .expect("second workspace is created");
    assert_eq!(
        service
            .list()
            .await
            .expect("workspaces are listed")
            .into_iter()
            .map(|workspace| workspace.id)
            .collect::<Vec<_>>(),
        [WORKSPACE_ID, SECOND_WORKSPACE_ID]
    );
    assert_eq!(
        service
            .get(WORKSPACE_ID)
            .await
            .expect("workspace is queried"),
        Some(workspace)
    );
    assert!(matches!(
        service
            .create(CreateWorkspace {
                workspace_id: WORKSPACE_ID,
                name: "Duplicate".to_owned(),
                created_at: timestamp(2),
            })
            .await,
        Err(WorkspaceError::AlreadyExists)
    ));

    let attachment = service
        .attach(AttachWorkspaceSource {
            workspace_id: WORKSPACE_ID,
            source_id: SOURCE_ID,
            attached_at: timestamp(3),
        })
        .await
        .expect("source is attached");
    let repeated = service
        .attach(AttachWorkspaceSource {
            workspace_id: WORKSPACE_ID,
            source_id: SOURCE_ID,
            attached_at: timestamp(4),
        })
        .await
        .expect("repeated attachment is idempotent");
    assert_eq!(repeated, attachment);
    let shared_attachment = service
        .attach(AttachWorkspaceSource {
            workspace_id: SECOND_WORKSPACE_ID,
            source_id: SOURCE_ID,
            attached_at: timestamp(5),
        })
        .await
        .expect("source is shared with a second workspace");
    assert_eq!(
        service
            .attachments(WORKSPACE_ID)
            .await
            .expect("attachments are listed"),
        [attachment]
    );
    assert!(
        service
            .detach(DetachWorkspaceSource {
                workspace_id: WORKSPACE_ID,
                source_id: SOURCE_ID,
            })
            .await
            .expect("source is detached")
    );
    assert!(
        !service
            .detach(DetachWorkspaceSource {
                workspace_id: WORKSPACE_ID,
                source_id: SOURCE_ID,
            })
            .await
            .expect("repeated detachment is idempotent")
    );
    assert!(
        service
            .attachments(WORKSPACE_ID)
            .await
            .expect("detached workspace is empty")
            .is_empty()
    );
    assert_eq!(
        service
            .attachments(SECOND_WORKSPACE_ID)
            .await
            .expect("shared attachment remains visible"),
        [shared_attachment]
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn attached_workspace_pages_catalog_and_inspects_state() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_published_source(&database.pool()).await;
    let repository = PostgresCatalogQueryRepository::new(database.pool());

    let first = repository
        .query_catalog_page(catalog_query(None, None, 1, None))
        .await
        .expect("first root page is queried");
    assert_eq!(first.publication_id, CATALOG_PUBLICATION_ID);
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].relation.key().value(), b"relation.series");
    assert_eq!(
        first.entries[0]
            .item
            .as_ref()
            .expect("series relation has an item")
            .display_name(),
        "Northstar"
    );
    let second = repository
        .query_catalog_page(catalog_query(None, None, 1, first.next_cursor))
        .await
        .expect("second root page is queried");
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].relation.key().value(), b"relation.movie");
    assert!(second.next_cursor.is_none());

    let parent = CatalogRelationKey::new("fixture.relation", b"relation.series".to_vec())
        .expect("parent key is valid");
    let children = repository
        .query_catalog_page(catalog_query(Some(parent.clone()), None, 10, None))
        .await
        .expect("child page is queried");
    assert_eq!(
        children
            .entries
            .iter()
            .map(|entry| {
                entry
                    .item
                    .as_ref()
                    .expect("child relation has an item")
                    .display_name()
            })
            .collect::<Vec<_>>(),
        ["Pilot", "Quiet Signal"]
    );
    let search = repository
        .query_catalog_page(catalog_query(
            Some(parent),
            Some("  SIGNAL ".to_owned()),
            10,
            None,
        ))
        .await
        .expect("child search is queried");
    assert_eq!(search.entries.len(), 1);
    let matched = search.entries[0]
        .item
        .as_ref()
        .expect("search match has an item");
    assert_eq!(matched.display_name(), "Quiet Signal");
    assert_eq!(matched.portable_references().len(), 1);
    assert_eq!(matched.binary_assets().len(), 1);

    let state = repository
        .inspect_source_state(InspectSourceState {
            workspace_id: WORKSPACE_ID,
            source_id: SOURCE_ID,
            subject: Some(StateSubject::ProviderItem(
                ProviderItemKey::new("fixture.item", b"item.signal".to_vec())
                    .expect("state subject is valid"),
            )),
        })
        .await
        .expect("source state is inspected");
    assert_eq!(state.publication_id, STATE_PUBLICATION_ID);
    assert_eq!(state.observations.len(), 1);
    assert_eq!(state.observations[0].value(), Some(&StateValue::Integer(7)));

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn catalog_cursor_expires_when_publication_changes_and_detachment_revokes_visibility() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_published_source(&database.pool()).await;
    let repository = PostgresCatalogQueryRepository::new(database.pool());
    let first = repository
        .query_catalog_page(catalog_query(None, None, 1, None))
        .await
        .expect("first page is queried");
    let cursor = first.next_cursor.expect("first page has a continuation");

    sqlx::query(
        "UPDATE source_streams
SET publication_id = $1, updated_at = $2
WHERE source_id = $3 AND stream_kind = 'catalog'
",
    )
    .bind(NEXT_CATALOG_PUBLICATION_ID)
    .bind(timestamp(20))
    .bind(SOURCE_ID)
    .execute(&database.pool())
    .await
    .expect("catalog publication advances");
    assert!(matches!(
        repository
            .query_catalog_page(catalog_query(None, None, 1, Some(cursor)))
            .await,
        Err(CatalogQueryError::CursorExpired)
    ));

    PostgresWorkspaceRepository::new(database.pool())
        .detach_source(DetachWorkspaceSource {
            workspace_id: WORKSPACE_ID,
            source_id: SOURCE_ID,
        })
        .await
        .expect("source is detached");
    assert!(matches!(
        repository
            .query_catalog_page(catalog_query(None, None, 1, None))
            .await,
        Err(CatalogQueryError::SourceNotAttached)
    ));

    database.shutdown().await.expect("database shuts down");
}

fn catalog_query(
    parent: Option<CatalogRelationKey>,
    search: Option<String>,
    limit: u32,
    cursor: Option<String>,
) -> QueryCatalogPage {
    QueryCatalogPage {
        workspace_id: WORKSPACE_ID,
        source_id: SOURCE_ID,
        parent,
        search,
        limit,
        cursor,
    }
}

async fn seed_published_source(pool: &sqlx::PgPool) {
    seed_source(pool).await;
    let now = timestamp(-10);
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES ($1, 'Workspace', 'active', $2, $2)
",
    )
    .bind(WORKSPACE_ID)
    .bind(now)
    .execute(pool)
    .await
    .expect("workspace is seeded");
    sqlx::query(
        "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
VALUES ($1, $2, $3)
",
    )
    .bind(WORKSPACE_ID)
    .bind(SOURCE_ID)
    .bind(now)
    .execute(pool)
    .await
    .expect("attachment is seeded");
    sqlx::query(
        "INSERT INTO source_streams (
    source_id, stream_kind, status, cursor, publication_id,
    evidence_revision, last_success_at, updated_at
)
VALUES
    ($1, 'catalog', 'available', $2, $3, $4, $5, $5),
    ($1, 'state', 'available', $6, $7, $8, $5, $5)
",
    )
    .bind(SOURCE_ID)
    .bind(b"catalog/v1".as_slice())
    .bind(CATALOG_PUBLICATION_ID)
    .bind(b"fixture/catalog/v1".as_slice())
    .bind(now)
    .bind(b"state/v1".as_slice())
    .bind(STATE_PUBLICATION_ID)
    .bind(b"fixture/state/v1".as_slice())
    .execute(pool)
    .await
    .expect("source streams are seeded");

    insert_item(pool, b"item.series", "series", "Northstar", now).await;
    insert_item(pool, b"item.pilot", "episode", "Pilot", now).await;
    insert_item(pool, b"item.signal", "episode", "Quiet Signal", now).await;
    insert_item(pool, b"item.movie", "movie", "Solaris", now).await;
    insert_relation(pool, b"relation.series", None, b"item.series", vec![0], now).await;
    insert_relation(
        pool,
        b"relation.pilot",
        Some(b"relation.series"),
        b"item.pilot",
        vec![0, 1],
        now,
    )
    .await;
    insert_relation(
        pool,
        b"relation.signal",
        Some(b"relation.series"),
        b"item.signal",
        vec![0, 2],
        now,
    )
    .await;
    insert_relation(pool, b"relation.movie", None, b"item.movie", vec![1], now).await;
    sqlx::query(
        "INSERT INTO catalog_item_references (
    source_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value
)
VALUES ($1, 'fixture.item', $2, 'example.media', $3)
",
    )
    .bind(SOURCE_ID)
    .bind(b"item.signal".as_slice())
    .bind(b"quiet-signal".as_slice())
    .execute(pool)
    .await
    .expect("portable reference is seeded");
    sqlx::query(
        "INSERT INTO catalog_assets (
    source_id, item_key_namespace, item_key_value,
    asset_key_namespace, asset_key_value,
    kind_namespace, kind_name, publication_id
)
VALUES (
    $1, 'fixture.item', $2, 'fixture.asset', $3,
    'fixture', 'poster', $4
)
",
    )
    .bind(SOURCE_ID)
    .bind(b"item.signal".as_slice())
    .bind(b"asset.signal.poster".as_slice())
    .bind(CATALOG_PUBLICATION_ID)
    .execute(pool)
    .await
    .expect("binary asset reference is seeded");
    sqlx::query(
        "INSERT INTO state_observations (
    source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    value, provider_revision, observed_at, publication_id, updated_at
)
VALUES (
    $1, 'provider_item', 'fixture.item', $2,
    'fixture.state', 'progress', 'fixture.unit', 'count',
    $3, $4, $5, $6, $5
)
",
    )
    .bind(SOURCE_ID)
    .bind(b"item.signal".as_slice())
    .bind(json!({"kind": "integer", "value": 7}))
    .bind(b"state/item.signal/v1".as_slice())
    .bind(now)
    .bind(STATE_PUBLICATION_ID)
    .execute(pool)
    .await
    .expect("state observation is seeded");
}

async fn seed_source(pool: &sqlx::PgPool) {
    let now = timestamp(-10);
    sqlx::query(
        "INSERT INTO connections (
    id, adapter_key, display_name, status, capabilities,
    capability_revision, created_at, updated_at
)
VALUES ($1, 'fixture', 'Fixture', 'ready', $2, 1, $3, $3)
",
    )
    .bind(CONNECTION_ID)
    .bind(json!({}))
    .bind(now)
    .execute(pool)
    .await
    .expect("connection is seeded");
    sqlx::query(
        "INSERT INTO accounts (
    id, connection_id, provider_key_namespace, provider_key_value,
    display_name, first_seen_at, last_seen_at
)
VALUES ($1, $2, 'fixture.account', $3, 'Fixture account', $4, $4)
",
    )
    .bind(ACCOUNT_ID)
    .bind(CONNECTION_ID)
    .bind(b"account".as_slice())
    .bind(now)
    .execute(pool)
    .await
    .expect("account is seeded");
    sqlx::query(
        "INSERT INTO sources (
    id, connection_id, account_id,
    provider_key_namespace, provider_key_value,
    display_name, kind_namespace, kind_name,
    availability, capabilities, capability_revision,
    first_seen_at, last_seen_at
)
VALUES (
    $1, $2, $3, 'fixture.source', $4,
    'Fixture source', 'fixture', 'catalog',
    'available', $5, 1, $6, $6
)
",
    )
    .bind(SOURCE_ID)
    .bind(CONNECTION_ID)
    .bind(ACCOUNT_ID)
    .bind(b"source".as_slice())
    .bind(json!({
        "catalog": {"full": true, "incremental": true},
        "state": {"full": true, "incremental": true},
    }))
    .bind(now)
    .execute(pool)
    .await
    .expect("source is seeded");
}

async fn insert_item(
    pool: &sqlx::PgPool,
    key: &[u8],
    kind: &str,
    display_name: &str,
    now: OffsetDateTime,
) {
    sqlx::query(
        "INSERT INTO catalog_items (
    source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, publication_id, updated_at
)
VALUES ($1, 'fixture.item', $2, 'fixture', $3, $4, $5, $6, $7)
",
    )
    .bind(SOURCE_ID)
    .bind(key)
    .bind(kind)
    .bind(display_name)
    .bind(json!({}))
    .bind(CATALOG_PUBLICATION_ID)
    .bind(now)
    .execute(pool)
    .await
    .expect("catalog item is seeded");
}

async fn insert_relation(
    pool: &sqlx::PgPool,
    key: &[u8],
    parent: Option<&[u8]>,
    item: &[u8],
    order: Vec<i64>,
    now: OffsetDateTime,
) {
    sqlx::query(
        "INSERT INTO catalog_relations (
    source_id, key_namespace, key_value,
    parent_key_namespace, parent_key_value,
    item_key_namespace, item_key_value,
    kind_namespace, kind_name, order_parts, attributes,
    publication_id, updated_at
)
VALUES (
    $1, 'fixture.relation', $2,
    CASE WHEN $3::BYTEA IS NULL THEN NULL ELSE 'fixture.relation' END, $3,
    'fixture.item', $4,
    'fixture', 'member', $5, $6, $7, $8
)
",
    )
    .bind(SOURCE_ID)
    .bind(key)
    .bind(parent)
    .bind(item)
    .bind(order)
    .bind(json!({}))
    .bind(CATALOG_PUBLICATION_ID)
    .bind(now)
    .execute(pool)
    .await
    .expect("catalog relation is seeded");
}

fn timestamp(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_900_000_000 + second).expect("test timestamp is valid")
}
