use serde_json::json;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

pub const SOURCE_ID: Uuid = Uuid::from_u128(3);
pub const WORKSPACE_ID: Uuid = Uuid::from_u128(4);
pub const TASK_ID: Uuid = Uuid::from_u128(5);
pub const TASK_ATTEMPT_ID: Uuid = Uuid::from_u128(6);
pub const TASK_OWNER_ID: Uuid = Uuid::from_u128(7);
pub const READ_ATTEMPT_ID: Uuid = Uuid::from_u128(8);
pub const OLD_PUBLICATION_ID: Uuid = Uuid::from_u128(9);
pub const NEW_PUBLICATION_ID: Uuid = Uuid::from_u128(10);
pub const STATE_PUBLICATION_ID: Uuid = Uuid::from_u128(11);

const CONNECTION_ID: Uuid = Uuid::from_u128(1);
const ACCOUNT_ID: Uuid = Uuid::from_u128(2);

pub async fn seed_catalog_read(pool: PgPool) {
    let now = timestamp(-10);
    sqlx::query(
        "INSERT INTO connections (
             id, adapter_key, display_name, status, capabilities,
             capability_revision, created_at, updated_at
         ) VALUES ($1, 'fixture', 'Fixture', 'ready', $2, 1, $3, $3)",
    )
    .bind(CONNECTION_ID)
    .bind(json!({}))
    .bind(now)
    .execute(&pool)
    .await
    .expect("connection is seeded");
    sqlx::query(
        "INSERT INTO accounts (
             id, connection_id, provider_key_namespace, provider_key_value,
             display_name, first_seen_at, last_seen_at
         ) VALUES ($1, $2, 'fixture.account', $3, 'Fixture account', $4, $4)",
    )
    .bind(ACCOUNT_ID)
    .bind(CONNECTION_ID)
    .bind(b"account".as_slice())
    .bind(now)
    .execute(&pool)
    .await
    .expect("account is seeded");
    sqlx::query(
        "INSERT INTO sources (
             id, connection_id, account_id,
             provider_key_namespace, provider_key_value,
             display_name, kind_namespace, kind_name,
             availability, capabilities, capability_revision,
             first_seen_at, last_seen_at
         ) VALUES (
             $1, $2, $3, 'fixture.source', $4,
             'Fixture source', 'fixture', 'catalog',
             'available', $5, 4, $6, $6
         )",
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
    .execute(&pool)
    .await
    .expect("source is seeded");
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
         VALUES ($1, 'Workspace', 'active', $2, $2)",
    )
    .bind(WORKSPACE_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("workspace is seeded");
    sqlx::query(
        "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
         VALUES ($1, $2, $3)",
    )
    .bind(WORKSPACE_ID)
    .bind(SOURCE_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("workspace source is seeded");
    sqlx::query(
        "INSERT INTO source_streams (
             source_id, stream_kind, status, cursor, publication_id,
             evidence_revision, last_success_at, updated_at
         ) VALUES ($1, 'catalog', 'available', $2, $3, $4, $5, $5)",
    )
    .bind(SOURCE_ID)
    .bind(b"catalog/v1".as_slice())
    .bind(OLD_PUBLICATION_ID)
    .bind(b"fixture/catalog/v1".as_slice())
    .bind(now)
    .execute(&pool)
    .await
    .expect("catalog stream is seeded");
    sqlx::query(
        "INSERT INTO catalog_items (
             source_id, key_namespace, key_value, kind_namespace, kind_name,
             display_name, attributes, publication_id, updated_at
         ) VALUES ($1, 'fixture.item', $2, 'fixture', 'episode', 'Old item', $3, $4, $5)",
    )
    .bind(SOURCE_ID)
    .bind(b"item.old".as_slice())
    .bind(json!({}))
    .bind(OLD_PUBLICATION_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("old catalog item is seeded");
    sqlx::query(
        "INSERT INTO catalog_item_references (
             source_id, item_key_namespace, item_key_value,
             reference_namespace, reference_value
         ) VALUES ($1, 'fixture.item', $2, 'example.media', $3)",
    )
    .bind(SOURCE_ID)
    .bind(b"item.old".as_slice())
    .bind(b"old".as_slice())
    .execute(&pool)
    .await
    .expect("old portable reference is seeded");
    sqlx::query(
        "INSERT INTO catalog_assets (
             source_id, item_key_namespace, item_key_value,
             asset_key_namespace, asset_key_value,
             kind_namespace, kind_name, publication_id
         ) VALUES ($1, 'fixture.item', $2, 'fixture.asset', $3, 'fixture', 'poster', $4)",
    )
    .bind(SOURCE_ID)
    .bind(b"item.old".as_slice())
    .bind(b"asset.old".as_slice())
    .bind(OLD_PUBLICATION_ID)
    .execute(&pool)
    .await
    .expect("old asset locator is seeded");
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
    .bind(b"relation.old".as_slice())
    .bind(b"item.old".as_slice())
    .bind(Vec::<i64>::new())
    .bind(json!({}))
    .bind(OLD_PUBLICATION_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("old catalog relation is seeded");
    sqlx::query(
        "INSERT INTO tasks (
             id, workspace_id, connection_id, kind, status,
             cancellation_supported, created_at, started_at, updated_at
         ) VALUES ($1, $2, $3, 'catalog_read', 'running', TRUE, $4, $4, $4)",
    )
    .bind(TASK_ID)
    .bind(WORKSPACE_ID)
    .bind(CONNECTION_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("catalog read task is seeded");
    sqlx::query(
        "INSERT INTO task_claims (task_id, owner_id, claimed_at, heartbeat_at)
         VALUES ($1, $2, $3, $3)",
    )
    .bind(TASK_ID)
    .bind(TASK_OWNER_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("task claim is seeded");
    sqlx::query(
        "INSERT INTO task_attempts (id, task_id, ordinal, status, started_at)
         VALUES ($1, $2, 0, 'running', $3)",
    )
    .bind(TASK_ATTEMPT_ID)
    .bind(TASK_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("task attempt is seeded");
    sqlx::query(
        "INSERT INTO read_tasks (task_id, source_id, stream_kind, read_mode)
         VALUES ($1, $2, 'catalog', 'full')",
    )
    .bind(TASK_ID)
    .bind(SOURCE_ID)
    .execute(&pool)
    .await
    .expect("read task detail is seeded");
}

pub fn timestamp(offset_seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_893_456_000 + offset_seconds)
        .expect("fixture timestamp is in range")
}
