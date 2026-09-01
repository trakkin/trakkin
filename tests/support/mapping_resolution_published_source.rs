use serde_json::json;
use time::OffsetDateTime;
use trakkin_mapping::{PortableEndpoint, PortableSelector};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub struct PublishedSourceIds {
    pub workspace_id: Uuid,
    pub connection_id: Uuid,
    pub account_id: Uuid,
    pub source_id: Uuid,
    pub catalog_publication_id: Uuid,
    pub coordinate_publication_id: Uuid,
}

pub async fn seed_published_reference(
    pool: &sqlx::PgPool,
    ids: PublishedSourceIds,
    endpoint: &PortableEndpoint,
    item_key: &[u8],
    now: OffsetDateTime,
) {
    sqlx::query(
        "INSERT INTO connections (
    id, provider_id, display_name, status, capabilities,
    capability_revision, created_at, updated_at
) VALUES ($1, 'fixture', 'Fixture', 'ready', $2, 1, $3, $3)
",
    )
    .bind(ids.connection_id)
    .bind(json!({}))
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
    .bind(ids.account_id)
    .bind(ids.connection_id)
    .bind(b"account".as_slice())
    .bind(now)
    .execute(pool)
    .await
    .expect("account is seeded");
    sqlx::query(
        "INSERT INTO sources (
    id, connection_id, account_id, provider_key_namespace, provider_key_value,
    display_name, kind_namespace, kind_name, availability, capabilities,
    capability_revision, first_seen_at, last_seen_at
) VALUES (
    $1, $2, $3, 'fixture.source', $4, 'Fixture source', 'fixture', 'catalog',
    'available', $5, 1, $6, $6
)
",
    )
    .bind(ids.source_id)
    .bind(ids.connection_id)
    .bind(ids.account_id)
    .bind(b"source".as_slice())
    .bind(json!({}))
    .bind(now)
    .execute(pool)
    .await
    .expect("source is seeded");
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES ($1, 'Mappings', 'active', $2, $2)
",
    )
    .bind(ids.workspace_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("workspace is seeded");
    sqlx::query(
        "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
VALUES ($1, $2, $3)
",
    )
    .bind(ids.workspace_id)
    .bind(ids.source_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("source is attached");
    sqlx::query(
        "INSERT INTO source_streams (
    source_id, stream_kind, status, publication_id, evidence_revision,
    last_success_at, updated_at
) VALUES ($1, 'catalog', 'available', $2, $3, $4, $4)
",
    )
    .bind(ids.source_id)
    .bind(ids.catalog_publication_id)
    .bind(b"catalog/v1".as_slice())
    .bind(now)
    .execute(pool)
    .await
    .expect("catalog publication is seeded");
    sqlx::query(
        "INSERT INTO catalog_items (
    source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, publication_id, updated_at
) VALUES ($1, 'fixture.item', $2, 'fixture', 'item', 'Published', $3, $4, $5)
",
    )
    .bind(ids.source_id)
    .bind(item_key)
    .bind(json!({}))
    .bind(ids.catalog_publication_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("catalog item is seeded");
    sqlx::query(
        "INSERT INTO catalog_item_references (
    source_id, item_key_namespace, item_key_value, reference_namespace, reference_value
) VALUES ($1, 'fixture.item', $2, $3, $4)
",
    )
    .bind(ids.source_id)
    .bind(item_key)
    .bind(endpoint.namespace())
    .bind(endpoint.value())
    .execute(pool)
    .await
    .expect("portable reference is seeded");
    sqlx::query(
        "INSERT INTO source_coordinate_publications (
    source_id, publication_id, evidence_revision, published_at
) VALUES ($1, $2, $3, $4)
",
    )
    .bind(ids.source_id)
    .bind(ids.coordinate_publication_id)
    .bind(b"coordinates/v1".as_slice())
    .bind(now)
    .execute(pool)
    .await
    .expect("coordinate publication is seeded");
    sqlx::query(
        "INSERT INTO source_coordinate_bindings (
    source_id, publication_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value, selector, backing, observed_at
) VALUES ($1, $2, 'fixture.item', $3, $4, $5, $6, 'materialized', $7)
",
    )
    .bind(ids.source_id)
    .bind(ids.coordinate_publication_id)
    .bind(item_key)
    .bind(endpoint.namespace())
    .bind(endpoint.value())
    .bind(endpoint.selector().map_or("", PortableSelector::as_str))
    .bind(now)
    .execute(pool)
    .await
    .expect("coordinate binding is seeded");
}
