#![cfg(feature = "embedded-database")]

use tempfile::tempdir;
use trakkin::database::DatabaseRuntime;

#[path = "support/catalog_publication.rs"]
#[expect(
    dead_code,
    reason = "Shared fixture IDs are used by other integration tests"
)]
mod catalog_publication_support;

use catalog_publication_support::{
    READ_ATTEMPT_ID, TASK_ATTEMPT_ID, TASK_ID, TASK_OWNER_ID, seed_catalog_read, timestamp,
};
use serde_json::json;
use trakkin::{
    application::acquisition::{BeginRead, PublicationRepository},
    infrastructure::catalog_publication::PostgresPublicationRepository,
};
use uuid::Uuid;

#[tokio::test]
async fn fresh_database_starts() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

#[tokio::test]
async fn staged_catalog_checks_reject_missing_required_upsert_fields() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_catalog_read(database.pool()).await;
    PostgresPublicationRepository::new(database.pool())
        .begin_catalog_read(BeginRead {
            attempt_id: READ_ATTEMPT_ID,
            task_id: TASK_ID,
            task_attempt_id: TASK_ATTEMPT_ID,
            owner_id: TASK_OWNER_ID,
            started_at: timestamp(0),
        })
        .await
        .expect("catalog read begins");

    let item = json!({
        "attempt_id": READ_ATTEMPT_ID,
        "key_namespace": "fixture.item", "key_value": "\\x01",
        "operation": "upsert", "kind_namespace": "fixture", "kind_name": "episode",
        "display_name": "Item", "attributes": {}, "batch_sequence": 0, "row_ordinal": 0,
    });
    let item_insert = "INSERT INTO staged_catalog_items
        SELECT * FROM jsonb_populate_record(NULL::staged_catalog_items, $1)";
    for field in ["kind_namespace", "kind_name", "display_name", "attributes"] {
        let mut invalid = item.clone();
        invalid[field] = serde_json::Value::Null;
        assert_check_violation(
            sqlx::query(item_insert)
                .bind(invalid)
                .execute(&database.pool())
                .await,
        );
        if field != "attributes" {
            let mut invalid = item.clone();
            invalid[field] = json!("");
            assert_check_violation(
                sqlx::query(item_insert)
                    .bind(invalid)
                    .execute(&database.pool())
                    .await,
            );
        }
    }
    sqlx::query(item_insert)
        .bind(item)
        .execute(&database.pool())
        .await
        .expect("complete item upsert is accepted");
    sqlx::query(item_insert)
        .bind(json!({
            "attempt_id": READ_ATTEMPT_ID,
            "key_namespace": "fixture.item", "key_value": "\\x02",
            "operation": "delete", "batch_sequence": 0, "row_ordinal": 1,
        }))
        .execute(&database.pool())
        .await
        .expect("item delete with no payload is accepted");

    let relation = json!({
        "attempt_id": READ_ATTEMPT_ID,
        "key_namespace": "fixture.relation", "key_value": "\\x01",
        "operation": "upsert", "kind_namespace": "fixture", "kind_name": "group",
        "order_parts": [], "attributes": {}, "batch_sequence": 0, "row_ordinal": 0,
    });
    let relation_insert = "INSERT INTO staged_catalog_relations
        SELECT * FROM jsonb_populate_record(NULL::staged_catalog_relations, $1)";
    for field in ["kind_namespace", "kind_name", "order_parts", "attributes"] {
        let mut invalid = relation.clone();
        invalid[field] = serde_json::Value::Null;
        assert_check_violation(
            sqlx::query(relation_insert)
                .bind(invalid)
                .execute(&database.pool())
                .await,
        );
        if matches!(field, "kind_namespace" | "kind_name") {
            let mut invalid = relation.clone();
            invalid[field] = json!("");
            assert_check_violation(
                sqlx::query(relation_insert)
                    .bind(invalid)
                    .execute(&database.pool())
                    .await,
            );
        }
    }
    sqlx::query(relation_insert)
        .bind(relation)
        .execute(&database.pool())
        .await
        .expect("virtual root with no parent or item is accepted");
    sqlx::query(relation_insert)
        .bind(json!({
            "attempt_id": READ_ATTEMPT_ID,
            "key_namespace": "fixture.relation", "key_value": "\\x02",
            "operation": "delete", "batch_sequence": 0, "row_ordinal": 1,
        }))
        .execute(&database.pool())
        .await
        .expect("relation delete with no payload is accepted");

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn mapping_source_checks_require_remote_url_only_for_remote_sources() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    for (kind, priority, url, valid) in [
        ("remote", 1_i64, None, false),
        ("remote", 1, Some(""), false),
        ("remote", 1, Some("   "), false),
        ("remote", 1, Some("https://example.test/mappings"), true),
        ("local", 0, None, true),
        ("upload", 1, None, true),
        ("local", 0, Some("https://example.test/mappings"), false),
        ("upload", 1, Some("https://example.test/mappings"), false),
    ] {
        let mut transaction = database.pool().begin().await.expect("transaction begins");
        let result = sqlx::query(
            "INSERT INTO mapping_sources (
                 id, source_key, source_kind, display_name, remote_url, priority,
                 enabled, current_version_id, created_at, updated_at
             ) VALUES ($1, 'fixture', $2, 'Fixture', $3, $4, TRUE, $5, $6, $6)",
        )
        .bind(Uuid::from_u128(100))
        .bind(kind)
        .bind(url)
        .bind(priority)
        .bind(Uuid::from_u128(101))
        .bind(timestamp(0))
        .execute(&mut *transaction)
        .await;
        if valid {
            result.expect("valid source shape is accepted before deferred version FK");
        } else {
            assert_check_violation(result);
        }
        transaction
            .rollback()
            .await
            .expect("fixture transaction rolls back");
    }
    database.shutdown().await.expect("database shuts down");
}

fn assert_check_violation(result: Result<sqlx::postgres::PgQueryResult, sqlx::Error>) {
    let error = result.expect_err("invalid row must fail a CHECK constraint");
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23514"),
        "unexpected database error: {error}"
    );
}
