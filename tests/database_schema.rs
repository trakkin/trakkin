#![cfg(feature = "embedded-database")]

use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::process::Command;
use trakkin::{
    application::{
        acquisition::{BeginRead, PublicationRepository},
        mappings::{
            ChangeMappingSource, DecideMapping, DeleteMapping, MappingApi, ResolveMappingUnits,
            SaveMapping, SaveMappingSource,
        },
    },
    database::DatabaseRuntime,
    infrastructure::{
        catalog_publication::PostgresPublicationRepository,
        mappings::{MappingStore, MappingUpdates},
    },
};
use trakkin_mappings_corpus::git::RepositoryStore;
use trakkin_mappings_language::parse_record;

#[path = "support/catalog_publication.rs"]
#[expect(
    dead_code,
    reason = "Shared fixture IDs are used by other integration tests"
)]
mod catalog_publication_support;
use catalog_publication_support::{
    READ_ATTEMPT_ID, TASK_ATTEMPT_ID, TASK_ID, TASK_OWNER_ID, seed_catalog_read, timestamp,
};

const ORIGINAL: &str = "com.imdb://title/tt0133093 <=> org.themoviedb://movie/603";
const CORRECTION: &str = "com.imdb://title/tt0133093 <=> org.themoviedb://movie/604";

#[path = "support/mapping_git.rs"]
mod mapping_git;
use mapping_git::GitServer;

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
async fn mapping_readers_catch_up_other_instances_without_taking_the_writer_lock() {
    let directory = tempdir().unwrap();
    let database = DatabaseRuntime::start(directory.path()).await.unwrap();
    let root = directory.path().join("mapping-repositories");
    let writer = MappingStore::open(database.pool(), &root).await.unwrap();
    let reader = MappingStore::open(database.pool(), &root).await.unwrap();
    let saved = writer
        .save(SaveMapping {
            expected_fingerprint: writer.state().await.unwrap().fingerprint,
            record: ORIGINAL.into(),
            replaces: None,
        })
        .await
        .unwrap();
    let mut transaction = database.pool().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(0x7472616b6d617073_i64)
        .execute(&mut *transaction)
        .await
        .unwrap();
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), reader.state())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed, saved);
    let resolved = reader
        .resolve_units(ResolveMappingUnits {
            expected_fingerprint: Some(saved.fingerprint.clone()),
            unit_keys: vec!["com.imdb://title/tt0133093".into()],
        })
        .await
        .unwrap();
    assert_eq!(resolved.units[0].matches.len(), 1);
    assert_eq!(
        reader.sources().await.unwrap().fingerprint,
        saved.fingerprint
    );
    transaction.rollback().await.unwrap();
    let rejected = writer
        .decide(DecideMapping {
            expected_fingerprint: saved.fingerprint,
            claim_id: parse_record(ORIGINAL)
                .unwrap()
                .statement
                .claim_id()
                .to_string(),
            rejected: true,
        })
        .await
        .unwrap();
    assert_eq!(reader.state().await.unwrap(), rejected);
    let mut latest = rejected;
    for step in 0..8 {
        latest = writer
            .decide(DecideMapping {
                expected_fingerprint: latest.fingerprint,
                claim_id: parse_record(ORIGINAL)
                    .unwrap()
                    .statement
                    .claim_id()
                    .to_string(),
                rejected: step % 2 != 0,
            })
            .await
            .unwrap();
    }
    let invalidations: i64 = sqlx::query_scalar("SELECT count(*) FROM mapping_invalidations")
        .fetch_one(&database.pool())
        .await
        .unwrap();
    assert_eq!(invalidations, 1);
    assert_eq!(reader.state().await.unwrap(), latest);
    let deleted = writer
        .delete(DeleteMapping {
            expected_fingerprint: latest.fingerprint,
            claim_id: parse_record(ORIGINAL)
                .unwrap()
                .statement
                .claim_id()
                .to_string(),
        })
        .await
        .unwrap();
    assert_eq!(deleted.records, 0);
    assert_eq!(reader.state().await.unwrap(), deleted);
    database.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_restoration_never_advances_the_local_git_revision() {
    let directory = tempdir().unwrap();
    let remote_directory = tempdir().unwrap();
    let server = GitServer::open(remote_directory.path()).await;
    let invalid = "a://1 @2 <=> b://1";
    mapping_git::commit_records(&server.source, "", invalid).await;
    let database = DatabaseRuntime::start(directory.path()).await.unwrap();
    let root = directory.path().join("mapping-repositories");
    let store = Arc::new(MappingStore::open(database.pool(), &root).await.unwrap());
    store
        .save_source(SaveMappingSource {
            expected_fingerprint: store.state().await.unwrap().fingerprint,
            id: None,
            display_name: "Suppressions".into(),
            url: server.remote.clone(),
            branch: "main".into(),
            poll_interval_seconds: 60,
            enabled: true,
        })
        .await
        .unwrap();
    MappingUpdates::new(store.clone())
        .poll_due(time::OffsetDateTime::now_utc())
        .await
        .unwrap();
    let prior = store.state().await.unwrap();
    assert_eq!(prior.rejected, 1);
    let local = RepositoryStore::open(&root).unwrap().local().await.unwrap();
    let head = local.head().await.unwrap();
    assert!(
        store
            .decide(DecideMapping {
                expected_fingerprint: prior.fingerprint.clone(),
                claim_id: parse_record(invalid)
                    .unwrap()
                    .statement
                    .claim_id()
                    .to_string(),
                rejected: false,
            })
            .await
            .is_err()
    );
    assert_eq!(local.head().await.unwrap(), head);
    assert_eq!(store.state().await.unwrap(), prior);
    assert_eq!(
        MappingStore::open(database.pool(), &root)
            .await
            .unwrap()
            .state()
            .await
            .unwrap(),
        prior
    );
    database.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_local_history_never_erases_saved_decisions() {
    let directory = tempdir().unwrap();
    let database = DatabaseRuntime::start(directory.path()).await.unwrap();
    let store = MappingStore::open(
        database.pool(),
        &directory.path().join("mapping-repositories"),
    )
    .await
    .unwrap();
    let saved = store
        .save(SaveMapping {
            expected_fingerprint: store.state().await.unwrap().fingerprint,
            record: ORIGINAL.into(),
            replaces: None,
        })
        .await
        .unwrap();
    assert!(
        MappingStore::open(
            database.pool(),
            &directory.path().join("missing-local-history")
        )
        .await
        .is_err()
    );
    assert_eq!(store.state().await.unwrap(), saved);
    let local_records: i64 =
        sqlx::query_scalar("SELECT count(*) FROM mapping_origins WHERE source_id='local'")
            .fetch_one(&database.pool())
            .await
            .unwrap();
    assert_eq!(local_records, 1);
    database.shutdown().await.unwrap();
}

#[tokio::test]
async fn upstream_cache_loss_reimports_current_tree_without_stale_origins() {
    let directory = tempdir().unwrap();
    let remote_directory = tempdir().unwrap();
    let server = GitServer::open(remote_directory.path()).await;
    server.commit(&format!("{ORIGINAL}\n{CORRECTION}")).await;
    let database = DatabaseRuntime::start(directory.path()).await.unwrap();
    let root = directory.path().join("mapping-repositories");
    let store = Arc::new(MappingStore::open(database.pool(), &root).await.unwrap());
    store
        .save_source(SaveMappingSource {
            expected_fingerprint: store.state().await.unwrap().fingerprint,
            id: None,
            display_name: "Community".into(),
            url: server.remote.clone(),
            branch: "main".into(),
            poll_interval_seconds: 60,
            enabled: true,
        })
        .await
        .unwrap();
    let updates = MappingUpdates::new(store.clone());
    let now = time::OffsetDateTime::now_utc();
    updates.poll_due(now).await.unwrap();
    let imported = store.state().await.unwrap();
    assert_eq!(imported.records, 2);
    store
        .decide(DecideMapping {
            expected_fingerprint: imported.fingerprint,
            claim_id: parse_record(ORIGINAL)
                .unwrap()
                .statement
                .claim_id()
                .to_string(),
            rejected: true,
        })
        .await
        .unwrap();
    let source_id = store.sources().await.unwrap().sources[1].id.clone();
    RepositoryStore::open(&root)
        .unwrap()
        .remove_upstream(&source_id)
        .unwrap();
    let current = server.commit("a://1 <=> b://1").await;
    updates
        .poll_due(now + time::Duration::seconds(61))
        .await
        .unwrap();
    let sources = store.sources().await.unwrap();
    assert_eq!(
        sources.sources[1].revision.as_deref(),
        Some(current.as_str())
    );
    assert_eq!(sources.sources[1].cached_records, 1);
    assert_eq!(sources.sources[1].consecutive_failures, 0);
    let state = store.state().await.unwrap();
    assert_eq!((state.records, state.resolved, state.rejected), (2, 1, 1));
    database.shutdown().await.unwrap();
}

#[tokio::test]
async fn git_sources_fetch_exact_commits_and_preserve_last_good_and_local_decisions() {
    let directory = tempdir().unwrap();
    let remote_directory = tempdir().unwrap();
    let server = GitServer::open(remote_directory.path()).await;
    let first = server.commit(ORIGINAL).await;
    let database = DatabaseRuntime::start(directory.path()).await.unwrap();
    let root = directory.path().join("mapping-repositories");
    let store = Arc::new(MappingStore::open(database.pool(), &root).await.unwrap());
    let initial = store.sources().await.unwrap();
    assert_eq!(initial.sources.len(), 1);
    assert_eq!(initial.sources[0].id, "local");
    assert_eq!(
        initial.sources[0].repository_path.as_deref(),
        root.join("local").to_str()
    );
    let command = SaveMappingSource {
        expected_fingerprint: initial.fingerprint,
        id: None,
        display_name: "Community".into(),
        url: server.remote.clone(),
        branch: "main".into(),
        poll_interval_seconds: 60,
        enabled: true,
    };
    store.save_source(command.clone()).await.unwrap();
    let source_id = store.sources().await.unwrap().sources[1].id.clone();
    let updates = Arc::new(MappingUpdates::new(store.clone()));
    let now = time::OffsetDateTime::now_utc();
    updates.poll_due(now).await.unwrap();
    assert_eq!(
        store.sources().await.unwrap().sources[1]
            .revision
            .as_deref(),
        Some(first.as_str())
    );
    let imported = store.state().await.unwrap();
    assert_eq!(imported.records, 1);
    let original_id = parse_record(ORIGINAL)
        .unwrap()
        .statement
        .claim_id()
        .to_string();
    let suppressed = store
        .decide(DecideMapping {
            expected_fingerprint: imported.fingerprint,
            claim_id: original_id.clone(),
            rejected: true,
        })
        .await
        .unwrap();
    let local_claim = "com.imdb://title/tt2 <=> org.themoviedb://movie/2";
    let saved = store
        .save(SaveMapping {
            expected_fingerprint: suppressed.fingerprint,
            record: local_claim.into(),
            replaces: None,
        })
        .await
        .unwrap();
    updates
        .poll_due(now + time::Duration::seconds(61))
        .await
        .unwrap();
    assert_eq!(store.state().await.unwrap(), saved);
    let second = server.commit(&format!("{ORIGINAL}\n{CORRECTION}")).await;
    updates
        .poll_due(now + time::Duration::seconds(122))
        .await
        .unwrap();
    let refreshed = store.state().await.unwrap();
    assert_eq!(
        (refreshed.records, refreshed.rejected, refreshed.resolved),
        (3, 1, 2)
    );
    let repositories = RepositoryStore::open(&root).unwrap();
    let local = repositories.local().await.unwrap();
    let artifacts = mapping_git::artifacts(&local).await;
    assert_eq!(artifacts.len(), 2);
    assert!(
        artifacts.iter().any(|artifact| artifact.rejected
            && artifact.text().contains("#@note Explicitly suppressed"))
    );
    let reopened = Arc::new(MappingStore::open(database.pool(), &root).await.unwrap());
    assert_eq!(reopened.state().await.unwrap(), refreshed);
    std::fs::write(
        server.source.path().join("mappings/v1/invalid.trk"),
        "not a canonical corpus",
    )
    .unwrap();
    for arguments in [
        vec!["add", "mappings"],
        vec!["commit", "-m", "Invalid corpus"],
    ] {
        let output = Command::new("git")
            .current_dir(server.source.path())
            .args([
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@localhost",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(arguments)
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
    }
    updates
        .poll_due(now + time::Duration::seconds(183))
        .await
        .unwrap();
    let failed = store.sources().await.unwrap().sources.remove(1);
    assert_eq!(failed.revision.as_deref(), Some(second.as_str()));
    assert_eq!(failed.consecutive_failures, 1);
    assert!(failed.last_error.is_some());
    assert_eq!(store.state().await.unwrap(), refreshed);
    assert_eq!(
        repositories
            .upstream(&source_id)
            .unwrap()
            .changes(None, &first, None, 1000)
            .await
            .unwrap()
            .changes
            .len(),
        1
    );
    Command::new("git")
        .current_dir(server.source.path())
        .args(["reset", "--soft", &second])
        .output()
        .await
        .unwrap();
    Command::new("git")
        .current_dir(server.source.path())
        .args(["restore", "--staged", ":/"])
        .output()
        .await
        .unwrap();
    std::fs::remove_file(server.source.path().join("mappings/v1/invalid.trk")).unwrap();
    let disabled = store
        .save_source(SaveMappingSource {
            expected_fingerprint: refreshed.fingerprint,
            id: Some(source_id.clone()),
            enabled: false,
            ..command.clone()
        })
        .await
        .unwrap();
    assert_eq!((disabled.records, disabled.rejected), (2, 1));
    updates
        .poll_due(now + time::Duration::days(8))
        .await
        .unwrap();
    assert!(repositories.upstream(&source_id).unwrap().path().exists());
    let enabled = store
        .save_source(SaveMappingSource {
            expected_fingerprint: disabled.fingerprint,
            id: Some(source_id.clone()),
            ..command.clone()
        })
        .await
        .unwrap();
    server.service.pause.send_replace(true);
    let pending = {
        let updates = updates.clone();
        tokio::spawn(async move { updates.poll_due(time::OffsetDateTime::now_utc()).await })
    };
    server.wait_for_fetch().await;
    let removed = store
        .delete_source(ChangeMappingSource {
            id: source_id.clone(),
            expected_fingerprint: enabled.fingerprint,
        })
        .await
        .unwrap();
    server.service.pause.send_replace(false);
    pending.await.unwrap().unwrap();
    assert_eq!(store.state().await.unwrap(), removed);
    assert_eq!(store.sources().await.unwrap().sources.len(), 1);
    assert!(!repositories.upstream(&source_id).unwrap().path().exists());
    assert!(repositories.local_path().exists());
    assert!(
        store
            .delete_source(ChangeMappingSource {
                id: "local".into(),
                expected_fingerprint: removed.fingerprint.clone()
            })
            .await
            .is_err()
    );
    let reverted = store
        .revert(DeleteMapping {
            claim_id: original_id,
            expected_fingerprint: removed.fingerprint,
        })
        .await
        .unwrap();
    assert_eq!(reverted.records, 1);
    database.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_git_recovery_rebuilds_status_and_rejects_external_dirty_writes() {
    let directory = tempdir().unwrap();
    let database = DatabaseRuntime::start(directory.path()).await.unwrap();
    let root = directory.path().join("mapping-repositories");
    let store = MappingStore::open(database.pool(), &root).await.unwrap();
    let original_id = parse_record(ORIGINAL)
        .unwrap()
        .statement
        .claim_id()
        .to_string();
    let saved = store
        .save(SaveMapping {
            expected_fingerprint: store.state().await.unwrap().fingerprint,
            record: ORIGINAL.into(),
            replaces: None,
        })
        .await
        .unwrap();
    let corrected = store
        .save(SaveMapping {
            expected_fingerprint: saved.fingerprint.clone(),
            record: CORRECTION.into(),
            replaces: Some(original_id.clone()),
        })
        .await
        .unwrap();
    assert_eq!(
        (corrected.records, corrected.active, corrected.rejected),
        (2, 1, 1)
    );
    assert!(
        store
            .save(SaveMapping {
                expected_fingerprint: saved.fingerprint,
                record: ORIGINAL.into(),
                replaces: None
            })
            .await
            .is_err()
    );
    let repository = RepositoryStore::open(&root).unwrap().local().await.unwrap();
    let committed = repository.head().await.unwrap();
    assert_eq!(mapping_git::artifacts(&repository).await.len(), 2);
    assert_eq!(
        MappingStore::open(database.pool(), &root)
            .await
            .unwrap()
            .state()
            .await
            .unwrap(),
        corrected
    );
    let matches = store
        .resolve_units(ResolveMappingUnits {
            expected_fingerprint: Some(corrected.fingerprint.clone()),
            unit_keys: vec!["com.imdb://title/tt0133093".into()],
        })
        .await
        .unwrap();
    assert_eq!(matches.units[0].matches.len(), 1);
    assert!(
        matches.units[0].matches[0].right.members[0]
            .unit_key
            .ends_with("/604")
    );
    std::fs::write(repository.path().join("external.trk"), "do not overwrite").unwrap();
    assert!(
        store
            .save(SaveMapping {
                expected_fingerprint: corrected.fingerprint.clone(),
                record: "a://9 <=> b://9".into(),
                replaces: None
            })
            .await
            .is_err()
    );
    assert_eq!(repository.head().await.unwrap(), committed);
    assert_eq!(
        std::fs::read_to_string(repository.path().join("external.trk")).unwrap(),
        "do not overwrite"
    );
    std::fs::remove_file(repository.path().join("external.trk")).unwrap();
    let artifacts = mapping_git::artifacts(&repository).await;
    let accepted = artifacts
        .iter()
        .filter(|artifact| !artifact.rejected)
        .map(|artifact| artifact.record.canonical())
        .collect::<String>();
    let rejected = artifacts
        .iter()
        .filter(|artifact| artifact.rejected)
        .map(|artifact| artifact.record.canonical())
        .collect::<String>();
    let interrupted = mapping_git::commit_records(
        &repository,
        &format!("{accepted}a://9 <=> b://9\n"),
        &rejected,
    )
    .await;
    assert_ne!(interrupted, committed);
    assert_eq!(mapping_git::artifacts(&repository).await.len(), 3);
    let applied: String =
        sqlx::query_scalar("SELECT revision FROM mapping_sources WHERE id = 'local'")
            .fetch_one(&database.pool())
            .await
            .unwrap();
    assert_eq!(applied, committed);
    let recovered = store
        .refresh_source(ChangeMappingSource {
            id: "local".into(),
            expected_fingerprint: corrected.fingerprint,
        })
        .await
        .unwrap();
    assert_eq!(store.state().await.unwrap(), recovered);
    assert_eq!(recovered.records, 3);
    assert_eq!(
        store.sources().await.unwrap().sources[0]
            .revision
            .as_deref(),
        Some(interrupted.as_str())
    );
    assert_eq!(
        MappingStore::open(database.pool(), &root)
            .await
            .unwrap()
            .state()
            .await
            .unwrap(),
        recovered
    );
    let conflicting = store
        .decide(DecideMapping {
            expected_fingerprint: recovered.fingerprint,
            claim_id: original_id,
            rejected: false,
        })
        .await
        .unwrap();
    assert_eq!(conflicting.conflicting, 0);
    assert_eq!(conflicting.resolved, 3);
    database.shutdown().await.unwrap();
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
    let item = json!({"attempt_id": READ_ATTEMPT_ID,"key_namespace": "fixture.item", "key_value": "\\x01","operation": "upsert", "kind_namespace": "fixture", "kind_name": "episode","display_name": "Item", "attributes": {}, "batch_sequence": 0, "row_ordinal": 0});
    let item_insert = "INSERT INTO staged_catalog_items SELECT * FROM jsonb_populate_record(NULL::staged_catalog_items, $1)";
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
    sqlx::query(item_insert).bind(json!({"attempt_id": READ_ATTEMPT_ID,"key_namespace": "fixture.item", "key_value": "\\x02","operation": "delete", "batch_sequence": 0, "row_ordinal": 1})).execute(&database.pool()).await.expect("item delete with no payload is accepted");
    let relation = json!({"attempt_id": READ_ATTEMPT_ID,"key_namespace": "fixture.relation", "key_value": "\\x01","operation": "upsert", "kind_namespace": "fixture", "kind_name": "group","order_parts": [], "attributes": {}, "batch_sequence": 0, "row_ordinal": 0});
    let relation_insert = "INSERT INTO staged_catalog_relations SELECT * FROM jsonb_populate_record(NULL::staged_catalog_relations, $1)";
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
    sqlx::query(relation_insert).bind(json!({"attempt_id": READ_ATTEMPT_ID,"key_namespace": "fixture.relation", "key_value": "\\x02","operation": "delete", "batch_sequence": 0, "row_ordinal": 1})).execute(&database.pool()).await.expect("relation delete with no payload is accepted");
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
