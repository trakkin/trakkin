#![cfg(feature = "embedded-database")]

use std::{io::Cursor, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{
        HeaderMap, Response, StatusCode,
        header::{ETAG, IF_NONE_MATCH},
    },
    routing::get,
};
use tempfile::tempdir;
use time::OffsetDateTime;
use tokio::io::AsyncReadExt;
use trakkin::{
    application::{
        error::ApplicationErrorCode,
        mappings::{
            CreateLocalMapping, CreateRemoteMappingSource, CreateUploadedMappingSource, MappingApi,
            MappingContentReader, MappingSourceKind, QueryMappings, RefreshRemoteMappingSource,
            RemoveLocalMapping, RemoveMappingSource, ReorderMappingSources, UpdateLocalMapping,
            UpdateMappingSource,
        },
    },
    database::DatabaseRuntime,
    infrastructure::mappings::{PostgresMappingManager, RemoteNetworkPolicy},
};
use uuid::Uuid;

const FIRST_SOURCE_ID: Uuid = Uuid::from_u128(10);
const SECOND_SOURCE_ID: Uuid = Uuid::from_u128(20);

#[derive(Clone)]
struct RemoteFixtureState {
    content: Vec<u8>,
    etag: String,
    status: StatusCode,
    disconnect: bool,
}

#[tokio::test]
async fn mapping_manager_owns_the_complete_source_and_query_lifecycle() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let manager = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(1))
        .await
        .expect("mapping manager starts");
    let initial = manager.state().await.unwrap();
    assert_eq!(initial.sources.len(), 1);
    assert_eq!(initial.sources[0].kind, MappingSourceKind::Local);
    assert!(initial.sources[0].enabled);
    assert_eq!(initial.sources[0].byte_length, 0);
    assert!(initial.active_chain_fingerprint.is_some());
    assert!(manager.query(query()).await.unwrap().items.is_empty());

    let first = manager
        .create_uploaded_source(
            CreateUploadedMappingSource {
                source_id: FIRST_SOURCE_ID,
                source_key: "community.first".to_owned(),
                display_name: "First community source".to_owned(),
                enabled: true,
                created_at: timestamp(2),
            },
            content("a://one <=> b://one\n"),
        )
        .await
        .expect("first source activates");
    assert_eq!(first.sources.len(), 2);
    let first_page = manager.query(query()).await.expect("mappings query");
    assert_eq!(first_page.items.len(), 1);
    assert_eq!(first_page.items[0].source_key, "community.first");
    let stale_query = manager
        .query(QueryMappings {
            expected_chain_fingerprint: initial.active_chain_fingerprint.clone(),
            ..query()
        })
        .await
        .expect_err("first-page queries reject a stale source-state generation");
    assert_eq!(stale_query.code, ApplicationErrorCode::MappingChainChanged);

    let invalid = manager
        .create_local_mapping(CreateLocalMapping {
            record: "this is not a mapping\n".to_owned(),
            created_at: timestamp(3),
        })
        .await
        .expect_err("invalid local mapping is rejected");
    assert_eq!(invalid.code, ApplicationErrorCode::InvalidMappingSource);
    assert_eq!(manager.state().await.unwrap(), first);
    assert_eq!(
        manager.query(query()).await.unwrap().items[0].canonical_statement,
        "a://one <=> b://one"
    );

    let created_local = manager
        .create_local_mapping(CreateLocalMapping {
            record: "#@note local\na://one <=> b://one\n".to_owned(),
            created_at: timestamp(4),
        })
        .await
        .expect("local mapping activates");
    let local_source = created_local
        .sources
        .iter()
        .find(|source| source.kind == MappingSourceKind::Local)
        .expect("local source exists");
    let local_page = manager.query(query()).await.unwrap();
    assert_eq!(local_page.items[0].source_key, "local");
    let local_statement_id = local_page.items[0].statement_id.clone();
    let mut stored_content = manager
        .read_source_content(local_source.id)
        .await
        .expect("local source content opens");

    manager
        .update_local_mapping(UpdateLocalMapping {
            statement_id: local_statement_id,
            record: "#@note updated\na://two <=> b://two\n".to_owned(),
            updated_at: timestamp(5),
        })
        .await
        .expect("local mapping updates");
    let mut superseded_content = String::new();
    stored_content
        .reader
        .read_to_string(&mut superseded_content)
        .await
        .expect("an open superseded source remains readable");
    assert_eq!(superseded_content, "#@note local\na://one <=> b://one\n");
    drop(stored_content);
    let updated_page = manager.query(query()).await.unwrap();
    let updated_local = updated_page
        .items
        .iter()
        .find(|item| item.source_key == "local")
        .expect("updated local mapping is queryable");
    assert_eq!(updated_local.canonical_statement, "a://two <=> b://two");
    manager
        .remove_local_mapping(RemoveLocalMapping {
            statement_id: updated_local.statement_id.clone(),
            removed_at: timestamp(6),
        })
        .await
        .expect("local mapping removes");
    assert_eq!(manager.query(query()).await.unwrap().items.len(), 1);

    manager
        .create_uploaded_source(
            CreateUploadedMappingSource {
                source_id: SECOND_SOURCE_ID,
                source_key: "community.second".to_owned(),
                display_name: "Second community source".to_owned(),
                enabled: true,
                created_at: timestamp(7),
            },
            content("b://one <=> a://one\n"),
        )
        .await
        .expect("second source activates");
    assert_eq!(
        manager.query(query()).await.unwrap().items[0].source_key,
        "community.first"
    );

    manager
        .reorder_sources(ReorderMappingSources {
            source_ids: vec![SECOND_SOURCE_ID, FIRST_SOURCE_ID],
            updated_at: timestamp(8),
        })
        .await
        .expect("sources reorder");
    assert_eq!(
        manager.query(query()).await.unwrap().items[0].source_key,
        "community.second"
    );

    manager
        .update_source(UpdateMappingSource {
            source_id: SECOND_SOURCE_ID,
            display_name: "Second community source".to_owned(),
            enabled: false,
            updated_at: timestamp(9),
        })
        .await
        .expect("source disables");
    assert_eq!(
        manager.query(query()).await.unwrap().items[0].source_key,
        "community.first"
    );

    let removed = manager
        .remove_source(RemoveMappingSource {
            source_id: FIRST_SOURCE_ID,
            removed_at: timestamp(10),
        })
        .await
        .expect("source removes");
    assert_eq!(removed.sources.len(), 2);
    assert!(manager.query(query()).await.unwrap().items.is_empty());

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM mapping_generations")
            .fetch_one(&database.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM mapping_source_versions")
            .fetch_one(&database.pool())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        file_count(&directory.path().join("mappings/generations")).await,
        1
    );
    assert_eq!(
        nested_file_count(&directory.path().join("mappings/sources")).await,
        2
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn disabled_source_changes_reuse_the_active_generation() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let manager = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(1))
        .await
        .expect("mapping manager starts");
    let active_fingerprint = manager
        .state()
        .await
        .unwrap()
        .active_chain_fingerprint
        .expect("initial generation is active");

    for (source_id, source_key, created_at) in [
        (FIRST_SOURCE_ID, "disabled.first", timestamp(2)),
        (SECOND_SOURCE_ID, "disabled.second", timestamp(3)),
    ] {
        let state = manager
            .create_uploaded_source(
                CreateUploadedMappingSource {
                    source_id,
                    source_key: source_key.to_owned(),
                    display_name: source_key.to_owned(),
                    enabled: false,
                    created_at,
                },
                content("a://disabled <=> b://disabled\n"),
            )
            .await
            .expect("disabled source is stored without rebuilding the runtime");
        assert_eq!(
            state.active_chain_fingerprint.as_deref(),
            Some(active_fingerprint.as_str())
        );
    }

    manager
        .reorder_sources(ReorderMappingSources {
            source_ids: vec![SECOND_SOURCE_ID, FIRST_SOURCE_ID],
            updated_at: timestamp(4),
        })
        .await
        .expect("disabled sources reorder without rebuilding the runtime");
    manager
        .update_source(UpdateMappingSource {
            source_id: FIRST_SOURCE_ID,
            display_name: "Renamed disabled source".to_owned(),
            enabled: false,
            updated_at: timestamp(5),
        })
        .await
        .expect("disabled source metadata updates without rebuilding the runtime");
    let state = manager
        .remove_source(RemoveMappingSource {
            source_id: FIRST_SOURCE_ID,
            removed_at: timestamp(6),
        })
        .await
        .expect("disabled source removes without rebuilding the runtime");

    assert_eq!(
        state.active_chain_fingerprint.as_deref(),
        Some(active_fingerprint.as_str())
    );
    assert!(manager.query(query()).await.unwrap().items.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM mapping_generations")
            .fetch_one(&database.pool())
            .await
            .unwrap(),
        1
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn startup_removes_unreferenced_published_artifacts() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let manager = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(1))
        .await
        .unwrap();

    let orphan_source = directory
        .path()
        .join(format!("mappings/sources/{FIRST_SOURCE_ID}"));
    tokio::fs::create_dir_all(&orphan_source).await.unwrap();
    let orphan_source = orphan_source.join(format!("{}.trakkin", "a".repeat(64)));
    tokio::fs::write(&orphan_source, b"unreferenced")
        .await
        .unwrap();
    let active_fingerprint = manager
        .state()
        .await
        .unwrap()
        .active_chain_fingerprint
        .unwrap();
    let active_index = directory
        .path()
        .join(format!("mappings/generations/{active_fingerprint}.sqlite"));
    let orphan_index = directory
        .path()
        .join(format!("mappings/generations/{}.sqlite", "b".repeat(64)));
    tokio::fs::copy(active_index, &orphan_index).await.unwrap();
    drop(manager);

    let _restarted = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(2))
        .await
        .unwrap();
    assert!(!tokio::fs::try_exists(orphan_source).await.unwrap());
    assert!(!tokio::fs::try_exists(orphan_index).await.unwrap());

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn mapping_artifact_store_has_one_live_runtime_owner() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let manager = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(1))
        .await
        .unwrap();

    let error = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(2))
        .await
        .err()
        .expect("a second live runtime is rejected");
    assert_eq!(error.code, ApplicationErrorCode::Internal);

    drop(manager);
    PostgresMappingManager::start(database.pool(), directory.path(), timestamp(3))
        .await
        .expect("runtime ownership transfers after shutdown");
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn database_chain_lock_serializes_activation_without_revisions() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let manager = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(1))
        .await
        .unwrap();
    let mut competing_write = database.pool().begin().await.unwrap();
    sqlx::query_scalar::<_, bool>(
        "SELECT singleton
FROM mapping_chain_state
WHERE singleton
FOR UPDATE
",
    )
    .fetch_one(&mut *competing_write)
    .await
    .unwrap();
    let activation_manager = manager.clone();
    let mut activation = tokio::spawn(async move {
        activation_manager
            .create_uploaded_source(
                CreateUploadedMappingSource {
                    source_id: FIRST_SOURCE_ID,
                    source_key: "community".to_owned(),
                    display_name: "Community".to_owned(),
                    enabled: true,
                    created_at: timestamp(2),
                },
                content("a://candidate <=> b://candidate\n"),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while nested_file_count(&directory.path().join("mappings/sources")).await < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("candidate source is published before activation");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut activation)
            .await
            .is_err(),
        "activation waits for the chain transaction lock"
    );
    competing_write.commit().await.unwrap();

    let active = activation
        .await
        .unwrap()
        .expect("activation resumes after the chain lock is released");
    assert_eq!(active.sources.len(), 2);
    assert_eq!(manager.query(query()).await.unwrap().items.len(), 1);
    assert_eq!(
        nested_file_count(&directory.path().join("mappings/sources")).await,
        2
    );
    assert_eq!(
        file_count(&directory.path().join("mappings/generations")).await,
        1
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn startup_rebuilds_a_corrupt_active_index_from_current_source_bytes() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let manager = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(1))
        .await
        .unwrap();
    let active = manager
        .create_uploaded_source(
            CreateUploadedMappingSource {
                source_id: FIRST_SOURCE_ID,
                source_key: "community".to_owned(),
                display_name: "Community".to_owned(),
                enabled: true,
                created_at: timestamp(2),
            },
            content("a://recovered => b://recovered\n"),
        )
        .await
        .expect("source activates");
    let fingerprint = active
        .active_chain_fingerprint
        .as_deref()
        .expect("active fingerprint exists");
    let index_path = directory
        .path()
        .join(format!("mappings/generations/{fingerprint}.sqlite"));
    drop(manager);
    tokio::fs::write(&index_path, b"SQLite format 3\0corrupt")
        .await
        .expect("active index is corrupted");

    let restarted = PostgresMappingManager::start(database.pool(), directory.path(), timestamp(3))
        .await
        .expect("active index rebuilds");
    let recovered = restarted.state().await.unwrap();
    assert_eq!(recovered, active);
    let page = restarted
        .query(query())
        .await
        .expect("rebuilt index queries");
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        page.items[0].canonical_statement,
        "a://recovered => b://recovered"
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn remote_source_refreshes_conditionally_and_preserves_the_active_generation_on_failure() {
    let fixture = Arc::new(tokio::sync::RwLock::new(RemoteFixtureState {
        content: b"a://remote-one <=> b://remote-one\n".to_vec(),
        etag: "\"v1\"".to_owned(),
        status: StatusCode::OK,
        disconnect: false,
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("remote fixture binds");
    let address = listener.local_addr().expect("fixture address is available");
    let server_fixture = Arc::clone(&fixture);
    let fixture_server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/mappings.trakkin", get(remote_mapping_source))
                .with_state(server_fixture),
        )
        .await
    });

    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let manager = PostgresMappingManager::start_with_remote_network_policy(
        database.pool(),
        directory.path(),
        RemoteNetworkPolicy::AllowPrivateHttp,
        timestamp(1),
    )
    .await
    .unwrap();
    let created = manager
        .create_remote_source(CreateRemoteMappingSource {
            source_id: FIRST_SOURCE_ID,
            source_key: "remote.community".to_owned(),
            display_name: "Remote community".to_owned(),
            remote_url: format!("http://{address}/mappings.trakkin"),
            enabled: true,
            created_at: timestamp(2),
        })
        .await
        .expect("remote source downloads and activates");
    assert_eq!(created.sources[1].kind, MappingSourceKind::Remote);
    assert_eq!(created.sources[1].last_checked_at, Some(timestamp(2)));
    assert_eq!(
        manager.query(query()).await.unwrap().items[0].canonical_statement,
        "a://remote-one <=> b://remote-one"
    );
    let duplicate = manager
        .create_remote_source(CreateRemoteMappingSource {
            source_id: SECOND_SOURCE_ID,
            source_key: "remote.duplicate".to_owned(),
            display_name: "Duplicate remote".to_owned(),
            remote_url: format!("http://{address}/mappings.trakkin"),
            enabled: true,
            created_at: timestamp(3),
        })
        .await
        .expect_err("canonical remote origins are unique");
    assert_eq!(duplicate.code, ApplicationErrorCode::InvalidMappingSource);

    let unchanged = manager
        .refresh_remote_source(RefreshRemoteMappingSource {
            source_id: FIRST_SOURCE_ID,
            refreshed_at: timestamp(3),
        })
        .await
        .expect("conditional refresh accepts not modified");
    assert_eq!(unchanged.sources[1].last_checked_at, Some(timestamp(3)));

    {
        let mut remote = fixture.write().await;
        remote.content = b"a://remote-two <=> b://remote-two\n".to_vec();
        remote.etag = "\"v2\"".to_owned();
    }
    manager
        .refresh_remote_source(RefreshRemoteMappingSource {
            source_id: FIRST_SOURCE_ID,
            refreshed_at: timestamp(4),
        })
        .await
        .expect("changed remote content activates");
    assert_eq!(
        manager.query(query()).await.unwrap().items[0].canonical_statement,
        "a://remote-two <=> b://remote-two"
    );

    {
        let mut remote = fixture.write().await;
        remote.etag = "\"v3\"".to_owned();
        remote.disconnect = true;
    }
    let error = manager
        .refresh_remote_source(RefreshRemoteMappingSource {
            source_id: FIRST_SOURCE_ID,
            refreshed_at: timestamp(5),
        })
        .await
        .expect_err("interrupted remote body is reported");
    assert_eq!(error.code, ApplicationErrorCode::MappingSourceUnavailable);
    let preserved = manager.state().await.unwrap();
    assert!(preserved.sources[1].last_refresh_error.is_some());
    assert_eq!(
        manager.query(query()).await.unwrap().items[0].canonical_statement,
        "a://remote-two <=> b://remote-two"
    );

    {
        let mut remote = fixture.write().await;
        remote.content = b"a://remote-three <=> b://remote-three\n".to_vec();
        remote.etag = "\"v4\"".to_owned();
        remote.disconnect = false;
    }
    let disabled = manager
        .update_source(UpdateMappingSource {
            source_id: FIRST_SOURCE_ID,
            display_name: "Remote community".to_owned(),
            enabled: false,
            updated_at: timestamp(6),
        })
        .await
        .expect("remote source disables");
    let disabled_fingerprint = disabled
        .active_chain_fingerprint
        .clone()
        .expect("disabled chain remains active");
    let disabled_content_hash = disabled.sources[1].content_hash.clone();
    let refreshed = manager
        .refresh_remote_source(RefreshRemoteMappingSource {
            source_id: FIRST_SOURCE_ID,
            refreshed_at: timestamp(7),
        })
        .await
        .expect("disabled remote source refreshes without rebuilding the runtime");
    assert_eq!(
        refreshed.active_chain_fingerprint.as_deref(),
        Some(disabled_fingerprint.as_str())
    );
    assert_ne!(refreshed.sources[1].content_hash, disabled_content_hash);
    assert!(manager.query(query()).await.unwrap().items.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM mapping_generations")
            .fetch_one(&database.pool())
            .await
            .unwrap(),
        1
    );

    fixture_server.abort();
    let _ = fixture_server.await;
    database.shutdown().await.expect("database shuts down");
}

async fn remote_mapping_source(
    State(fixture): State<Arc<tokio::sync::RwLock<RemoteFixtureState>>>,
    headers: HeaderMap,
) -> Response<Body> {
    let fixture = fixture.read().await;
    if fixture.status != StatusCode::OK {
        return Response::builder()
            .status(fixture.status)
            .body(Body::empty())
            .expect("fixture response is valid");
    }
    if fixture.disconnect {
        let stream = futures_util::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(b"a://partial")),
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "fixture disconnected",
            )),
        ]);
        return Response::builder()
            .status(StatusCode::OK)
            .header(ETAG, &fixture.etag)
            .body(Body::from_stream(stream))
            .expect("fixture response is valid");
    }
    if headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        == Some(fixture.etag.as_str())
    {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(ETAG, &fixture.etag)
            .body(Body::empty())
            .expect("fixture response is valid");
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(ETAG, &fixture.etag)
        .body(Body::from(fixture.content.clone()))
        .expect("fixture response is valid")
}

fn content(value: &str) -> MappingContentReader {
    Box::pin(Cursor::new(value.as_bytes().to_vec()))
}

fn query() -> QueryMappings {
    QueryMappings {
        expected_chain_fingerprint: None,
        search: None,
        canonical_statement: None,
        statement_id: None,
        relation_key: None,
        source_key: None,
        endpoint: None,
        annotation_name: None,
        annotation_value: None,
        include_shadowed: false,
        limit: 50,
        cursor: None,
    }
}

fn timestamp(offset: i64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(offset)
}

async fn file_count(directory: &std::path::Path) -> usize {
    let mut entries = tokio::fs::read_dir(directory).await.unwrap();
    let mut count = 0;
    while entries.next_entry().await.unwrap().is_some() {
        count += 1;
    }
    count
}

async fn nested_file_count(directory: &std::path::Path) -> usize {
    let mut directories = tokio::fs::read_dir(directory).await.unwrap();
    let mut count = 0;
    while let Some(entry) = directories.next_entry().await.unwrap() {
        if entry.file_type().await.unwrap().is_dir() {
            count += file_count(&entry.path()).await;
        }
    }
    count
}
