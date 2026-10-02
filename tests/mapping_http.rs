#![cfg(feature = "embedded-database")]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode, header::CONTENT_TYPE},
};
use http_body_util::BodyExt;
use tempfile::tempdir;
use tower::ServiceExt;
use trakkin::{
    application::facade::{ApplicationFacade, ApplicationInteractions},
    database::DatabaseRuntime,
    http,
    infrastructure::{
        adapter_process::AdapterProcess,
        catalog_queries::PostgresCatalogQueryRepository,
        connection::{ConnectionMasterKey, PostgresConnectionRepository, ProcessSessionRegistry},
        mappings::PostgresMappingManager,
        task_manager::PostgresTaskRepository,
        workspaces::PostgresWorkspaceRepository,
    },
};

#[path = "support/http/router.rs"]
pub mod support;

use support::{
    StaticBinaryAssetInteraction, StaticConnectionInteraction, request, static_provider_registry,
    timestamp,
};

#[tokio::test]
async fn mapping_routes_manage_local_rows_and_stream_read_only_source_content() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let mappings = Arc::new(
        PostgresMappingManager::start(database.pool(), directory.path(), timestamp(0))
            .await
            .expect("mapping manager starts"),
    );
    let application = ApplicationFacade::new(
        PostgresWorkspaceRepository::new(database.pool()),
        PostgresCatalogQueryRepository::new(database.pool()),
        PostgresTaskRepository::new(database.pool()),
        PostgresConnectionRepository::new(database.pool(), ConnectionMasterKey::new([9_u8; 32])),
        ProcessSessionRegistry::<AdapterProcess>::default(),
        ApplicationInteractions::new(
            Arc::new(StaticConnectionInteraction),
            Arc::new(StaticBinaryAssetInteraction),
            static_provider_registry(),
            mappings,
        ),
    );
    let router = http::router(Arc::new(application), directory.path().to_owned());

    let initial = request(
        &router,
        Request::get("/api/v1/mapping-sources")
            .body(Body::empty())
            .expect("state request is built"),
    )
    .await;
    assert_eq!(initial.0, StatusCode::OK);
    assert!(initial.1.get("revision").is_none());

    let created = request(
        &router,
        Request::post("/api/v1/mapping-sources/uploads?sourceKey=community&displayName=Community&enabled=true")
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(Body::from("a://http <=> b://http\n"))
            .expect("create request is built"),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED);
    let source_id = created.1["sources"]
        .as_array()
        .expect("sources are returned")
        .iter()
        .find(|source| source["sourceKey"] == "community")
        .and_then(|source| source["id"].as_str())
        .expect("created source is returned")
        .to_owned();

    let page = request(
        &router,
        Request::get("/api/v1/mappings?limit=10")
            .body(Body::empty())
            .expect("query request is built"),
    )
    .await;
    assert_eq!(page.0, StatusCode::OK);
    assert_eq!(
        page.1["items"][0]["canonicalStatement"],
        "a://http <=> b://http"
    );
    assert_eq!(page.1["items"][0]["sourceKey"], "community");

    let response = router
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/mapping-sources/{source_id}/content"))
                .body(Body::empty())
                .expect("content request is built"),
        )
        .await
        .expect("content request completes");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; charset=utf-8")
    );
    let body = response
        .into_body()
        .collect()
        .await
        .expect("content body is collected")
        .to_bytes();
    assert_eq!(body.as_ref(), b"a://http <=> b://http\n");

    let local = request(
        &router,
        Request::post("/api/v1/mappings/local")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r##"{"record":"#@note local\na://http <=> b://http\n"}"##,
            ))
            .expect("create local mapping request is built"),
    )
    .await;
    assert_eq!(local.0, StatusCode::CREATED);
    let local_page = request(
        &router,
        Request::get("/api/v1/mappings?limit=10")
            .body(Body::empty())
            .expect("local query request is built"),
    )
    .await;
    assert_eq!(local_page.1["items"][0]["sourceKey"], "local");
    let statement_id = local_page.1["items"][0]["statementId"]
        .as_str()
        .expect("local statement ID is returned")
        .to_owned();

    let invalid = request(
        &router,
        Request::put(format!("/api/v1/mappings/local/{statement_id}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"record":"not a mapping"}"#))
            .expect("invalid local update request is built"),
    )
    .await;
    assert_eq!(invalid.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(invalid.1["code"], "invalid_mapping_source");
    let preserved = request(
        &router,
        Request::get("/api/v1/mappings?limit=10")
            .body(Body::empty())
            .expect("preserved query request is built"),
    )
    .await;
    assert_eq!(
        preserved.1["items"][0]["canonicalStatement"],
        "a://http <=> b://http"
    );

    let updated = request(
        &router,
        Request::put(format!("/api/v1/mappings/local/{statement_id}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"record":"a://local <=> b://local"}"#))
            .expect("local update request is built"),
    )
    .await;
    assert_eq!(updated.0, StatusCode::OK);
    let updated_page = request(
        &router,
        Request::get("/api/v1/mappings?sourceKey=local&limit=10")
            .body(Body::empty())
            .expect("updated local query request is built"),
    )
    .await;
    assert_eq!(
        updated_page.1["items"][0]["canonicalStatement"],
        "a://local <=> b://local"
    );
    let updated_statement_id = updated_page.1["items"][0]["statementId"]
        .as_str()
        .expect("updated statement ID is returned");

    let removed = request(
        &router,
        Request::delete(format!("/api/v1/mappings/local/{updated_statement_id}"))
            .body(Body::empty())
            .expect("remove local mapping request is built"),
    )
    .await;
    assert_eq!(removed.0, StatusCode::OK);

    let content_write = router
        .clone()
        .oneshot(
            Request::put(format!("/api/v1/mapping-sources/{source_id}/content"))
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("content write request is built"),
        )
        .await
        .expect("content write request completes");
    assert!(!content_write.status().is_success());
    let unchanged_content = router
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/mapping-sources/{source_id}/content"))
                .body(Body::empty())
                .expect("unchanged content request is built"),
        )
        .await
        .expect("unchanged content request completes")
        .into_body()
        .collect()
        .await
        .expect("unchanged content body is collected")
        .to_bytes();
    assert_eq!(unchanged_content.as_ref(), b"a://http <=> b://http\n");

    database.shutdown().await.expect("database shuts down");
}
