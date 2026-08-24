#![cfg(feature = "embedded-database")]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use tempfile::tempdir;
use trakkin::{
    application::{
        facade::{ApplicationFacade, ApplicationInteractions, ApplicationMappingRepositories},
        mapping_resolution::MappingResolutionService,
        sync_activity::SyncActivityService,
        sync_policy::SyncPolicyService,
    },
    database::DatabaseRuntime,
    http,
    infrastructure::{
        adapter_process::AdapterProcess,
        catalog_queries::PostgresCatalogQueryRepository,
        connection::{ConnectionMasterKey, PostgresConnectionRepository, ProcessSessionRegistry},
        mapping_lifecycle::PostgresMappingLifecycleRepository,
        mapping_queries::PostgresMappingQueryRepository,
        mapping_resolution::PostgresMappingResolutionRepository,
        sync_activity::PostgresSyncActivityRepository,
        sync_policy::PostgresSyncPolicyRepository,
        task_manager::PostgresTaskRepository,
        workspaces::PostgresWorkspaceRepository,
    },
};

#[path = "support/http/router.rs"]
pub mod support;

use support::{
    StaticBinaryAssetInteraction, StaticConnectionInteraction, StaticReferenceLookupInteraction,
    assert_closed_error, mapping_activation_request, request,
};

#[tokio::test]
async fn mapping_authoring_uses_hash_native_revisions_and_activation() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let application = ApplicationFacade::new(
        PostgresWorkspaceRepository::new(database.pool()),
        PostgresCatalogQueryRepository::new(database.pool()),
        ApplicationMappingRepositories::new(
            PostgresMappingQueryRepository::new(database.pool()),
            PostgresMappingLifecycleRepository::new(database.pool()),
        ),
        PostgresTaskRepository::new(database.pool()),
        PostgresConnectionRepository::new(database.pool(), ConnectionMasterKey::new([7_u8; 32])),
        ProcessSessionRegistry::<AdapterProcess>::default(),
        ApplicationInteractions::new(
            Arc::new(StaticConnectionInteraction),
            Arc::new(StaticBinaryAssetInteraction),
            Arc::new(StaticReferenceLookupInteraction),
            Arc::new(MappingResolutionService::new(
                PostgresMappingResolutionRepository::new(database.pool()),
            )),
            Arc::new(SyncPolicyService::new(PostgresSyncPolicyRepository::new(
                database.pool(),
            ))),
            Arc::new(SyncActivityService::new(
                PostgresSyncActivityRepository::new(database.pool()),
            )),
        ),
    );
    let router = http::router(Arc::new(application));
    let workspace = request(
        &router,
        Request::post("/api/v1/workspaces")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"Mapping authoring"}"#))
            .expect("workspace request is built"),
    )
    .await;
    assert_eq!(workspace.0, StatusCode::CREATED);
    let workspace_id = workspace.1["id"]
        .as_str()
        .expect("workspace response has an ID");

    let invalid_schema = request(
        &router,
        Request::post(format!("/api/v1/workspaces/{workspace_id}/mappings"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"schema":"trakkin.mapping/v2","mappings":[["tmdb:movie/603","imdb:title/tt0133093"]]}"#,
            ))
            .expect("invalid mapping request is built"),
    )
    .await;
    assert_eq!(invalid_schema.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&invalid_schema.1, "invalid_field");
    assert_eq!(invalid_schema.1["fieldProblems"][0]["path"], "schema");

    let created = request(
        &router,
        Request::post(format!("/api/v1/workspaces/{workspace_id}/mappings"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"schema":"trakkin.mapping/v1","mappings":[["tmdb:movie/603","imdb:title/tt0133093"]]}"#,
            ))
            .expect("mapping create request is built"),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED);
    assert_eq!(created.1["document"]["revision"], 1);
    assert_eq!(created.1["document"]["schema"], "trakkin.mapping/v1");
    assert!(created.1["activation"].is_null());
    let mapping_id = created.1["mappingId"]
        .as_str()
        .expect("mapping response has an ID");
    let first_hash = created.1["contentHash"]
        .as_str()
        .expect("mapping response has a content hash");

    let updated = request(
        &router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions"
        ))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "baseContentHash": first_hash,
                "mappings": [
                    ["tmdb:movie/603", "imdb:title/tt0133093"],
                    ["example.media:head", "example.alt:head"]
                ]
            })
            .to_string(),
        ))
        .expect("mapping update request is built"),
    )
    .await;
    assert_eq!(updated.0, StatusCode::CREATED);
    assert_eq!(updated.1["document"]["revision"], 2);
    assert_eq!(updated.1["parents"][0]["contentHash"], first_hash);
    let head_hash = updated.1["contentHash"]
        .as_str()
        .expect("updated mapping has a content hash");

    let stale = request(
        &router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions"
        ))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "baseContentHash": first_hash,
                "mappings": [
                    ["tmdb:movie/603", "imdb:title/tt0133093"],
                    ["example.media:proposed", "example.alt:proposed"]
                ]
            })
            .to_string(),
        ))
        .expect("stale mapping update request is built"),
    )
    .await;
    assert_eq!(stale.0, StatusCode::CONFLICT);
    assert_eq!(stale.1["code"], "head_moved");
    assert_eq!(stale.1["currentHead"], head_hash);
    assert_eq!(
        stale.1["added"],
        json!([["example.alt:proposed", "example.media:proposed"]])
    );
    assert_eq!(
        stale.1["removed"],
        json!([["example.alt:head", "example.media:head"]])
    );
    assert_eq!(stale.1.as_object().map(serde_json::Map::len), Some(6));

    let detail = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}"
        ))
        .body(Body::empty())
        .expect("mapping detail request is built"),
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK);
    assert_eq!(detail.1["contentHash"], head_hash);
    let history = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions"
        ))
        .body(Body::empty())
        .expect("mapping history request is built"),
    )
    .await;
    assert_eq!(history.0, StatusCode::OK);
    assert_eq!(history.1["revisions"].as_array().map(Vec::len), Some(2));
    let exact_revision = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions/{first_hash}"
        ))
        .body(Body::empty())
        .expect("exact mapping revision request is built"),
    )
    .await;
    assert_eq!(exact_revision.0, StatusCode::OK);
    assert_eq!(exact_revision.1["contentHash"], first_hash);

    let rolled_back =
        mapping_activation_request(&router, workspace_id, mapping_id, first_hash).await;
    assert_eq!(rolled_back.0, StatusCode::OK);
    assert_eq!(rolled_back.1["activeContentHash"], first_hash);
    assert_eq!(rolled_back.1["generation"], 1);
    let activated = mapping_activation_request(&router, workspace_id, mapping_id, head_hash).await;
    assert_eq!(activated.0, StatusCode::OK);
    assert_eq!(activated.1["activeContentHash"], head_hash);
    assert_eq!(activated.1["generation"], 2);

    let retired = request(
        &router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/retirement"
        ))
        .body(Body::empty())
        .expect("mapping retirement request is built"),
    )
    .await;
    assert_eq!(retired.0, StatusCode::OK);
    assert!(retired.1["activeContentHash"].is_null());
    assert_eq!(retired.1["generation"], 3);

    let invalid_hash =
        mapping_activation_request(&router, workspace_id, mapping_id, "not-a-hash").await;
    assert_eq!(invalid_hash.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&invalid_hash.1, "invalid_field");
    assert_eq!(invalid_hash.1["fieldProblems"][0]["path"], "contentHash");

    database.shutdown().await.expect("database shuts down");
}
