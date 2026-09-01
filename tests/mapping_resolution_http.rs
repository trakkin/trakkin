#![cfg(feature = "embedded-database")]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tempfile::tempdir;
use trakkin::{
    application::{
        error::ApplicationError,
        facade::{ApplicationFacade, ApplicationInteractions, ApplicationMappingRepositories},
        mapping_resolution::{
            ExactMappingRevision, MappingResolution, MappingResolutionFuture,
            MappingResolutionInteraction, MappingResolutionService, ResolutionCompleteness,
            ResolutionRequest, ResolutionSemanticWork,
        },
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
    assert_closed_error, mapping_activation_request, request, static_provider_registry,
};

#[tokio::test]
async fn mapping_resolution_previews_exact_revisions_with_bounded_closed_results() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let router_for = |mapping_resolution: Arc<dyn MappingResolutionInteraction>| {
        let application = ApplicationFacade::new(
            PostgresWorkspaceRepository::new(database.pool()),
            PostgresCatalogQueryRepository::new(database.pool()),
            ApplicationMappingRepositories::new(
                PostgresMappingQueryRepository::new(database.pool()),
                PostgresMappingLifecycleRepository::new(database.pool()),
            ),
            PostgresTaskRepository::new(database.pool()),
            PostgresConnectionRepository::new(
                database.pool(),
                ConnectionMasterKey::new([7_u8; 32]),
            ),
            ProcessSessionRegistry::<AdapterProcess>::default(),
            ApplicationInteractions::new(
                Arc::new(StaticConnectionInteraction),
                Arc::new(StaticBinaryAssetInteraction),
                Arc::new(StaticReferenceLookupInteraction),
                mapping_resolution,
                Arc::new(SyncPolicyService::new(PostgresSyncPolicyRepository::new(
                    database.pool(),
                ))),
                Arc::new(SyncActivityService::new(
                    PostgresSyncActivityRepository::new(database.pool()),
                )),
                static_provider_registry(),
            ),
        );
        http::router(Arc::new(application))
    };
    let router = router_for(Arc::new(MappingResolutionService::new(
        PostgresMappingResolutionRepository::new(database.pool()),
    )));
    let workspace = request(
        &router,
        Request::post("/api/v1/workspaces")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"Resolution preview"}"#))
            .expect("workspace request is built"),
    )
    .await;
    assert_eq!(workspace.0, StatusCode::CREATED);
    let workspace_id = workspace.1["id"].as_str().expect("workspace has an ID");

    let historical = request(
        &router,
        Request::post(format!("/api/v1/workspaces/{workspace_id}/mappings"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"schema":"trakkin.mapping/v1","mappings":[["example.media://a","example.media://b"]]}"#,
            ))
            .expect("historical mapping request is built"),
    )
    .await;
    assert_eq!(historical.0, StatusCode::CREATED);
    let mapping_id = historical.1["mappingId"]
        .as_str()
        .expect("mapping has an ID");
    let historical_hash = historical.1["contentHash"]
        .as_str()
        .expect("historical mapping has a hash");
    let successor = request(
        &router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions"
        ))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "baseContentHash": historical_hash,
                "mappings": [
                    ["example.media://a", "example.media://b"],
                    ["example.media://b", "example.media://c"]
                ]
            })
            .to_string(),
        ))
        .expect("successor mapping request is built"),
    )
    .await;
    assert_eq!(successor.0, StatusCode::CREATED);
    let successor_hash = successor.1["contentHash"]
        .as_str()
        .expect("successor has a hash");
    assert_eq!(
        mapping_activation_request(&router, workspace_id, mapping_id, successor_hash)
            .await
            .0,
        StatusCode::OK
    );

    let decoy = request(
        &router,
        Request::post(format!("/api/v1/workspaces/{workspace_id}/mappings"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"schema":"trakkin.mapping/v1","mappings":[["example.media://a","example.media://decoy"]]}"#,
            ))
            .expect("decoy mapping request is built"),
    )
    .await;
    assert_eq!(decoy.0, StatusCode::CREATED);
    let decoy_id = decoy.1["mappingId"].as_str().expect("decoy has an ID");
    let decoy_hash = decoy.1["contentHash"].as_str().expect("decoy has a hash");
    assert_eq!(
        mapping_activation_request(&router, workspace_id, decoy_id, decoy_hash)
            .await
            .0,
        StatusCode::OK
    );

    let historical_preview = mapping_resolution_preview_request(
        &router,
        workspace_id,
        mapping_id,
        historical_hash,
        json!({"mode": "transitive", "references": ["example.media://a"]}),
    )
    .await;
    assert_eq!(historical_preview.0, StatusCode::OK);
    assert_eq!(historical_preview.1["mappingId"], mapping_id);
    assert_eq!(historical_preview.1["contentHash"], historical_hash);
    assert_eq!(historical_preview.1["mode"], "transitive");
    assert_eq!(historical_preview.1["completeness"], "complete");
    assert_eq!(historical_preview.1["transportLookupCalls"], 0);
    assert_eq!(historical_preview.1["semanticWork"]["endpointHits"], 2);
    assert_eq!(
        historical_preview.1["correspondences"][0]["endpoints"],
        json!(["example.media://a", "example.media://b"])
    );
    assert!(!historical_preview.1.to_string().contains("example.media://c"));
    assert!(
        !historical_preview
            .1
            .to_string()
            .contains("example.media://decoy")
    );

    let direct = mapping_resolution_preview_request(
        &router,
        workspace_id,
        mapping_id,
        successor_hash,
        json!({"mode": "direct", "references": ["example.media://a"]}),
    )
    .await;
    assert_eq!(direct.0, StatusCode::OK);
    assert!(!direct.1.to_string().contains("example.media://c"));
    let transitive = mapping_resolution_preview_request(
        &router,
        workspace_id,
        mapping_id,
        successor_hash,
        json!({"mode": "transitive", "references": ["example.media://a"]}),
    )
    .await;
    assert_eq!(transitive.0, StatusCode::OK);
    assert!(transitive.1.to_string().contains("example.media://c"));
    assert!(!transitive.1.to_string().contains("example.media://decoy"));

    let partial = mapping_resolution_preview_request(
        &router,
        workspace_id,
        mapping_id,
        historical_hash,
        json!({
            "mode": "direct",
            "references": ["example.media://a", "example.media://missing"]
        }),
    )
    .await;
    assert_eq!(partial.0, StatusCode::OK);
    assert_eq!(partial.1["completeness"], "partial");
    assert_eq!(partial.1["findings"][0]["kind"], "unresolved");

    for (payload, path) in [
        (
            json!({"mode": "sideways", "references": ["example.media://a"]}),
            "mode",
        ),
        (json!({"mode": "direct", "references": []}), "references"),
        (
            json!({"mode": "direct", "references": ["not a portable endpoint"]}),
            "references[0]",
        ),
        (
            json!({"mode": "direct", "references": ["example.media://a"], "window": "episode:1.."}),
            "window",
        ),
    ] {
        let invalid = mapping_resolution_preview_request(
            &router,
            workspace_id,
            mapping_id,
            historical_hash,
            payload,
        )
        .await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST);
        assert_closed_error(&invalid.1, "invalid_field");
        assert_eq!(invalid.1["fieldProblems"][0]["path"], path);
    }
    let invalid_hash = mapping_resolution_preview_request(
        &router,
        workspace_id,
        mapping_id,
        "not-a-hash",
        json!({"mode": "direct", "references": ["example.media://a"]}),
    )
    .await;
    assert_eq!(invalid_hash.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&invalid_hash.1, "invalid_field");
    assert_eq!(invalid_hash.1["fieldProblems"][0]["path"], "contentHash");
    let missing_revision = mapping_resolution_preview_request(
        &router,
        workspace_id,
        mapping_id,
        &format!("sha256:{}", "0".repeat(64)),
        json!({"mode": "direct", "references": ["example.media://a"]}),
    )
    .await;
    assert_eq!(missing_revision.0, StatusCode::NOT_FOUND);
    assert_closed_error(&missing_revision.1, "mapping_revision_not_found");

    let stale_router = router_for(Arc::new(StaticMappingResolutionInteraction {
        resolution: empty_resolution(ResolutionCompleteness::Stale, None),
    }));
    let stale = mapping_resolution_preview_request(
        &stale_router,
        workspace_id,
        mapping_id,
        historical_hash,
        json!({"mode": "direct", "references": ["example.media://a"]}),
    )
    .await;
    assert_eq!(stale.0, StatusCode::OK);
    assert_eq!(stale.1["completeness"], "stale");

    let oversized_router = router_for(Arc::new(StaticMappingResolutionInteraction {
        resolution: empty_resolution(
            ResolutionCompleteness::Truncated,
            Some("x".repeat(8 * 1024 * 1024)),
        ),
    }));
    let oversized = mapping_resolution_preview_request(
        &oversized_router,
        workspace_id,
        mapping_id,
        historical_hash,
        json!({"mode": "direct", "references": ["example.media://a"]}),
    )
    .await;
    assert_eq!(oversized.0, StatusCode::PAYLOAD_TOO_LARGE);
    assert_closed_error(&oversized.1, "mapping_response_too_large");

    database.shutdown().await.expect("database shuts down");
}

async fn mapping_resolution_preview_request(
    router: &axum::Router,
    workspace_id: &str,
    mapping_id: &str,
    content_hash: &str,
    payload: Value,
) -> (StatusCode, Value) {
    request(
        router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/revisions/{content_hash}/resolution-preview"
        ))
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .expect("mapping resolution preview request is built"),
    )
    .await
}

#[derive(Clone)]
struct StaticMappingResolutionInteraction {
    resolution: MappingResolution,
}

impl MappingResolutionInteraction for StaticMappingResolutionInteraction {
    fn resolve_workspace_mappings(
        &self,
        _request: ResolutionRequest,
    ) -> MappingResolutionFuture<'_, MappingResolution, ApplicationError> {
        let resolution = self.resolution.clone();
        Box::pin(async move { Ok(resolution) })
    }

    fn resolve_mapping_revision(
        &self,
        _revision: ExactMappingRevision,
        _request: ResolutionRequest,
    ) -> MappingResolutionFuture<'_, MappingResolution, ApplicationError> {
        let resolution = self.resolution.clone();
        Box::pin(async move { Ok(resolution) })
    }
}

fn empty_resolution(
    completeness: ResolutionCompleteness,
    next_work_key: Option<String>,
) -> MappingResolution {
    MappingResolution {
        completeness,
        pins: None,
        correspondences: Vec::new(),
        findings: Vec::new(),
        published_matches: Vec::new(),
        endpoint_lookup_matches: Vec::new(),
        lookup_calls: 0,
        semantic_work: ResolutionSemanticWork::default(),
        next_work_key,
    }
}
