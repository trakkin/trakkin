#![cfg(feature = "embedded-database")]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{
        Request, StatusCode,
        header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG},
    },
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::tempdir;
use tower::ServiceExt;
use trakkin::{
    application::{
        adapter_runtime::AdapterValue,
        connection::ConnectionRepository,
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
use uuid::Uuid;

#[path = "support/http/router.rs"]
pub mod support;

use support::{
    CONNECTION_ID, SOURCE_ID, StaticBinaryAssetInteraction, StaticConnectionInteraction,
    StaticReferenceLookupInteraction, assert_closed_error, request, seed_source,
    static_provider_registry, timestamp,
};

#[tokio::test]
async fn connection_and_acquisition_routes_are_closed_bounded_and_secret_safe() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_source(&database.pool()).await;
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
            static_provider_registry(),
        ),
    );
    let router = http::router(Arc::new(application), ".".into());

    let created_connection = request(
        &router,
        Request::post("/api/v1/connections")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"providerId":"dev.trakkin.fixture","displayName":"HTTP fixture","settings":[{"key":"scenario","value":{"kind":"text","value":"hierarchical-catalog-assets-v1"}}],"secrets":[{"key":"token","value":[104,116,116,112,45,99,111,110,110,101,99,116,105,111,110,45,115,101,99,114,101,116]},{"key":"preserve","value":[112,114,101,115,101,114,118,101,100]},{"key":"remove","value":[114,101,109,111,118,101,100]}]}"#,
            ))
            .expect("connection request is built"),
    )
    .await;
    assert_eq!(created_connection.0, StatusCode::CREATED);
    assert_eq!(created_connection.1["status"], "pending");
    assert!(created_connection.1.get("settings").is_none());
    assert!(created_connection.1.get("secrets").is_none());
    let created_connection_id = created_connection.1["id"]
        .as_str()
        .expect("connection response has an ID");

    let connections = request(
        &router,
        Request::get("/api/v1/connections")
            .body(Body::empty())
            .expect("connection list request is built"),
    )
    .await;
    assert_eq!(connections.0, StatusCode::OK);
    assert_eq!(connections.1.as_array().map(Vec::len), Some(2));
    assert!(!connections.1.to_string().contains("http-connection-secret"));

    let renamed_connection = request(
        &router,
        Request::patch(format!("/api/v1/connections/{created_connection_id}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"displayName":"Renamed fixture"}"#))
            .expect("rename request is built"),
    )
    .await;
    assert_eq!(renamed_connection.0, StatusCode::OK);
    assert_eq!(renamed_connection.1["displayName"], "Renamed fixture");

    let reconfigured_connection = request(
        &router,
        Request::put(format!(
            "/api/v1/connections/{created_connection_id}/configuration"
        ))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "settings": [{
                    "key": "scenario",
                    "value": {"kind": "text", "value": "reconfigured"},
                }],
                "secretPatches": [
                    {"key": "token", "value": b"replacement-secret"},
                    {"key": "remove", "value": Value::Null},
                ],
            })
            .to_string(),
        ))
        .expect("reconfiguration request is built"),
    )
    .await;
    assert_eq!(reconfigured_connection.0, StatusCode::OK);
    assert_eq!(reconfigured_connection.1["status"], "pending");
    assert!(
        !reconfigured_connection
            .1
            .to_string()
            .contains("replacement-secret")
    );

    let repository =
        PostgresConnectionRepository::new(database.pool(), ConnectionMasterKey::new([7_u8; 32]));
    let stored = repository
        .load_connection(Uuid::parse_str(created_connection_id).expect("connection ID is valid"))
        .await
        .expect("reconfigured connection loads")
        .expect("reconfigured connection exists");
    assert_eq!(
        stored.settings,
        vec![trakkin::application::adapter_runtime::AdapterSetting {
            key: "scenario".to_owned(),
            value: AdapterValue::Text("reconfigured".to_owned()),
        }]
    );
    assert_eq!(
        stored
            .secrets
            .iter()
            .map(|secret| (secret.key.as_str(), secret.value.as_slice()))
            .collect::<Vec<_>>(),
        vec![
            ("preserve", b"preserved".as_slice()),
            ("token", b"replacement-secret".as_slice()),
        ]
    );

    let inspection = request(
        &router,
        Request::get(format!(
            "/api/v1/connections/{created_connection_id}/inspection"
        ))
        .body(Body::empty())
        .expect("inspection request is built"),
    )
    .await;
    assert_eq!(inspection.0, StatusCode::OK);
    assert_eq!(inspection.1["fields"][0]["secret"], true);
    assert_eq!(inspection.1["fields"][0]["defaultValue"], Value::Null);
    assert!(
        inspection.1["fields"][0]["options"]
            .as_array()
            .is_some_and(Vec::is_empty)
    );
    assert!(!inspection.1.to_string().contains("secret-default"));
    assert!(!inspection.1.to_string().contains("secret-option"));

    let invalid_authentication = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/authentications"
        ))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"method":" ","values":[]}"#))
        .expect("invalid authentication request is built"),
    )
    .await;
    assert_eq!(invalid_authentication.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&invalid_authentication.1, "invalid_field");

    let started_authentication = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/authentications"
        ))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"method":"fixture/device","values":[]}"#))
        .expect("authentication start request is built"),
    )
    .await;
    assert_eq!(started_authentication.0, StatusCode::CREATED);
    assert_eq!(started_authentication.1["status"], "inputRequired");
    assert_eq!(started_authentication.1["prompt"]["userCode"], "FIXTURE");
    assert!(started_authentication.1.get("secretPatches").is_none());
    assert!(
        !started_authentication
            .1
            .to_string()
            .contains("response-secret")
    );
    assert!(
        !started_authentication
            .1
            .to_string()
            .contains("provider-device-auth")
    );
    let authentication_id = started_authentication.1["id"]
        .as_str()
        .expect("authentication response has an ID");
    let continued_authentication = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/authentications/{authentication_id}/continuation"
        ))
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"values":[{"key":"code","value":{"kind":"text","value":"approved"}}]}"#,
        ))
        .expect("authentication continuation request is built"),
    )
    .await;
    assert_eq!(continued_authentication.0, StatusCode::OK);
    assert_eq!(continued_authentication.1["status"], "completed");
    assert_eq!(
        continued_authentication.1["accounts"][0]["displayName"],
        "Authenticated"
    );
    assert!(continued_authentication.1.get("secretPatches").is_none());
    assert!(
        !continued_authentication
            .1
            .to_string()
            .contains("response-secret")
    );

    let cancellable_authentication = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/authentications"
        ))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"method":"fixture/cancel","values":[]}"#))
        .expect("cancellable authentication request is built"),
    )
    .await;
    assert_eq!(cancellable_authentication.0, StatusCode::CREATED);
    let cancellable_authentication_id = cancellable_authentication.1["id"]
        .as_str()
        .expect("cancellable authentication has an ID");
    let cancelled_authentication = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/authentications/{cancellable_authentication_id}/cancellation"
        ))
        .body(Body::empty())
        .expect("authentication cancellation request is built"),
    )
    .await;
    assert_eq!(cancelled_authentication.0, StatusCode::OK);
    assert_eq!(cancelled_authentication.1["status"], "cancelled");

    let discovered_sources = request(
        &router,
        Request::get(format!("/api/v1/connections/{CONNECTION_ID}/sources"))
            .body(Body::empty())
            .expect("source list request is built"),
    )
    .await;
    assert_eq!(discovered_sources.0, StatusCode::OK);
    assert_eq!(discovered_sources.1.as_array().map(Vec::len), Some(1));

    let discovery_task = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/discovery-tasks"
        ))
        .body(Body::empty())
        .expect("discovery task request is built"),
    )
    .await;
    assert_eq!(discovery_task.0, StatusCode::ACCEPTED);
    let discovery_task_id = discovery_task.1["id"]
        .as_str()
        .expect("discovery task has an ID");
    let hidden_discovery_task = request(
        &router,
        Request::get(format!(
            "/api/v1/connections/{CONNECTION_ID}/tasks/{discovery_task_id}"
        ))
        .body(Body::empty())
        .expect("cross-connection task request is built"),
    )
    .await;
    assert_eq!(hidden_discovery_task.0, StatusCode::NOT_FOUND);
    assert_closed_error(&hidden_discovery_task.1, "task_not_found");
    let busy_deletion = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/deletion"
        ))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"confirmed":true}"#))
        .expect("busy deletion request is built"),
    )
    .await;
    assert_eq!(busy_deletion.0, StatusCode::CONFLICT);
    assert_closed_error(&busy_deletion.1, "connection_busy");
    let cancelled_discovery = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/tasks/{discovery_task_id}/cancellation"
        ))
        .body(Body::empty())
        .expect("discovery cancellation request is built"),
    )
    .await;
    assert_eq!(cancelled_discovery.0, StatusCode::OK);

    let deleted_connection = request(
        &router,
        Request::post(format!(
            "/api/v1/connections/{created_connection_id}/deletion"
        ))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"confirmed":true}"#))
        .expect("deletion request is built"),
    )
    .await;
    assert_eq!(deleted_connection.0, StatusCode::OK);
    assert_eq!(deleted_connection.1["activeTaskCount"], 0);
    let missing_connection = request(
        &router,
        Request::get(format!(
            "/api/v1/connections/{created_connection_id}/deletion-impact"
        ))
        .body(Body::empty())
        .expect("deleted connection request is built"),
    )
    .await;
    assert_eq!(missing_connection.0, StatusCode::NOT_FOUND);
    assert_closed_error(&missing_connection.1, "connection_not_found");

    let malformed = request(
        &router,
        Request::post("/api/v1/workspaces")
            .header("content-type", "application/json")
            .body(Body::from("{"))
            .expect("malformed request is built"),
    )
    .await;
    assert_eq!(malformed.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&malformed.1, "invalid_request");
    let workspace = request(
        &router,
        Request::post("/api/v1/workspaces")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"Acquisition contract"}"#))
            .expect("workspace request is built"),
    )
    .await;
    assert_eq!(workspace.0, StatusCode::CREATED);
    let workspace_id = workspace.1["id"]
        .as_str()
        .expect("workspace response has an ID");

    let attached = request(
        &router,
        Request::put(format!(
            "/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}"
        ))
        .body(Body::empty())
        .expect("attachment request is built"),
    )
    .await;
    assert_eq!(attached.0, StatusCode::OK);

    let policy_path =
        format!("/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}/mapping-root-policy");
    let default_policy = request(
        &router,
        Request::get(&policy_path)
            .body(Body::empty())
            .expect("mapping root policy request is built"),
    )
    .await;
    assert_eq!(default_policy.0, StatusCode::OK);
    assert!(default_policy.1["namespaces"].is_null());
    assert!(default_policy.1["availableNamespaces"].is_array());

    let missing_namespaces = request(
        &router,
        Request::put(&policy_path)
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .expect("mapping root policy request is built"),
    )
    .await;
    assert_eq!(missing_namespaces.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&missing_namespaces.1, "invalid_request");

    let disabled_policy = request(
        &router,
        Request::put(&policy_path)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"namespaces":[]}"#))
            .expect("mapping root policy request is built"),
    )
    .await;
    assert_eq!(disabled_policy.0, StatusCode::OK);
    assert_eq!(disabled_policy.1["namespaces"], json!([]));

    let reset_policy = request(
        &router,
        Request::put(&policy_path)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"namespaces":null}"#))
            .expect("mapping root policy request is built"),
    )
    .await;
    assert_eq!(reset_policy.0, StatusCode::OK);
    assert!(reset_policy.1["namespaces"].is_null());

    let first_page = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}/catalog?limit=1"
        ))
        .body(Body::empty())
        .expect("catalog request is built"),
    )
    .await;
    assert_eq!(first_page.0, StatusCode::OK);
    assert_eq!(first_page.1["entries"].as_array().map(Vec::len), Some(1));
    let cursor = first_page.1["nextCursor"]
        .as_str()
        .expect("bounded page has a cursor");
    let asset_href = first_page.1["entries"][0]["item"]["binaryAssets"][0]["href"]
        .as_str()
        .expect("catalog asset has a browser URL");
    let asset_response = router
        .clone()
        .oneshot(
            Request::get(asset_href)
                .body(Body::empty())
                .expect("asset request is built"),
        )
        .await
        .expect("asset request completes");
    assert_eq!(asset_response.status(), StatusCode::OK);
    assert_eq!(asset_response.headers()[CONTENT_TYPE], "image/jpeg");
    assert_eq!(asset_response.headers()[CONTENT_LENGTH], "12");
    assert_eq!(
        asset_response.headers()[CACHE_CONTROL],
        "public, max-age=300"
    );
    assert_eq!(
        asset_response.headers()[ETAG],
        format!("\"{}\"", "ab".repeat(32))
    );
    assert_eq!(
        asset_response.headers()["x-content-type-options"],
        "nosniff"
    );
    let asset_body = asset_response
        .into_body()
        .collect()
        .await
        .expect("asset body is collected")
        .to_bytes();
    assert_eq!(asset_body.as_ref(), b"poster-bytes");

    let lookup = request(
        &router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}/reference-lookups"
        ))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "references": [
                    {"namespace": "example.media", "value": [1]},
                    {"namespace": "example.media", "value": [2]},
                    {"namespace": "example.media", "value": [3]},
                    {"namespace": "example.media", "value": [4]}
                ]
            })
            .to_string(),
        ))
        .expect("reference lookup request is built"),
    )
    .await;
    assert_eq!(lookup.0, StatusCode::OK);
    assert_eq!(lookup.1[0]["outcome"]["kind"], "matched");
    let lookup_candidate =
        &lookup.1[0]["outcome"]["candidate"]["providerItem"]["portableReferenceCandidates"][0];
    assert_eq!(lookup_candidate["providerRecommended"], true);
    assert_eq!(lookup_candidate["effectiveMappingRoot"], true);
    assert_eq!(lookup.1[1]["outcome"]["kind"], "notFound");
    assert_eq!(lookup.1[2]["outcome"]["kind"], "ambiguous");
    assert_eq!(lookup.1[3]["outcome"]["kind"], "unsupported");

    let state = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}/state?subjectKind=providerItem&subjectNamespace=fixture&subjectValue={}",
            hex::encode(b"item.0")
        ))
        .body(Body::empty())
        .expect("state request is built"),
    )
    .await;
    assert_eq!(state.0, StatusCode::OK);
    assert_eq!(state.1["observations"][0]["value"]["value"], 7);

    let invalid_read = request(
        &router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}/reads"
        ))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"stream":"catalog","mode":"latest"}"#))
        .expect("invalid read request is built"),
    )
    .await;
    assert_eq!(invalid_read.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&invalid_read.1, "invalid_field");
    let read_task = request(
        &router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}/reads"
        ))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"stream":"catalog","mode":"full"}"#))
        .expect("read task request is built"),
    )
    .await;
    assert_eq!(read_task.0, StatusCode::ACCEPTED);
    let read_task_id = read_task.1["id"].as_str().expect("read task has an ID");
    let listed_tasks = request(
        &router,
        Request::get(format!("/api/v1/workspaces/{workspace_id}/tasks"))
            .body(Body::empty())
            .expect("task list request is built"),
    )
    .await;
    assert_eq!(listed_tasks.0, StatusCode::OK);
    assert_eq!(listed_tasks.1.as_array().map(Vec::len), Some(1));
    let cancelled_task = request(
        &router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/tasks/{read_task_id}/cancellation"
        ))
        .body(Body::empty())
        .expect("task cancellation request is built"),
    )
    .await;
    assert_eq!(cancelled_task.0, StatusCode::OK);
    assert_eq!(cancelled_task.1["status"], "cancelled");

    sqlx::query(
        "UPDATE source_streams
SET publication_id = $1, updated_at = $2
WHERE source_id = $3 AND stream_kind = 'catalog'
",
    )
    .bind(Uuid::from_u128(6))
    .bind(timestamp(20))
    .bind(SOURCE_ID)
    .execute(&database.pool())
    .await
    .expect("catalog publication advances");
    let expired = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}/catalog?limit=1&cursor={cursor}"
        ))
        .body(Body::empty())
        .expect("expired cursor request is built"),
    )
    .await;
    assert_eq!(expired.0, StatusCode::CONFLICT);
    assert_closed_error(&expired.1, "catalog_cursor_expired");

    let detached = request(
        &router,
        Request::delete(format!(
            "/api/v1/workspaces/{workspace_id}/sources/{SOURCE_ID}"
        ))
        .body(Body::empty())
        .expect("detachment request is built"),
    )
    .await;
    assert_eq!(detached.0, StatusCode::OK);
    assert!(detached.1["sources"].as_array().is_some_and(Vec::is_empty));

    let malformed_path = request(
        &router,
        Request::get("/api/v1/workspaces/not-a-uuid")
            .body(Body::empty())
            .expect("malformed path request is built"),
    )
    .await;
    assert_eq!(malformed_path.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&malformed_path.1, "invalid_request");

    database.shutdown().await.expect("database shuts down");
}
