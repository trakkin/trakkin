#![cfg(feature = "embedded-database")]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use time::{Duration, OffsetDateTime};
use trakkin::{
    application::{
        facade::{ApplicationFacade, ApplicationInteractions, ApplicationMappingRepositories},
        mapping_resolution::MappingResolutionService,
        sync_activity::SyncActivityService,
        sync_policy::SyncPolicyService,
        sync_preview::{
            SyncPreviewFingerprint, SyncPreviewMembershipPin, sync_preview_policy_revision_pin,
        },
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
    CATALOG_PUBLICATION_ID, CONNECTION_ID, SOURCE_ID, STATE_PUBLICATION_ID,
    StaticBinaryAssetInteraction, StaticConnectionInteraction, StaticReferenceLookupInteraction,
    assert_closed_error, request, seed_source, timestamp,
};

#[tokio::test]
async fn activity_routes_page_runs_and_expose_bounded_provenance() {
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
        ),
    );
    let router = http::router(Arc::new(application));
    let workspace = request(
        &router,
        Request::post("/api/v1/workspaces")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"Activity contract"}"#))
            .expect("workspace request is built"),
    )
    .await;
    assert_eq!(workspace.0, StatusCode::CREATED);
    let workspace_id = Uuid::parse_str(
        workspace.1["id"]
            .as_str()
            .expect("workspace response has an ID"),
    )
    .expect("workspace ID is valid");
    let fixture = seed_http_activity(&database.pool(), workspace_id).await;

    let first_page = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/activity?status=succeeded&sourceId={SOURCE_ID}&field=progress&occurredAtOrAfter=2000-01-01T00:00:00Z&occurredBefore=2100-01-01T00:00:00Z&limit=1"
        ))
        .body(Body::empty())
        .expect("Activity page request is built"),
    )
    .await;
    assert_eq!(first_page.0, StatusCode::OK, "{:?}", first_page.1);
    assert_eq!(first_page.1["runs"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        first_page.1["runs"][0]["runId"],
        fixture.reconciled_run_id.to_string()
    );
    assert_eq!(first_page.1["runs"][0]["status"], "succeeded");
    assert_eq!(
        first_page.1["runs"][0]["actions"][0]["semanticField"],
        "progress"
    );
    assert_eq!(
        first_page.1["runs"][0]["actions"][0]["reconciliationTask"]["taskId"],
        fixture.reconciliation_task_id.to_string()
    );
    let cursor = first_page.1["nextCursor"]
        .as_str()
        .expect("bounded first page has a cursor");

    let second_page = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/activity?status=succeeded&sourceId={SOURCE_ID}&field=progress&occurredAtOrAfter=2000-01-01T00:00:00Z&occurredBefore=2100-01-01T00:00:00Z&limit=1&cursor={cursor}"
        ))
        .body(Body::empty())
        .expect("second Activity page request is built"),
    )
    .await;
    assert_eq!(second_page.0, StatusCode::OK);
    assert_eq!(second_page.1["runs"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        second_page.1["runs"][0]["runId"],
        fixture.direct_run_id.to_string()
    );
    assert!(second_page.1["nextCursor"].is_null());

    let changed_filter_cursor = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/activity?status=failed&limit=1&cursor={cursor}"
        ))
        .body(Body::empty())
        .expect("changed-filter cursor request is built"),
    )
    .await;
    assert_eq!(changed_filter_cursor.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&changed_filter_cursor.1, "activity_cursor_invalid");

    let other_workspace = request(
        &router,
        Request::post("/api/v1/workspaces")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"Other Activity"}"#))
            .expect("other workspace is built"),
    )
    .await;
    let other_workspace_id = other_workspace.1["id"]
        .as_str()
        .expect("other workspace has an ID");
    let cross_workspace_cursor = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{other_workspace_id}/activity?status=succeeded&sourceId={SOURCE_ID}&field=progress&occurredAtOrAfter=2000-01-01T00:00:00Z&occurredBefore=2100-01-01T00:00:00Z&limit=1&cursor={cursor}"
        ))
        .body(Body::empty())
        .expect("cross-workspace cursor request is built"),
    )
    .await;
    assert_eq!(cross_workspace_cursor.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&cross_workspace_cursor.1, "activity_cursor_invalid");

    let direct_detail = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/activity/actions/{}",
            fixture.direct_action_id
        ))
        .body(Body::empty())
        .expect("direct Activity detail request is built"),
    )
    .await;
    assert_eq!(direct_detail.0, StatusCode::OK);
    assert_eq!(direct_detail.1["reconciliationGuidance"], "not_required");
    assert_eq!(
        direct_detail.1["provenance"]["contributors"][0]["evidenceId"],
        "http-evidence"
    );
    assert_eq!(
        direct_detail.1["provenance"]["mappingProofs"][0]["key"],
        "http-proof"
    );
    assert_eq!(
        direct_detail.1["provenance"]["attempt"]["outcome"]["status"],
        "applied"
    );
    assert_eq!(direct_detail.1["provenance"]["reconciliation"], Value::Null);

    let reconciled_detail = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/activity/actions/{}",
            fixture.reconciled_action_id
        ))
        .body(Body::empty())
        .expect("reconciled Activity detail request is built"),
    )
    .await;
    assert_eq!(reconciled_detail.0, StatusCode::OK);
    assert_eq!(
        reconciled_detail.1["reconciliationGuidance"],
        "confirmed_applied"
    );
    assert!(reconciled_detail.1["provenance"]["attempt"]["outcome"].is_null());
    assert_eq!(
        reconciled_detail.1["provenance"]["reconciliation"]["result"],
        "confirmed_applied"
    );

    for response in [&first_page.1, &direct_detail.1, &reconciled_detail.1] {
        let serialized = response.to_string();
        assert!(!serialized.contains("credential"));
        assert!(!serialized.contains("launchToken"));
        assert!(!serialized.contains("rawProviderPayload"));
        assert!(!serialized.contains("protobuf"));
        assert!(!serialized.contains("canonicalMedia"));
        assert!(serialized.len() < 64 * 1024);
    }

    let invalid_status = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{workspace_id}/activity?status=unknown"
        ))
        .body(Body::empty())
        .expect("invalid status request is built"),
    )
    .await;
    assert_eq!(invalid_status.0, StatusCode::BAD_REQUEST);
    assert_closed_error(&invalid_status.1, "invalid_field");

    let hidden_action = request(
        &router,
        Request::get(format!(
            "/api/v1/workspaces/{other_workspace_id}/activity/actions/{}",
            fixture.direct_action_id
        ))
        .body(Body::empty())
        .expect("cross-workspace detail request is built"),
    )
    .await;
    assert_eq!(hidden_action.0, StatusCode::NOT_FOUND);
    assert_closed_error(&hidden_action.1, "activity_action_not_found");

    database.shutdown().await.expect("database shuts down");
}

struct HttpActivityFixture {
    direct_run_id: Uuid,
    direct_action_id: Uuid,
    reconciled_run_id: Uuid,
    reconciled_action_id: Uuid,
    reconciliation_task_id: Uuid,
}

async fn seed_http_activity(pool: &sqlx::PgPool, workspace_id: Uuid) -> HttpActivityFixture {
    let policy_id = Uuid::from_u128(200);
    let policy_hash = format!("sha256:{:064x}", 200);
    sqlx::query(
        "INSERT INTO sync_policies (
    id, workspace_id, head_revision, head_content_hash, created_at, updated_at
)
VALUES ($1, $2, 1, $3, $4, $4)
",
    )
    .bind(policy_id)
    .bind(workspace_id)
    .bind(&policy_hash)
    .bind(timestamp(0))
    .execute(pool)
    .await
    .expect("Activity policy is seeded");
    sqlx::query(
        "INSERT INTO sync_policy_revisions (
    policy_id, workspace_id, revision, content_hash,
    schema_name, canonical_json, created_at
)
VALUES ($1, $2, 1, $3, 'trakkin.sync-policy/v1', $4, $5)
",
    )
    .bind(policy_id)
    .bind(workspace_id)
    .bind(&policy_hash)
    .bind(b"{}".as_slice())
    .bind(timestamp(0))
    .execute(pool)
    .await
    .expect("Activity policy revision is seeded");

    let direct = seed_http_activity_run(
        pool,
        workspace_id,
        policy_id,
        &policy_hash,
        210,
        timestamp(10),
        false,
    )
    .await;
    let reconciled = seed_http_activity_run(
        pool,
        workspace_id,
        policy_id,
        &policy_hash,
        220,
        timestamp(20),
        true,
    )
    .await;
    HttpActivityFixture {
        direct_run_id: direct.run_id,
        direct_action_id: direct.action_id,
        reconciled_run_id: reconciled.run_id,
        reconciled_action_id: reconciled.action_id,
        reconciliation_task_id: reconciled
            .reconciliation_task_id
            .expect("reconciled fixture has a task"),
    }
}

struct SeededActivityRun {
    run_id: Uuid,
    action_id: Uuid,
    reconciliation_task_id: Option<Uuid>,
}

#[allow(clippy::too_many_arguments)]
async fn seed_http_activity_run(
    pool: &sqlx::PgPool,
    workspace_id: Uuid,
    policy_id: Uuid,
    policy_hash: &str,
    identity: u128,
    created_at: OffsetDateTime,
    reconciled: bool,
) -> SeededActivityRun {
    let preview_id = Uuid::from_u128(identity);
    let run_id = Uuid::from_u128(identity + 2);
    let action_id = Uuid::from_u128(identity + 3);
    let write_task_id = Uuid::from_u128(identity + 4);
    let write_attempt_id = Uuid::from_u128(identity + 5);
    let reconciliation_task_id = reconciled.then(|| Uuid::from_u128(identity + 6));
    let reconciliation_attempt_id = reconciled.then(|| Uuid::from_u128(identity + 7));
    let owner_id = Uuid::from_u128(identity + 8);
    let operation_id = Uuid::from_u128(identity + 9);
    let settled_at = created_at + Duration::seconds(1);
    let reconciliation_settled_at = settled_at + Duration::seconds(1);
    let preview_hash = format!("sha256:{identity:064x}");
    let preview_action_json = serde_json::to_vec(&json!({
        "aggregation": {
            "reducer": "maximum",
            "contributors": [{
                "evidenceId": "http-evidence",
                "statePublicationId": STATE_PUBLICATION_ID,
                "presence": "present",
                "mappingProofKey": "http-proof"
            }],
            "mappingProofs": [{
                "key": "http-proof",
                "revisionHash": "sha256:mapping-revision",
                "rowHash": "sha256:mapping-row"
            }]
        },
        "translation": {
            "unit": "count",
            "quantizer": "exact",
            "exactValue": {"numerator": 7, "denominator": 1},
            "intendedValue": 7,
            "lossy": false
        },
        "membershipEffect": "update"
    }))
    .expect("Activity action JSON serializes");
    let action_hash = format!("sha256:{:x}", Sha256::digest(&preview_action_json));
    let fingerprint = SyncPreviewFingerprint {
        policy_revision: sync_preview_policy_revision_pin(1, policy_hash),
        state_registry_digest: "sha256:http-state-registry".to_owned(),
        resolver_version: "http-resolver-v1".to_owned(),
        active_mapping_hashes: vec!["sha256:http-mapping".to_owned()],
        mapping_activation_generation: 1,
        mapping_proof_digest: "sha256:http-proof".to_owned(),
        attachment_digest: "sha256:http-attachment".to_owned(),
        source_capability_revision: "http-source-capability-v1".to_owned(),
        connection_capability_revision: "http-connection-capability-v1".to_owned(),
        state_publication: STATE_PUBLICATION_ID.to_string(),
        contributor_evidence_revisions: vec!["http-evidence-v1".to_owned()],
        catalog_publication: CATALOG_PUBLICATION_ID.to_string(),
        coordinate_publication: "http-coordinate-v1".to_owned(),
        lookup_process: "http-process-v1".to_owned(),
        lookup_revision: "http-lookup-v1".to_owned(),
        lookup_observed_at: created_at - Duration::seconds(1),
        lookup_expiry: created_at + Duration::seconds(100),
        targeted_membership: SyncPreviewMembershipPin::Present,
        targeted_state_revision: "http-target-v1".to_owned(),
        targeted_precondition: "http-precondition-v1".to_owned(),
        targeted_expiry: created_at + Duration::seconds(100),
        preview_time: created_at,
        preview_expiry: created_at + Duration::seconds(200),
    };
    let fingerprint_json = fingerprint
        .canonical_json()
        .expect("Activity fingerprint is canonical");
    let fingerprint_hash = fingerprint
        .content_hash()
        .expect("Activity fingerprint hashes");

    sqlx::query(
        "INSERT INTO sync_previews (
    id, workspace_id, policy_id, policy_revision, policy_content_hash,
    content_hash, schema_name, canonical_json, created_at, expires_at,
    fingerprint_hash, fingerprint_json, fresh_until
)
VALUES (
    $1, $2, $3, 1, $4, $5, 'trakkin.sync-preview/v1', $6, $7, $8,
    $9, $10, $11
)
",
    )
    .bind(preview_id)
    .bind(workspace_id)
    .bind(policy_id)
    .bind(policy_hash)
    .bind(&preview_hash)
    .bind(b"{}".as_slice())
    .bind(created_at)
    .bind(created_at + Duration::seconds(200))
    .bind(fingerprint_hash)
    .bind(fingerprint_json)
    .bind(created_at + Duration::seconds(100))
    .execute(pool)
    .await
    .expect("Activity preview is seeded");
    sqlx::query(
        "INSERT INTO sync_preview_actions (
    preview_id, workspace_id, preview_hash, ordinal, action_hash,
    receiver_source_id, receiver_subject_kind, receiver_subject_namespace,
    receiver_subject_value, semantic_field, segment_coordinate,
    segment_start, segment_end, canonical_json
)
VALUES (
    $1, $2, $3, 0, $4, $5, 'provider_item', 'fixture', $6,
    'progress', 'count', 0, 7, $7
)
",
    )
    .bind(preview_id)
    .bind(workspace_id)
    .bind(&preview_hash)
    .bind(&action_hash)
    .bind(SOURCE_ID)
    .bind(b"item.0".as_slice())
    .bind(&preview_action_json)
    .execute(pool)
    .await
    .expect("Activity preview action is seeded");
    sqlx::query(
        "INSERT INTO sync_runs (
    id, workspace_id, preview_id, preview_hash, created_at, status,
    started_at, finished_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, 'succeeded', $5, $6, $6)
",
    )
    .bind(run_id)
    .bind(workspace_id)
    .bind(preview_id)
    .bind(&preview_hash)
    .bind(created_at)
    .bind(if reconciled {
        reconciliation_settled_at
    } else {
        settled_at
    })
    .execute(pool)
    .await
    .expect("Activity run is seeded");
    sqlx::query(
        "INSERT INTO sync_actions (
    id, run_id, workspace_id, preview_id, preview_action_hash,
    receiver_source_id, created_at, status, started_at, finished_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'applied', $7, $8, $8)
",
    )
    .bind(action_id)
    .bind(run_id)
    .bind(workspace_id)
    .bind(preview_id)
    .bind(&action_hash)
    .bind(SOURCE_ID)
    .bind(created_at)
    .bind(if reconciled {
        reconciliation_settled_at
    } else {
        settled_at
    })
    .execute(pool)
    .await
    .expect("Activity action is seeded");
    seed_http_activity_task(pool, write_task_id, workspace_id, created_at, settled_at).await;
    sqlx::query(
        "INSERT INTO sync_action_tasks (task_id, action_id, receiver_source_id)
VALUES ($1, $2, $3)
",
    )
    .bind(write_task_id)
    .bind(action_id)
    .bind(SOURCE_ID)
    .execute(pool)
    .await
    .expect("Activity action task is linked");
    sqlx::query(
        "INSERT INTO task_attempts (
    id, task_id, ordinal, status, started_at, finished_at
)
VALUES ($1, $2, 0, 'succeeded', $3, $4)
",
    )
    .bind(write_attempt_id)
    .bind(write_task_id)
    .bind(created_at)
    .bind(settled_at)
    .execute(pool)
    .await
    .expect("Activity write task attempt is seeded");
    seed_http_sync_attempt(
        pool,
        write_attempt_id,
        write_task_id,
        action_id,
        owner_id,
        operation_id,
        &action_hash,
        created_at,
        settled_at,
        reconciled,
    )
    .await;

    if let (Some(reconciliation_task_id), Some(reconciliation_attempt_id)) =
        (reconciliation_task_id, reconciliation_attempt_id)
    {
        seed_http_activity_task(
            pool,
            reconciliation_task_id,
            workspace_id,
            settled_at,
            reconciliation_settled_at,
        )
        .await;
        sqlx::query(
            "INSERT INTO sync_action_reconciliation_tasks (
    task_id, action_id, receiver_source_id, write_task_attempt_id
)
VALUES ($1, $2, $3, $4)
",
        )
        .bind(reconciliation_task_id)
        .bind(action_id)
        .bind(SOURCE_ID)
        .bind(write_attempt_id)
        .execute(pool)
        .await
        .expect("Activity reconciliation task is linked");
        sqlx::query(
            "INSERT INTO task_attempts (
    id, task_id, ordinal, status, started_at, finished_at
)
VALUES ($1, $2, 0, 'succeeded', $3, $4)
",
        )
        .bind(reconciliation_attempt_id)
        .bind(reconciliation_task_id)
        .bind(settled_at)
        .bind(reconciliation_settled_at)
        .execute(pool)
        .await
        .expect("Activity reconciliation attempt is seeded");
        sqlx::query(
            "INSERT INTO sync_action_reconciliation_attempts (
    task_attempt_id, task_id, action_id, receiver_source_id,
    write_task_attempt_id, owner_id, operation_id, idempotency_key,
    source_key_namespace, source_key_value,
    receiver_subject_kind, receiver_subject_namespace, receiver_subject_value,
    provider_field_namespace, provider_field_name,
    intended_value, before_membership, before_presence, before_provider_revision,
    maximum_response_bytes, state, outcome_kind,
    observed_membership, observed_presence, observed_value,
    observed_provider_revision, observed_at, expires_at,
    causation_idempotency_key, causation_receipt, causation_provider_revision,
    result, prepared_at, settled_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $3,
    'fixture', $8, 'provider_item', 'fixture', $9,
    'trakkin.state', 'progress', 7, 'absent', 'absent', $10,
    1024, 'settled', 'matched', 'present', 'present', 7,
    $11, $12, $13, $3, $14, $11, 'confirmed_applied', $12, $15
)
",
        )
        .bind(reconciliation_attempt_id)
        .bind(reconciliation_task_id)
        .bind(action_id)
        .bind(SOURCE_ID)
        .bind(write_attempt_id)
        .bind(Uuid::from_u128(identity + 10))
        .bind(Uuid::from_u128(identity + 11))
        .bind(b"source".as_slice())
        .bind(b"item.0".as_slice())
        .bind(b"before".as_slice())
        .bind(b"observed".as_slice())
        .bind(settled_at)
        .bind(reconciliation_settled_at + Duration::seconds(100))
        .bind(b"reconciliation-receipt".as_slice())
        .bind(reconciliation_settled_at)
        .execute(pool)
        .await
        .expect("Activity reconciliation evidence is seeded");
        sqlx::query(
            "INSERT INTO sync_action_provenance (
    action_id, terminal_task_attempt_id, reconciliation_task_attempt_id, recorded_at
)
VALUES ($1, $2, $3, $4)
",
        )
        .bind(action_id)
        .bind(write_attempt_id)
        .bind(reconciliation_attempt_id)
        .bind(reconciliation_settled_at)
        .execute(pool)
        .await
        .expect("reconciled Activity provenance is seeded");
    } else {
        sqlx::query(
            "INSERT INTO sync_action_provenance (
    action_id, terminal_task_attempt_id, reconciliation_task_attempt_id, recorded_at
)
VALUES ($1, $2, NULL, $3)
",
        )
        .bind(action_id)
        .bind(write_attempt_id)
        .bind(settled_at)
        .execute(pool)
        .await
        .expect("direct Activity provenance is seeded");
    }

    SeededActivityRun {
        run_id,
        action_id,
        reconciliation_task_id,
    }
}

async fn seed_http_activity_task(
    pool: &sqlx::PgPool,
    task_id: Uuid,
    workspace_id: Uuid,
    started_at: OffsetDateTime,
    finished_at: OffsetDateTime,
) {
    sqlx::query(
        "INSERT INTO tasks (
    id, workspace_id, connection_id, kind, status,
    cancellation_supported, created_at, started_at, finished_at, updated_at
)
VALUES ($1, $2, $3, 'sync_action', 'succeeded', false, $4, $4, $5, $5)
",
    )
    .bind(task_id)
    .bind(workspace_id)
    .bind(CONNECTION_ID)
    .bind(started_at)
    .bind(finished_at)
    .execute(pool)
    .await
    .expect("Activity task is seeded");
}

#[allow(clippy::too_many_arguments)]
async fn seed_http_sync_attempt(
    pool: &sqlx::PgPool,
    task_attempt_id: Uuid,
    task_id: Uuid,
    action_id: Uuid,
    owner_id: Uuid,
    operation_id: Uuid,
    action_hash: &str,
    prepared_at: OffsetDateTime,
    settled_at: OffsetDateTime,
    reconciled: bool,
) {
    let capability = json!({
        "fields": [{
            "field": {"namespace": "trakkin.state", "name": "progress"},
            "unit": null,
            "setSupported": true,
            "clearSupported": false
        }],
        "mayCreateSourceMembership": false,
        "preconditionMode": "provider_token",
        "idempotencyMode": "stable_key",
        "maximumFields": 1,
        "maximumRequestBytes": 1024,
        "maximumResponseBytes": 1024,
        "maximumReceiptBytes": 1024
    });
    sqlx::query(
        "INSERT INTO sync_action_attempts (
    task_attempt_id, task_id, action_id, receiver_source_id,
    owner_id, operation_id, idempotency_key, preview_action_hash,
    receiver_subject_kind, receiver_subject_namespace, receiver_subject_value,
    semantic_field, provider_field_namespace, provider_field_name,
    intended_value, expected_membership, allow_create_membership,
    source_key_namespace, source_key_value, write_capability,
    state, recovery_result, settlement_kind,
    outcome_status, outcome_certainty, outcome_retry_disposition,
    outcome_membership_effect, outcome_provider_revision, outcome_receipt,
    prepared_at, dispatched_at, settled_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $3, $7,
    'provider_item', 'fixture', $8, 'progress', 'trakkin.state', 'progress',
    7, 'present', false, 'fixture', $9, $10,
    'settled', $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $20
)
",
    )
    .bind(task_attempt_id)
    .bind(task_id)
    .bind(action_id)
    .bind(SOURCE_ID)
    .bind(owner_id)
    .bind(operation_id)
    .bind(action_hash)
    .bind(b"item.0".as_slice())
    .bind(b"source".as_slice())
    .bind(capability)
    .bind(reconciled.then_some("indeterminate"))
    .bind(if reconciled {
        "recovery"
    } else {
        "provider_outcome"
    })
    .bind((!reconciled).then_some("applied"))
    .bind((!reconciled).then_some("confirmed_applied"))
    .bind((!reconciled).then_some("not_retryable"))
    .bind((!reconciled).then_some("unchanged"))
    .bind((!reconciled).then_some(b"provider-revision".as_slice()))
    .bind((!reconciled).then_some(b"provider-receipt".as_slice()))
    .bind(prepared_at)
    .bind(settled_at)
    .execute(pool)
    .await
    .expect("Activity write attempt is seeded");
}
