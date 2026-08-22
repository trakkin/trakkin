use std::{sync::Arc, time::Duration};

use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::{
        acquisition::ReadMode,
        adapter_runtime::{
            AdapterAuthenticationStatus, AdapterLookupOutcome, AdapterSecret, AdapterSetting,
            AdapterValue,
        },
        assets::{
            BinaryAssetInteractionService, BinaryAssetService, BinaryAssetServiceError,
            ReadPublishedBinaryAsset,
        },
        connection::{
            BeginConnectionDeletion, CancelConnectionAuthentication, ConnectionCoordinator,
            ConnectionCoordinatorError, ConnectionRepository, ContinueConnectionAuthentication,
            CreateConnection, InspectConnection, OpenConnection, SourceDiscoveryTaskHandler,
            StartConnectionAuthentication, StopConnection,
        },
        facade::{
            ApplicationApi, ApplicationFacade, ApplicationInteractions,
            ApplicationMappingRepositories,
        },
        mapping_lifecycle::{
            ChangeMappingActivation, CreateMapping, MappingActivationKind, MappingLifecycleService,
        },
        mapping_resolution::{
            AdapterMappingEndpointLookupService, CoordinateBacking, MappingResolutionService,
            ResolutionCompleteness, ResolutionFindingKind, ResolutionMode, ResolutionRequest,
        },
        reference_lookup::{
            LookupPortableReferences, ReferenceLookupInteractionService, ReferenceLookupService,
        },
        sync_activity::SyncActivityService,
        sync_policy::SyncPolicyService,
        task_dispatcher::{DispatchNextTask, TaskDispatcher},
        task_manager::{
            AdmitTask, CancellationDisposition, RequestTaskCancellation, SourceStream,
            TaskDefinition, TaskManager, TaskStatus,
        },
        workspaces::{AttachWorkspaceSource, CreateWorkspace, WorkspaceService},
    },
    database::DatabaseRuntime,
    domain::acquisition::{BinaryAssetKey, PortableReference, ProviderItemKey},
    infrastructure::{
        acquisition_dispatch::AcquisitionTaskHandler,
        adapter_process::{AdapterProcess, AdapterProcessError, AdapterProcessLauncher},
        catalog_publication::PostgresPublicationRepository,
        catalog_queries::PostgresCatalogQueryRepository,
        connection::{ConnectionMasterKey, PostgresConnectionRepository, ProcessSessionRegistry},
        mapping_lifecycle::PostgresMappingLifecycleRepository,
        mapping_queries::PostgresMappingQueryRepository,
        mapping_resolution::PostgresMappingResolutionRepository,
        provider_access::{PostgresProviderAccessRepository, ProviderAccessError},
        sync_activity::PostgresSyncActivityRepository,
        sync_policy::PostgresSyncPolicyRepository,
        task_manager::PostgresTaskRepository,
        workspaces::PostgresWorkspaceRepository,
    },
};
use trakkin_mapping_contract::PortableEndpoint;
use uuid::Uuid;

const CONNECTION_ID: Uuid = Uuid::from_u128(1);

#[tokio::test(flavor = "multi_thread")]
async fn stored_fixture_connection_publishes_discovery_and_retires_isolated_session() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let repository =
        PostgresConnectionRepository::new(database.pool(), ConnectionMasterKey::new([7_u8; 32]));
    repository
        .store_connection(CreateConnection {
            connection_id: CONNECTION_ID,
            adapter_key: "fixture".to_owned(),
            display_name: "Fixture connection".to_owned(),
            settings: fixture_settings(),
            secrets: vec![AdapterSecret {
                key: "fixture_token".to_owned(),
                value: b"fixture-secret".to_vec(),
            }],
            created_at: timestamp(0),
        })
        .await
        .expect("fixture connection is stored");

    let stored = repository
        .load_connection(CONNECTION_ID)
        .await
        .expect("stored connection loads")
        .expect("stored connection exists");
    let mut expected_settings = fixture_settings();
    expected_settings.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(stored.settings, expected_settings);
    assert_eq!(stored.secrets[0].value, b"fixture-secret");
    assert!(!format!("{stored:?}").contains("fixture-secret"));
    let (nonce, ciphertext) =
        sqlx::query_as::<_, (Vec<u8>, Vec<u8>)>("SELECT nonce, ciphertext FROM encrypted_secrets")
            .fetch_one(&database.pool())
            .await
            .expect("encrypted secret row exists");
    assert_eq!(nonce.len(), 24);
    assert_ne!(ciphertext, b"fixture-secret");

    let sessions = ProcessSessionRegistry::<AdapterProcess>::default();
    let coordinator = Arc::new(ConnectionCoordinator::new(
        repository.clone(),
        AdapterProcessLauncher::new(
            env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
            "stored-connection-flow-test",
        ),
        sessions.clone(),
    ));
    let inspection = coordinator
        .inspect(InspectConnection {
            connection_id: CONNECTION_ID,
            process_instance_id: "inspect-stored-fixture".to_owned(),
        })
        .await
        .expect("stored fixture connection is inspected");
    assert!(
        inspection
            .description
            .fields
            .iter()
            .any(|field| field.key == "scenario")
    );
    assert!(inspection.validation.field_problems.is_empty());
    assert!(
        inspection
            .authentication_methods
            .iter()
            .any(|method| method.key == "fixture/device")
    );
    assert!(sessions.is_empty());

    let device_authentication_id = Uuid::from_u128(2);
    let input = coordinator
        .start_authentication(StartConnectionAuthentication {
            authentication_id: device_authentication_id,
            connection_id: CONNECTION_ID,
            process_instance_id: "authenticate-stored-fixture".to_owned(),
            method: "fixture/device".to_owned(),
            values: Vec::new(),
            expires_at: timestamp(100),
            started_at: timestamp(1),
        })
        .await
        .expect("stored fixture authentication starts");
    assert_eq!(
        input.step.status,
        AdapterAuthenticationStatus::InputRequired
    );
    assert!(sessions.contains(CONNECTION_ID));
    let concealed_continuation = coordinator
        .continue_authentication(ContinueConnectionAuthentication {
            authentication_id: device_authentication_id,
            connection_id: Uuid::from_u128(999),
            values: Vec::new(),
            expires_at: timestamp(100),
            continued_at: timestamp(2),
        })
        .await
        .expect_err("authentication is concealed from another connection");
    assert!(matches!(
        concealed_continuation,
        ConnectionCoordinatorError::AuthenticationNotFound
    ));
    assert!(sessions.contains(CONNECTION_ID));
    let completed = coordinator
        .continue_authentication(ContinueConnectionAuthentication {
            authentication_id: device_authentication_id,
            connection_id: CONNECTION_ID,
            values: vec![AdapterSetting {
                key: "code".to_owned(),
                value: AdapterValue::Text("approved".to_owned()),
            }],
            expires_at: timestamp(100),
            continued_at: timestamp(2),
        })
        .await
        .expect("stored fixture authentication completes");
    assert_eq!(completed.status, AdapterAuthenticationStatus::Completed);
    assert!(sessions.is_empty());

    let cancelled_authentication_id = Uuid::from_u128(3);
    let waiting = coordinator
        .start_authentication(StartConnectionAuthentication {
            authentication_id: cancelled_authentication_id,
            connection_id: CONNECTION_ID,
            process_instance_id: "cancel-stored-fixture-authentication".to_owned(),
            method: "fixture/cancel".to_owned(),
            values: Vec::new(),
            expires_at: timestamp(100),
            started_at: timestamp(3),
        })
        .await
        .expect("cancellable stored fixture authentication starts");
    assert_eq!(waiting.step.status, AdapterAuthenticationStatus::Waiting);
    let concealed_cancellation = coordinator
        .cancel_authentication(CancelConnectionAuthentication {
            authentication_id: cancelled_authentication_id,
            connection_id: Uuid::from_u128(999),
            cancelled_at: timestamp(4),
        })
        .await
        .expect_err("authentication cancellation is concealed from another connection");
    assert!(matches!(
        concealed_cancellation,
        ConnectionCoordinatorError::AuthenticationNotFound
    ));
    assert!(sessions.contains(CONNECTION_ID));
    coordinator
        .cancel_authentication(CancelConnectionAuthentication {
            authentication_id: cancelled_authentication_id,
            connection_id: CONNECTION_ID,
            cancelled_at: timestamp(4),
        })
        .await
        .expect("stored fixture authentication cancels");
    assert!(sessions.is_empty());

    let authentication_rows = sqlx::query_as::<_, (Uuid, String, String)>(
        "SELECT id, status, step::TEXT FROM authentication_sessions ORDER BY id",
    )
    .fetch_all(&database.pool())
    .await
    .expect("authentication sessions are persisted");
    assert_eq!(authentication_rows.len(), 2);
    assert_eq!(authentication_rows[0].1, "completed");
    assert_eq!(authentication_rows[1].1, "cancelled");
    assert!(
        authentication_rows
            .iter()
            .all(|(_, _, step)| !step.contains("fixture-secret"))
    );

    let discovery_task_id = Uuid::from_u128(4);
    let task_manager = TaskManager::new(PostgresTaskRepository::new(database.pool()));
    task_manager
        .admit(AdmitTask {
            task_id: discovery_task_id,
            workspace_id: None,
            definition: TaskDefinition::SourceDiscovery {
                connection_id: CONNECTION_ID,
            },
            cancellation_supported: false,
            admitted_at: timestamp(5),
        })
        .await
        .expect("fixture discovery task is admitted");
    let dispatcher = TaskDispatcher::new(
        task_manager,
        SourceDiscoveryTaskHandler::new(ConnectionCoordinator::new(
            repository.clone(),
            AdapterProcessLauncher::new(
                env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
                "stored-connection-dispatch-test",
            ),
            sessions.clone(),
        )),
    );
    let dispatched = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(5),
            attempt_id: Uuid::from_u128(6),
            claimed_at: timestamp(6),
            completed_at: timestamp(7),
        })
        .await
        .expect("fixture discovery task dispatches")
        .expect("fixture discovery task is available");
    assert_eq!(dispatched.id, discovery_task_id);
    assert_eq!(dispatched.status, TaskStatus::Succeeded);
    assert!(sessions.contains(CONNECTION_ID));
    assert_eq!(sessions.len(), 1);

    let (status, capability_revision) = sqlx::query_as::<_, (String, i64)>(
        "SELECT status, capability_revision FROM connections WHERE id = $1",
    )
    .bind(CONNECTION_ID)
    .fetch_one(&database.pool())
    .await
    .expect("published connection exists");
    assert_eq!(status, "ready");
    assert_eq!(capability_revision, 1);
    assert_eq!(table_count(&database.pool(), "accounts").await, 1);
    let source_count = table_count(&database.pool(), "sources").await;
    assert!(source_count > 0);
    assert_eq!(
        table_count(&database.pool(), "source_streams").await,
        source_count * 2
    );

    let application = ApplicationFacade::new(
        PostgresWorkspaceRepository::new(database.pool()),
        PostgresCatalogQueryRepository::new(database.pool()),
        ApplicationMappingRepositories::new(
            PostgresMappingQueryRepository::new(database.pool()),
            PostgresMappingLifecycleRepository::new(database.pool()),
        ),
        PostgresTaskRepository::new(database.pool()),
        repository,
        sessions.clone(),
        ApplicationInteractions::new(
            coordinator,
            Arc::new(BinaryAssetInteractionService::<_, _, AdapterProcess>::new(
                PostgresProviderAccessRepository::new(database.pool()),
                sessions.clone(),
                1024,
            )),
            Arc::new(
                ReferenceLookupInteractionService::<_, _, AdapterProcess>::new(
                    PostgresProviderAccessRepository::new(database.pool()),
                    sessions.clone(),
                ),
            ),
            Arc::new(MappingResolutionService::with_endpoint_lookup(
                PostgresMappingResolutionRepository::new(database.pool()),
                AdapterMappingEndpointLookupService::<_, _, AdapterProcess>::new(
                    PostgresProviderAccessRepository::new(database.pool()),
                    sessions.clone(),
                ),
            )),
            Arc::new(SyncPolicyService::new(PostgresSyncPolicyRepository::new(
                database.pool(),
            ))),
            Arc::new(SyncActivityService::new(
                PostgresSyncActivityRepository::new(database.pool()),
            )),
        ),
    );
    let impact = application
        .connection_deletion_impact(CONNECTION_ID)
        .await
        .expect("stored connection deletion impact is inspected");
    assert_eq!(impact.source_count, source_count);
    assert_eq!(impact.task_count, 1);
    assert_eq!(impact.active_task_count, 0);
    assert_eq!(impact.authentication_session_count, 2);
    assert_eq!(
        application
            .delete_connection(BeginConnectionDeletion {
                connection_id: CONNECTION_ID,
                disabled_at: timestamp(8),
            })
            .await
            .expect("stored fixture connection is deleted"),
        impact
    );
    assert!(sessions.is_empty());
    assert_eq!(table_count(&database.pool(), "connections").await, 0);
    assert_eq!(table_count(&database.pool(), "encrypted_secrets").await, 0);

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_stored_secret_leaves_no_session_or_partial_discovery() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let repository =
        PostgresConnectionRepository::new(database.pool(), ConnectionMasterKey::new([9_u8; 32]));
    repository
        .store_connection(CreateConnection {
            connection_id: CONNECTION_ID,
            adapter_key: "fixture".to_owned(),
            display_name: "Rejected fixture connection".to_owned(),
            settings: fixture_settings(),
            secrets: vec![AdapterSecret {
                key: "fixture_token".to_owned(),
                value: b"must-not-leak".to_vec(),
            }],
            created_at: timestamp(0),
        })
        .await
        .expect("rejected fixture connection is stored");
    let sessions = ProcessSessionRegistry::<AdapterProcess>::default();
    let coordinator = ConnectionCoordinator::new(
        repository,
        AdapterProcessLauncher::new(
            env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
            "stored-connection-rejection-test",
        ),
        sessions.clone(),
    );

    let error = coordinator
        .open(OpenConnection {
            connection_id: CONNECTION_ID,
            process_instance_id: "rejected-stored-fixture".to_owned(),
            opened_at: timestamp(1),
        })
        .await
        .expect_err("invalid stored secret is rejected");
    assert!(matches!(
        error,
        ConnectionCoordinatorError::Adapter(
            trakkin::infrastructure::adapter_process::AdapterProcessError::Adapter(ref error)
        ) if error.code == "invalid_connection"
    ));
    assert!(!format!("{error:?}").contains("must-not-leak"));
    assert!(sessions.is_empty());
    assert_eq!(table_count(&database.pool(), "accounts").await, 0);
    assert_eq!(table_count(&database.pool(), "sources").await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM connections WHERE id = $1")
            .bind(CONNECTION_ID)
            .fetch_one(&database.pool())
            .await
            .expect("rejected connection exists"),
        "unavailable"
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test(flavor = "multi_thread")]
async fn task_driven_fixture_reads_cancel_reset_and_refetch_atomically() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let repository =
        PostgresConnectionRepository::new(database.pool(), ConnectionMasterKey::new([11_u8; 32]));
    store_fixture_connection(&repository, "none", timestamp(0)).await;
    let sessions = ProcessSessionRegistry::<AdapterProcess>::default();
    let task_manager = TaskManager::new(PostgresTaskRepository::new(database.pool()));
    let discovery_dispatcher = TaskDispatcher::new(
        task_manager.clone(),
        SourceDiscoveryTaskHandler::new(ConnectionCoordinator::new(
            repository.clone(),
            AdapterProcessLauncher::new(
                env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
                "fixture-read-discovery-test",
            ),
            sessions.clone(),
        )),
    );
    admit_discovery(&task_manager, Uuid::from_u128(100), timestamp(1)).await;
    let discovery = discovery_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(101), Uuid::from_u128(102), 2, 3))
        .await
        .expect("initial discovery dispatches")
        .expect("initial discovery task is available");
    assert_eq!(discovery.status, TaskStatus::Succeeded);
    let source_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM sources WHERE connection_id = $1 ORDER BY id LIMIT 1",
    )
    .bind(CONNECTION_ID)
    .fetch_one(&database.pool())
    .await
    .expect("fixture source is published");

    let acquisition_dispatcher = TaskDispatcher::new(
        task_manager.clone(),
        AcquisitionTaskHandler::new(
            PostgresPublicationRepository::new(database.pool()),
            task_manager.clone(),
            sessions.clone(),
        )
        .with_cancellation_timing(Duration::from_millis(10), Duration::from_secs(1)),
    );
    admit_read(
        &task_manager,
        Uuid::from_u128(110),
        source_id,
        SourceStream::Catalog,
        false,
        timestamp(4),
    )
    .await;
    let catalog = acquisition_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(111), Uuid::from_u128(112), 5, 6))
        .await
        .expect("catalog read dispatches")
        .expect("catalog read task is available");
    assert_eq!(catalog.status, TaskStatus::Succeeded);
    let catalog_before_cancellation = catalog_snapshot(&database.pool(), source_id).await;
    assert_eq!(catalog_before_cancellation.0, "available");
    assert!(catalog_before_cancellation.1.is_some());
    assert!(catalog_before_cancellation.2.is_some());
    assert!(catalog_before_cancellation.3 > 0);

    admit_read(
        &task_manager,
        Uuid::from_u128(120),
        source_id,
        SourceStream::State,
        false,
        timestamp(7),
    )
    .await;
    let state = acquisition_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(121), Uuid::from_u128(122), 8, 9))
        .await
        .expect("state read dispatches")
        .expect("state read task is available");
    assert_eq!(state.status, TaskStatus::Succeeded);
    assert!(table_count(&database.pool(), "state_observations").await > 0);

    store_fixture_connection(&repository, "read.hang-after-heartbeat", timestamp(10)).await;
    admit_discovery(&task_manager, Uuid::from_u128(130), timestamp(11)).await;
    discovery_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(131), Uuid::from_u128(132), 12, 13))
        .await
        .expect("hanging fixture discovery dispatches")
        .expect("hanging fixture discovery task is available");
    let cancelled_task_id = Uuid::from_u128(140);
    admit_read(
        &task_manager,
        cancelled_task_id,
        source_id,
        SourceStream::Catalog,
        true,
        timestamp(14),
    )
    .await;
    let cancellation = async {
        loop {
            let task = task_manager
                .get(cancelled_task_id)
                .await
                .expect("cancellable read task is queried")
                .expect("cancellable read task exists");
            if task.status == TaskStatus::Running {
                break;
            }
            tokio::task::yield_now().await;
        }
        task_manager
            .request_cancellation(RequestTaskCancellation {
                task_id: cancelled_task_id,
                requested_at: timestamp(16),
            })
            .await
            .expect("hanging read cancellation is requested")
    };
    let dispatch_cancelled = acquisition_dispatcher.dispatch_next(dispatch(
        Uuid::from_u128(141),
        Uuid::from_u128(142),
        15,
        17,
    ));
    let (cancelled, cancellation_disposition) = tokio::join!(dispatch_cancelled, cancellation);
    assert_eq!(
        cancellation_disposition,
        CancellationDisposition::CancellationRequested
    );
    assert_eq!(
        cancelled
            .expect("cancelled read dispatch completes")
            .expect("cancelled read task is available")
            .status,
        TaskStatus::Cancelled
    );
    assert_eq!(
        catalog_snapshot(&database.pool(), source_id).await,
        catalog_before_cancellation
    );

    store_fixture_connection(&repository, "read.cancel-after-batch", timestamp(18)).await;
    admit_discovery(&task_manager, Uuid::from_u128(145), timestamp(19)).await;
    discovery_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(146), Uuid::from_u128(147), 20, 21))
        .await
        .expect("cancelling fixture discovery dispatches")
        .expect("cancelling fixture discovery task is available");
    admit_read(
        &task_manager,
        Uuid::from_u128(148),
        source_id,
        SourceStream::Catalog,
        false,
        timestamp(22),
    )
    .await;
    let adapter_cancelled = acquisition_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(149), Uuid::from_u128(150), 23, 24))
        .await
        .expect("adapter-cancelled read dispatches")
        .expect("adapter-cancelled read task is available");
    assert_eq!(adapter_cancelled.status, TaskStatus::Cancelled);
    assert_eq!(
        catalog_snapshot(&database.pool(), source_id).await,
        catalog_before_cancellation
    );

    let reset_task_id = Uuid::from_u128(151);
    task_manager
        .admit(AdmitTask {
            task_id: reset_task_id,
            workspace_id: None,
            definition: TaskDefinition::Reset {
                source_id,
                stream: SourceStream::Catalog,
            },
            cancellation_supported: false,
            admitted_at: timestamp(18),
        })
        .await
        .expect("catalog reset is admitted");
    let reset = acquisition_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(151), Uuid::from_u128(152), 19, 20))
        .await
        .expect("catalog reset dispatches")
        .expect("catalog reset task is available");
    assert_eq!(reset.status, TaskStatus::Succeeded);
    assert_eq!(
        catalog_snapshot(&database.pool(), source_id).await,
        ("unavailable".to_owned(), None, None, 0)
    );

    store_fixture_connection(&repository, "none", timestamp(21)).await;
    admit_discovery(&task_manager, Uuid::from_u128(160), timestamp(22)).await;
    discovery_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(161), Uuid::from_u128(162), 23, 24))
        .await
        .expect("refetch discovery dispatches")
        .expect("refetch discovery task is available");
    admit_read(
        &task_manager,
        Uuid::from_u128(170),
        source_id,
        SourceStream::Catalog,
        false,
        timestamp(25),
    )
    .await;
    let refetch = acquisition_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(171), Uuid::from_u128(172), 26, 27))
        .await
        .expect("catalog refetch dispatches")
        .expect("catalog refetch task is available");
    assert_eq!(refetch.status, TaskStatus::Succeeded);
    assert!(catalog_snapshot(&database.pool(), source_id).await.3 > 0);

    let coordinator = ConnectionCoordinator::new(
        repository,
        AdapterProcessLauncher::new(
            env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
            "fixture-read-stop-test",
        ),
        sessions.clone(),
    );
    coordinator
        .stop(StopConnection {
            connection_id: CONNECTION_ID,
            stopped_at: timestamp(28),
        })
        .await
        .expect("fixture read session stops");
    assert!(sessions.is_empty());
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test(flavor = "multi_thread")]
async fn stored_provider_access_bounds_assets_and_keeps_lookup_transient() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let repository =
        PostgresConnectionRepository::new(database.pool(), ConnectionMasterKey::new([13_u8; 32]));
    store_scenario_connection(
        &repository,
        "hierarchical-catalog-assets-v1",
        "none",
        timestamp(0),
    )
    .await;
    let sessions = ProcessSessionRegistry::<AdapterProcess>::default();
    let coordinator = ConnectionCoordinator::new(
        repository.clone(),
        AdapterProcessLauncher::new(
            env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
            "stored-provider-access-test",
        ),
        sessions.clone(),
    );
    coordinator
        .open(OpenConnection {
            connection_id: CONNECTION_ID,
            process_instance_id: "provider-access-assets".to_owned(),
            opened_at: timestamp(1),
        })
        .await
        .expect("asset fixture opens");
    let asset_source_id = source_id(&database.pool(), b"source.assets.catalog").await;
    let task_manager = TaskManager::new(PostgresTaskRepository::new(database.pool()));
    admit_read(
        &task_manager,
        Uuid::from_u128(200),
        asset_source_id,
        SourceStream::Catalog,
        false,
        timestamp(2),
    )
    .await;
    let acquisition_dispatcher = TaskDispatcher::new(
        task_manager,
        AcquisitionTaskHandler::new(
            PostgresPublicationRepository::new(database.pool()),
            TaskManager::new(PostgresTaskRepository::new(database.pool())),
            sessions.clone(),
        ),
    );
    let catalog = acquisition_dispatcher
        .dispatch_next(dispatch(Uuid::from_u128(201), Uuid::from_u128(202), 3, 4))
        .await
        .expect("asset catalog read dispatches")
        .expect("asset catalog read is available");
    assert_eq!(catalog.status, TaskStatus::Succeeded);
    let publication_id = catalog_snapshot(&database.pool(), asset_source_id)
        .await
        .2
        .expect("asset catalog is published");

    let workspace_id = Uuid::from_u128(203);
    let workspaces = WorkspaceService::new(PostgresWorkspaceRepository::new(database.pool()));
    workspaces
        .create(CreateWorkspace {
            workspace_id,
            name: "Provider access".to_owned(),
            created_at: timestamp(5),
        })
        .await
        .expect("provider access workspace is created");
    workspaces
        .attach(AttachWorkspaceSource {
            workspace_id,
            source_id: asset_source_id,
            attached_at: timestamp(6),
        })
        .await
        .expect("asset source is attached");

    let provider_access = PostgresProviderAccessRepository::new(database.pool());
    let asset_command = ReadPublishedBinaryAsset {
        operation_id: b"stored-asset-read".to_vec(),
        workspace_id,
        source_id: asset_source_id,
        publication_id,
        provider_item_key: ProviderItemKey::new("fixture", b"media.asset.primary".to_vec())
            .expect("asset provider item key is valid"),
        asset_key: BinaryAssetKey::new("fixture", b"asset.poster.valid".to_vec())
            .expect("asset key is valid"),
    };
    let assets = BinaryAssetService::new(provider_access.clone(), sessions.clone(), 1024);
    let asset = assets
        .read(asset_command.clone())
        .await
        .expect("published fixture asset is read");
    assert_eq!(asset.content_type, "image/jpeg");
    assert_eq!(asset.full_length, asset.content.len() as u64);
    assert_eq!(asset.sha256_digest.len(), 32);
    assert_eq!(asset.cache_control, "private, max-age=300");
    assert!(sessions.contains(CONNECTION_ID));

    let bounded = BinaryAssetService::new(provider_access.clone(), sessions.clone(), 8);
    assert!(matches!(
        bounded.read(asset_command.clone()).await,
        Err(BinaryAssetServiceError::Rejected(ref error))
            if error.code == "asset_too_large"
    ));
    assert!(sessions.contains(CONNECTION_ID));

    sqlx::query(
        "UPDATE sources
SET capabilities = jsonb_set(capabilities, '{assets,contentTypes}', '[\"image/png\"]'::JSONB)
WHERE id = $1
",
    )
    .bind(asset_source_id)
    .execute(&database.pool())
    .await
    .expect("advertised asset content type is changed");
    assert!(matches!(
        assets.read(asset_command.clone()).await,
        Err(BinaryAssetServiceError::Adapter(
            AdapterProcessError::InvalidResponse("invalid asset response")
        ))
    ));
    assert!(sessions.is_empty());
    assert!(matches!(
        assets
            .read(ReadPublishedBinaryAsset {
                publication_id: Uuid::from_u128(204),
                ..asset_command
            })
            .await,
        Err(BinaryAssetServiceError::Repository(
            ProviderAccessError::PublicationExpired
        ))
    ));

    store_scenario_connection(
        &repository,
        "empty-targeted-receiver-v1",
        "none",
        timestamp(7),
    )
    .await;
    coordinator
        .open(OpenConnection {
            connection_id: CONNECTION_ID,
            process_instance_id: "provider-access-empty-receiver".to_owned(),
            opened_at: timestamp(8),
        })
        .await
        .expect("empty-receiver fixture opens");
    let receiver_source_id = source_id(&database.pool(), b"source.receiver.empty").await;
    workspaces
        .attach(AttachWorkspaceSource {
            workspace_id,
            source_id: receiver_source_id,
            attached_at: timestamp(9),
        })
        .await
        .expect("empty receiver is attached");
    assert_eq!(
        catalog_item_count(&database.pool(), receiver_source_id).await,
        0
    );
    let lookup = ReferenceLookupService::new(provider_access.clone(), sessions.clone());
    let matched = lookup
        .lookup(LookupPortableReferences {
            operation_id: b"stored-empty-receiver-lookup".to_vec(),
            workspace_id,
            source_id: receiver_source_id,
            references: vec![portable_reference("media/quiet-signal")],
        })
        .await
        .expect("empty receiver lookup succeeds");
    assert!(matches!(
        matched[0].outcome,
        AdapterLookupOutcome::Matched(_)
    ));
    assert_eq!(
        catalog_item_count(&database.pool(), receiver_source_id).await,
        0
    );
    let selected_endpoint = PortableEndpoint::parse("example.media:quiet-signal[segment:1..12]")
        .expect("selected endpoint is valid");
    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(database.pool()));
    let mapping_id = Uuid::from_u128(205);
    let mapping = lifecycle
        .create(CreateMapping {
            mapping_id,
            mappings: vec![vec![
                selected_endpoint.to_string(),
                "example.media:receiver[segment:1..12]".to_owned(),
            ]],
            created_at: timestamp(10),
        })
        .await
        .expect("empty-receiver mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: Uuid::from_u128(206),
            workspace_id,
            mapping_id,
            content_hash: Some(mapping.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: timestamp(11),
        })
        .await
        .expect("empty-receiver mapping is activated");
    let resolver = MappingResolutionService::with_endpoint_lookup(
        PostgresMappingResolutionRepository::new(database.pool()),
        AdapterMappingEndpointLookupService::<_, _, AdapterProcess>::new(
            provider_access.clone(),
            sessions.clone(),
        ),
    );
    let resolution = resolver
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: vec![selected_endpoint.clone()],
            resolved_at: timestamp(11),
        })
        .await
        .expect("empty-membership endpoint resolves through live lookup");
    assert_eq!(resolution.completeness, ResolutionCompleteness::Complete);
    assert_eq!(resolution.lookup_calls, 2);
    assert_eq!(resolution.endpoint_lookup_matches.len(), 1);
    assert_eq!(
        resolution.endpoint_lookup_matches[0].endpoint,
        selected_endpoint
    );
    assert_eq!(
        resolution.endpoint_lookup_matches[0].backing,
        Some(CoordinateBacking::Aggregate)
    );
    assert_eq!(
        resolution.endpoint_lookup_matches[0].process_instance_id,
        "provider-access-empty-receiver"
    );
    assert!(
        !resolution.endpoint_lookup_matches[0]
            .lookup_evidence_revision
            .is_empty()
    );
    assert!(resolution.findings.is_empty());
    assert_eq!(
        catalog_item_count(&database.pool(), receiver_source_id).await,
        0
    );
    assert_eq!(
        coordinate_binding_count(&database.pool(), receiver_source_id).await,
        0
    );

    store_scenario_connection(&repository, "adversarial-v1", "none", timestamp(12)).await;
    coordinator
        .open(OpenConnection {
            connection_id: CONNECTION_ID,
            process_instance_id: "provider-access-adversarial".to_owned(),
            opened_at: timestamp(13),
        })
        .await
        .expect("adversarial fixture opens");
    let lookup_source_id = source_id(&database.pool(), b"source.adversarial.lookup").await;
    workspaces
        .attach(AttachWorkspaceSource {
            workspace_id,
            source_id: lookup_source_id,
            attached_at: timestamp(14),
        })
        .await
        .expect("adversarial lookup source is attached");
    let outcomes = lookup
        .lookup(LookupPortableReferences {
            operation_id: b"stored-adversarial-lookup".to_vec(),
            workspace_id,
            source_id: lookup_source_id,
            references: vec![
                portable_reference("missing"),
                portable_reference("duplicate"),
                portable_reference("other"),
            ],
        })
        .await
        .expect("adversarial lookup succeeds");
    assert!(matches!(
        outcomes[0].outcome,
        AdapterLookupOutcome::NotFound
    ));
    assert!(matches!(
        outcomes[1].outcome,
        AdapterLookupOutcome::Ambiguous(ref candidates) if candidates.len() == 2
    ));
    assert!(matches!(
        outcomes[2].outcome,
        AdapterLookupOutcome::Unsupported
    ));
    let ambiguous_mapping_id = Uuid::from_u128(207);
    let ambiguous_mapping = lifecycle
        .create(CreateMapping {
            mapping_id: ambiguous_mapping_id,
            mappings: vec![
                vec![
                    "example.media:start".to_owned(),
                    "example.media:duplicate".to_owned(),
                ],
                vec![
                    "example.media:duplicate".to_owned(),
                    "example.media:end".to_owned(),
                ],
            ],
            created_at: timestamp(14),
        })
        .await
        .expect("ambiguous traversal mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: Uuid::from_u128(208),
            workspace_id,
            mapping_id: ambiguous_mapping_id,
            content_hash: Some(ambiguous_mapping.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: timestamp(14),
        })
        .await
        .expect("ambiguous traversal mapping is activated");
    let ambiguous_resolution = resolver
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Transitive,
            window: None,
            references: vec![
                PortableEndpoint::parse("example.media:start")
                    .expect("transitive root endpoint is valid"),
            ],
            resolved_at: timestamp(14),
        })
        .await
        .expect("ambiguous transitive lookup resolves partially");
    assert_eq!(ambiguous_resolution.lookup_calls, 2);
    assert_eq!(
        ambiguous_resolution.completeness,
        ResolutionCompleteness::Partial
    );
    assert_eq!(ambiguous_resolution.findings.len(), 1);
    assert_eq!(
        ambiguous_resolution.findings[0].kind,
        ResolutionFindingKind::Ambiguous
    );
    assert_eq!(
        ambiguous_resolution.findings[0].endpoint,
        PortableEndpoint::parse("example.media:duplicate").expect("ambiguous endpoint is valid")
    );
    assert!(
        ambiguous_resolution
            .correspondences
            .iter()
            .all(|correspondence| {
                !correspondence.endpoints.contains(
                    &PortableEndpoint::parse("example.media:end")
                        .expect("untraversed endpoint is valid"),
                )
            })
    );
    assert_eq!(
        catalog_item_count(&database.pool(), lookup_source_id).await,
        0
    );
    assert!(sessions.contains(CONNECTION_ID));

    coordinator
        .stop(StopConnection {
            connection_id: CONNECTION_ID,
            stopped_at: timestamp(15),
        })
        .await
        .expect("provider access session stops");
    assert!(sessions.is_empty());
    database.shutdown().await.expect("database shuts down");
}

fn fixture_settings() -> Vec<AdapterSetting> {
    fixture_settings_with_fault("none")
}

fn fixture_settings_with_fault(fault: &str) -> Vec<AdapterSetting> {
    [
        ("scenario", "hierarchical-catalog-assets-v1"),
        ("revision", "v1"),
        ("fault", fault),
        ("instance", "alpha"),
    ]
    .into_iter()
    .map(|(key, value)| AdapterSetting {
        key: key.to_owned(),
        value: AdapterValue::Text(value.to_owned()),
    })
    .collect()
}

async fn store_fixture_connection(
    repository: &PostgresConnectionRepository,
    fault: &str,
    created_at: OffsetDateTime,
) {
    store_scenario_connection(
        repository,
        "hierarchical-catalog-assets-v1",
        fault,
        created_at,
    )
    .await;
}

async fn store_scenario_connection(
    repository: &PostgresConnectionRepository,
    scenario: &str,
    fault: &str,
    created_at: OffsetDateTime,
) {
    repository
        .store_connection(CreateConnection {
            connection_id: CONNECTION_ID,
            adapter_key: "fixture".to_owned(),
            display_name: "Fixture read connection".to_owned(),
            settings: [
                ("scenario", scenario),
                ("revision", "v1"),
                ("fault", fault),
                ("instance", "alpha"),
            ]
            .into_iter()
            .map(|(key, value)| AdapterSetting {
                key: key.to_owned(),
                value: AdapterValue::Text(value.to_owned()),
            })
            .collect(),
            secrets: vec![AdapterSecret {
                key: "fixture_token".to_owned(),
                value: b"fixture-secret".to_vec(),
            }],
            created_at,
        })
        .await
        .expect("fixture read connection is stored");
}

async fn source_id(pool: &sqlx::PgPool, provider_key_value: &[u8]) -> Uuid {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT id
FROM sources
WHERE connection_id = $1 AND provider_key_value = $2
",
    )
    .bind(CONNECTION_ID)
    .bind(provider_key_value)
    .fetch_one(pool)
    .await
    .expect("fixture source is published")
}

async fn catalog_item_count(pool: &sqlx::PgPool, source_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM catalog_items WHERE source_id = $1")
        .bind(source_id)
        .fetch_one(pool)
        .await
        .expect("catalog item count is read")
}

async fn coordinate_binding_count(pool: &sqlx::PgPool, source_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM source_coordinate_bindings WHERE source_id = $1")
        .bind(source_id)
        .fetch_one(pool)
        .await
        .expect("coordinate binding count is read")
}

fn portable_reference(value: &str) -> PortableReference {
    PortableReference::new("example.media", value.as_bytes().to_vec())
        .expect("portable reference is valid")
}

async fn admit_discovery(
    task_manager: &TaskManager<PostgresTaskRepository>,
    task_id: Uuid,
    admitted_at: OffsetDateTime,
) {
    task_manager
        .admit(AdmitTask {
            task_id,
            workspace_id: None,
            definition: TaskDefinition::SourceDiscovery {
                connection_id: CONNECTION_ID,
            },
            cancellation_supported: false,
            admitted_at,
        })
        .await
        .expect("fixture discovery task is admitted");
}

async fn admit_read(
    task_manager: &TaskManager<PostgresTaskRepository>,
    task_id: Uuid,
    source_id: Uuid,
    stream: SourceStream,
    cancellation_supported: bool,
    admitted_at: OffsetDateTime,
) {
    task_manager
        .admit(AdmitTask {
            task_id,
            workspace_id: None,
            definition: TaskDefinition::Read {
                source_id,
                stream,
                mode: ReadMode::Full,
            },
            cancellation_supported,
            admitted_at,
        })
        .await
        .expect("fixture read task is admitted");
}

fn dispatch(owner_id: Uuid, attempt_id: Uuid, claimed: i64, completed: i64) -> DispatchNextTask {
    DispatchNextTask {
        owner_id,
        attempt_id,
        claimed_at: timestamp(claimed),
        completed_at: timestamp(completed),
    }
}

async fn catalog_snapshot(
    pool: &sqlx::PgPool,
    source_id: Uuid,
) -> (String, Option<Vec<u8>>, Option<Uuid>, i64) {
    let (status, cursor, publication_id) = sqlx::query_as::<_, (String, Option<Vec<u8>>, Option<Uuid>)>(
        "SELECT status, cursor, publication_id FROM source_streams WHERE source_id = $1 AND stream_kind = 'catalog'",
    )
    .bind(source_id)
    .fetch_one(pool)
    .await
    .expect("catalog stream is queried");
    let item_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM catalog_items WHERE source_id = $1")
            .bind(source_id)
            .fetch_one(pool)
            .await
            .expect("catalog projection count is queried");
    (status, cursor, publication_id, item_count)
}

async fn table_count(pool: &sqlx::PgPool, table: &str) -> i64 {
    let query = match table {
        "connections" => "SELECT COUNT(*) FROM connections",
        "encrypted_secrets" => "SELECT COUNT(*) FROM encrypted_secrets",
        "accounts" => "SELECT COUNT(*) FROM accounts",
        "sources" => "SELECT COUNT(*) FROM sources",
        "source_streams" => "SELECT COUNT(*) FROM source_streams",
        "state_observations" => "SELECT COUNT(*) FROM state_observations",
        _ => panic!("unsupported test table"),
    };
    sqlx::query_scalar(query)
        .fetch_one(pool)
        .await
        .expect("table count is read")
}

fn timestamp(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(second).expect("test timestamp is valid")
}
