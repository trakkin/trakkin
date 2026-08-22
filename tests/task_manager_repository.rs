#![cfg(feature = "embedded-database")]

use sqlx::PgPool;
use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::{
        acquisition::ReadMode,
        task_manager::{
            AdmitTask, CancellationDisposition, ClaimNextTask, CompleteTask, HeartbeatTask,
            RequestTaskCancellation, SourceStream, TaskCompletion, TaskDefinition, TaskFailure,
            TaskKind, TaskManager, TaskStatus,
        },
    },
    database::DatabaseRuntime,
    infrastructure::task_manager::{PostgresTaskRepository, TaskManagerError},
};
use uuid::Uuid;

const CONNECTION_ALPHA: Uuid = Uuid::from_u128(1);
const CONNECTION_BETA: Uuid = Uuid::from_u128(2);
const TASK_ALPHA_ONE: Uuid = Uuid::from_u128(10);
const TASK_ALPHA_TWO: Uuid = Uuid::from_u128(11);
const TASK_BETA: Uuid = Uuid::from_u128(12);
const OWNER_ONE: Uuid = Uuid::from_u128(20);
const OWNER_TWO: Uuid = Uuid::from_u128(21);
const OWNER_THREE: Uuid = Uuid::from_u128(22);
const ATTEMPT_ONE: Uuid = Uuid::from_u128(30);
const ATTEMPT_TWO: Uuid = Uuid::from_u128(31);
const ATTEMPT_THREE: Uuid = Uuid::from_u128(32);
const WORKSPACE_ID: Uuid = Uuid::from_u128(40);
const SOURCE_ID: Uuid = Uuid::from_u128(41);

#[tokio::test]
async fn claims_are_connection_scoped_and_terminal_completion_releases_them() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_connection(&database.pool(), CONNECTION_ALPHA, "Alpha").await;
    seed_connection(&database.pool(), CONNECTION_BETA, "Beta").await;
    let manager = manager(database.pool());
    admit_discovery(&manager, TASK_ALPHA_ONE, CONNECTION_ALPHA, timestamp(0)).await;
    admit_discovery(&manager, TASK_ALPHA_TWO, CONNECTION_ALPHA, timestamp(1)).await;
    admit_discovery(&manager, TASK_BETA, CONNECTION_BETA, timestamp(2)).await;

    let alpha = manager
        .claim_next(ClaimNextTask {
            owner_id: OWNER_ONE,
            attempt_id: ATTEMPT_ONE,
            accepted_kinds: vec![TaskKind::SourceDiscovery],
            claimed_at: timestamp(3),
        })
        .await
        .expect("first task claim succeeds")
        .expect("first task is available");
    assert_eq!(alpha.task.id, TASK_ALPHA_ONE);
    assert_eq!(alpha.task.status, TaskStatus::Running);
    assert_eq!(alpha.attempt_ordinal, 0);

    let beta = manager
        .claim_next(ClaimNextTask {
            owner_id: OWNER_TWO,
            attempt_id: ATTEMPT_TWO,
            accepted_kinds: vec![TaskKind::SourceDiscovery],
            claimed_at: timestamp(4),
        })
        .await
        .expect("second task claim succeeds")
        .expect("another connection remains available");
    assert_eq!(beta.task.id, TASK_BETA);
    assert!(
        manager
            .claim_next(ClaimNextTask {
                owner_id: OWNER_THREE,
                attempt_id: ATTEMPT_THREE,
                accepted_kinds: vec![TaskKind::SourceDiscovery],
                claimed_at: timestamp(5),
            })
            .await
            .expect("blocked claim query succeeds")
            .is_none()
    );

    manager
        .heartbeat(HeartbeatTask {
            task_id: TASK_ALPHA_ONE,
            owner_id: OWNER_ONE,
            heartbeat_at: timestamp(6),
        })
        .await
        .expect("claim owner can heartbeat");
    assert!(matches!(
        manager
            .heartbeat(HeartbeatTask {
                task_id: TASK_ALPHA_ONE,
                owner_id: OWNER_TWO,
                heartbeat_at: timestamp(7),
            })
            .await,
        Err(TaskManagerError::ClaimNotOwned)
    ));

    assert!(matches!(
        manager
            .complete(CompleteTask {
                task_id: TASK_ALPHA_ONE,
                attempt_id: ATTEMPT_THREE,
                owner_id: OWNER_ONE,
                completion: TaskCompletion::Succeeded,
                completed_at: timestamp(8),
            })
            .await,
        Err(TaskManagerError::InvalidCompletion)
    ));
    let completed = manager
        .complete(CompleteTask {
            task_id: TASK_ALPHA_ONE,
            attempt_id: ATTEMPT_ONE,
            owner_id: OWNER_ONE,
            completion: TaskCompletion::Succeeded,
            completed_at: timestamp(8),
        })
        .await
        .expect("claim owner completes the task");
    assert_eq!(completed.status, TaskStatus::Succeeded);
    let next_alpha = manager
        .claim_next(ClaimNextTask {
            owner_id: OWNER_THREE,
            attempt_id: ATTEMPT_THREE,
            accepted_kinds: vec![TaskKind::SourceDiscovery],
            claimed_at: timestamp(9),
        })
        .await
        .expect("claim after release succeeds")
        .expect("next task on the connection is available");
    assert_eq!(next_alpha.task.id, TASK_ALPHA_TWO);

    let failed = manager
        .complete(CompleteTask {
            task_id: TASK_BETA,
            attempt_id: ATTEMPT_TWO,
            owner_id: OWNER_TWO,
            completion: TaskCompletion::Failed(TaskFailure {
                code: "adapter_unavailable".to_owned(),
                retryable: true,
            }),
            completed_at: timestamp(10),
        })
        .await
        .expect("claimed task records failure evidence");
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(
        failed.failure,
        Some(TaskFailure {
            code: "adapter_unavailable".to_owned(),
            retryable: true,
        })
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn cancellation_is_idempotent_and_fenced_against_success() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_connection(&database.pool(), CONNECTION_ALPHA, "Alpha").await;
    let manager = manager(database.pool());
    admit_discovery(&manager, TASK_ALPHA_ONE, CONNECTION_ALPHA, timestamp(0)).await;

    assert_eq!(
        manager
            .request_cancellation(RequestTaskCancellation {
                task_id: TASK_ALPHA_ONE,
                requested_at: timestamp(1),
            })
            .await
            .expect("queued cancellation succeeds"),
        CancellationDisposition::CancelledBeforeStart
    );
    assert_eq!(
        manager
            .get(TASK_ALPHA_ONE)
            .await
            .expect("cancelled task can be read")
            .expect("cancelled task exists")
            .status,
        TaskStatus::Cancelled
    );

    admit_discovery(&manager, TASK_ALPHA_TWO, CONNECTION_ALPHA, timestamp(2)).await;
    manager
        .claim_next(ClaimNextTask {
            owner_id: OWNER_ONE,
            attempt_id: ATTEMPT_ONE,
            accepted_kinds: vec![TaskKind::SourceDiscovery],
            claimed_at: timestamp(3),
        })
        .await
        .expect("task claim succeeds")
        .expect("task is available");
    assert_eq!(
        manager
            .request_cancellation(RequestTaskCancellation {
                task_id: TASK_ALPHA_TWO,
                requested_at: timestamp(4),
            })
            .await
            .expect("running cancellation succeeds"),
        CancellationDisposition::CancellationRequested
    );
    assert_eq!(
        manager
            .request_cancellation(RequestTaskCancellation {
                task_id: TASK_ALPHA_TWO,
                requested_at: timestamp(5),
            })
            .await
            .expect("repeated cancellation succeeds"),
        CancellationDisposition::AlreadyRequested
    );
    let cancelled = manager
        .complete(CompleteTask {
            task_id: TASK_ALPHA_TWO,
            attempt_id: ATTEMPT_ONE,
            owner_id: OWNER_ONE,
            completion: TaskCompletion::Succeeded,
            completed_at: timestamp(6),
        })
        .await
        .expect("cancellation wins over late success");
    assert_eq!(cancelled.status, TaskStatus::Cancelled);
    assert_eq!(
        manager
            .request_cancellation(RequestTaskCancellation {
                task_id: TASK_ALPHA_TWO,
                requested_at: timestamp(7),
            })
            .await
            .expect("terminal cancellation is idempotent"),
        CancellationDisposition::AlreadyTerminal(TaskStatus::Cancelled)
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn typed_source_tasks_require_workspace_attachment_and_round_trip() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_workspace_source(database.pool()).await;
    let manager = manager(database.pool());
    let definition = TaskDefinition::Read {
        source_id: SOURCE_ID,
        stream: SourceStream::Catalog,
        mode: ReadMode::Incremental,
    };

    let admitted = manager
        .admit(AdmitTask {
            task_id: TASK_ALPHA_ONE,
            workspace_id: Some(WORKSPACE_ID),
            definition: definition.clone(),
            cancellation_supported: true,
            admitted_at: timestamp(0),
        })
        .await
        .expect("attached source read is admitted");
    assert_eq!(admitted.definition, definition);
    assert_eq!(admitted.connection_id, CONNECTION_ALPHA);
    assert!(
        manager
            .claim_next(ClaimNextTask {
                owner_id: OWNER_ONE,
                attempt_id: ATTEMPT_ONE,
                accepted_kinds: vec![TaskKind::SourceDiscovery],
                claimed_at: timestamp(1),
            })
            .await
            .expect("filtered claim query succeeds")
            .is_none()
    );
    let claimed_read = manager
        .claim_next(ClaimNextTask {
            owner_id: OWNER_ONE,
            attempt_id: ATTEMPT_ONE,
            accepted_kinds: vec![TaskKind::CatalogRead],
            claimed_at: timestamp(1),
        })
        .await
        .expect("catalog claim query succeeds")
        .expect("catalog task is selected");
    assert_eq!(claimed_read.task.id, TASK_ALPHA_ONE);
    manager
        .complete(CompleteTask {
            task_id: TASK_ALPHA_ONE,
            attempt_id: ATTEMPT_ONE,
            owner_id: OWNER_ONE,
            completion: TaskCompletion::Succeeded,
            completed_at: timestamp(2),
        })
        .await
        .expect("filtered catalog task completes");
    let error = manager
        .admit(AdmitTask {
            task_id: TASK_ALPHA_TWO,
            workspace_id: Some(Uuid::from_u128(99)),
            definition: TaskDefinition::Reset {
                source_id: SOURCE_ID,
                stream: SourceStream::State,
            },
            cancellation_supported: true,
            admitted_at: timestamp(3),
        })
        .await
        .expect_err("unattached source task is rejected");
    assert!(matches!(error, TaskManagerError::SourceNotAttached));

    database.shutdown().await.expect("database shuts down");
}

fn manager(pool: PgPool) -> TaskManager<PostgresTaskRepository> {
    TaskManager::new(PostgresTaskRepository::new(pool))
}

async fn admit_discovery(
    manager: &TaskManager<PostgresTaskRepository>,
    task_id: Uuid,
    connection_id: Uuid,
    admitted_at: OffsetDateTime,
) {
    manager
        .admit(AdmitTask {
            task_id,
            workspace_id: None,
            definition: TaskDefinition::SourceDiscovery { connection_id },
            cancellation_supported: true,
            admitted_at,
        })
        .await
        .expect("source discovery task is admitted");
}

async fn seed_connection(pool: &PgPool, connection_id: Uuid, display_name: &str) {
    let now = timestamp(-10);
    sqlx::query(
        "INSERT INTO connections (
             id, adapter_key, display_name, status, created_at, updated_at
         ) VALUES ($1, 'fixture', $2, 'ready', $3, $3)",
    )
    .bind(connection_id)
    .bind(display_name)
    .bind(now)
    .execute(pool)
    .await
    .expect("connection is seeded");
}

async fn seed_workspace_source(pool: PgPool) {
    let now = timestamp(-10);
    let account_id = Uuid::from_u128(42);
    seed_connection(&pool, CONNECTION_ALPHA, "Alpha").await;
    sqlx::query(
        "INSERT INTO accounts (
             id, connection_id, provider_key_namespace, provider_key_value,
             display_name, first_seen_at, last_seen_at
         ) VALUES ($1, $2, 'fixture.account', $3, 'Account', $4, $4)",
    )
    .bind(account_id)
    .bind(CONNECTION_ALPHA)
    .bind(b"account".as_slice())
    .bind(now)
    .execute(&pool)
    .await
    .expect("account is seeded");
    sqlx::query(
        "INSERT INTO sources (
             id, connection_id, account_id, provider_key_namespace,
             provider_key_value, display_name, kind_namespace, kind_name,
             availability, capabilities, first_seen_at, last_seen_at
         ) VALUES (
             $1, $2, $3, 'fixture.source', $4, 'Source', 'fixture', 'library',
             'available', '{}', $5, $5
         )",
    )
    .bind(SOURCE_ID)
    .bind(CONNECTION_ALPHA)
    .bind(account_id)
    .bind(b"source".as_slice())
    .bind(now)
    .execute(&pool)
    .await
    .expect("source is seeded");
    sqlx::query(
        "INSERT INTO workspaces (id, name, created_at, updated_at)
         VALUES ($1, 'Workspace', $2, $2)",
    )
    .bind(WORKSPACE_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("workspace is seeded");
    sqlx::query(
        "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
         VALUES ($1, $2, $3)",
    )
    .bind(WORKSPACE_ID)
    .bind(SOURCE_ID)
    .bind(now)
    .execute(&pool)
    .await
    .expect("workspace source is seeded");
}

fn timestamp(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(second).expect("test timestamp is valid")
}
