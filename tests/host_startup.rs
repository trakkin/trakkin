#![cfg(feature = "embedded-database")]

use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::task_manager::{AdmitTask, TaskDefinition, TaskRepository, TaskStatus},
    database::DatabaseRuntime,
    host::Host,
};
use uuid::Uuid;

const CONNECTION_ID: Uuid = Uuid::from_u128(1);
const INTERRUPTED_TASK_ID: Uuid = Uuid::from_u128(2);
const ADMITTED_TASK_ID: Uuid = Uuid::from_u128(3);

#[tokio::test]
async fn startup_interrupts_existing_work_and_accepts_new_tasks() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("initial database starts");
    let queued_at = timestamp(0);
    sqlx::query(
        "INSERT INTO connections (
             id, provider_id, display_name, status, created_at, updated_at
         ) VALUES ($1, 'fixture', 'Fixture', 'ready', $2, $2)",
    )
    .bind(CONNECTION_ID)
    .bind(queued_at)
    .execute(&database.pool())
    .await
    .expect("connection is persisted before restart");
    sqlx::query(
        "INSERT INTO tasks (
             id, connection_id, kind, status, cancellation_supported,
             created_at, updated_at
         ) VALUES (
             $1, $2, 'source_discovery', 'queued', TRUE,
             $3, $3
         )",
    )
    .bind(INTERRUPTED_TASK_ID)
    .bind(CONNECTION_ID)
    .bind(queued_at)
    .execute(&database.pool())
    .await
    .expect("queued task is persisted before restart");
    sqlx::query(
        "INSERT INTO source_discovery_tasks (task_id, connection_id)
         VALUES ($1, $2)",
    )
    .bind(INTERRUPTED_TASK_ID)
    .bind(CONNECTION_ID)
    .execute(&database.pool())
    .await
    .expect("queued task detail is persisted before restart");
    database
        .shutdown()
        .await
        .expect("initial database shuts down");

    let host = Host::start(directory.path(), timestamp(1))
        .await
        .expect("host starts after recovering active work");

    assert_eq!(
        host.task_repository()
            .get_task(INTERRUPTED_TASK_ID)
            .await
            .expect("recovered task can be read")
            .expect("recovered task exists")
            .status,
        TaskStatus::Interrupted
    );
    let admitted = host
        .task_repository()
        .admit_task(AdmitTask {
            task_id: ADMITTED_TASK_ID,
            workspace_id: None,
            definition: TaskDefinition::SourceDiscovery {
                connection_id: CONNECTION_ID,
            },
            cancellation_supported: true,
            admitted_at: timestamp(2),
        })
        .await
        .expect("new work is admitted after recovery");
    assert_eq!(admitted.status, TaskStatus::Queued);
    host.shutdown().await.expect("host shuts down");
}

fn timestamp(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(second).expect("test timestamp is valid")
}
