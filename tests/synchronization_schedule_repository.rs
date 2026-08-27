#![cfg(feature = "embedded-database")]

use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::sync_schedule::{
        EnableSyncSchedule, MaterializeDueSyncSchedule, PauseSyncSchedule,
        ReplacePendingSyncPreview, SyncScheduleOccurrenceStatus, SyncScheduleRecurrence,
        SyncScheduleService, SyncScheduleState,
    },
    database::DatabaseRuntime,
    infrastructure::sync_schedule::PostgresSyncScheduleRepository,
};
use uuid::Uuid;

const WORKSPACE_ID: Uuid = Uuid::from_u128(1);
const POLICY_ID: Uuid = Uuid::from_u128(2);
const SCHEDULE_ID: Uuid = Uuid::from_u128(3);
const OCCURRENCE_ID: Uuid = Uuid::from_u128(4);
const PREVIEW_ID: Uuid = Uuid::from_u128(5);
const RUN_ID: Uuid = Uuid::from_u128(6);

#[tokio::test]
async fn due_schedule_survives_restart_without_duplicate_materialization() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    seed_policy(&database.pool()).await;
    let service = SyncScheduleService::new(PostgresSyncScheduleRepository::new(database.pool()));
    let recurrence = SyncScheduleRecurrence::every(300).expect("five minutes is valid");

    let enabled = service
        .enable(EnableSyncSchedule {
            schedule_id: SCHEDULE_ID,
            workspace_id: WORKSPACE_ID,
            policy_id: POLICY_ID,
            recurrence,
            enabled_at: timestamp(1_000),
        })
        .await
        .expect("schedule is enabled");
    assert_eq!(enabled.state, SyncScheduleState::Enabled);
    assert_eq!(enabled.next_run_at, Some(timestamp(1_300)));

    let occurrence = service
        .materialize_due(MaterializeDueSyncSchedule {
            occurrence_id: OCCURRENCE_ID,
            preview_id: PREVIEW_ID,
            run_id: RUN_ID,
            observed_at: timestamp(2_001),
        })
        .await
        .expect("due schedule materializes")
        .expect("one schedule is due");
    assert_eq!(occurrence.scheduled_for, timestamp(1_300));
    assert_eq!(occurrence.status, SyncScheduleOccurrenceStatus::Pending);
    assert_eq!(
        service
            .get(WORKSPACE_ID, POLICY_ID)
            .await
            .expect("advanced schedule reloads")
            .next_run_at,
        Some(timestamp(2_200))
    );
    assert!(
        service
            .materialize_due(MaterializeDueSyncSchedule {
                occurrence_id: Uuid::from_u128(7),
                preview_id: Uuid::from_u128(8),
                run_id: Uuid::from_u128(9),
                observed_at: timestamp(2_001),
            })
            .await
            .expect("second tick is valid")
            .is_none()
    );

    let restarted = SyncScheduleService::new(PostgresSyncScheduleRepository::new(database.pool()));
    assert_eq!(
        restarted
            .oldest_pending()
            .await
            .expect("pending occurrence reloads after restart"),
        Some(occurrence)
    );
    let replacement_preview_id = Uuid::from_u128(10);
    let recovered = restarted
        .replace_pending_preview(ReplacePendingSyncPreview {
            occurrence_id: OCCURRENCE_ID,
            current_preview_id: PREVIEW_ID,
            replacement_preview_id,
        })
        .await
        .expect("pending occurrence rotates an unusable preview");
    assert_eq!(recovered.preview_id, replacement_preview_id);
    assert_eq!(recovered.run_id, RUN_ID);
    assert_eq!(recovered.scheduled_for, timestamp(1_300));
    let paused = restarted
        .pause(PauseSyncSchedule {
            workspace_id: WORKSPACE_ID,
            policy_id: POLICY_ID,
            paused_at: timestamp(2_002),
        })
        .await
        .expect("schedule pauses");
    assert_eq!(paused.state, SyncScheduleState::Paused);
    assert_eq!(paused.next_run_at, None);

    database.shutdown().await.expect("database shuts down");
}

async fn seed_policy(pool: &sqlx::PgPool) {
    sqlx::query(
        "INSERT INTO workspaces (id, name, created_at, updated_at)
VALUES ($1, 'Schedule test', $2, $2)",
    )
    .bind(WORKSPACE_ID)
    .bind(timestamp(0))
    .execute(pool)
    .await
    .expect("workspace is seeded");
    sqlx::query(
        "INSERT INTO sync_policies (
    id, workspace_id, head_revision, head_content_hash, created_at, updated_at
) VALUES ($1, $2, 1, $3, $4, $4)",
    )
    .bind(POLICY_ID)
    .bind(WORKSPACE_ID)
    .bind(format!("sha256:{}", "a".repeat(64)))
    .bind(timestamp(0))
    .execute(pool)
    .await
    .expect("policy is seeded");
}

fn timestamp(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(second).expect("test timestamp is valid")
}
