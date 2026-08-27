#[tokio::test]
async fn recurring_sync_resumes_admitted_occurrence_without_duplicate_tasks() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    seed_sources(&pool).await;
    let policy = SyncPolicyService::new(PostgresSyncPolicyRepository::new(pool.clone()))
        .create(CreateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            document: policy_document(),
            created_at: CREATED_AT - TimeDuration::minutes(2),
        })
        .await
        .expect("policy revision is stored");
    let construction = preview_command(policy);
    let partitions = construction.partitions.clone();
    let draft = SyncPreviewBuilder::new(StateRegistry::v1())
        .construct(construction)
        .expect("preview draft constructs");
    let fingerprint = preview_fingerprint(&draft);
    let schedule_id = Uuid::from_u128(60);
    let occurrence_id = Uuid::from_u128(61);
    let preview_id = Uuid::from_u128(62);
    let run_id = Uuid::from_u128(63);
    let recurrence = SyncScheduleRecurrence::every(60).expect("one minute is valid");
    SyncScheduleService::new(PostgresSyncScheduleRepository::new(pool.clone()))
        .enable(EnableSyncSchedule {
            schedule_id,
            workspace_id: WORKSPACE_ID,
            policy_id: POLICY_ID,
            recurrence,
            enabled_at: CREATED_AT - TimeDuration::minutes(1),
        })
        .await
        .expect("schedule is enabled");
    let collector = StaticSyncPreviewInputCollector {
        collected: CollectedSyncPreviewInputs {
            partitions,
            fingerprint,
        },
        commands: Arc::new(Mutex::new(Vec::new())),
    };
    let runner = SyncScheduleRunner::new(
        PostgresSyncScheduleRepository::new(pool.clone()),
        PostgresSyncPolicyRepository::new(pool.clone()),
        collector.clone(),
        PostgresSyncPreviewRepository::new(pool.clone()),
        PostgresSyncRunRepository::new(pool.clone()),
    );
    let first = runner
        .tick(TickSyncSchedules {
            occurrence_id,
            preview_id,
            run_id,
            observed_at: CREATED_AT,
            preview_expires_at: EXPIRES_AT,
        })
        .await
        .expect("due schedule starts a run");
    assert!(matches!(
        first,
        SyncScheduleTickResult::RunStarted {
            occurrence_id: admitted_occurrence_id,
            ..
        } if admitted_occurrence_id == occurrence_id
    ));

    sqlx::query(
        "UPDATE sync_schedule_occurrences
SET status = 'pending', completed_at = NULL
WHERE id = $1",
    )
    .bind(occurrence_id)
    .execute(&pool)
    .await
    .expect("post-admission interruption is simulated");
    let restarted = SyncScheduleRunner::new(
        PostgresSyncScheduleRepository::new(pool.clone()),
        PostgresSyncPolicyRepository::new(pool.clone()),
        collector,
        PostgresSyncPreviewRepository::new(pool.clone()),
        PostgresSyncRunRepository::new(pool.clone()),
    );
    let resumed = restarted
        .tick(TickSyncSchedules {
            occurrence_id: Uuid::from_u128(64),
            preview_id: Uuid::from_u128(65),
            run_id: Uuid::from_u128(66),
            observed_at: CREATED_AT + TimeDuration::seconds(1),
            preview_expires_at: EXPIRES_AT,
        })
        .await
        .expect("admitted occurrence completes after restart");
    assert_eq!(
        resumed,
        SyncScheduleTickResult::RunAlreadyStarted {
            occurrence_id,
            run_id,
        }
    );
    assert_eq!(table_count(&pool, "sync_runs").await, 1);
    assert_eq!(table_count(&pool, "sync_actions").await, 2);
    assert_eq!(table_count(&pool, "tasks").await, 2);
}
