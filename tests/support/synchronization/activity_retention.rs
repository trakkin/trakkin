#[tokio::test]
async fn sync_activity_pages_filters_and_details_applied_and_reconciled_runs() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    let draft = admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let direct_writes = Arc::new(Mutex::new(Vec::new()));
    let direct_dispatcher = TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            RecordingSessionRegistry::new(RecordingSyncSession {
                writes: Arc::clone(&direct_writes),
                provider_mutations: Arc::new(Mutex::new(0)),
                cancelled_operations: Arc::new(Mutex::new(Vec::new())),
                outcomes: VecDeque::from([Some(applied_outcome())]),
            }),
        ),
    );
    direct_dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(60),
            attempt_id: Uuid::from_u128(61),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("direct applied action dispatches")
        .expect("direct applied action is claimable");
    assert_eq!(direct_writes.lock().expect("direct writes lock").len(), 1);

    let followup_preview_id = Uuid::from_u128(62);
    let followup_run_id = Uuid::from_u128(64);
    let followup_action_id = Uuid::from_u128(65);
    let followup_task_id = Uuid::from_u128(66);
    let followup_created_at = CREATED_AT + time::Duration::seconds(1);
    admit_followup_receiver_action(
        &pool,
        draft,
        followup_preview_id,
        followup_run_id,
        followup_action_id,
        followup_task_id,
        followup_created_at,
    )
    .await;

    let mut indeterminate = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    indeterminate.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    let reconciled_writes = Arc::new(Mutex::new(Vec::new()));
    let reconciled_reads = Arc::new(Mutex::new(Vec::new()));
    let mut reconciliation_outcome = matched_reconciliation(true, true);
    let AdapterTargetedStateReadOutcome::Matched(observed) = &mut reconciliation_outcome else {
        unreachable!("matched reconciliation helper returns a match")
    };
    observed
        .write_causation
        .as_mut()
        .expect("matched reconciliation includes causation")
        .idempotency_key = followup_action_id.as_bytes().to_vec();
    let reconciliation_sessions =
        ReconciliationSessionRegistry::new(ReconciliationRecordingSession {
            writes: Arc::clone(&reconciled_writes),
            reads: Arc::clone(&reconciled_reads),
            write_outcomes: VecDeque::from([indeterminate]),
            read_outcomes: VecDeque::from([Ok(reconciliation_outcome)]),
        });
    TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            reconciliation_sessions.clone(),
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(67),
        attempt_id: Uuid::from_u128(68),
        claimed_at: followup_created_at,
        completed_at: followup_created_at,
    })
    .await
    .expect("indeterminate action dispatches")
    .expect("indeterminate action is claimable");
    TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionReconciliationTaskHandler::new(
            PostgresSyncActionReconciliationRepository::new(pool.clone()),
            reconciliation_sessions,
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(69),
        attempt_id: Uuid::from_u128(70),
        claimed_at: followup_created_at + time::Duration::seconds(1),
        completed_at: followup_created_at + time::Duration::seconds(1),
    })
    .await
    .expect("reconciliation dispatches")
    .expect("reconciliation is claimable");
    assert_eq!(
        reconciled_writes
            .lock()
            .expect("reconciled writes lock")
            .len(),
        1
    );
    assert_eq!(
        reconciled_reads
            .lock()
            .expect("reconciled reads lock")
            .len(),
        1
    );

    let unrelated_task_id = Uuid::from_u128(71);
    TaskManager::new(PostgresTaskRepository::new(pool.clone()))
        .admit(AdmitTask {
            task_id: unrelated_task_id,
            workspace_id: Some(WORKSPACE_ID),
            definition: TaskDefinition::SourceDiscovery {
                connection_id: CONNECTION_ID,
            },
            cancellation_supported: true,
            admitted_at: followup_created_at + time::Duration::seconds(2),
        })
        .await
        .expect("unrelated operational task is admitted");

    let activity = SyncActivityService::new(PostgresSyncActivityRepository::new(pool.clone()));
    let query = QuerySyncActivity {
        workspace_id: WORKSPACE_ID,
        statuses: Vec::new(),
        receiver_source_id: None,
        semantic_field: None,
        occurred_at_or_after: None,
        occurred_before: None,
        limit: 1,
        cursor: None,
    };
    let first_page = activity
        .list(query.clone())
        .await
        .expect("first Activity page loads");
    assert_eq!(first_page.runs.len(), 1);
    assert_eq!(first_page.runs[0].run_id, followup_run_id);
    assert_eq!(first_page.runs[0].status, SyncActivityRunStatus::Succeeded);
    assert_eq!(first_page.runs[0].actions.len(), 1);
    assert_ne!(
        first_page.runs[0].actions[0].write_task.task_id,
        unrelated_task_id
    );
    assert_ne!(
        first_page.runs[0].actions[0]
            .reconciliation_task
            .as_ref()
            .expect("reconciliation task is projected")
            .task_id,
        unrelated_task_id
    );
    let cursor = first_page.next_cursor.expect("bounded page has cursor");
    let second_page = activity
        .list(QuerySyncActivity {
            cursor: Some(cursor.clone()),
            ..query.clone()
        })
        .await
        .expect("second Activity page loads");
    assert_eq!(second_page.runs.len(), 1);
    assert_eq!(second_page.runs[0].run_id, RUN_ID);
    assert!(second_page.next_cursor.is_none());

    let filtered = activity
        .list(QuerySyncActivity {
            statuses: vec![SyncActivityRunStatus::Succeeded],
            receiver_source_id: Some(RECEIVER_B_ID),
            semantic_field: Some("trakkin.state:progress".to_owned()),
            occurred_at_or_after: Some(followup_created_at),
            occurred_before: Some(followup_created_at + time::Duration::seconds(1)),
            limit: 10,
            ..query.clone()
        })
        .await
        .expect("all Activity filters compose");
    assert_eq!(filtered.runs.len(), 1);
    assert_eq!(filtered.runs[0].run_id, followup_run_id);
    assert!(matches!(
        activity
            .list(QuerySyncActivity {
                semantic_field: Some("trakkin.state:progress".to_owned()),
                cursor: Some(cursor),
                ..query.clone()
            })
            .await,
        Err(trakkin::application::sync_activity::SyncActivityError::Repository(_))
    ));
    let isolated = activity
        .list(QuerySyncActivity {
            workspace_id: OTHER_WORKSPACE_ID,
            limit: 10,
            ..query
        })
        .await
        .expect("other workspace Activity loads");
    assert!(isolated.runs.is_empty());

    let direct = activity
        .get_action(WORKSPACE_ID, ACTION_ID)
        .await
        .expect("direct applied Activity detail loads");
    assert_eq!(direct.summary.status, SyncActivityActionStatus::Applied);
    assert_eq!(
        direct.reconciliation_guidance,
        SyncActivityReconciliationGuidance::NotRequired
    );
    let direct_provenance = direct
        .provenance
        .as_ref()
        .expect("direct provenance is present");
    assert!(direct_provenance.receipt_edge().is_some());
    assert!(direct_provenance.reconciliation_edge().is_none());
    assert!(!direct_provenance.preview_action.contributors.is_empty());
    assert!(!direct_provenance.preview_action.mapping_proofs.is_empty());
    assert_eq!(direct_provenance.policy_id, POLICY_ID);
    assert_eq!(direct.summary.receiver_source_id, RECEIVER_B_ID);

    let reconciled = activity
        .get_action(WORKSPACE_ID, followup_action_id)
        .await
        .expect("reconciled Activity detail loads");
    assert_eq!(reconciled.summary.status, SyncActivityActionStatus::Applied);
    assert_eq!(
        reconciled.reconciliation_guidance,
        SyncActivityReconciliationGuidance::ConfirmedApplied
    );
    let reconciled_provenance = reconciled
        .provenance
        .as_ref()
        .expect("reconciled provenance is present");
    assert!(reconciled_provenance.receipt_edge().is_none());
    assert!(reconciled_provenance.reconciliation_edge().is_some());
    assert_eq!(
        reconciled_provenance.attempt.task_attempt_id,
        Uuid::from_u128(68)
    );

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_activity_details_preserve_direct_and_reconciled_evidence() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(
        &pool,
        &[
            (RECEIVER_A_ID, ACTION_ID, TASK_ID),
            (RECEIVER_B_ID, SECOND_ACTION_ID, SECOND_TASK_ID),
        ],
    )
    .await;

    let writes = Arc::new(Mutex::new(Vec::new()));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let mut indeterminate = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    indeterminate.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    let mut reconciliation = matched_reconciliation(true, true);
    let AdapterTargetedStateReadOutcome::Matched(observed) = &mut reconciliation else {
        unreachable!("matched reconciliation helper returns a match")
    };
    observed
        .write_causation
        .as_mut()
        .expect("matched reconciliation includes causation")
        .idempotency_key = SECOND_ACTION_ID.as_bytes().to_vec();
    let sessions = ReconciliationSessionRegistry::new(ReconciliationRecordingSession {
        writes: Arc::clone(&writes),
        reads: Arc::clone(&reads),
        write_outcomes: VecDeque::from([applied_outcome(), indeterminate]),
        read_outcomes: VecDeque::from([Ok(reconciliation)]),
    });
    let write_dispatcher = TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions.clone(),
        ),
    );
    for (owner_id, attempt_id) in [(80_u128, 81_u128), (82, 83)] {
        write_dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id: Uuid::from_u128(owner_id),
                attempt_id: Uuid::from_u128(attempt_id),
                claimed_at: CREATED_AT,
                completed_at: CREATED_AT,
            })
            .await
            .expect("receiver action dispatches")
            .expect("receiver action is claimable");
    }
    TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionReconciliationTaskHandler::new(
            PostgresSyncActionReconciliationRepository::new(pool.clone()),
            sessions,
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(84),
        attempt_id: Uuid::from_u128(85),
        claimed_at: CREATED_AT,
        completed_at: CREATED_AT,
    })
    .await
    .expect("reconciliation dispatches")
    .expect("reconciliation is claimable");

    let activity = SyncActivityService::new(PostgresSyncActivityRepository::new(pool.clone()));
    let direct = activity
        .get_action(WORKSPACE_ID, ACTION_ID)
        .await
        .expect("direct action detail loads");
    let reconciled = activity
        .get_action(WORKSPACE_ID, SECOND_ACTION_ID)
        .await
        .expect("reconciled action detail loads");
    assert_eq!(writes.lock().expect("writes lock").len(), 2);
    assert_eq!(reads.lock().expect("reads lock").len(), 1);
    assert!(
        direct
            .provenance
            .as_ref()
            .and_then(|provenance| provenance.receipt_edge())
            .is_some()
    );
    assert!(
        direct
            .provenance
            .as_ref()
            .and_then(|provenance| provenance.reconciliation_edge())
            .is_none()
    );
    assert!(
        reconciled
            .provenance
            .as_ref()
            .and_then(|provenance| provenance.receipt_edge())
            .is_none()
    );
    assert!(
        reconciled
            .provenance
            .as_ref()
            .and_then(|provenance| provenance.reconciliation_edge())
            .is_some()
    );
    assert!(matches!(
        activity.get_action(OTHER_WORKSPACE_ID, ACTION_ID).await,
        Err(trakkin::application::sync_activity::SyncActivityError::NotFound)
    ));
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_retention_deletes_expired_records_in_bounded_causal_order() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    let draft = admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            RecordingSessionRegistry::new(RecordingSyncSession {
                writes: Arc::new(Mutex::new(Vec::new())),
                provider_mutations: Arc::new(Mutex::new(0)),
                cancelled_operations: Arc::new(Mutex::new(Vec::new())),
                outcomes: VecDeque::from([Some(applied_outcome())]),
            }),
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(90),
        attempt_id: Uuid::from_u128(91),
        claimed_at: CREATED_AT,
        completed_at: CREATED_AT,
    })
    .await
    .expect("expired run action dispatches")
    .expect("expired run action is claimable");

    let preview_service = SyncPreviewService::new(PostgresSyncPreviewRepository::new(pool.clone()));
    let mut unused_fingerprint = preview_fingerprint(&draft);
    unused_fingerprint.preview_time = CREATED_AT + TimeDuration::seconds(10);
    unused_fingerprint.preview_expiry = CREATED_AT + TimeDuration::minutes(1);
    let unused_preview_id = Uuid::from_u128(92);
    preview_service
        .create(CreateSyncPreviewSnapshot {
            preview_id: unused_preview_id,
            workspace_id: WORKSPACE_ID,
            draft: draft.clone(),
            fingerprint: unused_fingerprint,
            created_at: CREATED_AT + TimeDuration::seconds(10),
            expires_at: CREATED_AT + TimeDuration::minutes(1),
        })
        .await
        .expect("expired unused preview is stored");

    let retained_run_id = Uuid::from_u128(95);
    admit_followup_receiver_action(
        &pool,
        draft,
        Uuid::from_u128(96),
        retained_run_id,
        Uuid::from_u128(98),
        Uuid::from_u128(99),
        CREATED_AT + TimeDuration::seconds(40),
    )
    .await;
    TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            RecordingSessionRegistry::new(RecordingSyncSession {
                writes: Arc::new(Mutex::new(Vec::new())),
                provider_mutations: Arc::new(Mutex::new(0)),
                cancelled_operations: Arc::new(Mutex::new(Vec::new())),
                outcomes: VecDeque::from([Some(applied_outcome())]),
            }),
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(100),
        attempt_id: Uuid::from_u128(101),
        claimed_at: CREATED_AT + TimeDuration::seconds(40),
        completed_at: CREATED_AT + TimeDuration::seconds(40),
    })
    .await
    .expect("retained run action dispatches")
    .expect("retained run action is claimable");

    let unreferenced_policy_id = Uuid::from_u128(102);
    let policies = SyncPolicyService::new(PostgresSyncPolicyRepository::new(pool.clone()));
    let unreferenced = policies
        .create(CreateSyncPolicy {
            policy_id: unreferenced_policy_id,
            workspace_id: WORKSPACE_ID,
            document: policy_document(),
            created_at: CREATED_AT,
        })
        .await
        .expect("unreferenced policy is created");
    policies
        .update(UpdateSyncPolicy {
            policy_id: unreferenced_policy_id,
            workspace_id: WORKSPACE_ID,
            base_content_hash: unreferenced.content_hash,
            document: concurrency_policy_document(),
            updated_at: CREATED_AT + TimeDuration::seconds(40),
        })
        .await
        .expect("unreferenced policy receives a new head");

    sqlx::query(
        "INSERT INTO workspace_sources (workspace_id, source_id, attached_at)
SELECT $1, id, $2 FROM sources
",
    )
    .bind(OTHER_WORKSPACE_ID)
    .bind(CREATED_AT)
    .execute(&pool)
    .await
    .expect("sources are attached to the isolated workspace");
    let isolated_policy_id = Uuid::from_u128(103);
    let isolated = policies
        .create(CreateSyncPolicy {
            policy_id: isolated_policy_id,
            workspace_id: OTHER_WORKSPACE_ID,
            document: policy_document(),
            created_at: CREATED_AT,
        })
        .await
        .expect("isolated policy is created");
    policies
        .update(UpdateSyncPolicy {
            policy_id: isolated_policy_id,
            workspace_id: OTHER_WORKSPACE_ID,
            base_content_hash: isolated.content_hash,
            document: concurrency_policy_document(),
            updated_at: CREATED_AT + TimeDuration::seconds(40),
        })
        .await
        .expect("isolated policy receives a new head");

    let activity = SyncActivityService::new(PostgresSyncActivityRepository::new(pool.clone()));
    let expired_detail = activity
        .get_action(WORKSPACE_ID, ACTION_ID)
        .await
        .expect("expiring action evidence is inspectable before its audit cutoff");
    let retained_action_id = Uuid::from_u128(98);
    let retained_detail = activity
        .get_action(WORKSPACE_ID, retained_action_id)
        .await
        .expect("newer shared-policy action evidence is inspectable");
    assert!(
        sqlx::query("DELETE FROM sync_action_provenance WHERE action_id = $1")
            .bind(ACTION_ID)
            .execute(&pool)
            .await
            .is_err(),
        "ordinary deletion cannot erase unexpired provenance"
    );
    let retention = SyncRetentionService::new(PostgresSyncRetentionRepository::new(pool.clone()));
    let retained_at = CREATED_AT + TimeDuration::minutes(2);
    let unused_rows = sqlx::query_scalar::<_, i64>(
        "SELECT 1 + COUNT(*) FROM sync_preview_actions WHERE preview_id = $1
",
    )
    .bind(unused_preview_id)
    .fetch_one(&pool)
    .await
    .expect("unused preview rows are measurable") as u32;
    let before_first = retention_physical_rows(&pool).await;
    let first = retention
        .retain(RetainSyncEvidence {
            workspace_id: WORKSPACE_ID,
            retained_at,
            policy: SyncRetentionPolicy {
                audit_retention: TimeDuration::seconds(90),
                maximum_deleted_rows: unused_rows,
            },
        })
        .await
        .expect("bounded unused-preview retention succeeds");
    assert_eq!(first.classes[0].class, SyncRetentionClass::UnusedPreview);
    assert_eq!(first.classes[0].deleted_roots, 1);
    assert_eq!(first.classes[0].deleted_rows, unused_rows);
    assert_eq!(first.classes[1].deleted_roots, 0);
    assert!(first.deferred);
    assert_eq!(first.next_batch_minimum_rows, None);
    assert_eq!(
        before_first - retention_physical_rows(&pool).await,
        i64::from(first.deleted_rows())
    );
    assert_eq!(
        activity
            .get_action(WORKSPACE_ID, ACTION_ID)
            .await
            .expect("deferred action evidence remains inspectable"),
        expired_detail
    );

    let before_oversized = retention_physical_rows(&pool).await;
    let oversized = retention
        .retain(RetainSyncEvidence {
            workspace_id: WORKSPACE_ID,
            retained_at,
            policy: SyncRetentionPolicy {
                audit_retention: TimeDuration::seconds(90),
                maximum_deleted_rows: 1,
            },
        })
        .await
        .expect("oversized atomic run bundle is deferred");
    assert_eq!(oversized.deleted_rows(), 0);
    assert!(oversized.deferred);
    assert!(
        oversized
            .next_batch_minimum_rows
            .is_some_and(|rows| rows > 1)
    );
    assert_eq!(retention_physical_rows(&pool).await, before_oversized);
    assert_eq!(
        activity
            .get_action(WORKSPACE_ID, ACTION_ID)
            .await
            .expect("oversized deferred evidence remains inspectable"),
        expired_detail
    );

    let before_second = retention_physical_rows(&pool).await;
    let second = retention
        .retain(RetainSyncEvidence {
            workspace_id: WORKSPACE_ID,
            retained_at,
            policy: SyncRetentionPolicy {
                audit_retention: TimeDuration::seconds(90),
                maximum_deleted_rows: 512,
            },
        })
        .await
        .expect("run-bundle retention succeeds");
    assert_eq!(second.classes[1].class, SyncRetentionClass::RunBundle);
    assert_eq!(second.classes[1].deleted_roots, 1);
    assert_eq!(
        second.classes[2].class,
        SyncRetentionClass::UnreferencedPolicy
    );
    assert_eq!(second.classes[2].deleted_roots, 1);
    assert!(!second.deferred);
    assert_eq!(second.next_batch_minimum_rows, None);
    assert_eq!(
        before_second - retention_physical_rows(&pool).await,
        i64::from(second.deleted_rows())
    );
    assert!(matches!(
        activity.get_action(WORKSPACE_ID, ACTION_ID).await,
        Err(trakkin::application::sync_activity::SyncActivityError::NotFound)
    ));
    assert_eq!(
        activity
            .get_action(WORKSPACE_ID, retained_action_id)
            .await
            .expect("newer action evidence survives unchanged"),
        retained_detail
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sync_policy_revisions WHERE policy_id = $1"
        )
        .bind(POLICY_ID)
        .fetch_one(&pool)
        .await
        .expect("shared policy revision survival is queryable"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sync_policy_revisions WHERE policy_id = $1"
        )
        .bind(isolated_policy_id)
        .fetch_one(&pool)
        .await
        .expect("isolated policy revisions are queryable"),
        2
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_retention_preserves_runs_with_active_reconciliation() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let mut indeterminate = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    indeterminate.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            RecordingSessionRegistry::new(RecordingSyncSession {
                writes: Arc::new(Mutex::new(Vec::new())),
                provider_mutations: Arc::new(Mutex::new(0)),
                cancelled_operations: Arc::new(Mutex::new(Vec::new())),
                outcomes: VecDeque::from([Some(indeterminate)]),
            }),
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(104),
        attempt_id: Uuid::from_u128(105),
        claimed_at: CREATED_AT,
        completed_at: CREATED_AT,
    })
    .await
    .expect("indeterminate action dispatches")
    .expect("indeterminate action is claimable");
    assert_eq!(
        table_count(&pool, "sync_action_reconciliation_tasks").await,
        1
    );

    let before = retention_physical_rows(&pool).await;
    let report = SyncRetentionService::new(PostgresSyncRetentionRepository::new(pool.clone()))
        .retain(RetainSyncEvidence {
            workspace_id: WORKSPACE_ID,
            retained_at: CREATED_AT + TimeDuration::days(100),
            policy: SyncRetentionPolicy {
                audit_retention: TimeDuration::days(1),
                maximum_deleted_rows: 512,
            },
        })
        .await
        .expect("active reconciliation retention pass succeeds");
    assert_eq!(report.deleted_rows(), 0);
    assert!(!report.deferred);
    assert_eq!(retention_physical_rows(&pool).await, before);
    assert_eq!(table_count(&pool, "sync_runs").await, 1);
    assert_eq!(
        table_count(&pool, "sync_action_reconciliation_tasks").await,
        1
    );
    database.shutdown().await.expect("database shuts down");
}

async fn admit_followup_receiver_action(
    pool: &sqlx::PgPool,
    draft: trakkin::application::sync_preview::SyncPreviewDraft,
    preview_id: Uuid,
    run_id: Uuid,
    action_id: Uuid,
    task_id: Uuid,
    admitted_at: OffsetDateTime,
) {
    let mut fingerprint = preview_fingerprint(&draft);
    fingerprint.preview_time = admitted_at;
    let preview_service = SyncPreviewService::new(PostgresSyncPreviewRepository::new(pool.clone()));
    let stored = preview_service
        .create(CreateSyncPreviewSnapshot {
            preview_id,
            workspace_id: WORKSPACE_ID,
            draft,
            fingerprint: fingerprint.clone(),
            created_at: admitted_at,
            expires_at: EXPIRES_AT,
        })
        .await
        .expect("follow-up preview is stored");
    let selected = stored
        .actions
        .iter()
        .find(|action| action.receiver_source_id == RECEIVER_B_ID)
        .expect("follow-up preview retains the receiver target");
    SyncRunService::new(PostgresSyncRunRepository::new(pool.clone()))
        .admit(AdmitSyncPreview {
            run_id,
            schedule_occurrence_id: None,
            preview_id,
            workspace_id: WORKSPACE_ID,
            preview_hash: stored.content_hash,
            current_fingerprint: fingerprint,
            actions: vec![SyncActionAdmission {
                action_hash: selected.action_hash.clone(),
                action_id,
                task_id,
            }],
            admitted_at,
        })
        .await
        .expect("follow-up receiver action is admitted");
}

async fn retention_physical_rows(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT
    (SELECT COUNT(*) FROM sync_policy_revisions)
    + (SELECT COUNT(*) FROM sync_policy_source_roles)
    + (SELECT COUNT(*) FROM sync_previews)
    + (SELECT COUNT(*) FROM sync_preview_actions)
    + (SELECT COUNT(*) FROM sync_runs)
    + (SELECT COUNT(*) FROM sync_actions)
    + (SELECT COUNT(*) FROM sync_action_tasks)
    + (SELECT COUNT(*) FROM sync_action_attempts)
    + (SELECT COUNT(*) FROM sync_action_attempt_field_effects)
    + (SELECT COUNT(*) FROM sync_action_attempt_error_field_problems)
    + (SELECT COUNT(*) FROM sync_action_reconciliation_tasks)
    + (SELECT COUNT(*) FROM sync_action_reconciliation_attempts)
    + (SELECT COUNT(*) FROM sync_action_provenance)
    + (SELECT COUNT(*) FROM task_claims
       WHERE task_id IN (
           SELECT id FROM tasks
           WHERE kind IN ('sync_action', 'sync_action_reconciliation')
       ))
    + (SELECT COUNT(*) FROM task_attempts
       WHERE task_id IN (
           SELECT id FROM tasks
           WHERE kind IN ('sync_action', 'sync_action_reconciliation')
       ))
    + (SELECT COUNT(*) FROM tasks
       WHERE kind IN ('sync_action', 'sync_action_reconciliation'))
",
    )
    .fetch_one(pool)
    .await
    .expect("retention physical rows are queryable")
}
