#[tokio::test]
async fn sync_action_crash_recovery_classifies_prepared_and_dispatched_attempts() {
    assert_sync_action_crash_recovery(false, "confirmed_not_applied").await;
    assert_sync_action_crash_recovery(true, "indeterminate").await;
}

async fn assert_sync_action_crash_recovery(dispatched: bool, expected_result: &str) {
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
            created_at: CREATED_AT,
        })
        .await
        .expect("policy revision is stored");
    let draft = SyncPreviewBuilder::new(StateRegistry::v1())
        .construct(preview_command(policy))
        .expect("preview draft constructs");
    let fingerprint = preview_fingerprint(&draft);
    let preview_service = SyncPreviewService::new(PostgresSyncPreviewRepository::new(pool.clone()));
    let stored = preview_service
        .create(CreateSyncPreviewSnapshot {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            draft,
            fingerprint: fingerprint.clone(),
            created_at: CREATED_AT,
            expires_at: EXPIRES_AT,
        })
        .await
        .expect("preview is stored");
    let selected_action_hash = stored.actions[1].action_hash.clone();
    SyncRunService::new(PostgresSyncRunRepository::new(pool.clone()))
        .admit(admission_command(
            &stored.content_hash,
            &selected_action_hash,
            fingerprint,
            CREATED_AT,
        ))
        .await
        .expect("preview action is admitted");

    let owner_id = Uuid::from_u128(40);
    let task_attempt_id = Uuid::from_u128(41);
    let claimed = TaskManager::new(PostgresTaskRepository::new(pool.clone()))
        .claim_next(ClaimNextTask {
            owner_id,
            attempt_id: task_attempt_id,
            accepted_kinds: vec![TaskKind::SyncAction],
            claimed_at: CREATED_AT,
        })
        .await
        .expect("sync action claim succeeds")
        .expect("sync action is claimable by the explicit test worker");
    let attempts =
        SyncActionAttemptService::new(PostgresSyncActionAttemptRepository::new(pool.clone()));
    let prepared = attempts
        .prepare(PrepareSyncActionAttempt {
            claimed: claimed.clone(),
            operation_id: Uuid::from_u128(42),
            prepared_at: CREATED_AT,
        })
        .await
        .expect("semantic request is durably prepared");
    assert_eq!(prepared.request.action_id, ACTION_ID);
    assert_eq!(prepared.request.idempotency_key, ACTION_ID);
    assert_eq!(prepared.request.intended_value, 6);
    assert!(matches!(
        attempts
            .prepare(PrepareSyncActionAttempt {
                claimed,
                operation_id: Uuid::from_u128(43),
                prepared_at: CREATED_AT,
            })
            .await,
        Err(SyncActionAttemptError::AttemptAlreadyPrepared)
    ));
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 1);
    assert!(
        sqlx::query(
            "UPDATE sync_action_attempts
SET state = 'settled', recovery_result = 'indeterminate', settled_at = $1
WHERE task_attempt_id = $2
",
        )
        .bind(CREATED_AT)
        .bind(task_attempt_id)
        .execute(&pool)
        .await
        .is_err(),
        "indeterminate recovery requires a recorded dispatch"
    );
    assert!(
        sqlx::query("DELETE FROM task_attempts WHERE id = $1")
            .bind(task_attempt_id)
            .execute(&pool)
            .await
            .is_err(),
        "durable synchronization evidence restricts task-attempt deletion"
    );
    if dispatched {
        attempts
            .record_dispatch(RecordSyncActionDispatch {
                task_id: TASK_ID,
                task_attempt_id,
                owner_id,
                dispatched_at: CREATED_AT,
            })
            .await
            .expect("dispatch fence is recorded without an adapter call");
    }
    assert_eq!(table_count(&pool, "sync_actions").await, 1);
    assert_eq!(table_count(&pool, "task_attempts").await, 1);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 1);
    assert_eq!(
        table_count(&pool, "sync_action_reconciliation_tasks").await,
        0
    );
    database
        .shutdown()
        .await
        .expect("database stops before restart");

    let host =
        trakkin::host::Host::start(directory.path(), CREATED_AT + time::Duration::seconds(1))
            .await
            .expect("host recovers sync action before startup completes");
    assert_eq!(host.startup_recovery().tasks, 1);
    assert_eq!(host.startup_recovery().task_attempts, 1);
    assert_eq!(
        host.startup_recovery().sync_actions_confirmed_not_applied,
        u64::from(!dispatched)
    );
    assert_eq!(
        host.startup_recovery().sync_actions_indeterminate,
        u64::from(dispatched)
    );
    assert_eq!(
        host.task_manager()
            .get(TASK_ID)
            .await
            .expect("recovered task reloads")
            .expect("recovered task exists")
            .status,
        TaskStatus::Interrupted
    );
    host.shutdown().await.expect("recovered host shuts down");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("recovered database reopens for inspection");
    let pool = database.pool();
    let recovered = sqlx::query_as::<_, (String, Option<String>, bool)>(
        "SELECT state, recovery_result, dispatched_at IS NOT NULL
FROM sync_action_attempts
WHERE task_attempt_id = $1",
    )
    .bind(task_attempt_id)
    .fetch_one(&pool)
    .await
    .expect("recovered attempt is queryable");
    assert_eq!(recovered.0, "settled");
    assert_eq!(recovered.1.as_deref(), Some(expected_result));
    assert_eq!(recovered.2, dispatched);
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("recovered execution status is queryable"),
        if dispatched {
            ("indeterminate".to_owned(), "indeterminate".to_owned())
        } else {
            ("failed".to_owned(), "failed".to_owned())
        }
    );
    assert_eq!(table_count(&pool, "sync_actions").await, 1);
    assert_eq!(table_count(&pool, "task_attempts").await, 1);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 1);
    assert_eq!(
        table_count(&pool, "sync_action_reconciliation_tasks").await,
        i64::from(dispatched)
    );
    if dispatched {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let reads = Arc::new(Mutex::new(Vec::new()));
        let sessions = ReconciliationSessionRegistry::new(ReconciliationRecordingSession {
            writes: Arc::clone(&writes),
            reads: Arc::clone(&reads),
            write_outcomes: VecDeque::new(),
            read_outcomes: VecDeque::from([Ok(matched_reconciliation(true, true))]),
        });
        let reconciled_at = CREATED_AT + time::Duration::seconds(2);
        let reconciled = TaskDispatcher::new(
            TaskManager::new(PostgresTaskRepository::new(pool.clone())),
            SyncActionReconciliationTaskHandler::new(
                PostgresSyncActionReconciliationRepository::new(pool.clone()),
                sessions,
            ),
        )
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(50),
            attempt_id: Uuid::from_u128(51),
            claimed_at: reconciled_at,
            completed_at: reconciled_at,
        })
        .await
        .expect("crashed write reconciliation dispatches")
        .expect("crashed write reconciliation is queued");
        assert_eq!(reconciled.status, TaskStatus::Succeeded);
        assert!(writes.lock().expect("recorded writes lock").is_empty());
        assert_eq!(reads.lock().expect("recorded reads lock").len(), 1);
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT
    reconciliation_attempts.result,
    sync_actions.status,
    sync_runs.status
FROM sync_action_reconciliation_attempts AS reconciliation_attempts
INNER JOIN sync_actions ON sync_actions.id = reconciliation_attempts.action_id
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE reconciliation_attempts.action_id = $1
",
            )
            .bind(ACTION_ID)
            .fetch_one(&pool)
            .await
            .expect("crashed write reconciliation result is queryable"),
            (
                "confirmed_applied".to_owned(),
                "applied".to_owned(),
                "succeeded".to_owned(),
            )
        );
    }
    database
        .shutdown()
        .await
        .expect("inspected database shuts down");

    let restarted =
        trakkin::host::Host::start(directory.path(), CREATED_AT + time::Duration::seconds(3))
            .await
            .expect("second restart is idempotent");
    assert_eq!(restarted.startup_recovery(), &Default::default());
    restarted.shutdown().await.expect("second host shuts down");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("twice-recovered database reopens for inspection");
    let pool = database.pool();
    assert_eq!(table_count(&pool, "sync_actions").await, 1);
    assert_eq!(
        table_count(&pool, "task_attempts").await,
        1 + i64::from(dispatched)
    );
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 1);
    assert_eq!(
        table_count(&pool, "sync_action_reconciliation_attempts").await,
        i64::from(dispatched)
    );
    database
        .shutdown()
        .await
        .expect("final database shuts down");
}

#[tokio::test]
async fn sync_empty_receiver_create_dispatches_one_durable_targeted_write() {
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
            created_at: CREATED_AT,
        })
        .await
        .expect("policy revision is stored");
    let draft = SyncPreviewBuilder::new(StateRegistry::v1())
        .construct(preview_command(policy))
        .expect("preview draft constructs");
    let fingerprint = preview_fingerprint(&draft);
    let preview_service = SyncPreviewService::new(PostgresSyncPreviewRepository::new(pool.clone()));
    let stored = preview_service
        .create(CreateSyncPreviewSnapshot {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            draft,
            fingerprint: fingerprint.clone(),
            created_at: CREATED_AT,
            expires_at: EXPIRES_AT,
        })
        .await
        .expect("preview is stored");
    let selected_action_hash = stored.actions[1].action_hash.clone();
    SyncRunService::new(PostgresSyncRunRepository::new(pool.clone()))
        .admit(admission_command(
            &stored.content_hash,
            &selected_action_hash,
            fingerprint,
            CREATED_AT,
        ))
        .await
        .expect("preview action is admitted");

    let writes = Arc::new(Mutex::new(Vec::new()));
    let sessions = RecordingSessionRegistry::new(RecordingSyncSession {
        writes: Arc::clone(&writes),
        provider_mutations: Arc::new(Mutex::new(0)),
        cancelled_operations: Arc::new(Mutex::new(Vec::new())),
        outcomes: VecDeque::from([Some(AdapterTargetedStateWriteOutcome {
            status: AdapterTargetedStateWriteStatus::Applied,
            certainty: AdapterTargetedStateWriteCertainty::ConfirmedApplied,
            retry_disposition: AdapterTargetedStateWriteRetryDisposition::NotRetryable,
            membership_effect: AdapterTargetedStateMembershipEffect::Created,
            field_effects: vec![AdapterTargetedStateFieldEffect {
                field: progress_field(),
                effect: AdapterTargetedStateFieldEffectKind::Set,
                value: Some(StateValue::Integer(6)),
            }],
            provider_revision: b"receiver-proof-b-r2".to_vec(),
            successor_precondition: Some(b"receiver-proof-b-precondition-r2".to_vec()),
            receipt: b"created-receiver-membership".to_vec(),
            error: None,
        })]),
    });
    let tasks = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    let dispatcher = TaskDispatcher::new(
        tasks.clone(),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions.clone(),
        ),
    );
    let completed = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(41),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("sync action dispatch succeeds")
        .expect("one sync action is claimable");
    assert_eq!(completed.status, TaskStatus::Succeeded);
    assert!(
        dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id: Uuid::from_u128(40),
                attempt_id: Uuid::from_u128(42),
                claimed_at: CREATED_AT,
                completed_at: CREATED_AT,
            })
            .await
            .expect("second dispatch checks cleanly")
            .is_none(),
        "a terminal receiver task cannot issue a duplicate write"
    );
    {
        let writes = writes.lock().expect("recorded writes lock");
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].idempotency_key, ACTION_ID.as_bytes());
        assert_eq!(
            writes[0].expected_membership,
            AdapterSourceMembership::Absent
        );
        assert!(writes[0].allow_create_membership);
        assert_eq!(writes[0].source_key.value, b"receiver-b");
    }

    let attempt = sqlx::query_as::<_, (String, String, String, String, String, i64)>(
        "SELECT
    state,
    outcome_status,
    outcome_certainty,
    outcome_retry_disposition,
    outcome_membership_effect,
    (SELECT COUNT(*) FROM sync_action_attempt_field_effects AS effect
     WHERE effect.task_attempt_id = sync_action_attempts.task_attempt_id)
FROM sync_action_attempts
",
    )
    .fetch_one(&pool)
    .await
    .expect("settled attempt is queryable");
    assert_eq!(
        attempt,
        (
            "settled".to_owned(),
            "applied".to_owned(),
            "confirmed_applied".to_owned(),
            "not_retryable".to_owned(),
            "created".to_owned(),
            1,
        )
    );
    assert!(
        sqlx::query(
            "UPDATE sync_action_attempts
SET outcome_certainty = NULL
WHERE task_attempt_id = $1
",
        )
        .bind(Uuid::from_u128(41))
        .execute(&pool)
        .await
        .is_err(),
        "a settled provider outcome requires a complete closed tuple"
    );
    let execution_status = sqlx::query_as::<_, (String, String)>(
        "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
    )
    .bind(ACTION_ID)
    .fetch_one(&pool)
    .await
    .expect("terminal execution status is queryable");
    assert_eq!(
        execution_status,
        ("applied".to_owned(), "succeeded".to_owned())
    );
    assert_eq!(table_count(&pool, "sync_actions").await, 1);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 1);
    assert_eq!(table_count(&pool, "sync_action_provenance").await, 1);
    assert_eq!(table_count(&pool, "task_attempts").await, 1);
    let provenance = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT action_id, terminal_task_attempt_id
FROM sync_action_provenance
",
    )
    .fetch_one(&pool)
    .await
    .expect("terminal action provenance is queryable");
    assert_eq!(provenance, (ACTION_ID, Uuid::from_u128(41)));
    assert!(
        sqlx::query(
            "UPDATE sync_action_provenance
SET recorded_at = recorded_at + INTERVAL '1 second'
WHERE action_id = $1
",
        )
        .bind(ACTION_ID)
        .execute(&pool)
        .await
        .is_err(),
        "terminal action provenance is immutable"
    );
    let provenance =
        SyncActionProvenanceService::new(PostgresSyncActionProvenanceRepository::new(pool.clone()))
            .get(WORKSPACE_ID, ACTION_ID)
            .await
            .expect("complete terminal action provenance loads");
    assert_eq!(provenance.policy_id, POLICY_ID);
    assert_eq!(provenance.policy_revision, 1);
    assert_eq!(provenance.policy_content_hash, stored.policy_content_hash);
    assert_eq!(provenance.state_registry_digest, "sha256:state-registry-v1");
    assert_eq!(provenance.preview_id, PREVIEW_ID);
    assert_eq!(provenance.preview_hash, stored.content_hash);
    assert_eq!(provenance.preview_action_hash, selected_action_hash);
    assert_eq!(provenance.run_id, RUN_ID);
    assert_eq!(provenance.action_id, ACTION_ID);
    assert_eq!(
        provenance.preview_action.reducer.as_str(),
        "coverage_contiguous_prefix"
    );
    assert!(!provenance.preview_action.contributors.is_empty());
    assert!(
        provenance
            .preview_action
            .contributors
            .iter()
            .all(|contributor| {
                contributor.presence == PublishedStatePresence::Present
                    && contributor.state_publication_id != Uuid::nil()
            })
    );
    assert!(!provenance.preview_action.mapping_proofs.is_empty());
    assert!(
        provenance
            .preview_action
            .contributors
            .iter()
            .all(|contributor| {
                provenance
                    .preview_action
                    .mapping_proofs
                    .iter()
                    .any(|proof| proof.key == contributor.mapping_proof_key)
            })
    );
    assert_eq!(provenance.preview_action.translation.intended_value, 6);
    assert_eq!(provenance.attempt.request.action_id, ACTION_ID);
    assert_eq!(
        provenance
            .attempt
            .outcome
            .as_ref()
            .expect("provider outcome")
            .membership_effect,
        AdapterTargetedStateMembershipEffect::Created
    );
    assert_eq!(
        provenance
            .attempt
            .outcome
            .as_ref()
            .expect("provider outcome")
            .receipt,
        b"created-receiver-membership"
    );
    assert_eq!(
        provenance.receipt_edge(),
        Some(b"created-receiver-membership".as_slice())
    );
    assert!(provenance.reconciliation_edge().is_none());
    let forbidden_columns = sqlx::query_scalar::<_, String>(
        "SELECT column_name::TEXT
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name LIKE 'sync_%'
  AND (
      column_name LIKE '%credential%'
      OR column_name LIKE '%launch_token%'
      OR column_name LIKE '%protobuf%'
      OR column_name LIKE '%raw_provider_payload%'
      OR column_name LIKE '%canonical_media%'
  )
",
    )
    .fetch_all(&pool)
    .await
    .expect("synchronization evidence columns are inspectable");
    assert!(forbidden_columns.is_empty());
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_precondition_failure_records_no_write_and_isolates_receiver() {
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
            created_at: CREATED_AT,
        })
        .await
        .expect("policy revision is stored");
    let draft = SyncPreviewBuilder::new(StateRegistry::v1())
        .construct(preview_command(policy))
        .expect("preview draft constructs");
    let fingerprint = preview_fingerprint(&draft);
    let preview_service = SyncPreviewService::new(PostgresSyncPreviewRepository::new(pool.clone()));
    let stored = preview_service
        .create(CreateSyncPreviewSnapshot {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            draft,
            fingerprint: fingerprint.clone(),
            created_at: CREATED_AT,
            expires_at: EXPIRES_AT,
        })
        .await
        .expect("preview is stored");
    let selected_actions = stored
        .actions
        .iter()
        .enumerate()
        .map(|(ordinal, action)| SyncActionAdmission {
            action_hash: action.action_hash.clone(),
            action_id: if action.receiver_source_id == RECEIVER_B_ID {
                ACTION_ID
            } else {
                Uuid::from_u128(34)
            },
            task_id: if action.receiver_source_id == RECEIVER_B_ID {
                TASK_ID
            } else {
                Uuid::from_u128(35 + ordinal as u128)
            },
        })
        .collect();
    SyncRunService::new(PostgresSyncRunRepository::new(pool.clone()))
        .admit(AdmitSyncPreview {
            run_id: RUN_ID,
            schedule_occurrence_id: None,
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            preview_hash: stored.content_hash,
            current_fingerprint: fingerprint,
            actions: selected_actions,
            admitted_at: CREATED_AT,
        })
        .await
        .expect("both receiver actions are admitted");

    let writes = Arc::new(Mutex::new(Vec::new()));
    let provider_mutations = Arc::new(Mutex::new(0));
    let sessions = RecordingSessionRegistry::new(RecordingSyncSession {
        writes: Arc::clone(&writes),
        provider_mutations: Arc::clone(&provider_mutations),
        cancelled_operations: Arc::new(Mutex::new(Vec::new())),
        outcomes: VecDeque::from([Some(AdapterTargetedStateWriteOutcome {
            status: AdapterTargetedStateWriteStatus::Rejected,
            certainty: AdapterTargetedStateWriteCertainty::ConfirmedNotApplied,
            retry_disposition: AdapterTargetedStateWriteRetryDisposition::NotRetryable,
            membership_effect: AdapterTargetedStateMembershipEffect::Unchanged,
            field_effects: Vec::new(),
            provider_revision: Vec::new(),
            successor_precondition: None,
            receipt: Vec::new(),
            error: Some(AdapterCallError {
                code: "precondition_failed".to_owned(),
                safe_message: "receiver state changed after preview".to_owned(),
                retryable: false,
                retry_after: None,
                field_problems: Vec::new(),
            }),
        })]),
    });
    let tasks = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    let dispatcher = TaskDispatcher::new(
        tasks.clone(),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions,
        ),
    );
    let completed = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(41),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("precondition rejection is a recorded terminal outcome")
        .expect("the receiver action is claimable");
    assert_eq!(completed.id, TASK_ID);
    assert_eq!(completed.status, TaskStatus::Failed);
    assert_eq!(
        completed.failure.expect("task failure is retained").code,
        "precondition_failed"
    );
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(
        *provider_mutations.lock().expect("provider mutations lock"),
        0
    );

    let attempt = sqlx::query_as::<_, (String, String, String, bool)>(
        "SELECT
    outcome_status,
    outcome_certainty,
    outcome_error_code,
    outcome_error_retryable
FROM sync_action_attempts
WHERE action_id = $1
",
    )
    .bind(ACTION_ID)
    .fetch_one(&pool)
    .await
    .expect("precondition outcome is queryable");
    assert_eq!(
        attempt,
        (
            "rejected".to_owned(),
            "confirmed_not_applied".to_owned(),
            "precondition_failed".to_owned(),
            false,
        )
    );
    let action_statuses = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT id, status FROM sync_actions ORDER BY id
",
    )
    .fetch_all(&pool)
    .await
    .expect("receiver action statuses are queryable");
    assert_eq!(
        action_statuses,
        vec![
            (ACTION_ID, "precondition_failed".to_owned()),
            (Uuid::from_u128(34), "queued".to_owned()),
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM sync_runs WHERE id = $1")
            .bind(RUN_ID)
            .fetch_one(&pool)
            .await
            .expect("run status is queryable"),
        "running"
    );
    assert_eq!(table_count(&pool, "sync_previews").await, 1);
    assert_eq!(
        tasks
            .request_cancellation(RequestTaskCancellation {
                task_id: Uuid::from_u128(35),
                requested_at: CREATED_AT + time::Duration::seconds(1),
            })
            .await
            .expect("independent queued receiver can be cancelled"),
        CancellationDisposition::CancelledBeforeStart
    );
    let cancelled_statuses = sqlx::query_as::<_, (String, String)>(
        "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
    )
    .bind(Uuid::from_u128(34))
    .fetch_one(&pool)
    .await
    .expect("cancelled receiver status is queryable");
    assert_eq!(
        cancelled_statuses,
        ("cancelled".to_owned(), "failed".to_owned())
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_running_cancellation_before_dispatch_records_confirmed_nonapplication() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let tasks = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    let owner_id = Uuid::from_u128(40);
    let task_attempt_id = Uuid::from_u128(41);
    let claimed = tasks
        .claim_next(ClaimNextTask {
            owner_id,
            attempt_id: task_attempt_id,
            accepted_kinds: vec![TaskKind::SyncAction],
            claimed_at: CREATED_AT,
        })
        .await
        .expect("sync action claim succeeds")
        .expect("sync action is claimable");
    assert_eq!(
        tasks
            .request_cancellation(RequestTaskCancellation {
                task_id: TASK_ID,
                requested_at: CREATED_AT,
            })
            .await
            .expect("running cancellation is requested"),
        CancellationDisposition::CancellationRequested
    );

    let writes = Arc::new(Mutex::new(Vec::new()));
    let provider_mutations = Arc::new(Mutex::new(0));
    let sessions = RecordingSessionRegistry::new(RecordingSyncSession {
        writes: Arc::clone(&writes),
        provider_mutations: Arc::clone(&provider_mutations),
        cancelled_operations: Arc::new(Mutex::new(Vec::new())),
        outcomes: VecDeque::from([Some(applied_outcome())]),
    });
    let handler = SyncActionTaskHandler::new(
        PostgresSyncActionAttemptRepository::new(pool.clone()),
        sessions,
    );
    assert_eq!(
        handler
            .execute(&claimed, CREATED_AT)
            .await
            .expect("pre-dispatch cancellation terminalizes cleanly"),
        TaskHandlerOutcome::Terminalized
    );
    assert!(writes.lock().expect("recorded writes lock").is_empty());
    assert_eq!(
        *provider_mutations.lock().expect("provider mutations lock"),
        0
    );
    assert_eq!(
        tasks
            .get(TASK_ID)
            .await
            .expect("cancelled task reloads")
            .expect("cancelled task exists")
            .status,
        TaskStatus::Cancelled
    );
    let attempt = sqlx::query_as::<_, (String, String, bool)>(
        "SELECT state, recovery_result, dispatched_at IS NOT NULL
FROM sync_action_attempts
WHERE task_attempt_id = $1
",
    )
    .bind(task_attempt_id)
    .fetch_one(&pool)
    .await
    .expect("cancelled attempt is queryable");
    assert_eq!(
        attempt,
        (
            "settled".to_owned(),
            "confirmed_not_applied".to_owned(),
            false,
        )
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("cancelled execution status is queryable"),
        ("cancelled".to_owned(), "cancelled".to_owned())
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_queued_cancellation_records_no_attempt() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let tasks = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    assert_eq!(
        tasks
            .request_cancellation(RequestTaskCancellation {
                task_id: TASK_ID,
                requested_at: CREATED_AT,
            })
            .await
            .expect("queued receiver cancellation succeeds"),
        CancellationDisposition::CancelledBeforeStart
    );
    assert_eq!(table_count(&pool, "task_attempts").await, 0);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 0);
    assert_eq!(table_count(&pool, "task_claims").await, 0);
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("queued cancellation status is queryable"),
        ("cancelled".to_owned(), "cancelled".to_owned())
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_running_cancellation_after_dispatch_records_indeterminate() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let tasks = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    let writes = Arc::new(Mutex::new(Vec::new()));
    let provider_mutations = Arc::new(Mutex::new(0));
    let cancelled_operations = Arc::new(Mutex::new(Vec::new()));
    let sessions = RecordingSessionRegistry::new(RecordingSyncSession {
        writes: Arc::clone(&writes),
        provider_mutations: Arc::clone(&provider_mutations),
        cancelled_operations: Arc::clone(&cancelled_operations),
        outcomes: VecDeque::from([None]),
    });
    let dispatcher = TaskDispatcher::new(
        tasks.clone(),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions.clone(),
        )
        .with_cancellation_poll_interval(Duration::from_millis(1)),
    );
    let cancellation = async {
        loop {
            if !writes.lock().expect("recorded writes lock").is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        tasks
            .request_cancellation(RequestTaskCancellation {
                task_id: TASK_ID,
                requested_at: CREATED_AT,
            })
            .await
            .expect("post-dispatch cancellation is requested")
    };
    let dispatch = dispatcher.dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(40),
        attempt_id: Uuid::from_u128(41),
        claimed_at: CREATED_AT,
        completed_at: CREATED_AT,
    });
    let (completed, cancellation) = tokio::join!(dispatch, cancellation);
    assert_eq!(cancellation, CancellationDisposition::CancellationRequested);
    let completed = completed
        .expect("post-dispatch cancellation settles cleanly")
        .expect("sync action is claimable");
    assert_eq!(completed.status, TaskStatus::Cancelled);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(
        *provider_mutations.lock().expect("provider mutations lock"),
        0
    );
    let operation_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT operation_id FROM sync_action_attempts WHERE task_attempt_id = $1
",
    )
    .bind(Uuid::from_u128(41))
    .fetch_one(&pool)
    .await
    .expect("dispatched operation id is queryable");
    assert_eq!(
        cancelled_operations
            .lock()
            .expect("cancelled operations lock")
            .as_slice(),
        &[operation_id.as_bytes().to_vec()]
    );
    let attempt = sqlx::query_as::<_, (String, String, bool)>(
        "SELECT state, recovery_result, dispatched_at IS NOT NULL
FROM sync_action_attempts
WHERE task_attempt_id = $1
",
    )
    .bind(Uuid::from_u128(41))
    .fetch_one(&pool)
    .await
    .expect("indeterminate attempt is queryable");
    assert_eq!(
        attempt,
        ("settled".to_owned(), "indeterminate".to_owned(), true,)
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("indeterminate execution status is queryable"),
        ("indeterminate".to_owned(), "indeterminate".to_owned())
    );
    assert!(
        dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id: Uuid::from_u128(40),
                attempt_id: Uuid::from_u128(42),
                claimed_at: CREATED_AT,
                completed_at: CREATED_AT,
            })
            .await
            .expect("duplicate dispatch check succeeds")
            .is_none()
    );
    assert!(
        sessions.remove(CONNECTION_ID).is_some(),
        "successful operation cancellation restores the connection session"
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_valid_provider_outcome_wins_post_dispatch_cancellation() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let tasks = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    let owner_id = Uuid::from_u128(40);
    let task_attempt_id = Uuid::from_u128(41);
    let claimed = tasks
        .claim_next(ClaimNextTask {
            owner_id,
            attempt_id: task_attempt_id,
            accepted_kinds: vec![TaskKind::SyncAction],
            claimed_at: CREATED_AT,
        })
        .await
        .expect("sync action claim succeeds")
        .expect("sync action is claimable");
    let attempts =
        SyncActionAttemptService::new(PostgresSyncActionAttemptRepository::new(pool.clone()));
    let prepared = attempts
        .prepare(PrepareSyncActionAttempt {
            claimed,
            operation_id: Uuid::from_u128(42),
            prepared_at: CREATED_AT,
        })
        .await
        .expect("sync action attempt prepares");
    attempts
        .record_dispatch(RecordSyncActionDispatch {
            task_id: prepared.task_id,
            task_attempt_id: prepared.task_attempt_id,
            owner_id: prepared.owner_id,
            dispatched_at: CREATED_AT,
        })
        .await
        .expect("sync action dispatch is recorded");
    assert_eq!(
        tasks
            .request_cancellation(RequestTaskCancellation {
                task_id: TASK_ID,
                requested_at: CREATED_AT,
            })
            .await
            .expect("post-dispatch cancellation is requested"),
        CancellationDisposition::CancellationRequested
    );
    let outcome = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    let settlement = attempts
        .record_outcome(RecordSyncActionOutcome {
            task_id: prepared.task_id,
            task_attempt_id: prepared.task_attempt_id,
            owner_id: prepared.owner_id,
            completion: sync_action_task_completion(&outcome),
            outcome,
            settled_at: CREATED_AT,
        })
        .await
        .expect("valid provider outcome settles after cancellation");
    assert_eq!(
        settlement.disposition,
        SyncActionOutcomeDisposition::Terminalized
    );
    assert_eq!(
        tasks
            .get(TASK_ID)
            .await
            .expect("cancelled task reloads")
            .expect("cancelled task exists")
            .status,
        TaskStatus::Cancelled
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("provider outcome execution status is queryable"),
        ("failed".to_owned(), "failed".to_owned())
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_invalid_terminal_outcome_records_uncertainty() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let mut invalid_outcome = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    invalid_outcome.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    let sessions = RecordingSessionRegistry::new(RecordingSyncSession {
        writes: Arc::new(Mutex::new(Vec::new())),
        provider_mutations: Arc::new(Mutex::new(0)),
        cancelled_operations: Arc::new(Mutex::new(Vec::new())),
        outcomes: VecDeque::from([Some(invalid_outcome)]),
    });
    let dispatcher = TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions,
        ),
    );
    let completed = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(41),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("invalid outcome settles as uncertainty")
        .expect("sync action is claimable");
    assert_eq!(completed.status, TaskStatus::Failed);
    assert_eq!(table_count(&pool, "task_claims").await, 0);
    assert_eq!(
        sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT
    sync_actions.status,
    sync_runs.status,
    sync_action_attempts.recovery_result,
    sync_action_attempts.outcome_status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
INNER JOIN sync_action_attempts
    ON sync_action_attempts.action_id = sync_actions.id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("uncertain execution status is queryable"),
        (
            "indeterminate".to_owned(),
            "indeterminate".to_owned(),
            "indeterminate".to_owned(),
            None,
        )
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_indeterminate_reconciliation_classifies_all_outcomes_without_rewriting() {
    assert_reconciliation_case(
        "confirmed_applied",
        matched_reconciliation(true, true),
        "applied",
        "succeeded",
        false,
    )
    .await;
    assert_reconciliation_case(
        "confirmed_not_applied",
        matched_reconciliation(false, false),
        "failed",
        "failed",
        false,
    )
    .await;
    assert_reconciliation_case(
        "state_matches_causation_unknown",
        matched_reconciliation(true, false),
        "indeterminate",
        "indeterminate",
        true,
    )
    .await;
}

async fn assert_reconciliation_case(
    expected_result: &str,
    read_outcome: AdapterTargetedStateReadOutcome,
    expected_action_status: &str,
    expected_run_status: &str,
    assert_uncertainty_fence: bool,
) {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    let preview_draft = admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let mut invalid_outcome = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    invalid_outcome.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let sessions = ReconciliationSessionRegistry::new(ReconciliationRecordingSession {
        writes: Arc::clone(&writes),
        reads: Arc::clone(&reads),
        write_outcomes: VecDeque::from([invalid_outcome]),
        read_outcomes: VecDeque::from([Ok(read_outcome)]),
    });
    let task_manager = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    let write_dispatcher = TaskDispatcher::new(
        task_manager.clone(),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions.clone(),
        ),
    );
    let write_task = write_dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(41),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("uncertain write dispatch completes")
        .expect("sync action is claimable");
    assert_eq!(write_task.status, TaskStatus::Failed);
    assert_eq!(table_count(&pool, "sync_action_provenance").await, 0);

    let reconciliation_dispatcher = TaskDispatcher::new(
        task_manager.clone(),
        SyncActionReconciliationTaskHandler::new(
            PostgresSyncActionReconciliationRepository::new(pool.clone()),
            sessions,
        ),
    );
    let reconciled_at = CREATED_AT + time::Duration::seconds(1);
    let reconciliation_task = reconciliation_dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(42),
            attempt_id: Uuid::from_u128(43),
            claimed_at: reconciled_at,
            completed_at: reconciled_at,
        })
        .await
        .expect("reconciliation dispatch completes")
        .expect("reconciliation task is claimable");
    assert_eq!(reconciliation_task.status, TaskStatus::Succeeded);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    {
        let reads = reads.lock().expect("recorded reads lock");
        assert_eq!(reads.len(), 1);
        assert_eq!(
            reads[0].reconciliation_idempotency_key.as_deref(),
            Some(ACTION_ID.as_bytes().as_slice())
        );
        assert_eq!(reads[0].fields, vec![progress_field()]);
    }
    assert_eq!(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT
    reconciliation_attempts.result,
    sync_actions.status,
    sync_runs.status
FROM sync_action_reconciliation_attempts AS reconciliation_attempts
INNER JOIN sync_actions ON sync_actions.id = reconciliation_attempts.action_id
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE reconciliation_attempts.action_id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("reconciliation result is queryable"),
        (
            expected_result.to_owned(),
            expected_action_status.to_owned(),
            expected_run_status.to_owned(),
        )
    );
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 1);
    assert_eq!(
        table_count(&pool, "sync_action_reconciliation_attempts").await,
        1
    );
    if matches!(
        expected_result,
        "confirmed_applied" | "confirmed_not_applied"
    ) {
        assert_eq!(table_count(&pool, "sync_action_provenance").await, 1);
        let provenance = SyncActionProvenanceService::new(
            PostgresSyncActionProvenanceRepository::new(pool.clone()),
        )
        .get(WORKSPACE_ID, ACTION_ID)
        .await
        .expect("decisive reconciliation provenance loads");
        assert!(provenance.attempt.outcome.is_none());
        assert!(provenance.attempt.requires_reconciliation());
        assert!(provenance.receipt_edge().is_none());
        let reconciliation = provenance
            .reconciliation_edge()
            .expect("reconciliation evidence is present");
        assert_eq!(provenance.attempt.task_attempt_id, Uuid::from_u128(41));
        assert_eq!(reconciliation.task_attempt_id, Uuid::from_u128(43));
        assert_eq!(reconciliation.write_task_attempt_id, Uuid::from_u128(41));
        assert_eq!(reconciliation.action_id, ACTION_ID);
        assert_eq!(reconciliation.idempotency_key, ACTION_ID);
        assert_eq!(
            reconciliation.result,
            if expected_result == "confirmed_applied" {
                SyncActionReconciliationResult::ConfirmedApplied
            } else {
                SyncActionReconciliationResult::ConfirmedNotApplied
            }
        );
        assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
        assert!(
            sqlx::query(
                "UPDATE sync_action_provenance
SET reconciliation_task_attempt_id = NULL
WHERE action_id = $1
",
            )
            .bind(ACTION_ID)
            .execute(&pool)
            .await
            .is_err(),
            "reconciled action provenance is immutable"
        );
    } else {
        assert_eq!(table_count(&pool, "sync_action_provenance").await, 0);
        assert!(matches!(
            SyncActionProvenanceService::new(PostgresSyncActionProvenanceRepository::new(
                pool.clone()
            ))
            .get(WORKSPACE_ID, ACTION_ID)
            .await,
            Err(SyncActionProvenanceError::NotFound)
        ));
    }
    if assert_uncertainty_fence {
        assert_fresh_preview_action_blocked(&pool, preview_draft, reconciled_at).await;
        assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    }
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_reconciliation_retries_missing_session_once_without_rewriting() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let mut invalid_outcome = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    invalid_outcome.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let sessions = ReconciliationSessionRegistry::new(ReconciliationRecordingSession {
        writes: Arc::clone(&writes),
        reads: Arc::clone(&reads),
        write_outcomes: VecDeque::from([invalid_outcome]),
        read_outcomes: VecDeque::from([Ok(matched_reconciliation(true, true))]),
    });
    let task_manager = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    TaskDispatcher::new(
        task_manager.clone(),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions.clone(),
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(40),
        attempt_id: Uuid::from_u128(41),
        claimed_at: CREATED_AT,
        completed_at: CREATED_AT,
    })
    .await
    .expect("uncertain write dispatch completes")
    .expect("sync action is claimable");
    let session = sessions
        .remove(CONNECTION_ID)
        .expect("connection session is removed for the first reconciliation attempt");
    let dispatcher = TaskDispatcher::new(
        task_manager,
        SyncActionReconciliationTaskHandler::new(
            PostgresSyncActionReconciliationRepository::new(pool.clone()),
            sessions.clone(),
        ),
    );
    let first_attempt_at = CREATED_AT + time::Duration::seconds(1);
    let requeued = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(42),
            attempt_id: Uuid::from_u128(43),
            claimed_at: first_attempt_at,
            completed_at: first_attempt_at,
        })
        .await
        .expect("missing session reconciliation is recorded")
        .expect("reconciliation task is claimable");
    assert_eq!(requeued.status, TaskStatus::Queued);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert!(reads.lock().expect("recorded reads lock").is_empty());
    assert!(sessions.replace(CONNECTION_ID, session).is_none());

    let second_attempt_at = CREATED_AT + time::Duration::seconds(2);
    let completed = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(44),
            attempt_id: Uuid::from_u128(45),
            claimed_at: second_attempt_at,
            completed_at: second_attempt_at,
        })
        .await
        .expect("second reconciliation attempt completes")
        .expect("requeued reconciliation task is claimable");
    assert_eq!(completed.status, TaskStatus::Succeeded);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(reads.lock().expect("recorded reads lock").len(), 1);
    assert_eq!(
        sqlx::query_as::<_, (String, String, bool)>(
            "SELECT result, outcome_error_code, outcome_error_retryable
FROM sync_action_reconciliation_attempts
WHERE action_id = $1
ORDER BY prepared_at, task_attempt_id
LIMIT 1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("retryable reconciliation evidence is queryable"),
        (
            "unresolved".to_owned(),
            "connection_session_unavailable".to_owned(),
            true,
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sync_action_reconciliation_attempts
WHERE action_id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("reconciliation attempts are countable"),
        2
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_reconciliation_session_collision_records_retryable_evidence() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let mut invalid_outcome = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    invalid_outcome.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let write_sessions = ReconciliationSessionRegistry::new(ReconciliationRecordingSession {
        writes: Arc::clone(&writes),
        reads: Arc::clone(&reads),
        write_outcomes: VecDeque::from([invalid_outcome]),
        read_outcomes: VecDeque::new(),
    });
    let task_manager = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    TaskDispatcher::new(
        task_manager.clone(),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            write_sessions,
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(40),
        attempt_id: Uuid::from_u128(41),
        claimed_at: CREATED_AT,
        completed_at: CREATED_AT,
    })
    .await
    .expect("uncertain write dispatch completes")
    .expect("sync action is claimable");

    let reconciliation_sessions =
        ReconciliationSessionRegistry::with_restore_collision(ReconciliationRecordingSession {
            writes: Arc::clone(&writes),
            reads: Arc::clone(&reads),
            write_outcomes: VecDeque::new(),
            read_outcomes: VecDeque::from([Ok(matched_reconciliation(true, true))]),
        });
    let reconciled_at = CREATED_AT + time::Duration::seconds(1);
    let dispatcher = TaskDispatcher::new(
        task_manager,
        SyncActionReconciliationTaskHandler::new(
            PostgresSyncActionReconciliationRepository::new(pool.clone()),
            reconciliation_sessions,
        ),
    );
    let task = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(42),
            attempt_id: Uuid::from_u128(43),
            claimed_at: reconciled_at,
            completed_at: reconciled_at,
        })
        .await
        .expect("session collision reconciliation is recorded")
        .expect("reconciliation task is claimable");
    assert_eq!(task.status, TaskStatus::Queued);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(reads.lock().expect("recorded reads lock").len(), 1);
    assert_eq!(
        sqlx::query_as::<_, (String, String, bool)>(
            "SELECT result, outcome_error_code, outcome_error_retryable
FROM sync_action_reconciliation_attempts
WHERE action_id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("session collision evidence is queryable"),
        (
            "unresolved".to_owned(),
            "connection_session_collision".to_owned(),
            true,
        )
    );
    let terminal_at = CREATED_AT + time::Duration::seconds(2);
    let terminal = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(44),
            attempt_id: Uuid::from_u128(45),
            claimed_at: terminal_at,
            completed_at: terminal_at,
        })
        .await
        .expect("second retryable reconciliation is recorded")
        .expect("requeued reconciliation task is claimable");
    assert_eq!(terminal.status, TaskStatus::Failed);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(reads.lock().expect("recorded reads lock").len(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sync_action_reconciliation_attempts
WHERE action_id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("bounded reconciliation attempts are countable"),
        2
    );
    assert!(
        dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id: Uuid::from_u128(46),
                attempt_id: Uuid::from_u128(47),
                claimed_at: terminal_at,
                completed_at: terminal_at,
            })
            .await
            .expect("terminal reconciliation is not claimable")
            .is_none()
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_reconciliation_transport_failure_exhausts_without_rewriting() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let mut invalid_outcome = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    invalid_outcome.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let sessions = ReconciliationSessionRegistry::new(ReconciliationRecordingSession {
        writes: Arc::clone(&writes),
        reads: Arc::clone(&reads),
        write_outcomes: VecDeque::from([invalid_outcome]),
        read_outcomes: VecDeque::from([Err(())]),
    });
    let task_manager = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    TaskDispatcher::new(
        task_manager.clone(),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions.clone(),
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(40),
        attempt_id: Uuid::from_u128(41),
        claimed_at: CREATED_AT,
        completed_at: CREATED_AT,
    })
    .await
    .expect("uncertain write dispatch completes")
    .expect("sync action is claimable");

    let dispatcher = TaskDispatcher::new(
        task_manager,
        SyncActionReconciliationTaskHandler::new(
            PostgresSyncActionReconciliationRepository::new(pool.clone()),
            sessions.clone(),
        ),
    );
    let first_attempt_at = CREATED_AT + time::Duration::seconds(1);
    let requeued = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(42),
            attempt_id: Uuid::from_u128(43),
            claimed_at: first_attempt_at,
            completed_at: first_attempt_at,
        })
        .await
        .expect("transport failure reconciliation is recorded")
        .expect("reconciliation task is claimable");
    assert_eq!(requeued.status, TaskStatus::Queued);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(reads.lock().expect("recorded reads lock").len(), 1);
    assert!(
        sessions
            .replace(
                CONNECTION_ID,
                ReconciliationRecordingSession {
                    writes: Arc::clone(&writes),
                    reads: Arc::clone(&reads),
                    write_outcomes: VecDeque::new(),
                    read_outcomes: VecDeque::from([Err(())]),
                },
            )
            .is_none()
    );

    let second_attempt_at = CREATED_AT + time::Duration::seconds(2);
    let terminal = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(44),
            attempt_id: Uuid::from_u128(45),
            claimed_at: second_attempt_at,
            completed_at: second_attempt_at,
        })
        .await
        .expect("second transport failure reconciliation is recorded")
        .expect("requeued reconciliation task is claimable");
    assert_eq!(terminal.status, TaskStatus::Failed);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(reads.lock().expect("recorded reads lock").len(), 2);
    assert_eq!(
        sqlx::query_as::<_, (String, bool, i64)>(
            "SELECT
    MIN(outcome_error_code),
    BOOL_AND(outcome_error_retryable),
    COUNT(*)
FROM sync_action_reconciliation_attempts
WHERE action_id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("transport failure reconciliation evidence is queryable"),
        (
            "sync_action_reconciliation_transport_failed".to_owned(),
            true,
            2,
        )
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_reconciliation_restart_requeues_prepared_read_without_rewriting() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let mut invalid_outcome = failed_outcome(
        AdapterTargetedStateWriteRetryDisposition::NotRetryable,
        false,
    );
    invalid_outcome.certainty = AdapterTargetedStateWriteCertainty::Unknown;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let sessions = ReconciliationSessionRegistry::new(ReconciliationRecordingSession {
        writes: Arc::clone(&writes),
        reads: Arc::clone(&reads),
        write_outcomes: VecDeque::from([invalid_outcome]),
        read_outcomes: VecDeque::from([Ok(matched_reconciliation(true, true))]),
    });
    let task_manager = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    TaskDispatcher::new(
        task_manager.clone(),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions.clone(),
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(40),
        attempt_id: Uuid::from_u128(41),
        claimed_at: CREATED_AT,
        completed_at: CREATED_AT,
    })
    .await
    .expect("uncertain write dispatch completes")
    .expect("sync action is claimable");
    let prepared_at = CREATED_AT + time::Duration::seconds(1);
    let reconciliation_attempt_id = Uuid::from_u128(43);
    let claimed = task_manager
        .claim_next(ClaimNextTask {
            owner_id: Uuid::from_u128(42),
            attempt_id: reconciliation_attempt_id,
            accepted_kinds: vec![TaskKind::SyncActionReconciliation],
            claimed_at: prepared_at,
        })
        .await
        .expect("reconciliation claim succeeds")
        .expect("reconciliation task is queued");
    let reconciliation_task_id = claimed.task.id;
    SyncActionReconciliationService::new(PostgresSyncActionReconciliationRepository::new(
        pool.clone(),
    ))
    .prepare(PrepareSyncActionReconciliation {
        claimed,
        operation_id: reconciliation_attempt_id,
        prepared_at,
    })
    .await
    .expect("reconciliation read is durably prepared");
    database
        .shutdown()
        .await
        .expect("database stops with prepared reconciliation");

    let recovered_at = CREATED_AT + time::Duration::seconds(2);
    let host = trakkin::host::Host::start(directory.path(), recovered_at)
        .await
        .expect("host recovers prepared reconciliation");
    assert_eq!(host.startup_recovery().tasks, 1);
    assert_eq!(host.startup_recovery().task_attempts, 1);
    assert_eq!(host.startup_recovery().claims, 1);
    assert_eq!(host.startup_recovery().sync_actions_indeterminate, 0);
    assert_eq!(
        host.task_manager()
            .get(reconciliation_task_id)
            .await
            .expect("recovered reconciliation task reloads")
            .expect("recovered reconciliation task exists")
            .status,
        TaskStatus::Queued
    );
    host.shutdown().await.expect("recovered host shuts down");

    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("recovered database reopens");
    let pool = database.pool();
    assert_eq!(
        sqlx::query_as::<_, (String, String, String, String)>(
            "SELECT state, outcome_kind, result, outcome_error_code
FROM sync_action_reconciliation_attempts
WHERE task_attempt_id = $1
",
        )
        .bind(reconciliation_attempt_id)
        .fetch_one(&pool)
        .await
        .expect("interrupted reconciliation evidence is queryable"),
        (
            "settled".to_owned(),
            "indeterminate".to_owned(),
            "unresolved".to_owned(),
            "sync_action_reconciliation_interrupted".to_owned(),
        )
    );
    let completed_at = CREATED_AT + time::Duration::seconds(3);
    let completed = TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionReconciliationTaskHandler::new(
            PostgresSyncActionReconciliationRepository::new(pool.clone()),
            sessions,
        ),
    )
    .dispatch_next(DispatchNextTask {
        owner_id: Uuid::from_u128(44),
        attempt_id: Uuid::from_u128(45),
        claimed_at: completed_at,
        completed_at,
    })
    .await
    .expect("recovered reconciliation dispatches")
    .expect("recovered reconciliation is claimable");
    assert_eq!(completed.status, TaskStatus::Succeeded);
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(reads.lock().expect("recorded reads lock").len(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sync_action_reconciliation_attempts
WHERE action_id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("reconciliation attempts are countable"),
        2
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_partial_failure_retains_applied_receiver_without_rollback() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions_on_distinct_connections(
        &pool,
        &[
            (RECEIVER_A_ID, ACTION_ID, TASK_ID),
            (RECEIVER_B_ID, SECOND_ACTION_ID, SECOND_TASK_ID),
        ],
    )
    .await;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let provider_mutations = Arc::new(Mutex::new(0));
    let cancelled_operations = Arc::new(Mutex::new(Vec::new()));
    let sessions = MultiConnectionSessionRegistry::new([
        (
            CONNECTION_ID,
            RecordingSyncSession {
                writes: Arc::clone(&writes),
                provider_mutations: Arc::clone(&provider_mutations),
                cancelled_operations: Arc::clone(&cancelled_operations),
                outcomes: VecDeque::from([Some(applied_outcome())]),
            },
        ),
        (
            SECOND_CONNECTION_ID,
            RecordingSyncSession {
                writes: Arc::clone(&writes),
                provider_mutations: Arc::clone(&provider_mutations),
                cancelled_operations: Arc::clone(&cancelled_operations),
                outcomes: VecDeque::from([Some(failed_outcome(
                    AdapterTargetedStateWriteRetryDisposition::NotRetryable,
                    false,
                ))]),
            },
        ),
    ]);
    let dispatcher = TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions,
        ),
    );

    for (ordinal, expected_status) in [(0_u128, TaskStatus::Succeeded), (1, TaskStatus::Failed)] {
        let completed_at = CREATED_AT + time::Duration::seconds(ordinal as i64);
        let task = dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id: Uuid::from_u128(40 + ordinal),
                attempt_id: Uuid::from_u128(42 + ordinal),
                claimed_at: completed_at,
                completed_at,
            })
            .await
            .expect("receiver action dispatch completes")
            .expect("receiver action is claimable");
        assert_eq!(task.status, expected_status);
    }
    assert!(
        dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id: Uuid::from_u128(44),
                attempt_id: Uuid::from_u128(45),
                claimed_at: CREATED_AT + time::Duration::seconds(2),
                completed_at: CREATED_AT + time::Duration::seconds(2),
            })
            .await
            .expect("completed run has no queued receiver action")
            .is_none()
    );

    let action_statuses = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT receiver_source_id, status
FROM sync_actions
WHERE run_id = $1
",
    )
    .bind(RUN_ID)
    .fetch_all(&pool)
    .await
    .expect("receiver action statuses are queryable")
    .into_iter()
    .collect::<HashMap<_, _>>();
    assert_eq!(action_statuses[&RECEIVER_A_ID], "applied");
    assert_eq!(action_statuses[&RECEIVER_B_ID], "failed");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM sync_runs WHERE id = $1")
            .bind(RUN_ID)
            .fetch_one(&pool)
            .await
            .expect("partial run status is queryable"),
        "partial"
    );
    {
        let writes = writes.lock().expect("recorded writes lock");
        assert_eq!(writes.len(), 2);
        assert_eq!(
            writes
                .iter()
                .map(|write| write.source_key.value.clone())
                .collect::<std::collections::HashSet<_>>(),
            std::collections::HashSet::from([b"receiver-a".to_vec(), b"receiver-b".to_vec(),])
        );
    }
    assert_eq!(
        *provider_mutations.lock().expect("provider mutations lock"),
        1
    );
    assert!(
        cancelled_operations
            .lock()
            .expect("cancelled operations lock")
            .is_empty()
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_action_dispatch_is_connection_serialized_and_globally_bounded() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_concurrent_receiver_actions(
        &pool,
        &[
            (RECEIVER_A_ID, ACTION_ID, TASK_ID),
            (RECEIVER_B_ID, SECOND_ACTION_ID, SECOND_TASK_ID),
            (RECEIVER_C_ID, THIRD_ACTION_ID, THIRD_TASK_ID),
        ],
    )
    .await;

    let probe = Arc::new(WriteConcurrencyProbe::default());
    let release_a = Arc::new(Semaphore::new(0));
    let release_b = Arc::new(Semaphore::new(0));
    let release_c = Arc::new(Semaphore::new(0));
    let sessions = MultiConnectionSessionRegistry::new([
        (
            CONNECTION_ID,
            ConcurrencyRecordingSession {
                probe: Arc::clone(&probe),
                writes: VecDeque::from([
                    (Arc::clone(&release_a), applied_outcome()),
                    (Arc::clone(&release_b), applied_outcome()),
                ]),
            },
        ),
        (
            SECOND_CONNECTION_ID,
            ConcurrencyRecordingSession {
                probe: Arc::clone(&probe),
                writes: VecDeque::from([(Arc::clone(&release_c), applied_outcome())]),
            },
        ),
    ]);
    let first_worker = spawn_sync_dispatch(
        pool.clone(),
        sessions.clone(),
        Uuid::from_u128(50),
        Uuid::from_u128(51),
    );
    let second_worker = spawn_sync_dispatch(
        pool.clone(),
        sessions.clone(),
        Uuid::from_u128(52),
        Uuid::from_u128(53),
    );
    probe.wait_for_active(2).await;
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 2);
    assert_eq!(
        probe.started_sources(),
        [b"receiver-a".to_vec(), b"receiver-c".to_vec()]
            .into_iter()
            .collect()
    );

    release_a.add_permits(1);
    wait_for_task_status(&pool, TASK_ID, "succeeded").await;
    let third_worker = spawn_sync_dispatch(
        pool.clone(),
        sessions,
        Uuid::from_u128(54),
        Uuid::from_u128(55),
    );
    probe.wait_for_started_source(b"receiver-b").await;
    assert_eq!(probe.active.load(Ordering::SeqCst), 2);
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 2);
    assert!(probe.finished_before_start(b"receiver-a", b"receiver-b"));

    release_b.add_permits(1);
    release_c.add_permits(1);
    for worker in [first_worker, second_worker, third_worker] {
        let task = worker.await.expect("sync worker joins");
        assert_eq!(task.status, TaskStatus::Succeeded);
    }
    assert_eq!(probe.maximum_active.load(Ordering::SeqCst), 2);
    assert_eq!(probe.started_sources().len(), 3);
    assert_eq!(probe.finished_sources().len(), 3);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM sync_runs WHERE id = $1")
            .bind(RUN_ID)
            .fetch_one(&pool)
            .await
            .expect("concurrent run status is queryable"),
        "succeeded"
    );
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 3);
    assert_eq!(table_count(&pool, "task_claims").await, 0);
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_safe_same_key_failure_appends_attempt_and_retries_once() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let sessions = RecordingSessionRegistry::new(RecordingSyncSession {
        writes: Arc::clone(&writes),
        provider_mutations: Arc::new(Mutex::new(0)),
        cancelled_operations: Arc::new(Mutex::new(Vec::new())),
        outcomes: VecDeque::from([
            Some(failed_outcome(
                AdapterTargetedStateWriteRetryDisposition::SafeSameKey,
                true,
            )),
            Some(applied_outcome()),
        ]),
    });
    let tasks = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    let dispatcher = TaskDispatcher::new(
        tasks,
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions,
        ),
    );
    let first = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(41),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("retryable failure is recorded")
        .expect("first sync action attempt is claimable");
    assert_eq!(first.status, TaskStatus::Queued);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 1);
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("retry-pending execution status is queryable"),
        ("retryable_failure".to_owned(), "running".to_owned())
    );
    let second = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(42),
            claimed_at: CREATED_AT + time::Duration::seconds(1),
            completed_at: CREATED_AT + time::Duration::seconds(1),
        })
        .await
        .expect("safe same-key retry succeeds")
        .expect("requeued sync action is claimable");
    assert_eq!(second.status, TaskStatus::Succeeded);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 2);
    let attempts = sqlx::query_as::<_, (i32, String, Uuid, Uuid)>(
        "SELECT
    task_attempts.ordinal,
    sync_action_attempts.outcome_status,
    sync_action_attempts.operation_id,
    sync_action_attempts.idempotency_key
FROM sync_action_attempts
INNER JOIN task_attempts
    ON task_attempts.id = sync_action_attempts.task_attempt_id
ORDER BY task_attempts.ordinal
",
    )
    .fetch_all(&pool)
    .await
    .expect("retry attempts are queryable");
    assert_eq!(attempts[0].0, 0);
    assert_eq!(attempts[0].1, "failed");
    assert_eq!(attempts[1].0, 1);
    assert_eq!(attempts[1].1, "applied");
    assert_ne!(attempts[0].2, attempts[1].2);
    assert!(attempts.iter().all(|attempt| attempt.3 == ACTION_ID));
    {
        let writes = writes.lock().expect("recorded writes lock");
        assert_eq!(writes.len(), 2);
        assert!(
            writes
                .iter()
                .all(|write| write.idempotency_key == ACTION_ID.as_bytes())
        );
    }
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("retried execution status is queryable"),
        ("applied".to_owned(), "succeeded".to_owned())
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_safe_same_key_retry_stops_at_attempt_bound() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let sessions = RecordingSessionRegistry::new(RecordingSyncSession {
        writes: Arc::clone(&writes),
        provider_mutations: Arc::new(Mutex::new(0)),
        cancelled_operations: Arc::new(Mutex::new(Vec::new())),
        outcomes: VecDeque::from([
            Some(failed_outcome(
                AdapterTargetedStateWriteRetryDisposition::SafeSameKey,
                true,
            )),
            Some(failed_outcome(
                AdapterTargetedStateWriteRetryDisposition::SafeSameKey,
                true,
            )),
        ]),
    });
    let dispatcher = TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions,
        ),
    );
    let first = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(41),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("first safe failure is recorded")
        .expect("first bounded attempt is claimable");
    assert_eq!(first.status, TaskStatus::Queued);
    let second = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(42),
            claimed_at: CREATED_AT + time::Duration::seconds(1),
            completed_at: CREATED_AT + time::Duration::seconds(1),
        })
        .await
        .expect("second safe failure is recorded")
        .expect("second bounded attempt is claimable");
    assert_eq!(second.status, TaskStatus::Failed);
    assert!(
        dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id: Uuid::from_u128(40),
                attempt_id: Uuid::from_u128(43),
                claimed_at: CREATED_AT + time::Duration::seconds(2),
                completed_at: CREATED_AT + time::Duration::seconds(2),
            })
            .await
            .expect("retry bound check succeeds")
            .is_none()
    );
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 2);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 2);
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("bounded failure status is queryable"),
        ("failed".to_owned(), "failed".to_owned())
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_fatal_failure_terminalizes_without_retry() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let writes = Arc::new(Mutex::new(Vec::new()));
    let sessions = RecordingSessionRegistry::new(RecordingSyncSession {
        writes: Arc::clone(&writes),
        provider_mutations: Arc::new(Mutex::new(0)),
        cancelled_operations: Arc::new(Mutex::new(Vec::new())),
        outcomes: VecDeque::from([Some(failed_outcome(
            AdapterTargetedStateWriteRetryDisposition::NotRetryable,
            false,
        ))]),
    });
    let dispatcher = TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions,
        ),
    );
    let completed = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(41),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("fatal failure is recorded")
        .expect("fatal sync action is claimable");
    assert_eq!(completed.status, TaskStatus::Failed);
    assert!(
        !completed
            .failure
            .expect("fatal task failure exists")
            .retryable
    );
    assert!(
        dispatcher
            .dispatch_next(DispatchNextTask {
                owner_id: Uuid::from_u128(40),
                attempt_id: Uuid::from_u128(42),
                claimed_at: CREATED_AT,
                completed_at: CREATED_AT,
            })
            .await
            .expect("fatal retry check succeeds")
            .is_none()
    );
    assert_eq!(writes.lock().expect("recorded writes lock").len(), 1);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 1);
    assert_eq!(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT
    sync_actions.status,
    sync_runs.status,
    sync_action_attempts.outcome_retry_disposition
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
INNER JOIN sync_action_attempts
    ON sync_action_attempts.action_id = sync_actions.id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("fatal execution status is queryable"),
        (
            "failed".to_owned(),
            "failed".to_owned(),
            "not_retryable".to_owned(),
        )
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn sync_missing_session_terminalizes_execution_consistently() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    admit_receiver_actions(&pool, &[(RECEIVER_B_ID, ACTION_ID, TASK_ID)]).await;
    let sessions = RecordingSessionRegistry {
        session: Arc::new(Mutex::new(None)),
    };
    let dispatcher = TaskDispatcher::new(
        TaskManager::new(PostgresTaskRepository::new(pool.clone())),
        SyncActionTaskHandler::new(
            PostgresSyncActionAttemptRepository::new(pool.clone()),
            sessions,
        ),
    );
    let completed = dispatcher
        .dispatch_next(DispatchNextTask {
            owner_id: Uuid::from_u128(40),
            attempt_id: Uuid::from_u128(41),
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("missing session failure is recorded")
        .expect("sessionless sync action is claimable");
    assert_eq!(completed.status, TaskStatus::Failed);
    assert_eq!(
        completed.failure,
        Some(trakkin::application::task_manager::TaskFailure {
            code: "connection_session_unavailable".to_owned(),
            retryable: true,
        })
    );
    assert_eq!(table_count(&pool, "task_attempts").await, 1);
    assert_eq!(table_count(&pool, "sync_action_attempts").await, 0);
    assert_eq!(table_count(&pool, "task_claims").await, 0);
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT sync_actions.status, sync_runs.status
FROM sync_actions
INNER JOIN sync_runs ON sync_runs.id = sync_actions.run_id
WHERE sync_actions.id = $1
",
        )
        .bind(ACTION_ID)
        .fetch_one(&pool)
        .await
        .expect("sessionless execution status is queryable"),
        ("failed".to_owned(), "failed".to_owned())
    );
    database.shutdown().await.expect("database shuts down");
}
async fn assert_fresh_preview_action_blocked(
    pool: &sqlx::PgPool,
    draft: trakkin::application::sync_preview::SyncPreviewDraft,
    admitted_at: OffsetDateTime,
) {
    let preview_id = Uuid::from_u128(44);
    let run_id = Uuid::from_u128(46);
    let action_id = Uuid::from_u128(47);
    let task_id = Uuid::from_u128(48);
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
        .expect("fresh uncertainty-check preview is stored");
    let selected = stored
        .actions
        .iter()
        .find(|action| action.receiver_source_id == RECEIVER_B_ID)
        .expect("fresh preview retains the uncertain receiver target");
    assert!(matches!(
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
            .await,
        Err(SyncRunAdmissionError::ReceiverActionBlocked)
    ));
    assert_eq!(table_count(pool, "sync_runs").await, 1);
    assert_eq!(table_count(pool, "sync_actions").await, 1);
    assert_eq!(table_count(pool, "sync_action_tasks").await, 1);
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum WriteConcurrencyEvent {
    Started(Vec<u8>),
    Finished(Vec<u8>),
}

#[derive(Default)]
struct WriteConcurrencyProbe {
    active: AtomicUsize,
    maximum_active: AtomicUsize,
    events: Mutex<Vec<WriteConcurrencyEvent>>,
    changed: Notify,
}

impl WriteConcurrencyProbe {
    fn started(&self, source_key: Vec<u8>) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum_active.fetch_max(active, Ordering::SeqCst);
        self.events
            .lock()
            .expect("concurrency events lock")
            .push(WriteConcurrencyEvent::Started(source_key));
        self.changed.notify_waiters();
    }

    fn finished(&self, source_key: Vec<u8>) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.events
            .lock()
            .expect("concurrency events lock")
            .push(WriteConcurrencyEvent::Finished(source_key));
        self.changed.notify_waiters();
    }

    async fn wait_for_active(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.active.load(Ordering::SeqCst) != expected {
                self.changed.notified().await;
            }
        })
        .await
        .expect("expected concurrent writes start");
    }

    async fn wait_for_started_source(&self, source_key: &[u8]) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !self
                .events
                .lock()
                .expect("concurrency events lock")
                .contains(&WriteConcurrencyEvent::Started(source_key.to_vec()))
            {
                self.changed.notified().await;
            }
        })
        .await
        .expect("expected receiver write starts");
    }

    fn started_sources(&self) -> std::collections::HashSet<Vec<u8>> {
        self.events
            .lock()
            .expect("concurrency events lock")
            .iter()
            .filter_map(|event| match event {
                WriteConcurrencyEvent::Started(source) => Some(source.clone()),
                WriteConcurrencyEvent::Finished(_) => None,
            })
            .collect()
    }

    fn finished_sources(&self) -> std::collections::HashSet<Vec<u8>> {
        self.events
            .lock()
            .expect("concurrency events lock")
            .iter()
            .filter_map(|event| match event {
                WriteConcurrencyEvent::Started(_) => None,
                WriteConcurrencyEvent::Finished(source) => Some(source.clone()),
            })
            .collect()
    }

    fn finished_before_start(&self, finished_source: &[u8], started_source: &[u8]) -> bool {
        let events = self.events.lock().expect("concurrency events lock");
        let finished = events
            .iter()
            .position(|event| *event == WriteConcurrencyEvent::Finished(finished_source.to_vec()));
        let started = events
            .iter()
            .position(|event| *event == WriteConcurrencyEvent::Started(started_source.to_vec()));
        finished
            .zip(started)
            .is_some_and(|(finished, started)| finished < started)
    }
}

struct ConcurrencyRecordingSession {
    probe: Arc<WriteConcurrencyProbe>,
    writes: VecDeque<(Arc<Semaphore>, AdapterTargetedStateWriteOutcome)>,
}

impl SyncActionAdapterSession for ConcurrencyRecordingSession {
    type Error = std::convert::Infallible;

    fn write_sync_action(
        &mut self,
        command: WriteAdapterTargetedState,
    ) -> trakkin::application::adapter_runtime::AdapterFuture<
        '_,
        AdapterTargetedStateWriteOutcome,
        Self::Error,
    > {
        let probe = Arc::clone(&self.probe);
        let source_key = command.source_key.value;
        let (release, outcome) = self
            .writes
            .pop_front()
            .expect("concurrency write outcome is configured");
        Box::pin(async move {
            probe.started(source_key.clone());
            release
                .acquire()
                .await
                .expect("concurrency release remains open")
                .forget();
            probe.finished(source_key);
            Ok(outcome)
        })
    }

    fn cancel_sync_action(
        &mut self,
        _operation_id: Vec<u8>,
    ) -> trakkin::application::adapter_runtime::AdapterFuture<'_, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn retire_sync_action(
        self,
    ) -> trakkin::application::adapter_runtime::AdapterRetirementFuture<
        AdapterRetirement,
        Self::Error,
    > {
        Box::pin(async { Ok(AdapterRetirement::Graceful) })
    }
}

fn spawn_sync_dispatch(
    pool: sqlx::PgPool,
    sessions: MultiConnectionSessionRegistry<ConcurrencyRecordingSession>,
    owner_id: Uuid,
    attempt_id: Uuid,
) -> tokio::task::JoinHandle<trakkin::application::task_manager::TaskSnapshot> {
    tokio::spawn(async move {
        TaskDispatcher::new(
            TaskManager::new(PostgresTaskRepository::new(pool.clone())),
            SyncActionTaskHandler::new(PostgresSyncActionAttemptRepository::new(pool), sessions),
        )
        .dispatch_next(DispatchNextTask {
            owner_id,
            attempt_id,
            claimed_at: CREATED_AT,
            completed_at: CREATED_AT,
        })
        .await
        .expect("sync dispatch succeeds")
        .expect("sync action is claimable")
    })
}

async fn wait_for_task_status(pool: &sqlx::PgPool, task_id: Uuid, expected: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = sqlx::query_scalar::<_, String>("SELECT status FROM tasks WHERE id = $1")
                .bind(task_id)
                .fetch_one(pool)
                .await
                .expect("sync task status is queryable");
            if status == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sync task reaches expected status");
}
