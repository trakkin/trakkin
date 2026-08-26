const PREVIEW_VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures.sync/v1/preview/vectors.json"
));

#[tokio::test]
async fn sync_workflow_creates_pinned_preview_without_admitting_writes() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    seed_sources(&pool).await;
    let policies = SyncPolicyService::new(PostgresSyncPolicyRepository::new(pool.clone()));
    let policy = policies
        .create(CreateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            document: policy_document(),
            created_at: CREATED_AT,
        })
        .await
        .expect("policy revision is stored");
    let second_policy = policies
        .create(CreateSyncPolicy {
            policy_id: Uuid::from_u128(6),
            workspace_id: WORKSPACE_ID,
            document: policy_document(),
            created_at: CREATED_AT,
        })
        .await
        .expect("second policy revision is stored");
    let first_policy_page = policies
        .list(QuerySyncPolicies {
            workspace_id: WORKSPACE_ID,
            limit: 1,
            after_policy_id: None,
        })
        .await
        .expect("workspace policy heads are listed");
    assert_eq!(first_policy_page.policies, vec![policy.clone()]);
    assert_eq!(first_policy_page.next_cursor, Some(POLICY_ID));
    let second_policy_page = policies
        .list(QuerySyncPolicies {
            workspace_id: WORKSPACE_ID,
            limit: 1,
            after_policy_id: first_policy_page.next_cursor,
        })
        .await
        .expect("workspace policy continuation is listed");
    assert_eq!(second_policy_page.policies, vec![second_policy]);
    assert_eq!(second_policy_page.next_cursor, None);
    let preview_command = preview_command(policy.clone());
    let draft = SyncPreviewBuilder::new(StateRegistry::v1())
        .construct(preview_command.clone())
        .expect("preview draft constructs");
    let fingerprint = preview_fingerprint(&draft);
    let commands = Arc::new(Mutex::new(Vec::new()));
    let workflow = SyncWorkflowService::new(
        PostgresSyncPolicyRepository::new(pool.clone()),
        StaticSyncPreviewInputCollector {
            collected: CollectedSyncPreviewInputs {
                partitions: preview_command.partitions,
                fingerprint: fingerprint.clone(),
            },
            commands: commands.clone(),
        },
        PostgresSyncPreviewRepository::new(pool.clone()),
        StateRegistry::v1(),
    );

    let view = workflow
        .create_workspace_preview(CreateWorkspaceSyncPreview {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            policy_id: POLICY_ID,
            created_at: CREATED_AT,
            expires_at: EXPIRES_AT,
        })
        .await
        .expect("workspace preview is created");
    let preview = SyncPreviewService::new(PostgresSyncPreviewRepository::new(pool.clone()))
        .get(WORKSPACE_ID, PREVIEW_ID)
        .await
        .expect("stored workspace preview is readable");

    assert_eq!(preview.policy_content_hash, policy.content_hash);
    assert_eq!(
        preview.fingerprint_json,
        fingerprint.canonical_json().unwrap()
    );
    assert_eq!(preview.actions.len(), draft.actions.len());
    assert_eq!(
        commands.lock().expect("collector command lock").as_slice(),
        &[CollectSyncPreviewInputs {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            policy,
            preview_time: CREATED_AT,
            preview_expiry: EXPIRES_AT,
        }]
    );
    assert_eq!(view.entries.len(), preview.actions.len());
    assert_eq!(view.counts.proposed as usize, view.entries.len());
    assert_eq!(view.counts.no_op, 0);
    assert_eq!(view.counts.findings, 0);
    assert!(
        view.entries
            .iter()
            .all(|entry| entry.category == SyncPreviewEntryCategory::Proposed)
    );
    assert_eq!(
        view.entries
            .iter()
            .map(|entry| entry.action_hash.as_str())
            .collect::<Vec<_>>(),
        preview
            .actions
            .iter()
            .map(|action| action.action_hash.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        view.entries
            .iter()
            .any(|entry| entry.membership_effect == PreviewMembershipEffect::Create)
    );
    assert!(view.entries.iter().any(|entry| entry.before == Some(0)));

    let execution_side_effects = sqlx::query_scalar::<_, i64>(
        "SELECT
    (SELECT COUNT(*) FROM sync_runs)
    + (SELECT COUNT(*) FROM sync_actions)
    + (SELECT COUNT(*) FROM sync_action_attempts)
",
    )
    .fetch_one(&pool)
    .await
    .expect("workflow side-effect count is readable");
    assert_eq!(execution_side_effects, 0);
}

#[tokio::test]
async fn sync_run_workflow_recollects_original_pins_and_queues_proposed_action() {
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
    let preview_command = preview_command(policy.clone());
    let draft = SyncPreviewBuilder::new(StateRegistry::v1())
        .construct(preview_command)
        .expect("preview draft constructs");
    let fingerprint = preview_fingerprint(&draft);
    let preview_service = SyncPreviewService::new(PostgresSyncPreviewRepository::new(pool.clone()));
    let preview = preview_service
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
    let proposed_action_hashes = preview
        .actions
        .iter()
        .map(|action| action.action_hash.clone())
        .collect::<Vec<_>>();
    let commands = Arc::new(Mutex::new(Vec::new()));
    let workflow = SyncRunWorkflowService::new(
        PostgresSyncPolicyRepository::new(pool.clone()),
        StaticSyncPreviewInputCollector {
            collected: CollectedSyncPreviewInputs {
                partitions: Vec::new(),
                fingerprint,
            },
            commands: commands.clone(),
        },
        PostgresSyncPreviewRepository::new(pool.clone()),
        PostgresSyncRunRepository::new(pool.clone()),
    );

    let run = workflow
        .start_run(StartWorkspaceSyncRun {
            run_id: RUN_ID,
            schedule_occurrence_id: None,
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            preview_hash: preview.content_hash,
            admitted_at: CREATED_AT,
        })
        .await
        .expect("proposed preview action is admitted directly");

    assert_eq!(run.actions.len(), proposed_action_hashes.len());
    assert_eq!(
        run.actions
            .iter()
            .map(|action| action.action_hash.clone())
            .collect::<Vec<_>>(),
        proposed_action_hashes
    );
    assert!(
        run.actions
            .iter()
            .all(|action| action.task.status == TaskStatus::Queued)
    );
    assert_eq!(
        commands.lock().expect("collector command lock").as_slice(),
        &[CollectSyncPreviewInputs {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            policy,
            preview_time: CREATED_AT,
            preview_expiry: EXPIRES_AT,
        }]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sync_actions")
            .fetch_one(&pool)
            .await
            .expect("sync action count is readable"),
        i64::try_from(proposed_action_hashes.len()).unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tasks")
            .fetch_one(&pool)
            .await
            .expect("task count is readable"),
        i64::try_from(proposed_action_hashes.len()).unwrap()
    );
}

#[tokio::test]
async fn postgres_sync_preview_persists_canonical_actions_and_admits_fresh_run() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    seed_sources(&pool).await;

    let policy_service = SyncPolicyService::new(PostgresSyncPolicyRepository::new(pool.clone()));
    let policy = policy_service
        .create(CreateSyncPolicy {
            policy_id: POLICY_ID,
            workspace_id: WORKSPACE_ID,
            document: policy_document(),
            created_at: CREATED_AT,
        })
        .await
        .expect("policy revision is stored");
    let command = preview_command(policy);
    let mut reordered_command = command.clone();
    reordered_command.partitions.reverse();
    for partition in &mut reordered_command.partitions {
        partition.contributors.reverse();
        partition.mapping_proofs.reverse();
        partition.receivers.reverse();
    }
    let builder = SyncPreviewBuilder::new(StateRegistry::v1());
    let draft = builder
        .construct(command)
        .expect("preview draft constructs");
    let mut reordered_draft = builder
        .construct(reordered_command)
        .expect("reordered preview draft constructs");
    reordered_draft.candidates.reverse();
    reordered_draft.actions.reverse();
    for candidate in &mut reordered_draft.candidates {
        candidate.explanation.contributors.reverse();
        candidate.explanation.mapping_proofs.reverse();
    }
    for action in &mut reordered_draft.actions {
        action.aggregation.contributors.reverse();
        action.aggregation.mapping_proofs.reverse();
        action.targeted_state.fields.reverse();
    }

    let expected = SyncPreviewSnapshot::from_draft(CreateSyncPreviewSnapshot {
        preview_id: PREVIEW_ID,
        workspace_id: WORKSPACE_ID,
        fingerprint: preview_fingerprint(&draft),
        draft: draft.clone(),
        created_at: CREATED_AT,
        expires_at: EXPIRES_AT,
    })
    .expect("canonical snapshot constructs");
    let mut submicrosecond_fingerprint = preview_fingerprint(&draft);
    submicrosecond_fingerprint.lookup_expiry += time::Duration::nanoseconds(1);
    assert!(matches!(
        SyncPreviewSnapshot::from_draft(CreateSyncPreviewSnapshot {
            preview_id: Uuid::from_u128(22),
            workspace_id: WORKSPACE_ID,
            fingerprint: submicrosecond_fingerprint,
            draft: draft.clone(),
            created_at: CREATED_AT,
            expires_at: EXPIRES_AT,
        }),
        Err(SyncPreviewSnapshotError::InvalidFingerprint(
            SyncPreviewFingerprintError::UnsupportedTimePrecision {
                field: "lookupExpiry"
            }
        ))
    ));
    let reordered = SyncPreviewSnapshot::from_draft(CreateSyncPreviewSnapshot {
        preview_id: Uuid::from_u128(21),
        workspace_id: WORKSPACE_ID,
        fingerprint: preview_fingerprint(&reordered_draft),
        draft: reordered_draft.clone(),
        created_at: CREATED_AT,
        expires_at: EXPIRES_AT,
    })
    .expect("reordered canonical snapshot constructs");
    assert_eq!(expected.canonical_json, reordered.canonical_json);
    assert_eq!(expected.content_hash, reordered.content_hash);
    assert_eq!(expected.actions, reordered.actions);
    assert_eq!(
        expected
            .actions
            .iter()
            .map(|action| action.ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );

    let repository = PostgresSyncPreviewRepository::new(pool.clone());
    let mut malformed_snapshot = expected.clone();
    malformed_snapshot.fingerprint_json = b"{}".to_vec();
    assert!(matches!(
        repository.create_preview(malformed_snapshot).await,
        Err(SyncPreviewLifecycleError::Repository(
            PostgresSyncPreviewRepositoryError::CorruptPreview
        ))
    ));
    assert_eq!(table_count(&pool, "sync_previews").await, 0);

    let preview_service = SyncPreviewService::new(repository);
    let stored = preview_service
        .create(CreateSyncPreviewSnapshot {
            preview_id: PREVIEW_ID,
            workspace_id: WORKSPACE_ID,
            fingerprint: preview_fingerprint(&draft),
            draft,
            created_at: CREATED_AT,
            expires_at: EXPIRES_AT,
        })
        .await
        .expect("canonical preview is stored");
    assert_eq!(stored, expected);
    assert_eq!(
        preview_service
            .get(WORKSPACE_ID, PREVIEW_ID)
            .await
            .expect("stored preview reloads"),
        stored
    );
    assert!(matches!(
        preview_service.get(OTHER_WORKSPACE_ID, PREVIEW_ID).await,
        Err(SyncPreviewLifecycleError::PreviewNotFound)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>("SELECT canonical_json FROM sync_previews WHERE id = $1")
            .bind(PREVIEW_ID)
            .fetch_one(&pool)
            .await
            .expect("canonical preview bytes are queryable"),
        stored.canonical_json
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT fingerprint_json FROM sync_previews WHERE id = $1"
        )
        .bind(PREVIEW_ID)
        .fetch_one(&pool)
        .await
        .expect("canonical fingerprint bytes are queryable"),
        stored.fingerprint_json
    );
    let current_fingerprint = preview_fingerprint(&reordered_draft);
    assert_eq!(
        preview_service
            .recheck(CheckSyncPreviewFreshness {
                preview_id: PREVIEW_ID,
                workspace_id: WORKSPACE_ID,
                preview_hash: stored.content_hash.clone(),
                current_fingerprint: current_fingerprint.clone(),
                checked_at: CREATED_AT,
            })
            .await
            .expect("unchanged preview rechecks")
            .status,
        SyncPreviewFreshness::Fresh
    );
    for mutation in stale_mutations() {
        let mut changed_fingerprint = current_fingerprint.clone();
        mutate_fingerprint(&mut changed_fingerprint, &mutation);
        assert_eq!(
            preview_service
                .recheck(CheckSyncPreviewFreshness {
                    preview_id: PREVIEW_ID,
                    workspace_id: WORKSPACE_ID,
                    preview_hash: stored.content_hash.clone(),
                    current_fingerprint: changed_fingerprint,
                    checked_at: CREATED_AT,
                })
                .await
                .expect("changed preview rechecks")
                .status,
            SyncPreviewFreshness::Stale,
            "fingerprint mutation {} must be stale",
            mutation,
        );
    }
    let mut malformed_fingerprint = current_fingerprint.clone();
    malformed_fingerprint.source_capability_revision.clear();
    assert_eq!(
        preview_service
            .recheck(CheckSyncPreviewFreshness {
                preview_id: PREVIEW_ID,
                workspace_id: WORKSPACE_ID,
                preview_hash: stored.content_hash.clone(),
                current_fingerprint: malformed_fingerprint,
                checked_at: CREATED_AT,
            })
            .await
            .expect("malformed current fingerprint becomes stale")
            .status,
        SyncPreviewFreshness::Stale
    );
    assert_eq!(
        preview_service
            .recheck(CheckSyncPreviewFreshness {
                preview_id: PREVIEW_ID,
                workspace_id: WORKSPACE_ID,
                preview_hash: stored.content_hash.clone(),
                current_fingerprint: current_fingerprint.clone(),
                checked_at: CREATED_AT + time::Duration::minutes(1),
            })
            .await
            .expect("expired input rechecks")
            .status,
        SyncPreviewFreshness::Stale
    );

    let duplicate = preview_service
        .create(CreateSyncPreviewSnapshot {
            preview_id: Uuid::from_u128(21),
            workspace_id: WORKSPACE_ID,
            fingerprint: preview_fingerprint(&reordered_draft),
            draft: reordered_draft,
            created_at: CREATED_AT,
            expires_at: EXPIRES_AT,
        })
        .await
        .expect_err("equivalent preview content is stored only once");
    assert!(matches!(
        duplicate,
        SyncPreviewLifecycleError::Repository(
            PostgresSyncPreviewRepositoryError::DuplicatePreviewContent
        )
    ));

    let selected_action_hash = stored.actions[1].action_hash.clone();
    let selected_action = stored
        .actions
        .iter()
        .find(|action| action.action_hash == selected_action_hash)
        .expect("selected action remains in the preview");
    assert_eq!(selected_action.receiver_source_id, RECEIVER_B_ID);
    let selected_action_json: serde_json::Value =
        serde_json::from_slice(&selected_action.canonical_json)
            .expect("selected action JSON is valid");
    assert_eq!(selected_action_json["membershipEffect"], "create");

    let run_service = SyncRunService::new(PostgresSyncRunRepository::new(pool.clone()));
    let mut stale_fingerprint = current_fingerprint.clone();
    mutate_fingerprint(&mut stale_fingerprint, "policyRevision");
    assert!(matches!(
        run_service
            .admit(admission_command(
                &stored.content_hash,
                &selected_action_hash,
                stale_fingerprint,
                CREATED_AT,
            ))
            .await,
        Err(SyncRunAdmissionError::PreviewStale)
    ));
    assert_execution_counts(&pool, 0).await;
    let mut malformed_admission_fingerprint = current_fingerprint.clone();
    malformed_admission_fingerprint
        .source_capability_revision
        .clear();
    assert!(matches!(
        run_service
            .admit(admission_command(
                &stored.content_hash,
                &selected_action_hash,
                malformed_admission_fingerprint,
                CREATED_AT,
            ))
            .await,
        Err(SyncRunAdmissionError::PreviewStale)
    ));
    assert_execution_counts(&pool, 0).await;
    assert!(matches!(
        run_service
            .admit(admission_command(
                &stored.content_hash,
                &selected_action_hash,
                current_fingerprint.clone(),
                CREATED_AT + time::Duration::minutes(1),
            ))
            .await,
        Err(SyncRunAdmissionError::PreviewStale)
    ));
    assert_execution_counts(&pool, 0).await;
    assert!(matches!(
        run_service
            .admit(admission_command(
                &format!("sha256:{}", "d".repeat(64)),
                &selected_action_hash,
                current_fingerprint.clone(),
                CREATED_AT,
            ))
            .await,
        Err(SyncRunAdmissionError::PreviewHashMismatch)
    ));
    assert_execution_counts(&pool, 0).await;
    let admitted = run_service
        .admit(admission_command(
            &stored.content_hash,
            &selected_action_hash,
            current_fingerprint.clone(),
            CREATED_AT,
        ))
        .await
        .expect("fresh preview action is admitted");
    assert_eq!(admitted.run_id, RUN_ID);
    assert_eq!(admitted.actions.len(), 1);
    assert_eq!(admitted.actions[0].action_id, ACTION_ID);
    assert_eq!(admitted.actions[0].action_hash, selected_action_hash);
    assert_eq!(admitted.actions[0].idempotency_key, ACTION_ID.to_string());
    assert_eq!(admitted.actions[0].receiver_source_id, RECEIVER_B_ID);
    assert_eq!(admitted.actions[0].task.id, TASK_ID);
    assert_eq!(admitted.actions[0].task.connection_id, CONNECTION_ID);
    assert_execution_counts(&pool, 1).await;
    assert_eq!(table_count(&pool, "task_attempts").await, 0);
    assert_eq!(table_count(&pool, "task_claims").await, 0);

    let task_manager = TaskManager::new(PostgresTaskRepository::new(pool.clone()));
    assert_eq!(
        task_manager
            .get(TASK_ID)
            .await
            .expect("admitted task reloads"),
        Some(admitted.actions[0].task.clone())
    );
    assert!(
        task_manager
            .claim_next(ClaimNextTask {
                owner_id: Uuid::from_u128(34),
                attempt_id: Uuid::from_u128(35),
                accepted_kinds: vec![
                    TaskKind::SourceDiscovery,
                    TaskKind::CatalogRead,
                    TaskKind::StateRead,
                    TaskKind::CatalogReset,
                    TaskKind::StateReset,
                ],
                claimed_at: CREATED_AT,
            })
            .await
            .expect("existing workers inspect their accepted task kinds")
            .is_none()
    );
    assert_eq!(table_count(&pool, "task_attempts").await, 0);
    assert_eq!(table_count(&pool, "task_claims").await, 0);
    assert!(matches!(
        run_service
            .admit(admission_command(
                &stored.content_hash,
                &selected_action_hash,
                current_fingerprint,
                CREATED_AT,
            ))
            .await,
        Err(SyncRunAdmissionError::PreviewAlreadyAdmitted)
    ));
    assert_execution_counts(&pool, 1).await;

    assert_eq!(table_count(&pool, "tasks").await, 1);
    assert_eq!(table_count(&pool, "task_attempts").await, 0);
    assert_eq!(table_count(&pool, "sync_previews").await, 1);
    assert_eq!(table_count(&pool, "sync_preview_actions").await, 2);
    assert!(
        sqlx::query("DELETE FROM sync_previews WHERE id = $1")
            .bind(PREVIEW_ID)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM sync_actions WHERE id = $1")
            .bind(ACTION_ID)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM tasks WHERE id = $1")
            .bind(TASK_ID)
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE sync_previews SET expires_at = expires_at WHERE id = $1")
            .bind(PREVIEW_ID)
            .execute(&pool)
            .await
            .is_err()
    );

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

fn stale_mutations() -> Vec<String> {
    let suite: serde_json::Value =
        serde_json::from_str(PREVIEW_VECTORS).expect("valid preview vectors");
    let vector = suite["vectors"]
        .as_array()
        .expect("preview vectors")
        .iter()
        .find(|vector| vector["id"] == "SYNC-STALE-PREVIEW-001")
        .expect("stale preview vector");
    let mutations = vector["input"]["mutations"]
        .as_array()
        .expect("stale mutations");
    mutations
        .iter()
        .map(|mutation| mutation.as_str().expect("stale mutation name").to_owned())
        .collect()
}

fn mutate_fingerprint(fingerprint: &mut SyncPreviewFingerprint, mutation: &str) {
    match mutation {
        "policyRevision" => fingerprint.policy_revision.push_str("-changed"),
        "stateRegistryDigest" => fingerprint.state_registry_digest.push_str("-changed"),
        "resolverVersion" => fingerprint.resolver_version.push_str("-changed"),
        "activeMappingHashes" => fingerprint
            .active_mapping_hashes
            .push("sha256:mapping-r2".to_owned()),
        "mappingActivationGeneration" => fingerprint.mapping_activation_generation += 1,
        "mappingProofDigest" => fingerprint.mapping_proof_digest.push_str("-changed"),
        "attachmentDigest" => fingerprint.attachment_digest.push_str("-changed"),
        "sourceCapabilityRevision" => fingerprint.source_capability_revision.push_str("-changed"),
        "connectionCapabilityRevision" => fingerprint
            .connection_capability_revision
            .push_str("-changed"),
        "statePublication" => fingerprint.state_publication.push_str("-changed"),
        "contributorEvidenceRevisions" => fingerprint
            .contributor_evidence_revisions
            .push("sha256:state-evidence-r2".to_owned()),
        "catalogPublication" => fingerprint.catalog_publication.push_str("-changed"),
        "coordinatePublication" => fingerprint.coordinate_publication.push_str("-changed"),
        "lookupProcess" => fingerprint.lookup_process.push_str("-changed"),
        "lookupRevision" => fingerprint.lookup_revision.push_str("-changed"),
        "lookupObservedAt" => fingerprint.lookup_observed_at += time::Duration::seconds(1),
        "lookupExpiry" => fingerprint.lookup_expiry += time::Duration::seconds(1),
        "targetedMembership" => {
            fingerprint.targeted_membership = match fingerprint.targeted_membership {
                SyncPreviewMembershipPin::Present => SyncPreviewMembershipPin::Absent,
                SyncPreviewMembershipPin::Absent | SyncPreviewMembershipPin::Unknown => {
                    SyncPreviewMembershipPin::Present
                }
            };
        }
        "targetedStateRevision" => fingerprint.targeted_state_revision.push_str("-changed"),
        "targetedPrecondition" => fingerprint.targeted_precondition.push_str("-changed"),
        "targetedExpiry" => fingerprint.targeted_expiry += time::Duration::seconds(1),
        "previewTime" => fingerprint.preview_time += time::Duration::seconds(1),
        "previewExpiry" => fingerprint.preview_expiry += time::Duration::seconds(1),
        other => panic!("unsupported fingerprint mutation {}", other),
    }
}

async fn assert_execution_counts(pool: &sqlx::PgPool, expected: i64) {
    assert_eq!(table_count(pool, "sync_runs").await, expected);
    assert_eq!(table_count(pool, "sync_actions").await, expected);
    assert_eq!(table_count(pool, "sync_action_tasks").await, expected);
    assert_eq!(table_count(pool, "tasks").await, expected);
}
