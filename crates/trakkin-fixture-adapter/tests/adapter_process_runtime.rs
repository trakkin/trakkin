use trakkin::{
    application::acquisition::ReadMode,
    application::adapter_runtime::{
        AdapterAuthenticationStatus, AdapterEndpointLookupOutcome,
        AdapterEndpointResolutionOperation, AdapterHealthStatus, AdapterKey, AdapterLauncher,
        AdapterLookupOutcome, AdapterReadEvent, AdapterReadStream, AdapterRetirement,
        AdapterSecret, AdapterSession, AdapterSetting, AdapterSourceMembership,
        AdapterStatePresence, AdapterTargetedStateFieldEffectKind,
        AdapterTargetedStateMembershipEffect, AdapterTargetedStateReadOutcome,
        AdapterTargetedStateWriteCapability, AdapterTargetedStateWriteCertainty,
        AdapterTargetedStateWriteIntent, AdapterTargetedStateWriteOperation,
        AdapterTargetedStateWritePreconditionMode, AdapterTargetedStateWriteRetryDisposition,
        AdapterTargetedStateWriteStatus, AdapterValue, ContinueAdapterAuthentication,
        LaunchAdapter, LookupAdapterPortableReferences, OpenAdapterConnection, ReadAdapterAsset,
        ReadAdapterSource, ReadAdapterTargetedState, ResolveAdapterPortableEndpoints,
        StartAdapterAuthentication, ValidateAdapterConnection, WriteAdapterTargetedState,
        endpoint_resolution_operation,
    },
    domain::acquisition::{
        CoordinateBacking, PortableReference, ProviderItemKey, StateField, StateSubject,
        StateValue, Term,
    },
    domain::synchronization::Quantizer,
    infrastructure::adapter_process::{AdapterProcessError, AdapterProcessLauncher},
};

#[tokio::test(flavor = "multi_thread")]
async fn host_runtime_authenticates_opens_discovers_and_retires_fixture_child() {
    let launcher = AdapterProcessLauncher::new(
        env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
        "host-process-runtime-test",
    );
    let mut process = launcher
        .launch(LaunchAdapter {
            process_instance_id: "fixture-host-runtime".to_owned(),
        })
        .await
        .expect("fixture child launches and negotiates");

    assert_eq!(process.identity().adapter_key, "fixture");
    assert_eq!(
        process.identity().process_instance_id,
        "fixture-host-runtime"
    );
    assert_eq!(
        (
            process.identity().protocol_major,
            process.identity().protocol_minor
        ),
        (1, 0)
    );
    assert_eq!(
        process
            .health()
            .await
            .expect("fixture health succeeds")
            .status,
        AdapterHealthStatus::Ready
    );
    assert!(matches!(
        process.discover_sources().await,
        Err(AdapterProcessError::ConnectionNotOpen)
    ));

    let description = process
        .describe_connection()
        .await
        .expect("fixture connection is described");
    assert!(
        description
            .fields
            .iter()
            .any(|field| field.key == "scenario")
    );
    let invalid = process
        .validate_connection(ValidateAdapterConnection {
            settings: Vec::new(),
            secrets: Vec::new(),
        })
        .await
        .expect("invalid fixture connection returns field problems");
    assert!(!invalid.field_problems.is_empty());
    let methods = process
        .list_authentication_methods()
        .await
        .expect("fixture authentication methods are listed");
    assert!(methods.iter().any(|method| method.key == "fixture/device"));
    let input = process
        .start_authentication(StartAdapterAuthentication {
            method: "fixture/device".to_owned(),
            values: Vec::new(),
        })
        .await
        .expect("fixture device authentication starts");
    assert_eq!(input.status, AdapterAuthenticationStatus::InputRequired);
    let completed = process
        .continue_authentication(ContinueAdapterAuthentication {
            authentication_id: input.authentication_id,
            values: vec![AdapterSetting {
                key: "code".to_owned(),
                value: AdapterValue::Text("approved".to_owned()),
            }],
        })
        .await
        .expect("fixture device authentication completes");
    assert_eq!(completed.status, AdapterAuthenticationStatus::Completed);
    let cancellable = process
        .start_authentication(StartAdapterAuthentication {
            method: "fixture/cancel".to_owned(),
            values: Vec::new(),
        })
        .await
        .expect("fixture cancellable authentication starts");
    process
        .cancel_authentication(cancellable.authentication_id)
        .await
        .expect("fixture authentication cancels");

    let opened = process
        .open_connection(OpenAdapterConnection {
            settings: [
                ("scenario", "hierarchical-catalog-assets-v1"),
                ("revision", "v1"),
                ("fault", "none"),
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
        })
        .await
        .expect("fixture connection opens");
    assert_eq!(opened.accounts.len(), 1);
    assert_eq!(opened.accounts[0].display_name, "account.assets.primary");

    let discovered = process
        .discover_sources()
        .await
        .expect("fixture sources are discovered");
    assert!(!discovered.sources.is_empty());
    assert!(
        discovered
            .sources
            .iter()
            .all(|source| source.account_key == opened.accounts[0].key)
    );
    let source_key = discovered
        .sources
        .iter()
        .find(|source| source.key.value == b"source.hierarchy.catalog")
        .expect("fixture publishes the coordinate-bearing hierarchy source")
        .key
        .clone();
    let mut catalog = process
        .read_catalog(ReadAdapterSource {
            operation_id: b"host-catalog-read".to_vec(),
            source_key: source_key.clone(),
            mode: ReadMode::Full,
            prior_cursor: None,
            preferred_batch_size: 100,
        })
        .await
        .expect("fixture catalog read starts");
    let mut catalog_batches = 0;
    let mut coordinate_backings = Vec::new();
    loop {
        match catalog
            .next_event()
            .await
            .expect("fixture catalog event is valid")
        {
            Some(AdapterReadEvent::Batch(batch)) => {
                catalog_batches += 1;
                assert!(!batch.item_upserts().is_empty());
                coordinate_backings.extend(
                    batch
                        .coordinate_binding_upserts()
                        .iter()
                        .map(|binding| binding.backing()),
                );
            }
            Some(AdapterReadEvent::Completed(completed)) => {
                assert!(!completed.next_cursor.is_empty());
                break;
            }
            Some(AdapterReadEvent::Heartbeat { .. }) => {}
            Some(AdapterReadEvent::Failed(error)) => {
                panic!("fixture catalog read failed: {}", error.code)
            }
            Some(AdapterReadEvent::Cancelled) => panic!("fixture catalog read was cancelled"),
            None => panic!("fixture catalog stream ended before completion"),
        }
    }
    assert!(catalog_batches > 0);
    assert!(coordinate_backings.contains(&CoordinateBacking::Materialized));
    assert!(coordinate_backings.contains(&CoordinateBacking::Virtual));
    assert!(
        catalog
            .next_event()
            .await
            .expect("fixture catalog EOF is valid")
            .is_none()
    );

    let mut state = process
        .read_state(ReadAdapterSource {
            operation_id: b"host-state-read".to_vec(),
            source_key: source_key.clone(),
            mode: ReadMode::Full,
            prior_cursor: None,
            preferred_batch_size: 100,
        })
        .await
        .expect("fixture state read starts");
    let mut state_batches = 0;
    loop {
        match state
            .next_event()
            .await
            .expect("fixture state event is valid")
        {
            Some(AdapterReadEvent::Batch(batch)) => {
                state_batches += 1;
                assert!(!batch.observations().is_empty());
            }
            Some(AdapterReadEvent::Completed(completed)) => {
                assert!(!completed.evidence_revision.is_empty());
                break;
            }
            Some(AdapterReadEvent::Heartbeat { .. }) => {}
            Some(AdapterReadEvent::Failed(error)) => {
                panic!("fixture state read failed: {}", error.code)
            }
            Some(AdapterReadEvent::Cancelled) => panic!("fixture state read was cancelled"),
            None => panic!("fixture state stream ended before completion"),
        }
    }
    assert!(state_batches > 0);
    assert!(
        state
            .next_event()
            .await
            .expect("fixture state EOF is valid")
            .is_none()
    );
    let asset_source = discovered
        .sources
        .iter()
        .find(|source| source.capabilities.assets.is_some())
        .expect("fixture advertises an asset source")
        .key
        .clone();
    let asset = process
        .read_asset(ReadAdapterAsset {
            operation_id: b"host-asset-read".to_vec(),
            source_key: asset_source,
            provider_item_key: adapter_key("fixture", "media.asset.primary"),
            asset_key: adapter_key("fixture", "asset.poster.valid"),
            maximum_bytes: 1024,
            allowed_content_types: vec!["image/jpeg".to_owned()],
        })
        .await
        .expect("fixture asset transport succeeds")
        .expect("fixture binary asset is validated");
    assert_eq!(asset.full_length, asset.content.len() as u64);
    assert_eq!(asset.content_type, "image/jpeg");
    assert_eq!(asset.sha256_digest.len(), 32);
    assert_eq!(asset.cache_control, "private, max-age=300");
    let cancellation = process
        .cancel_operation(b"missing-operation".to_vec())
        .await
        .expect_err("fixture reports unknown operation cancellation");
    assert!(matches!(
        cancellation,
        AdapterProcessError::Adapter(error) if error.code == "operation_not_found"
    ));

    assert_eq!(
        process.retire().await.expect("fixture child retires"),
        AdapterRetirement::Graceful
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn host_runtime_resolves_portable_reference_outside_source_membership() {
    let launcher = AdapterProcessLauncher::new(
        env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
        "host-process-lookup-test",
    );
    let mut process = launcher
        .launch(LaunchAdapter {
            process_instance_id: "fixture-host-lookup".to_owned(),
        })
        .await
        .expect("fixture child launches");
    let opened = process
        .open_connection(OpenAdapterConnection {
            settings: [
                ("scenario", "empty-targeted-receiver-v1"),
                ("revision", "v1"),
                ("fault", "none"),
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
        })
        .await
        .expect("empty-receiver fixture opens");
    let requested = PortableReference::new("example.media", b"media/quiet-signal".to_vec())
        .expect("portable reference is valid");
    let results = process
        .lookup_portable_references(LookupAdapterPortableReferences {
            operation_id: b"host-reference-lookup".to_vec(),
            references: vec![requested.clone()],
        })
        .await
        .expect("portable reference lookup transport succeeds")
        .expect("portable reference lookup succeeds");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].requested, requested);
    let AdapterLookupOutcome::Matched(candidate) = &results[0].outcome else {
        panic!("portable reference should match outside source membership")
    };
    assert_eq!(candidate.provider_item.display_name(), "Quiet Signal");
    assert!(!candidate.evidence.adapter_revision.is_empty());

    let endpoint = trakkin_mapping_contract::PortableEndpoint::parse(
        "example.media:media/quiet-signal[segment:1..12]",
    )
    .expect("portable endpoint is valid");
    assert_eq!(
        endpoint_resolution_operation(&opened.capabilities, &endpoint),
        AdapterEndpointResolutionOperation::EndpointLookup
    );
    let results = process
        .resolve_portable_endpoints(ResolveAdapterPortableEndpoints {
            operation_id: b"host-endpoint-lookup".to_vec(),
            endpoints: vec![endpoint.clone()],
            maximum_response_bytes: 4096,
        })
        .await
        .expect("portable endpoint lookup transport succeeds")
        .expect("portable endpoint lookup succeeds");
    assert_eq!(results[0].requested, endpoint);
    let AdapterEndpointLookupOutcome::Matched(candidate) = &results[0].outcome else {
        panic!("portable endpoint should match outside source membership")
    };
    assert_eq!(
        candidate
            .binding
            .as_ref()
            .expect("coordinate binding")
            .backing(),
        CoordinateBacking::Aggregate
    );
    assert!(candidate.evidence.expires_at.is_some());

    let discovered = process
        .discover_sources()
        .await
        .expect("empty-receiver sources are discovered");
    let receiver = discovered
        .sources
        .into_iter()
        .find(|source| source.key.value == b"source.receiver.empty")
        .expect("receiving list is discovered");
    let progress = receiver
        .capabilities
        .state_fields
        .iter()
        .find(|field| field.field.namespace == "trakkin.state" && field.field.name == "progress")
        .expect("receiving list advertises progress translation metadata");
    assert_eq!(
        progress.unit.as_ref().map(|unit| unit.name.as_str()),
        Some("time")
    );
    let numeric_range = progress
        .numeric_range
        .as_ref()
        .expect("progress numeric range is advertised");
    assert_eq!(numeric_range.minimum, "0");
    assert_eq!(numeric_range.maximum, "12");
    assert_eq!(numeric_range.step, "1");
    assert_eq!(progress.quantizer, Some(Quantizer::Exact));
    let read_capability = receiver
        .capabilities
        .targeted_state_read
        .clone()
        .expect("receiving list advertises targeted state reads");
    let write_capability = receiver
        .capabilities
        .targeted_state_write
        .clone()
        .expect("receiving list advertises targeted state writes");
    assert!(write_capability.may_create_source_membership);
    assert_eq!(
        write_capability.precondition_mode,
        Some(AdapterTargetedStateWritePreconditionMode::ProviderToken)
    );
    let receiver_key = receiver.key.clone();
    let watched_field = StateField::new(
        Term::new("trakkin.state", "watched").expect("state field is valid"),
        None,
    );
    let outcome = process
        .read_targeted_state(ReadAdapterTargetedState {
            operation_id: b"host-targeted-state-read".to_vec(),
            source_key: receiver_key.clone(),
            subject: StateSubject::ProviderItem(
                ProviderItemKey::new("fixture", b"media.quiet-signal".to_vec())
                    .expect("provider item key is valid"),
            ),
            fields: vec![watched_field.clone()],
            maximum_response_bytes: 4096,
            capability: read_capability.clone(),
            reconciliation_idempotency_key: None,
        })
        .await
        .expect("targeted state transport succeeds");
    let AdapterTargetedStateReadOutcome::Matched(matched) = outcome else {
        panic!("targeted state should match outside source membership")
    };
    assert_eq!(matched.membership, AdapterSourceMembership::Absent);
    assert!(matched.write_causation.is_none());
    assert_eq!(matched.fields[0].presence, AdapterStatePresence::Absent);
    assert!(matched.fields[0].value.is_none());
    assert_eq!(
        matched.precondition.as_deref(),
        Some(b"expected-absent-r1".as_slice())
    );
    assert!(matched.expires_at.unwrap() > matched.observed_at);

    let write_command = WriteAdapterTargetedState {
        operation_id: b"host-targeted-state-write".to_vec(),
        source_key: receiver_key.clone(),
        subject: StateSubject::ProviderItem(
            ProviderItemKey::new("fixture", b"media.quiet-signal".to_vec())
                .expect("provider item key is valid"),
        ),
        idempotency_key: b"sync-action-key!".to_vec(),
        expected_membership: AdapterSourceMembership::Absent,
        precondition: matched.precondition,
        allow_create_membership: true,
        intents: vec![AdapterTargetedStateWriteIntent {
            field: watched_field.clone(),
            operation: AdapterTargetedStateWriteOperation::Set(StateValue::Boolean(true)),
        }],
        maximum_receipt_bytes: 64,
        maximum_response_bytes: 4096,
        capability: write_capability.clone(),
    };
    let write = process
        .write_targeted_state(write_command.clone())
        .await
        .expect("targeted state write transport succeeds");
    assert_eq!(write.status, AdapterTargetedStateWriteStatus::Applied);
    assert_eq!(
        write.certainty,
        AdapterTargetedStateWriteCertainty::ConfirmedApplied
    );
    assert_eq!(
        write.retry_disposition,
        AdapterTargetedStateWriteRetryDisposition::NotRetryable
    );
    assert_eq!(
        write.membership_effect,
        AdapterTargetedStateMembershipEffect::Created
    );
    assert_eq!(
        write.field_effects[0].effect,
        AdapterTargetedStateFieldEffectKind::Set
    );
    assert_eq!(
        write.field_effects[0].value,
        Some(StateValue::Boolean(true))
    );
    assert!(!write.receipt.is_empty());
    assert!(write.receipt.len() as u64 <= write_capability.maximum_receipt_bytes);

    let mut replay_command = write_command.clone();
    replay_command.operation_id = b"host-targeted-state-write-replay".to_vec();
    assert_eq!(
        process
            .write_targeted_state(replay_command)
            .await
            .expect("stable-key targeted state write replay succeeds"),
        write
    );
    let after_write = process
        .read_targeted_state(ReadAdapterTargetedState {
            operation_id: b"host-targeted-state-read-after-write".to_vec(),
            source_key: receiver_key.clone(),
            subject: write_command.subject.clone(),
            fields: vec![watched_field.clone()],
            maximum_response_bytes: 4096,
            capability: read_capability.clone(),
            reconciliation_idempotency_key: Some(write_command.idempotency_key.clone()),
        })
        .await
        .expect("post-write targeted state read succeeds");
    let AdapterTargetedStateReadOutcome::Matched(after_write) = after_write else {
        panic!("targeted state should match after membership creation")
    };
    assert_eq!(after_write.membership, AdapterSourceMembership::Present);
    assert_eq!(
        after_write.fields[0].presence,
        AdapterStatePresence::Present
    );
    assert_eq!(after_write.fields[0].value, Some(StateValue::Boolean(true)));
    let write_causation = after_write
        .write_causation
        .expect("reconciliation read returns write causation");
    assert_eq!(
        write_causation.idempotency_key,
        write_command.idempotency_key
    );
    assert_eq!(write_causation.receipt, write.receipt);
    assert_eq!(write_causation.provider_revision, write.provider_revision);
    assert_eq!(
        after_write.precondition.as_deref(),
        Some(b"expected-present-r2".as_slice())
    );

    let mut conflict_command = write_command.clone();
    conflict_command.operation_id = b"host-targeted-state-write-conflict".to_vec();
    conflict_command.expected_membership = AdapterSourceMembership::Present;
    conflict_command.precondition = after_write.precondition.clone();
    conflict_command.allow_create_membership = false;
    conflict_command.intents[0].operation = AdapterTargetedStateWriteOperation::Clear;
    let conflict = process
        .write_targeted_state(conflict_command)
        .await
        .expect("same-key conflict returns a closed outcome");
    assert_eq!(conflict.status, AdapterTargetedStateWriteStatus::Rejected);
    assert_eq!(
        conflict.error.as_ref().map(|error| error.code.as_str()),
        Some("idempotency_key_conflict")
    );

    let mut clear_command = write_command.clone();
    clear_command.operation_id = b"host-targeted-state-clear".to_vec();
    clear_command.idempotency_key = b"sync-action-clear".to_vec();
    clear_command.expected_membership = AdapterSourceMembership::Present;
    clear_command.precondition = Some(b"stale-present-r1".to_vec());
    clear_command.allow_create_membership = false;
    clear_command.intents[0].operation = AdapterTargetedStateWriteOperation::Clear;
    let stale = process
        .write_targeted_state(clear_command.clone())
        .await
        .expect("stale precondition returns a closed outcome");
    assert_eq!(stale.status, AdapterTargetedStateWriteStatus::Rejected);
    assert_eq!(
        stale.error.as_ref().map(|error| error.code.as_str()),
        Some("precondition_failed")
    );
    clear_command.operation_id = b"host-targeted-state-clear-retry".to_vec();
    clear_command.precondition = after_write.precondition;
    let cleared = process
        .write_targeted_state(clear_command)
        .await
        .expect("targeted state clear transport succeeds");
    assert_eq!(cleared.status, AdapterTargetedStateWriteStatus::Applied);
    assert_eq!(
        cleared.membership_effect,
        AdapterTargetedStateMembershipEffect::Unchanged
    );
    assert_eq!(
        cleared.field_effects[0].effect,
        AdapterTargetedStateFieldEffectKind::Cleared
    );
    assert!(cleared.field_effects[0].value.is_none());
    assert_eq!(cleared.provider_revision, b"targeted.receiver.empty.v3");
    assert_eq!(
        cleared.successor_precondition.as_deref(),
        Some(b"expected-present-r3".as_slice())
    );
    let after_clear = process
        .read_targeted_state(ReadAdapterTargetedState {
            operation_id: b"host-targeted-state-read-after-clear".to_vec(),
            source_key: receiver_key,
            subject: write_command.subject.clone(),
            fields: vec![watched_field],
            maximum_response_bytes: 4096,
            capability: read_capability,
            reconciliation_idempotency_key: Some(write_command.idempotency_key.clone()),
        })
        .await
        .expect("post-clear targeted state read succeeds");
    let AdapterTargetedStateReadOutcome::Matched(after_clear) = after_clear else {
        panic!("targeted state should match after clear")
    };
    assert_eq!(after_clear.membership, AdapterSourceMembership::Present);
    assert!(after_clear.write_causation.is_none());
    assert_eq!(
        after_clear.fields[0].presence,
        AdapterStatePresence::Deleted
    );
    assert!(after_clear.fields[0].value.is_none());
    assert_eq!(
        after_clear.precondition.as_deref(),
        Some(b"expected-present-r3".as_slice())
    );

    let mut incomplete_command = write_command;
    incomplete_command.operation_id = b"host-targeted-state-write-incomplete".to_vec();
    incomplete_command.capability = AdapterTargetedStateWriteCapability {
        fields: Vec::new(),
        may_create_source_membership: false,
        precondition_mode: None,
        idempotency_mode: None,
        maximum_fields: 0,
        maximum_request_bytes: 0,
        maximum_response_bytes: 0,
        maximum_receipt_bytes: 0,
    };
    assert!(matches!(
        process.write_targeted_state(incomplete_command).await,
        Err(AdapterProcessError::InvalidTargetedStateWriteRequest)
    ));

    assert_eq!(
        process.retire().await.expect("fixture child retires"),
        AdapterRetirement::Graceful
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn host_runtime_force_stops_malformed_targeted_write_responder() {
    let launcher = AdapterProcessLauncher::new(
        env!("CARGO_BIN_EXE_trakkin-fixture-adapter"),
        "host-process-malformed-write-test",
    );
    let mut process = launcher
        .launch(LaunchAdapter {
            process_instance_id: "fixture-host-malformed-write".to_owned(),
        })
        .await
        .expect("fixture child launches");
    process
        .open_connection(OpenAdapterConnection {
            settings: [
                ("scenario", "empty-targeted-receiver-v1"),
                ("revision", "v1"),
                ("fault", "write.malformed-response"),
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
        })
        .await
        .expect("malformed-write fixture opens");
    let receiver = process
        .discover_sources()
        .await
        .expect("malformed-write fixture sources are discovered")
        .sources
        .into_iter()
        .find(|source| source.key.value == b"source.receiver.empty")
        .expect("receiving list is discovered");
    let capability = receiver
        .capabilities
        .targeted_state_write
        .expect("receiving list advertises targeted state writes");
    let error = process
        .write_targeted_state(WriteAdapterTargetedState {
            operation_id: b"host-malformed-targeted-state-write".to_vec(),
            source_key: receiver.key,
            subject: StateSubject::ProviderItem(
                ProviderItemKey::new("fixture", b"media.quiet-signal".to_vec())
                    .expect("provider item key is valid"),
            ),
            idempotency_key: b"sync-action-malformed".to_vec(),
            expected_membership: AdapterSourceMembership::Absent,
            precondition: Some(b"expected-absent-r1".to_vec()),
            allow_create_membership: true,
            intents: vec![AdapterTargetedStateWriteIntent {
                field: StateField::new(
                    Term::new("trakkin.state", "watched").expect("state field is valid"),
                    None,
                ),
                operation: AdapterTargetedStateWriteOperation::Set(StateValue::Boolean(true)),
            }],
            maximum_receipt_bytes: 64,
            maximum_response_bytes: 4096,
            capability,
        })
        .await
        .expect_err("malformed targeted write response is rejected");
    assert!(matches!(
        error,
        AdapterProcessError::InvalidResponse("invalid targeted state write response")
    ));
    assert_eq!(
        process
            .retire()
            .await
            .expect("force-stopped fixture child is reaped"),
        AdapterRetirement::Forced
    );
}

fn adapter_key(namespace: &str, value: &str) -> AdapterKey {
    AdapterKey {
        namespace: namespace.to_owned(),
        value: value.as_bytes().to_vec(),
    }
}
