use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use time::OffsetDateTime;
use trakkin::application::mapping_resolution::{
    CoordinateBacking, DirectFrontierRow, DirectFrontierSnapshot, DirectResolutionPins,
    EndpointLookupMatch, ExactMappingRevision, FrontierEndpoint, LoadDirectFrontier,
    MappingEndpointLookup, MappingEndpointLookupBatch, MappingEndpointLookupRequest,
    MappingResolutionFuture, MappingResolutionRepository, MappingResolutionService,
    ProcessInstancePin, ResolutionCompleteness, ResolutionMode, ResolutionRequest,
    ResolutionRevisionPin, ResolutionSemanticLimits,
};
use trakkin_mapping_contract::PortableEndpoint;
use uuid::Uuid;

#[tokio::test]
async fn direct_resolution_discards_proof_when_live_lookup_pins_change() {
    let resolved_at = fixture_time();
    let requested = endpoint("example.media:item[episode:1..12]");
    let snapshot = live_lookup_snapshot(requested.clone());

    for (name, mutation) in [
        ("processInstance", LivePinMutation::ProcessInstance),
        (
            "lookupEvidenceRevision",
            LivePinMutation::LookupEvidenceRevision,
        ),
        ("lookupExpiry", LivePinMutation::LookupExpiry),
    ] {
        let resolution = MappingResolutionService::with_endpoint_lookup(
            StableResolutionRepository {
                snapshot: snapshot.clone(),
            },
            ChangingEndpointLookup {
                calls: Arc::new(AtomicUsize::new(0)),
                mutation,
                resolved_at,
            },
        )
        .resolve(ResolutionRequest {
            workspace_id: id(401),
            mode: ResolutionMode::Direct,
            window: None,
            references: vec![requested.clone()],
            resolved_at,
        })
        .await
        .expect("changed live pin returns a closed result");

        assert_eq!(
            resolution.completeness,
            ResolutionCompleteness::Stale,
            "{name} mutation must return stale"
        );
        assert!(resolution.correspondences.is_empty());
        assert!(resolution.findings.is_empty());
        assert!(resolution.published_matches.is_empty());
        assert!(resolution.endpoint_lookup_matches.is_empty());
        assert_eq!(resolution.lookup_calls, 2);
    }
}

#[tokio::test]
async fn direct_resolution_enforces_lookup_reference_and_byte_budgets() {
    let resolved_at = fixture_time();
    let requested = vec![
        endpoint("example.media:a[episode:1]"),
        endpoint("example.media:b[episode:1]"),
    ];
    let snapshot = lookup_budget_snapshot(&requested);
    let exact_bytes = requested
        .iter()
        .map(|endpoint| endpoint.as_str().len() as u64)
        .sum();

    let exact_requests = Arc::new(Mutex::new(Vec::new()));
    let exact = MappingResolutionService::with_endpoint_lookup(
        StableResolutionRepository {
            snapshot: snapshot.clone(),
        },
        StableEndpointLookup {
            requests: exact_requests.clone(),
            resolved_at,
        },
    )
    .with_semantic_limits(ResolutionSemanticLimits {
        lookup_references: 2,
        lookup_bytes: exact_bytes,
        ..ResolutionSemanticLimits::unbounded()
    })
    .resolve(ResolutionRequest {
        workspace_id: id(430),
        mode: ResolutionMode::Direct,
        window: None,
        references: requested.clone(),
        resolved_at,
    })
    .await
    .expect("exact lookup budget resolves");
    assert_eq!(exact.completeness, ResolutionCompleteness::Complete);
    assert_eq!(exact.semantic_work.lookup_references, 2);
    assert_eq!(exact.semantic_work.lookup_bytes, exact_bytes);
    assert_eq!(exact_requests.lock().expect("requests lock").len(), 2);

    let reference_requests = Arc::new(Mutex::new(Vec::new()));
    let reference_overflow = MappingResolutionService::with_endpoint_lookup(
        StableResolutionRepository {
            snapshot: snapshot.clone(),
        },
        StableEndpointLookup {
            requests: reference_requests.clone(),
            resolved_at,
        },
    )
    .with_semantic_limits(ResolutionSemanticLimits {
        lookup_references: 1,
        ..ResolutionSemanticLimits::unbounded()
    })
    .resolve(ResolutionRequest {
        workspace_id: id(430),
        mode: ResolutionMode::Direct,
        window: None,
        references: requested.clone(),
        resolved_at,
    })
    .await
    .expect("reference overflow returns a closed result");
    assert_eq!(
        reference_overflow.completeness,
        ResolutionCompleteness::Truncated
    );
    assert_eq!(reference_overflow.semantic_work.lookup_references, 1);
    assert_eq!(
        reference_overflow.next_work_key.as_deref(),
        Some("reference:example.media:b[episode:1]")
    );
    assert!(
        reference_requests
            .lock()
            .expect("requests lock")
            .iter()
            .all(|endpoints| endpoints == &requested[..1])
    );

    let byte_requests = Arc::new(Mutex::new(Vec::new()));
    let byte_overflow = MappingResolutionService::with_endpoint_lookup(
        StableResolutionRepository { snapshot },
        StableEndpointLookup {
            requests: byte_requests.clone(),
            resolved_at,
        },
    )
    .with_semantic_limits(ResolutionSemanticLimits {
        lookup_bytes: requested[0].as_str().len() as u64,
        ..ResolutionSemanticLimits::unbounded()
    })
    .resolve(ResolutionRequest {
        workspace_id: id(430),
        mode: ResolutionMode::Direct,
        window: None,
        references: requested.clone(),
        resolved_at,
    })
    .await
    .expect("byte overflow returns a closed result");
    assert_eq!(
        byte_overflow.completeness,
        ResolutionCompleteness::Truncated
    );
    assert_eq!(byte_overflow.semantic_work.lookup_references, 1);
    assert_eq!(
        byte_overflow.semantic_work.lookup_bytes,
        requested[0].as_str().len() as u64
    );
    assert_eq!(
        byte_overflow.next_work_key.as_deref(),
        Some("reference:example.media:b[episode:1]@byte:0")
    );
    assert!(
        byte_requests
            .lock()
            .expect("requests lock")
            .iter()
            .all(|endpoints| endpoints == &requested[..1])
    );
}

#[tokio::test]
async fn direct_resolution_enforces_lookup_batch_call_budget() {
    let resolved_at = fixture_time();
    let requested = vec![
        endpoint("example.media:a[episode:1]"),
        endpoint("example.media:b[episode:1]"),
    ];
    let snapshot = lookup_budget_snapshot(&requested);

    let exact = MappingResolutionService::with_endpoint_lookup(
        StableResolutionRepository {
            snapshot: snapshot.clone(),
        },
        BatchedEndpointLookup { resolved_at },
    )
    .with_semantic_limits(ResolutionSemanticLimits {
        lookup_calls: 2,
        ..ResolutionSemanticLimits::unbounded()
    })
    .resolve(ResolutionRequest {
        workspace_id: id(431),
        mode: ResolutionMode::Direct,
        window: None,
        references: requested.clone(),
        resolved_at,
    })
    .await
    .expect("exact lookup call budget resolves");
    assert_eq!(exact.completeness, ResolutionCompleteness::Complete);
    assert_eq!(exact.semantic_work.lookup_calls, 2);
    assert_eq!(exact.lookup_calls, 4);
    assert_eq!(exact.next_work_key, None);

    let overflow = MappingResolutionService::with_endpoint_lookup(
        StableResolutionRepository { snapshot },
        BatchedEndpointLookup { resolved_at },
    )
    .with_semantic_limits(ResolutionSemanticLimits {
        lookup_calls: 1,
        ..ResolutionSemanticLimits::unbounded()
    })
    .resolve(ResolutionRequest {
        workspace_id: id(431),
        mode: ResolutionMode::Direct,
        window: None,
        references: requested,
        resolved_at,
    })
    .await
    .expect("lookup call overflow returns a closed result");
    assert_eq!(overflow.completeness, ResolutionCompleteness::Truncated);
    assert_eq!(overflow.semantic_work.lookup_calls, 1);
    assert_eq!(overflow.lookup_calls, 2);
    assert_eq!(
        overflow.next_work_key.as_deref(),
        Some("lookup-call:example.media:b[episode:1]")
    );
}

fn lookup_budget_snapshot(requested: &[PortableEndpoint]) -> DirectFrontierSnapshot {
    DirectFrontierSnapshot {
        rows: requested
            .iter()
            .enumerate()
            .map(|(index, requested_endpoint)| DirectFrontierRow {
                pin: ResolutionRevisionPin {
                    mapping_id: id(440),
                    content_hash: "sha256:lookup-budget".to_owned(),
                    activation_generation: Some(1),
                },
                row_hash: format!("sha256:lookup-budget-row-{index}"),
                row_ordinal: index as i32,
                requested_endpoint: requested_endpoint.clone(),
                matched_endpoint_hash: format!("sha256:lookup-budget-endpoint-{index}"),
                endpoints: vec![
                    FrontierEndpoint {
                        endpoint: requested_endpoint.clone(),
                        endpoint_hash: format!("sha256:lookup-budget-endpoint-{index}"),
                        endpoint_ordinal: 0,
                    },
                    FrontierEndpoint {
                        endpoint: endpoint(&format!("example.media:peer-{index}[episode:1]")),
                        endpoint_hash: format!("sha256:lookup-budget-peer-{index}"),
                        endpoint_ordinal: 1,
                    },
                ],
            })
            .collect(),
        published_matches: Vec::new(),
        pins: DirectResolutionPins {
            revisions: Vec::new(),
            sources: Vec::new(),
        },
    }
}

fn live_lookup_snapshot(requested: PortableEndpoint) -> DirectFrontierSnapshot {
    DirectFrontierSnapshot {
        rows: vec![DirectFrontierRow {
            pin: ResolutionRevisionPin {
                mapping_id: id(410),
                content_hash: "sha256:live-pins".to_owned(),
                activation_generation: Some(7),
            },
            row_hash: "sha256:live-row".to_owned(),
            row_ordinal: 0,
            requested_endpoint: requested.clone(),
            matched_endpoint_hash: "sha256:requested".to_owned(),
            endpoints: vec![
                FrontierEndpoint {
                    endpoint: requested,
                    endpoint_hash: "sha256:requested".to_owned(),
                    endpoint_ordinal: 0,
                },
                FrontierEndpoint {
                    endpoint: endpoint("example.media:peer[episode:1..12]"),
                    endpoint_hash: "sha256:peer".to_owned(),
                    endpoint_ordinal: 1,
                },
            ],
        }],
        published_matches: Vec::new(),
        pins: DirectResolutionPins {
            revisions: Vec::new(),
            sources: Vec::new(),
        },
    }
}

#[derive(Clone)]
struct StableResolutionRepository {
    snapshot: DirectFrontierSnapshot,
}

impl MappingResolutionRepository for StableResolutionRepository {
    type Error = std::convert::Infallible;

    fn load_direct_frontier(
        &self,
        _request: LoadDirectFrontier,
    ) -> MappingResolutionFuture<'_, DirectFrontierSnapshot, Self::Error> {
        Box::pin(async { Ok(self.snapshot.clone()) })
    }

    fn recheck_direct_resolution(
        &self,
        _workspace_id: Uuid,
        _revision: Option<ExactMappingRevision>,
        expected: DirectResolutionPins,
    ) -> MappingResolutionFuture<'_, bool, Self::Error> {
        Box::pin(async move { Ok(expected == self.snapshot.pins) })
    }
}

#[derive(Clone, Copy)]
enum LivePinMutation {
    ProcessInstance,
    LookupEvidenceRevision,
    LookupExpiry,
}

#[derive(Clone)]
struct ChangingEndpointLookup {
    calls: Arc<AtomicUsize>,
    mutation: LivePinMutation,
    resolved_at: OffsetDateTime,
}

#[derive(Clone)]
struct StableEndpointLookup {
    requests: Arc<Mutex<Vec<Vec<PortableEndpoint>>>>,
    resolved_at: OffsetDateTime,
}

#[derive(Clone)]
struct BatchedEndpointLookup {
    resolved_at: OffsetDateTime,
}

impl MappingEndpointLookup for BatchedEndpointLookup {
    fn lookup_endpoints(
        &self,
        request: MappingEndpointLookupRequest,
    ) -> MappingResolutionFuture<
        '_,
        MappingEndpointLookupBatch,
        trakkin::application::error::ApplicationError,
    > {
        Box::pin(async move {
            let admitted = request.endpoints.len().min(request.maximum_calls as usize);
            let next_work_key = request
                .endpoints
                .get(admitted)
                .map(|endpoint| format!("lookup-call:{endpoint}"));
            Ok(MappingEndpointLookupBatch {
                matches: request
                    .endpoints
                    .into_iter()
                    .take(admitted)
                    .map(|endpoint| EndpointLookupMatch {
                        item_key_value: endpoint.as_str().as_bytes().to_vec(),
                        endpoint,
                        connection_id: id(451),
                        process_instance_id: "batched-process".to_owned(),
                        item_key_namespace: "fixture.item".to_owned(),
                        backing: Some(CoordinateBacking::Virtual),
                        coordinate_evidence_revision: Some(b"coordinates-r1".to_vec()),
                        lookup_evidence_revision: b"lookup-r1".to_vec(),
                        observed_at: self.resolved_at,
                        expires_at: None,
                    })
                    .collect(),
                findings: Vec::new(),
                process_instances: vec![ProcessInstancePin {
                    connection_id: id(451),
                    process_instance_id: "batched-process".to_owned(),
                }],
                lookup_calls: admitted as u32,
                next_work_key,
                stale: false,
            })
        })
    }
}

fn endpoint(value: &str) -> PortableEndpoint {
    PortableEndpoint::parse(value).expect("fixture endpoint is valid")
}

fn fixture_time() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_893_456_245).expect("fixture time")
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

impl MappingEndpointLookup for StableEndpointLookup {
    fn lookup_endpoints(
        &self,
        request: MappingEndpointLookupRequest,
    ) -> MappingResolutionFuture<
        '_,
        MappingEndpointLookupBatch,
        trakkin::application::error::ApplicationError,
    > {
        Box::pin(async move {
            self.requests
                .lock()
                .expect("requests lock")
                .push(request.endpoints.clone());
            Ok(MappingEndpointLookupBatch {
                matches: request
                    .endpoints
                    .into_iter()
                    .map(|endpoint| EndpointLookupMatch {
                        item_key_value: endpoint.as_str().as_bytes().to_vec(),
                        endpoint,
                        connection_id: id(450),
                        process_instance_id: "stable-process".to_owned(),
                        item_key_namespace: "fixture.item".to_owned(),
                        backing: Some(CoordinateBacking::Virtual),
                        coordinate_evidence_revision: Some(b"coordinates-r1".to_vec()),
                        lookup_evidence_revision: b"lookup-r1".to_vec(),
                        observed_at: self.resolved_at,
                        expires_at: None,
                    })
                    .collect(),
                findings: Vec::new(),
                process_instances: vec![ProcessInstancePin {
                    connection_id: id(450),
                    process_instance_id: "stable-process".to_owned(),
                }],
                lookup_calls: 1,
                next_work_key: None,
                stale: false,
            })
        })
    }
}

impl MappingEndpointLookup for ChangingEndpointLookup {
    fn lookup_endpoints(
        &self,
        request: MappingEndpointLookupRequest,
    ) -> MappingResolutionFuture<
        '_,
        MappingEndpointLookupBatch,
        trakkin::application::error::ApplicationError,
    > {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.recheck, call == 1);
            let changed = call == 1;
            let process_instance_id =
                if changed && matches!(self.mutation, LivePinMutation::ProcessInstance) {
                    "process-b"
                } else {
                    "process-a"
                }
                .to_owned();
            let lookup_evidence_revision =
                if changed && matches!(self.mutation, LivePinMutation::LookupEvidenceRevision) {
                    b"lookup-r2".to_vec()
                } else {
                    b"lookup-r1".to_vec()
                };
            let expiry_seconds =
                if changed && matches!(self.mutation, LivePinMutation::LookupExpiry) {
                    120
                } else {
                    60
                };
            let requested = request
                .endpoints
                .into_iter()
                .next()
                .expect("live lookup request has an endpoint");
            Ok(MappingEndpointLookupBatch {
                matches: vec![EndpointLookupMatch {
                    endpoint: requested,
                    connection_id: id(420),
                    process_instance_id: process_instance_id.clone(),
                    item_key_namespace: "fixture.item".to_owned(),
                    item_key_value: b"live-item".to_vec(),
                    backing: Some(CoordinateBacking::Virtual),
                    coordinate_evidence_revision: Some(b"coordinates-r1".to_vec()),
                    lookup_evidence_revision,
                    observed_at: self.resolved_at,
                    expires_at: Some(self.resolved_at + time::Duration::seconds(expiry_seconds)),
                }],
                findings: Vec::new(),
                process_instances: vec![ProcessInstancePin {
                    connection_id: id(420),
                    process_instance_id,
                }],
                lookup_calls: 1,
                next_work_key: None,
                stale: false,
            })
        })
    }
}
