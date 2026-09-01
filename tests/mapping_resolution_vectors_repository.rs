#![cfg(feature = "embedded-database")]

#[path = "support/mapping_resolution.rs"]
mod support;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use support::{MappingResolutionDatabase, endpoint, fixture_time, id};
use time::OffsetDateTime;
use trakkin::{
    application::{
        mapping_lifecycle::{
            ChangeMappingActivation, CreateMapping, MappingActivationKind, MappingLifecycleService,
        },
        mapping_resolution::{
            MappingEndpointLookup, MappingEndpointLookupBatch, MappingEndpointLookupRequest,
            MappingResolutionFuture, MappingResolutionService, ResolutionCompleteness,
            ResolutionFinding, ResolutionFindingKind, ResolutionMode, ResolutionProofKind,
            ResolutionRequest, ResolutionSemanticLimits,
        },
    },
    infrastructure::{
        mapping_lifecycle::PostgresMappingLifecycleRepository,
        mapping_resolution::PostgresMappingResolutionRepository,
    },
};
use trakkin_mapping::{PortableEndpoint, PortableSelector, fixtures::CORRESPONDENCE_VECTORS};
use uuid::Uuid;

fn fixture_vector(document: &str, id: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(document)
        .expect("fixture vector document is valid JSON")["vectors"]
        .as_array()
        .expect("fixture vector document contains vectors")
        .iter()
        .find(|vector| vector["id"] == id)
        .unwrap_or_else(|| panic!("fixture vector {id} exists"))
        .clone()
}

#[tokio::test]
async fn postgres_direct_segment_projection_executes_frozen_vectors() {
    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();

    activate_correspondence_vector(&pool, "GRAPH-FINITE-RATIO-001", 500, now).await;
    let ratio_resolution = resolve_direct_segments(
        &pool,
        id(500),
        vec![endpoint("example.media://original")],
        Some("episode:1..24"),
        now,
    )
    .await;
    assert_eq!(
        ratio_resolution.completeness,
        ResolutionCompleteness::Complete
    );
    assert_eq!(ratio_resolution.correspondences.len(), 1);
    assert_eq!(
        ratio_resolution.correspondences[0]
            .proof
            .transform
            .numerator,
        2
    );
    assert_eq!(
        ratio_resolution.correspondences[0]
            .proof
            .transform
            .denominator,
        1
    );
    let ratio_segment = ratio_resolution.correspondences[0]
        .segment
        .as_ref()
        .expect("finite ratio has an exact segment");
    assert_eq!(
        (ratio_segment.source_start, ratio_segment.source_end),
        (1, 24)
    );
    assert_eq!(
        (
            ratio_segment.target_start_numerator,
            ratio_segment.target_end_exclusive_numerator,
            ratio_segment.target_denominator,
        ),
        (1, 13, 1)
    );

    activate_correspondence_vector(&pool, "GRAPH-ONE-TO-MANY-SPLIT-001", 510, now).await;
    let split_resolution = resolve_direct_segments(
        &pool,
        id(510),
        vec![endpoint("example.media://original")],
        Some("episode:1..12"),
        now,
    )
    .await;
    assert_eq!(
        split_resolution.completeness,
        ResolutionCompleteness::Complete
    );
    assert_eq!(source_spans(&split_resolution), vec![(1, 6, 1), (7, 12, 1)]);

    activate_correspondence_vector(&pool, "GRAPH-MANY-TO-ONE-MERGE-001", 520, now).await;
    let merge_resolution = resolve_direct_segments(
        &pool,
        id(520),
        vec![
            endpoint("example.media://part-a"),
            endpoint("example.media://part-b"),
        ],
        None,
        now,
    )
    .await;
    assert_eq!(
        merge_resolution.completeness,
        ResolutionCompleteness::Complete
    );
    let mut merge_targets = merge_resolution
        .correspondences
        .iter()
        .map(|correspondence| {
            let segment = correspondence
                .segment
                .as_ref()
                .expect("merge correspondence has a segment");
            (
                segment.target_start_numerator,
                segment.target_end_exclusive_numerator,
                segment.target_denominator,
            )
        })
        .collect::<Vec<_>>();
    merge_targets.sort_unstable();
    assert_eq!(merge_targets, vec![(1, 7, 1), (7, 13, 1)]);

    activate_correspondence_vector(&pool, "GRAPH-COMPATIBLE-OVERLAP-001", 530, now).await;
    let compatible = resolve_direct_segments(
        &pool,
        id(530),
        vec![endpoint("example.media://a")],
        Some("episode:1..18"),
        now,
    )
    .await;
    assert_eq!(compatible.completeness, ResolutionCompleteness::Complete);
    assert_eq!(
        source_spans(&compatible),
        vec![(1, 6, 1), (7, 12, 2), (13, 18, 1)]
    );
    assert_eq!(compatible.correspondences[1].supporting_proofs.len(), 1);

    activate_correspondence_vector(&pool, "GRAPH-CONFLICT-LOCALIZED-001", 540, now).await;
    let conflict = resolve_direct_segments(
        &pool,
        id(540),
        vec![endpoint("example.media://a")],
        Some("episode:1..18"),
        now,
    )
    .await;
    assert_eq!(conflict.completeness, ResolutionCompleteness::Partial);
    assert_eq!(source_spans(&conflict), vec![(1, 6, 1), (13, 18, 1)]);
    assert_eq!(conflict.findings.len(), 1);
    assert_eq!(
        conflict.findings[0].kind,
        ResolutionFindingKind::ContradictorySpans
    );
    assert_eq!(
        conflict.findings[0]
            .span
            .as_ref()
            .map(|span| (span.start, span.end)),
        Some((7, 12))
    );
    assert_eq!(conflict.findings[0].proof_paths.len(), 2);

    database.shutdown().await;
}

#[tokio::test]
async fn postgres_transitive_resolution_executes_frozen_vectors() {
    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();

    activate_correspondence_vector(&pool, "GRAPH-DIRECT-VERSUS-TRANSITIVE-001", 550, now).await;
    let resolver =
        MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()));
    let direct = resolver
        .resolve(ResolutionRequest {
            workspace_id: id(550),
            mode: ResolutionMode::Direct,
            references: vec![endpoint("example.media://a")],
            window: None,
            resolved_at: now,
        })
        .await
        .expect("direct graph resolution succeeds");
    assert_eq!(direct.correspondences.len(), 1);
    assert_eq!(
        direct.correspondences[0].proof.kind,
        ResolutionProofKind::Direct
    );

    let transitive = resolver
        .resolve(ResolutionRequest {
            workspace_id: id(550),
            mode: ResolutionMode::Transitive,
            references: vec![endpoint("example.media://a")],
            window: None,
            resolved_at: now,
        })
        .await
        .expect("transitive graph resolution succeeds");
    assert_eq!(transitive.completeness, ResolutionCompleteness::Complete);
    assert_eq!(transitive.correspondences.len(), 2);
    assert!(transitive.correspondences.iter().any(|correspondence| {
        correspondence.endpoints
            == vec![endpoint("example.media://a"), endpoint("example.media://b")]
            && correspondence.proof.kind == ResolutionProofKind::Direct
    }));
    assert!(transitive.correspondences.iter().any(|correspondence| {
        correspondence.endpoints
            == vec![endpoint("example.media://a"), endpoint("example.media://c")]
            && correspondence.proof.kind == ResolutionProofKind::Derived
    }));

    activate_correspondence_vector(&pool, "GRAPH-CONSISTENT-CYCLE-001", 560, now).await;
    let consistent =
        MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()))
            .resolve(ResolutionRequest {
                workspace_id: id(560),
                mode: ResolutionMode::Transitive,
                references: vec![endpoint("example.media://a"), endpoint("example.media://c")],
                window: None,
                resolved_at: now,
            })
            .await
            .expect("consistent cycle resolves");
    assert_eq!(consistent.completeness, ResolutionCompleteness::Complete);
    assert!(consistent.findings.is_empty());
    assert!(consistent.correspondences.iter().all(|correspondence| {
        correspondence.proof.transform.numerator == 1
            && correspondence.proof.transform.denominator == 1
    }));

    activate_correspondence_vector(&pool, "GRAPH-CONTRADICTORY-CYCLE-001", 570, now).await;
    let contradictory =
        MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()))
            .resolve(ResolutionRequest {
                workspace_id: id(570),
                mode: ResolutionMode::Transitive,
                references: vec![endpoint("example.media://a"), endpoint("example.media://c")],
                window: None,
                resolved_at: now,
            })
            .await
            .expect("contradictory cycle resolves partially");
    assert_eq!(contradictory.completeness, ResolutionCompleteness::Partial);
    assert_eq!(contradictory.findings.len(), 1);
    assert_eq!(contradictory.findings[0].kind, ResolutionFindingKind::Cycle);
    assert_eq!(contradictory.findings[0].proof_paths.len(), 2);

    activate_correspondence_vector(&pool, "GRAPH-AMBIGUOUS-NO-TRAVERSAL-001", 580, now).await;
    let ambiguous_calls = Arc::new(AtomicUsize::new(0));
    let ambiguous = MappingResolutionService::with_endpoint_lookup(
        PostgresMappingResolutionRepository::new(pool.clone()),
        AmbiguousTraversalLookup {
            calls: ambiguous_calls.clone(),
        },
    )
    .resolve(ResolutionRequest {
        workspace_id: id(580),
        mode: ResolutionMode::Transitive,
        references: vec![endpoint("example.media://a")],
        window: None,
        resolved_at: now,
    })
    .await
    .expect("ambiguous traversal resolves partially");
    assert_eq!(ambiguous_calls.load(Ordering::SeqCst), 2);
    assert_eq!(ambiguous.completeness, ResolutionCompleteness::Partial);
    assert_eq!(ambiguous.findings.len(), 1);
    assert_eq!(ambiguous.findings[0].kind, ResolutionFindingKind::Ambiguous);
    assert_eq!(
        ambiguous.findings[0].endpoint,
        endpoint("example.media://b")
    );
    assert!(ambiguous.correspondences.iter().all(|correspondence| {
        !correspondence
            .endpoints
            .contains(&endpoint("example.media://c"))
    }));

    database.shutdown().await;
}

#[tokio::test]
async fn postgres_semantic_budgets_execute_bounded_direct_topologies() {
    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();

    activate_mapping_rows(
        &pool,
        600,
        "bounded-sparse",
        vec![vec![
            "budget.example://sparse/a".to_owned(),
            "budget.example://sparse/b".to_owned(),
        ]],
        now,
    )
    .await;
    let sparse = resolve_with_limits(
        &pool,
        id(600),
        ResolutionMode::Direct,
        "budget.example://sparse/a",
        None,
        ResolutionSemanticLimits {
            endpoint_hits: 1,
            candidate_rows: 1,
            traversal_hops: 0,
            partitions: 1,
            ..ResolutionSemanticLimits::unbounded()
        },
        now,
    )
    .await;
    assert_eq!(sparse.completeness, ResolutionCompleteness::Complete);
    assert_eq!(sparse.semantic_work.endpoint_hits, 1);
    assert_eq!(sparse.semantic_work.candidate_rows, 1);
    assert_eq!(sparse.semantic_work.traversal_hops, 0);
    assert_eq!(sparse.semantic_work.partitions, 1);

    activate_mapping_rows(
        &pool,
        610,
        "bounded-high-degree",
        (0..3)
            .map(|index| {
                vec![
                    "budget.example://hub".to_owned(),
                    format!("budget.example://leaf/{index}"),
                ]
            })
            .collect(),
        now,
    )
    .await;
    let exact_limits = ResolutionSemanticLimits {
        endpoint_hits: 3,
        candidate_rows: 3,
        traversal_hops: 0,
        partitions: 3,
        ..ResolutionSemanticLimits::unbounded()
    };
    let exact = resolve_with_limits(
        &pool,
        id(610),
        ResolutionMode::Direct,
        "budget.example://hub",
        None,
        exact_limits,
        now,
    )
    .await;
    assert_eq!(exact.completeness, ResolutionCompleteness::Complete);
    assert_eq!(exact.semantic_work.endpoint_hits, 3);
    assert_eq!(exact.semantic_work.candidate_rows, 3);
    assert_eq!(exact.semantic_work.partitions, 3);
    assert!(exact.next_work_key.is_none());

    let endpoint_overflow = resolve_with_limits(
        &pool,
        id(610),
        ResolutionMode::Direct,
        "budget.example://hub",
        None,
        ResolutionSemanticLimits {
            endpoint_hits: 2,
            ..exact_limits
        },
        now,
    )
    .await;
    assert_eq!(
        endpoint_overflow.completeness,
        ResolutionCompleteness::Truncated
    );
    assert_eq!(endpoint_overflow.semantic_work.endpoint_hits, 2);
    assert!(
        endpoint_overflow
            .next_work_key
            .as_deref()
            .is_some_and(|key| key.starts_with("endpoint:budget.example://leaf/"))
    );

    let candidate_overflow = resolve_with_limits(
        &pool,
        id(610),
        ResolutionMode::Direct,
        "budget.example://hub",
        None,
        ResolutionSemanticLimits {
            candidate_rows: 2,
            ..exact_limits
        },
        now,
    )
    .await;
    assert_eq!(
        candidate_overflow.completeness,
        ResolutionCompleteness::Truncated
    );
    assert_eq!(candidate_overflow.semantic_work.candidate_rows, 2);
    assert!(
        candidate_overflow
            .next_work_key
            .as_deref()
            .is_some_and(|key| key.starts_with("row:sha256:"))
    );

    let partition_overflow = resolve_with_limits(
        &pool,
        id(610),
        ResolutionMode::Direct,
        "budget.example://hub",
        None,
        ResolutionSemanticLimits {
            partitions: 2,
            ..exact_limits
        },
        now,
    )
    .await;
    assert_eq!(
        partition_overflow.completeness,
        ResolutionCompleteness::Truncated
    );
    assert_eq!(partition_overflow.semantic_work.partitions, 2);
    assert!(
        partition_overflow
            .next_work_key
            .as_deref()
            .is_some_and(|key| key.starts_with("correspondence:budget.example://"))
    );

    activate_mapping_rows(
        &pool,
        620,
        "bounded-dense-overlap",
        (1..=3)
            .map(|start| {
                let end = start + 2;
                vec![
                    format!("budget.example://dense/a[episode:{start}..{end}]"),
                    format!("budget.example://dense/b[episode:{start}..{end}]"),
                ]
            })
            .collect(),
        now,
    )
    .await;
    let dense_limits = ResolutionSemanticLimits {
        endpoint_hits: 3,
        candidate_rows: 3,
        traversal_hops: 0,
        partitions: 3,
        ..ResolutionSemanticLimits::unbounded()
    };
    let dense = resolve_with_limits(
        &pool,
        id(620),
        ResolutionMode::Direct,
        "budget.example://dense/a",
        Some("episode:1..5"),
        dense_limits,
        now,
    )
    .await;
    assert_eq!(dense.completeness, ResolutionCompleteness::Complete);
    assert_eq!(dense.semantic_work.endpoint_hits, 3);
    assert_eq!(dense.semantic_work.candidate_rows, 3);
    assert_eq!(dense.semantic_work.partitions, 3);

    let dense_overflow = resolve_with_limits(
        &pool,
        id(620),
        ResolutionMode::Direct,
        "budget.example://dense/a",
        Some("episode:1..5"),
        ResolutionSemanticLimits {
            partitions: 2,
            ..dense_limits
        },
        now,
    )
    .await;
    assert_eq!(
        dense_overflow.completeness,
        ResolutionCompleteness::Truncated
    );
    assert_eq!(dense_overflow.semantic_work.partitions, 2);
    assert!(dense_overflow.next_work_key.as_deref().is_some_and(|key| {
        key.starts_with("segment:budget.example://dense/a[episode:3..5]:episode:3")
    }));

    database.shutdown().await;
}

#[tokio::test]
async fn postgres_semantic_budgets_execute_bounded_transitive_topologies() {
    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();

    activate_mapping_rows(
        &pool,
        630,
        "bounded-long-chain",
        (0..3)
            .map(|index| {
                vec![
                    format!("budget.example://chain/{index}"),
                    format!("budget.example://chain/{}", index + 1),
                ]
            })
            .collect(),
        now,
    )
    .await;
    let chain_limits = ResolutionSemanticLimits {
        endpoint_hits: u64::MAX,
        candidate_rows: 3,
        traversal_hops: 3,
        partitions: u64::MAX,
        ..ResolutionSemanticLimits::unbounded()
    };
    let chain = resolve_with_limits(
        &pool,
        id(630),
        ResolutionMode::Transitive,
        "budget.example://chain/0",
        None,
        chain_limits,
        now,
    )
    .await;
    assert_eq!(chain.completeness, ResolutionCompleteness::Complete);
    assert_eq!(chain.semantic_work.candidate_rows, 3);
    assert_eq!(chain.semantic_work.traversal_hops, 3);

    let hop_overflow = resolve_with_limits(
        &pool,
        id(630),
        ResolutionMode::Transitive,
        "budget.example://chain/0",
        None,
        ResolutionSemanticLimits {
            traversal_hops: 2,
            ..chain_limits
        },
        now,
    )
    .await;
    assert_eq!(hop_overflow.completeness, ResolutionCompleteness::Truncated);
    assert_eq!(hop_overflow.semantic_work.traversal_hops, 2);
    assert_eq!(
        hop_overflow.next_work_key.as_deref(),
        Some("endpoint:budget.example://chain/2")
    );

    activate_mapping_rows(
        &pool,
        640,
        "bounded-cycle",
        vec![
            vec![
                "budget.example://cycle/a".to_owned(),
                "budget.example://cycle/b".to_owned(),
            ],
            vec![
                "budget.example://cycle/b".to_owned(),
                "budget.example://cycle/c".to_owned(),
            ],
            vec![
                "budget.example://cycle/c".to_owned(),
                "budget.example://cycle/a".to_owned(),
            ],
        ],
        now,
    )
    .await;
    let cycle = resolve_with_limits(
        &pool,
        id(640),
        ResolutionMode::Transitive,
        "budget.example://cycle/a",
        None,
        ResolutionSemanticLimits {
            endpoint_hits: u64::MAX,
            candidate_rows: 3,
            traversal_hops: 2,
            partitions: u64::MAX,
            ..ResolutionSemanticLimits::unbounded()
        },
        now,
    )
    .await;
    assert_eq!(cycle.completeness, ResolutionCompleteness::Complete);
    assert_eq!(cycle.semantic_work.candidate_rows, 3);
    assert_eq!(cycle.semantic_work.traversal_hops, 2);

    database.shutdown().await;
}

async fn activate_mapping_rows(
    pool: &sqlx::PgPool,
    identity: u128,
    name: &str,
    mappings: Vec<Vec<String>>,
    now: OffsetDateTime,
) {
    let workspace_id = id(identity);
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES ($1, $2, 'active', $3, $3)
",
    )
    .bind(workspace_id)
    .bind(name)
    .bind(now)
    .execute(pool)
    .await
    .expect("bounded workspace is seeded");
    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let mapping = lifecycle
        .create(CreateMapping {
            mapping_id: id(identity + 1),
            mappings,
            created_at: now,
        })
        .await
        .expect("bounded mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(identity + 2),
            workspace_id,
            mapping_id: id(identity + 1),
            content_hash: Some(mapping.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("bounded mapping is activated");
}

async fn resolve_with_limits(
    pool: &sqlx::PgPool,
    workspace_id: Uuid,
    mode: ResolutionMode,
    reference: &str,
    window: Option<&str>,
    semantic_limits: ResolutionSemanticLimits,
    resolved_at: OffsetDateTime,
) -> trakkin::application::mapping_resolution::MappingResolution {
    MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()))
        .with_semantic_limits(semantic_limits)
        .resolve(ResolutionRequest {
            workspace_id,
            mode,
            references: vec![endpoint(reference)],
            window: window
                .map(|value| PortableSelector::parse(value).expect("budget window is valid")),
            resolved_at,
        })
        .await
        .expect("bounded resolution succeeds")
}

async fn activate_correspondence_vector(
    pool: &sqlx::PgPool,
    vector_id: &str,
    identity: u128,
    now: OffsetDateTime,
) {
    let vector = fixture_vector(CORRESPONDENCE_VECTORS, vector_id);
    let mappings = serde_json::from_value::<Vec<Vec<String>>>(vector["mappings"].clone())
        .expect("correspondence mappings are strings");
    let workspace_id = id(identity);
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES ($1, $2, 'active', $3, $3)
",
    )
    .bind(workspace_id)
    .bind(vector_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("correspondence workspace is seeded");
    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let mapping_id = id(identity + 1);
    let mapping = lifecycle
        .create(CreateMapping {
            mapping_id,
            mappings,
            created_at: now,
        })
        .await
        .expect("correspondence mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(identity + 2),
            workspace_id,
            mapping_id,
            content_hash: Some(mapping.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("correspondence mapping is activated");
}

async fn resolve_direct_segments(
    pool: &sqlx::PgPool,
    workspace_id: Uuid,
    references: Vec<PortableEndpoint>,
    window: Option<&str>,
    resolved_at: OffsetDateTime,
) -> trakkin::application::mapping_resolution::MappingResolution {
    MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()))
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            references,
            window: window
                .map(|value| PortableSelector::parse(value).expect("resolution window is valid")),
            resolved_at,
        })
        .await
        .expect("direct segment resolution succeeds")
}

fn source_spans(
    resolution: &trakkin::application::mapping_resolution::MappingResolution,
) -> Vec<(u64, u64, u32)> {
    resolution
        .correspondences
        .iter()
        .map(|correspondence| {
            let segment = correspondence
                .segment
                .as_ref()
                .expect("finite correspondence has a segment");
            (
                segment.source_start,
                segment.source_end,
                correspondence.support_count,
            )
        })
        .collect()
}

#[derive(Clone)]
struct AmbiguousTraversalLookup {
    calls: Arc<AtomicUsize>,
}

impl MappingEndpointLookup for AmbiguousTraversalLookup {
    fn lookup_endpoints(
        &self,
        request: MappingEndpointLookupRequest,
    ) -> MappingResolutionFuture<
        '_,
        MappingEndpointLookupBatch,
        trakkin::application::error::ApplicationError,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(request.endpoints.contains(&endpoint("example.media://b")));
            Ok(MappingEndpointLookupBatch {
                matches: Vec::new(),
                findings: vec![ResolutionFinding {
                    endpoint: endpoint("example.media://b"),
                    kind: ResolutionFindingKind::Ambiguous,
                    span: None,
                    proof_paths: Vec::new(),
                }],
                process_instances: Vec::new(),
                lookup_calls: 1,
                next_work_key: None,
                stale: false,
            })
        })
    }
}
