#![cfg(feature = "embedded-database")]

#[path = "support/mapping_resolution_published_source.rs"]
mod published_source;
#[path = "support/mapping_resolution.rs"]
mod support;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use published_source::{PublishedSourceIds, seed_published_reference};
use serde_json::json;
use support::{MappingResolutionDatabase, endpoint, fixture_time, id};
use time::OffsetDateTime;
use trakkin::{
    application::{
        mapping_lifecycle::{
            ChangeMappingActivation, CreateMapping, MappingActivationKind, MappingLifecycleService,
        },
        mapping_resolution::{
            CoordinateBacking, DirectFrontierSnapshot, DirectResolutionPins, ExactMappingRevision,
            LoadDirectFrontier, MappingResolutionFuture, MappingResolutionInteraction,
            MappingResolutionRepository, MappingResolutionService, ResolutionCompleteness,
            ResolutionMode, ResolutionRequest,
        },
    },
    infrastructure::{
        mapping_lifecycle::PostgresMappingLifecycleRepository,
        mapping_resolution::PostgresMappingResolutionRepository,
    },
};
use trakkin_mapping_contract::PortableEndpoint;
use uuid::Uuid;

const CORRESPONDENCE_VECTORS: &str = include_str!(
    "../crates/trakkin-mapping-contract/tests/fixtures.mapping/v1/correspondence/vectors.json"
);
const EVIDENCE_VECTORS: &str = include_str!(
    "../crates/trakkin-mapping-contract/tests/fixtures.mapping/v1/evidence/vectors.json"
);

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
async fn postgres_direct_bare_reference_resolution_executes_initial_vectors() {
    let graph_vector = fixture_vector(CORRESPONDENCE_VECTORS, "GRAPH-ONE-TO-ONE-001");
    let graph_mappings =
        serde_json::from_value::<Vec<Vec<String>>>(graph_vector["mappings"].clone())
            .expect("graph vector mappings are strings");
    let graph_references =
        serde_json::from_value::<Vec<String>>(graph_vector["request"]["references"].clone())
            .expect("graph vector references are strings");
    let published_vector = fixture_vector(EVIDENCE_VECTORS, "EVIDENCE-PUBLISHED-MATCH-001");
    let published_reference = published_vector["input"]["reference"]
        .as_str()
        .expect("published evidence reference is a string");

    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();
    let workspace_id = id(1);
    let source_id = id(2);
    let catalog_publication_id = id(3);
    let coordinate_publication_id = id(4);
    seed_standard_published_reference(
        &pool,
        workspace_id,
        source_id,
        catalog_publication_id,
        coordinate_publication_id,
        now,
    )
    .await;

    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let active_mapping_id = id(10);
    let active = lifecycle
        .create(CreateMapping {
            mapping_id: active_mapping_id,
            mappings: graph_mappings
                .into_iter()
                .chain([vec![
                    published_reference.to_owned(),
                    "example.media:receiver".to_owned(),
                ]])
                .collect(),
            created_at: now,
        })
        .await
        .expect("active mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(11),
            workspace_id,
            mapping_id: active_mapping_id,
            content_hash: Some(active.content_hash.clone()),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("mapping revision is activated");

    lifecycle
        .create(CreateMapping {
            mapping_id: id(20),
            mappings: vec![vec![
                "imdb:title/tt0133093".to_owned(),
                "example.media:inactive".to_owned(),
            ]],
            created_at: now,
        })
        .await
        .expect("inactive decoy is created");

    let resolver =
        MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()));
    let graph = resolver
        .resolve_workspace_mappings(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: graph_references
                .iter()
                .map(|reference| endpoint(reference))
                .collect(),
            resolved_at: now,
        })
        .await
        .expect("one-to-one graph resolves");
    assert_eq!(graph.completeness, ResolutionCompleteness::Complete);
    let pins = graph
        .pins
        .as_ref()
        .expect("workspace-active resolution preserves its pins");
    assert_eq!(pins.revisions.len(), 1);
    assert_eq!(pins.revisions[0].mapping_id, active_mapping_id);
    assert_eq!(pins.revisions[0].content_hash, active.content_hash);
    assert_eq!(pins.revisions[0].activation_generation, Some(1));
    assert_eq!(pins.sources.len(), 1);
    assert_eq!(pins.sources[0].source_id, source_id);
    assert_eq!(pins.sources[0].connection_id, id(30));
    assert_eq!(pins.sources[0].attached_at, now);
    assert_eq!(pins.sources[0].source_capability_revision, 1);
    assert_eq!(pins.sources[0].connection_capability_revision, 1);
    assert_eq!(
        pins.sources[0].catalog_publication_id,
        Some(catalog_publication_id)
    );
    assert_eq!(
        pins.sources[0].coordinate_publication_id,
        Some(coordinate_publication_id)
    );
    assert_eq!(
        graph.correspondences.len(),
        graph_vector["expected"]["correspondences"]
            .as_u64()
            .expect("expected correspondence count") as usize
    );
    assert_eq!(
        graph.correspondences[0].endpoints,
        vec![endpoint("imdb:title/tt0133093"), endpoint("tmdb:movie/603")]
    );
    assert_eq!(
        graph.correspondences[0].proof.kind,
        trakkin::application::mapping_resolution::ResolutionProofKind::Direct
    );
    assert!(graph.correspondences[0].proof.parent_proofs.is_empty());
    assert_eq!(graph.correspondences[0].proof.transform.numerator, 1);
    assert_eq!(graph.correspondences[0].proof.transform.denominator, 1);
    assert_eq!(
        graph.correspondences[0].proof.revision_hash,
        active.content_hash
    );
    assert!(graph.findings.is_empty());

    let published = resolver
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: vec![endpoint(published_reference)],
            resolved_at: now,
        })
        .await
        .expect("published evidence resolves");
    assert_eq!(published.completeness, ResolutionCompleteness::Complete);
    assert_eq!(published.correspondences.len(), 1);
    assert_eq!(
        u64::from(published.lookup_calls),
        published_vector["expected"]["lookupCalls"]
            .as_u64()
            .expect("expected lookup call count")
    );
    assert_eq!(published.published_matches.len(), 1);
    assert_eq!(
        published.published_matches[0].backing,
        Some(CoordinateBacking::Materialized)
    );
    assert_eq!(
        published.correspondences[0].proof.catalog_publications,
        vec![catalog_publication_id]
    );
    assert_eq!(
        published.correspondences[0].proof.coordinate_publications,
        vec![coordinate_publication_id]
    );

    database.shutdown().await;
}

#[tokio::test]
async fn postgres_direct_selected_resolution_uses_exact_coordinate_evidence() {
    let graph_vector = fixture_vector(
        CORRESPONDENCE_VECTORS,
        "GRAPH-MATERIALIZED-VIRTUAL-AGGREGATE-001",
    );
    let graph_mappings =
        serde_json::from_value::<Vec<Vec<String>>>(graph_vector["mappings"].clone())
            .expect("graph vector mappings are strings");
    let graph_endpoints = graph_mappings[0]
        .iter()
        .map(|value| endpoint(value))
        .collect::<Vec<_>>();
    let missing_vector = fixture_vector(EVIDENCE_VECTORS, "EVIDENCE-MISSING-COORDINATE-001");
    let missing_endpoint = endpoint(
        missing_vector["input"]["endpoint"]
            .as_str()
            .expect("missing-coordinate endpoint is a string"),
    );

    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();
    let workspace_id = id(101);
    let source_id = id(102);
    let catalog_publication_id = id(103);
    let coordinate_publication_id = id(104);
    seed_standard_published_reference(
        &pool,
        workspace_id,
        source_id,
        catalog_publication_id,
        coordinate_publication_id,
        now,
    )
    .await;
    for (ordinal, (endpoint, evidence)) in graph_endpoints
        .iter()
        .zip(
            graph_vector["evidence"]
                .as_array()
                .expect("graph evidence is an array"),
        )
        .enumerate()
    {
        seed_catalog_endpoint(
            &pool,
            source_id,
            catalog_publication_id,
            coordinate_publication_id,
            endpoint,
            evidence["backing"].as_str(),
            ordinal,
            now,
        )
        .await;
    }
    seed_catalog_endpoint(
        &pool,
        source_id,
        catalog_publication_id,
        coordinate_publication_id,
        &missing_endpoint,
        None,
        graph_endpoints.len(),
        now,
    )
    .await;

    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let graph_mapping = lifecycle
        .create(CreateMapping {
            mapping_id: id(110),
            mappings: graph_mappings,
            created_at: now,
        })
        .await
        .expect("selected graph mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(111),
            workspace_id,
            mapping_id: id(110),
            content_hash: Some(graph_mapping.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("selected graph mapping is activated");
    let missing_mapping = lifecycle
        .create(CreateMapping {
            mapping_id: id(120),
            mappings: vec![vec![
                missing_endpoint.to_string(),
                "example.media:missing-peer[episode:1..12]".to_owned(),
            ]],
            created_at: now,
        })
        .await
        .expect("missing-coordinate mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(121),
            workspace_id,
            mapping_id: id(120),
            content_hash: Some(missing_mapping.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("missing-coordinate mapping is activated");

    let resolver =
        MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()));
    let graph = resolver
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: graph_endpoints.clone(),
            resolved_at: now,
        })
        .await
        .expect("selected graph resolves");
    assert_eq!(graph.completeness, ResolutionCompleteness::Complete);
    assert_eq!(graph.correspondences.len(), 1);
    let mut backings = graph
        .published_matches
        .iter()
        .filter_map(|evidence| evidence.backing)
        .collect::<Vec<_>>();
    backings.sort_unstable();
    assert_eq!(
        backings,
        vec![
            CoordinateBacking::Materialized,
            CoordinateBacking::Virtual,
            CoordinateBacking::Aggregate,
        ]
    );
    assert!(graph.findings.is_empty());

    let missing = resolver
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: vec![missing_endpoint.clone()],
            resolved_at: now,
        })
        .await
        .expect("missing-coordinate graph returns a finding");
    assert_eq!(missing.completeness, ResolutionCompleteness::Partial);
    assert_eq!(missing.findings.len(), 1);
    assert_eq!(missing.findings[0].endpoint, missing_endpoint);
    assert_eq!(
        missing.findings[0].kind,
        trakkin::application::mapping_resolution::ResolutionFindingKind::MissingCoordinateEvidence
    );

    sqlx::query(
        "INSERT INTO source_coordinate_bindings (
    source_id, publication_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value, selector, backing, observed_at, expires_at
) VALUES ($1, $2, 'fixture.item', $3, $4, $5, $6, 'materialized', $7, $8)
",
    )
    .bind(source_id)
    .bind(coordinate_publication_id)
    .bind(format!("selected-{}", graph_endpoints.len()).into_bytes())
    .bind(missing_endpoint.namespace())
    .bind(missing_endpoint.value())
    .bind(
        missing_endpoint
            .selector()
            .expect("expired endpoint has a selector")
            .as_str(),
    )
    .bind(now - time::Duration::seconds(1))
    .bind(now)
    .execute(&pool)
    .await
    .expect("expired coordinate evidence is seeded");
    let expired = resolver
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: vec![missing_endpoint.clone()],
            resolved_at: now + time::Duration::seconds(1),
        })
        .await
        .expect("expired coordinate evidence returns a finding");
    assert_eq!(expired.completeness, ResolutionCompleteness::Partial);
    assert_eq!(expired.lookup_calls, 0);
    assert_eq!(expired.findings.len(), 1);
    assert_eq!(expired.findings[0].endpoint, missing_endpoint);
    assert_eq!(
        expired.findings[0].kind,
        trakkin::application::mapping_resolution::ResolutionFindingKind::ExpiredEvidence
    );

    database.shutdown().await;
}

#[tokio::test]
async fn postgres_direct_resolution_tracks_successive_publications() {
    let vector = serde_json::from_str::<serde_json::Value>(EVIDENCE_VECTORS)
        .expect("evidence fixture is valid JSON")["publicationChange"]
        .clone();
    let mappings = serde_json::from_value::<Vec<Vec<String>>>(vector["mapping"].clone())
        .expect("publication-change mappings are strings");
    let requested = endpoint(&mappings[0][0]);
    let publications = vector["publications"]
        .as_array()
        .expect("publication changes are an array");
    let expected = vector["expected"]["resolutionOutputs"]
        .as_array()
        .expect("publication outputs are an array");

    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();
    let workspace_id = id(301);
    let source_id = id(302);
    let first_catalog_publication = id(303);
    let first_coordinate_publication = id(304);
    seed_standard_published_reference(
        &pool,
        workspace_id,
        source_id,
        first_catalog_publication,
        first_coordinate_publication,
        now,
    )
    .await;
    seed_named_catalog_endpoint(
        &pool,
        source_id,
        first_catalog_publication,
        first_coordinate_publication,
        &requested,
        expected[0]["providerItemKey"]
            .as_str()
            .expect("first provider item key is a string")
            .as_bytes(),
        publications[0]["receiverBinding"]["backing"].as_str(),
        now,
    )
    .await;

    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let mapping_id = id(310);
    let mapping = lifecycle
        .create(CreateMapping {
            mapping_id,
            mappings,
            created_at: now,
        })
        .await
        .expect("publication-change mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(311),
            workspace_id,
            mapping_id,
            content_hash: Some(mapping.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("publication-change mapping is activated");
    let resolver =
        MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()));

    let first = resolver
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: vec![requested.clone()],
            resolved_at: now,
        })
        .await
        .expect("first publication resolves");
    assert_publication_resolution(
        &first,
        &expected[0],
        first_catalog_publication,
        first_coordinate_publication,
    );

    let second_catalog_publication = id(305);
    let second_coordinate_publication = id(306);
    replace_selected_publication(
        &pool,
        source_id,
        second_catalog_publication,
        second_coordinate_publication,
        &requested,
        expected[1]["providerItemKey"]
            .as_str()
            .expect("second provider item key is a string")
            .as_bytes(),
        publications[1]["receiverBinding"]["backing"]
            .as_str()
            .expect("second backing is a string"),
        now + time::Duration::seconds(1),
    )
    .await;
    let second = resolver
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: vec![requested],
            resolved_at: now + time::Duration::seconds(1),
        })
        .await
        .expect("second publication resolves");
    assert_publication_resolution(
        &second,
        &expected[1],
        second_catalog_publication,
        second_coordinate_publication,
    );
    assert_ne!(first.published_matches, second.published_matches);

    database.shutdown().await;
}

#[tokio::test]
async fn postgres_direct_resolution_discards_proof_when_database_pins_change() {
    for (name, mutation) in [
        ("activationGeneration", DatabasePinMutation::Activation),
        ("attachmentSetDigest", DatabasePinMutation::Attachment),
        (
            "catalogPublication",
            DatabasePinMutation::CatalogPublication,
        ),
        (
            "coordinatePublication",
            DatabasePinMutation::CoordinatePublication,
        ),
        ("capabilityRevision", DatabasePinMutation::Capability),
    ] {
        let resolution = resolve_after_database_pin_mutation(mutation).await;
        assert_eq!(
            resolution.completeness,
            ResolutionCompleteness::Stale,
            "{name} mutation must return stale"
        );
        assert!(resolution.correspondences.is_empty());
        assert!(resolution.findings.is_empty());
        assert!(resolution.published_matches.is_empty());
        assert!(resolution.endpoint_lookup_matches.is_empty());
        assert_eq!(resolution.lookup_calls, 0);
    }
}

fn assert_publication_resolution(
    resolution: &trakkin::application::mapping_resolution::MappingResolution,
    expected: &serde_json::Value,
    catalog_publication_id: Uuid,
    coordinate_publication_id: Uuid,
) {
    assert_eq!(resolution.completeness, ResolutionCompleteness::Complete);
    assert_eq!(resolution.published_matches.len(), 1);
    assert_eq!(
        resolution.published_matches[0].item_key_value,
        expected["providerItemKey"]
            .as_str()
            .expect("provider item key is a string")
            .as_bytes()
    );
    assert_eq!(
        resolution.published_matches[0].backing,
        Some(
            match expected["backing"]
                .as_str()
                .expect("expected backing is a string")
            {
                "virtual" => CoordinateBacking::Virtual,
                "aggregate" => CoordinateBacking::Aggregate,
                backing => panic!("unsupported fixture backing {backing}"),
            }
        )
    );
    assert_eq!(
        resolution.correspondences[0].proof.catalog_publications,
        vec![catalog_publication_id]
    );
    assert_eq!(
        resolution.correspondences[0].proof.coordinate_publications,
        vec![coordinate_publication_id]
    );
}

async fn resolve_after_database_pin_mutation(
    mutation: DatabasePinMutation,
) -> trakkin::application::mapping_resolution::MappingResolution {
    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();
    let workspace_id = id(201);
    let source_id = id(202);
    seed_standard_published_reference(&pool, workspace_id, source_id, id(203), id(204), now).await;
    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let mapping_id = id(210);
    let mapping = lifecycle
        .create(CreateMapping {
            mapping_id,
            mappings: vec![vec![
                "example.media:published".to_owned(),
                "example.media:receiver".to_owned(),
            ]],
            created_at: now,
        })
        .await
        .expect("stale mapping is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(211),
            workspace_id,
            mapping_id,
            content_hash: Some(mapping.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("stale mapping is activated");
    let repository = MutatingResolutionRepository {
        inner: PostgresMappingResolutionRepository::new(pool.clone()),
        pool: pool.clone(),
        mutated: Arc::new(AtomicBool::new(false)),
        mutation,
    };
    let resolution = MappingResolutionService::new(repository)
        .resolve(ResolutionRequest {
            workspace_id,
            mode: ResolutionMode::Direct,
            window: None,
            references: vec![endpoint("example.media:published")],
            resolved_at: now,
        })
        .await
        .expect("stale resolution returns a closed result");

    database.shutdown().await;
    resolution
}

#[derive(Clone, Copy)]
enum DatabasePinMutation {
    Activation,
    Attachment,
    CatalogPublication,
    CoordinatePublication,
    Capability,
}

#[derive(Clone)]
struct MutatingResolutionRepository {
    inner: PostgresMappingResolutionRepository,
    pool: sqlx::PgPool,
    mutated: Arc<AtomicBool>,
    mutation: DatabasePinMutation,
}

impl MappingResolutionRepository for MutatingResolutionRepository {
    type Error =
        trakkin::infrastructure::mapping_resolution::PostgresMappingResolutionRepositoryError;

    fn load_direct_frontier(
        &self,
        request: LoadDirectFrontier,
    ) -> MappingResolutionFuture<'_, DirectFrontierSnapshot, Self::Error> {
        self.inner.load_direct_frontier(request)
    }

    fn recheck_direct_resolution(
        &self,
        workspace_id: Uuid,
        revision: Option<ExactMappingRevision>,
        expected: DirectResolutionPins,
    ) -> MappingResolutionFuture<'_, bool, Self::Error> {
        Box::pin(async move {
            if !self.mutated.swap(true, Ordering::SeqCst) {
                let statement = match self.mutation {
                    DatabasePinMutation::Activation => {
                        "UPDATE workspace_mapping_activations
SET generation = generation + 1
WHERE workspace_id = $1 AND active_content_hash IS NOT NULL
"
                    }
                    DatabasePinMutation::Attachment => {
                        "UPDATE workspace_sources
SET attached_at = attached_at + INTERVAL '1 second'
WHERE workspace_id = $1
"
                    }
                    DatabasePinMutation::CatalogPublication => {
                        "UPDATE source_streams
SET publication_id = '00000000-0000-0000-0000-0000000003e7'
WHERE source_id IN (
    SELECT source_id FROM workspace_sources WHERE workspace_id = $1
) AND stream_kind = 'catalog'
"
                    }
                    DatabasePinMutation::CoordinatePublication => {
                        "WITH attached AS (
    SELECT source_id FROM workspace_sources WHERE workspace_id = $1
), removed AS (
    DELETE FROM source_coordinate_bindings
    WHERE source_id IN (SELECT source_id FROM attached)
)
UPDATE source_coordinate_publications
SET publication_id = '00000000-0000-0000-0000-0000000003e6'
WHERE source_id IN (SELECT source_id FROM attached)
"
                    }
                    DatabasePinMutation::Capability => {
                        "UPDATE connections
SET capability_revision = capability_revision + 1
WHERE id IN (
    SELECT source.connection_id
    FROM workspace_sources AS attachment
    INNER JOIN sources AS source ON source.id = attachment.source_id
    WHERE attachment.workspace_id = $1
)
"
                    }
                };
                sqlx::query(statement)
                    .bind(workspace_id)
                    .execute(&self.pool)
                    .await?;
            }
            self.inner
                .recheck_direct_resolution(workspace_id, revision, expected)
                .await
        })
    }
}

async fn seed_standard_published_reference(
    pool: &sqlx::PgPool,
    workspace_id: Uuid,
    source_id: Uuid,
    catalog_publication_id: Uuid,
    coordinate_publication_id: Uuid,
    now: OffsetDateTime,
) {
    seed_published_reference(
        pool,
        PublishedSourceIds {
            workspace_id,
            connection_id: id(30),
            account_id: id(31),
            source_id,
            catalog_publication_id,
            coordinate_publication_id,
        },
        &endpoint("example.media:published"),
        b"published-item".as_slice(),
        now,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn seed_catalog_endpoint(
    pool: &sqlx::PgPool,
    source_id: Uuid,
    catalog_publication_id: Uuid,
    coordinate_publication_id: Uuid,
    endpoint: &PortableEndpoint,
    backing: Option<&str>,
    ordinal: usize,
    now: OffsetDateTime,
) {
    let item_key = format!("selected-{ordinal}").into_bytes();
    seed_named_catalog_endpoint(
        pool,
        source_id,
        catalog_publication_id,
        coordinate_publication_id,
        endpoint,
        &item_key,
        backing,
        now,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn seed_named_catalog_endpoint(
    pool: &sqlx::PgPool,
    source_id: Uuid,
    catalog_publication_id: Uuid,
    coordinate_publication_id: Uuid,
    endpoint: &PortableEndpoint,
    item_key: &[u8],
    backing: Option<&str>,
    now: OffsetDateTime,
) {
    sqlx::query(
        "INSERT INTO catalog_items (
    source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, publication_id, updated_at
) VALUES ($1, 'fixture.item', $2, 'fixture', 'item', 'Selected', $3, $4, $5)
",
    )
    .bind(source_id)
    .bind(item_key)
    .bind(json!({}))
    .bind(catalog_publication_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("selected catalog item is seeded");
    sqlx::query(
        "INSERT INTO catalog_item_references (
    source_id, item_key_namespace, item_key_value, reference_namespace, reference_value
) VALUES ($1, 'fixture.item', $2, $3, $4)
",
    )
    .bind(source_id)
    .bind(item_key)
    .bind(endpoint.namespace())
    .bind(endpoint.value())
    .execute(pool)
    .await
    .expect("selected portable reference is seeded");
    if let Some(backing) = backing {
        sqlx::query(
            "INSERT INTO source_coordinate_bindings (
    source_id, publication_id, item_key_namespace, item_key_value,
    reference_namespace, reference_value, selector, backing, observed_at
) VALUES ($1, $2, 'fixture.item', $3, $4, $5, $6, $7, $8)
",
        )
        .bind(source_id)
        .bind(coordinate_publication_id)
        .bind(item_key)
        .bind(endpoint.namespace())
        .bind(endpoint.value())
        .bind(
            endpoint
                .selector()
                .expect("selected endpoint has a selector")
                .as_str(),
        )
        .bind(backing)
        .bind(now)
        .execute(pool)
        .await
        .expect("selected coordinate binding is seeded");
    }
}

#[allow(clippy::too_many_arguments)]
async fn replace_selected_publication(
    pool: &sqlx::PgPool,
    source_id: Uuid,
    catalog_publication_id: Uuid,
    coordinate_publication_id: Uuid,
    endpoint: &PortableEndpoint,
    item_key: &[u8],
    backing: &str,
    now: OffsetDateTime,
) {
    sqlx::query(
        "DELETE FROM source_coordinate_bindings
    WHERE source_id = $1
    ",
    )
    .bind(source_id)
    .execute(pool)
    .await
    .expect("prior coordinate bindings are removed");
    sqlx::query(
        "DELETE FROM catalog_item_references
    WHERE source_id = $1
        AND reference_namespace = $2
        AND reference_value = $3
    ",
    )
    .bind(source_id)
    .bind(endpoint.namespace())
    .bind(endpoint.value())
    .execute(pool)
    .await
    .expect("prior selected reference is removed");
    sqlx::query(
        "DELETE FROM catalog_items
    WHERE source_id = $1 AND key_namespace = 'fixture.item' AND key_value LIKE 'receiver-v%'::BYTEA
    ",
    )
    .bind(source_id)
    .execute(pool)
    .await
    .expect("prior selected item is removed");
    sqlx::query(
        "UPDATE source_streams
    SET publication_id = $2, evidence_revision = $3, updated_at = $4
    WHERE source_id = $1 AND stream_kind = 'catalog'
    ",
    )
    .bind(source_id)
    .bind(catalog_publication_id)
    .bind(format!("catalog/{catalog_publication_id}").into_bytes())
    .bind(now)
    .execute(pool)
    .await
    .expect("catalog publication is replaced");
    sqlx::query(
        "UPDATE catalog_items
    SET publication_id = $2, updated_at = $3
    WHERE source_id = $1
    ",
    )
    .bind(source_id)
    .bind(catalog_publication_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("catalog projection is advanced");
    sqlx::query(
        "UPDATE source_coordinate_publications
    SET publication_id = $2, evidence_revision = $3, published_at = $4
    WHERE source_id = $1
    ",
    )
    .bind(source_id)
    .bind(coordinate_publication_id)
    .bind(format!("coordinates/{coordinate_publication_id}").into_bytes())
    .bind(now)
    .execute(pool)
    .await
    .expect("coordinate publication is replaced");
    seed_named_catalog_endpoint(
        pool,
        source_id,
        catalog_publication_id,
        coordinate_publication_id,
        endpoint,
        item_key,
        Some(backing),
        now,
    )
    .await;
}
