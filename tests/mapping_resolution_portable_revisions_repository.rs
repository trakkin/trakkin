#![cfg(feature = "embedded-database")]

#[path = "support/mapping_resolution_published_source.rs"]
mod published_source;
#[path = "support/mapping_resolution.rs"]
mod support;

use published_source::{PublishedSourceIds, seed_published_reference};
use support::{MappingResolutionDatabase, endpoint, fixture_time, id};
use trakkin::{
    application::{
        mapping_lifecycle::{
            ChangeMappingActivation, CreateMapping, ImportMapping, MappingActivationKind,
            MappingLifecycleService, UpdateMapping,
        },
        mapping_resolution::{
            ExactMappingRevision, MappingResolution, MappingResolutionService,
            ResolutionCompleteness, ResolutionMode, ResolutionProof, ResolutionRequest,
        },
    },
    infrastructure::{
        mapping_lifecycle::PostgresMappingLifecycleRepository,
        mapping_resolution::PostgresMappingResolutionRepository,
    },
};
use trakkin_mapping::{
    MappingDocument, PortableEndpoint, fixtures::TWO_INSTANCES_PORTABILITY_VECTOR,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_resolution_is_portable_across_independent_instances() {
    let vector = serde_json::from_str::<serde_json::Value>(TWO_INSTANCES_PORTABILITY_VECTOR)
        .expect("portability vector is valid JSON");
    let document = MappingDocument::from_json(&vector["document"].to_string())
        .expect("portable mapping document is valid");
    let requested = endpoint("example.media:original[episode:1..12]");
    let now = fixture_time();
    let first_ids = PortableInstanceIds {
        mapping_id: id(601),
        workspace_id: id(602),
        connection_id: id(603),
        account_id: id(604),
        source_id: id(605),
        catalog_publication_id: id(606),
        coordinate_publication_id: id(607),
    };
    let second_ids = PortableInstanceIds {
        mapping_id: id(701),
        workspace_id: id(702),
        connection_id: id(703),
        account_id: id(704),
        source_id: id(705),
        catalog_publication_id: id(706),
        coordinate_publication_id: id(707),
    };

    let first_database = MappingResolutionDatabase::start().await;
    let second_database = MappingResolutionDatabase::start().await;
    let first_pool = first_database.pool();
    let second_pool = second_database.pool();
    let first_database_process_id = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&first_pool)
        .await
        .expect("first database identity is readable");
    let second_database_process_id = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&second_pool)
        .await
        .expect("second database identity is readable");

    let first = execute_portable_instance(
        &first_pool,
        first_ids,
        document.clone(),
        requested.clone(),
        now,
    )
    .await;
    let second =
        execute_portable_instance(&second_pool, second_ids, document, requested, now).await;

    assert_ne!(first_database_process_id, second_database_process_id);
    assert_eq!(first.canonical_json, second.canonical_json);
    assert_eq!(
        first.canonical_json,
        vector["expectedCanonicalBytes"]
            .as_str()
            .expect("expected canonical bytes are strings")
    );
    assert_eq!(first.content_hash, second.content_hash);
    assert_eq!(
        first.content_hash,
        vector["expectedContentHash"]
            .as_str()
            .expect("expected content hash is a string")
    );
    for local_id in [
        first_ids.mapping_id,
        first_ids.workspace_id,
        first_ids.connection_id,
        first_ids.source_id,
        second_ids.mapping_id,
        second_ids.workspace_id,
        second_ids.connection_id,
        second_ids.source_id,
    ] {
        assert!(!first.canonical_json.contains(&local_id.to_string()));
    }

    assert_ne!(first.resolution, second.resolution);
    let first_resolution = without_local_resolution_ids(first.resolution);
    let second_resolution = without_local_resolution_ids(second.resolution);
    assert_eq!(
        first_resolution.correspondences,
        second_resolution.correspondences
    );
    assert_eq!(first_resolution.findings, second_resolution.findings);
    assert_eq!(
        first_resolution.published_matches,
        second_resolution.published_matches
    );
    assert_eq!(first_resolution, second_resolution);

    first_database.shutdown().await;
    second_database.shutdown().await;
}

#[tokio::test]
async fn postgres_exact_revision_resolution_ignores_active_workspace_revisions() {
    let database = MappingResolutionDatabase::start().await;
    let pool = database.pool();
    let now = fixture_time();
    let workspace_id = id(501);
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES ($1, 'Exact revision resolution', 'active', $2, $2)
",
    )
    .bind(workspace_id)
    .bind(now)
    .execute(&pool)
    .await
    .expect("workspace is seeded");

    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let mapping_id = id(502);
    let historical = lifecycle
        .create(CreateMapping {
            mapping_id,
            mappings: vec![vec![
                "example.media:a".to_owned(),
                "example.media:b".to_owned(),
            ]],
            created_at: now,
        })
        .await
        .expect("historical revision is created");
    let active = lifecycle
        .update(UpdateMapping {
            mapping_id,
            base_content_hash: historical.content_hash.clone(),
            mappings: vec![
                vec!["example.media:a".to_owned(), "example.media:b".to_owned()],
                vec!["example.media:b".to_owned(), "example.media:c".to_owned()],
            ],
            updated_at: now,
        })
        .await
        .expect("active successor is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(503),
            workspace_id,
            mapping_id,
            content_hash: Some(active.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("successor is activated");

    let decoy_mapping_id = id(504);
    let decoy = lifecycle
        .create(CreateMapping {
            mapping_id: decoy_mapping_id,
            mappings: vec![vec![
                "example.media:a".to_owned(),
                "example.media:decoy".to_owned(),
            ]],
            created_at: now,
        })
        .await
        .expect("active decoy is created");
    lifecycle
        .change_activation(ChangeMappingActivation {
            event_id: id(505),
            workspace_id,
            mapping_id: decoy_mapping_id,
            content_hash: Some(decoy.content_hash),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("decoy is activated");

    let resolution =
        MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()))
            .resolve_revision(
                ExactMappingRevision {
                    mapping_id,
                    content_hash: historical.content_hash.clone(),
                },
                ResolutionRequest {
                    workspace_id,
                    mode: ResolutionMode::Transitive,
                    references: vec![endpoint("example.media:a")],
                    window: None,
                    resolved_at: now,
                },
            )
            .await
            .expect("historical revision resolves");

    assert_eq!(resolution.completeness, ResolutionCompleteness::Complete);
    assert_eq!(resolution.correspondences.len(), 1);
    assert_eq!(
        resolution.correspondences[0].endpoints,
        vec![endpoint("example.media:a"), endpoint("example.media:b")]
    );
    assert!(resolution.correspondences.iter().all(|correspondence| {
        correspondence.proof.revision_hash == historical.content_hash
            && correspondence
                .supporting_proofs
                .iter()
                .all(|proof| proof.revision_hash == historical.content_hash)
    }));

    database.shutdown().await;
}

#[derive(Clone, Copy)]
struct PortableInstanceIds {
    mapping_id: Uuid,
    workspace_id: Uuid,
    connection_id: Uuid,
    account_id: Uuid,
    source_id: Uuid,
    catalog_publication_id: Uuid,
    coordinate_publication_id: Uuid,
}

impl PortableInstanceIds {
    fn source_ids(self) -> PublishedSourceIds {
        PublishedSourceIds {
            workspace_id: self.workspace_id,
            connection_id: self.connection_id,
            account_id: self.account_id,
            source_id: self.source_id,
            catalog_publication_id: self.catalog_publication_id,
            coordinate_publication_id: self.coordinate_publication_id,
        }
    }
}

struct PortableInstanceResolution {
    canonical_json: String,
    content_hash: String,
    resolution: MappingResolution,
}

async fn execute_portable_instance(
    pool: &sqlx::PgPool,
    ids: PortableInstanceIds,
    document: MappingDocument,
    requested: PortableEndpoint,
    now: time::OffsetDateTime,
) -> PortableInstanceResolution {
    seed_published_reference(
        pool,
        ids.source_ids(),
        &requested,
        b"original".as_slice(),
        now,
    )
    .await;
    let lifecycle =
        MappingLifecycleService::new(PostgresMappingLifecycleRepository::new(pool.clone()));
    let imported = lifecycle
        .import(ImportMapping {
            mapping_id: ids.mapping_id,
            document,
            parents: Vec::new(),
            imported_at: now,
        })
        .await
        .expect("portable document imports");
    let exported = lifecycle
        .revision(ids.mapping_id, &imported.content_hash)
        .await
        .expect("portable document exports");
    let resolution =
        MappingResolutionService::new(PostgresMappingResolutionRepository::new(pool.clone()))
            .resolve_revision(
                ExactMappingRevision {
                    mapping_id: ids.mapping_id,
                    content_hash: imported.content_hash.clone(),
                },
                ResolutionRequest {
                    workspace_id: ids.workspace_id,
                    mode: ResolutionMode::Direct,
                    references: vec![requested],
                    window: None,
                    resolved_at: now,
                },
            )
            .await
            .expect("portable document resolves");
    assert_eq!(resolution.completeness, ResolutionCompleteness::Complete);
    assert_eq!(resolution.correspondences.len(), 1);
    assert_eq!(resolution.published_matches.len(), 1);
    assert!(resolution.findings.is_empty());
    PortableInstanceResolution {
        canonical_json: String::from_utf8(exported.document.canonical_json())
            .expect("canonical JSON is UTF-8"),
        content_hash: exported.content_hash,
        resolution,
    }
}

fn without_local_resolution_ids(mut resolution: MappingResolution) -> MappingResolution {
    if let Some(pins) = &mut resolution.pins {
        for revision in &mut pins.revisions {
            revision.mapping_id = Uuid::nil();
        }
        for source in &mut pins.sources {
            source.source_id = Uuid::nil();
            source.connection_id = Uuid::nil();
            source.catalog_publication_id = source.catalog_publication_id.map(|_| Uuid::nil());
            source.coordinate_publication_id =
                source.coordinate_publication_id.map(|_| Uuid::nil());
        }
    }
    for published_match in &mut resolution.published_matches {
        published_match.source_id = Uuid::nil();
        published_match.catalog_publication_id = Uuid::nil();
        published_match.coordinate_publication_id = published_match
            .coordinate_publication_id
            .map(|_| Uuid::nil());
    }
    for lookup_match in &mut resolution.endpoint_lookup_matches {
        lookup_match.connection_id = Uuid::nil();
    }
    for correspondence in &mut resolution.correspondences {
        without_local_proof_ids(&mut correspondence.proof);
        for proof in &mut correspondence.supporting_proofs {
            without_local_proof_ids(proof);
        }
    }
    for finding in &mut resolution.findings {
        for proof in &mut finding.proof_paths {
            without_local_proof_ids(proof);
        }
    }
    resolution
}

fn without_local_proof_ids(proof: &mut ResolutionProof) {
    proof.catalog_publications.fill(Uuid::nil());
    proof.coordinate_publications.fill(Uuid::nil());
}
