#![cfg(feature = "embedded-database")]

use std::collections::BTreeMap;

use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::mapping_lifecycle::{
        AppendMappingRevision, ChangeMappingActivation, CreateMapping, ForkMapping, ImportMapping,
        ImportMappingPackage, MappingActivationKind, MappingLifecycleError,
        MappingLifecycleRepository, MappingLifecycleService, MappingRevisionOrigin,
        MappingRevisionParent, StoreMappingRevision, UpdateMapping,
    },
    database::DatabaseRuntime,
    infrastructure::mapping_lifecycle::PostgresMappingLifecycleRepository,
};
use trakkin_mapping_contract::{MappingDocument, MappingPackage, PackageCompatibility};
use uuid::Uuid;

const REGISTRY_DIGEST: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn postgres_mapping_lifecycle_executes_revision_and_activation_vectors() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");
    let pool = database.pool();
    let repository = PostgresMappingLifecycleRepository::new(pool.clone());
    let service = MappingLifecycleService::new(repository.clone());
    let now = OffsetDateTime::from_unix_timestamp(1_893_456_245).expect("fixture time");
    let workspace_id = id(1);
    let mapping_id = id(10);
    sqlx::query(
        "INSERT INTO workspaces (id, name, status, created_at, updated_at)
VALUES ($1, 'Mappings', 'active', $2, $2)
",
    )
    .bind(workspace_id)
    .bind(now)
    .execute(&pool)
    .await
    .expect("workspace should be inserted");

    let original_rows = vec![vec![
        "imdb:title/tt0133093".to_owned(),
        "tmdb:movie/603".to_owned(),
    ]];
    let created = service
        .create(CreateMapping {
            mapping_id,
            mappings: original_rows.clone(),
            created_at: now,
        })
        .await
        .expect("mapping should be created");
    let first_hash = created.content_hash.clone();
    assert_eq!(created.document.revision(), 1);
    assert!(created.parents.is_empty());
    assert_eq!(created.origin, MappingRevisionOrigin::Created);
    assert!(!created.content_reused);

    let original_bytes = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT canonical_json
FROM mapping_revisions
WHERE content_hash = $1
",
    )
    .bind(&first_hash)
    .fetch_one(&pool)
    .await
    .expect("original revision bytes should be stored");

    let updated_rows = vec![
        original_rows[0].clone(),
        vec!["example.media:a".to_owned(), "example.media:b".to_owned()],
    ];
    let updated = service
        .update(UpdateMapping {
            mapping_id,
            base_content_hash: first_hash.clone(),
            mappings: updated_rows.clone(),
            updated_at: now,
        })
        .await
        .expect("mapping should be updated");
    let second_hash = updated.content_hash.clone();
    assert_eq!(updated.document.revision(), 2);
    assert_eq!(updated.parents.len(), 1);
    assert_eq!(updated.parents[0].content_hash, first_hash);
    assert!(updated.parents[0].verified);
    assert_eq!(updated.origin, MappingRevisionOrigin::Updated);

    let writes_before = linked_revision_count(&pool, mapping_id).await;
    let stale_rows = vec![
        updated_rows[0].clone(),
        updated_rows[1].clone(),
        vec!["example.media:c".to_owned(), "example.media:d".to_owned()],
    ];
    let stale = service
        .update(UpdateMapping {
            mapping_id,
            base_content_hash: created.content_hash.clone(),
            mappings: stale_rows,
            updated_at: now,
        })
        .await
        .expect_err("stale base should fail");
    let expected_added: Vec<String> = MappingDocument::from_mappings(
        1,
        vec![vec![
            "example.media:c".to_owned(),
            "example.media:d".to_owned(),
        ]],
    )
    .expect("expected diff row is valid")
    .mappings()[0]
        .iter()
        .map(ToString::to_string)
        .collect();
    assert!(matches!(
        stale,
        MappingLifecycleError::HeadMoved {
            current_head,
            diff,
        } if current_head == second_hash && diff.added == vec![expected_added] && diff.removed.is_empty()
    ));
    assert_eq!(
        linked_revision_count(&pool, mapping_id).await,
        writes_before
    );

    let duplicate = service
        .import(ImportMapping {
            mapping_id: id(11),
            document: MappingDocument::from_mappings(1, original_rows.clone())
                .expect("duplicate document should parse"),
            parents: Vec::new(),
            imported_at: now,
        })
        .await
        .expect("exact duplicate should import");
    assert_eq!(duplicate.content_hash, created.content_hash);
    assert!(duplicate.content_reused);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
FROM mapping_revisions
WHERE content_hash = $1
",
        )
        .bind(&created.content_hash)
        .fetch_one(&pool)
        .await
        .expect("revision count should be queryable"),
        1
    );

    let semantic_duplicate = service
        .import(ImportMapping {
            mapping_id: id(12),
            document: MappingDocument::from_mappings(9, original_rows.clone())
                .expect("semantic duplicate should parse"),
            parents: Vec::new(),
            imported_at: now,
        })
        .await
        .expect("semantic duplicate should import");
    assert_ne!(semantic_duplicate.content_hash, created.content_hash);
    assert_eq!(
        semantic_duplicate.document.semantic_fingerprint(),
        created.document.semantic_fingerprint()
    );

    let external_parent = format!("sha256:{}", "1".repeat(64));
    let incomplete = service
        .import(ImportMapping {
            mapping_id: id(13),
            document: MappingDocument::from_mappings(7, updated_rows.clone())
                .expect("imported document should parse"),
            parents: vec![external_parent.clone()],
            imported_at: now,
        })
        .await
        .expect("incomplete ancestry should import");
    assert_eq!(incomplete.parents[0].content_hash, external_parent);
    assert!(!incomplete.parents[0].verified);

    let fork = service
        .fork(ForkMapping {
            source_mapping_id: mapping_id,
            selected_content_hash: created.content_hash.clone(),
            new_mapping_id: id(14),
            mappings: vec![
                original_rows[0].clone(),
                vec![
                    "example.media:fork".to_owned(),
                    "example.media:new".to_owned(),
                ],
            ],
            forked_at: now,
        })
        .await
        .expect("non-head revision should fork");
    assert_eq!(fork.document.revision(), 2);
    assert_eq!(fork.origin, MappingRevisionOrigin::Forked);
    assert_eq!(fork.parents[0].content_hash, created.content_hash);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT head_content_hash FROM mappings WHERE id = $1")
            .bind(mapping_id)
            .fetch_one(&pool)
            .await
            .expect("original head should be queryable"),
        updated.content_hash
    );

    let exported = service
        .revision(mapping_id, &created.content_hash)
        .await
        .expect("exact revision should be readable");
    assert_eq!(exported.document.canonical_json(), original_bytes);
    assert_eq!(
        MappingDocument::from_yaml(&exported.document.canonical_yaml())
            .expect("canonical YAML should round trip"),
        exported.document
    );

    let self_parent_document = MappingDocument::from_mappings(3, original_rows.clone())
        .expect("self-parent document should parse");
    let self_parent_hash = self_parent_document.content_hash();
    assert!(matches!(
        service
            .import(ImportMapping {
                mapping_id: id(15),
                document: self_parent_document,
                parents: vec![self_parent_hash],
                imported_at: now,
            })
            .await,
        Err(MappingLifecycleError::LineageCycle)
    ));

    let cycle_mapping_id = id(17);
    let cycle_one = MappingDocument::from_mappings(
        1,
        vec![vec![
            "cycle.example:one".to_owned(),
            "cycle.example:shared".to_owned(),
        ]],
    )
    .expect("first cycle document should parse");
    let cycle_two = MappingDocument::from_mappings(
        2,
        vec![vec![
            "cycle.example:two".to_owned(),
            "cycle.example:shared".to_owned(),
        ]],
    )
    .expect("second cycle document should parse");
    let cycle_three = MappingDocument::from_mappings(
        3,
        vec![vec![
            "cycle.example:three".to_owned(),
            "cycle.example:shared".to_owned(),
        ]],
    )
    .expect("third cycle document should parse");
    let cycle_one_hash = cycle_one.content_hash();
    let cycle_two_hash = cycle_two.content_hash();
    let cycle_three_hash = cycle_three.content_hash();
    repository
        .create_mapping(StoreMappingRevision {
            mapping_id: cycle_mapping_id,
            document: cycle_one,
            parents: vec![MappingRevisionParent {
                content_hash: cycle_two_hash.clone(),
                verified: false,
            }],
            origin: MappingRevisionOrigin::Imported,
            package: None,
            created_at: now,
        })
        .await
        .expect("first cycle node should store while its parent is external");
    repository
        .append_revision(AppendMappingRevision {
            expected_head: cycle_one_hash.clone(),
            revision: StoreMappingRevision {
                mapping_id: cycle_mapping_id,
                document: cycle_two,
                parents: vec![MappingRevisionParent {
                    content_hash: cycle_three_hash,
                    verified: false,
                }],
                origin: MappingRevisionOrigin::Imported,
                package: None,
                created_at: now,
            },
        })
        .await
        .expect("second cycle node should store while its parent is external");
    assert!(matches!(
        repository
            .append_revision(AppendMappingRevision {
                expected_head: cycle_two_hash,
                revision: StoreMappingRevision {
                    mapping_id: cycle_mapping_id,
                    document: cycle_three,
                    parents: vec![MappingRevisionParent {
                        content_hash: cycle_one_hash,
                        verified: true,
                    }],
                    origin: MappingRevisionOrigin::Imported,
                    package: None,
                    created_at: now,
                },
            })
            .await,
        Err(MappingLifecycleError::LineageCycle)
    ));
    assert_eq!(linked_revision_count(&pool, cycle_mapping_id).await, 2);

    let packaged_parent_document = MappingDocument::from_mappings(
        1,
        vec![vec![
            "example.media:package-parent".to_owned(),
            "example.media:shared".to_owned(),
        ]],
    )
    .expect("package parent document should parse");
    let packaged_parent_hash = packaged_parent_document.content_hash();
    let package_document = MappingDocument::from_mappings(
        1,
        vec![
            vec![
                "example.media:double-length[episode:1..*2]".to_owned(),
                "example.media:weekly[episode:1..]".to_owned(),
            ],
            original_rows[0].clone(),
        ],
    )
    .expect("package document should parse");
    let package_hash = package_document.content_hash();
    let package_manifest = serde_json::json!({
        "schema": "trakkin.mapping-package/v1",
        "documents": [
            {"path": "mappings/parent.json", "hash": packaged_parent_hash},
            {"path": "mappings/signal.json", "hash": package_hash},
        ],
        "roots": [external_parent],
        "parents": {
            package_hash.clone(): [packaged_parent_hash.clone(), external_parent.clone()],
        },
        "licenseExpression": "MIT",
        "attribution": [{"name": "Example Mapping Authors"}],
        "signatures": [{"kind": "sigstore", "digest": package_hash}],
        "forge": {"commit": "0123456789abcdef"},
        "requires": {
            "mappingSchema": "trakkin.mapping/v1",
            "resolver": ">=1.0 <2.0",
            "coordinateRegistryDigest": REGISTRY_DIGEST,
        },
    });
    let package = MappingPackage::from_json(&package_manifest.to_string())
        .expect("package manifest should parse");
    let package_import = service
        .import_package(ImportMappingPackage {
            package_id: id(100),
            package,
            documents: BTreeMap::from([
                ("mappings/parent.json".to_owned(), packaged_parent_document),
                ("mappings/signal.json".to_owned(), package_document),
            ]),
            mapping_ids: BTreeMap::from([
                ("mappings/parent.json".to_owned(), id(16)),
                ("mappings/signal.json".to_owned(), id(18)),
            ]),
            compatibility: PackageCompatibility::new(
                "trakkin.mapping/v1",
                "1.0.0",
                REGISTRY_DIGEST,
            ),
            imported_at: now,
        })
        .await
        .expect("package should import atomically");
    assert_eq!(package_import.len(), 2);
    let imported_child = package_import
        .iter()
        .find(|revision| revision.content_hash == package_hash)
        .expect("package child should be imported");
    assert_eq!(imported_child.origin, MappingRevisionOrigin::Imported);
    let package_metadata = imported_child
        .package
        .as_ref()
        .expect("package provenance should round trip");
    assert_eq!(package_metadata.license_expression, "MIT");
    assert_eq!(package_metadata.document_path, "mappings/signal.json");
    assert_eq!(imported_child.parents.len(), 2);
    assert!(
        imported_child
            .parents
            .windows(2)
            .all(|pair| pair[0].content_hash < pair[1].content_hash)
    );
    assert!(
        imported_child
            .parents
            .iter()
            .any(|parent| parent.content_hash == packaged_parent_hash && parent.verified)
    );
    assert!(
        imported_child
            .parents
            .iter()
            .any(|parent| parent.content_hash == external_parent && !parent.verified)
    );

    assert_eq!(row_count(&pool, &created.content_hash).await, 1);
    assert_eq!(endpoint_count(&pool, &created.content_hash).await, 2);
    assert_eq!(row_count(&pool, &updated.content_hash).await, 2);
    assert_eq!(endpoint_count(&pool, &updated.content_hash).await, 4);

    let activated = service
        .change_activation(ChangeMappingActivation {
            event_id: id(200),
            workspace_id,
            mapping_id,
            content_hash: Some(updated.content_hash.clone()),
            kind: MappingActivationKind::Activate,
            changed_at: now,
        })
        .await
        .expect("revision should activate");
    assert_eq!(activated.generation, 1);
    let rolled_back = service
        .change_activation(ChangeMappingActivation {
            event_id: id(201),
            workspace_id,
            mapping_id,
            content_hash: Some(created.content_hash.clone()),
            kind: MappingActivationKind::Rollback,
            changed_at: now,
        })
        .await
        .expect("revision should roll back");
    assert_eq!(rolled_back.generation, 2);
    let retired = service
        .change_activation(ChangeMappingActivation {
            event_id: id(202),
            workspace_id,
            mapping_id,
            content_hash: None,
            kind: MappingActivationKind::Retire,
            changed_at: now,
        })
        .await
        .expect("mapping should retire");
    assert_eq!(retired.generation, 3);
    assert!(retired.active_content_hash.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
FROM workspace_mapping_activation_events
WHERE workspace_id = $1 AND mapping_id = $2
",
        )
        .bind(workspace_id)
        .bind(mapping_id)
        .fetch_one(&pool)
        .await
        .expect("activation events should be queryable"),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT canonical_json
FROM mapping_revisions
WHERE content_hash = $1
",
        )
        .bind(&created.content_hash)
        .fetch_one(&pool)
        .await
        .expect("original revision bytes should remain queryable"),
        original_bytes
    );

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}

async fn linked_revision_count(pool: &sqlx::PgPool, mapping_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)
FROM mapping_record_revisions
WHERE mapping_id = $1
",
    )
    .bind(mapping_id)
    .fetch_one(pool)
    .await
    .expect("linked revision count should be queryable")
}

async fn row_count(pool: &sqlx::PgPool, content_hash: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM mapping_rows WHERE content_hash = $1")
        .bind(content_hash)
        .fetch_one(pool)
        .await
        .expect("mapping row count should be queryable")
}

async fn endpoint_count(pool: &sqlx::PgPool, content_hash: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM mapping_row_endpoints WHERE content_hash = $1")
        .bind(content_hash)
        .fetch_one(pool)
        .await
        .expect("mapping endpoint count should be queryable")
}

const fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
