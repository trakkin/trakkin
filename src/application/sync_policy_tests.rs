use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde_json::{Value, json};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::synchronization::{
    Quantizer, Reducer, ResolutionMode, STATE_REGISTRY_SCHEMA, SYNC_POLICY_SCHEMA, StateRegistry,
    SyncPolicyDocument, validate_sync_policy_hash,
};

use super::{
    AppendSyncPolicyRevision, CreateSyncPolicy, QuerySyncPolicies, StoreSyncPolicyRevision,
    SyncPolicyFuture, SyncPolicyLifecycleError, SyncPolicyPage, SyncPolicyRepository,
    SyncPolicyRevisionSnapshot, SyncPolicyService, UpdateSyncPolicy,
};

const FIXTURE_ROOT: &str = "tests/fixtures.sync/v1";

#[test]
fn state_registry_executes_core_and_gating_vectors() {
    let suite = read_json("registry/vectors.json");
    let registry = StateRegistry::v1();
    let core = vector(&suite, "STATE-REGISTRY-CORE-001");

    assert_eq!(registry.schema(), STATE_REGISTRY_SCHEMA);
    assert_eq!(registry.schema(), input(core, "registry").as_str().unwrap());
    let expected_fields = input(core, "fields").as_array().unwrap();
    for expected in expected_fields {
        let id = expected.get("id").and_then(Value::as_str).unwrap();
        let field = registry.field(id).unwrap();
        assert_eq!(field.shape().as_str(), expected["shape"].as_str().unwrap());
        assert_eq!(
            field
                .units()
                .iter()
                .map(|unit| unit.as_str())
                .collect::<Vec<_>>(),
            strings(&expected["units"])
        );
        assert_eq!(
            field
                .reducers()
                .iter()
                .map(|reducer| reducer.as_str())
                .collect::<Vec<_>>(),
            strings(&expected["reducers"])
        );
        assert_eq!(
            field
                .quantizers()
                .iter()
                .map(|quantizer| quantizer.as_str())
                .collect::<Vec<_>>(),
            strings(&expected["quantizers"])
        );
    }

    let gating = vector(&suite, "STATE-REGISTRY-GATING-001");
    for case in input(gating, "cases").as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let result = registry.validate_rule(
            case["field"].as_str().unwrap(),
            Reducer::parse(case["reducer"].as_str().unwrap()).unwrap(),
            Quantizer::parse(case["quantizer"].as_str().unwrap()).unwrap(),
        );
        let expected = expected(gating, "cases")
            .as_array()
            .unwrap()
            .iter()
            .find(|expected| expected["id"] == id)
            .unwrap();
        assert_eq!(result.is_ok(), expected["valid"].as_bool().unwrap());
        if let Some(finding) = expected.get("finding").and_then(Value::as_str) {
            assert_eq!(result.unwrap_err().code(), finding);
        }
    }

    let extension = vector(&suite, "STATE-EXTENSION-UNSUPPORTED-001");
    let error = registry
        .validate_rule(
            input(extension, "field").as_str().unwrap(),
            Reducer::Equal,
            Quantizer::Exact,
        )
        .unwrap_err();
    assert_eq!(
        error.code(),
        expected(extension, "finding").as_str().unwrap()
    );
}

#[tokio::test]
async fn sync_policy_executes_local_immutable_policy_vectors() {
    let suite = read_json("policy/vectors.json");
    let source_a = Uuid::from_u128(0xa1);
    let source_b = Uuid::from_u128(0xb2);
    let source_c = Uuid::from_u128(0xc3);
    let field_rule = input(vector(&suite, "POLICY-FIELD-RULES-001"), "fieldRule");
    let policy_json = json!({
        "contributors": [source_b, source_a],
        "receivers": [source_c, source_b],
        "resolutionMode": "transitive",
        "rules": [field_rule],
        "schema": SYNC_POLICY_SCHEMA,
    });
    let document = SyncPolicyDocument::from_json(&policy_json.to_string(), StateRegistry::v1())
        .expect("policy vectors should form a valid production document");

    assert_eq!(document.resolution_mode(), ResolutionMode::Transitive);
    assert_eq!(document.contributors(), &[source_a, source_b]);
    assert_eq!(document.receivers(), &[source_b, source_c]);
    assert!(document.contributes(source_b));
    assert!(document.receives(source_b));
    assert!(!document.receives(source_a));
    assert_eq!(document.rules().len(), 1);

    let participants = vector(&suite, "POLICY-PARTICIPANT-ROLES-001");
    assert_eq!(
        document.contributes(source_b),
        expected(participants, "sourceLocalBContributes")
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        document.receives(source_b),
        expected(participants, "sourceLocalBReceives")
            .as_bool()
            .unwrap()
    );

    let modes = vector(&suite, "POLICY-RESOLUTION-MODES-001");
    for mode in input(modes, "valid").as_array().unwrap() {
        let mode = mode.as_str().unwrap();
        let candidate = policy_json
            .to_string()
            .replace("\"transitive\"", &format!("\"{mode}\""));
        let parsed = SyncPolicyDocument::from_json(&candidate, StateRegistry::v1()).unwrap();
        assert_eq!(
            parsed.resolution_mode(),
            if mode == "direct" {
                ResolutionMode::Direct
            } else {
                ResolutionMode::Transitive
            }
        );
    }
    let mut embedded_mapping = policy_json.clone();
    embedded_mapping
        .as_object_mut()
        .unwrap()
        .insert("mappingRevision".to_owned(), json!(1));
    assert_eq!(
        SyncPolicyDocument::from_json(&embedded_mapping.to_string(), StateRegistry::v1())
            .unwrap_err()
            .code(),
        "unknown_field"
    );

    let canonical = document.canonical_json();
    let reordered = json!({
        "schema": SYNC_POLICY_SCHEMA,
        "rules": [field_rule],
        "resolutionMode": "transitive",
        "receivers": [source_b, source_c],
        "contributors": [source_a, source_b],
    });
    let equivalent = SyncPolicyDocument::from_json(&reordered.to_string(), StateRegistry::v1())
        .expect("equivalent policy should parse");
    let expected_canonical = concat!(
        "{\"contributors\":[\"00000000-0000-0000-0000-0000000000a1\",",
        "\"00000000-0000-0000-0000-0000000000b2\"],",
        "\"receivers\":[\"00000000-0000-0000-0000-0000000000b2\",",
        "\"00000000-0000-0000-0000-0000000000c3\"],",
        "\"resolutionMode\":\"transitive\",\"rules\":[{",
        "\"conflict\":\"require_reducer\",\"presence\":{",
        "\"absent\":\"clear\",\"deleted\":\"ignore\",",
        "\"stale\":\"block\",\"unknown\":\"block\"},",
        "\"reducer\":\"coverage_contiguous_prefix\",",
        "\"semanticField\":\"trakkin.state:progress\",",
        "\"translation\":{\"allowLossy\":false,\"quantizer\":\"nearest\"},",
        "\"writeConstraints\":{\"allowClear\":true,",
        "\"allowCreateMembership\":false,\"requireStrongPrecondition\":true}}],",
        "\"schema\":\"trakkin.sync-policy/v1\"}"
    );
    assert_eq!(canonical, expected_canonical.as_bytes());
    assert_eq!(equivalent.canonical_json(), canonical);
    assert_eq!(equivalent.content_hash(), document.content_hash());
    assert_eq!(
        document.content_hash(),
        "sha256:8ec76c2d8c001c1b4fd74ccb59cd165361c9ec0c35eefd5d1aab2ec9c4fe1eff"
    );

    let immutable = vector(&suite, "POLICY-IMMUTABLE-LOCAL-001");
    validate_sync_policy_hash(input(immutable, "baseContentHash").as_str().unwrap()).unwrap();
    let repository = TestPolicyRepository::default();
    let service = SyncPolicyService::new(repository.clone());
    let policy_id = Uuid::from_u128(0xd4);
    let workspace_id = Uuid::from_u128(0xe5);
    let created_at = OffsetDateTime::UNIX_EPOCH;
    let first = service
        .create(CreateSyncPolicy {
            policy_id,
            workspace_id,
            document: document.clone(),
            created_at,
        })
        .await
        .unwrap();
    assert_eq!(first.revision, 1);
    let duplicate = service
        .create(CreateSyncPolicy {
            policy_id,
            workspace_id,
            document: document.clone(),
            created_at,
        })
        .await
        .unwrap_err();
    assert_eq!(
        duplicate,
        SyncPolicyLifecycleError::Repository("policy exists")
    );

    let successor_json = policy_json
        .to_string()
        .replace("\"allowLossy\":false", "\"allowLossy\":true");
    let successor = SyncPolicyDocument::from_json(&successor_json, StateRegistry::v1()).unwrap();
    let second = service
        .update(UpdateSyncPolicy {
            policy_id,
            workspace_id,
            base_content_hash: first.content_hash.clone(),
            document: successor,
            updated_at: created_at,
        })
        .await
        .unwrap();
    assert_eq!(second.revision, 2);
    assert_ne!(second.content_hash, first.content_hash);
    assert_eq!(repository.revisions()[0], first);

    let stale = service
        .update(UpdateSyncPolicy {
            policy_id,
            workspace_id,
            base_content_hash: first.content_hash.clone(),
            document,
            updated_at: created_at,
        })
        .await
        .unwrap_err();
    assert_eq!(
        stale,
        SyncPolicyLifecycleError::HeadMoved {
            current_head: second.content_hash,
        }
    );

    let corrupt_repository = TestPolicyRepository::default();
    corrupt_repository
        .revisions
        .lock()
        .unwrap()
        .push(SyncPolicyRevisionSnapshot {
            content_hash: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_owned(),
            ..first
        });
    let corrupt = SyncPolicyService::new(corrupt_repository)
        .head(workspace_id, policy_id)
        .await
        .unwrap_err();
    assert_eq!(corrupt, SyncPolicyLifecycleError::RepositoryInvariant);
}

#[derive(Clone, Default)]
struct TestPolicyRepository {
    revisions: Arc<Mutex<Vec<SyncPolicyRevisionSnapshot>>>,
}

impl TestPolicyRepository {
    fn revisions(&self) -> Vec<SyncPolicyRevisionSnapshot> {
        self.revisions.lock().unwrap().clone()
    }

    fn snapshot(revision: StoreSyncPolicyRevision) -> SyncPolicyRevisionSnapshot {
        SyncPolicyRevisionSnapshot {
            policy_id: revision.policy_id,
            workspace_id: revision.workspace_id,
            revision: revision.revision,
            content_hash: revision.document.content_hash(),
            document: revision.document,
            created_at: revision.created_at,
        }
    }
}

impl SyncPolicyRepository for TestPolicyRepository {
    type Error = &'static str;

    fn get_head(
        &self,
        workspace_id: Uuid,
        policy_id: Uuid,
    ) -> SyncPolicyFuture<'_, Option<SyncPolicyRevisionSnapshot>, Self::Error> {
        let head = self
            .revisions
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|revision| {
                revision.workspace_id == workspace_id && revision.policy_id == policy_id
            })
            .cloned();
        Box::pin(async move { Ok(head) })
    }

    fn list_heads(
        &self,
        query: QuerySyncPolicies,
    ) -> SyncPolicyFuture<'_, SyncPolicyPage, Self::Error> {
        let mut policies = self
            .revisions
            .lock()
            .unwrap()
            .iter()
            .filter(|revision| revision.workspace_id == query.workspace_id)
            .cloned()
            .fold(
                Vec::<SyncPolicyRevisionSnapshot>::new(),
                |mut heads, revision| {
                    if let Some(head) = heads
                        .iter_mut()
                        .find(|head| head.policy_id == revision.policy_id)
                    {
                        *head = revision;
                    } else {
                        heads.push(revision);
                    }
                    heads
                },
            );
        policies.sort_by_key(|policy| policy.policy_id);
        if let Some(after_policy_id) = query.after_policy_id {
            policies.retain(|policy| policy.policy_id > after_policy_id);
        }
        let has_more = policies.len() > query.limit as usize;
        policies.truncate(query.limit as usize);
        let next_cursor = has_more.then(|| policies.last().unwrap().policy_id);
        Box::pin(async move {
            Ok(SyncPolicyPage {
                policies,
                next_cursor,
            })
        })
    }

    fn create_policy(
        &self,
        revision: StoreSyncPolicyRevision,
    ) -> SyncPolicyFuture<'_, SyncPolicyRevisionSnapshot, Self::Error> {
        let revisions = Arc::clone(&self.revisions);
        Box::pin(async move {
            let mut stored = revisions.lock().unwrap();
            if stored.iter().any(|stored| {
                stored.workspace_id == revision.workspace_id
                    && stored.policy_id == revision.policy_id
            }) {
                return Err(SyncPolicyLifecycleError::Repository("policy exists"));
            }
            let snapshot = Self::snapshot(revision);
            stored.push(snapshot.clone());
            Ok(snapshot)
        })
    }

    fn append_revision(
        &self,
        revision: AppendSyncPolicyRevision,
    ) -> SyncPolicyFuture<'_, SyncPolicyRevisionSnapshot, Self::Error> {
        let revisions = Arc::clone(&self.revisions);
        Box::pin(async move {
            let mut stored = revisions.lock().unwrap();
            let head = stored.last().ok_or("missing head")?;
            if head.content_hash != revision.expected_head {
                return Err(SyncPolicyLifecycleError::Repository("head moved"));
            }
            let snapshot = Self::snapshot(revision.revision);
            stored.push(snapshot.clone());
            Ok(snapshot)
        })
    }
}

fn vector<'a>(suite: &'a Value, id: &str) -> &'a Value {
    suite["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|vector| vector["id"] == id)
        .unwrap_or_else(|| panic!("missing vector: {id}"))
}

fn input<'a>(vector: &'a Value, field: &str) -> &'a Value {
    &vector["input"][field]
}

fn expected<'a>(vector: &'a Value, field: &str) -> &'a Value {
    &vector["expected"][field]
}

fn strings(value: &Value) -> Vec<&str> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect()
}

fn read_json(relative: &str) -> Value {
    let path = fixture_path(relative);
    serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap()
}

fn fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(FIXTURE_ROOT)
        .join(relative)
}
