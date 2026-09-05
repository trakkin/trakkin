use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tower::ServiceExt;
use trakkin::{
    application::{
        adapter_runtime::{
            AdapterAccount, AdapterAuthenticationMethod, AdapterAuthenticationPrompt,
            AdapterAuthenticationStatus, AdapterAuthenticationStep, AdapterBinaryAsset,
            AdapterConfigurationField, AdapterConfigurationOption, AdapterConnectionDescription,
            AdapterConnectionValidation, AdapterKey, AdapterLookupCandidate, AdapterLookupEvidence,
            AdapterLookupOutcome, AdapterPortableReferenceLookup, AdapterSecretPatch, AdapterTerm,
            AdapterValue, AdapterValueKind,
        },
        assets::{AssetFuture, BinaryAssetInteraction, ReadPublishedBinaryAsset},
        connection::{
            CancelConnectionAuthentication, ConnectionFuture, ConnectionInspection,
            ConnectionInteraction, ContinueConnectionAuthentication, InspectConnection,
            StartConnectionAuthentication, StartedConnectionAuthentication,
        },
        error::{ApplicationError, ApplicationErrorCode},
        providers::{
            ChangeProviderUpdatePolicy, InstallProvider, ProviderApiFuture, ProviderRegistryApi,
            ProviderSnapshot, ProviderSourceSnapshot, RefreshProviderSource, RemoveProvider,
            RemoveProviderSource, SharedProviderRegistry, TrustProviderSource, UpdateProvider,
        },
        reference_lookup::{
            LookupPortableReferences, ReferenceLookupFuture, ReferenceLookupInteraction,
        },
    },
    domain::acquisition::{
        BinaryAssetKey, BinaryAssetReference, PortableReference, ProviderItem, ProviderItemKey,
        Term,
    },
};
use uuid::Uuid;

pub const CONNECTION_ID: Uuid = Uuid::from_u128(1);
const ACCOUNT_ID: Uuid = Uuid::from_u128(2);
pub const SOURCE_ID: Uuid = Uuid::from_u128(3);
pub const CATALOG_PUBLICATION_ID: Uuid = Uuid::from_u128(4);
pub const STATE_PUBLICATION_ID: Uuid = Uuid::from_u128(5);

pub fn static_provider_registry() -> SharedProviderRegistry {
    Arc::new(StaticProviderRegistry)
}

struct StaticProviderRegistry;

impl ProviderRegistryApi for StaticProviderRegistry {
    fn ensure_provider_installed(&self, _provider_id: &str) -> ProviderApiFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn list_provider_sources(&self) -> ProviderApiFuture<'_, Vec<ProviderSourceSnapshot>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn trust_provider_source(
        &self,
        _command: TrustProviderSource,
    ) -> ProviderApiFuture<'_, ProviderSourceSnapshot> {
        unsupported_provider_operation()
    }

    fn refresh_provider_source(
        &self,
        _command: RefreshProviderSource,
    ) -> ProviderApiFuture<'_, ProviderSourceSnapshot> {
        unsupported_provider_operation()
    }

    fn remove_provider_source(&self, _command: RemoveProviderSource) -> ProviderApiFuture<'_, ()> {
        unsupported_provider_operation()
    }

    fn list_providers(&self) -> ProviderApiFuture<'_, Vec<ProviderSnapshot>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn install_provider(
        &self,
        _command: InstallProvider,
    ) -> ProviderApiFuture<'_, ProviderSnapshot> {
        unsupported_provider_operation()
    }

    fn update_provider(&self, _command: UpdateProvider) -> ProviderApiFuture<'_, ProviderSnapshot> {
        unsupported_provider_operation()
    }

    fn change_provider_update_policy(
        &self,
        _command: ChangeProviderUpdatePolicy,
    ) -> ProviderApiFuture<'_, ProviderSnapshot> {
        unsupported_provider_operation()
    }

    fn remove_provider(&self, _command: RemoveProvider) -> ProviderApiFuture<'_, ()> {
        unsupported_provider_operation()
    }
}

fn unsupported_provider_operation<T>() -> ProviderApiFuture<'static, T> {
    Box::pin(async {
        Err(ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            "provider operation is unavailable in this test",
        ))
    })
}

pub struct StaticConnectionInteraction;

impl ConnectionInteraction for StaticConnectionInteraction {
    fn inspect_connection(
        &self,
        _command: InspectConnection,
    ) -> ConnectionFuture<'_, ConnectionInspection, ApplicationError> {
        Box::pin(async {
            Ok(ConnectionInspection {
                description: AdapterConnectionDescription {
                    fields: vec![secret_configuration_field()],
                },
                validation: AdapterConnectionValidation {
                    field_problems: Vec::new(),
                },
                authentication_methods: vec![AdapterAuthenticationMethod {
                    key: "fixture/device".to_owned(),
                    display_name: "Device".to_owned(),
                    interactions: vec![AdapterTerm {
                        namespace: "fixture".to_owned(),
                        name: "browser".to_owned(),
                    }],
                    fields: Vec::new(),
                }],
                settings: Vec::new(),
                configured_secret_keys: Vec::new(),
            })
        })
    }

    fn start_connection_authentication(
        &self,
        command: StartConnectionAuthentication,
    ) -> ConnectionFuture<'_, StartedConnectionAuthentication, ApplicationError> {
        Box::pin(async move {
            let cancellable = command.method == "fixture/cancel";
            Ok(StartedConnectionAuthentication {
                authentication_id: command.authentication_id,
                step: AdapterAuthenticationStep {
                    authentication_id: if cancellable {
                        "provider-cancel-auth"
                    } else {
                        "provider-device-auth"
                    }
                    .to_owned(),
                    status: if cancellable {
                        AdapterAuthenticationStatus::Waiting
                    } else {
                        AdapterAuthenticationStatus::InputRequired
                    },
                    prompt: (!cancellable).then(|| AdapterAuthenticationPrompt {
                        message: "Enter the fixture authentication code".to_owned(),
                        verification_url: "https://example.invalid/fixture".to_owned(),
                        user_code: "FIXTURE".to_owned(),
                        fields: Vec::new(),
                    }),
                    accounts: Vec::new(),
                    secret_patches: vec![AdapterSecretPatch {
                        key: "token".to_owned(),
                        value: Some(b"response-secret".to_vec()),
                    }],
                    retry_after: None,
                    error: None,
                },
            })
        })
    }

    fn continue_connection_authentication(
        &self,
        _command: ContinueConnectionAuthentication,
    ) -> ConnectionFuture<'_, AdapterAuthenticationStep, ApplicationError> {
        Box::pin(async {
            Ok(AdapterAuthenticationStep {
                authentication_id: "provider-device-auth".to_owned(),
                status: AdapterAuthenticationStatus::Completed,
                prompt: None,
                accounts: vec![AdapterAccount {
                    key: AdapterKey {
                        namespace: "fixture".to_owned(),
                        value: b"authenticated".to_vec(),
                    },
                    display_name: "Authenticated".to_owned(),
                }],
                secret_patches: vec![AdapterSecretPatch {
                    key: "token".to_owned(),
                    value: Some(b"response-secret".to_vec()),
                }],
                retry_after: None,
                error: None,
            })
        })
    }

    fn cancel_connection_authentication(
        &self,
        _command: CancelConnectionAuthentication,
    ) -> ConnectionFuture<'_, (), ApplicationError> {
        Box::pin(async { Ok(()) })
    }
}

pub struct StaticBinaryAssetInteraction;

impl BinaryAssetInteraction for StaticBinaryAssetInteraction {
    fn read_published_binary_asset(
        &self,
        command: ReadPublishedBinaryAsset,
    ) -> AssetFuture<'_, AdapterBinaryAsset, ApplicationError> {
        Box::pin(async move {
            assert!(!command.operation_id.is_empty());
            assert_eq!(command.source_id, SOURCE_ID);
            assert_eq!(command.publication_id, CATALOG_PUBLICATION_ID);
            assert_eq!(command.provider_item_key.namespace(), "fixture");
            assert_eq!(command.provider_item_key.value(), b"item.0");
            assert_eq!(command.asset_key.namespace(), "fixture.asset");
            assert_eq!(command.asset_key.value(), b"poster");
            Ok(AdapterBinaryAsset {
                content: b"poster-bytes".to_vec(),
                content_type: "image/jpeg".to_owned(),
                full_length: 12,
                sha256_digest: vec![0xab; 32],
                cache_control: "public, max-age=300".to_owned(),
            })
        })
    }
}

pub struct StaticReferenceLookupInteraction;

impl ReferenceLookupInteraction for StaticReferenceLookupInteraction {
    fn lookup_portable_references(
        &self,
        command: LookupPortableReferences,
    ) -> ReferenceLookupFuture<'_, Vec<AdapterPortableReferenceLookup>, ApplicationError> {
        Box::pin(async move {
            assert!(!command.operation_id.is_empty());
            assert_eq!(command.source_id, SOURCE_ID);
            Ok(command
                .references
                .into_iter()
                .enumerate()
                .map(|(index, requested)| AdapterPortableReferenceLookup {
                    outcome: match index {
                        0 => AdapterLookupOutcome::Matched(Box::new(lookup_candidate(
                            index,
                            requested.clone(),
                        ))),
                        1 => AdapterLookupOutcome::NotFound,
                        2 => AdapterLookupOutcome::Ambiguous(vec![
                            lookup_candidate(index, requested.clone()),
                            lookup_candidate(index + 1, requested.clone()),
                        ]),
                        _ => AdapterLookupOutcome::Unsupported,
                    },
                    requested,
                })
                .collect())
        })
    }
}

fn lookup_candidate(index: usize, matched_reference: PortableReference) -> AdapterLookupCandidate {
    let provider_item = ProviderItem::new(
        ProviderItemKey::new("fixture.lookup", format!("item.{index}").into_bytes())
            .expect("lookup provider-item key is valid"),
        Term::new("fixture", "item").expect("lookup item kind is valid"),
        format!("Lookup item {index}"),
        json!({"lookup": true}),
        vec![matched_reference.clone()],
        vec![BinaryAssetReference::new(
            BinaryAssetKey::new("fixture.asset", b"poster".to_vec())
                .expect("lookup asset key is valid"),
            Term::new("fixture", "poster").expect("lookup asset kind is valid"),
        )],
    )
    .expect("lookup provider item is valid");
    AdapterLookupCandidate {
        provider_item,
        evidence: AdapterLookupEvidence {
            adapter_revision: format!("lookup/{index}").into_bytes(),
            observed_at: timestamp(10),
            expires_at: Some(timestamp(20)),
            matched_references: vec![matched_reference],
        },
    }
}

fn secret_configuration_field() -> AdapterConfigurationField {
    AdapterConfigurationField {
        key: "token".to_owned(),
        display_name: "Token".to_owned(),
        value_kind: AdapterValueKind::Text,
        required: true,
        secret: true,
        options: vec![AdapterConfigurationOption {
            value: AdapterValue::Text("secret-option".to_owned()),
            display_name: "Secret option".to_owned(),
        }],
        default_value: Some(AdapterValue::Text("secret-default".to_owned())),
        placeholder: "Token".to_owned(),
    }
}

pub async fn request(router: &axum::Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("HTTP request completes");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body is collected")
        .to_bytes();
    let value = serde_json::from_slice(&body).expect("response body is JSON");
    (status, value)
}

pub async fn mapping_activation_request(
    router: &axum::Router,
    workspace_id: &str,
    mapping_id: &str,
    content_hash: &str,
) -> (StatusCode, Value) {
    request(
        router,
        Request::post(format!(
            "/api/v1/workspaces/{workspace_id}/mappings/{mapping_id}/activation"
        ))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "contentHash": content_hash }).to_string(),
        ))
        .expect("mapping activation request is built"),
    )
    .await
}

pub fn assert_closed_error(error: &Value, code: &str) {
    assert_eq!(error["code"], code);
    assert!(error["message"].is_string());
    assert!(error["fieldProblems"].is_array());
    assert_eq!(error.as_object().map(serde_json::Map::len), Some(3));
}

pub async fn seed_source(pool: &sqlx::PgPool) {
    let now = timestamp(0);
    sqlx::query(
        "INSERT INTO connections (
    id, provider_id, display_name, status, capabilities,
    capability_revision, created_at, updated_at
)
VALUES ($1, 'fixture', 'Fixture', 'ready', $2, 1, $3, $3)
",
    )
    .bind(CONNECTION_ID)
    .bind(json!({}))
    .bind(now)
    .execute(pool)
    .await
    .expect("connection is seeded");
    sqlx::query(
        "INSERT INTO accounts (
    id, connection_id, provider_key_namespace, provider_key_value,
    display_name, first_seen_at, last_seen_at
)
VALUES ($1, $2, 'fixture', $3, 'Fixture account', $4, $4)
",
    )
    .bind(ACCOUNT_ID)
    .bind(CONNECTION_ID)
    .bind(b"account".as_slice())
    .bind(now)
    .execute(pool)
    .await
    .expect("account is seeded");
    sqlx::query(
        "INSERT INTO sources (
    id, connection_id, account_id, provider_key_namespace, provider_key_value,
    display_name, kind_namespace, kind_name, availability, capabilities,
    capability_revision, first_seen_at, last_seen_at
)
VALUES (
    $1, $2, $3, 'fixture', $4, 'Fixture source', 'fixture', 'catalog',
    'available', $5, 1, $6, $6
)
",
    )
    .bind(SOURCE_ID)
    .bind(CONNECTION_ID)
    .bind(ACCOUNT_ID)
    .bind(b"source".as_slice())
    .bind(json!({"catalog": {"full": true, "incremental": true}}))
    .bind(now)
    .execute(pool)
    .await
    .expect("source is seeded");
    sqlx::query(
        "INSERT INTO source_streams (
    source_id, stream_kind, status, cursor, publication_id,
    evidence_revision, last_success_at, updated_at
)
VALUES
    ($1, 'catalog', 'available', $2, $3, $4, $5, $5),
    ($1, 'state', 'available', $6, $7, $8, $5, $5)
",
    )
    .bind(SOURCE_ID)
    .bind(b"catalog/v1".as_slice())
    .bind(CATALOG_PUBLICATION_ID)
    .bind(b"catalog/revision/v1".as_slice())
    .bind(now)
    .bind(b"state/v1".as_slice())
    .bind(STATE_PUBLICATION_ID)
    .bind(b"state/revision/v1".as_slice())
    .execute(pool)
    .await
    .expect("streams are seeded");
    for (index, name) in ["Alpha", "Beta"].into_iter().enumerate() {
        let key = format!("item.{index}").into_bytes();
        let relation = format!("relation.{index}").into_bytes();
        sqlx::query(
            "INSERT INTO catalog_items (
    source_id, key_namespace, key_value, kind_namespace, kind_name,
    display_name, attributes, publication_id, updated_at
)
VALUES ($1, 'fixture', $2, 'fixture', 'item', $3, $4, $5, $6)
",
        )
        .bind(SOURCE_ID)
        .bind(&key)
        .bind(name)
        .bind(json!({}))
        .bind(CATALOG_PUBLICATION_ID)
        .bind(now)
        .execute(pool)
        .await
        .expect("catalog item is seeded");
        sqlx::query(
            "INSERT INTO catalog_relations (
    source_id, key_namespace, key_value,
    item_key_namespace, item_key_value,
    kind_namespace, kind_name, order_parts, attributes,
    publication_id, updated_at
)
VALUES (
    $1, 'fixture', $2, 'fixture', $3,
    'fixture', 'member', $4, $5, $6, $7
)
",
        )
        .bind(SOURCE_ID)
        .bind(relation)
        .bind(&key)
        .bind(vec![index as i64])
        .bind(json!({}))
        .bind(CATALOG_PUBLICATION_ID)
        .bind(now)
        .execute(pool)
        .await
        .expect("catalog relation is seeded");
        if index == 0 {
            sqlx::query(
                "INSERT INTO catalog_assets (
    source_id, item_key_namespace, item_key_value,
    asset_key_namespace, asset_key_value,
    kind_namespace, kind_name, publication_id
)
VALUES ($1, 'fixture', $2, 'fixture.asset', $3, 'fixture', 'poster', $4)
",
            )
            .bind(SOURCE_ID)
            .bind(&key)
            .bind(b"poster".as_slice())
            .bind(CATALOG_PUBLICATION_ID)
            .execute(pool)
            .await
            .expect("catalog asset is seeded");
        }
    }
    sqlx::query(
        "INSERT INTO state_observations (
    source_id, subject_kind, subject_key_namespace, subject_key_value,
    field_namespace, field_name, unit_namespace, unit_name,
    value, provider_revision, observed_at, publication_id, updated_at
)
VALUES (
    $1, 'provider_item', 'fixture', $2,
    'fixture.state', 'progress', 'fixture.unit', 'count',
    $3, $4, $5, $6, $5
)
",
    )
    .bind(SOURCE_ID)
    .bind(b"item.0".as_slice())
    .bind(json!({"kind": "integer", "value": 7}))
    .bind(b"state/item.0/v1".as_slice())
    .bind(now)
    .bind(STATE_PUBLICATION_ID)
    .execute(pool)
    .await
    .expect("state observation is seeded");
}

pub fn timestamp(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_900_000_000 + second).expect("test timestamp is valid")
}
