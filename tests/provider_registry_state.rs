#![cfg(feature = "embedded-database")]

use std::path::Path;

use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::{
        error::ApplicationErrorCode,
        providers::{
            ChangeProviderUpdatePolicy, ProviderLaunchResolver, ProviderRegistryApi,
            ProviderUpdatePolicy, RemoveProvider, RemoveProviderSource,
        },
    },
    database::DatabaseRuntime,
    infrastructure::provider_registry::{PostgresProviderRegistry, ProbedProviderRegistry},
};
use uuid::Uuid;

const COMMUNITY_SOURCE_ID: Uuid = Uuid::from_u128(1);

#[tokio::test]
async fn reconciliation_preserves_valid_artifacts_and_removes_invalid_state() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let persistent_root = directory.path().join("providers");
    let runtime_root = directory.path().join("runtime");
    let valid_artifact_id = Uuid::from_u128(10);
    let missing_artifact_id = Uuid::from_u128(11);
    let mismatched_artifact_id = Uuid::from_u128(12);
    let orphan_artifact_id = Uuid::from_u128(13);
    let valid_source_id = Uuid::from_u128(101);
    let missing_source_id = Uuid::from_u128(102);
    let mismatched_source_id = Uuid::from_u128(103);

    seed_source(&database.pool(), valid_source_id).await;
    seed_source(&database.pool(), missing_source_id).await;
    seed_source(&database.pool(), mismatched_source_id).await;

    seed_provider(
        &database.pool(),
        &persistent_root,
        SeedProvider {
            artifact_id: valid_artifact_id,
            provider_id: "dev.trakkin.valid",
            metadata_provider_id: "dev.trakkin.valid",
            source_id: valid_source_id,
            version: "1.0.0",
            binary: b"valid-provider",
        },
    )
    .await;
    seed_provider(
        &database.pool(),
        &persistent_root,
        SeedProvider {
            artifact_id: missing_artifact_id,
            provider_id: "dev.trakkin.missing",
            metadata_provider_id: "dev.trakkin.missing",
            source_id: missing_source_id,
            version: "1.0.0",
            binary: b"missing-provider",
        },
    )
    .await;
    tokio::fs::remove_dir_all(artifact_directory(&persistent_root, missing_artifact_id))
        .await
        .expect("missing artifact is removed");
    seed_provider(
        &database.pool(),
        &persistent_root,
        SeedProvider {
            artifact_id: mismatched_artifact_id,
            provider_id: "dev.trakkin.mismatched",
            metadata_provider_id: "dev.trakkin.someone-else",
            source_id: mismatched_source_id,
            version: "1.0.0",
            binary: b"mismatched-provider",
        },
    )
    .await;
    write_executable(
        &artifact_directory(&persistent_root, orphan_artifact_id).join("provider"),
        b"orphan-provider",
    )
    .await;
    tokio::fs::create_dir_all(persistent_root.join("staging/interrupted"))
        .await
        .expect("staging state exists");
    tokio::fs::create_dir_all(runtime_root.join("stale"))
        .await
        .expect("runtime state exists");

    let registry = PostgresProviderRegistry::new(database.pool(), &persistent_root, &runtime_root);
    registry
        .reconcile(timestamp(2))
        .await
        .expect("provider state reconciles");

    assert_eq!(
        provider_count(&database.pool(), "dev.trakkin.valid").await,
        1
    );
    assert_eq!(
        provider_count(&database.pool(), "dev.trakkin.missing").await,
        0
    );
    assert_eq!(
        provider_count(&database.pool(), "dev.trakkin.mismatched").await,
        0
    );
    assert!(
        tokio::fs::try_exists(artifact_directory(&persistent_root, valid_artifact_id))
            .await
            .expect("valid artifact path")
    );
    assert!(
        !tokio::fs::try_exists(artifact_directory(&persistent_root, orphan_artifact_id))
            .await
            .expect("orphan artifact path")
    );
    assert_directory_empty(&persistent_root.join("staging")).await;
    assert_directory_empty(&runtime_root).await;

    let resolved = registry
        .resolve_provider("dev.trakkin.valid")
        .await
        .expect("valid provider resolves");
    assert_eq!(resolved.artifact_id, valid_artifact_id);
    assert!(resolved.executable.is_file());
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn provider_identity_is_unique_and_one_source_hosts_multiple_providers() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let persistent_root = directory.path().join("providers");
    let provider_id = "dev.trakkin.fixture";
    seed_source(&database.pool(), COMMUNITY_SOURCE_ID).await;
    seed_provider(
        &database.pool(),
        &persistent_root,
        SeedProvider {
            artifact_id: Uuid::from_u128(20),
            provider_id,
            metadata_provider_id: provider_id,
            source_id: COMMUNITY_SOURCE_ID,
            version: "1.0.0",
            binary: b"fixture-provider",
        },
    )
    .await;
    sqlx::query(
        "UPDATE providers
SET available_version = '1.1.0', available_discovered_at = $2
WHERE provider_id = $1",
    )
    .bind(provider_id)
    .bind(timestamp(3))
    .execute(&database.pool())
    .await
    .expect("manual update is discovered");

    let duplicate_source_id = Uuid::from_u128(2);
    seed_source(&database.pool(), duplicate_source_id).await;
    let duplicate_identity = sqlx::query(
        "INSERT INTO providers
SELECT provider_id, $2, $3, version, platform,
    index_digest, manifest_digest, metadata_digest, binary_digest, metadata,
    update_policy, available_version, available_discovered_at,
    last_update_error, installed_at, updated_at
FROM providers WHERE provider_id = $1",
    )
    .bind(provider_id)
    .bind(Uuid::from_u128(21))
    .bind(duplicate_source_id)
    .execute(&database.pool())
    .await
    .expect_err("provider identity cannot be installed twice");
    assert_database_code(&duplicate_identity, "23505");

    sqlx::query(
        "INSERT INTO providers
SELECT 'dev.trakkin.fixture-copy', $2, source_id, version, platform,
    index_digest, manifest_digest, metadata_digest, binary_digest,
    jsonb_set(metadata, '{id}', '\"dev.trakkin.fixture-copy\"'),
    update_policy, available_version, available_discovered_at,
    last_update_error, installed_at, updated_at
FROM providers WHERE provider_id = $1",
    )
    .bind(provider_id)
    .bind(Uuid::from_u128(22))
    .execute(&database.pool())
    .await
    .expect("one source can represent multiple installed providers");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM providers WHERE source_id = $1")
            .bind(COMMUNITY_SOURCE_ID)
            .fetch_one(&database.pool())
            .await
            .expect("source provider count"),
        2
    );

    let registry = PostgresProviderRegistry::new(
        database.pool(),
        &persistent_root,
        directory.path().join("runtime"),
    );
    let management = ProbedProviderRegistry::new(registry, env!("CARGO_PKG_VERSION"));
    let providers = management
        .list_providers()
        .await
        .expect("providers are listed");
    let provider = providers.first().expect("installed provider");
    assert_eq!(provider.update_policy, ProviderUpdatePolicy::Manual);
    assert_eq!(
        provider
            .available_update
            .as_ref()
            .map(|update| update.version.as_str()),
        Some("1.1.0")
    );
    assert_eq!(
        provider
            .available_update
            .as_ref()
            .map(|update| update.discovered_at),
        Some(timestamp(3))
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn automatic_policy_change_persists_when_pending_update_application_fails() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let persistent_root = directory.path().join("providers");
    let provider_id = "dev.trakkin.policy";
    seed_source(&database.pool(), COMMUNITY_SOURCE_ID).await;
    seed_provider(
        &database.pool(),
        &persistent_root,
        SeedProvider {
            artifact_id: Uuid::from_u128(25),
            provider_id,
            metadata_provider_id: provider_id,
            source_id: COMMUNITY_SOURCE_ID,
            version: "1.0.0",
            binary: b"policy-provider",
        },
    )
    .await;
    sqlx::query(
        "UPDATE providers
SET available_version = '0.9.0', available_discovered_at = $2
WHERE provider_id = $1",
    )
    .bind(provider_id)
    .bind(timestamp(2))
    .execute(&database.pool())
    .await
    .expect("pending update is seeded");
    let registry = PostgresProviderRegistry::new(
        database.pool(),
        &persistent_root,
        directory.path().join("runtime"),
    );
    let management = ProbedProviderRegistry::new(registry, env!("CARGO_PKG_VERSION"));

    let provider = management
        .change_provider_update_policy(ChangeProviderUpdatePolicy {
            provider_id: provider_id.to_owned(),
            update_policy: ProviderUpdatePolicy::Automatic,
            updated_at: timestamp(3),
        })
        .await
        .expect("policy change remains successful when automatic update fails");

    assert_eq!(provider.update_policy, ProviderUpdatePolicy::Automatic);
    assert_eq!(
        provider
            .available_update
            .as_ref()
            .map(|update| update.version.as_str()),
        Some("0.9.0")
    );
    assert!(
        provider
            .last_update_error
            .as_deref()
            .is_some_and(|error| error.contains("newer"))
    );
    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn source_removal_is_blocked_until_its_provider_is_removed() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let persistent_root = directory.path().join("providers");
    let source_id = Uuid::from_u128(30);
    seed_source(&database.pool(), source_id).await;
    seed_provider(
        &database.pool(),
        &persistent_root,
        SeedProvider {
            artifact_id: Uuid::from_u128(31),
            provider_id: "dev.example.fixture",
            metadata_provider_id: "dev.example.fixture",
            source_id,
            version: "1.0.0",
            binary: b"fixture-provider",
        },
    )
    .await;
    let registry = PostgresProviderRegistry::new(
        database.pool(),
        &persistent_root,
        directory.path().join("runtime"),
    );
    let management = ProbedProviderRegistry::new(registry, env!("CARGO_PKG_VERSION"));

    let error = management
        .remove_provider_source(RemoveProviderSource { source_id })
        .await
        .expect_err("source in use is protected");
    assert_eq!(
        error.code,
        ApplicationErrorCode::ProviderConflict,
        "{error:?}"
    );
    management
        .remove_provider(RemoveProvider {
            provider_id: "dev.example.fixture".to_owned(),
        })
        .await
        .expect("provider is removed");
    management
        .remove_provider_source(RemoveProviderSource { source_id })
        .await
        .expect("unused source is removed");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM provider_sources WHERE id = $1")
            .bind(source_id)
            .fetch_one(&database.pool())
            .await
            .expect("source count"),
        0
    );
    database.shutdown().await.expect("database shuts down");
}

struct SeedProvider<'a> {
    artifact_id: Uuid,
    provider_id: &'a str,
    metadata_provider_id: &'a str,
    source_id: Uuid,
    version: &'a str,
    binary: &'a [u8],
}

async fn seed_provider(pool: &sqlx::PgPool, persistent_root: &Path, seed: SeedProvider<'_>) {
    write_executable(
        &artifact_directory(persistent_root, seed.artifact_id).join("provider"),
        seed.binary,
    )
    .await;
    let platform = host_platform();
    let metadata = json!({
        "schema": "trakkin.provider/v1",
        "id": seed.metadata_provider_id,
        "name": "Fixture provider",
        "version": seed.version,
        "platforms": [platform],
    });
    sqlx::query(
        "INSERT INTO providers (
    provider_id, artifact_id, source_id, version, platform, index_digest,
    manifest_digest, metadata_digest, binary_digest, metadata, update_policy,
    installed_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'manual', $11, $11)",
    )
    .bind(seed.provider_id)
    .bind(seed.artifact_id)
    .bind(seed.source_id)
    .bind(seed.version)
    .bind(platform)
    .bind(digest(b"index"))
    .bind(digest(b"manifest"))
    .bind(digest(b"metadata"))
    .bind(digest(seed.binary))
    .bind(metadata)
    .bind(timestamp(1))
    .execute(pool)
    .await
    .expect("provider is seeded");
}

async fn seed_source(pool: &sqlx::PgPool, source_id: Uuid) {
    let repository_url = format!("https://registry.example/trakkin/providers-{source_id}");
    sqlx::query(
        "INSERT INTO provider_sources (
    id, repository_url, trusted_at
) VALUES ($1, $2, $3)",
    )
    .bind(source_id)
    .bind(repository_url)
    .bind(timestamp(1))
    .execute(pool)
    .await
    .expect("provider source is seeded");
}

fn artifact_directory(persistent_root: &Path, artifact_id: Uuid) -> std::path::PathBuf {
    persistent_root
        .join("artifacts")
        .join(artifact_id.to_string())
}

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn host_platform() -> String {
    let architecture = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };
    format!("{}/{architecture}", std::env::consts::OS)
}

async fn provider_count(pool: &sqlx::PgPool, provider_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM providers WHERE provider_id = $1")
        .bind(provider_id)
        .fetch_one(pool)
        .await
        .expect("provider count")
}

fn assert_database_code(error: &sqlx::Error, expected: &str) {
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some(expected)
    );
}

async fn write_executable(path: &Path, bytes: &[u8]) {
    tokio::fs::create_dir_all(path.parent().expect("executable parent"))
        .await
        .expect("executable directory");
    tokio::fs::write(path, bytes)
        .await
        .expect("executable is written");
    set_executable(path).await;
}

#[cfg(unix)]
async fn set_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o500))
        .await
        .expect("executable permissions");
}

#[cfg(not(unix))]
async fn set_executable(_path: &Path) {}

async fn assert_directory_empty(path: &Path) {
    let mut entries = tokio::fs::read_dir(path).await.expect("directory exists");
    assert!(
        entries
            .next_entry()
            .await
            .expect("directory can be read")
            .is_none()
    );
}

fn timestamp(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(second).expect("valid timestamp")
}
