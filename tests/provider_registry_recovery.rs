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
            ActivateProviderInstallation, ProviderRegistryApi, RemoveProviderInstallation,
        },
    },
    database::DatabaseRuntime,
    infrastructure::provider_registry::{PostgresProviderRegistry, ProbedProviderRegistry},
};
use uuid::Uuid;

const VALID_INSTALLATION_ID: Uuid = Uuid::from_u128(1);
const INVALID_INSTALLATION_ID: Uuid = Uuid::from_u128(2);
const ORPHAN_INSTALLATION_ID: Uuid = Uuid::from_u128(3);
const MISMATCHED_INSTALLATION_ID: Uuid = Uuid::from_u128(4);
const VALID_PROVIDER_ID: &str = "dev.trakkin.valid";
const INVALID_PROVIDER_ID: &str = "dev.trakkin.invalid";
const MISMATCHED_PROVIDER_ID: &str = "dev.trakkin.mismatched";

#[tokio::test]
async fn reconciliation_preserves_valid_packages_and_retires_invalid_state() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let persistent_root = directory.path().join("providers");
    let runtime_root = directory.path().join("runtime");
    let installed_at = timestamp(1);
    seed_repository(&database.pool(), installed_at).await;

    let executable = b"valid-provider";
    let valid_directory = persistent_root
        .join("installations")
        .join(VALID_INSTALLATION_ID.to_string());
    write_executable(&valid_directory.join("bin/provider"), executable).await;
    seed_installation(
        &database.pool(),
        VALID_INSTALLATION_ID,
        VALID_PROVIDER_ID,
        "0.1.0",
        manifest(VALID_PROVIDER_ID, "0.1.0", executable),
        installed_at,
    )
    .await;
    seed_installation(
        &database.pool(),
        INVALID_INSTALLATION_ID,
        INVALID_PROVIDER_ID,
        "0.1.0",
        manifest(INVALID_PROVIDER_ID, "0.1.0", b"missing-provider"),
        installed_at,
    )
    .await;
    let mismatched_executable = b"mismatched-provider";
    write_executable(
        &persistent_root
            .join("installations")
            .join(MISMATCHED_INSTALLATION_ID.to_string())
            .join("bin/provider"),
        mismatched_executable,
    )
    .await;
    let mut mismatched_manifest = manifest(MISMATCHED_PROVIDER_ID, "0.1.0", mismatched_executable);
    mismatched_manifest["target"] = json!("x86_64-unknown-linux-gnu");
    seed_installation(
        &database.pool(),
        MISMATCHED_INSTALLATION_ID,
        MISMATCHED_PROVIDER_ID,
        "0.1.0",
        mismatched_manifest,
        installed_at,
    )
    .await;
    sqlx::query(
        "INSERT INTO active_provider_installations (
    provider_id, installation_id, generation, activated_at
) VALUES ($1, $2, $3, $4)",
    )
    .bind(INVALID_PROVIDER_ID)
    .bind(INVALID_INSTALLATION_ID)
    .bind(Uuid::from_u128(4))
    .bind(installed_at)
    .execute(&database.pool())
    .await
    .expect("invalid installation is activated");

    tokio::fs::create_dir_all(persistent_root.join("staging/interrupted"))
        .await
        .expect("persistent staging exists");
    tokio::fs::create_dir_all(
        persistent_root
            .join("installations")
            .join(ORPHAN_INSTALLATION_ID.to_string()),
    )
    .await
    .expect("orphan installation exists");
    tokio::fs::create_dir_all(runtime_root.join("stale"))
        .await
        .expect("stale runtime exists");

    let registry = PostgresProviderRegistry::new(database.pool(), &persistent_root, &runtime_root);
    registry
        .reconcile(timestamp(2))
        .await
        .expect("provider state reconciles");

    assert_eq!(
        installation_status(&database.pool(), VALID_INSTALLATION_ID).await,
        "installed"
    );
    assert_eq!(
        installation_status(&database.pool(), INVALID_INSTALLATION_ID).await,
        "failed"
    );
    assert_eq!(
        installation_status(&database.pool(), MISMATCHED_INSTALLATION_ID).await,
        "failed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM active_provider_installations")
            .fetch_one(&database.pool())
            .await
            .expect("active installation count"),
        0
    );
    assert!(
        tokio::fs::try_exists(valid_directory)
            .await
            .expect("valid path")
    );
    assert!(
        !tokio::fs::try_exists(
            persistent_root
                .join("installations")
                .join(ORPHAN_INSTALLATION_ID.to_string())
        )
        .await
        .expect("orphan path")
    );
    assert_directory_empty(&persistent_root.join("staging")).await;
    assert_directory_empty(&runtime_root).await;

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn activation_rolls_back_and_removal_preserves_running_generations() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let persistent_root = directory.path().join("providers");
    let runtime_root = directory.path().join("runtime");
    let provider_id = "dev.trakkin.rollback";
    let first_installation_id = Uuid::from_u128(10);
    let second_installation_id = Uuid::from_u128(11);
    let installed_at = timestamp(1);
    seed_repository(&database.pool(), installed_at).await;
    for (installation_id, version, executable) in [
        (first_installation_id, "0.1.0", b"first".as_slice()),
        (second_installation_id, "0.2.0", b"second".as_slice()),
    ] {
        write_executable(
            &persistent_root
                .join("installations")
                .join(installation_id.to_string())
                .join("bin/provider"),
            executable,
        )
        .await;
        let blob = persistent_root
            .join("blobs/sha256")
            .join(package_digest(version));
        tokio::fs::create_dir_all(blob.parent().expect("blob parent"))
            .await
            .expect("blob directory");
        tokio::fs::write(blob, version.as_bytes())
            .await
            .expect("package blob");
        seed_installation(
            &database.pool(),
            installation_id,
            provider_id,
            version,
            manifest(provider_id, version, executable),
            installed_at,
        )
        .await;
    }

    let registry = PostgresProviderRegistry::new(database.pool(), &persistent_root, &runtime_root);
    registry
        .reconcile(timestamp(2))
        .await
        .expect("provider state reconciles");
    activate(
        &registry,
        provider_id,
        first_installation_id,
        Uuid::from_u128(12),
        timestamp(3),
    )
    .await;
    activate(
        &registry,
        provider_id,
        second_installation_id,
        Uuid::from_u128(13),
        timestamp(4),
    )
    .await;
    activate(
        &registry,
        provider_id,
        first_installation_id,
        Uuid::from_u128(14),
        timestamp(5),
    )
    .await;

    let error = registry
        .remove_provider_installation(RemoveProviderInstallation {
            installation_id: first_installation_id,
        })
        .await
        .expect_err("active installation is protected");
    assert_eq!(error.code, ApplicationErrorCode::ProviderInstallationActive);
    registry
        .remove_provider_installation(RemoveProviderInstallation {
            installation_id: second_installation_id,
        })
        .await
        .expect("inactive installation is removed");
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT installation_id FROM active_provider_installations WHERE provider_id = $1",
        )
        .bind(provider_id)
        .fetch_one(&database.pool())
        .await
        .expect("active installation"),
        first_installation_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM provider_installations WHERE id = $1",)
            .bind(second_installation_id)
            .fetch_one(&database.pool())
            .await
            .expect("removed installation count"),
        0
    );
    assert!(
        tokio::fs::try_exists(
            persistent_root
                .join("installations")
                .join(first_installation_id.to_string())
        )
        .await
        .expect("active installation path")
    );
    assert!(
        !tokio::fs::try_exists(
            persistent_root
                .join("installations")
                .join(second_installation_id.to_string())
        )
        .await
        .expect("removed installation path")
    );
    assert!(
        tokio::fs::try_exists(runtime_root.join(second_installation_id.to_string()))
            .await
            .expect("pinned runtime path")
    );
    assert!(
        tokio::fs::try_exists(
            persistent_root
                .join("blobs/sha256")
                .join(package_digest("0.1.0"))
        )
        .await
        .expect("active package blob")
    );
    assert!(
        !tokio::fs::try_exists(
            persistent_root
                .join("blobs/sha256")
                .join(package_digest("0.2.0"))
        )
        .await
        .expect("removed package blob")
    );
    registry
        .reconcile(timestamp(6))
        .await
        .expect("startup reconciliation removes retained runtimes");
    assert_directory_empty(&runtime_root).await;

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn failed_activation_probe_preserves_active_generation() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let persistent_root = directory.path().join("providers");
    let runtime_root = directory.path().join("runtime");
    let provider_id = "dev.trakkin.probe";
    let active_installation_id = Uuid::from_u128(30);
    let broken_installation_id = Uuid::from_u128(31);
    let active_generation = Uuid::from_u128(32);
    let installed_at = timestamp(1);
    seed_repository(&database.pool(), installed_at).await;
    for (installation_id, version) in [
        (active_installation_id, "0.1.0"),
        (broken_installation_id, "0.2.0"),
    ] {
        let executable = version.as_bytes();
        write_executable(
            &persistent_root
                .join("installations")
                .join(installation_id.to_string())
                .join("bin/provider"),
            executable,
        )
        .await;
        seed_installation(
            &database.pool(),
            installation_id,
            provider_id,
            version,
            manifest(provider_id, version, executable),
            installed_at,
        )
        .await;
    }
    let registry = PostgresProviderRegistry::new(database.pool(), &persistent_root, &runtime_root);
    activate(
        &registry,
        provider_id,
        active_installation_id,
        active_generation,
        timestamp(2),
    )
    .await;
    let probed = ProbedProviderRegistry::new(registry, "test-host");

    let error = probed
        .activate_provider_installation(ActivateProviderInstallation {
            provider_id: provider_id.to_owned(),
            installation_id: broken_installation_id,
            generation: Uuid::from_u128(33),
            activated_at: timestamp(3),
        })
        .await
        .expect_err("broken provider probe is rejected");

    assert_eq!(error.code, ApplicationErrorCode::InvalidProviderData);
    let active = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT installation_id, generation
FROM active_provider_installations
WHERE provider_id = $1",
    )
    .bind(provider_id)
    .fetch_one(&database.pool())
    .await
    .expect("active installation");
    assert_eq!(active, (active_installation_id, active_generation));

    database.shutdown().await.expect("database shuts down");
}

#[tokio::test]
async fn release_state_is_reported_per_artifact_target() {
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let now = timestamp(1);
    seed_repository(&database.pool(), now).await;
    sqlx::query(
        "UPDATE provider_repositories
SET catalog_sequence = 1, catalog = $1, refreshed_at = $2, updated_at = $2
WHERE id = 'dev.trakkin.test'",
    )
    .bind(json!({
        "schema": "trakkin.provider-repository/v1",
        "repositoryId": "dev.trakkin.test",
        "sequence": 1,
        "providers": [{
            "id": VALID_PROVIDER_ID,
            "name": "Valid provider",
            "summary": "Multi-target provider",
            "releases": [{
                "version": "0.1.0",
                "protocol": {
                    "minimum": { "major": 1, "minor": 0 },
                    "maximum": { "major": 1, "minor": 0 }
                },
                "channels": ["stable"],
                "artifacts": [
                    {
                        "target": "x86_64-unknown-linux-musl",
                        "targetPath": "provider-musl.trp",
                        "sha256": "a".repeat(64),
                        "size": 1
                    },
                    {
                        "target": "x86_64-unknown-linux-gnu",
                        "targetPath": "provider-gnu.trp",
                        "sha256": "b".repeat(64),
                        "size": 1
                    }
                ]
            }]
        }]
    }))
    .bind(now)
    .execute(&database.pool())
    .await
    .expect("repository catalog is seeded");
    seed_installation(
        &database.pool(),
        VALID_INSTALLATION_ID,
        VALID_PROVIDER_ID,
        "0.1.0",
        manifest(VALID_PROVIDER_ID, "0.1.0", b"provider"),
        now,
    )
    .await;
    sqlx::query(
        "INSERT INTO active_provider_installations (
    provider_id, installation_id, generation, activated_at
) VALUES ($1, $2, $3, $4)",
    )
    .bind(VALID_PROVIDER_ID)
    .bind(VALID_INSTALLATION_ID)
    .bind(Uuid::from_u128(20))
    .bind(now)
    .execute(&database.pool())
    .await
    .expect("installation is activated");

    let registry = PostgresProviderRegistry::new(
        database.pool(),
        directory.path().join("providers"),
        directory.path().join("runtime"),
    );
    let release = registry
        .list_provider_releases()
        .await
        .expect("provider releases")
        .pop()
        .expect("provider release");
    assert_eq!(
        release.targets,
        [
            "x86_64-unknown-linux-musl".to_owned(),
            "x86_64-unknown-linux-gnu".to_owned()
        ]
    );
    assert_eq!(
        release.installed_targets,
        ["x86_64-unknown-linux-musl".to_owned()]
    );
    assert_eq!(
        release.active_target.as_deref(),
        Some("x86_64-unknown-linux-musl")
    );

    database.shutdown().await.expect("database shuts down");
}

async fn seed_repository(pool: &sqlx::PgPool, created_at: OffsetDateTime) {
    sqlx::query(
        "INSERT INTO provider_repositories (
    id, display_name, metadata_base_url, targets_base_url, trusted_root,
    created_at, updated_at
) VALUES ('dev.trakkin.test', 'Test', 'file:///metadata/', 'file:///targets/',
    $1, $2, $2)",
    )
    .bind(b"{}".as_slice())
    .bind(created_at)
    .execute(pool)
    .await
    .expect("repository is seeded");
}

async fn seed_installation(
    pool: &sqlx::PgPool,
    installation_id: Uuid,
    provider_id: &str,
    version: &str,
    manifest: serde_json::Value,
    installed_at: OffsetDateTime,
) {
    sqlx::query(
        "INSERT INTO provider_installations (
    id, repository_id, provider_id, version, target, package_sha256,
    manifest, status, installed_at, created_at, updated_at
) VALUES ($1, 'dev.trakkin.test', $2, $3,
    'x86_64-unknown-linux-musl', $4, $5, 'installed', $6, $6, $6)",
    )
    .bind(installation_id)
    .bind(provider_id)
    .bind(version)
    .bind(package_digest(version))
    .bind(manifest)
    .bind(installed_at)
    .execute(pool)
    .await
    .expect("installation is seeded");
}

fn package_digest(version: &str) -> String {
    hex::encode(Sha256::digest(version.as_bytes()))
}

fn manifest(provider_id: &str, version: &str, executable: &[u8]) -> serde_json::Value {
    json!({
        "schema": "trakkin.provider-package/v1",
        "providerId": provider_id,
        "version": version,
        "target": "x86_64-unknown-linux-musl",
        "protocol": {
            "minimum": { "major": 1, "minor": 0 },
            "maximum": { "major": 1, "minor": 0 }
        },
        "entrypoint": "bin/provider",
        "files": [{
            "path": "bin/provider",
            "sha256": hex::encode(Sha256::digest(executable)),
            "size": executable.len(),
            "executable": true
        }]
    })
}

async fn activate(
    registry: &PostgresProviderRegistry,
    provider_id: &str,
    installation_id: Uuid,
    generation: Uuid,
    activated_at: OffsetDateTime,
) {
    registry
        .activate_provider_installation(ActivateProviderInstallation {
            provider_id: provider_id.to_owned(),
            installation_id,
            generation,
            activated_at,
        })
        .await
        .expect("provider installation activates");
}

async fn installation_status(pool: &sqlx::PgPool, installation_id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM provider_installations WHERE id = $1")
        .bind(installation_id)
        .fetch_one(pool)
        .await
        .expect("installation status")
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
