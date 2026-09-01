#![cfg(all(feature = "embedded-database", unix))]

use std::path::PathBuf;

use sha2::Digest;
use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::{
        adapter_runtime::{
            AdapterHealthStatus, AdapterLauncher, AdapterRetirement, AdapterSession, LaunchAdapter,
        },
        providers::{
            ActivateProviderInstallation, InstallProvider, ProviderLaunchResolver,
            ProviderRegistryApi, RegisterProviderRepository,
        },
    },
    database::DatabaseRuntime,
    infrastructure::{
        adapter_process::AdapterProcessLauncher, provider_registry::PostgresProviderRegistry,
    },
};
use url::Url;
use uuid::Uuid;

const PROVIDER_ID: &str = "dev.trakkin.fixture";
const REPOSITORY_ID: &str = "dev.trakkin.community";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set TRAKKIN_TEST_PROVIDER_REPOSITORY to a signed provider repository"]
async fn signed_repository_installs_activates_and_launches_provider() {
    let repository = PathBuf::from(
        std::env::var_os("TRAKKIN_TEST_PROVIDER_REPOSITORY")
            .expect("TRAKKIN_TEST_PROVIDER_REPOSITORY is set"),
    );
    assert!(repository.is_dir(), "signed provider repository exists");
    let directory = tempdir().expect("temporary directory");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("database starts");
    let registry = PostgresProviderRegistry::new(
        database.pool(),
        directory.path().join("providers"),
        directory.path().join("runtime"),
    );
    registry
        .reconcile(timestamp(1))
        .await
        .expect("provider state reconciles");
    let trusted_root = tokio::fs::read(repository.join("metadata/1.root.json"))
        .await
        .expect("trusted root");
    let mut invite_url =
        Url::from_file_path(repository.join("repository.json")).expect("repository invite URL");
    invite_url.set_fragment(Some(&format!(
        "sha256={}",
        hex::encode(sha2::Sha256::digest(&trusted_root))
    )));
    registry
        .register_provider_repository(RegisterProviderRepository {
            url: invite_url.to_string(),
            registered_at: timestamp(2),
        })
        .await
        .expect("repository registers");
    let release = registry
        .list_provider_releases()
        .await
        .expect("provider releases")
        .into_iter()
        .find(|release| release.provider_id == PROVIDER_ID)
        .expect("fixture release");
    let target = release.targets.first().expect("fixture target").clone();
    let installation_id = Uuid::from_u128(1);
    registry
        .install_provider(InstallProvider {
            installation_id,
            repository_id: REPOSITORY_ID.to_owned(),
            provider_id: PROVIDER_ID.to_owned(),
            version: release.version,
            target,
            installed_at: timestamp(4),
        })
        .await
        .expect("provider installs");
    registry
        .activate_provider_installation(ActivateProviderInstallation {
            provider_id: PROVIDER_ID.to_owned(),
            installation_id,
            generation: Uuid::from_u128(2),
            activated_at: timestamp(5),
        })
        .await
        .expect("provider activates");
    let resolved = registry
        .resolve_provider(PROVIDER_ID)
        .await
        .expect("active provider resolves");
    assert!(resolved.executable.is_file());

    let launcher = AdapterProcessLauncher::new(registry, "test-host");
    let mut session = launcher
        .launch(LaunchAdapter {
            provider_id: PROVIDER_ID.to_owned(),
            process_instance_id: "repository-install-test".to_owned(),
        })
        .await
        .expect("installed provider launches");
    assert_eq!(session.identity().adapter_key, PROVIDER_ID);
    assert_eq!(
        session.health().await.expect("provider health").status,
        AdapterHealthStatus::Ready
    );
    assert_eq!(
        session.retire().await.expect("provider retires"),
        AdapterRetirement::Graceful
    );
    database.shutdown().await.expect("database shuts down");
}

fn timestamp(second: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(second).expect("valid timestamp")
}
