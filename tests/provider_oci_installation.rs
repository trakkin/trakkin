#![cfg(all(feature = "embedded-database", unix))]

use tempfile::tempdir;
use time::OffsetDateTime;
use trakkin::{
    application::{
        adapter_runtime::{AdapterLauncher, AdapterRetirement, AdapterSession, LaunchAdapter},
        providers::{
            InstallProvider, ProviderLaunchResolver, ProviderRegistryApi, ProviderUpdatePolicy,
            TrustProviderSource,
        },
    },
    database::DatabaseRuntime,
    infrastructure::{
        adapter_process::AdapterProcessLauncher,
        provider_registry::{PostgresProviderRegistry, ProbedProviderRegistry},
    },
};
use trakkin_provider_sdk::v1::HealthStatus;
use uuid::Uuid;

const SOURCE_ID: Uuid = Uuid::from_u128(1);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set TRAKKIN_TEST_PROVIDER_REPOSITORY_URL, TRAKKIN_TEST_PROVIDER_ID, and TRAKKIN_TEST_PROVIDER_VERSION to test a published provider"]
async fn trusted_oci_source_installs_and_launches_provider() {
    let repository_url = std::env::var("TRAKKIN_TEST_PROVIDER_REPOSITORY_URL")
        .expect("TRAKKIN_TEST_PROVIDER_REPOSITORY_URL is set");
    let provider_id =
        std::env::var("TRAKKIN_TEST_PROVIDER_ID").expect("TRAKKIN_TEST_PROVIDER_ID is set");
    let version = std::env::var("TRAKKIN_TEST_PROVIDER_VERSION")
        .expect("TRAKKIN_TEST_PROVIDER_VERSION is set");
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
    let management = ProbedProviderRegistry::new(registry.clone(), env!("CARGO_PKG_VERSION"));
    management
        .trust_provider_source(TrustProviderSource {
            source_id: SOURCE_ID,
            repository_url,
            trusted_at: timestamp(1),
        })
        .await
        .expect("provider source is trusted");
    let installed = management
        .install_provider(InstallProvider {
            artifact_id: Uuid::from_u128(2),
            source_id: SOURCE_ID,
            provider_id: provider_id.clone(),
            version: version.clone(),
            update_policy: ProviderUpdatePolicy::Manual,
            installed_at: timestamp(2),
        })
        .await
        .expect("provider installs");
    assert_eq!(installed.provider_id, provider_id);
    assert_eq!(installed.version, version);

    let resolved = registry
        .resolve_provider(&installed.provider_id)
        .await
        .expect("installed provider resolves");
    assert!(resolved.executable.is_file());

    let launcher = AdapterProcessLauncher::new(registry, env!("CARGO_PKG_VERSION"));
    let mut session = launcher
        .launch(LaunchAdapter {
            provider_id: installed.provider_id.clone(),
            process_instance_id: "oci-install-test".to_owned(),
        })
        .await
        .expect("installed provider launches");
    assert_eq!(session.identity().adapter_key, installed.provider_id);
    assert_eq!(
        session.health().await.expect("provider health").status,
        HealthStatus::Ready as i32
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
