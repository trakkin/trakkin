use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    io::{Cursor, Read},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use futures_util::{TryStream, TryStreamExt};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;
use tough::{RepositoryLoader, TargetName};
use trakkin_provider_release::{
    MAXIMUM_PROVIDER_MANIFEST_BYTES, MAXIMUM_PROVIDER_PACKAGE_BYTES,
    MAXIMUM_PROVIDER_PACKAGE_ENTRIES, MAXIMUM_PROVIDER_PACKAGE_FILES,
    MAXIMUM_PROVIDER_UNPACKED_BYTES, PackageFile, ProtocolVersion as PackageProtocolVersion,
    ProviderPackageManifest, ProviderRepositoryIndex, REPOSITORY_SCHEMA_V1,
};
use url::Url;
use uuid::Uuid;

use crate::application::error::{ApplicationError, ApplicationErrorCode};
use crate::application::providers::{
    ActivateProviderInstallation, InstallProvider, ProviderApiFuture, ProviderFuture,
    ProviderInstallationSnapshot, ProviderLaunchError, ProviderLaunchResolver,
    ProviderLaunchTarget, ProviderRegistryApi, ProviderReleaseSnapshot, ProviderRepositorySnapshot,
    RefreshProviderRepository, RegisterProviderRepository, RemoveProviderInstallation,
};
use crate::infrastructure::adapter_process::AdapterProcessLauncher;

const CATALOG_TARGET: &str = "catalog.json";
const MAXIMUM_TRUSTED_ROOT_BYTES: usize = 1024 * 1024;
const MAXIMUM_CATALOG_BYTES: usize = 4 * 1024 * 1024;
const OFFICIAL_REPOSITORY_ID: &str = "dev.trakkin.official";
const OFFICIAL_REPOSITORY_METADATA_URL: &str = "https://providers.trakkin.dev/metadata/";
const OFFICIAL_REPOSITORY_TARGETS_URL: &str = "https://providers.trakkin.dev/targets/";
const OFFICIAL_REPOSITORY_ROOT: &[u8] = include_bytes!("official_repository_root.json");
static BLOB_STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct PostgresProviderRegistry {
    pool: PgPool,
    persistent_root: PathBuf,
    runtime_root: PathBuf,
}

struct ProviderLifecycleLock {
    _file: std::fs::File,
}

#[derive(Clone)]
pub struct ProbedProviderRegistry {
    registry: PostgresProviderRegistry,
    launcher: AdapterProcessLauncher<PostgresProviderRegistry>,
}

impl ProbedProviderRegistry {
    pub fn new(registry: PostgresProviderRegistry, host_version: impl Into<String>) -> Self {
        Self {
            launcher: AdapterProcessLauncher::new(registry.clone(), host_version),
            registry,
        }
    }
}

impl PostgresProviderRegistry {
    pub fn new(
        pool: PgPool,
        persistent_root: impl Into<PathBuf>,
        runtime_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            pool,
            persistent_root: persistent_root.into(),
            runtime_root: runtime_root.into(),
        }
    }

    pub async fn reconcile(&self, reconciled_at: OffsetDateTime) -> Result<(), ApplicationError> {
        let _lifecycle_lock = self
            .acquire_lifecycle_lock()
            .await
            .map_err(internal_error)?;
        reset_directory(&self.persistent_root.join("staging"))
            .await
            .map_err(internal_error)?;
        reset_runtime_directory(&self.runtime_root)
            .await
            .map_err(internal_error)?;

        let rows = sqlx::query_as::<_, ReconciliationRow>(
            "SELECT id, provider_id, version, target, status, manifest
FROM provider_installations
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)?;
        let mut valid_installations = HashSet::new();
        let mut invalid_installations = Vec::new();
        for row in rows {
            if row.status != "installed" {
                invalid_installations.push(row.id);
                continue;
            }
            let Ok(manifest) = serde_json::from_value::<ProviderPackageManifest>(row.manifest)
            else {
                invalid_installations.push(row.id);
                continue;
            };
            if validate_manifest_identity(&manifest, &row.provider_id, &row.version, &row.target)
                .is_err()
            {
                invalid_installations.push(row.id);
                continue;
            }
            let directory = self
                .persistent_root
                .join("installations")
                .join(row.id.to_string());
            match verify_directory(&directory, &manifest.files).await {
                Ok(true) => {
                    valid_installations.insert(row.id);
                }
                Ok(false) => invalid_installations.push(row.id),
                Err(error) => return Err(internal_error(error)),
            }
        }

        if !invalid_installations.is_empty() {
            let mut transaction = self.pool.begin().await.map_err(internal_error)?;
            sqlx::query(
                "DELETE FROM active_provider_installations
WHERE installation_id = ANY($1)
",
            )
            .bind(&invalid_installations)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
            sqlx::query(
                "UPDATE provider_installations
SET status = 'failed', installed_at = NULL, updated_at = $2
WHERE id = ANY($1)
",
            )
            .bind(&invalid_installations)
            .bind(reconciled_at)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
            transaction.commit().await.map_err(internal_error)?;
        }

        let referenced_blobs = sqlx::query_scalar::<_, String>(
            "SELECT package_sha256
FROM provider_installations
WHERE status = 'installed'
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)?
        .into_iter()
        .collect();
        remove_unreferenced_or_invalid_blobs(
            &self.persistent_root.join("blobs").join("sha256"),
            &referenced_blobs,
        )
        .await
        .map_err(internal_error)?;

        let installations = self.persistent_root.join("installations");
        tokio::fs::create_dir_all(&installations)
            .await
            .map_err(internal_error)?;
        let mut entries = tokio::fs::read_dir(&installations)
            .await
            .map_err(internal_error)?;
        while let Some(entry) = entries.next_entry().await.map_err(internal_error)? {
            let keep = entry
                .file_name()
                .to_str()
                .and_then(|name| Uuid::parse_str(name).ok())
                .is_some_and(|installation_id| valid_installations.contains(&installation_id));
            if !keep {
                remove_path_if_present(&entry.path())
                    .await
                    .map_err(internal_error)?;
            }
        }
        Ok(())
    }

    pub async fn bootstrap_official_repository(
        &self,
        bootstrapped_at: OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        sqlx::query(
            "INSERT INTO provider_repositories (
    id, display_name, metadata_base_url, targets_base_url, trusted_root,
    official, enabled, created_at, updated_at
) VALUES ($1, 'Trakkin official', $2, $3, $4, TRUE, TRUE, $5, $5)
ON CONFLICT (id) DO UPDATE SET
display_name = excluded.display_name,
metadata_base_url = excluded.metadata_base_url,
targets_base_url = excluded.targets_base_url,
trusted_root = excluded.trusted_root,
official = TRUE,
updated_at = excluded.updated_at
",
        )
        .bind(OFFICIAL_REPOSITORY_ID)
        .bind(OFFICIAL_REPOSITORY_METADATA_URL)
        .bind(OFFICIAL_REPOSITORY_TARGETS_URL)
        .bind(OFFICIAL_REPOSITORY_ROOT)
        .bind(bootstrapped_at)
        .execute(&self.pool)
        .await
        .map_err(internal_error)?;
        Ok(())
    }

    async fn resolve(
        &self,
        provider_id: &str,
    ) -> Result<ProviderLaunchTarget, ProviderLaunchError> {
        let row = sqlx::query_as::<_, ActiveProviderRow>(
            "SELECT
    installation.id AS installation_id,
    installation.provider_id,
    installation.version,
    installation.target,
    installation.manifest,
    activation.generation
FROM active_provider_installations AS activation
INNER JOIN provider_installations AS installation
    ON installation.id = activation.installation_id
WHERE
    activation.provider_id = $1
    AND installation.status = 'installed'
",
        )
        .bind(provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| ProviderLaunchError::Registry(error.to_string()))?
        .ok_or_else(|| ProviderLaunchError::NotInstalled(provider_id.to_owned()))?;
        let manifest =
            serde_json::from_value::<ProviderPackageManifest>(row.manifest).map_err(|error| {
                ProviderLaunchError::InvalidInstallation {
                    provider_id: provider_id.to_owned(),
                    reason: error.to_string(),
                }
            })?;
        validate_launchable_manifest(&manifest, &row.provider_id, &row.version, &row.target)
            .map_err(|reason| invalid_installation(provider_id, reason))?;

        let runtime_directory = self.materialize(row.installation_id, &manifest).await?;
        Ok(ProviderLaunchTarget {
            provider_id: row.provider_id,
            version: row.version,
            installation_id: row.installation_id,
            generation: row.generation,
            executable: runtime_directory.join(&manifest.entrypoint),
            working_directory: runtime_directory,
        })
    }

    async fn materialize(
        &self,
        installation_id: Uuid,
        manifest: &ProviderPackageManifest,
    ) -> Result<PathBuf, ProviderLaunchError> {
        let _lifecycle_lock = self
            .acquire_lifecycle_lock()
            .await
            .map_err(|error| ProviderLaunchError::Registry(error.to_string()))?;
        let source = self
            .persistent_root
            .join("installations")
            .join(installation_id.to_string());
        let destination = self.runtime_root.join(installation_id.to_string());
        if verify_directory(&destination, &manifest.files).await? {
            return Ok(destination);
        }

        remove_path_if_present(&destination).await?;
        let staging = self.runtime_root.join(format!("{installation_id}.staging"));
        remove_path_if_present(&staging).await?;
        tokio::fs::create_dir_all(&staging).await?;

        let result = async {
            for file in &manifest.files {
                let bytes = read_installation_file(&source, file, &manifest.provider_id).await?;
                verify_file(file, &bytes, &manifest.provider_id)?;
                let destination_file = staging.join(&file.path);
                if let Some(parent) = destination_file.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(&destination_file, bytes).await?;
                set_file_permissions(&destination_file, file.executable).await?;
            }
            Ok::<(), ProviderLaunchError>(())
        }
        .await;
        if let Err(error) = result {
            let _ = tokio::fs::remove_dir_all(&staging).await;
            return Err(error);
        }
        tokio::fs::create_dir_all(&self.runtime_root).await?;
        tokio::fs::rename(&staging, &destination).await?;
        Ok(destination)
    }

    async fn repositories(&self) -> Result<Vec<ProviderRepositorySnapshot>, ApplicationError> {
        sqlx::query_as::<_, RepositoryRow>(
            "SELECT
    id, display_name, metadata_base_url, targets_base_url, trusted_root,
    official, enabled, catalog_sequence, catalog, refreshed_at
FROM provider_repositories
ORDER BY official DESC, display_name, id
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)?
        .into_iter()
        .map(RepositoryRow::snapshot)
        .collect()
    }

    async fn register_repository(
        &self,
        command: RegisterProviderRepository,
    ) -> Result<ProviderRepositorySnapshot, ApplicationError> {
        validate_repository_registration(&command)?;
        let row = sqlx::query_as::<_, RepositoryRow>(
            "INSERT INTO provider_repositories (
    id, display_name, metadata_base_url, targets_base_url, trusted_root,
    official, enabled, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, FALSE, TRUE, $6, $6)
RETURNING
    id, display_name, metadata_base_url, targets_base_url, trusted_root,
    official, enabled, catalog_sequence, catalog, refreshed_at
",
        )
        .bind(command.id)
        .bind(command.display_name.trim())
        .bind(command.metadata_base_url)
        .bind(command.targets_base_url)
        .bind(command.trusted_root)
        .bind(command.registered_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| {
            if has_database_code(&error, "23505") {
                ApplicationError::new(
                    ApplicationErrorCode::InvalidField,
                    "provider repository already exists",
                )
            } else {
                internal_error(error)
            }
        })?;
        row.snapshot()
    }

    async fn refresh_repository(
        &self,
        command: RefreshProviderRepository,
    ) -> Result<ProviderRepositorySnapshot, ApplicationError> {
        let _lifecycle_lock = self
            .acquire_lifecycle_lock()
            .await
            .map_err(internal_error)?;
        let row = self.repository_row(&command.repository_id).await?;
        if !row.enabled {
            return Err(ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "provider repository is disabled",
            ));
        }
        let repository = self.load_tuf_repository(&row).await?;
        let target_name = TargetName::new(CATALOG_TARGET).map_err(invalid_repository)?;
        let stream = repository
            .read_target(&target_name)
            .await
            .map_err(invalid_repository)?
            .ok_or_else(|| {
                ApplicationError::new(
                    ApplicationErrorCode::InvalidProviderData,
                    "provider repository does not publish catalog.json",
                )
            })?;
        let bytes = collect_bounded_target(
            stream,
            MAXIMUM_CATALOG_BYTES,
            "provider repository catalog exceeds the size limit",
        )
        .await?;
        let catalog = serde_json::from_slice::<ProviderRepositoryIndex>(&bytes)
            .map_err(invalid_repository)?;
        catalog.validate().map_err(invalid_repository)?;
        if catalog.repository_id != row.id {
            return Err(ApplicationError::new(
                ApplicationErrorCode::InvalidProviderData,
                "provider repository catalog identity does not match its trust record",
            ));
        }
        let sequence = i64::try_from(catalog.sequence).map_err(|_| {
            ApplicationError::new(
                ApplicationErrorCode::InvalidProviderData,
                "provider repository catalog sequence is too large",
            )
        })?;
        let catalog_json = serde_json::to_value(&catalog).map_err(internal_error)?;
        self.persist_catalog(&row.id, sequence, catalog_json, command.refreshed_at)
            .await
    }

    async fn persist_catalog(
        &self,
        repository_id: &str,
        sequence: i64,
        catalog: Value,
        refreshed_at: OffsetDateTime,
    ) -> Result<ProviderRepositorySnapshot, ApplicationError> {
        let mut transaction = self.pool.begin().await.map_err(internal_error)?;
        let current = sqlx::query_as::<_, CatalogStateRow>(
            "SELECT enabled, catalog_sequence, catalog
FROM provider_repositories
WHERE id = $1
FOR UPDATE
",
        )
        .bind(repository_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            ApplicationError::new(
                ApplicationErrorCode::ProviderRepositoryNotFound,
                "provider repository was not found",
            )
        })?;
        if !current.enabled {
            return Err(ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "provider repository is disabled",
            ));
        }
        validate_catalog_update(
            current.catalog_sequence,
            current.catalog.as_ref(),
            sequence,
            &catalog,
        )?;
        let updated = sqlx::query_as::<_, RepositoryRow>(
            "UPDATE provider_repositories
SET
    catalog_sequence = $2,
    catalog = $3,
    refreshed_at = $4,
    updated_at = $4
WHERE id = $1
RETURNING
    id, display_name, metadata_base_url, targets_base_url, trusted_root,
    official, enabled, catalog_sequence, catalog, refreshed_at
",
        )
        .bind(repository_id)
        .bind(sequence)
        .bind(catalog)
        .bind(refreshed_at)
        .fetch_one(&mut *transaction)
        .await
        .map_err(internal_error)?;
        transaction.commit().await.map_err(internal_error)?;
        updated.snapshot()
    }

    async fn releases(&self) -> Result<Vec<ProviderReleaseSnapshot>, ApplicationError> {
        let repositories = sqlx::query_as::<_, RepositoryRow>(
            "SELECT
    id, display_name, metadata_base_url, targets_base_url, trusted_root,
    official, enabled, catalog_sequence, catalog, refreshed_at
FROM provider_repositories
WHERE enabled AND catalog IS NOT NULL
ORDER BY official DESC, display_name, id
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)?;
        let installed = sqlx::query_as::<_, InstalledReleaseRow>(
            "SELECT
    installation.repository_id, installation.provider_id,
    installation.version, installation.target,
    activation.installation_id IS NOT NULL AS active
FROM provider_installations AS installation
LEFT JOIN active_provider_installations AS activation
    ON activation.installation_id = installation.id
WHERE installation.status = 'installed'
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)?;
        let installed_keys = installed
            .iter()
            .map(|row| {
                (
                    row.repository_id.as_str(),
                    row.provider_id.as_str(),
                    row.version.as_str(),
                    row.target.as_str(),
                )
            })
            .collect::<HashSet<_>>();
        let active_keys = installed
            .iter()
            .filter(|row| row.active)
            .map(|row| {
                (
                    row.repository_id.as_str(),
                    row.provider_id.as_str(),
                    row.version.as_str(),
                    row.target.as_str(),
                )
            })
            .collect::<HashSet<_>>();

        let mut releases = Vec::new();
        for repository in repositories {
            let catalog = serde_json::from_value::<ProviderRepositoryIndex>(
                repository.catalog.ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorCode::Internal,
                        "refreshed provider repository has no catalog",
                    )
                })?,
            )
            .map_err(internal_error)?;
            for provider in catalog.providers {
                for release in provider.releases {
                    let version = release.version.to_string();
                    let targets = release
                        .artifacts
                        .iter()
                        .map(|artifact| artifact.target.clone())
                        .collect::<Vec<_>>();
                    let installed_targets = release
                        .artifacts
                        .iter()
                        .filter(|artifact| {
                            installed_keys.contains(&(
                                repository.id.as_str(),
                                provider.id.as_str(),
                                version.as_str(),
                                artifact.target.as_str(),
                            ))
                        })
                        .map(|artifact| artifact.target.clone())
                        .collect();
                    let active_target = release
                        .artifacts
                        .iter()
                        .find(|artifact| {
                            active_keys.contains(&(
                                repository.id.as_str(),
                                provider.id.as_str(),
                                version.as_str(),
                                artifact.target.as_str(),
                            ))
                        })
                        .map(|artifact| artifact.target.clone());
                    releases.push(ProviderReleaseSnapshot {
                        repository_id: repository.id.clone(),
                        provider_id: provider.id.clone(),
                        name: provider.name.clone(),
                        summary: provider.summary.clone(),
                        version: version.clone(),
                        channels: release.channels,
                        targets,
                        installed_targets,
                        active_target,
                    });
                }
            }
        }
        Ok(releases)
    }

    async fn installations(&self) -> Result<Vec<ProviderInstallationSnapshot>, ApplicationError> {
        sqlx::query_as::<_, InstallationRow>(
            "SELECT
    installation.id, installation.repository_id, installation.provider_id,
    installation.version, installation.target, installation.package_sha256,
    installation.installed_at, activation.generation,
    activation.installation_id IS NOT NULL AS active
FROM provider_installations AS installation
LEFT JOIN active_provider_installations AS activation
    ON activation.installation_id = installation.id
WHERE installation.status = 'installed'
ORDER BY installation.installed_at DESC, installation.id
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)?
        .into_iter()
        .map(InstallationRow::snapshot)
        .collect()
    }

    async fn install(
        &self,
        command: InstallProvider,
    ) -> Result<ProviderInstallationSnapshot, ApplicationError> {
        let _lifecycle_lock = self
            .acquire_lifecycle_lock()
            .await
            .map_err(internal_error)?;
        let repository_row = self.repository_row(&command.repository_id).await?;
        if !repository_row.enabled {
            return Err(ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "provider repository is disabled",
            ));
        }
        let catalog_value = repository_row.catalog.clone().ok_or_else(|| {
            ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "provider repository must be refreshed before installing providers",
            )
        })?;
        let catalog = serde_json::from_value::<ProviderRepositoryIndex>(catalog_value)
            .map_err(invalid_repository)?;
        let provider = catalog
            .providers
            .iter()
            .find(|provider| provider.id == command.provider_id)
            .ok_or_else(|| {
                ApplicationError::new(
                    ApplicationErrorCode::ProviderNotFound,
                    "provider was not found in the repository",
                )
            })?;
        let release = provider
            .releases
            .iter()
            .find(|release| release.version.to_string() == command.version)
            .ok_or_else(|| {
                ApplicationError::new(
                    ApplicationErrorCode::ProviderNotFound,
                    "provider release was not found in the repository",
                )
            })?;
        let host_protocol = trakkin_provider_sdk::current_protocol_version();
        if !release.protocol.supports(&PackageProtocolVersion {
            major: host_protocol.major,
            minor: host_protocol.minor,
        }) {
            return Err(ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "provider release is incompatible with this Trakkin protocol",
            ));
        }
        let artifact = release
            .artifacts
            .iter()
            .find(|artifact| artifact.target == command.target)
            .ok_or_else(|| {
                ApplicationError::new(
                    ApplicationErrorCode::ProviderNotFound,
                    "provider release has no artifact for the requested target",
                )
            })?;
        if artifact.size == 0 || artifact.size > MAXIMUM_PROVIDER_PACKAGE_BYTES {
            return Err(ApplicationError::new(
                ApplicationErrorCode::InvalidProviderData,
                "provider package size is outside the supported limit",
            ));
        }

        let repository = self.load_tuf_repository(&repository_row).await?;
        let target_name =
            TargetName::new(artifact.target_path.clone()).map_err(invalid_repository)?;
        let stream = repository
            .read_target(&target_name)
            .await
            .map_err(invalid_repository)?
            .ok_or_else(|| {
                ApplicationError::new(
                    ApplicationErrorCode::InvalidProviderData,
                    "provider package is absent from signed repository targets",
                )
            })?;
        let package = collect_bounded_target(
            stream,
            usize::try_from(MAXIMUM_PROVIDER_PACKAGE_BYTES).map_err(internal_error)?,
            "provider package exceeds the size limit",
        )
        .await?;
        let package_digest = hex::encode(Sha256::digest(&package));
        if package.len() as u64 != artifact.size || package_digest != artifact.sha256 {
            return Err(ApplicationError::new(
                ApplicationErrorCode::InvalidProviderData,
                "provider package does not match the catalog digest and size",
            ));
        }

        sqlx::query(
            "INSERT INTO provider_installations (
    id, repository_id, provider_id, version, target, package_sha256,
    manifest, status, installed_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, '{}'::JSONB, 'installing', NULL, $7, $7)
",
        )
        .bind(command.installation_id)
        .bind(&command.repository_id)
        .bind(&command.provider_id)
        .bind(&command.version)
        .bind(&command.target)
        .bind(&package_digest)
        .bind(command.installed_at)
        .execute(&self.pool)
        .await
        .map_err(|error| {
            if has_database_code(&error, "23505") {
                ApplicationError::new(
                    ApplicationErrorCode::ProviderAlreadyInstalled,
                    "provider release is already installed or being installed for this target",
                )
            } else {
                internal_error(error)
            }
        })?;

        let staging = self
            .persistent_root
            .join("staging")
            .join(command.installation_id.to_string());
        let installation = self
            .persistent_root
            .join("installations")
            .join(command.installation_id.to_string());
        let install_result = async {
            tokio::fs::create_dir_all(self.persistent_root.join("staging"))
                .await
                .map_err(internal_error)?;
            tokio::fs::create_dir_all(self.persistent_root.join("installations"))
                .await
                .map_err(internal_error)?;
            remove_path_if_present(&staging)
                .await
                .map_err(internal_error)?;
            if tokio::fs::try_exists(&installation)
                .await
                .map_err(internal_error)?
            {
                return Err(ApplicationError::new(
                    ApplicationErrorCode::ProviderAlreadyInstalled,
                    "provider installation ID already exists",
                ));
            }
            let staging_for_extraction = staging.clone();
            let (manifest, package) = tokio::task::spawn_blocking(move || {
                extract_package(&package, &staging_for_extraction)
                    .map(|manifest| (manifest, package))
            })
            .await
            .map_err(internal_error)?
            .map_err(invalid_repository)?;
            validate_installed_manifest(&manifest, &command, &release.protocol)?;
            self.persist_blob(&package_digest, &package).await?;
            tokio::fs::rename(&staging, &installation)
                .await
                .map_err(internal_error)?;
            Ok(manifest)
        }
        .await;
        let manifest = match install_result {
            Ok(manifest) => manifest,
            Err(error) => {
                let _ = remove_path_if_present(&staging).await;
                let _ = remove_path_if_present(&installation).await;
                self.fail_installation(command.installation_id, command.installed_at)
                    .await?;
                return Err(error);
            }
        };
        let manifest_json = serde_json::to_value(&manifest).map_err(internal_error)?;
        let completed = sqlx::query_as::<_, InstallationRow>(
            "UPDATE provider_installations
SET manifest = $2, status = 'installed', installed_at = $3, updated_at = $3
WHERE id = $1 AND status = 'installing'
RETURNING
    id, repository_id, provider_id, version, target, package_sha256,
    installed_at, NULL::UUID AS generation, FALSE AS active
",
        )
        .bind(command.installation_id)
        .bind(manifest_json)
        .bind(command.installed_at)
        .fetch_optional(&self.pool)
        .await;
        match completed {
            Ok(Some(row)) => row.snapshot(),
            Ok(None) => {
                let _ = remove_path_if_present(&installation).await;
                self.fail_installation(command.installation_id, command.installed_at)
                    .await?;
                Err(ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "provider installation changed state before completion",
                ))
            }
            Err(error) => {
                let _ = tokio::fs::remove_dir_all(&installation).await;
                self.fail_installation(command.installation_id, command.installed_at)
                    .await?;
                Err(internal_error(error))
            }
        }
    }

    async fn fail_installation(
        &self,
        installation_id: Uuid,
        failed_at: OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        sqlx::query(
            "UPDATE provider_installations
SET status = 'failed', installed_at = NULL, updated_at = $2
WHERE id = $1 AND status = 'installing'
",
        )
        .bind(installation_id)
        .bind(failed_at)
        .execute(&self.pool)
        .await
        .map_err(internal_error)?;
        Ok(())
    }

    async fn activate(
        &self,
        command: ActivateProviderInstallation,
    ) -> Result<ProviderInstallationSnapshot, ApplicationError> {
        self.activation_target(&command).await?;
        let mut transaction = self.pool.begin().await.map_err(internal_error)?;
        let locked = sqlx::query_scalar::<_, Uuid>(
            "SELECT id
FROM provider_installations
WHERE id = $1 AND provider_id = $2 AND status = 'installed'
FOR UPDATE
",
        )
        .bind(command.installation_id)
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(internal_error)?;
        if locked.is_none() {
            return Err(ApplicationError::new(
                ApplicationErrorCode::ProviderInstallationNotFound,
                "provider installation was not found",
            ));
        }
        sqlx::query(
            "INSERT INTO active_provider_installations (
    provider_id, installation_id, generation, activated_at
) VALUES ($1, $2, $3, $4)
ON CONFLICT (provider_id) DO UPDATE SET
installation_id = excluded.installation_id,
generation = excluded.generation,
activated_at = excluded.activated_at
",
        )
        .bind(&command.provider_id)
        .bind(command.installation_id)
        .bind(command.generation)
        .bind(command.activated_at)
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
        let result = sqlx::query_as::<_, InstallationRow>(
            "SELECT
    installation.id, installation.repository_id, installation.provider_id,
    installation.version, installation.target, installation.package_sha256,
    installation.installed_at, activation.generation, TRUE AS active
FROM provider_installations AS installation
INNER JOIN active_provider_installations AS activation
    ON activation.installation_id = installation.id
WHERE installation.id = $1
",
        )
        .bind(command.installation_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(internal_error)?;
        transaction.commit().await.map_err(internal_error)?;
        result.snapshot()
    }

    async fn activation_target(
        &self,
        command: &ActivateProviderInstallation,
    ) -> Result<ProviderLaunchTarget, ApplicationError> {
        let row = sqlx::query_as::<_, InstallationManifestRow>(
            "SELECT id, provider_id, version, target, manifest
FROM provider_installations
WHERE id = $1 AND provider_id = $2 AND status = 'installed'
",
        )
        .bind(command.installation_id)
        .bind(&command.provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            ApplicationError::new(
                ApplicationErrorCode::ProviderInstallationNotFound,
                "provider installation was not found",
            )
        })?;
        let manifest = serde_json::from_value::<ProviderPackageManifest>(row.manifest)
            .map_err(invalid_repository)?;
        validate_launchable_manifest(&manifest, &row.provider_id, &row.version, &row.target)
            .map_err(invalid_repository)?;
        let runtime_directory = self
            .materialize(row.id, &manifest)
            .await
            .map_err(provider_launch_application_error)?;
        Ok(ProviderLaunchTarget {
            provider_id: row.provider_id,
            version: row.version,
            installation_id: row.id,
            generation: command.generation,
            executable: runtime_directory.join(&manifest.entrypoint),
            working_directory: runtime_directory,
        })
    }

    async fn remove_installation(
        &self,
        command: RemoveProviderInstallation,
    ) -> Result<(), ApplicationError> {
        let _lifecycle_lock = self
            .acquire_lifecycle_lock()
            .await
            .map_err(internal_error)?;
        let mut transaction = self.pool.begin().await.map_err(internal_error)?;
        let (package_sha256, active) = sqlx::query_as::<_, (String, bool)>(
            "SELECT
    installation.package_sha256,
    EXISTS(
        SELECT 1
        FROM active_provider_installations
        WHERE installation_id = $1
    ) AS active
FROM provider_installations AS installation
WHERE installation.id = $1
FOR UPDATE
",
        )
        .bind(command.installation_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            ApplicationError::new(
                ApplicationErrorCode::ProviderInstallationNotFound,
                "provider installation was not found",
            )
        })?;
        if active {
            return Err(ApplicationError::new(
                ApplicationErrorCode::ProviderInstallationActive,
                "active provider installation cannot be removed",
            ));
        }
        sqlx::query("DELETE FROM provider_installations WHERE id = $1
")
            .bind(command.installation_id)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
        let blob_referenced = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1
    FROM provider_installations
    WHERE package_sha256 = $1
)
",
        )
        .bind(&package_sha256)
        .fetch_one(&mut *transaction)
        .await
        .map_err(internal_error)?;
        transaction.commit().await.map_err(internal_error)?;

        let _ = remove_path_if_present(
            &self
                .persistent_root
                .join("installations")
                .join(command.installation_id.to_string()),
        )
        .await;
        if !blob_referenced {
            let _ = remove_path_if_present(
                &self
                    .persistent_root
                    .join("blobs")
                    .join("sha256")
                    .join(package_sha256),
            )
            .await;
        }
        Ok(())
    }

    async fn persist_blob(&self, digest: &str, bytes: &[u8]) -> Result<(), ApplicationError> {
        if hex::encode(Sha256::digest(bytes)) != digest {
            return Err(internal_error(
                "provider blob bytes do not match their content address",
            ));
        }
        let blob_directory = self.persistent_root.join("blobs").join("sha256");
        tokio::fs::create_dir_all(&blob_directory)
            .await
            .map_err(internal_error)?;
        let destination = blob_directory.join(digest);
        if blob_matches_digest(&destination, digest)
            .await
            .map_err(internal_error)?
        {
            return Ok(());
        }
        remove_path_if_present(&destination)
            .await
            .map_err(internal_error)?;
        let staging = write_staged_blob(&blob_directory, digest, bytes)
            .await
            .map_err(internal_error)?;
        match tokio::fs::rename(&staging, &destination).await {
            Ok(()) => Ok(()),
            Err(_error)
                if blob_matches_digest(&destination, digest)
                    .await
                    .unwrap_or(false) =>
            {
                let _ = tokio::fs::remove_file(&staging).await;
                Ok(())
            }
            Err(error) => {
                let _ = remove_path_if_present(&staging).await;
                Err(internal_error(error))
            }
        }
    }

    async fn acquire_lifecycle_lock(&self) -> Result<ProviderLifecycleLock, std::io::Error> {
        tokio::fs::create_dir_all(&self.persistent_root).await?;
        let lock_path = self.persistent_root.join(".lifecycle.lock");
        loop {
            let lock_path = lock_path.clone();
            let lock = tokio::task::spawn_blocking(move || {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(lock_path)?;
                match file.try_lock() {
                    Ok(()) => Ok(Some(ProviderLifecycleLock { _file: file })),
                    Err(std::fs::TryLockError::WouldBlock) => Ok(None),
                    Err(std::fs::TryLockError::Error(error)) => Err(error),
                }
            })
            .await
            .map_err(std::io::Error::other)??;
            if let Some(lock) = lock {
                return Ok(lock);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    async fn repository_row(&self, repository_id: &str) -> Result<RepositoryRow, ApplicationError> {
        sqlx::query_as::<_, RepositoryRow>(
            "SELECT
    id, display_name, metadata_base_url, targets_base_url, trusted_root,
    official, enabled, catalog_sequence, catalog, refreshed_at
FROM provider_repositories
WHERE id = $1
",
        )
        .bind(repository_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            ApplicationError::new(
                ApplicationErrorCode::ProviderRepositoryNotFound,
                "provider repository was not found",
            )
        })
    }

    async fn load_tuf_repository(
        &self,
        repository: &RepositoryRow,
    ) -> Result<tough::Repository, ApplicationError> {
        let metadata_url = parse_repository_url(&repository.metadata_base_url)?;
        let targets_url = parse_repository_url(&repository.targets_base_url)?;
        let datastore = self
            .persistent_root
            .join("repositories")
            .join(&repository.id)
            .join("metadata");
        tokio::fs::create_dir_all(&datastore)
            .await
            .map_err(internal_error)?;
        RepositoryLoader::new(&repository.trusted_root, metadata_url, targets_url)
            .datastore(datastore)
            .load()
            .await
            .map_err(invalid_repository)
    }
}

impl ProviderLaunchResolver for PostgresProviderRegistry {
    fn resolve_provider(&self, provider_id: &str) -> ProviderFuture<'_, ProviderLaunchTarget> {
        let provider_id = provider_id.to_owned();
        Box::pin(async move { self.resolve(&provider_id).await })
    }
}

impl ProviderRegistryApi for PostgresProviderRegistry {
    fn ensure_provider_active(&self, provider_id: &str) -> ProviderApiFuture<'_, ()> {
        let provider_id = provider_id.to_owned();
        Box::pin(async move {
            self.resolve(&provider_id)
                .await
                .map(|_| ())
                .map_err(provider_launch_application_error)
        })
    }

    fn list_provider_repositories(&self) -> ProviderApiFuture<'_, Vec<ProviderRepositorySnapshot>> {
        Box::pin(self.repositories())
    }

    fn register_provider_repository(
        &self,
        command: RegisterProviderRepository,
    ) -> ProviderApiFuture<'_, ProviderRepositorySnapshot> {
        Box::pin(self.register_repository(command))
    }

    fn refresh_provider_repository(
        &self,
        command: RefreshProviderRepository,
    ) -> ProviderApiFuture<'_, ProviderRepositorySnapshot> {
        Box::pin(self.refresh_repository(command))
    }

    fn list_provider_releases(&self) -> ProviderApiFuture<'_, Vec<ProviderReleaseSnapshot>> {
        Box::pin(self.releases())
    }

    fn list_provider_installations(
        &self,
    ) -> ProviderApiFuture<'_, Vec<ProviderInstallationSnapshot>> {
        Box::pin(self.installations())
    }

    fn install_provider(
        &self,
        command: InstallProvider,
    ) -> ProviderApiFuture<'_, ProviderInstallationSnapshot> {
        Box::pin(self.install(command))
    }

    fn activate_provider_installation(
        &self,
        command: ActivateProviderInstallation,
    ) -> ProviderApiFuture<'_, ProviderInstallationSnapshot> {
        Box::pin(self.activate(command))
    }

    fn remove_provider_installation(
        &self,
        command: RemoveProviderInstallation,
    ) -> ProviderApiFuture<'_, ()> {
        Box::pin(self.remove_installation(command))
    }
}

impl ProviderRegistryApi for ProbedProviderRegistry {
    fn ensure_provider_active(&self, provider_id: &str) -> ProviderApiFuture<'_, ()> {
        self.registry.ensure_provider_active(provider_id)
    }

    fn list_provider_repositories(&self) -> ProviderApiFuture<'_, Vec<ProviderRepositorySnapshot>> {
        self.registry.list_provider_repositories()
    }

    fn register_provider_repository(
        &self,
        command: RegisterProviderRepository,
    ) -> ProviderApiFuture<'_, ProviderRepositorySnapshot> {
        self.registry.register_provider_repository(command)
    }

    fn refresh_provider_repository(
        &self,
        command: RefreshProviderRepository,
    ) -> ProviderApiFuture<'_, ProviderRepositorySnapshot> {
        self.registry.refresh_provider_repository(command)
    }

    fn list_provider_releases(&self) -> ProviderApiFuture<'_, Vec<ProviderReleaseSnapshot>> {
        self.registry.list_provider_releases()
    }

    fn list_provider_installations(
        &self,
    ) -> ProviderApiFuture<'_, Vec<ProviderInstallationSnapshot>> {
        self.registry.list_provider_installations()
    }

    fn install_provider(
        &self,
        command: InstallProvider,
    ) -> ProviderApiFuture<'_, ProviderInstallationSnapshot> {
        self.registry.install_provider(command)
    }

    fn activate_provider_installation(
        &self,
        command: ActivateProviderInstallation,
    ) -> ProviderApiFuture<'_, ProviderInstallationSnapshot> {
        Box::pin(async move {
            let target = self.registry.activation_target(&command).await?;
            self.launcher
                .probe_target(
                    target,
                    format!("provider-activation-probe-{}", command.generation),
                )
                .await
                .map_err(|error| {
                    ApplicationError::new(
                        ApplicationErrorCode::InvalidProviderData,
                        format!("provider activation probe failed: {error}"),
                    )
                })?;
            self.registry.activate(command).await
        })
    }

    fn remove_provider_installation(
        &self,
        command: RemoveProviderInstallation,
    ) -> ProviderApiFuture<'_, ()> {
        self.registry.remove_provider_installation(command)
    }
}

#[derive(FromRow)]
struct RepositoryRow {
    id: String,
    display_name: String,
    metadata_base_url: String,
    targets_base_url: String,
    trusted_root: Vec<u8>,
    official: bool,
    enabled: bool,
    catalog_sequence: Option<i64>,
    catalog: Option<Value>,
    refreshed_at: Option<OffsetDateTime>,
}

#[derive(FromRow)]
struct CatalogStateRow {
    enabled: bool,
    catalog_sequence: Option<i64>,
    catalog: Option<Value>,
}

impl RepositoryRow {
    fn snapshot(self) -> Result<ProviderRepositorySnapshot, ApplicationError> {
        Ok(ProviderRepositorySnapshot {
            id: self.id,
            display_name: self.display_name,
            metadata_base_url: self.metadata_base_url,
            targets_base_url: self.targets_base_url,
            official: self.official,
            enabled: self.enabled,
            catalog_sequence: self
                .catalog_sequence
                .map(u64::try_from)
                .transpose()
                .map_err(internal_error)?,
            refreshed_at: self.refreshed_at,
        })
    }
}

#[derive(FromRow)]
struct InstalledReleaseRow {
    repository_id: String,
    provider_id: String,
    version: String,
    target: String,
    active: bool,
}

#[derive(FromRow)]
struct InstallationRow {
    id: Uuid,
    repository_id: String,
    provider_id: String,
    version: String,
    target: String,
    package_sha256: String,
    installed_at: Option<OffsetDateTime>,
    generation: Option<Uuid>,
    active: bool,
}

#[derive(FromRow)]
struct InstallationManifestRow {
    id: Uuid,
    provider_id: String,
    version: String,
    target: String,
    manifest: Value,
}

#[derive(FromRow)]
struct ReconciliationRow {
    id: Uuid,
    provider_id: String,
    version: String,
    target: String,
    status: String,
    manifest: Value,
}

impl InstallationRow {
    fn snapshot(self) -> Result<ProviderInstallationSnapshot, ApplicationError> {
        Ok(ProviderInstallationSnapshot {
            id: self.id,
            repository_id: self.repository_id,
            provider_id: self.provider_id,
            version: self.version,
            target: self.target,
            package_sha256: self.package_sha256,
            active: self.active,
            generation: self.generation,
            installed_at: self.installed_at.ok_or_else(|| {
                ApplicationError::new(
                    ApplicationErrorCode::Internal,
                    "installed provider is missing its installation timestamp",
                )
            })?,
        })
    }
}

fn validate_repository_registration(
    command: &RegisterProviderRepository,
) -> Result<(), ApplicationError> {
    let identity = ProviderRepositoryIndex {
        schema: REPOSITORY_SCHEMA_V1.to_owned(),
        repository_id: command.id.clone(),
        sequence: 0,
        providers: Vec::new(),
    };
    identity.validate().map_err(|error| {
        ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "provider repository is invalid",
            "id",
            "invalid_identifier",
            error.to_string(),
        )
    })?;
    if command.display_name.trim().is_empty() {
        return Err(ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "provider repository is invalid",
            "displayName",
            "required",
            "display name must not be empty",
        ));
    }
    parse_repository_url(&command.metadata_base_url)?;
    parse_repository_url(&command.targets_base_url)?;
    if command.trusted_root.is_empty() || command.trusted_root.len() > MAXIMUM_TRUSTED_ROOT_BYTES {
        return Err(ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "provider repository is invalid",
            "trustedRoot",
            "invalid_tuf_root",
            "trusted root must contain bounded TUF root metadata",
        ));
    }
    let trusted_root =
        serde_json::from_slice::<tough::schema::Signed<tough::schema::Root>>(&command.trusted_root)
            .map_err(|_| {
                ApplicationError::field(
                    ApplicationErrorCode::InvalidField,
                    "provider repository is invalid",
                    "trustedRoot",
                    "invalid_tuf_root",
                    "trusted root must contain valid TUF root metadata",
                )
            })?;
    trusted_root
        .signed
        .verify_role(&trusted_root)
        .map_err(|_| {
            ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "provider repository is invalid",
                "trustedRoot",
                "invalid_tuf_root",
                "trusted root must carry a valid root-role signature",
            )
        })?;
    let trusted_root_sha256 = hex::encode(Sha256::digest(&command.trusted_root));
    if command.trusted_root_sha256 != trusted_root_sha256 {
        return Err(ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "provider repository is invalid",
            "trustedRootSha256",
            "fingerprint_mismatch",
            "trusted root fingerprint does not match the submitted root metadata",
        ));
    }
    Ok(())
}

fn parse_repository_url(value: &str) -> Result<Url, ApplicationError> {
    let url = Url::parse(value).map_err(|_| {
        ApplicationError::new(
            ApplicationErrorCode::InvalidField,
            "provider repository URL is invalid",
        )
    })?;
    if !matches!(url.scheme(), "https" | "file") {
        return Err(ApplicationError::new(
            ApplicationErrorCode::InvalidField,
            "provider repository URL must use HTTPS or a local file URL",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ApplicationError::new(
            ApplicationErrorCode::InvalidField,
            "provider repository URL must not contain credentials",
        ));
    }
    Ok(url)
}

fn invalid_repository(error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::InvalidProviderData,
        format!("provider repository verification failed: {error}"),
    )
}

fn validate_catalog_update(
    previous_sequence: Option<i64>,
    previous_catalog: Option<&Value>,
    sequence: i64,
    catalog: &Value,
) -> Result<(), ApplicationError> {
    let Some(previous_sequence) = previous_sequence else {
        return Ok(());
    };
    if sequence < previous_sequence {
        return Err(ApplicationError::new(
            ApplicationErrorCode::InvalidProviderData,
            "provider repository catalog sequence moved backwards",
        ));
    }
    if sequence == previous_sequence && previous_catalog != Some(catalog) {
        return Err(ApplicationError::new(
            ApplicationErrorCode::InvalidProviderData,
            "provider repository changed catalog contents without advancing its sequence",
        ));
    }
    Ok(())
}

async fn collect_bounded_target<Stream, Chunk, Error>(
    stream: Stream,
    maximum_bytes: usize,
    limit_message: &'static str,
) -> Result<Vec<u8>, ApplicationError>
where
    Stream: TryStream<Ok = Chunk, Error = Error>,
    Chunk: AsRef<[u8]>,
    Error: std::fmt::Display,
{
    stream
        .map_err(invalid_repository)
        .try_fold(Vec::new(), |mut collected, chunk| async move {
            let chunk = chunk.as_ref();
            let within_limit = collected
                .len()
                .checked_add(chunk.len())
                .is_some_and(|length| length <= maximum_bytes);
            if !within_limit {
                return Err(ApplicationError::new(
                    ApplicationErrorCode::InvalidProviderData,
                    limit_message,
                ));
            }
            collected.extend_from_slice(chunk);
            Ok(collected)
        })
        .await
}

fn provider_launch_application_error(error: ProviderLaunchError) -> ApplicationError {
    match error {
        ProviderLaunchError::NotInstalled(_) => ApplicationError::new(
            ApplicationErrorCode::ProviderInstallationNotFound,
            "provider has no active installation",
        ),
        ProviderLaunchError::InvalidInstallation { reason, .. } => ApplicationError::new(
            ApplicationErrorCode::InvalidProviderData,
            format!("active provider installation is invalid: {reason}"),
        ),
        ProviderLaunchError::Registry(reason) => internal_error(reason),
        ProviderLaunchError::Storage(error) => internal_error(error),
    }
}

fn internal_error(error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::Internal,
        format!("provider registry failed: {error}"),
    )
}

fn has_database_code(error: &sqlx::Error, code: &str) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|database_code| database_code == code)
}

fn validate_installed_manifest(
    manifest: &ProviderPackageManifest,
    command: &InstallProvider,
    release_protocol: &trakkin_provider_release::ProtocolRange,
) -> Result<(), ApplicationError> {
    manifest.validate().map_err(invalid_repository)?;
    if manifest.provider_id != command.provider_id
        || manifest.version.to_string() != command.version
        || manifest.target != command.target
        || &manifest.protocol != release_protocol
    {
        return Err(ApplicationError::new(
            ApplicationErrorCode::InvalidProviderData,
            "provider package manifest does not match the selected release",
        ));
    }
    Ok(())
}

fn validate_manifest_identity(
    manifest: &ProviderPackageManifest,
    provider_id: &str,
    version: &str,
    target: &str,
) -> Result<(), String> {
    manifest.validate().map_err(|error| error.to_string())?;
    if manifest.provider_id != provider_id
        || manifest.version.to_string() != version
        || manifest.target != target
    {
        return Err("manifest identity does not match the installation record".to_owned());
    }
    Ok(())
}

fn validate_launchable_manifest(
    manifest: &ProviderPackageManifest,
    provider_id: &str,
    version: &str,
    target: &str,
) -> Result<(), String> {
    validate_manifest_identity(manifest, provider_id, version, target)?;
    let host_protocol = trakkin_provider_sdk::current_protocol_version();
    if !manifest.protocol.supports(&PackageProtocolVersion {
        major: host_protocol.major,
        minor: host_protocol.minor,
    }) {
        return Err("provider protocol range is incompatible with this Trakkin host".to_owned());
    }
    Ok(())
}

fn extract_package(package: &[u8], destination: &Path) -> Result<ProviderPackageManifest, String> {
    let decoder = zstd::stream::read::Decoder::new(Cursor::new(package))
        .map_err(|error| error.to_string())?;
    let mut archive = tar::Archive::new(decoder);
    let mut files = HashMap::<String, Vec<u8>>::new();
    let mut archive_entries = 0_usize;
    let mut unpacked_bytes = 0_u64;
    for entry in archive.entries().map_err(|error| error.to_string())? {
        let mut entry = entry.map_err(|error| error.to_string())?;
        archive_entries = archive_entries
            .checked_add(1)
            .filter(|count| *count <= MAXIMUM_PROVIDER_PACKAGE_ENTRIES)
            .ok_or_else(|| "provider package contains too many entries".to_owned())?;
        let path = entry.path().map_err(|error| error.to_string())?;
        let path = safe_archive_path(&path)?;
        if entry.header().entry_type().is_dir() {
            continue;
        }
        if !entry.header().entry_type().is_file() {
            return Err(format!("package entry {path} is not a regular file"));
        }
        if files.len() >= MAXIMUM_PROVIDER_PACKAGE_FILES {
            return Err("provider package contains too many files".to_owned());
        }
        let declared_size = entry.size();
        unpacked_bytes = unpacked_bytes
            .checked_add(declared_size)
            .filter(|total| *total <= MAXIMUM_PROVIDER_UNPACKED_BYTES)
            .ok_or_else(|| "provider package exceeds the unpacked size limit".to_owned())?;
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        if bytes.len() as u64 != declared_size {
            return Err(format!("package entry {path} size is inconsistent"));
        }
        if files.insert(path.clone(), bytes).is_some() {
            return Err(format!("package entry {path} appears more than once"));
        }
    }
    let manifest_bytes = files
        .remove("manifest.json")
        .ok_or_else(|| "provider package is missing manifest.json".to_owned())?;
    if manifest_bytes.len() as u64 > MAXIMUM_PROVIDER_MANIFEST_BYTES {
        return Err("provider package manifest exceeds the size limit".to_owned());
    }
    let manifest = serde_json::from_slice::<ProviderPackageManifest>(&manifest_bytes)
        .map_err(|error| error.to_string())?;
    manifest.validate().map_err(|error| error.to_string())?;
    if files.len() != manifest.files.len() {
        return Err("provider package files do not match its manifest".to_owned());
    }
    for file in &manifest.files {
        let bytes = files
            .remove(&file.path)
            .ok_or_else(|| format!("provider package is missing {}", file.path))?;
        if bytes.len() as u64 != file.size || hex::encode(Sha256::digest(&bytes)) != file.sha256 {
            return Err(format!(
                "provider package file {} failed verification",
                file.path
            ));
        }
        let output = destination.join(&file.path);
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        std::fs::write(&output, bytes).map_err(|error| error.to_string())?;
        set_file_permissions_sync(&output, file.executable).map_err(|error| error.to_string())?;
    }
    Ok(manifest)
}

fn safe_archive_path(path: &Path) -> Result<String, String> {
    if path.as_os_str().is_empty()
        || !path.components().all(|component| {
            matches!(component, Component::Normal(_)) && component.as_os_str().to_str().is_some()
        })
    {
        return Err("provider package contains an unsafe path".to_owned());
    }
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| "provider package path is not UTF-8".to_owned())
}

#[cfg(unix)]
fn set_file_permissions_sync(path: &Path, executable: bool) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;

    let mode = if executable { 0o500 } else { 0o400 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_file_permissions_sync(_path: &Path, _executable: bool) -> Result<(), std::io::Error> {
    Ok(())
}

#[derive(FromRow)]
struct ActiveProviderRow {
    installation_id: Uuid,
    provider_id: String,
    version: String,
    target: String,
    manifest: Value,
    generation: Uuid,
}

async fn verify_directory(
    directory: &std::path::Path,
    files: &[PackageFile],
) -> Result<bool, ProviderLaunchError> {
    let Ok(root_metadata) = tokio::fs::symlink_metadata(directory).await else {
        return Ok(false);
    };
    if !root_metadata.file_type().is_dir() || root_metadata.file_type().is_symlink() {
        return Ok(false);
    }

    let expected = files
        .iter()
        .map(|file| PathBuf::from(&file.path))
        .collect::<HashSet<_>>();
    let expected_directories = expected
        .iter()
        .flat_map(|path| path.ancestors().skip(1))
        .filter(|path| !path.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .collect::<HashSet<_>>();
    let mut actual = HashSet::new();
    let mut directories = vec![directory.to_path_buf()];
    while let Some(current) = directories.pop() {
        let mut entries = tokio::fs::read_dir(&current).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = tokio::fs::symlink_metadata(&path).await?;
            if metadata.file_type().is_symlink() {
                return Ok(false);
            }
            let Ok(relative) = path.strip_prefix(directory) else {
                return Ok(false);
            };
            if metadata.file_type().is_dir() {
                if !expected_directories.contains(relative) {
                    return Ok(false);
                }
                directories.push(path);
                continue;
            }
            if !metadata.file_type().is_file() {
                return Ok(false);
            }
            actual.insert(relative.to_path_buf());
        }
    }
    if actual != expected {
        return Ok(false);
    }

    for file in files {
        let path = directory.join(&file.path);
        let Ok(metadata) = tokio::fs::symlink_metadata(&path).await else {
            return Ok(false);
        };
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Ok(false);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let expected_mode = if file.executable { 0o500 } else { 0o400 };
            if metadata.permissions().mode() & 0o777 != expected_mode {
                return Ok(false);
            }
        }
        let bytes = tokio::fs::read(path).await?;
        if bytes.len() as u64 != file.size || hex::encode(Sha256::digest(&bytes)) != file.sha256 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn verify_file(
    file: &PackageFile,
    bytes: &[u8],
    provider_id: &str,
) -> Result<(), ProviderLaunchError> {
    if bytes.len() as u64 != file.size || hex::encode(Sha256::digest(bytes)) != file.sha256 {
        return Err(invalid_installation(
            provider_id,
            format!("{} does not match its declared digest and size", file.path),
        ));
    }
    Ok(())
}

async fn read_installation_file(
    installation_root: &Path,
    file: &PackageFile,
    provider_id: &str,
) -> Result<Vec<u8>, ProviderLaunchError> {
    let root_metadata = tokio::fs::symlink_metadata(installation_root).await?;
    if !root_metadata.file_type().is_dir() || root_metadata.file_type().is_symlink() {
        return Err(invalid_installation(
            provider_id,
            "installation root is a symlink or invalid file type".to_owned(),
        ));
    }
    let mut path = installation_root.to_path_buf();
    let mut components = Path::new(&file.path).components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(component) = component else {
            return Err(invalid_installation(
                provider_id,
                format!("{} has an unsafe path", file.path),
            ));
        };
        path.push(component);
        let metadata = tokio::fs::symlink_metadata(&path).await?;
        let valid = if components.peek().is_some() {
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink()
        } else {
            metadata.file_type().is_file() && !metadata.file_type().is_symlink()
        };
        if !valid {
            return Err(invalid_installation(
                provider_id,
                format!("{} traverses a symlink or invalid file type", file.path),
            ));
        }
    }
    tokio::fs::read(path).await.map_err(Into::into)
}

fn invalid_installation(provider_id: &str, reason: String) -> ProviderLaunchError {
    ProviderLaunchError::InvalidInstallation {
        provider_id: provider_id.to_owned(),
        reason,
    }
}

async fn reset_directory(path: &Path) -> Result<(), std::io::Error> {
    remove_path_if_present(path).await?;
    tokio::fs::create_dir_all(path).await
}

async fn reset_runtime_directory(path: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "provider runtime root must be a directory and not a symlink",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(path).await?;
        }
        Err(error) => return Err(error),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
    }
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        remove_path_if_present(&entry.path()).await?;
    }
    Ok(())
}

async fn remove_path_if_present(path: &Path) -> Result<(), std::io::Error> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        tokio::fs::remove_dir_all(path).await
    } else {
        tokio::fs::remove_file(path).await
    }
}

async fn blob_matches_digest(path: &Path, digest: &str) -> Result<bool, std::io::Error> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let bytes = tokio::fs::read(path).await?;
    Ok(hex::encode(Sha256::digest(bytes)) == digest)
}

async fn write_staged_blob(
    directory: &Path,
    digest: &str,
    bytes: &[u8],
) -> Result<PathBuf, std::io::Error> {
    loop {
        let sequence = BLOB_STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("{digest}.{sequence}.staging"));
        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        if let Err(error) = file.write_all(bytes).await {
            drop(file);
            let _ = remove_path_if_present(&path).await;
            return Err(error);
        }
        return Ok(path);
    }
}

async fn remove_unreferenced_or_invalid_blobs(
    directory: &Path,
    referenced_blobs: &HashSet<String>,
) -> Result<(), std::io::Error> {
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        let digest = file_name
            .to_str()
            .filter(|name| referenced_blobs.contains(*name));
        let valid = match digest {
            Some(digest) => blob_matches_digest(&entry.path(), digest).await?,
            None => false,
        };
        if !valid {
            remove_path_if_present(&entry.path()).await?;
        }
    }
    Ok(())
}

#[cfg(unix)]
async fn set_file_permissions(
    path: &std::path::Path,
    executable: bool,
) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;

    let mode = if executable { 0o500 } else { 0o400 };
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).await
}

#[cfg(not(unix))]
async fn set_file_permissions(
    _path: &std::path::Path,
    _executable: bool,
) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn bounded_target_read_stops_before_oversized_chunk() {
        let target = stream::iter([
            Ok::<_, &'static str>(vec![1_u8, 2]),
            Ok(vec![3_u8, 4]),
            Ok(vec![5_u8]),
        ]);

        let error = collect_bounded_target(target, 3, "target exceeds limit")
            .await
            .expect_err("oversized target is rejected");

        assert_eq!(error.code, ApplicationErrorCode::InvalidProviderData);
        assert_eq!(error.message, "target exceeds limit");
    }

    #[test]
    fn repository_registration_requires_matching_trust_root_fingerprint() {
        let trusted_root = OFFICIAL_REPOSITORY_ROOT.to_vec();
        let mut command = RegisterProviderRepository {
            id: "dev.trakkin.test".to_owned(),
            display_name: "Test".to_owned(),
            metadata_base_url: "https://providers.example/metadata/".to_owned(),
            targets_base_url: "https://providers.example/targets/".to_owned(),
            trusted_root,
            trusted_root_sha256: "0".repeat(64),
            registered_at: OffsetDateTime::UNIX_EPOCH,
        };

        let error = validate_repository_registration(&command)
            .expect_err("mismatched fingerprint is rejected");
        assert_eq!(error.code, ApplicationErrorCode::InvalidField);
        assert_eq!(error.field_problems[0].path, "trustedRootSha256");

        command.trusted_root_sha256 = hex::encode(Sha256::digest(&command.trusted_root));
        validate_repository_registration(&command).expect("matching fingerprint is accepted");
    }

    #[test]
    fn repository_registration_rejects_invalid_tuf_root() {
        let trusted_root = b"{}".to_vec();
        let command = RegisterProviderRepository {
            id: "dev.trakkin.test".to_owned(),
            display_name: "Test".to_owned(),
            metadata_base_url: "https://providers.example/metadata/".to_owned(),
            targets_base_url: "https://providers.example/targets/".to_owned(),
            trusted_root_sha256: hex::encode(Sha256::digest(&trusted_root)),
            trusted_root,
            registered_at: OffsetDateTime::UNIX_EPOCH,
        };

        let error = validate_repository_registration(&command)
            .expect_err("invalid TUF root is rejected during registration");

        assert_eq!(error.code, ApplicationErrorCode::InvalidField);
        assert_eq!(error.field_problems[0].path, "trustedRoot");
        assert_eq!(error.field_problems[0].code, "invalid_tuf_root");
    }

    #[test]
    fn repository_urls_reject_embedded_credentials() {
        let error = parse_repository_url("https://token@example.com/metadata/")
            .expect_err("repository URL credentials are rejected");

        assert_eq!(error.code, ApplicationErrorCode::InvalidField);
    }

    #[tokio::test]
    async fn persisting_blob_repairs_corrupt_existing_content() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let bytes = b"verified provider package";
        let digest = hex::encode(Sha256::digest(bytes));
        let blob = directory.path().join("blobs/sha256").join(&digest);
        tokio::fs::create_dir_all(blob.parent().expect("blob parent"))
            .await
            .expect("blob directory");
        tokio::fs::write(&blob, b"corrupt")
            .await
            .expect("corrupt blob");
        let pool = PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/trakkin")
            .expect("lazy database pool");
        let registry = PostgresProviderRegistry::new(pool, directory.path(), directory.path());

        registry
            .persist_blob(&digest, bytes)
            .await
            .expect("verified blob persists");

        assert_eq!(tokio::fs::read(blob).await.expect("persisted blob"), bytes);
    }

    #[tokio::test]
    async fn reconciliation_removes_corrupt_referenced_blobs() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let blob_directory = directory.path().join("blobs/sha256");
        tokio::fs::create_dir_all(&blob_directory)
            .await
            .expect("blob directory");
        let valid_bytes = b"verified provider package";
        let valid_digest = hex::encode(Sha256::digest(valid_bytes));
        let corrupt_digest = hex::encode(Sha256::digest(b"expected provider package"));
        tokio::fs::write(blob_directory.join(&valid_digest), valid_bytes)
            .await
            .expect("valid blob");
        tokio::fs::write(blob_directory.join(&corrupt_digest), b"corrupt")
            .await
            .expect("corrupt blob");

        remove_unreferenced_or_invalid_blobs(
            &blob_directory,
            &HashSet::from([valid_digest.clone(), corrupt_digest.clone()]),
        )
        .await
        .expect("blobs reconcile");

        assert!(blob_directory.join(valid_digest).is_file());
        assert!(!blob_directory.join(corrupt_digest).exists());
    }

    #[tokio::test]
    async fn lifecycle_lock_serializes_registry_instances() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let pool = PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/trakkin")
            .expect("lazy database pool");
        let first_registry = PostgresProviderRegistry::new(
            pool.clone(),
            directory.path(),
            directory.path().join("runtime-one"),
        );
        let second_registry = PostgresProviderRegistry::new(
            pool,
            directory.path(),
            directory.path().join("runtime-two"),
        );
        let first_lock = first_registry
            .acquire_lifecycle_lock()
            .await
            .expect("first lifecycle lock");
        let second_lock = tokio::spawn(async move {
            second_registry
                .acquire_lifecycle_lock()
                .await
                .expect("second lifecycle lock")
        });
        tokio::task::yield_now().await;
        assert!(!second_lock.is_finished());

        drop(first_lock);
        tokio::time::timeout(std::time::Duration::from_secs(1), second_lock)
            .await
            .expect("second lock is released")
            .expect("second lock task succeeds");
    }

    #[test]
    fn extraction_rejects_oversized_manifest_before_writing() {
        let encoder =
            zstd::stream::write::Encoder::new(Vec::new(), 1).expect("provider package encoder");
        let mut archive = tar::Builder::new(encoder);
        let manifest = vec![b' '; (MAXIMUM_PROVIDER_MANIFEST_BYTES + 1) as usize];
        let mut header = tar::Header::new_gnu();
        header.set_size(manifest.len() as u64);
        header.set_mode(0o400);
        header.set_cksum();
        archive
            .append_data(&mut header, "manifest.json", Cursor::new(manifest))
            .expect("manifest entry");
        archive.finish().expect("provider package archive");
        let package = archive
            .into_inner()
            .expect("provider package encoder")
            .finish()
            .expect("provider package compression");
        let directory = tempfile::tempdir().expect("temporary directory");
        let destination = directory.path().join("installation");

        let error =
            extract_package(&package, &destination).expect_err("oversized manifest is rejected");

        assert!(error.contains("manifest exceeds the size limit"));
        assert!(!destination.exists());
    }

    #[test]
    fn extraction_rejects_excessive_archive_entries() {
        let encoder =
            zstd::stream::write::Encoder::new(Vec::new(), 1).expect("provider package encoder");
        let mut archive = tar::Builder::new(encoder);
        for index in 0..=MAXIMUM_PROVIDER_PACKAGE_ENTRIES {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_size(0);
            header.set_mode(0o500);
            header.set_cksum();
            archive
                .append_data(&mut header, format!("directory-{index}"), Cursor::new([]))
                .expect("directory entry");
        }
        archive.finish().expect("provider package archive");
        let package = archive
            .into_inner()
            .expect("provider package encoder")
            .finish()
            .expect("provider package compression");
        let directory = tempfile::tempdir().expect("temporary directory");
        let destination = directory.path().join("installation");

        let error =
            extract_package(&package, &destination).expect_err("excessive entries are rejected");

        assert!(error.contains("too many entries"));
        assert!(!destination.exists());
    }

    #[test]
    fn catalog_update_requires_monotonic_sequence_and_stable_contents() {
        let previous = serde_json::json!({"sequence": 2});
        let changed = serde_json::json!({"sequence": "changed"});

        let rollback = validate_catalog_update(Some(2), Some(&previous), 1, &previous)
            .expect_err("catalog rollback is rejected");
        assert_eq!(rollback.code, ApplicationErrorCode::InvalidProviderData);
        let mutation = validate_catalog_update(Some(2), Some(&previous), 2, &changed)
            .expect_err("same-sequence mutation is rejected");
        assert_eq!(mutation.code, ApplicationErrorCode::InvalidProviderData);
        validate_catalog_update(Some(2), Some(&previous), 2, &previous)
            .expect("idempotent refresh is accepted");
        validate_catalog_update(Some(2), Some(&previous), 3, &changed)
            .expect("advanced catalog is accepted");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_root_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("temporary directory");
        let target = directory.path().join("target");
        let runtime = directory.path().join("runtime");
        tokio::fs::create_dir(&target)
            .await
            .expect("symlink target directory");
        symlink(&target, &runtime).expect("runtime symlink");

        let error = reset_runtime_directory(&runtime)
            .await
            .expect_err("runtime symlink is rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(target.is_dir());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn installation_file_rejects_symlinked_parent_directory() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("temporary directory");
        let installation = directory.path().join("installation");
        let outside = directory.path().join("outside");
        tokio::fs::create_dir(&installation)
            .await
            .expect("installation directory");
        tokio::fs::create_dir(&outside)
            .await
            .expect("outside directory");
        tokio::fs::write(outside.join("provider"), b"provider")
            .await
            .expect("outside provider");
        symlink(&outside, installation.join("bin")).expect("symlinked parent");
        let file = package_file("bin/provider", b"provider", true);

        let error = read_installation_file(&installation, &file, "dev.trakkin.fixture")
            .await
            .expect_err("symlinked parent is rejected");

        assert!(matches!(
            error,
            ProviderLaunchError::InvalidInstallation { .. }
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn installation_file_rejects_symlinked_root_directory() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("temporary directory");
        let installation = directory.path().join("installation");
        let outside = directory.path().join("outside");
        tokio::fs::create_dir(&outside)
            .await
            .expect("outside directory");
        tokio::fs::write(outside.join("provider"), b"provider")
            .await
            .expect("outside provider");
        symlink(&outside, &installation).expect("symlinked installation root");
        let file = package_file("provider", b"provider", true);

        let error = read_installation_file(&installation, &file, "dev.trakkin.fixture")
            .await
            .expect_err("symlinked installation root is rejected");

        assert!(matches!(
            error,
            ProviderLaunchError::InvalidInstallation { .. }
        ));
    }

    #[tokio::test]
    async fn materialization_repairs_non_directory_destination() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let persistent = directory.path().join("persistent");
        let runtime = directory.path().join("runtime");
        let installation_id = Uuid::from_u128(1);
        let source = persistent
            .join("installations")
            .join(installation_id.to_string())
            .join("provider");
        tokio::fs::create_dir_all(source.parent().expect("source parent"))
            .await
            .expect("source directory");
        tokio::fs::write(&source, b"provider")
            .await
            .expect("source provider");
        set_file_permissions(&source, true)
            .await
            .expect("source permissions");
        tokio::fs::create_dir_all(&runtime)
            .await
            .expect("runtime directory");
        tokio::fs::write(runtime.join(installation_id.to_string()), b"stale")
            .await
            .expect("stale runtime file");
        let manifest = serde_json::from_value::<ProviderPackageManifest>(serde_json::json!({
            "schema": "trakkin.provider-package/v1",
            "providerId": "dev.trakkin.fixture",
            "version": "0.1.0",
            "target": "x86_64-unknown-linux-musl",
            "protocol": {
                "minimum": { "major": 1, "minor": 0 },
                "maximum": { "major": 1, "minor": 0 }
            },
            "entrypoint": "provider",
            "files": [{
                "path": "provider",
                "sha256": hex::encode(Sha256::digest(b"provider")),
                "size": 8,
                "executable": true
            }]
        }))
        .expect("provider manifest");
        let pool = PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/trakkin")
            .expect("lazy database pool");
        let registry = PostgresProviderRegistry::new(pool, persistent, &runtime);

        let materialized = registry
            .materialize(installation_id, &manifest)
            .await
            .expect("provider materializes");

        assert_eq!(
            tokio::fs::read(materialized.join("provider"))
                .await
                .expect("materialized provider"),
            b"provider"
        );
    }

    fn package_file(path: &str, bytes: &[u8], executable: bool) -> PackageFile {
        PackageFile {
            path: path.to_owned(),
            sha256: hex::encode(Sha256::digest(bytes)),
            size: bytes.len() as u64,
            executable,
        }
    }

    #[tokio::test]
    async fn runtime_directory_rejects_undeclared_entries() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let executable = directory.path().join("bin/provider");
        tokio::fs::create_dir_all(executable.parent().expect("provider parent"))
            .await
            .expect("provider directory");
        tokio::fs::write(&executable, b"provider")
            .await
            .expect("provider executable");
        set_file_permissions(&executable, true)
            .await
            .expect("provider permissions");
        let files = vec![package_file("bin/provider", b"provider", true)];
        assert!(
            verify_directory(directory.path(), &files)
                .await
                .expect("valid runtime directory")
        );

        tokio::fs::create_dir(directory.path().join("undeclared"))
            .await
            .expect("undeclared directory");
        assert!(
            !verify_directory(directory.path(), &files)
                .await
                .expect("invalid runtime directory")
        );
        tokio::fs::remove_dir(directory.path().join("undeclared"))
            .await
            .expect("undeclared directory is removed");
        tokio::fs::write(directory.path().join("unexpected"), b"unexpected")
            .await
            .expect("unexpected file");
        assert!(
            !verify_directory(directory.path(), &files)
                .await
                .expect("invalid runtime directory")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runtime_directory_rejects_incorrect_executable_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary directory");
        let executable = directory.path().join("provider");
        tokio::fs::write(&executable, b"provider")
            .await
            .expect("provider executable");
        tokio::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o777))
            .await
            .expect("provider permissions");

        assert!(
            !verify_directory(
                directory.path(),
                &[package_file("provider", b"provider", true)],
            )
            .await
            .expect("invalid runtime directory")
        );
    }
}
