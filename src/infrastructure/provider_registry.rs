use std::{
    collections::{BTreeMap, HashSet},
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::Arc,
};

use semver::Version;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, types::Json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::application::error::{ApplicationError, ApplicationErrorCode};
use crate::application::providers::{
    AvailableProviderSnapshot, ChangeProviderUpdatePolicy, InspectProviderRelease, InstallProvider,
    ProviderApiFuture, ProviderFuture, ProviderLaunchError, ProviderLaunchResolver,
    ProviderLaunchTarget, ProviderMaintainerSnapshot, ProviderRegistryApi,
    ProviderReleaseCompatibility, ProviderReleaseInspectionSnapshot, ProviderSnapshot,
    ProviderSourceSnapshot, ProviderUpdatePolicy, ProviderUpdateSnapshot, RefreshProviderSource,
    RemoveProvider, RemoveProviderSource, TrustProviderSource, UpdateProvider,
};
use crate::infrastructure::adapter_process::AdapterProcessLauncher;
use crate::infrastructure::provider_oci::{
    InspectedProviderArtifact, OciProviderSource, ProviderArtifactCompatibility,
    ProviderArtifactSource, ProviderMetadata, PulledProviderArtifact, valid_provider_digest,
    valid_repository_url,
};

const PROVIDER_EXECUTABLE: &str = "provider";

#[derive(Clone)]
pub struct PostgresProviderRegistry {
    pool: PgPool,
    persistent_root: PathBuf,
    runtime_root: PathBuf,
    artifact_source: Arc<dyn ProviderArtifactSource>,
    host_version: Version,
    platform: String,
}

struct ProviderLifecycleLock {
    _file: std::fs::File,
}

#[derive(Clone)]
pub struct ProbedProviderRegistry {
    registry: PostgresProviderRegistry,
    launcher: AdapterProcessLauncher<PostgresProviderRegistry>,
}

struct PreparedArtifact {
    artifact_id: Uuid,
    artifact: PulledProviderArtifact,
}

impl ProbedProviderRegistry {
    pub fn new(registry: PostgresProviderRegistry, host_version: impl Into<String>) -> Self {
        Self {
            launcher: AdapterProcessLauncher::new(registry.clone(), host_version),
            registry,
        }
    }

    pub async fn refresh_all_provider_sources(&self, checked_at: OffsetDateTime) {
        let source_ids = match self.registry.source_ids().await {
            Ok(source_ids) => source_ids,
            Err(error) => {
                tracing::warn!(
                    error.code = error.code.as_str(),
                    "provider source refresh failed"
                );
                return;
            }
        };
        for source_id in source_ids {
            if let Err(error) = self
                .refresh(RefreshProviderSource {
                    source_id,
                    checked_at,
                })
                .await
            {
                tracing::warn!(
                    %source_id,
                    error.code = error.code.as_str(),
                    "provider source refresh failed"
                );
            }
        }
    }

    async fn install(
        &self,
        command: InstallProvider,
    ) -> Result<ProviderSnapshot, ApplicationError> {
        let version = parse_provider_version(&command.version)?;
        validate_index_digest(&command.index_digest)?;
        ensure_supported_platform(&self.registry.platform)?;
        let _lifecycle_lock = self
            .registry
            .acquire_lifecycle_lock()
            .await
            .map_err(internal_error)?;
        let source = self.registry.source(command.source_id).await?;
        let prepared = self
            .registry
            .prepare_artifact(
                &source,
                &command.provider_id,
                &version,
                &command.index_digest,
                command.artifact_id,
            )
            .await?;
        if let Err(error) = self.probe(&prepared).await {
            let _ = remove_path_if_present(&self.registry.artifact_directory(prepared.artifact_id))
                .await;
            return Err(error);
        }
        self.registry.commit_install(command, prepared).await
    }

    async fn update(&self, command: UpdateProvider) -> Result<ProviderSnapshot, ApplicationError> {
        let version = parse_provider_version(&command.version)?;
        validate_index_digest(&command.index_digest)?;
        ensure_supported_platform(&self.registry.platform)?;
        let _lifecycle_lock = self
            .registry
            .acquire_lifecycle_lock()
            .await
            .map_err(internal_error)?;
        let current = self.registry.update_target(&command.provider_id).await?;
        let current_version = Version::parse(&current.version).map_err(internal_error)?;
        validate_update_selection(
            &current_version,
            &current.index_digest,
            &version,
            &command.index_digest,
        )?;
        let source = current.source_snapshot()?;
        let prepared = match self
            .registry
            .prepare_artifact(
                &source,
                &command.provider_id,
                &version,
                &command.index_digest,
                command.artifact_id,
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                self.registry
                    .record_update_error(
                        &command.provider_id,
                        &provider_update_error_message(&error),
                        command.updated_at,
                    )
                    .await?;
                return Err(error);
            }
        };
        if prepared.artifact.metadata.id != command.provider_id {
            let _ = remove_path_if_present(&self.registry.artifact_directory(prepared.artifact_id))
                .await;
            let error = ApplicationError::new(
                ApplicationErrorCode::InvalidProviderData,
                "provider update metadata identity does not match the installed provider",
            );
            self.registry
                .record_update_error(&command.provider_id, &error.message, command.updated_at)
                .await?;
            return Err(error);
        }
        if let Err(error) = self.probe(&prepared).await {
            let _ = remove_path_if_present(&self.registry.artifact_directory(prepared.artifact_id))
                .await;
            self.registry
                .record_update_error(
                    &command.provider_id,
                    &provider_update_error_message(&error),
                    command.updated_at,
                )
                .await?;
            return Err(error);
        }
        self.registry
            .commit_update(command, current, prepared)
            .await
    }

    async fn change_policy(
        &self,
        command: ChangeProviderUpdatePolicy,
    ) -> Result<ProviderSnapshot, ApplicationError> {
        let provider = self.registry.change_policy(&command).await?;
        let Some(update) = provider.available_update.clone() else {
            return Ok(provider);
        };
        if command.update_policy == ProviderUpdatePolicy::Manual {
            return Ok(provider);
        }
        let result = self
            .update_discovered_release(
                command.provider_id.clone(),
                provider.source_id,
                update.version,
                command.updated_at,
            )
            .await;
        match result {
            Ok(provider) => {
                tracing::info!(
                    provider.id = %provider.provider_id,
                    provider.version = %provider.version,
                    update.policy = ?provider.update_policy,
                    "provider updated automatically"
                );
                Ok(provider)
            }
            Err(error) => {
                tracing::warn!(
                    provider.id = %command.provider_id,
                    error.code = error.code.as_str(),
                    "automatic provider update failed"
                );
                let error_message = provider_update_error_message(&error);
                self.registry
                    .record_update_error(&command.provider_id, &error_message, command.updated_at)
                    .await?;
                self.registry.provider(&command.provider_id).await
            }
        }
    }

    async fn refresh(
        &self,
        command: RefreshProviderSource,
    ) -> Result<ProviderSourceSnapshot, ApplicationError> {
        let (source, updates) = self.registry.discover_updates(&command).await?;
        for update in updates {
            let provider_id = update.provider_id.clone();
            let version = update.version.clone();
            let updated_at = command.checked_at;
            let result = self
                .update_discovered_release(
                    update.provider_id,
                    update.source_id,
                    update.version,
                    updated_at,
                )
                .await;
            match result {
                Ok(provider) => tracing::info!(
                    provider.id = %provider.provider_id,
                    provider.version = %provider.version,
                    provider.source.id = %command.source_id,
                    "provider updated automatically"
                ),
                Err(error) => {
                    tracing::warn!(
                        provider.id = %provider_id,
                        provider.version = %version,
                        provider.source.id = %command.source_id,
                        error.code = error.code.as_str(),
                        "automatic provider update failed"
                    );
                    let error_message = provider_update_error_message(&error);
                    self.registry
                        .record_update_error(&provider_id, &error_message, updated_at)
                        .await?;
                }
            }
        }
        Ok(source)
    }

    async fn inspect(
        &self,
        command: InspectProviderRelease,
    ) -> Result<ProviderReleaseInspectionSnapshot, ApplicationError> {
        let version = parse_provider_version(&command.version)?;
        ensure_supported_platform(&self.registry.platform)?;
        let source = self.registry.source(command.source_id).await?;
        let inspection = self
            .registry
            .artifact_source
            .inspect(
                &source,
                &command.provider_id,
                &version,
                &self.registry.platform,
                &self.registry.host_version,
            )
            .await?;
        inspection_snapshot(inspection, self.registry.platform.clone())
    }

    async fn update_discovered_release(
        &self,
        provider_id: String,
        source_id: Uuid,
        version: String,
        updated_at: OffsetDateTime,
    ) -> Result<ProviderSnapshot, ApplicationError> {
        let requested_version = parse_provider_version(&version)?;
        let current = self.registry.update_target(&provider_id).await?;
        let current_version = Version::parse(&current.version).map_err(internal_error)?;
        if requested_version <= current_version {
            return Err(ApplicationError::field(
                ApplicationErrorCode::InvalidField,
                "provider update request is invalid",
                "version",
                "provider_version_not_newer",
                "automatic provider update version must be newer than the installed version",
            ));
        }
        let inspection = self
            .inspect(InspectProviderRelease {
                source_id,
                provider_id: provider_id.clone(),
                version: version.clone(),
            })
            .await?;
        self.update(UpdateProvider {
            provider_id,
            artifact_id: random_uuid().map_err(internal_error)?,
            version,
            index_digest: inspection.index_digest,
            updated_at,
        })
        .await
    }

    async fn probe(&self, prepared: &PreparedArtifact) -> Result<(), ApplicationError> {
        let working_directory = self.registry.artifact_directory(prepared.artifact_id);
        self.launcher
            .probe_target(
                ProviderLaunchTarget {
                    provider_id: prepared.artifact.metadata.id.clone(),
                    version: prepared.artifact.metadata.version.to_string(),
                    artifact_id: prepared.artifact_id,
                    executable: working_directory.join(PROVIDER_EXECUTABLE),
                    working_directory,
                },
                format!("provider-artifact-probe-{}", prepared.artifact_id),
            )
            .await
            .map_err(|error| {
                ApplicationError::new(
                    ApplicationErrorCode::InvalidProviderData,
                    format!("provider probe failed: {error}"),
                )
            })
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
            artifact_source: Arc::new(OciProviderSource::default()),
            host_version: Version::parse(env!("CARGO_PKG_VERSION"))
                .expect("Trakkin package version must be valid SemVer"),
            platform: host_platform(),
        }
    }

    pub async fn reconcile(&self, _reconciled_at: OffsetDateTime) -> Result<(), ApplicationError> {
        let _lifecycle_lock = self
            .acquire_lifecycle_lock()
            .await
            .map_err(internal_error)?;
        reset_directory(&self.persistent_root.join("staging"))
            .await
            .map_err(internal_error)?;
        reset_directory(&self.runtime_root)
            .await
            .map_err(internal_error)?;

        let rows = sqlx::query_as::<_, ReconciliationRow>(
            "SELECT artifact_id, provider_id, version, platform, binary_digest, metadata
FROM providers
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)?;
        let mut valid_artifacts = HashSet::new();
        let mut invalid_providers = Vec::new();
        for row in rows {
            if self
                .validate_artifact_metadata(
                    &row.provider_id,
                    &row.version,
                    &row.platform,
                    row.metadata,
                )
                .is_err()
                || !artifact_matches(
                    &self.artifact_directory(row.artifact_id),
                    &row.binary_digest,
                )
                .await
                .map_err(internal_error)?
            {
                invalid_providers.push(row.provider_id);
            } else {
                valid_artifacts.insert(row.artifact_id);
            }
        }
        if !invalid_providers.is_empty() {
            sqlx::query(
                "DELETE FROM providers WHERE provider_id = ANY($1)
",
            )
            .bind(&invalid_providers)
            .execute(&self.pool)
            .await
            .map_err(internal_error)?;
        }

        let artifacts = self.persistent_root.join("artifacts");
        ensure_plain_directory(&artifacts)
            .await
            .map_err(internal_error)?;
        let mut entries = tokio::fs::read_dir(&artifacts)
            .await
            .map_err(internal_error)?;
        while let Some(entry) = entries.next_entry().await.map_err(internal_error)? {
            let keep = entry
                .file_name()
                .to_str()
                .and_then(|name| Uuid::parse_str(name).ok())
                .is_some_and(|artifact_id| valid_artifacts.contains(&artifact_id));
            if !keep {
                remove_path_if_present(&entry.path())
                    .await
                    .map_err(internal_error)?;
            }
        }
        Ok(())
    }

    async fn resolve(
        &self,
        provider_id: &str,
    ) -> Result<ProviderLaunchTarget, ProviderLaunchError> {
        let row = sqlx::query_as::<_, LaunchRow>(
            "SELECT artifact_id, provider_id, version, platform, binary_digest, metadata
FROM providers
WHERE provider_id = $1
",
        )
        .bind(provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| ProviderLaunchError::Registry(error.to_string()))?
        .ok_or_else(|| ProviderLaunchError::NotInstalled(provider_id.to_owned()))?;
        self.launch_target(row).await
    }

    async fn launch_target(
        &self,
        row: LaunchRow,
    ) -> Result<ProviderLaunchTarget, ProviderLaunchError> {
        self.validate_artifact_metadata(
            &row.provider_id,
            &row.version,
            &row.platform,
            row.metadata,
        )
        .map_err(|reason| invalid_artifact(&row.provider_id, reason))?;
        let working_directory = self
            .materialize(row.artifact_id, &row.binary_digest, &row.provider_id)
            .await?;
        Ok(ProviderLaunchTarget {
            provider_id: row.provider_id,
            version: row.version,
            artifact_id: row.artifact_id,
            executable: working_directory.join(PROVIDER_EXECUTABLE),
            working_directory,
        })
    }

    fn validate_artifact_metadata(
        &self,
        provider_id: &str,
        version: &str,
        platform: &str,
        metadata: Value,
    ) -> Result<(), String> {
        if platform != self.platform {
            return Err("provider platform does not match this host".to_owned());
        }
        let version = Version::parse(version).map_err(|error| error.to_string())?;
        let metadata = serde_json::from_value::<ProviderMetadata>(metadata)
            .map_err(|error| error.to_string())?;
        metadata
            .validate(provider_id, &version, platform)
            .map_err(|error| error.message)?;
        if metadata.compatibility(true, &self.host_version)
            != ProviderArtifactCompatibility::Compatible
        {
            return Err("provider release is incompatible with this Trakkin version".to_owned());
        }
        Ok(())
    }

    async fn materialize(
        &self,
        artifact_id: Uuid,
        binary_digest: &str,
        provider_id: &str,
    ) -> Result<PathBuf, ProviderLaunchError> {
        let _lifecycle_lock = self
            .acquire_lifecycle_lock()
            .await
            .map_err(|error| ProviderLaunchError::Registry(error.to_string()))?;
        let source = self.artifact_directory(artifact_id);
        if !artifact_matches(&source, binary_digest).await? {
            return Err(invalid_artifact(
                provider_id,
                "provider binary does not match its database record",
            ));
        }
        let destination = self.runtime_root.join(artifact_id.to_string());
        if artifact_matches(&destination, binary_digest).await? {
            return Ok(destination);
        }
        remove_path_if_present(&destination).await?;
        ensure_plain_directory(&self.runtime_root).await?;
        let staging = self.runtime_root.join(format!("{artifact_id}.staging"));
        remove_path_if_present(&staging).await?;
        tokio::fs::create_dir(&staging).await?;
        let executable = staging.join(PROVIDER_EXECUTABLE);
        tokio::fs::copy(source.join(PROVIDER_EXECUTABLE), &executable).await?;
        set_executable_permissions(&executable).await?;
        if !artifact_matches(&staging, binary_digest).await? {
            let _ = tokio::fs::remove_dir_all(&staging).await;
            return Err(invalid_artifact(
                provider_id,
                "materialized provider binary failed verification",
            ));
        }
        tokio::fs::rename(&staging, &destination).await?;
        Ok(destination)
    }

    async fn source_ids(&self) -> Result<Vec<Uuid>, ApplicationError> {
        sqlx::query_scalar(
            "SELECT id FROM provider_sources ORDER BY repository_url, id
",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)
    }

    async fn sources(&self) -> Result<Vec<ProviderSourceSnapshot>, ApplicationError> {
        sqlx::query_as::<_, SourceRow>(SOURCE_COLUMNS)
            .fetch_all(&self.pool)
            .await
            .map_err(internal_error)?
            .into_iter()
            .map(SourceRow::snapshot)
            .collect()
    }

    async fn source(&self, source_id: Uuid) -> Result<ProviderSourceSnapshot, ApplicationError> {
        sqlx::query_as::<_, SourceRow>(SOURCE_BY_ID)
            .bind(source_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(internal_error)?
            .ok_or_else(provider_source_not_found)?
            .snapshot()
    }

    async fn trust_source(
        &self,
        command: TrustProviderSource,
    ) -> Result<ProviderSourceSnapshot, ApplicationError> {
        validate_source(&command)?;
        let source = ProviderSourceSnapshot {
            id: command.source_id,
            repository_url: command.repository_url.clone(),
            available_providers: Vec::new(),
            trusted_at: command.trusted_at,
            last_checked_at: None,
            last_error: None,
        };
        let available_providers = self.artifact_source.list_releases(&source).await?;
        let row = sqlx::query_as::<_, SourceRow>(
            "INSERT INTO provider_sources (
    id, repository_url, available_providers, trusted_at, last_checked_at
) VALUES ($1, $2, $3, $4, $4)
RETURNING
    id,
    repository_url,
    available_providers,
    trusted_at,
    last_checked_at,
    last_error
",
        )
        .bind(command.source_id)
        .bind(&command.repository_url)
        .bind(Json(&available_providers))
        .bind(command.trusted_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| {
            if has_database_code(&error, "23505") {
                ApplicationError::new(
                    ApplicationErrorCode::ProviderConflict,
                    "provider source is already trusted",
                )
            } else {
                internal_error(error)
            }
        })?;
        row.snapshot()
    }

    async fn remove_source(&self, command: RemoveProviderSource) -> Result<(), ApplicationError> {
        let deleted = sqlx::query_scalar::<_, Uuid>(
            "DELETE FROM provider_sources
WHERE id = $1
RETURNING id
",
        )
        .bind(command.source_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            if has_database_code(&error, "23001") || has_database_code(&error, "23503") {
                ApplicationError::new(
                    ApplicationErrorCode::ProviderConflict,
                    "provider source is still used by an installed provider",
                )
            } else {
                internal_error(error)
            }
        })?;
        if deleted.is_none() {
            return Err(provider_source_not_found());
        }
        Ok(())
    }

    async fn providers(&self) -> Result<Vec<ProviderSnapshot>, ApplicationError> {
        sqlx::query_as::<_, ProviderRow>(PROVIDER_COLUMNS)
            .fetch_all(&self.pool)
            .await
            .map_err(internal_error)?
            .into_iter()
            .map(ProviderRow::snapshot)
            .collect()
    }

    async fn provider(&self, provider_id: &str) -> Result<ProviderSnapshot, ApplicationError> {
        sqlx::query_as::<_, ProviderRow>(PROVIDER_BY_ID)
            .bind(provider_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(internal_error)?
            .ok_or_else(provider_not_found)?
            .snapshot()
    }

    async fn prepare_artifact(
        &self,
        source: &ProviderSourceSnapshot,
        provider_id: &str,
        version: &Version,
        index_digest: &str,
        artifact_id: Uuid,
    ) -> Result<PreparedArtifact, ApplicationError> {
        let staging_root = self.persistent_root.join("staging");
        ensure_plain_directory(&staging_root)
            .await
            .map_err(internal_error)?;
        let artifact = self
            .artifact_source
            .pull_pinned(
                source,
                provider_id,
                version,
                index_digest,
                &self.platform,
                &self.host_version,
            )
            .await?;
        self.stage_artifact(artifact_id, &artifact.binary).await?;
        Ok(PreparedArtifact {
            artifact_id,
            artifact,
        })
    }

    async fn stage_artifact(
        &self,
        artifact_id: Uuid,
        binary: &[u8],
    ) -> Result<(), ApplicationError> {
        let staging = self
            .persistent_root
            .join("staging")
            .join(artifact_id.to_string());
        let artifact = self.artifact_directory(artifact_id);
        ensure_plain_directory(&self.persistent_root.join("artifacts"))
            .await
            .map_err(internal_error)?;
        remove_path_if_present(&staging)
            .await
            .map_err(internal_error)?;
        if tokio::fs::try_exists(&artifact)
            .await
            .map_err(internal_error)?
        {
            return Err(ApplicationError::new(
                ApplicationErrorCode::ProviderConflict,
                "provider artifact ID already exists",
            ));
        }
        tokio::fs::create_dir(&staging)
            .await
            .map_err(internal_error)?;
        let executable = staging.join(PROVIDER_EXECUTABLE);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&executable)
            .await
            .map_err(internal_error)?;
        file.write_all(binary).await.map_err(internal_error)?;
        file.sync_all().await.map_err(internal_error)?;
        drop(file);
        set_executable_permissions(&executable)
            .await
            .map_err(internal_error)?;
        tokio::fs::rename(&staging, &artifact)
            .await
            .map_err(internal_error)?;
        Ok(())
    }

    async fn commit_install(
        &self,
        command: InstallProvider,
        prepared: PreparedArtifact,
    ) -> Result<ProviderSnapshot, ApplicationError> {
        let policy = update_policy_value(command.update_policy);
        let result = sqlx::query(
            "INSERT INTO providers (
    provider_id, artifact_id, source_id, version, platform, index_digest,
    manifest_digest, metadata_digest, binary_digest, metadata, update_policy,
    installed_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12, $12
)
",
        )
        .bind(&prepared.artifact.metadata.id)
        .bind(prepared.artifact_id)
        .bind(command.source_id)
        .bind(&command.version)
        .bind(&self.platform)
        .bind(&prepared.artifact.index_digest)
        .bind(&prepared.artifact.manifest_digest)
        .bind(&prepared.artifact.metadata_digest)
        .bind(&prepared.artifact.binary_digest)
        .bind(&prepared.artifact.metadata_value)
        .bind(policy)
        .bind(command.installed_at)
        .execute(&self.pool)
        .await;
        if let Err(error) = result {
            let _ = remove_path_if_present(&self.artifact_directory(prepared.artifact_id)).await;
            return Err(if has_database_code(&error, "23505") {
                ApplicationError::new(
                    ApplicationErrorCode::ProviderAlreadyInstalled,
                    "this provider identity is already installed",
                )
            } else if has_database_code(&error, "23503") {
                provider_source_not_found()
            } else {
                internal_error(error)
            });
        }
        self.provider(&prepared.artifact.metadata.id).await
    }

    async fn update_target(&self, provider_id: &str) -> Result<UpdateTargetRow, ApplicationError> {
        sqlx::query_as::<_, UpdateTargetRow>(
            "SELECT
    provider.artifact_id, provider.version, provider.index_digest,
    source.id AS source_id, source.repository_url,
    source.available_providers,
    source.trusted_at, source.last_checked_at, source.last_error
FROM providers AS provider
INNER JOIN provider_sources AS source ON source.id = provider.source_id
WHERE provider.provider_id = $1
",
        )
        .bind(provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal_error)?
        .ok_or_else(provider_not_found)
    }

    async fn commit_update(
        &self,
        command: UpdateProvider,
        current: UpdateTargetRow,
        prepared: PreparedArtifact,
    ) -> Result<ProviderSnapshot, ApplicationError> {
        let committed: Result<(), ApplicationError> = async {
            let mut transaction = self.pool.begin().await.map_err(internal_error)?;
            let locked = sqlx::query_as::<_, LockedProviderRow>(
                "SELECT artifact_id, version
FROM providers
WHERE provider_id = $1
FOR UPDATE
",
            )
            .bind(&command.provider_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(internal_error)?
            .ok_or_else(provider_not_found)?;
            if locked.artifact_id != current.artifact_id || locked.version != current.version {
                return Err(ApplicationError::new(
                    ApplicationErrorCode::ProviderConflict,
                    "provider changed while its update was being prepared",
                ));
            }
            sqlx::query(
                "UPDATE providers
SET
    artifact_id = $2,
    version = $3,
    platform = $4,
    index_digest = $5,
    manifest_digest = $6,
    metadata_digest = $7,
    binary_digest = $8,
    metadata = $9,
    available_version = NULL,
    available_discovered_at = NULL,
    last_update_error = NULL,
    updated_at = $10
WHERE provider_id = $1
",
            )
            .bind(&command.provider_id)
            .bind(prepared.artifact_id)
            .bind(&command.version)
            .bind(&self.platform)
            .bind(&prepared.artifact.index_digest)
            .bind(&prepared.artifact.manifest_digest)
            .bind(&prepared.artifact.metadata_digest)
            .bind(&prepared.artifact.binary_digest)
            .bind(&prepared.artifact.metadata_value)
            .bind(command.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
            transaction.commit().await.map_err(internal_error)?;
            Ok(())
        }
        .await;
        if let Err(error) = committed {
            let _ = remove_path_if_present(&self.artifact_directory(prepared.artifact_id)).await;
            return Err(error);
        }

        let _ = remove_path_if_present(&self.artifact_directory(current.artifact_id)).await;
        let _ =
            remove_path_if_present(&self.runtime_root.join(current.artifact_id.to_string())).await;
        self.provider(&command.provider_id).await
    }

    async fn change_policy(
        &self,
        command: &ChangeProviderUpdatePolicy,
    ) -> Result<ProviderSnapshot, ApplicationError> {
        let updated = sqlx::query_scalar::<_, String>(
            "UPDATE providers
SET update_policy = $2, updated_at = $3
WHERE provider_id = $1
RETURNING provider_id
",
        )
        .bind(&command.provider_id)
        .bind(update_policy_value(command.update_policy))
        .bind(command.updated_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal_error)?;
        if updated.is_none() {
            return Err(provider_not_found());
        }
        self.provider(&command.provider_id).await
    }

    async fn discover_updates(
        &self,
        command: &RefreshProviderSource,
    ) -> Result<(ProviderSourceSnapshot, Vec<AutomaticUpdate>), ApplicationError> {
        let source = self.source(command.source_id).await?;
        let available_providers = match self.artifact_source.list_releases(&source).await {
            Ok(available_providers) => available_providers,
            Err(error) => {
                sqlx::query(
                    "UPDATE provider_sources
SET last_checked_at = $2, last_error = $3
WHERE id = $1
",
                )
                .bind(command.source_id)
                .bind(command.checked_at)
                .bind(&error.message)
                .execute(&self.pool)
                .await
                .map_err(internal_error)?;
                return Err(error);
            }
        };
        let providers = sqlx::query_as::<_, DiscoveryRow>(
            "SELECT provider_id, version, update_policy
FROM providers
WHERE source_id = $1
",
        )
        .bind(command.source_id)
        .fetch_all(&self.pool)
        .await
        .map_err(internal_error)?;
        let mut automatic = Vec::new();
        let mut transaction = self.pool.begin().await.map_err(internal_error)?;
        for provider in providers {
            let installed = Version::parse(&provider.version).map_err(internal_error)?;
            let available = latest_update(&available_providers, &provider.provider_id, &installed);
            let available_version = available.as_ref().map(ToString::to_string);
            sqlx::query(
                "UPDATE providers
SET
    available_discovered_at = CASE
        WHEN available_version = $2 THEN available_discovered_at
        WHEN $2 IS NULL THEN NULL
        ELSE $3
    END,
    available_version = $2,
    last_update_error = CASE
        WHEN available_version = $2 THEN last_update_error
        ELSE NULL
    END
WHERE provider_id = $1
",
            )
            .bind(&provider.provider_id)
            .bind(&available_version)
            .bind(command.checked_at)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
            if parse_update_policy(&provider.update_policy)? == ProviderUpdatePolicy::Automatic
                && let Some(version) = available_version
            {
                automatic.push(AutomaticUpdate {
                    provider_id: provider.provider_id,
                    source_id: command.source_id,
                    version,
                });
            }
        }
        sqlx::query(
            "UPDATE provider_sources
SET available_providers = $2, last_checked_at = $3, last_error = NULL
WHERE id = $1
",
        )
        .bind(command.source_id)
        .bind(Json(&available_providers))
        .bind(command.checked_at)
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
        transaction.commit().await.map_err(internal_error)?;
        Ok((self.source(command.source_id).await?, automatic))
    }

    async fn record_update_error(
        &self,
        provider_id: &str,
        error: &str,
        updated_at: OffsetDateTime,
    ) -> Result<(), ApplicationError> {
        sqlx::query(
            "UPDATE providers
SET last_update_error = $2, updated_at = $3
WHERE provider_id = $1
",
        )
        .bind(provider_id)
        .bind(error)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .map_err(internal_error)?;
        Ok(())
    }

    async fn remove_provider(&self, command: RemoveProvider) -> Result<(), ApplicationError> {
        let _lifecycle_lock = self
            .acquire_lifecycle_lock()
            .await
            .map_err(internal_error)?;
        let mut transaction = self.pool.begin().await.map_err(internal_error)?;
        let artifact_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT artifact_id
FROM providers
WHERE provider_id = $1
FOR UPDATE
",
        )
        .bind(&command.provider_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(internal_error)?
        .ok_or_else(provider_not_found)?;
        let in_use = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
    SELECT 1 FROM connections
    WHERE provider_id = $1 AND deleted_at IS NULL
)
",
        )
        .bind(&command.provider_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(internal_error)?;
        if in_use {
            return Err(ApplicationError::new(
                ApplicationErrorCode::ProviderConflict,
                "provider cannot be removed while connections use it",
            ));
        }
        sqlx::query(
            "DELETE FROM providers WHERE provider_id = $1
",
        )
        .bind(&command.provider_id)
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
        transaction.commit().await.map_err(internal_error)?;
        let _ = remove_path_if_present(&self.artifact_directory(artifact_id)).await;
        let _ = remove_path_if_present(&self.runtime_root.join(artifact_id.to_string())).await;
        Ok(())
    }

    async fn acquire_lifecycle_lock(&self) -> Result<ProviderLifecycleLock, std::io::Error> {
        ensure_plain_directory(&self.persistent_root).await?;
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

    fn artifact_directory(&self, artifact_id: Uuid) -> PathBuf {
        self.persistent_root
            .join("artifacts")
            .join(artifact_id.to_string())
    }
}

impl ProviderLaunchResolver for PostgresProviderRegistry {
    fn resolve_provider(&self, provider_id: &str) -> ProviderFuture<'_, ProviderLaunchTarget> {
        let provider_id = provider_id.to_owned();
        Box::pin(async move { self.resolve(&provider_id).await })
    }
}

impl ProviderRegistryApi for ProbedProviderRegistry {
    fn ensure_provider_installed(&self, provider_id: &str) -> ProviderApiFuture<'_, ()> {
        let provider_id = provider_id.to_owned();
        Box::pin(async move {
            self.registry
                .resolve(&provider_id)
                .await
                .map(|_| ())
                .map_err(provider_launch_application_error)
        })
    }

    fn list_provider_sources(&self) -> ProviderApiFuture<'_, Vec<ProviderSourceSnapshot>> {
        Box::pin(self.registry.sources())
    }

    fn trust_provider_source(
        &self,
        command: TrustProviderSource,
    ) -> ProviderApiFuture<'_, ProviderSourceSnapshot> {
        Box::pin(async move {
            let source_id = command.source_id;
            let result = self.registry.trust_source(command).await;
            match &result {
                Ok(source) => tracing::info!(
                    provider.source.id = %source_id,
                    available_provider_count = source.available_providers.len(),
                    "provider source trusted"
                ),
                Err(error) => tracing::warn!(
                    provider.source.id = %source_id,
                    error.code = error.code.as_str(),
                    "provider source trust failed"
                ),
            }
            result
        })
    }

    fn refresh_provider_source(
        &self,
        command: RefreshProviderSource,
    ) -> ProviderApiFuture<'_, ProviderSourceSnapshot> {
        Box::pin(async move {
            let source_id = command.source_id;
            let result = self.refresh(command).await;
            match &result {
                Ok(source) => tracing::info!(
                    provider.source.id = %source_id,
                    available_provider_count = source.available_providers.len(),
                    "provider source refreshed"
                ),
                Err(error) => tracing::warn!(
                    provider.source.id = %source_id,
                    error.code = error.code.as_str(),
                    "provider source refresh failed"
                ),
            }
            result
        })
    }

    fn remove_provider_source(&self, command: RemoveProviderSource) -> ProviderApiFuture<'_, ()> {
        Box::pin(async move {
            let source_id = command.source_id;
            let result = self.registry.remove_source(command).await;
            match &result {
                Ok(()) => tracing::info!(
                    provider.source.id = %source_id,
                    "provider source removed"
                ),
                Err(error) => tracing::warn!(
                    provider.source.id = %source_id,
                    error.code = error.code.as_str(),
                    "provider source removal failed"
                ),
            }
            result
        })
    }

    fn inspect_provider_release(
        &self,
        command: InspectProviderRelease,
    ) -> ProviderApiFuture<'_, ProviderReleaseInspectionSnapshot> {
        Box::pin(self.inspect(command))
    }

    fn list_providers(&self) -> ProviderApiFuture<'_, Vec<ProviderSnapshot>> {
        Box::pin(self.registry.providers())
    }

    fn install_provider(
        &self,
        command: InstallProvider,
    ) -> ProviderApiFuture<'_, ProviderSnapshot> {
        Box::pin(async move {
            let source_id = command.source_id;
            let artifact_id = command.artifact_id;
            let provider_id = command.provider_id.clone();
            let result = self.install(command).await;
            match &result {
                Ok(provider) => tracing::info!(
                    provider.id = %provider.provider_id,
                    provider.version = %provider.version,
                    provider.source.id = %source_id,
                    provider.artifact.id = %artifact_id,
                    "provider installed"
                ),
                Err(error) => tracing::warn!(
                    provider.id = %provider_id,
                    provider.source.id = %source_id,
                    provider.artifact.id = %artifact_id,
                    error.code = error.code.as_str(),
                    "provider installation failed"
                ),
            }
            result
        })
    }

    fn update_provider(&self, command: UpdateProvider) -> ProviderApiFuture<'_, ProviderSnapshot> {
        Box::pin(async move {
            let provider_id = command.provider_id.clone();
            let artifact_id = command.artifact_id;
            let result = self.update(command).await;
            match &result {
                Ok(provider) => tracing::info!(
                    provider.id = %provider.provider_id,
                    provider.version = %provider.version,
                    provider.artifact.id = %artifact_id,
                    "provider updated"
                ),
                Err(error) => tracing::warn!(
                    provider.id = %provider_id,
                    provider.artifact.id = %artifact_id,
                    error.code = error.code.as_str(),
                    "provider update failed"
                ),
            }
            result
        })
    }

    fn change_provider_update_policy(
        &self,
        command: ChangeProviderUpdatePolicy,
    ) -> ProviderApiFuture<'_, ProviderSnapshot> {
        Box::pin(async move {
            let provider_id = command.provider_id.clone();
            let update_policy = command.update_policy;
            let result = self.change_policy(command).await;
            match &result {
                Ok(provider) => tracing::info!(
                    provider.id = %provider.provider_id,
                    ?update_policy,
                    "provider update policy changed"
                ),
                Err(error) => tracing::warn!(
                    provider.id = %provider_id,
                    ?update_policy,
                    error.code = error.code.as_str(),
                    "provider update policy change failed"
                ),
            }
            result
        })
    }

    fn remove_provider(&self, command: RemoveProvider) -> ProviderApiFuture<'_, ()> {
        Box::pin(async move {
            let provider_id = command.provider_id.clone();
            let result = self.registry.remove_provider(command).await;
            match &result {
                Ok(()) => tracing::info!(provider.id = %provider_id, "provider removed"),
                Err(error) => tracing::warn!(
                    provider.id = %provider_id,
                    error.code = error.code.as_str(),
                    "provider removal failed"
                ),
            }
            result
        })
    }
}

const SOURCE_COLUMNS: &str = "SELECT
    id, repository_url, available_providers, trusted_at, last_checked_at, last_error
FROM provider_sources";

const SOURCE_BY_ID: &str = "SELECT
    id, repository_url, available_providers, trusted_at, last_checked_at, last_error
FROM provider_sources
WHERE id = $1";

const PROVIDER_COLUMNS: &str = "SELECT
    provider_id, source_id, metadata->>'name' AS name,
    metadata->>'description' AS description, version, platform, index_digest,
    binary_digest, update_policy, available_version, available_discovered_at,
    last_update_error, installed_at, updated_at
FROM providers";

const PROVIDER_BY_ID: &str = "SELECT
    provider_id, source_id, metadata->>'name' AS name,
    metadata->>'description' AS description, version, platform, index_digest,
    binary_digest, update_policy, available_version, available_discovered_at,
    last_update_error, installed_at, updated_at
FROM providers
WHERE provider_id = $1";

#[derive(FromRow)]
struct SourceRow {
    id: Uuid,
    repository_url: String,
    available_providers: Json<Vec<AvailableProviderSnapshot>>,
    trusted_at: OffsetDateTime,
    last_checked_at: Option<OffsetDateTime>,
    last_error: Option<String>,
}

impl SourceRow {
    fn snapshot(self) -> Result<ProviderSourceSnapshot, ApplicationError> {
        Ok(ProviderSourceSnapshot {
            id: self.id,
            repository_url: self.repository_url,
            available_providers: self.available_providers.0,
            trusted_at: self.trusted_at,
            last_checked_at: self.last_checked_at,
            last_error: self.last_error,
        })
    }
}

#[derive(FromRow)]
struct ProviderRow {
    provider_id: String,
    source_id: Uuid,
    name: Option<String>,
    description: Option<String>,
    version: String,
    platform: String,
    index_digest: String,
    binary_digest: String,
    update_policy: String,
    available_version: Option<String>,
    available_discovered_at: Option<OffsetDateTime>,
    last_update_error: Option<String>,
    installed_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl ProviderRow {
    fn snapshot(self) -> Result<ProviderSnapshot, ApplicationError> {
        let available_update = match (self.available_version, self.available_discovered_at) {
            (Some(version), Some(discovered_at)) => Some(ProviderUpdateSnapshot {
                version,
                discovered_at,
            }),
            (None, None) => None,
            _ => return Err(internal_error("provider update state is inconsistent")),
        };
        Ok(ProviderSnapshot {
            provider_id: self.provider_id,
            source_id: self.source_id,
            name: self.name,
            description: self.description,
            version: self.version,
            platform: self.platform,
            index_digest: self.index_digest,
            binary_digest: self.binary_digest,
            update_policy: parse_update_policy(&self.update_policy)?,
            available_update,
            last_update_error: self.last_update_error,
            installed_at: self.installed_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(FromRow)]
struct LaunchRow {
    artifact_id: Uuid,
    provider_id: String,
    version: String,
    platform: String,
    binary_digest: String,
    metadata: Value,
}

#[derive(FromRow)]
struct ReconciliationRow {
    artifact_id: Uuid,
    provider_id: String,
    version: String,
    platform: String,
    binary_digest: String,
    metadata: Value,
}

#[derive(FromRow)]
struct UpdateTargetRow {
    artifact_id: Uuid,
    version: String,
    index_digest: String,
    source_id: Uuid,
    repository_url: String,
    available_providers: Json<Vec<AvailableProviderSnapshot>>,
    trusted_at: OffsetDateTime,
    last_checked_at: Option<OffsetDateTime>,
    last_error: Option<String>,
}

impl UpdateTargetRow {
    fn source_snapshot(&self) -> Result<ProviderSourceSnapshot, ApplicationError> {
        SourceRow {
            id: self.source_id,
            repository_url: self.repository_url.clone(),
            available_providers: self.available_providers.clone(),
            trusted_at: self.trusted_at,
            last_checked_at: self.last_checked_at,
            last_error: self.last_error.clone(),
        }
        .snapshot()
    }
}

#[derive(FromRow)]
struct LockedProviderRow {
    artifact_id: Uuid,
    version: String,
}

#[derive(FromRow)]
struct DiscoveryRow {
    provider_id: String,
    version: String,
    update_policy: String,
}

struct AutomaticUpdate {
    provider_id: String,
    source_id: Uuid,
    version: String,
}

fn latest_update(
    available_providers: &[AvailableProviderSnapshot],
    provider_id: &str,
    installed: &Version,
) -> Option<Version> {
    available_providers
        .iter()
        .find(|provider| provider.provider_id == provider_id)?
        .versions
        .iter()
        .filter_map(|value| Version::parse(value).ok())
        .filter(|candidate| candidate.build.is_empty() && candidate > installed)
        .filter(|candidate| !installed.pre.is_empty() || candidate.pre.is_empty())
        .max()
}

fn update_policy_value(policy: ProviderUpdatePolicy) -> &'static str {
    match policy {
        ProviderUpdatePolicy::Manual => "manual",
        ProviderUpdatePolicy::Automatic => "automatic",
    }
}

fn parse_update_policy(value: &str) -> Result<ProviderUpdatePolicy, ApplicationError> {
    match value {
        "manual" => Ok(ProviderUpdatePolicy::Manual),
        "automatic" => Ok(ProviderUpdatePolicy::Automatic),
        _ => Err(internal_error("provider update policy is invalid")),
    }
}

fn validate_source(command: &TrustProviderSource) -> Result<(), ApplicationError> {
    if !valid_repository_url(&command.repository_url) {
        return Err(ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "provider source is invalid",
            "repositoryUrl",
            "invalid_provider_repository_url",
            "provider repository URL must be canonical HTTP or HTTPS without credentials, a tag, or a digest",
        ));
    }
    Ok(())
}

fn validate_index_digest(value: &str) -> Result<(), ApplicationError> {
    if valid_provider_digest(value) {
        return Ok(());
    }
    Err(ApplicationError::field(
        ApplicationErrorCode::InvalidField,
        "provider request is invalid",
        "indexDigest",
        "invalid_provider_index_digest",
        "provider index digest must be a lowercase SHA-256 digest",
    ))
}

fn validate_update_selection(
    current_version: &Version,
    current_index_digest: &str,
    requested_version: &Version,
    requested_index_digest: &str,
) -> Result<(), ApplicationError> {
    if requested_version < current_version {
        return Err(ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "provider update request is invalid",
            "version",
            "provider_version_not_newer",
            "provider update version must not be older than the installed version",
        ));
    }
    if requested_version == current_version && requested_index_digest == current_index_digest {
        return Err(ApplicationError::field(
            ApplicationErrorCode::ProviderConflict,
            "provider release is already installed",
            "indexDigest",
            "provider_digest_already_installed",
            "select a release with a different index digest",
        ));
    }
    Ok(())
}

fn inspection_snapshot(
    inspection: InspectedProviderArtifact,
    platform: String,
) -> Result<ProviderReleaseInspectionSnapshot, ApplicationError> {
    let InspectedProviderArtifact {
        metadata,
        metadata_value: _,
        index_annotations,
        index_digest,
        manifest_digest,
        metadata_digest,
        binary_digest,
        binary_size,
        compatibility,
    } = inspection;
    let mut requirements = BTreeMap::new();
    if let Some(requirement) = metadata.requires.trakkin {
        requirements.insert("trakkin".to_owned(), requirement.to_string());
    }
    if let Some(requirement) = metadata.requires.provider_sdk {
        requirements.insert("trakkin-provider-sdk".to_owned(), requirement.to_string());
    }
    let published_at = index_annotations
        .get("org.opencontainers.image.created")
        .map(|value| OffsetDateTime::parse(value, &Rfc3339).map_err(invalid_provider_annotation))
        .transpose()?;
    let distributor = valid_annotation(&index_annotations, "org.opencontainers.image.vendor")?;
    let revision = valid_annotation(&index_annotations, "org.opencontainers.image.revision")?;
    let maintainer = metadata
        .maintainer
        .map(|maintainer| ProviderMaintainerSnapshot {
            name: maintainer.name,
            email: maintainer.email,
            url: maintainer.url,
        });

    let compatibility = match compatibility {
        ProviderArtifactCompatibility::Compatible => ProviderReleaseCompatibility::Compatible,
        ProviderArtifactCompatibility::UnsupportedPlatform => {
            ProviderReleaseCompatibility::UnsupportedPlatform
        }
        ProviderArtifactCompatibility::IncompatibleTrakkin => {
            ProviderReleaseCompatibility::IncompatibleTrakkin
        }
    };
    let platform_available = compatibility != ProviderReleaseCompatibility::UnsupportedPlatform;

    Ok(ProviderReleaseInspectionSnapshot {
        provider_id: metadata.id,
        version: metadata.version.to_string(),
        name: metadata.name,
        description: metadata.description,
        repository_url: metadata.repo,
        homepage_url: metadata.homepage,
        documentation_url: metadata.documentation,
        license: metadata.license,
        maintainer,
        distributor,
        published_at,
        revision,
        platforms: metadata.platforms,
        requirements,
        compatibility,
        platform,
        index_digest,
        manifest_digest: platform_available.then_some(manifest_digest),
        metadata_digest: platform_available.then_some(metadata_digest),
        binary_digest: platform_available.then_some(binary_digest),
        binary_size: platform_available.then_some(binary_size),
    })
}

fn valid_annotation(
    annotations: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<String>, ApplicationError> {
    let Some(value) = annotations.get(key) else {
        return Ok(None);
    };
    if value.is_empty() || value.len() > 2048 || value.contains(['\r', '\n']) {
        return Err(invalid_provider_annotation("annotation value is invalid"));
    }
    Ok(Some(value.clone()))
}

fn invalid_provider_annotation(error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::InvalidProviderData,
        format!("provider OCI annotations are invalid: {error}"),
    )
}

fn parse_provider_version(value: &str) -> Result<Version, ApplicationError> {
    let version = Version::parse(value).map_err(|error| {
        ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "provider request is invalid",
            "version",
            "invalid_provider_version",
            format!("provider version must be valid SemVer: {error}"),
        )
    })?;
    if !version.build.is_empty() {
        return Err(ApplicationError::field(
            ApplicationErrorCode::InvalidField,
            "provider request is invalid",
            "version",
            "invalid_provider_version",
            "provider version must not contain build metadata",
        ));
    }
    Ok(version)
}

fn ensure_supported_platform(platform: &str) -> Result<(), ApplicationError> {
    if matches!(platform, "linux/amd64" | "linux/arm64") {
        Ok(())
    } else {
        Err(ApplicationError::new(
            ApplicationErrorCode::InvalidRequest,
            "provider installation is unsupported on this host platform",
        ))
    }
}

fn host_platform() -> String {
    let architecture = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };
    format!("{}/{architecture}", std::env::consts::OS)
}

async fn artifact_matches(directory: &Path, digest: &str) -> Result<bool, std::io::Error> {
    let root_metadata = match tokio::fs::symlink_metadata(directory).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !root_metadata.file_type().is_dir() || root_metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let mut entries = tokio::fs::read_dir(directory).await?;
    let Some(entry) = entries.next_entry().await? else {
        return Ok(false);
    };
    if entry.file_name() != PROVIDER_EXECUTABLE || entries.next_entry().await?.is_some() {
        return Ok(false);
    }
    let metadata = tokio::fs::symlink_metadata(entry.path()).await?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o777 != 0o500 {
            return Ok(false);
        }
    }
    let expected = match digest.strip_prefix("sha256:") {
        Some(value)
            if value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()) =>
        {
            value
        }
        _ => return Ok(false),
    };
    let mut file = tokio::fs::File::open(entry.path()).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex::encode(hasher.finalize()) == expected)
}

async fn ensure_plain_directory(directory: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::symlink_metadata(directory).await {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            Ok(())
        }
        Ok(_) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} is not a plain directory", directory.display()),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(directory).await
        }
        Err(error) => Err(error),
    }
}

async fn reset_directory(directory: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::symlink_metadata(directory).await {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            tokio::fs::remove_dir_all(directory).await?;
        }
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{} is not a plain directory", directory.display()),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    tokio::fs::create_dir_all(directory).await
}

async fn remove_path_if_present(path: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            tokio::fs::remove_dir_all(path).await
        }
        Ok(_) => tokio::fs::remove_file(path).await,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
async fn set_executable_permissions(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o500)).await
}

#[cfg(not(unix))]
async fn set_executable_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

fn has_database_code(error: &sqlx::Error, code: &str) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|candidate| candidate == code)
}

fn random_uuid() -> Result<Uuid, getrandom::Error> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

fn provider_source_not_found() -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::ProviderNotFound,
        "provider source was not found",
    )
}

fn provider_not_found() -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::ProviderNotFound,
        "provider is not installed",
    )
}

fn provider_launch_application_error(error: ProviderLaunchError) -> ApplicationError {
    let code = match error {
        ProviderLaunchError::NotInstalled(_) => ApplicationErrorCode::ProviderNotFound,
        ProviderLaunchError::InvalidInstallation { .. } => {
            ApplicationErrorCode::InvalidProviderData
        }
        ProviderLaunchError::Registry(_) | ProviderLaunchError::Storage(_) => {
            ApplicationErrorCode::Internal
        }
    };
    ApplicationError::new(code, error.to_string())
}

fn provider_update_error_message(error: &ApplicationError) -> String {
    error.field_problems.first().map_or_else(
        || error.message.clone(),
        |problem| format!("{}: {}", error.message, problem.message),
    )
}

fn invalid_artifact(provider_id: &str, reason: impl Into<String>) -> ProviderLaunchError {
    ProviderLaunchError::InvalidInstallation {
        provider_id: provider_id.to_owned(),
        reason: reason.into(),
    }
}

fn internal_error(_error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::internal("provider registry failed")
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[test]
    fn source_locations_require_an_embedded_http_protocol() {
        let command = |repository_url: &str| TrustProviderSource {
            source_id: Uuid::nil(),
            repository_url: repository_url.to_owned(),
            trusted_at: OffsetDateTime::UNIX_EPOCH,
        };

        validate_source(&command("http://127.0.0.1:5000/trakkin-dev/providers"))
            .expect("HTTP source is valid");
        validate_source(&command("https://registry.example/trakkin/providers"))
            .expect("HTTPS source is valid");
        let error = validate_source(&command("registry.example/trakkin/providers"))
            .expect_err("source without a protocol is rejected");
        assert_eq!(error.code, ApplicationErrorCode::InvalidField);
        assert_eq!(error.field_problems[0].path, "repositoryUrl");
    }

    #[test]
    fn stable_installations_ignore_prerelease_updates() {
        let releases = [AvailableProviderSnapshot {
            provider_id: "dev.trakkin.fixture".to_owned(),
            versions: vec!["1.1.0-beta.1".to_owned(), "1.0.1".to_owned()],
        }];
        assert_eq!(
            latest_update(&releases, "dev.trakkin.fixture", &Version::new(1, 0, 0)),
            Some(Version::new(1, 0, 1))
        );
    }

    #[test]
    fn explicit_updates_allow_same_version_digest_replacements() {
        let version = Version::new(1, 2, 3);
        validate_update_selection(&version, "sha256:installed", &version, "sha256:replacement")
            .expect("different digest may replace the same version");

        let error =
            validate_update_selection(&version, "sha256:installed", &version, "sha256:installed")
                .expect_err("the installed digest is not replaced");
        assert_eq!(error.code, ApplicationErrorCode::ProviderConflict);
        assert_eq!(
            error.field_problems[0].code,
            "provider_digest_already_installed"
        );
    }

    #[test]
    fn explicit_updates_reject_downgrades() {
        let error = validate_update_selection(
            &Version::new(1, 2, 3),
            "sha256:installed",
            &Version::new(1, 2, 2),
            "sha256:older",
        )
        .expect_err("older version is rejected");
        assert_eq!(error.code, ApplicationErrorCode::InvalidField);
        assert_eq!(error.field_problems[0].code, "provider_version_not_newer");
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

    #[cfg(unix)]
    #[tokio::test]
    async fn artifact_verification_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("temporary directory");
        let artifact = directory.path().join("artifact");
        let outside = directory.path().join("outside");
        tokio::fs::create_dir(&artifact)
            .await
            .expect("artifact directory");
        tokio::fs::write(&outside, b"provider")
            .await
            .expect("outside provider");
        symlink(&outside, artifact.join(PROVIDER_EXECUTABLE)).expect("symlinked provider");
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(b"provider")));

        assert!(
            !artifact_matches(&artifact, &digest)
                .await
                .expect("artifact inspection")
        );
    }
}
