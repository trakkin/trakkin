use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex as SyncMutex, RwLock as SyncRwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use rusqlite::Connection;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::{Mutex, RwLock},
};
use trakkin_mappings_corpus::{
    CompileOptions, MappingCursor, MappingLayer, MappingQuery, compile, list_mappings,
    open_read_only,
};
use uuid::Uuid;

use crate::application::{
    error::{ApplicationError, ApplicationErrorCode},
    mappings::{
        CreateLocalMapping, CreateRemoteMappingSource, CreateUploadedMappingSource, MappingApi,
        MappingContentReader, MappingFuture, MappingPageSnapshot, MappingResult,
        MappingSourceContent, MappingSourceKind, MappingSourceSnapshot, MappingStateSnapshot,
        QueryMappings, RefreshRemoteMappingSource, RemoveLocalMapping, RemoveMappingSource,
        ReorderMappingSources, UpdateLocalMapping, UpdateMappingSource,
    },
};

use super::{
    local_mappings::{LocalMappingDocument, LocalMappingDocumentError},
    mapping_artifacts::{
        MappingArtifactError, MappingArtifactStore, MappingRuntimeLease, PublishedSourceArtifact,
    },
    mapping_remote::{RemoteDownload, RemoteSourceFetchError, RemoteSourceFetcher},
};

pub use super::mapping_remote::RemoteNetworkPolicy;

const LOCAL_SOURCE_KEY: &str = "local";
const LOCAL_SOURCE_NAME: &str = "Local mappings";
const MAXIMUM_LOCAL_SOURCE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_IMPORTED_SOURCE_BYTES: u64 = 1024 * 1024 * 1024;
const RUNTIME_CONNECTIONS: usize = 4;

#[derive(Clone)]
pub struct PostgresMappingManager {
    pool: PgPool,
    artifacts: MappingArtifactStore,
    compile_options: CompileOptions,
    remote_sources: RemoteSourceFetcher,
    writes: Arc<Mutex<()>>,
    runtime_lease: Arc<Mutex<Option<MappingRuntimeLease>>>,
    runtime_artifacts: Arc<RuntimeArtifactLifecycle>,
    source_artifacts: Arc<SourceArtifactLifecycle>,
    active_index: Arc<RwLock<Option<Arc<ActiveMappingIndex>>>>,
}

impl PostgresMappingManager {
    pub fn start(
        pool: PgPool,
        data_directory: impl Into<PathBuf>,
        recovered_at: OffsetDateTime,
    ) -> impl Future<Output = Result<Self, ApplicationError>> + Send {
        Self::start_with_remote_network_policy(
            pool,
            data_directory,
            RemoteNetworkPolicy::PublicHttps,
            recovered_at,
        )
    }

    pub fn start_with_remote_network_policy(
        pool: PgPool,
        data_directory: impl Into<PathBuf>,
        remote_network_policy: RemoteNetworkPolicy,
        recovered_at: OffsetDateTime,
    ) -> impl Future<Output = Result<Self, ApplicationError>> + Send {
        let data_directory = data_directory.into();
        Box::pin(async move {
            let manager = Self {
                pool,
                artifacts: MappingArtifactStore::new(data_directory),
                compile_options: CompileOptions::default(),
                remote_sources: RemoteSourceFetcher::new(remote_network_policy),
                writes: Arc::new(Mutex::new(())),
                runtime_lease: Arc::new(Mutex::new(None)),
                runtime_artifacts: Arc::new(RuntimeArtifactLifecycle::default()),
                source_artifacts: Arc::new(SourceArtifactLifecycle::default()),
                active_index: Arc::new(RwLock::new(None)),
            };
            manager.recover_inner(recovered_at).await?;
            Ok(manager)
        })
    }

    async fn recover_inner(
        &self,
        recovered_at: OffsetDateTime,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let _write = self.writes.lock().await;
        self.ensure_runtime_lease().await?;
        self.artifacts
            .reset_staging()
            .await
            .map_err(internal_error("mapping staging could not be reset"))?;

        let mut chain = self
            .load_chain()
            .await
            .map_err(internal_error("mapping state could not be loaded"))?;
        if chain.sources.is_empty() {
            chain = self.initialize(recovered_at).await?;
        }

        self.verify_source_artifacts(&chain).await?;
        match chain.active_generation.as_ref() {
            Some(active) => {
                if !active_manifest_matches(&chain, active) {
                    return Err(ApplicationError::internal(
                        "active mapping manifest does not match source state",
                    ));
                }
                let valid_runtime = self
                    .verified_runtime_index(&active.chain_fingerprint, &active.artifact_path, false)
                    .await;
                let runtime = match valid_runtime {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        tracing::warn!(%error, "active mapping index requires rebuilding");
                        let rebuilt = self
                            .compile_generation(
                                &chain,
                                random_uuid()?,
                                recovered_at,
                                Some(&active.chain_fingerprint),
                                true,
                            )
                            .await
                            .map_err(internal_error("active mapping index could not be rebuilt"))?;
                        rebuilt.runtime
                    }
                };
                self.set_active_index(runtime).await;
            }
            None => {
                let generation = self
                    .compile_generation(&chain, random_uuid()?, recovered_at, None, false)
                    .await
                    .map_err(internal_error("initial mapping index could not be built"))?;
                self.commit_generation_only(&generation).await?;
                chain = self
                    .load_chain()
                    .await
                    .map_err(internal_error("mapping state could not be reloaded"))?;
            }
        }
        self.compact_recovered_storage(&chain).await?;
        Ok(chain.snapshot())
    }

    async fn compact_recovered_storage(&self, chain: &StoredChain) -> Result<(), ApplicationError> {
        let active = chain
            .active_generation
            .as_ref()
            .ok_or_else(|| ApplicationError::internal("active mapping generation is missing"))?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(internal_error("mapping storage cleanup could not start"))?;
        retire_inactive_manifests(&mut transaction, active.id)
            .await
            .map_err(mapping_write_error)?;
        transaction
            .commit()
            .await
            .map_err(internal_error("mapping storage cleanup could not commit"))?;

        let mut retained_paths = chain
            .sources
            .iter()
            .map(|source| source.artifact_path.clone())
            .collect::<BTreeSet<_>>();
        retained_paths.insert(active.artifact_path.clone());
        self.artifacts
            .retain_only(&retained_paths)
            .await
            .map_err(internal_error("mapping artifact cleanup failed"))
    }

    async fn initialize(
        &self,
        created_at: OffsetDateTime,
    ) -> Result<StoredChain, ApplicationError> {
        let source_id = random_uuid()?;
        let version_id = random_uuid()?;
        let generation_id = random_uuid()?;
        let mut empty = tokio::io::empty();
        let artifact = self
            .artifacts
            .ingest_source(
                source_id,
                version_id,
                MAXIMUM_LOCAL_SOURCE_BYTES,
                &mut empty,
            )
            .await
            .map_err(internal_error(
                "local mapping source could not be initialized",
            ))?;
        let source = stored_source(
            NewStoredSource {
                id: source_id,
                version_id,
                source_key: LOCAL_SOURCE_KEY.to_owned(),
                display_name: LOCAL_SOURCE_NAME.to_owned(),
                kind: MappingSourceKind::Local,
                remote_url: None,
                http_etag: None,
                http_last_modified: None,
                priority: 0,
                enabled: true,
                created_at,
            },
            artifact,
        )?;
        let candidate = StoredChain {
            active_generation: None,
            sources: vec![source],
        };
        let source_artifact_path = candidate.sources[0].artifact_path.clone();
        let activation = async {
            let generation = self
                .compile_generation(&candidate, generation_id, created_at, None, false)
                .await
                .map_err(internal_error("initial mapping index could not be built"))?;
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(internal_error("mapping initialization could not start"))?;
            lock_chain(&mut transaction)
                .await
                .map_err(internal_error("mapping chain could not be locked"))?;
            let source_count = sqlx::query_scalar::<_, i64>(
                "SELECT count(*)
FROM mapping_sources
",
            )
            .fetch_one(&mut *transaction)
            .await
            .map_err(internal_error("mapping sources could not be inspected"))?;
            if source_count != 0 {
                return Err(ApplicationError::internal(
                    "mapping sources were initialized concurrently",
                ));
            }
            insert_source(&mut transaction, &candidate.sources[0])
                .await
                .map_err(mapping_write_error)?;
            insert_generation(&mut transaction, &generation)
                .await
                .map_err(mapping_write_error)?;
            let retired = activate_generation(&mut transaction, generation.id, created_at)
                .await
                .map_err(mapping_write_error)?;
            self.commit_activation(
                transaction,
                &generation,
                retired,
                "mapping initialization could not commit",
            )
            .await
        }
        .await;
        if let Err(error) = activation {
            self.discard_source_artifact(&source_artifact_path).await;
            return Err(error);
        }
        self.load_chain().await.map_err(internal_error(
            "initialized mapping state could not be loaded",
        ))
    }

    async fn state_inner(&self) -> Result<MappingStateSnapshot, ApplicationError> {
        let active_index = self.active_index.read().await;
        self.load_chain()
            .await
            .map(|chain| chain.snapshot())
            .map_err(internal_error("mapping state could not be loaded"))
            .inspect(|_| drop(active_index))
    }

    async fn ensure_runtime_lease(&self) -> Result<(), ApplicationError> {
        let mut lease = self.runtime_lease.lock().await;
        if lease.is_none() {
            *lease = Some(
                self.artifacts
                    .acquire_runtime_lease()
                    .await
                    .map_err(internal_error("mapping artifact store is already active"))?,
            );
        }
        Ok(())
    }

    async fn create_uploaded_source_inner(
        &self,
        command: CreateUploadedMappingSource,
        mut content: MappingContentReader,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let _write = self.writes.lock().await;
        let source_key = required_text(&command.source_key, "sourceKey")?;
        let display_name = required_text(&command.display_name, "displayName")?;
        let chain = self.load_current_chain().await?;
        ensure_source_absent(&chain, command.source_id, &source_key)?;

        let version_id = random_uuid()?;
        let artifact = self
            .artifacts
            .ingest_source(
                command.source_id,
                version_id,
                MAXIMUM_IMPORTED_SOURCE_BYTES,
                &mut content,
            )
            .await
            .map_err(source_artifact_error)?;
        let source = stored_source(
            NewStoredSource {
                id: command.source_id,
                version_id,
                source_key,
                display_name,
                kind: MappingSourceKind::Upload,
                remote_url: None,
                http_etag: None,
                http_last_modified: None,
                priority: next_source_priority(&chain)?,
                enabled: command.enabled,
                created_at: command.created_at,
            },
            artifact,
        )?;
        self.activate_new_source(chain, source).await
    }

    async fn create_remote_source_inner(
        &self,
        command: CreateRemoteMappingSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let _write = self.writes.lock().await;
        let source_key = required_text(&command.source_key, "sourceKey")?;
        let display_name = required_text(&command.display_name, "displayName")?;
        let remote_url = self
            .remote_sources
            .canonical_url(&command.remote_url)
            .map_err(remote_source_fetch_error)?;
        let chain = self.load_current_chain().await?;
        ensure_source_absent(&chain, command.source_id, &source_key)?;
        ensure_remote_url_absent(&chain, &remote_url)?;

        let mut download = match self.download_remote_source(&remote_url, None, None).await? {
            RemoteDownload::Modified(download) => download,
            RemoteDownload::NotModified(_) => {
                return Err(remote_source_unavailable(
                    "remote mapping source returned not modified without validators",
                ));
            }
        };
        let version_id = random_uuid()?;
        let artifact = self
            .artifacts
            .ingest_source(
                command.source_id,
                version_id,
                MAXIMUM_IMPORTED_SOURCE_BYTES,
                &mut download.reader,
            )
            .await
            .map_err(remote_source_artifact_error)?;
        let source = stored_source(
            NewStoredSource {
                id: command.source_id,
                version_id,
                source_key,
                display_name,
                kind: MappingSourceKind::Remote,
                remote_url: Some(remote_url),
                http_etag: download.etag,
                http_last_modified: download.last_modified,
                priority: next_source_priority(&chain)?,
                enabled: command.enabled,
                created_at: command.created_at,
            },
            artifact,
        )?;
        self.activate_new_source(chain, source).await
    }

    async fn activate_new_source(
        &self,
        mut chain: StoredChain,
        source: StoredSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        chain.sources.push(source.clone());
        chain.sort_sources();
        let generation = match self
            .prepare_generation(&chain, random_uuid()?, source.created_at)
            .await
        {
            Ok(generation) => generation,
            Err(error) => {
                self.discard_source_artifact(&source.artifact_path).await;
                return Err(mapping_compilation_error(error));
            }
        };

        let activation = async {
            let mut transaction = self.begin_write().await?;
            insert_source(&mut transaction, &source)
                .await
                .map_err(mapping_write_error)?;
            let retired = stage_generation(&mut transaction, &generation, source.created_at)
                .await
                .map_err(mapping_write_error)?;
            self.commit_prepared_generation(
                transaction,
                &generation,
                retired,
                "mapping source creation could not commit",
            )
            .await
        }
        .await;
        if let Err(error) = activation {
            self.discard_source_artifact(&source.artifact_path).await;
            return Err(error);
        }
        self.state_inner().await
    }

    async fn update_source_inner(
        &self,
        command: UpdateMappingSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let _write = self.writes.lock().await;
        let display_name = required_text(&command.display_name, "displayName")?;
        let mut chain = self.load_current_chain().await?;
        let source = chain
            .sources
            .iter_mut()
            .find(|source| source.id == command.source_id)
            .ok_or_else(source_not_found)?;
        if source.kind == MappingSourceKind::Local && !command.enabled {
            return Err(invalid_source(
                "the local mapping source cannot be disabled",
            ));
        }
        if source.display_name == display_name && source.enabled == command.enabled {
            return Ok(chain.snapshot());
        }
        let enabled_changed = source.enabled != command.enabled;
        source.display_name = display_name.clone();
        source.enabled = command.enabled;
        source.updated_at = command.updated_at;
        if !enabled_changed {
            return self
                .commit_source_metadata(command.source_id, &display_name, command.updated_at)
                .await;
        }
        let generation = self
            .prepare_generation(&chain, random_uuid()?, command.updated_at)
            .await
            .map_err(mapping_compilation_error)?;

        let mut transaction = self.begin_write().await?;
        let result = sqlx::query(
            "UPDATE mapping_sources
SET display_name = $1, enabled = $2, updated_at = $3
WHERE id = $4
",
        )
        .bind(&display_name)
        .bind(command.enabled)
        .bind(command.updated_at)
        .bind(command.source_id)
        .execute(&mut *transaction)
        .await
        .map_err(mapping_write_error)?;
        if result.rows_affected() != 1 {
            return Err(source_not_found());
        }
        let retired = stage_generation(&mut transaction, &generation, command.updated_at)
            .await
            .map_err(mapping_write_error)?;
        self.commit_prepared_generation(
            transaction,
            &generation,
            retired,
            "mapping source update could not commit",
        )
        .await?;
        self.state_inner().await
    }

    async fn commit_source_metadata(
        &self,
        source_id: Uuid,
        display_name: &str,
        updated_at: OffsetDateTime,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let mut transaction = self.begin_write().await?;
        let result = sqlx::query(
            "UPDATE mapping_sources
SET display_name = $1, updated_at = $2
WHERE id = $3
",
        )
        .bind(display_name)
        .bind(updated_at)
        .bind(source_id)
        .execute(&mut *transaction)
        .await
        .map_err(mapping_write_error)?;
        if result.rows_affected() != 1 {
            return Err(source_not_found());
        }
        transaction
            .commit()
            .await
            .map_err(internal_error("mapping source metadata could not commit"))?;
        self.state_inner().await
    }

    async fn refresh_remote_source_inner(
        &self,
        command: RefreshRemoteMappingSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let _write = self.writes.lock().await;
        let mut chain = self.load_current_chain().await?;
        let source_index = chain
            .sources
            .iter()
            .position(|source| source.id == command.source_id)
            .ok_or_else(source_not_found)?;
        let current = chain.sources[source_index].clone();
        if current.kind != MappingSourceKind::Remote {
            return Err(invalid_source(
                "only remote mapping sources can be refreshed",
            ));
        }
        let remote_url = current
            .remote_url
            .as_deref()
            .ok_or_else(|| ApplicationError::internal("remote mapping source URL is missing"))?;
        let download = match self
            .download_remote_source(
                remote_url,
                current.http_etag.as_deref(),
                current.http_last_modified.as_deref(),
            )
            .await
        {
            Ok(download) => download,
            Err(error) => {
                self.record_remote_check(
                    current.id,
                    command.refreshed_at,
                    current.http_etag.as_deref(),
                    current.http_last_modified.as_deref(),
                    Some(&error.to_string()),
                )
                .await?;
                return Err(error);
            }
        };
        let mut download = match download {
            RemoteDownload::NotModified(validators) => {
                let etag = validators.etag.as_deref().or(current.http_etag.as_deref());
                let last_modified = validators
                    .last_modified
                    .as_deref()
                    .or(current.http_last_modified.as_deref());
                self.record_remote_check(
                    current.id,
                    command.refreshed_at,
                    etag,
                    last_modified,
                    None,
                )
                .await?;
                chain.sources[source_index].http_etag = etag.map(str::to_owned);
                chain.sources[source_index].http_last_modified = last_modified.map(str::to_owned);
                chain.sources[source_index].last_checked_at = Some(command.refreshed_at);
                chain.sources[source_index].last_refresh_error = None;
                return Ok(chain.snapshot());
            }
            RemoteDownload::Modified(download) => download,
        };

        let version_id = random_uuid()?;
        let artifact = match self
            .artifacts
            .ingest_source(
                current.id,
                version_id,
                MAXIMUM_IMPORTED_SOURCE_BYTES,
                &mut download.reader,
            )
            .await
        {
            Ok(artifact) => artifact,
            Err(error) => {
                let error = remote_source_artifact_error(error);
                self.record_remote_check(
                    current.id,
                    command.refreshed_at,
                    current.http_etag.as_deref(),
                    current.http_last_modified.as_deref(),
                    Some(&error.to_string()),
                )
                .await?;
                return Err(error);
            }
        };
        if artifact.content_hash == current.content_hash {
            self.record_remote_check(
                current.id,
                command.refreshed_at,
                download.etag.as_deref(),
                download.last_modified.as_deref(),
                None,
            )
            .await?;
            chain.sources[source_index].http_etag = download.etag;
            chain.sources[source_index].http_last_modified = download.last_modified;
            chain.sources[source_index].last_checked_at = Some(command.refreshed_at);
            chain.sources[source_index].last_refresh_error = None;
            return Ok(chain.snapshot());
        }

        let source = &mut chain.sources[source_index];
        source.version_id = version_id;
        source.content_hash = artifact.content_hash;
        source.byte_length = i64::try_from(artifact.byte_length)
            .map_err(|_| invalid_source("mapping source is too large"))?;
        source.artifact_path = artifact.relative_path;
        source.http_etag = download.etag;
        source.http_last_modified = download.last_modified;
        source.last_checked_at = Some(command.refreshed_at);
        source.last_refresh_error = None;
        source.updated_at = command.refreshed_at;
        let source = source.clone();
        let generation = match self
            .prepare_generation(&chain, random_uuid()?, command.refreshed_at)
            .await
        {
            Ok(generation) => generation,
            Err(error) => {
                self.discard_source_artifact(&source.artifact_path).await;
                let message = format!("remote mapping source is invalid: {error}");
                self.record_remote_check(
                    current.id,
                    command.refreshed_at,
                    current.http_etag.as_deref(),
                    current.http_last_modified.as_deref(),
                    Some(&message),
                )
                .await?;
                return Err(invalid_source(message));
            }
        };

        let activation = async {
            let mut transaction = self.begin_write().await?;
            insert_version(&mut transaction, &source)
                .await
                .map_err(mapping_write_error)?;
            let result = sqlx::query(
                "UPDATE mapping_sources
SET
    current_version_id = $1, http_etag = $2, http_last_modified = $3,
    last_checked_at = $4, last_refresh_error = NULL, updated_at = $4
WHERE id = $5 AND source_kind = 'remote'
",
            )
            .bind(source.version_id)
            .bind(&source.http_etag)
            .bind(&source.http_last_modified)
            .bind(command.refreshed_at)
            .bind(source.id)
            .execute(&mut *transaction)
            .await
            .map_err(mapping_write_error)?;
            if result.rows_affected() != 1 {
                return Err(source_not_found());
            }
            let retired = stage_generation(&mut transaction, &generation, command.refreshed_at)
                .await
                .map_err(mapping_write_error)?;
            self.commit_prepared_generation(
                transaction,
                &generation,
                retired,
                "remote mapping source refresh could not commit",
            )
            .await
        }
        .await;
        if let Err(error) = activation {
            self.discard_source_artifact(&source.artifact_path).await;
            return Err(error);
        }
        self.state_inner().await
    }

    async fn record_remote_check(
        &self,
        source_id: Uuid,
        checked_at: OffsetDateTime,
        http_etag: Option<&str>,
        http_last_modified: Option<&str>,
        refresh_error: Option<&str>,
    ) -> Result<(), ApplicationError> {
        let mut transaction = self.begin_write().await?;
        let result = sqlx::query(
            "UPDATE mapping_sources
SET
    http_etag = $1, http_last_modified = $2,
    last_checked_at = $3, last_refresh_error = $4
WHERE id = $5 AND source_kind = 'remote'
",
        )
        .bind(http_etag)
        .bind(http_last_modified)
        .bind(checked_at)
        .bind(refresh_error)
        .bind(source_id)
        .execute(&mut *transaction)
        .await
        .map_err(mapping_write_error)?;
        if result.rows_affected() != 1 {
            return Err(source_not_found());
        }
        transaction.commit().await.map_err(internal_error(
            "mapping source refresh status could not commit",
        ))
    }

    async fn download_remote_source(
        &self,
        remote_url: &str,
        http_etag: Option<&str>,
        http_last_modified: Option<&str>,
    ) -> Result<RemoteDownload, ApplicationError> {
        self.remote_sources
            .download(remote_url, http_etag, http_last_modified)
            .await
            .map_err(remote_source_fetch_error)
    }

    async fn create_local_mapping_inner(
        &self,
        command: CreateLocalMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.mutate_local_mapping(LocalMappingMutation::Create {
            record: command.record,
            changed_at: command.created_at,
        })
        .await
    }

    async fn update_local_mapping_inner(
        &self,
        command: UpdateLocalMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.mutate_local_mapping(LocalMappingMutation::Update {
            statement_id: command.statement_id,
            record: command.record,
            changed_at: command.updated_at,
        })
        .await
    }

    async fn remove_local_mapping_inner(
        &self,
        command: RemoveLocalMapping,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        self.mutate_local_mapping(LocalMappingMutation::Remove {
            statement_id: command.statement_id,
            changed_at: command.removed_at,
        })
        .await
    }

    async fn mutate_local_mapping(
        &self,
        mutation: LocalMappingMutation,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let _write = self.writes.lock().await;
        let mut chain = self.load_current_chain().await?;
        let source_index = chain
            .sources
            .iter()
            .position(|source| source.kind == MappingSourceKind::Local)
            .ok_or_else(|| ApplicationError::internal("local mapping source is missing"))?;
        let current = chain.sources[source_index].clone();
        let path = self
            .artifacts
            .verified_source_path(
                current.id,
                &current.content_hash,
                u64::try_from(current.byte_length)
                    .map_err(|_| ApplicationError::internal("stored mapping size is invalid"))?,
                &current.artifact_path,
            )
            .await
            .map_err(internal_error("local mapping source is unavailable"))?;
        let changed_at = mutation.changed_at();
        let maximum_record_bytes = self.compile_options.max_record_bytes;
        let content = tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(path)?;
            let mut document =
                LocalMappingDocument::read(std::io::BufReader::new(file), maximum_record_bytes)?;
            mutation.apply(&mut document)?;
            Ok::<_, LocalMappingMutationError>(document.into_bytes())
        })
        .await
        .map_err(internal_error("local mapping mutation task failed"))?
        .map_err(local_mapping_mutation_error)?;
        let version_id = random_uuid()?;
        let mut content = std::io::Cursor::new(content);
        let artifact = self
            .artifacts
            .ingest_source(
                current.id,
                version_id,
                MAXIMUM_LOCAL_SOURCE_BYTES,
                &mut content,
            )
            .await
            .map_err(source_artifact_error)?;
        if current.content_hash == artifact.content_hash {
            return Ok(chain.snapshot());
        }
        let source = &mut chain.sources[source_index];
        source.version_id = version_id;
        source.content_hash = artifact.content_hash;
        source.byte_length = i64::try_from(artifact.byte_length)
            .map_err(|_| invalid_source("mapping source is too large"))?;
        source.artifact_path = artifact.relative_path;
        source.updated_at = changed_at;
        let source = chain.sources[source_index].clone();
        let generation = match self
            .prepare_generation(&chain, random_uuid()?, changed_at)
            .await
        {
            Ok(generation) => generation,
            Err(error) => {
                self.discard_source_artifact(&source.artifact_path).await;
                return Err(mapping_compilation_error(error));
            }
        };

        let activation = async {
            let mut transaction = self.begin_write().await?;
            insert_version(&mut transaction, &source)
                .await
                .map_err(mapping_write_error)?;
            let result = sqlx::query(
                "UPDATE mapping_sources
SET current_version_id = $1, updated_at = $2
WHERE id = $3 AND source_kind = 'local'
",
            )
            .bind(source.version_id)
            .bind(changed_at)
            .bind(source.id)
            .execute(&mut *transaction)
            .await
            .map_err(mapping_write_error)?;
            if result.rows_affected() != 1 {
                return Err(source_not_found());
            }
            let retired = stage_generation(&mut transaction, &generation, changed_at)
                .await
                .map_err(mapping_write_error)?;
            self.commit_prepared_generation(
                transaction,
                &generation,
                retired,
                "local mapping mutation could not commit",
            )
            .await
        }
        .await;
        if let Err(error) = activation {
            self.discard_source_artifact(&source.artifact_path).await;
            return Err(error);
        }
        self.state_inner().await
    }

    async fn reorder_sources_inner(
        &self,
        command: ReorderMappingSources,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let _write = self.writes.lock().await;
        let mut chain = self.load_current_chain().await?;
        let requested = command.source_ids.iter().copied().collect::<BTreeSet<_>>();
        let imported = chain
            .sources
            .iter()
            .filter(|source| source.kind != MappingSourceKind::Local)
            .map(|source| source.id)
            .collect::<BTreeSet<_>>();
        if requested.len() != command.source_ids.len() || requested != imported {
            return Err(invalid_source(
                "mapping source order must contain every imported source exactly once",
            ));
        }
        let current = chain
            .sources
            .iter()
            .filter(|source| source.kind != MappingSourceKind::Local)
            .map(|source| source.id)
            .collect::<Vec<_>>();
        if current == command.source_ids {
            return Ok(chain.snapshot());
        }
        for (index, source_id) in command.source_ids.iter().enumerate() {
            let source = chain
                .sources
                .iter_mut()
                .find(|source| source.id == *source_id)
                .ok_or_else(source_not_found)?;
            source.priority = i64::try_from(index)
                .map_err(|_| invalid_source("mapping source order is too large"))?
                + 1;
            source.updated_at = command.updated_at;
        }
        chain.sort_sources();
        let generation = self
            .prepare_generation(&chain, random_uuid()?, command.updated_at)
            .await
            .map_err(mapping_compilation_error)?;

        let mut transaction = self.begin_write().await?;
        #[allow(unknown_lints, cargo_sqruff)]
        sqlx::query("SET CONSTRAINTS mapping_sources_priority_key DEFERRED\n")
            .execute(&mut *transaction)
            .await
            .map_err(mapping_write_error)?;
        for source in chain
            .sources
            .iter()
            .filter(|source| source.kind != MappingSourceKind::Local)
        {
            sqlx::query(
                "UPDATE mapping_sources
SET priority = $1, updated_at = $2
WHERE id = $3 AND source_kind <> 'local'
",
            )
            .bind(source.priority)
            .bind(command.updated_at)
            .bind(source.id)
            .execute(&mut *transaction)
            .await
            .map_err(mapping_write_error)?;
        }
        let retired = stage_generation(&mut transaction, &generation, command.updated_at)
            .await
            .map_err(mapping_write_error)?;
        self.commit_prepared_generation(
            transaction,
            &generation,
            retired,
            "mapping source reorder could not commit",
        )
        .await?;
        self.state_inner().await
    }

    async fn remove_source_inner(
        &self,
        command: RemoveMappingSource,
    ) -> Result<MappingStateSnapshot, ApplicationError> {
        let _write = self.writes.lock().await;
        let mut chain = self.load_current_chain().await?;
        let source = chain
            .sources
            .iter()
            .find(|source| source.id == command.source_id)
            .ok_or_else(source_not_found)?;
        if source.kind == MappingSourceKind::Local {
            return Err(invalid_source("the local mapping source cannot be removed"));
        }
        chain
            .sources
            .retain(|source| source.id != command.source_id);
        let generation = self
            .prepare_generation(&chain, random_uuid()?, command.removed_at)
            .await
            .map_err(mapping_compilation_error)?;

        let mut transaction = self.begin_write().await?;
        let mut retired = stage_generation(&mut transaction, &generation, command.removed_at)
            .await
            .map_err(mapping_write_error)?;
        let removed_source_artifacts = sqlx::query_scalar::<_, String>(
            "SELECT artifact_path
FROM mapping_source_versions
WHERE source_id = $1
",
        )
        .bind(command.source_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(mapping_write_error)?;
        let result = sqlx::query(
            "DELETE FROM mapping_sources
WHERE id = $1 AND source_kind <> 'local'
",
        )
        .bind(command.source_id)
        .execute(&mut *transaction)
        .await
        .map_err(mapping_write_error)?;
        if result.rows_affected() != 1 {
            return Err(source_not_found());
        }
        retired.source_paths.extend(removed_source_artifacts);
        self.commit_prepared_generation(
            transaction,
            &generation,
            retired,
            "mapping source removal could not commit",
        )
        .await?;
        self.state_inner().await
    }

    async fn read_source_content_inner(
        &self,
        source_id: Uuid,
    ) -> Result<MappingSourceContent, ApplicationError> {
        let _write = self.writes.lock().await;
        let chain = self
            .load_chain()
            .await
            .map_err(internal_error("mapping state could not be loaded"))?;
        let source = chain
            .sources
            .iter()
            .find(|source| source.id == source_id)
            .ok_or_else(source_not_found)?;
        let path = self
            .artifacts
            .verified_source_path(
                source.id,
                &source.content_hash,
                u64::try_from(source.byte_length)
                    .map_err(|_| ApplicationError::internal("stored mapping size is invalid"))?,
                &source.artifact_path,
            )
            .await
            .map_err(internal_error("mapping source content is unavailable"))?;
        let lease = self.source_artifacts.acquire(path.clone());
        let reader = tokio::fs::File::open(path)
            .await
            .map_err(internal_error("mapping source content could not be opened"))?;
        Ok(MappingSourceContent {
            content_hash: source.content_hash.clone(),
            byte_length: u64::try_from(source.byte_length)
                .map_err(|_| ApplicationError::internal("stored mapping size is invalid"))?,
            reader: Box::pin(LeasedSourceReader {
                reader,
                _lease: lease,
            }),
        })
    }

    async fn query_inner(
        &self,
        query: QueryMappings,
    ) -> Result<MappingPageSnapshot, ApplicationError> {
        let active = self
            .active_index
            .read()
            .await
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| ApplicationError::internal("mapping index is not initialized"))?;
        if query
            .expected_chain_fingerprint
            .as_deref()
            .is_some_and(|expected| expected != active.chain_fingerprint)
        {
            return Err(mapping_chain_changed());
        }
        let cursor = decode_cursor(query.cursor.as_deref())?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.chain_fingerprint != active.chain_fingerprint)
        {
            return Err(ApplicationError::new(
                ApplicationErrorCode::MappingChainChanged,
                "mapping query cursor is stale",
            ));
        }
        let runtime_query = MappingQuery {
            search: query.search,
            canonical_statement: query.canonical_statement,
            statement_id: query.statement_id,
            relation_key: query.relation_key,
            operator: None,
            source_key: query.source_key,
            endpoint: query.endpoint,
            annotation_name: query.annotation_name,
            annotation_value: query.annotation_value,
            include_shadowed: query.include_shadowed,
            limit: query.limit,
        };
        let page = tokio::task::spawn_blocking(move || {
            let position =
                active.next_connection.fetch_add(1, Ordering::Relaxed) % active.connections.len();
            let connection = active.connections[position]
                .lock()
                .map_err(|_| "mapping runtime connection is unavailable".to_owned())?;
            list_mappings(&connection, &runtime_query, cursor.as_ref())
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(internal_error("mapping query task failed"))?
        .map_err(|message| {
            ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                format!("mapping query is invalid: {message}"),
            )
        })?;
        Ok(MappingPageSnapshot {
            chain_fingerprint: page.chain_fingerprint,
            items: page
                .items
                .into_iter()
                .map(|row| MappingResult {
                    relation_key: row.relation_key,
                    statement_id: row.statement_id,
                    canonical_statement: row.canonical_statement,
                    canonical_record: row.canonical_record,
                    operator: row.operator.text().to_owned(),
                    source_key: row.source_key,
                    source_content_hash: row.source_content_hash,
                    layer_position: row.layer_position,
                    record_ordinal: row.record_ordinal,
                    active: row.active,
                    occurrence_count: row.occurrence_count,
                    evidence_fingerprint: row.evidence_fingerprint,
                    metadata: row.metadata,
                })
                .collect(),
            next_cursor: page.next_cursor.map(encode_cursor).transpose()?,
        })
    }

    async fn load_current_chain(&self) -> Result<StoredChain, ApplicationError> {
        self.load_chain()
            .await
            .map_err(internal_error("mapping state could not be loaded"))
    }

    async fn begin_write(&self) -> Result<Transaction<'_, Postgres>, ApplicationError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(internal_error("mapping transaction could not start"))?;
        lock_chain(&mut transaction)
            .await
            .map_err(internal_error("mapping chain could not be locked"))?;
        Ok(transaction)
    }

    async fn commit_activation(
        &self,
        transaction: Transaction<'_, Postgres>,
        generation: &BuiltGeneration,
        retired: RetiredArtifacts,
        failure_message: &'static str,
    ) -> Result<(), ApplicationError> {
        let mut active_index = self.active_index.write().await;
        for path in &generation.source_paths {
            self.source_artifacts.retain(path);
        }
        transaction
            .commit()
            .await
            .map_err(internal_error(failure_message))?;
        generation
            .runtime
            .retire_on_drop
            .store(false, Ordering::Release);
        generation
            .runtime
            .artifacts
            .set_active(generation.runtime.path.clone());
        let previous = active_index.replace(Arc::clone(&generation.runtime));
        if let Some(previous) = previous.as_ref()
            && previous.path != generation.runtime.path
        {
            previous.retire_on_drop.store(true, Ordering::Release);
        }
        drop(active_index);
        self.remove_retired_artifacts(
            retired,
            &generation.runtime,
            previous.as_ref().map(|runtime| &runtime.path),
        )
        .await;
        Ok(())
    }

    async fn remove_retired_artifacts(
        &self,
        retired: RetiredArtifacts,
        active: &ActiveMappingIndex,
        previous_path: Option<&PathBuf>,
    ) {
        for path in retired.source_paths {
            let published_path = match self.artifacts.published_path(&path) {
                Ok(path) => path,
                Err(error) => {
                    tracing::warn!(%error, artifact.path = %path, "retired mapping source artifact path is invalid");
                    continue;
                }
            };
            if !self.source_artifacts.retire(published_path) {
                continue;
            }
            if let Err(error) = self.artifacts.remove_published(&path).await {
                tracing::warn!(%error, artifact.path = %path, "retired mapping source artifact could not be removed");
            }
        }
        for path in retired.generation_paths {
            if active.path.ends_with(&path)
                || previous_path.is_some_and(|previous| previous.ends_with(&path))
            {
                continue;
            }
            if let Err(error) = self.artifacts.remove_published(&path).await {
                tracing::warn!(%error, artifact.path = %path, "retired mapping generation artifact could not be removed");
            }
        }
    }

    async fn discard_source_artifact(&self, path: &str) {
        if let Err(error) = self.artifacts.remove_published(path).await {
            tracing::warn!(%error, artifact.path = %path, "rejected mapping source artifact could not be removed");
        }
    }

    async fn commit_generation_only(
        &self,
        generation: &BuiltGeneration,
    ) -> Result<(), ApplicationError> {
        let mut transaction = self.begin_write().await?;
        insert_generation(&mut transaction, generation)
            .await
            .map_err(mapping_write_error)?;
        let retired = activate_generation(&mut transaction, generation.id, generation.created_at)
            .await
            .map_err(mapping_write_error)?;
        self.commit_activation(
            transaction,
            generation,
            retired,
            "mapping generation could not commit",
        )
        .await
    }

    async fn prepare_generation(
        &self,
        chain: &StoredChain,
        generation_id: Uuid,
        created_at: OffsetDateTime,
    ) -> Result<PreparedGeneration, MappingBuildError> {
        if let Some(active) = chain.active_generation.as_ref()
            && active_manifest_matches(chain, active)
        {
            return Ok(PreparedGeneration::Active { id: active.id });
        }
        self.compile_generation(chain, generation_id, created_at, None, false)
            .await
            .map(PreparedGeneration::Built)
    }

    async fn commit_prepared_generation(
        &self,
        transaction: Transaction<'_, Postgres>,
        generation: &PreparedGeneration,
        retired: RetiredArtifacts,
        failure_message: &'static str,
    ) -> Result<(), ApplicationError> {
        match generation {
            PreparedGeneration::Built(generation) => {
                self.commit_activation(transaction, generation, retired, failure_message)
                    .await
            }
            PreparedGeneration::Active { .. } => {
                let active = self
                    .active_index
                    .read()
                    .await
                    .as_ref()
                    .map(Arc::clone)
                    .ok_or_else(|| {
                        ApplicationError::internal("mapping index is not initialized")
                    })?;
                transaction
                    .commit()
                    .await
                    .map_err(internal_error(failure_message))?;
                self.remove_retired_artifacts(retired, &active, None).await;
                Ok(())
            }
        }
    }

    async fn compile_generation(
        &self,
        chain: &StoredChain,
        generation_id: Uuid,
        created_at: OffsetDateTime,
        expected_fingerprint: Option<&str>,
        replace_existing: bool,
    ) -> Result<BuiltGeneration, MappingBuildError> {
        let mut layers = Vec::new();
        let mut sources = Vec::new();
        let source_paths = chain
            .sources
            .iter()
            .map(|source| self.artifacts.published_path(&source.artifact_path))
            .collect::<Result<Vec<_>, _>>()?;
        for source in chain.sources.iter().filter(|source| source.enabled) {
            let path = self
                .artifacts
                .verified_source_path(
                    source.id,
                    &source.content_hash,
                    u64::try_from(source.byte_length)?,
                    &source.artifact_path,
                )
                .await?;
            layers.push(MappingLayer {
                source_key: source.source_key.clone(),
                content_hash: source.content_hash.clone(),
                path,
            });
            sources.push((source.id, source.version_id));
        }
        let candidate = self
            .artifacts
            .prepare_generation_candidate(generation_id)
            .await?;
        let destination = candidate.path().to_owned();
        let options = self.compile_options.clone();
        let summary = tokio::task::spawn_blocking(move || {
            compile(&layers, &destination, options).map_err(|error| error.to_string())
        })
        .await?
        .map_err(MappingBuildError::Compiler)?;
        if expected_fingerprint
            .is_some_and(|expected| expected != summary.chain_fingerprint.as_str())
        {
            return Err(MappingBuildError::UnexpectedFingerprint);
        }
        let artifact = if replace_existing {
            candidate.replace(&summary.chain_fingerprint).await?
        } else {
            candidate.publish(&summary.chain_fingerprint).await?
        };
        let runtime = match self
            .verified_runtime_index(
                &summary.chain_fingerprint,
                &artifact.relative_path,
                artifact.created,
            )
            .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                if artifact.created {
                    let _ = self
                        .artifacts
                        .remove_published(&artifact.relative_path)
                        .await;
                }
                return Err(MappingBuildError::Runtime(error));
            }
        };
        Ok(BuiltGeneration {
            id: generation_id,
            chain_fingerprint: summary.chain_fingerprint,
            artifact_path: artifact.relative_path,
            runtime,
            sources,
            source_paths,
            created_at,
        })
    }

    async fn verify_source_artifacts(&self, chain: &StoredChain) -> Result<(), ApplicationError> {
        for source in &chain.sources {
            self.artifacts
                .verified_source_path(
                    source.id,
                    &source.content_hash,
                    u64::try_from(source.byte_length).map_err(|_| {
                        ApplicationError::internal("stored mapping size is invalid")
                    })?,
                    &source.artifact_path,
                )
                .await
                .map_err(internal_error("mapping source artifact is invalid"))?;
        }
        Ok(())
    }

    async fn verified_runtime_index(
        &self,
        chain_fingerprint: &str,
        artifact_path: &str,
        retire_on_drop: bool,
    ) -> Result<Arc<ActiveMappingIndex>, RuntimeIndexError> {
        let path = self
            .artifacts
            .verified_generation_path(chain_fingerprint, artifact_path)
            .await?;
        open_runtime_index(
            chain_fingerprint.to_owned(),
            path,
            Arc::clone(&self.runtime_artifacts),
            retire_on_drop,
        )
        .await
    }

    async fn set_active_index(&self, runtime: Arc<ActiveMappingIndex>) {
        let mut active_index = self.active_index.write().await;
        runtime.retire_on_drop.store(false, Ordering::Release);
        runtime.artifacts.set_active(runtime.path.clone());
        if let Some(previous) = active_index.replace(runtime.clone())
            && previous.path != runtime.path
        {
            previous.retire_on_drop.store(true, Ordering::Release);
        }
    }

    async fn load_chain(&self) -> Result<StoredChain, sqlx::Error> {
        let mut transaction = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n")
            .await?;
        let state = sqlx::query_as::<_, ChainStateRow>(
            "SELECT active_generation_id
FROM mapping_chain_state
WHERE singleton
",
        )
        .fetch_one(&mut *transaction)
        .await?;
        let rows = sqlx::query_as::<_, SourceRow>(
            "SELECT
    source.id,
    source.source_key,
    source.display_name,
    source.source_kind,
    source.remote_url,
    source.http_etag,
    source.http_last_modified,
    source.priority,
    source.enabled,
    source.last_checked_at,
    source.last_refresh_error,
    source.created_at,
    source.updated_at,
    version.id AS version_id,
    version.content_hash,
    version.byte_length,
    version.artifact_path
FROM mapping_sources AS source
JOIN mapping_source_versions AS version
    ON
        version.id = source.current_version_id
        AND version.source_id = source.id
ORDER BY source.priority, source.id
",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let active_generation = match state.active_generation_id {
            Some(generation_id) => {
                let row = sqlx::query_as::<_, GenerationRow>(
                    "SELECT id, chain_fingerprint, artifact_path
FROM mapping_generations
WHERE id = $1
",
                )
                .bind(generation_id)
                .fetch_one(&mut *transaction)
                .await?;
                let sources = sqlx::query_as::<_, GenerationSourceRow>(
                    "SELECT source_id, source_version_id
FROM mapping_generation_sources
WHERE generation_id = $1
ORDER BY position
",
                )
                .bind(generation_id)
                .fetch_all(&mut *transaction)
                .await?
                .into_iter()
                .map(|source| (source.source_id, source.source_version_id))
                .collect();
                Some(StoredGeneration {
                    id: row.id,
                    chain_fingerprint: row.chain_fingerprint,
                    artifact_path: row.artifact_path,
                    sources,
                })
            }
            None => None,
        };
        transaction.commit().await?;
        Ok(StoredChain {
            active_generation,
            sources: rows
                .into_iter()
                .map(SourceRow::stored)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl MappingApi for PostgresMappingManager {
    fn state(&self) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.state_inner())
    }

    fn create_uploaded_source(
        &self,
        command: CreateUploadedMappingSource,
        content: MappingContentReader,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.create_uploaded_source_inner(command, content))
    }

    fn create_remote_source(
        &self,
        command: CreateRemoteMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.create_remote_source_inner(command))
    }

    fn update_source(
        &self,
        command: UpdateMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.update_source_inner(command))
    }

    fn create_local_mapping(
        &self,
        command: CreateLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.create_local_mapping_inner(command))
    }

    fn update_local_mapping(
        &self,
        command: UpdateLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.update_local_mapping_inner(command))
    }

    fn remove_local_mapping(
        &self,
        command: RemoveLocalMapping,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.remove_local_mapping_inner(command))
    }

    fn reorder_sources(
        &self,
        command: ReorderMappingSources,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.reorder_sources_inner(command))
    }

    fn remove_source(
        &self,
        command: RemoveMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.remove_source_inner(command))
    }

    fn refresh_remote_source(
        &self,
        command: RefreshRemoteMappingSource,
    ) -> MappingFuture<'_, MappingStateSnapshot> {
        Box::pin(self.refresh_remote_source_inner(command))
    }

    fn read_source_content(&self, source_id: Uuid) -> MappingFuture<'_, MappingSourceContent> {
        Box::pin(self.read_source_content_inner(source_id))
    }

    fn query(&self, query: QueryMappings) -> MappingFuture<'_, MappingPageSnapshot> {
        Box::pin(self.query_inner(query))
    }
}

struct ActiveMappingIndex {
    chain_fingerprint: String,
    path: PathBuf,
    connections: Vec<SyncMutex<Connection>>,
    next_connection: AtomicUsize,
    artifacts: Arc<RuntimeArtifactLifecycle>,
    retire_on_drop: AtomicBool,
}

struct LeasedSourceReader {
    reader: tokio::fs::File,
    _lease: SourceArtifactLease,
}

impl AsyncRead for LeasedSourceReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(context, buffer)
    }
}

impl Drop for ActiveMappingIndex {
    fn drop(&mut self) {
        if self.retire_on_drop.load(Ordering::Acquire)
            && !self.artifacts.is_active(&self.path)
            && std::fs::remove_file(&self.path).is_ok()
            && let Some(parent) = self.path.parent()
            && let Ok(directory) = std::fs::File::open(parent)
        {
            let _ = directory.sync_all();
        }
    }
}

#[derive(Default)]
struct RuntimeArtifactLifecycle {
    active_path: SyncRwLock<Option<PathBuf>>,
}

impl RuntimeArtifactLifecycle {
    fn set_active(&self, path: PathBuf) {
        if let Ok(mut active_path) = self.active_path.write() {
            *active_path = Some(path);
        }
    }

    fn is_active(&self, path: &PathBuf) -> bool {
        self.active_path
            .read()
            .map_or(true, |active_path| active_path.as_ref() == Some(path))
    }
}

#[derive(Default)]
struct SourceArtifactLifecycle {
    state: SyncMutex<SourceArtifactState>,
}

#[derive(Default)]
struct SourceArtifactState {
    readers: HashMap<PathBuf, usize>,
    retired: BTreeSet<PathBuf>,
}

impl SourceArtifactLifecycle {
    fn acquire(self: &Arc<Self>, path: PathBuf) -> SourceArtifactLease {
        if let Ok(mut state) = self.state.lock() {
            *state.readers.entry(path.clone()).or_default() += 1;
        }
        SourceArtifactLease {
            lifecycle: Arc::clone(self),
            path,
        }
    }

    fn retain(&self, path: &PathBuf) {
        if let Ok(mut state) = self.state.lock() {
            state.retired.remove(path);
        }
    }

    fn retire(&self, path: PathBuf) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.readers.contains_key(&path) {
            state.retired.insert(path);
            false
        } else {
            true
        }
    }
}

struct SourceArtifactLease {
    lifecycle: Arc<SourceArtifactLifecycle>,
    path: PathBuf,
}

impl Drop for SourceArtifactLease {
    fn drop(&mut self) {
        let remove = {
            let Ok(mut state) = self.lifecycle.state.lock() else {
                return;
            };
            let Some(readers) = state.readers.get_mut(&self.path) else {
                return;
            };
            *readers -= 1;
            if *readers > 0 {
                return;
            }
            state.readers.remove(&self.path);
            state.retired.remove(&self.path)
        };
        if remove
            && std::fs::remove_file(&self.path).is_ok()
            && let Some(parent) = self.path.parent()
            && let Ok(directory) = std::fs::File::open(parent)
        {
            let _ = directory.sync_all();
        }
    }
}

enum LocalMappingMutation {
    Create {
        record: String,
        changed_at: OffsetDateTime,
    },
    Update {
        statement_id: String,
        record: String,
        changed_at: OffsetDateTime,
    },
    Remove {
        statement_id: String,
        changed_at: OffsetDateTime,
    },
}

impl LocalMappingMutation {
    fn changed_at(&self) -> OffsetDateTime {
        match self {
            Self::Create { changed_at, .. }
            | Self::Update { changed_at, .. }
            | Self::Remove { changed_at, .. } => *changed_at,
        }
    }

    fn apply(self, document: &mut LocalMappingDocument) -> Result<(), LocalMappingDocumentError> {
        match self {
            Self::Create { record, .. } => document.create(&record),
            Self::Update {
                statement_id,
                record,
                ..
            } => document.update(&statement_id, &record),
            Self::Remove { statement_id, .. } => document.remove(&statement_id),
        }
    }
}

#[derive(Debug, Error)]
enum LocalMappingMutationError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Document(#[from] LocalMappingDocumentError),
}

#[derive(Clone)]
struct StoredSource {
    id: Uuid,
    version_id: Uuid,
    source_key: String,
    display_name: String,
    kind: MappingSourceKind,
    remote_url: Option<String>,
    http_etag: Option<String>,
    http_last_modified: Option<String>,
    priority: i64,
    enabled: bool,
    content_hash: String,
    byte_length: i64,
    artifact_path: String,
    last_checked_at: Option<OffsetDateTime>,
    last_refresh_error: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

struct NewStoredSource {
    id: Uuid,
    version_id: Uuid,
    source_key: String,
    display_name: String,
    kind: MappingSourceKind,
    remote_url: Option<String>,
    http_etag: Option<String>,
    http_last_modified: Option<String>,
    priority: i64,
    enabled: bool,
    created_at: OffsetDateTime,
}

impl StoredSource {
    fn snapshot(&self) -> MappingSourceSnapshot {
        MappingSourceSnapshot {
            id: self.id,
            source_key: self.source_key.clone(),
            display_name: self.display_name.clone(),
            kind: self.kind,
            remote_url: self.remote_url.clone(),
            priority: self.priority,
            enabled: self.enabled,
            content_hash: self.content_hash.clone(),
            byte_length: self.byte_length,
            last_checked_at: self.last_checked_at,
            last_refresh_error: self.last_refresh_error.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

struct StoredChain {
    active_generation: Option<StoredGeneration>,
    sources: Vec<StoredSource>,
}

impl StoredChain {
    fn snapshot(&self) -> MappingStateSnapshot {
        MappingStateSnapshot {
            active_chain_fingerprint: self
                .active_generation
                .as_ref()
                .map(|generation| generation.chain_fingerprint.clone()),
            sources: self.sources.iter().map(StoredSource::snapshot).collect(),
        }
    }

    fn sort_sources(&mut self) {
        self.sources
            .sort_by_key(|source| (source.priority, source.id));
    }
}

struct StoredGeneration {
    id: Uuid,
    chain_fingerprint: String,
    artifact_path: String,
    sources: Vec<(Uuid, Uuid)>,
}

struct BuiltGeneration {
    id: Uuid,
    chain_fingerprint: String,
    artifact_path: String,
    runtime: Arc<ActiveMappingIndex>,
    sources: Vec<(Uuid, Uuid)>,
    source_paths: Vec<PathBuf>,
    created_at: OffsetDateTime,
}

enum PreparedGeneration {
    Active { id: Uuid },
    Built(BuiltGeneration),
}

#[derive(Default)]
struct RetiredArtifacts {
    generation_paths: Vec<String>,
    source_paths: Vec<String>,
}

#[derive(FromRow)]
struct ChainStateRow {
    active_generation_id: Option<Uuid>,
}

#[derive(FromRow)]
struct SourceRow {
    id: Uuid,
    source_key: String,
    display_name: String,
    source_kind: String,
    remote_url: Option<String>,
    http_etag: Option<String>,
    http_last_modified: Option<String>,
    priority: i64,
    enabled: bool,
    last_checked_at: Option<OffsetDateTime>,
    last_refresh_error: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    version_id: Uuid,
    content_hash: String,
    byte_length: i64,
    artifact_path: String,
}

impl SourceRow {
    fn stored(self) -> Result<StoredSource, sqlx::Error> {
        Ok(StoredSource {
            id: self.id,
            version_id: self.version_id,
            source_key: self.source_key,
            display_name: self.display_name,
            kind: parse_source_kind(&self.source_kind)?,
            remote_url: self.remote_url,
            http_etag: self.http_etag,
            http_last_modified: self.http_last_modified,
            priority: self.priority,
            enabled: self.enabled,
            content_hash: self.content_hash,
            byte_length: self.byte_length,
            artifact_path: self.artifact_path,
            last_checked_at: self.last_checked_at,
            last_refresh_error: self.last_refresh_error,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(FromRow)]
struct GenerationRow {
    id: Uuid,
    chain_fingerprint: String,
    artifact_path: String,
}

#[derive(FromRow)]
struct GenerationSourceRow {
    source_id: Uuid,
    source_version_id: Uuid,
}

fn stored_source(
    source: NewStoredSource,
    artifact: PublishedSourceArtifact,
) -> Result<StoredSource, ApplicationError> {
    Ok(StoredSource {
        id: source.id,
        version_id: source.version_id,
        source_key: source.source_key,
        display_name: source.display_name,
        kind: source.kind,
        remote_url: source.remote_url,
        http_etag: source.http_etag,
        http_last_modified: source.http_last_modified,
        priority: source.priority,
        enabled: source.enabled,
        content_hash: artifact.content_hash,
        byte_length: i64::try_from(artifact.byte_length)
            .map_err(|_| invalid_source("mapping source is too large"))?,
        artifact_path: artifact.relative_path,
        last_checked_at: (source.kind == MappingSourceKind::Remote).then_some(source.created_at),
        last_refresh_error: None,
        created_at: source.created_at,
        updated_at: source.created_at,
    })
}

fn active_manifest_matches(chain: &StoredChain, generation: &StoredGeneration) -> bool {
    chain
        .sources
        .iter()
        .filter(|source| source.enabled)
        .map(|source| (source.id, source.version_id))
        .eq(generation.sources.iter().copied())
}

async fn lock_chain(transaction: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT singleton
FROM mapping_chain_state
WHERE singleton
FOR UPDATE
",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_source(
    transaction: &mut Transaction<'_, Postgres>,
    source: &StoredSource,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mapping_sources (
    id, source_key, source_kind, display_name, remote_url,
    http_etag, http_last_modified, priority, enabled, current_version_id,
    last_checked_at, last_refresh_error, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)
",
    )
    .bind(source.id)
    .bind(&source.source_key)
    .bind(source_kind_text(source.kind))
    .bind(&source.display_name)
    .bind(&source.remote_url)
    .bind(&source.http_etag)
    .bind(&source.http_last_modified)
    .bind(source.priority)
    .bind(source.enabled)
    .bind(source.version_id)
    .bind(source.last_checked_at)
    .bind(&source.last_refresh_error)
    .bind(source.created_at)
    .execute(&mut **transaction)
    .await?;
    insert_version(transaction, source).await
}

async fn insert_version(
    transaction: &mut Transaction<'_, Postgres>,
    source: &StoredSource,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mapping_source_versions (
    id, source_id, content_hash, byte_length, artifact_path, created_at
)
VALUES ($1, $2, $3, $4, $5, $6)
",
    )
    .bind(source.version_id)
    .bind(source.id)
    .bind(&source.content_hash)
    .bind(source.byte_length)
    .bind(&source.artifact_path)
    .bind(source.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_generation(
    transaction: &mut Transaction<'_, Postgres>,
    generation: &BuiltGeneration,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mapping_generations (
    id, chain_fingerprint, artifact_path, created_at
)
VALUES ($1, $2, $3, $4)
",
    )
    .bind(generation.id)
    .bind(&generation.chain_fingerprint)
    .bind(&generation.artifact_path)
    .bind(generation.created_at)
    .execute(&mut **transaction)
    .await?;
    for (position, (source_id, version_id)) in generation.sources.iter().enumerate() {
        sqlx::query(
            "INSERT INTO mapping_generation_sources (
    generation_id, position, source_id, source_version_id
)
VALUES ($1, $2, $3, $4)
",
        )
        .bind(generation.id)
        .bind(i64::try_from(position).map_err(|error| {
            sqlx::Error::Protocol(format!("mapping source position is out of range: {error}"))
        })?)
        .bind(source_id)
        .bind(version_id)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn stage_generation(
    transaction: &mut Transaction<'_, Postgres>,
    generation: &PreparedGeneration,
    activated_at: OffsetDateTime,
) -> Result<RetiredArtifacts, sqlx::Error> {
    match generation {
        PreparedGeneration::Active { id } => retire_inactive_manifests(transaction, *id).await,
        PreparedGeneration::Built(generation) => {
            insert_generation(transaction, generation).await?;
            activate_generation(transaction, generation.id, activated_at).await
        }
    }
}

async fn activate_generation(
    transaction: &mut Transaction<'_, Postgres>,
    generation_id: Uuid,
    activated_at: OffsetDateTime,
) -> Result<RetiredArtifacts, sqlx::Error> {
    sqlx::query(
        "UPDATE mapping_chain_state
SET active_generation_id = $1, updated_at = $2
WHERE singleton
",
    )
    .bind(generation_id)
    .bind(activated_at)
    .execute(&mut **transaction)
    .await?;
    retire_inactive_manifests(transaction, generation_id).await
}

async fn retire_inactive_manifests(
    transaction: &mut Transaction<'_, Postgres>,
    generation_id: Uuid,
) -> Result<RetiredArtifacts, sqlx::Error> {
    let generation_paths = sqlx::query_scalar::<_, String>(
        "DELETE FROM mapping_generations
WHERE id <> $1
RETURNING artifact_path
",
    )
    .bind(generation_id)
    .fetch_all(&mut **transaction)
    .await?;
    let source_paths = sqlx::query_scalar::<_, String>(
        "DELETE FROM mapping_source_versions AS version
WHERE
    NOT EXISTS (
        SELECT 1
        FROM mapping_sources AS source
        WHERE source.current_version_id = version.id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM mapping_generation_sources AS generation_source
        WHERE generation_source.source_version_id = version.id
    )
RETURNING artifact_path
",
    )
    .fetch_all(&mut **transaction)
    .await?;
    Ok(RetiredArtifacts {
        generation_paths,
        source_paths,
    })
}

async fn open_runtime_index(
    chain_fingerprint: String,
    path: PathBuf,
    artifacts: Arc<RuntimeArtifactLifecycle>,
    retire_on_drop: bool,
) -> Result<Arc<ActiveMappingIndex>, RuntimeIndexError> {
    tokio::task::spawn_blocking(move || {
        let mut connections = Vec::with_capacity(RUNTIME_CONNECTIONS);
        for _ in 0..RUNTIME_CONNECTIONS {
            connections
                .push(SyncMutex::new(open_read_only(&path).map_err(|error| {
                    RuntimeIndexError::Invalid(error.to_string())
                })?));
        }
        let connection = connections[0]
            .lock()
            .map_err(|_| RuntimeIndexError::Invalid("connection lock is poisoned".to_owned()))?;
        #[allow(unknown_lints, cargo_sqruff)]
        let integrity = connection
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
            .map_err(|error| RuntimeIndexError::Invalid(error.to_string()))?;
        if integrity != "ok" {
            return Err(RuntimeIndexError::Invalid(integrity));
        }
        #[allow(unknown_lints, cargo_sqruff)]
        let stored_fingerprint = connection
            .query_row(
                "SELECT value FROM build_metadata WHERE key = 'chain_fingerprint'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| RuntimeIndexError::Invalid(error.to_string()))?;
        if stored_fingerprint != chain_fingerprint {
            return Err(RuntimeIndexError::Invalid(
                "chain fingerprint does not match the active manifest".to_owned(),
            ));
        }
        drop(connection);
        Ok(Arc::new(ActiveMappingIndex {
            chain_fingerprint,
            path,
            connections,
            next_connection: AtomicUsize::new(0),
            artifacts,
            retire_on_drop: AtomicBool::new(retire_on_drop),
        }))
    })
    .await?
}

fn required_text(value: &str, field: &str) -> Result<String, ApplicationError> {
    let value = value.trim();
    if value.is_empty() {
        Err(ApplicationError::field(
            ApplicationErrorCode::InvalidMappingSource,
            "mapping source is invalid",
            field,
            "required",
            "value is required",
        ))
    } else {
        Ok(value.to_owned())
    }
}

fn ensure_source_absent(
    chain: &StoredChain,
    source_id: Uuid,
    source_key: &str,
) -> Result<(), ApplicationError> {
    if chain
        .sources
        .iter()
        .any(|source| source.id == source_id || source.source_key == source_key)
    {
        Err(invalid_source("mapping source already exists"))
    } else {
        Ok(())
    }
}

fn ensure_remote_url_absent(chain: &StoredChain, remote_url: &str) -> Result<(), ApplicationError> {
    if chain
        .sources
        .iter()
        .any(|source| source.remote_url.as_deref() == Some(remote_url))
    {
        Err(invalid_source("remote mapping source already exists"))
    } else {
        Ok(())
    }
}

fn next_source_priority(chain: &StoredChain) -> Result<i64, ApplicationError> {
    chain
        .sources
        .iter()
        .filter(|source| source.kind != MappingSourceKind::Local)
        .map(|source| source.priority)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid_source("mapping source priority is out of range"))
}

fn source_kind_text(kind: MappingSourceKind) -> &'static str {
    match kind {
        MappingSourceKind::Local => "local",
        MappingSourceKind::Upload => "upload",
        MappingSourceKind::Remote => "remote",
    }
}

fn parse_source_kind(value: &str) -> Result<MappingSourceKind, sqlx::Error> {
    match value {
        "local" => Ok(MappingSourceKind::Local),
        "upload" => Ok(MappingSourceKind::Upload),
        "remote" => Ok(MappingSourceKind::Remote),
        value => Err(sqlx::Error::Protocol(format!(
            "stored mapping source kind is invalid: {value}"
        ))),
    }
}

fn decode_cursor(value: Option<&str>) -> Result<Option<MappingCursor>, ApplicationError> {
    value
        .map(|value| {
            let bytes = hex::decode(value).map_err(|_| invalid_cursor())?;
            serde_json::from_slice(&bytes).map_err(|_| invalid_cursor())
        })
        .transpose()
}

fn encode_cursor(cursor: MappingCursor) -> Result<String, ApplicationError> {
    serde_json::to_vec(&cursor)
        .map(hex::encode)
        .map_err(|_| ApplicationError::internal("mapping cursor could not be encoded"))
}

fn invalid_cursor() -> ApplicationError {
    ApplicationError::field(
        ApplicationErrorCode::InvalidField,
        "mapping query contains an invalid cursor",
        "cursor",
        "invalid_cursor",
        "cursor is malformed",
    )
}

fn source_not_found() -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::MappingSourceNotFound,
        "mapping source was not found",
    )
}

fn mapping_chain_changed() -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::MappingChainChanged,
        "mapping source chain changed; reload it and retry",
    )
}

fn local_mapping_mutation_error(error: LocalMappingMutationError) -> ApplicationError {
    match error {
        LocalMappingMutationError::Document(LocalMappingDocumentError::Invalid(message)) => {
            ApplicationError::field(
                ApplicationErrorCode::InvalidMappingSource,
                "local mapping is invalid",
                "record",
                "invalid_mapping",
                message,
            )
        }
        LocalMappingMutationError::Document(LocalMappingDocumentError::AlreadyExists) => {
            invalid_source("local mapping already exists")
        }
        LocalMappingMutationError::Document(LocalMappingDocumentError::NotFound) => {
            ApplicationError::new(
                ApplicationErrorCode::MappingNotFound,
                "local mapping was not found",
            )
        }
        LocalMappingMutationError::Document(LocalMappingDocumentError::Stored(message)) => {
            tracing::error!(%message, "stored local mappings are invalid");
            ApplicationError::internal("stored local mappings are invalid")
        }
        LocalMappingMutationError::Io(error) => {
            tracing::error!(%error, "local mapping source could not be read");
            ApplicationError::internal("local mapping source could not be read")
        }
    }
}

fn invalid_source(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ApplicationErrorCode::InvalidMappingSource, message)
}

fn remote_source_unavailable(message: impl Into<String>) -> ApplicationError {
    ApplicationError::new(ApplicationErrorCode::MappingSourceUnavailable, message)
}

fn remote_source_fetch_error(error: RemoteSourceFetchError) -> ApplicationError {
    if error.is_invalid_source() {
        ApplicationError::field(
            ApplicationErrorCode::InvalidMappingSource,
            "mapping source is invalid",
            "remoteUrl",
            "unsupported_url",
            error.to_string(),
        )
    } else {
        tracing::warn!(%error, "remote mapping source fetch failed");
        remote_source_unavailable(error.to_string())
    }
}

fn source_artifact_error(error: MappingArtifactError) -> ApplicationError {
    match error {
        MappingArtifactError::SourceTooLarge { maximum } => source_too_large(maximum),
        error => {
            tracing::error!(%error, "mapping source artifact operation failed");
            ApplicationError::internal("mapping source content could not be stored")
        }
    }
}

fn remote_source_artifact_error(error: MappingArtifactError) -> ApplicationError {
    match error {
        MappingArtifactError::SourceTooLarge { maximum } => source_too_large(maximum),
        MappingArtifactError::Io(error)
            if error.kind() == std::io::ErrorKind::ConnectionAborted =>
        {
            tracing::warn!(%error, "remote mapping source body download failed");
            remote_source_unavailable("remote mapping source download was interrupted")
        }
        error => source_artifact_error(error),
    }
}

fn source_too_large(maximum: u64) -> ApplicationError {
    invalid_source(format!(
        "mapping source exceeds the {} MiB limit",
        maximum / (1024 * 1024)
    ))
}

fn mapping_compilation_error(error: MappingBuildError) -> ApplicationError {
    tracing::info!(%error, "mapping candidate was rejected");
    invalid_source(format!("mapping source is invalid: {error}"))
}

fn mapping_write_error(error: sqlx::Error) -> ApplicationError {
    if error.as_database_error().is_some_and(|database| {
        matches!(
            database.constraint(),
            Some(
                "mapping_sources_pkey"
                    | "mapping_sources_source_key_key"
                    | "mapping_sources_priority_key"
                    | "mapping_sources_local_singleton_idx"
                    | "mapping_sources_remote_url_unique_idx"
                    | "mapping_source_versions_pkey"
                    | "mapping_source_versions_source_id_content_hash_key"
            )
        )
    }) {
        invalid_source("mapping source conflicts with existing state")
    } else {
        tracing::error!(%error, "mapping database write failed");
        ApplicationError::internal("mapping changes could not be stored")
    }
}

fn internal_error<E>(message: &'static str) -> impl FnOnce(E) -> ApplicationError
where
    E: std::fmt::Display,
{
    move |error| {
        tracing::error!(%error, "{message}");
        ApplicationError::internal(message)
    }
}

fn random_uuid() -> Result<Uuid, ApplicationError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|_| ApplicationError::internal("mapping identity could not be created"))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

#[derive(Debug, Error)]
enum MappingBuildError {
    #[error(transparent)]
    Artifact(#[from] MappingArtifactError),
    #[error("mapping compiler rejected the source chain: {0}")]
    Compiler(String),
    #[error("mapping index fingerprint changed during recovery")]
    UnexpectedFingerprint,
    #[error("mapping runtime index is invalid: {0}")]
    Runtime(#[source] RuntimeIndexError),
    #[error("mapping compiler task failed")]
    Join(#[from] tokio::task::JoinError),
    #[error("mapping source size is out of range")]
    Integer(#[from] std::num::TryFromIntError),
}

#[derive(Debug, Error)]
enum RuntimeIndexError {
    #[error(transparent)]
    Artifact(#[from] MappingArtifactError),
    #[error("{0}")]
    Invalid(String),
    #[error("mapping index validation task failed")]
    Join(#[from] tokio::task::JoinError),
}
