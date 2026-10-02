use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";

#[derive(Clone)]
pub struct MappingArtifactStore {
    data_directory: PathBuf,
    root: PathBuf,
}

pub struct MappingGenerationCandidate {
    path: PathBuf,
    generations_directory: PathBuf,
    _writer_lock: MappingWriterLock,
}

pub struct MappingRuntimeLease {
    _file: File,
}

impl MappingGenerationCandidate {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn publish(
        self,
        chain_fingerprint: &str,
    ) -> Result<PublishedGenerationArtifact, MappingArtifactError> {
        self.publish_with_mode(chain_fingerprint, false).await
    }

    pub async fn replace(
        self,
        chain_fingerprint: &str,
    ) -> Result<PublishedGenerationArtifact, MappingArtifactError> {
        self.publish_with_mode(chain_fingerprint, true).await
    }

    async fn publish_with_mode(
        self,
        chain_fingerprint: &str,
        replace_existing: bool,
    ) -> Result<PublishedGenerationArtifact, MappingArtifactError> {
        validate_hash(chain_fingerprint)?;
        verify_sqlite_file(&self.path).await?;
        sync_file(self.path.clone()).await?;
        let destination = self
            .generations_directory
            .join(format!("{chain_fingerprint}.sqlite"));
        let relative_path = format!("mappings/generations/{chain_fingerprint}.sqlite");
        if path_exists(&destination).await? {
            if replace_existing {
                remove_path_if_present(&destination).await?;
            } else {
                verify_sqlite_file(&destination).await?;
                remove_path_if_present(&self.path).await?;
                let byte_length = tokio::fs::metadata(&destination).await?.len();
                return Ok(PublishedGenerationArtifact {
                    relative_path,
                    byte_length,
                    created: false,
                });
            }
        }
        let byte_length = tokio::fs::metadata(&self.path).await?.len();
        tokio::fs::rename(&self.path, &destination).await?;
        sync_directory(self.generations_directory.clone()).await?;
        Ok(PublishedGenerationArtifact {
            relative_path,
            byte_length,
            created: true,
        })
    }
}

impl Drop for MappingGenerationCandidate {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl MappingArtifactStore {
    pub fn new(data_directory: impl Into<PathBuf>) -> Self {
        let data_directory = data_directory.into();
        let root = data_directory.join("mappings");
        Self {
            data_directory,
            root,
        }
    }

    pub async fn ingest_source<R>(
        &self,
        source_id: Uuid,
        candidate_id: Uuid,
        maximum_bytes: u64,
        reader: &mut R,
    ) -> Result<PublishedSourceArtifact, MappingArtifactError>
    where
        R: AsyncRead + Unpin,
    {
        let _writer_lock = self.acquire_writer_lock().await?;
        self.prepare_layout().await?;
        let source_directory = self.root.join("sources").join(source_id.to_string());
        ensure_plain_directory(&source_directory).await?;
        sync_directory(self.root.join("sources")).await?;
        let staging = self
            .root
            .join("staging")
            .join(format!("source-{source_id}-{candidate_id}.tmp"));
        remove_path_if_present(&staging).await?;
        let (content_hash, byte_length) = match self
            .write_source_candidate(&staging, maximum_bytes, reader)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let _ = remove_path_if_present(&staging).await;
                return Err(error);
            }
        };
        let destination = source_directory.join(format!("{content_hash}.trakkin"));
        let relative_path = format!("mappings/sources/{source_id}/{content_hash}.trakkin");
        if path_exists(&destination).await? {
            verify_source_file(&destination, &content_hash, byte_length).await?;
            remove_path_if_present(&staging).await?;
        } else {
            tokio::fs::rename(&staging, &destination).await?;
        }
        sync_directory(source_directory).await?;
        Ok(PublishedSourceArtifact {
            relative_path,
            content_hash,
            byte_length,
        })
    }

    pub async fn reset_staging(&self) -> Result<(), MappingArtifactError> {
        let _writer_lock = self.acquire_writer_lock().await?;
        self.prepare_layout().await?;
        reset_staging(&self.root.join("staging")).await
    }

    pub async fn acquire_runtime_lease(&self) -> Result<MappingRuntimeLease, MappingArtifactError> {
        ensure_plain_directory(&self.root).await?;
        let lock_path = self.root.join(".runtime.lock");
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_path)?;
            match file.try_lock() {
                Ok(()) => Ok(MappingRuntimeLease { _file: file }),
                Err(std::fs::TryLockError::WouldBlock) => {
                    Err(MappingArtifactError::RuntimeAlreadyActive)
                }
                Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
            }
        })
        .await
        .map_err(std::io::Error::other)?
    }

    pub async fn prepare_generation_candidate(
        &self,
        generation_id: Uuid,
    ) -> Result<MappingGenerationCandidate, MappingArtifactError> {
        let writer_lock = self.acquire_writer_lock().await?;
        self.prepare_layout().await?;
        let candidate = self.generation_candidate_path(generation_id);
        if path_exists(&candidate).await? {
            return Err(MappingArtifactError::CandidateAlreadyExists);
        }
        Ok(MappingGenerationCandidate {
            path: candidate,
            generations_directory: self.root.join("generations"),
            _writer_lock: writer_lock,
        })
    }

    pub async fn verified_source_path(
        &self,
        source_id: Uuid,
        content_hash: &str,
        expected_bytes: u64,
        artifact_path: &str,
    ) -> Result<PathBuf, MappingArtifactError> {
        validate_hash(content_hash)?;
        let expected_path = format!("mappings/sources/{source_id}/{content_hash}.trakkin");
        if artifact_path != expected_path {
            return Err(MappingArtifactError::InvalidArtifactPath);
        }
        let path = self.data_directory.join(expected_path);
        verify_source_file(&path, content_hash, expected_bytes).await?;
        Ok(path)
    }

    pub async fn verified_generation_path(
        &self,
        chain_fingerprint: &str,
        artifact_path: &str,
    ) -> Result<PathBuf, MappingArtifactError> {
        validate_hash(chain_fingerprint)?;
        let expected_path = format!("mappings/generations/{chain_fingerprint}.sqlite");
        if artifact_path != expected_path {
            return Err(MappingArtifactError::InvalidArtifactPath);
        }
        let path = self.data_directory.join(expected_path);
        verify_sqlite_file(&path).await?;
        Ok(path)
    }

    pub async fn remove_published(&self, relative_path: &str) -> Result<(), MappingArtifactError> {
        let path = published_artifact_path(&self.data_directory, relative_path)?;
        let _writer_lock = self.acquire_writer_lock().await?;
        match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) if metadata.file_type().is_file() => {
                tokio::fs::remove_file(&path).await?;
                if let Some(parent) = path.parent() {
                    sync_directory(parent.to_owned()).await?;
                }
                Ok(())
            }
            Ok(_) => Err(MappingArtifactError::InvalidArtifact),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn published_path(&self, relative_path: &str) -> Result<PathBuf, MappingArtifactError> {
        published_artifact_path(&self.data_directory, relative_path)
    }

    pub async fn retain_only(
        &self,
        retained_paths: &BTreeSet<String>,
    ) -> Result<(), MappingArtifactError> {
        let retained = retained_paths
            .iter()
            .map(|path| published_artifact_path(&self.data_directory, path))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let _writer_lock = self.acquire_writer_lock().await?;
        self.prepare_layout().await?;
        prune_flat_directory(&self.root.join("generations"), &retained).await?;

        let sources_directory = self.root.join("sources");
        let mut source_directories = tokio::fs::read_dir(&sources_directory).await?;
        while let Some(entry) = source_directories.next_entry().await? {
            let path = entry.path();
            let metadata = tokio::fs::symlink_metadata(&path).await?;
            if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
                prune_flat_directory(&path, &retained).await?;
                if tokio::fs::read_dir(&path)
                    .await?
                    .next_entry()
                    .await?
                    .is_none()
                {
                    tokio::fs::remove_dir(&path).await?;
                }
            } else {
                remove_path_if_present(&path).await?;
            }
        }
        sync_directory(sources_directory).await?;
        Ok(())
    }

    async fn write_source_candidate<R>(
        &self,
        staging: &Path,
        maximum_bytes: u64,
        reader: &mut R,
    ) -> Result<(String, u64), MappingArtifactError>
    where
        R: AsyncRead + Unpin,
    {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(staging)
            .await?;
        let mut hasher = Sha256::new();
        let mut byte_length = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = reader.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            byte_length = byte_length
                .checked_add(u64::try_from(count)?)
                .ok_or(MappingArtifactError::ByteLengthOverflow)?;
            if byte_length > maximum_bytes {
                return Err(MappingArtifactError::SourceTooLarge {
                    maximum: maximum_bytes,
                });
            }
            hasher.update(&buffer[..count]);
            file.write_all(&buffer[..count]).await?;
        }
        file.sync_all().await?;
        Ok((hex::encode(hasher.finalize()), byte_length))
    }

    async fn prepare_layout(&self) -> Result<(), MappingArtifactError> {
        ensure_plain_directory(&self.root).await?;
        for child in ["sources", "generations", "staging"] {
            ensure_plain_directory(&self.root.join(child)).await?;
        }
        sync_directory(self.root.clone()).await?;
        Ok(())
    }

    async fn acquire_writer_lock(&self) -> Result<MappingWriterLock, MappingArtifactError> {
        ensure_plain_directory(&self.root).await?;
        let lock_path = self.root.join(".writer.lock");
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
                    Ok(()) => Ok(Some(MappingWriterLock { _file: file })),
                    Err(std::fs::TryLockError::WouldBlock) => Ok(None),
                    Err(std::fs::TryLockError::Error(error)) => Err(error),
                }
            })
            .await
            .map_err(std::io::Error::other)??;
            if let Some(lock) = lock {
                return Ok(lock);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn generation_candidate_path(&self, generation_id: Uuid) -> PathBuf {
        self.root
            .join("staging")
            .join(format!("generation-{generation_id}.sqlite"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedSourceArtifact {
    pub relative_path: String,
    pub content_hash: String,
    pub byte_length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedGenerationArtifact {
    pub relative_path: String,
    pub byte_length: u64,
    pub created: bool,
}

struct MappingWriterLock {
    _file: File,
}

async fn verify_source_file(
    path: &Path,
    expected_hash: &str,
    expected_bytes: u64,
) -> Result<(), MappingArtifactError> {
    let metadata = plain_file_metadata(path).await?;
    if metadata.len() != expected_bytes {
        return Err(MappingArtifactError::ByteLengthMismatch {
            expected: expected_bytes,
            actual: metadata.len(),
        });
    }
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    if hex::encode(hasher.finalize()) != expected_hash {
        return Err(MappingArtifactError::ContentHashMismatch);
    }
    Ok(())
}

async fn verify_sqlite_file(path: &Path) -> Result<(), MappingArtifactError> {
    let metadata = plain_file_metadata(path).await?;
    if metadata.len() < u64::try_from(SQLITE_HEADER.len())? {
        return Err(MappingArtifactError::InvalidArtifact);
    }
    let mut file = tokio::fs::File::open(path).await?;
    let mut header = [0_u8; SQLITE_HEADER.len()];
    file.read_exact(&mut header).await?;
    if &header != SQLITE_HEADER {
        return Err(MappingArtifactError::InvalidArtifact);
    }
    Ok(())
}

async fn plain_file_metadata(path: &Path) -> Result<std::fs::Metadata, MappingArtifactError> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(MappingArtifactError::InvalidArtifact);
    }
    Ok(metadata)
}

async fn ensure_plain_directory(directory: &Path) -> Result<(), MappingArtifactError> {
    match tokio::fs::symlink_metadata(directory).await {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            Ok(())
        }
        Ok(_) => Err(MappingArtifactError::InvalidArtifact),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(directory).await?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

async fn reset_staging(directory: &Path) -> Result<(), MappingArtifactError> {
    ensure_plain_directory(directory).await?;
    let mut entries = tokio::fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        remove_path_if_present(&entry.path()).await?;
    }
    sync_directory(directory.to_owned()).await?;
    Ok(())
}

async fn prune_flat_directory(
    directory: &Path,
    retained: &BTreeSet<PathBuf>,
) -> Result<(), MappingArtifactError> {
    ensure_plain_directory(directory).await?;
    let mut entries = tokio::fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !retained.contains(&path) {
            remove_path_if_present(&path).await?;
        }
    }
    sync_directory(directory.to_owned()).await?;
    Ok(())
}

async fn remove_path_if_present(path: &Path) -> Result<(), MappingArtifactError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            tokio::fs::remove_dir_all(path).await?;
            Ok(())
        }
        Ok(_) => {
            tokio::fs::remove_file(path).await?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn path_exists(path: &Path) -> Result<bool, MappingArtifactError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn sync_file(path: PathBuf) -> Result<(), MappingArtifactError> {
    tokio::task::spawn_blocking(move || File::open(path)?.sync_all())
        .await
        .map_err(std::io::Error::other)??;
    Ok(())
}

#[cfg(unix)]
async fn sync_directory(path: PathBuf) -> Result<(), MappingArtifactError> {
    tokio::task::spawn_blocking(move || File::open(path)?.sync_all())
        .await
        .map_err(std::io::Error::other)??;
    Ok(())
}

#[cfg(not(unix))]
async fn sync_directory(_path: PathBuf) -> Result<(), MappingArtifactError> {
    Ok(())
}

fn validate_hash(value: &str) -> Result<(), MappingArtifactError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(MappingArtifactError::InvalidContentHash)
    }
}

fn published_artifact_path(
    data_directory: &Path,
    relative_path: &str,
) -> Result<PathBuf, MappingArtifactError> {
    let relative = Path::new(relative_path);
    let components = relative.components().collect::<Vec<_>>();
    let valid_root = matches!(
        components.as_slice(),
        [
            std::path::Component::Normal(root),
            std::path::Component::Normal(kind),
            ..
        ] if *root == "mappings" && (*kind == "sources" || *kind == "generations")
    );
    if !valid_root
        || components.len() < 3
        || components
            .iter()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(MappingArtifactError::InvalidArtifactPath);
    }
    Ok(data_directory.join(relative))
}

#[derive(Debug, Error)]
pub enum MappingArtifactError {
    #[error("mapping content hash must be a lowercase SHA-256 digest")]
    InvalidContentHash,
    #[error("mapping artifact is not a plain valid file")]
    InvalidArtifact,
    #[error("mapping artifact path does not match its content address")]
    InvalidArtifactPath,
    #[error("mapping artifact candidate already exists")]
    CandidateAlreadyExists,
    #[error("another Trakkin runtime already owns the mapping artifact store")]
    RuntimeAlreadyActive,
    #[error("mapping artifact byte length mismatch: expected {expected}, got {actual}")]
    ByteLengthMismatch { expected: u64, actual: u64 },
    #[error("mapping artifact content hash mismatch")]
    ContentHashMismatch,
    #[error("mapping artifact byte length overflow")]
    ByteLengthOverflow,
    #[error("mapping source exceeds the {maximum}-byte limit")]
    SourceTooLarge { maximum: u64 },
    #[error("mapping artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("mapping artifact numeric value is out of range")]
    Integer(#[from] std::num::TryFromIntError),
}
