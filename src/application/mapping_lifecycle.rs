use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
};

use time::OffsetDateTime;
use trakkin_mapping::{
    MappingDocument, MappingError, MappingPackage, PackageCompatibility, validate_content_hash,
};
use uuid::Uuid;

pub type MappingLifecycleFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, MappingLifecycleError<E>>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingRevisionOrigin {
    Created,
    Updated,
    Imported,
    Forked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingRevisionParent {
    pub content_hash: String,
    pub verified: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingRevisionSnapshot {
    pub mapping_id: Uuid,
    pub document: MappingDocument,
    pub content_hash: String,
    pub parents: Vec<MappingRevisionParent>,
    pub origin: MappingRevisionOrigin,
    pub package: Option<PackageImportMetadata>,
    pub content_reused: bool,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalRowSetDiff {
    pub added: Vec<Vec<String>>,
    pub removed: Vec<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingActivationKind {
    Activate,
    Rollback,
    Retire,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceMappingActivation {
    pub workspace_id: Uuid,
    pub mapping_id: Uuid,
    pub active_content_hash: Option<String>,
    pub generation: i64,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackageImportMetadata {
    pub package_id: Uuid,
    pub document_path: String,
    pub roots: Vec<String>,
    pub license_expression: String,
    pub attribution: Vec<String>,
    pub signatures: Vec<(String, String)>,
    pub forge_commit: Option<String>,
    pub required_mapping_schema: String,
    pub required_resolver: String,
    pub required_coordinate_registry_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreMappingRevision {
    pub mapping_id: Uuid,
    pub document: MappingDocument,
    pub parents: Vec<MappingRevisionParent>,
    pub origin: MappingRevisionOrigin,
    pub package: Option<PackageImportMetadata>,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendMappingRevision {
    pub expected_head: String,
    pub revision: StoreMappingRevision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeMappingActivation {
    pub event_id: Uuid,
    pub workspace_id: Uuid,
    pub mapping_id: Uuid,
    pub content_hash: Option<String>,
    pub kind: MappingActivationKind,
    pub changed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMapping {
    pub mapping_id: Uuid,
    pub mappings: Vec<Vec<String>>,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorkspaceMappingDocument {
    pub mapping_id: Uuid,
    pub workspace_id: Uuid,
    pub schema: String,
    pub mappings: Vec<Vec<String>>,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateMapping {
    pub mapping_id: Uuid,
    pub base_content_hash: String,
    pub mappings: Vec<Vec<String>>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateWorkspaceMappingDocument {
    pub workspace_id: Uuid,
    pub mapping_id: Uuid,
    pub base_content_hash: String,
    pub mappings: Vec<Vec<String>>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivateWorkspaceMappingRevision {
    pub event_id: Uuid,
    pub workspace_id: Uuid,
    pub mapping_id: Uuid,
    pub content_hash: String,
    pub changed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetireWorkspaceMapping {
    pub event_id: Uuid,
    pub workspace_id: Uuid,
    pub mapping_id: Uuid,
    pub changed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportMapping {
    pub mapping_id: Uuid,
    pub document: MappingDocument,
    pub parents: Vec<String>,
    pub imported_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkMapping {
    pub source_mapping_id: Uuid,
    pub selected_content_hash: String,
    pub new_mapping_id: Uuid,
    pub mappings: Vec<Vec<String>>,
    pub forked_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportMappingPackage {
    pub package_id: Uuid,
    pub package: MappingPackage,
    pub documents: BTreeMap<String, MappingDocument>,
    pub mapping_ids: BTreeMap<String, Uuid>,
    pub compatibility: PackageCompatibility,
    pub imported_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MappingLifecycleError<E> {
    InvalidDocument(MappingError),
    MappingNotFound,
    RevisionNotFound,
    WorkspaceNotFound,
    PackageDocumentSet,
    RevisionOverflow,
    HeadMoved {
        current_head: String,
        diff: CanonicalRowSetDiff,
    },
    LineageCycle,
    Repository(E),
}

pub trait MappingLifecycleRepository: Send + Sync {
    type Error;

    fn get_head(
        &self,
        mapping_id: Uuid,
    ) -> MappingLifecycleFuture<'_, Option<MappingRevisionSnapshot>, Self::Error>;

    fn get_revision(
        &self,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> MappingLifecycleFuture<'_, Option<MappingRevisionSnapshot>, Self::Error>;

    fn create_mapping(
        &self,
        command: StoreMappingRevision,
    ) -> MappingLifecycleFuture<'_, MappingRevisionSnapshot, Self::Error>;

    fn append_revision(
        &self,
        command: AppendMappingRevision,
    ) -> MappingLifecycleFuture<'_, MappingRevisionSnapshot, Self::Error>;

    fn import_package(
        &self,
        revisions: Vec<StoreMappingRevision>,
    ) -> MappingLifecycleFuture<'_, Vec<MappingRevisionSnapshot>, Self::Error>;

    fn change_activation(
        &self,
        command: ChangeMappingActivation,
    ) -> MappingLifecycleFuture<'_, WorkspaceMappingActivation, Self::Error>;
}

#[derive(Clone)]
pub struct MappingLifecycleService<R> {
    repository: R,
}

impl<R> MappingLifecycleService<R>
where
    R: MappingLifecycleRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn head(
        &self,
        mapping_id: Uuid,
    ) -> Result<MappingRevisionSnapshot, MappingLifecycleError<R::Error>> {
        self.repository
            .get_head(mapping_id)
            .await?
            .ok_or(MappingLifecycleError::MappingNotFound)
    }

    pub async fn revision(
        &self,
        mapping_id: Uuid,
        content_hash: &str,
    ) -> Result<MappingRevisionSnapshot, MappingLifecycleError<R::Error>> {
        validate_content_hash(content_hash).map_err(MappingLifecycleError::InvalidDocument)?;
        self.repository
            .get_revision(mapping_id, content_hash)
            .await?
            .ok_or(MappingLifecycleError::RevisionNotFound)
    }

    pub async fn create(
        &self,
        command: CreateMapping,
    ) -> Result<MappingRevisionSnapshot, MappingLifecycleError<R::Error>> {
        let document = MappingDocument::from_mappings(1, command.mappings)
            .map_err(MappingLifecycleError::InvalidDocument)?;
        self.repository
            .create_mapping(StoreMappingRevision {
                mapping_id: command.mapping_id,
                document,
                parents: Vec::new(),
                origin: MappingRevisionOrigin::Created,
                package: None,
                created_at: command.created_at,
            })
            .await
    }

    pub async fn update(
        &self,
        command: UpdateMapping,
    ) -> Result<MappingRevisionSnapshot, MappingLifecycleError<R::Error>> {
        validate_content_hash(&command.base_content_hash)
            .map_err(MappingLifecycleError::InvalidDocument)?;
        let draft = MappingDocument::from_mappings(1, command.mappings.clone())
            .map_err(MappingLifecycleError::InvalidDocument)?;
        let current = self
            .repository
            .get_head(command.mapping_id)
            .await?
            .ok_or(MappingLifecycleError::MappingNotFound)?;
        if current.content_hash != command.base_content_hash {
            return Err(MappingLifecycleError::HeadMoved {
                current_head: current.content_hash,
                diff: canonical_row_set_diff(&current.document, &draft),
            });
        }
        let revision = current
            .document
            .revision()
            .checked_add(1)
            .ok_or(MappingLifecycleError::RevisionOverflow)?;
        let document = MappingDocument::from_mappings(revision, command.mappings)
            .map_err(MappingLifecycleError::InvalidDocument)?;
        let parents = normalize_parents(
            vec![command.base_content_hash.clone()],
            &document.content_hash(),
            true,
        )?;
        self.repository
            .append_revision(AppendMappingRevision {
                expected_head: command.base_content_hash,
                revision: StoreMappingRevision {
                    mapping_id: command.mapping_id,
                    document,
                    parents,
                    origin: MappingRevisionOrigin::Updated,
                    package: None,
                    created_at: command.updated_at,
                },
            })
            .await
    }

    pub async fn import(
        &self,
        command: ImportMapping,
    ) -> Result<MappingRevisionSnapshot, MappingLifecycleError<R::Error>> {
        let parents = normalize_parents(command.parents, &command.document.content_hash(), false)?;
        self.repository
            .create_mapping(StoreMappingRevision {
                mapping_id: command.mapping_id,
                document: command.document,
                parents,
                origin: MappingRevisionOrigin::Imported,
                package: None,
                created_at: command.imported_at,
            })
            .await
    }

    pub async fn fork(
        &self,
        command: ForkMapping,
    ) -> Result<MappingRevisionSnapshot, MappingLifecycleError<R::Error>> {
        validate_content_hash(&command.selected_content_hash)
            .map_err(MappingLifecycleError::InvalidDocument)?;
        let selected = self
            .repository
            .get_revision(command.source_mapping_id, &command.selected_content_hash)
            .await?
            .ok_or(MappingLifecycleError::RevisionNotFound)?;
        let revision = selected
            .document
            .revision()
            .checked_add(1)
            .ok_or(MappingLifecycleError::RevisionOverflow)?;
        let document = MappingDocument::from_mappings(revision, command.mappings)
            .map_err(MappingLifecycleError::InvalidDocument)?;
        let parents = normalize_parents(
            vec![command.selected_content_hash],
            &document.content_hash(),
            true,
        )?;
        self.repository
            .create_mapping(StoreMappingRevision {
                mapping_id: command.new_mapping_id,
                document,
                parents,
                origin: MappingRevisionOrigin::Forked,
                package: None,
                created_at: command.forked_at,
            })
            .await
    }

    pub async fn import_package(
        &self,
        command: ImportMappingPackage,
    ) -> Result<Vec<MappingRevisionSnapshot>, MappingLifecycleError<R::Error>> {
        command
            .package
            .verify(&command.documents, &command.compatibility)
            .map_err(MappingLifecycleError::InvalidDocument)?;
        if command.mapping_ids.len() != command.documents.len()
            || command
                .documents
                .keys()
                .any(|path| !command.mapping_ids.contains_key(path))
        {
            return Err(MappingLifecycleError::PackageDocumentSet);
        }

        let packaged_hashes = command
            .documents
            .values()
            .map(MappingDocument::content_hash)
            .collect::<BTreeSet<_>>();
        let metadata = |path: &str| PackageImportMetadata {
            package_id: command.package_id,
            document_path: path.to_owned(),
            roots: command.package.roots().to_vec(),
            license_expression: command.package.license_expression().to_owned(),
            attribution: command
                .package
                .attribution()
                .iter()
                .map(|entry| entry.name.clone())
                .collect(),
            signatures: command
                .package
                .signatures()
                .iter()
                .map(|signature| (signature.kind.clone(), signature.digest.clone()))
                .collect(),
            forge_commit: command.package.forge().map(|forge| forge.commit.clone()),
            required_mapping_schema: command.package.requirements().mapping_schema.clone(),
            required_resolver: command.package.requirements().resolver.clone(),
            required_coordinate_registry_digest: command
                .package
                .requirements()
                .coordinate_registry_digest
                .clone(),
        };

        let mut revisions = Vec::with_capacity(command.documents.len());
        for (path, document) in command.documents {
            let content_hash = document.content_hash();
            let parents = command
                .package
                .parents()
                .get(&content_hash)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|parent| MappingRevisionParent {
                    verified: packaged_hashes.contains(&parent),
                    content_hash: parent,
                })
                .collect();
            revisions.push(StoreMappingRevision {
                mapping_id: *command
                    .mapping_ids
                    .get(&path)
                    .expect("package document set was validated"),
                document,
                parents,
                origin: MappingRevisionOrigin::Imported,
                package: Some(metadata(&path)),
                created_at: command.imported_at,
            });
        }
        self.repository.import_package(revisions).await
    }

    pub async fn change_activation(
        &self,
        command: ChangeMappingActivation,
    ) -> Result<WorkspaceMappingActivation, MappingLifecycleError<R::Error>> {
        match command.kind {
            MappingActivationKind::Activate | MappingActivationKind::Rollback => {
                let content_hash = command
                    .content_hash
                    .as_deref()
                    .ok_or(MappingLifecycleError::RevisionNotFound)?;
                validate_content_hash(content_hash)
                    .map_err(MappingLifecycleError::InvalidDocument)?;
            }
            MappingActivationKind::Retire if command.content_hash.is_some() => {
                return Err(MappingLifecycleError::RevisionNotFound);
            }
            MappingActivationKind::Retire => {}
        }
        self.repository.change_activation(command).await
    }
}

#[must_use]
pub fn canonical_row_set_diff(
    current: &MappingDocument,
    proposed: &MappingDocument,
) -> CanonicalRowSetDiff {
    let current = canonical_rows(current);
    let proposed = canonical_rows(proposed);
    CanonicalRowSetDiff {
        added: proposed.difference(&current).cloned().collect(),
        removed: current.difference(&proposed).cloned().collect(),
    }
}

fn canonical_rows(document: &MappingDocument) -> BTreeSet<Vec<String>> {
    document
        .mappings()
        .iter()
        .map(|row| row.iter().map(ToString::to_string).collect())
        .collect()
}

fn normalize_parents<E>(
    parent_hashes: Vec<String>,
    child_hash: &str,
    verified: bool,
) -> Result<Vec<MappingRevisionParent>, MappingLifecycleError<E>> {
    let mut parents = BTreeSet::new();
    for parent in parent_hashes {
        validate_content_hash(&parent).map_err(MappingLifecycleError::InvalidDocument)?;
        if parent == child_hash {
            return Err(MappingLifecycleError::LineageCycle);
        }
        parents.insert(parent);
    }
    Ok(parents
        .into_iter()
        .map(|content_hash| MappingRevisionParent {
            content_hash,
            verified,
        })
        .collect())
}

#[cfg(test)]
#[path = "mapping_lifecycle_tests.rs"]
mod tests;
