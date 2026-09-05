use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
};

use crate::application::{
    error::{ApplicationError, ApplicationErrorCode},
    providers::{AvailableProviderSnapshot, ProviderSourceSnapshot},
};
use oci_client::{
    Client, Reference,
    client::{ClientConfig, ClientProtocol},
    manifest::{
        OCI_IMAGE_INDEX_MEDIA_TYPE, OCI_IMAGE_MEDIA_TYPE, OciDescriptor, OciImageIndex,
        OciImageManifest, OciManifest,
    },
    secrets::RegistryAuth,
};
use semver::{Version, VersionReq};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

const PROVIDER_ARTIFACT_TYPE: &str = "application/vnd.trakkin.provider.v1";
const PROVIDER_BINARY_MEDIA_TYPE: &str = "application/vnd.trakkin.provider.binary.v1";
const PROVIDER_METADATA_MEDIA_TYPE: &str = "application/vnd.trakkin.provider.metadata.v1+json";
const PROVIDER_SCHEMA: &str = "trakkin.provider/v1";
const MAXIMUM_INDEX_BYTES: usize = 1024 * 1024;
const MAXIMUM_MANIFEST_BYTES: usize = 1024 * 1024;
const MAXIMUM_METADATA_BYTES: usize = 64 * 1024;
const MAXIMUM_BINARY_BYTES: usize = 256 * 1024 * 1024;
const MAXIMUM_TAGS: usize = 10_000;

pub(crate) type ProviderArtifactFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PulledProviderArtifact, ApplicationError>> + Send + 'a>>;
pub(crate) type ProviderReleasesFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Vec<AvailableProviderSnapshot>, ApplicationError>> + Send + 'a>,
>;

pub(crate) trait ProviderArtifactSource: Send + Sync {
    fn list_releases<'a>(
        &'a self,
        source: &'a ProviderSourceSnapshot,
    ) -> ProviderReleasesFuture<'a>;
    fn pull<'a>(
        &'a self,
        source: &'a ProviderSourceSnapshot,
        provider_id: &'a str,
        version: &'a Version,
        platform: &'a str,
        host_version: &'a Version,
    ) -> ProviderArtifactFuture<'a>;
}

#[derive(Clone)]
pub(crate) struct OciProviderSource {
    https_client: Arc<Client>,
    http_client: Arc<Client>,
}

#[derive(Clone, Copy)]
enum RepositoryProtocol {
    Https,
    Http,
}

struct ParsedRepository {
    protocol: RepositoryProtocol,
    reference: String,
}

impl Default for OciProviderSource {
    fn default() -> Self {
        let http_config = ClientConfig {
            protocol: ClientProtocol::Http,
            ..Default::default()
        };
        Self {
            https_client: Arc::new(Client::new(Default::default())),
            http_client: Arc::new(Client::new(http_config)),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PulledProviderArtifact {
    pub metadata: ProviderMetadata,
    pub metadata_value: Value,
    pub binary: Vec<u8>,
    pub index_digest: String,
    pub manifest_digest: String,
    pub metadata_digest: String,
    pub binary_digest: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ProviderMetadata {
    pub schema: String,
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub version: Version,
    pub platforms: Vec<String>,
    #[serde(default)]
    pub requires: ProviderRequirements,
    #[serde(flatten)]
    _additional: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ProviderRequirements {
    pub trakkin: Option<VersionReq>,
    #[serde(flatten)]
    _additional: BTreeMap<String, Value>,
}

impl ProviderMetadata {
    pub fn validate(
        &self,
        expected_provider_id: &str,
        expected_version: &Version,
        platform: &str,
        host_version: &Version,
    ) -> Result<(), ApplicationError> {
        if self.schema != PROVIDER_SCHEMA {
            return Err(invalid_provider(
                "provider metadata uses an unsupported schema",
            ));
        }
        if !valid_provider_id(&self.id) {
            return Err(invalid_provider("provider metadata contains an invalid ID"));
        }
        if self.id != expected_provider_id {
            return Err(invalid_provider(
                "provider metadata identity does not match the requested provider",
            ));
        }
        if [&self.name, &self.description]
            .into_iter()
            .flatten()
            .any(|value| value.is_empty() || value.contains(['\r', '\n']))
        {
            return Err(invalid_provider(
                "provider metadata contains invalid display text",
            ));
        }
        if &self.version != expected_version {
            return Err(invalid_provider(
                "provider metadata version does not match the requested release",
            ));
        }
        if self.platforms.is_empty()
            || self.platforms.iter().collect::<HashSet<_>>().len() != self.platforms.len()
            || self
                .platforms
                .iter()
                .any(|candidate| !matches!(candidate.as_str(), "linux/amd64" | "linux/arm64"))
            || !self.platforms.iter().any(|candidate| candidate == platform)
        {
            return Err(invalid_provider(
                "provider metadata does not declare the selected platform",
            ));
        }
        if self
            .requires
            .trakkin
            .as_ref()
            .is_some_and(|requirement| !requirement.matches(host_version))
        {
            return Err(ApplicationError::new(
                ApplicationErrorCode::InvalidRequest,
                "provider release is incompatible with this Trakkin version",
            ));
        }
        Ok(())
    }
}

impl ProviderArtifactSource for OciProviderSource {
    fn list_releases<'a>(
        &'a self,
        source: &'a ProviderSourceSnapshot,
    ) -> ProviderReleasesFuture<'a> {
        Box::pin(async move {
            let repository = parse_repository_url(&source.repository_url)?;
            let reference = format!("{}:latest", repository.reference)
                .parse::<Reference>()
                .map_err(invalid_provider)?;
            let response = self
                .client(repository.protocol)
                .list_tags(&reference, &RegistryAuth::Anonymous, None, None)
                .await
                .map_err(provider_source_error)?;
            if response.tags.len() > MAXIMUM_TAGS {
                return Err(invalid_provider(
                    "provider OCI repository contains too many tags",
                ));
            }
            Ok(available_providers(&response.tags))
        })
    }

    fn pull<'a>(
        &'a self,
        source: &'a ProviderSourceSnapshot,
        provider_id: &'a str,
        version: &'a Version,
        platform: &'a str,
        host_version: &'a Version,
    ) -> ProviderArtifactFuture<'a> {
        Box::pin(async move {
            self.pull_artifact(source, provider_id, version, platform, host_version)
                .await
        })
    }
}

impl OciProviderSource {
    fn client(&self, protocol: RepositoryProtocol) -> &Client {
        match protocol {
            RepositoryProtocol::Https => &self.https_client,
            RepositoryProtocol::Http => &self.http_client,
        }
    }

    async fn pull_artifact(
        &self,
        source: &ProviderSourceSnapshot,
        provider_id: &str,
        version: &Version,
        platform: &str,
        host_version: &Version,
    ) -> Result<PulledProviderArtifact, ApplicationError> {
        let repository = parse_repository_url(&source.repository_url)?;
        let tag = provider_release_tag(provider_id, version)?;
        let reference = format!("{}:{tag}", repository.reference)
            .parse::<Reference>()
            .map_err(invalid_provider)?;
        let (index_bytes, index_digest) = self
            .client(repository.protocol)
            .pull_manifest_raw(
                &reference,
                &RegistryAuth::Anonymous,
                &[OCI_IMAGE_INDEX_MEDIA_TYPE],
            )
            .await
            .map_err(provider_source_error)?;
        ensure_bounded(index_bytes.len(), MAXIMUM_INDEX_BYTES, "provider OCI index")?;
        verify_digest(&index_bytes, &index_digest, "provider OCI index")?;
        let index =
            match serde_json::from_slice::<OciManifest>(&index_bytes).map_err(invalid_provider)? {
                OciManifest::ImageIndex(index) => index,
                OciManifest::Image(_) => {
                    return Err(invalid_provider("provider release tag is not an OCI index"));
                }
            };
        validate_index(&index)?;
        let descriptor = select_platform_manifest(&index, platform)?;
        let manifest_reference = reference.clone_with_digest(descriptor.digest.clone());
        let (manifest_bytes, manifest_digest) = self
            .client(repository.protocol)
            .pull_manifest_raw(
                &manifest_reference,
                &RegistryAuth::Anonymous,
                &[OCI_IMAGE_MEDIA_TYPE],
            )
            .await
            .map_err(provider_source_error)?;
        ensure_bounded(
            manifest_bytes.len(),
            MAXIMUM_MANIFEST_BYTES,
            "provider OCI manifest",
        )?;
        verify_descriptor_bytes(
            &manifest_bytes,
            &descriptor.digest,
            descriptor.size,
            "provider OCI manifest",
        )?;
        if manifest_digest != descriptor.digest {
            return Err(invalid_provider(
                "provider OCI manifest digest does not match its index descriptor",
            ));
        }
        let manifest = match serde_json::from_slice::<OciManifest>(&manifest_bytes)
            .map_err(invalid_provider)?
        {
            OciManifest::Image(manifest) => manifest,
            OciManifest::ImageIndex(_) => {
                return Err(invalid_provider(
                    "provider platform descriptor points to another OCI index",
                ));
            }
        };
        let (metadata_descriptor, binary_descriptor) = validate_manifest(&manifest)?;
        let metadata_bytes = self
            .pull_blob(
                self.client(repository.protocol),
                &manifest_reference,
                metadata_descriptor,
                MAXIMUM_METADATA_BYTES,
                "provider metadata",
            )
            .await?;
        let binary = self
            .pull_blob(
                self.client(repository.protocol),
                &manifest_reference,
                binary_descriptor,
                MAXIMUM_BINARY_BYTES,
                "provider binary",
            )
            .await?;
        let metadata_value =
            serde_json::from_slice::<Value>(&metadata_bytes).map_err(invalid_provider)?;
        let metadata = serde_json::from_value::<ProviderMetadata>(metadata_value.clone())
            .map_err(invalid_provider)?;
        metadata.validate(provider_id, version, platform, host_version)?;

        Ok(PulledProviderArtifact {
            metadata,
            metadata_value,
            binary,
            index_digest,
            manifest_digest,
            metadata_digest: metadata_descriptor.digest.clone(),
            binary_digest: binary_descriptor.digest.clone(),
        })
    }

    async fn pull_blob(
        &self,
        client: &Client,
        reference: &Reference,
        descriptor: &OciDescriptor,
        maximum_bytes: usize,
        label: &'static str,
    ) -> Result<Vec<u8>, ApplicationError> {
        let expected_size = descriptor_size(descriptor.size, maximum_bytes, label)?;
        let mut bytes = Vec::with_capacity(expected_size);
        client
            .pull_blob(reference, descriptor, &mut bytes)
            .await
            .map_err(provider_source_error)?;
        verify_descriptor_bytes(&bytes, &descriptor.digest, descriptor.size, label)?;
        Ok(bytes)
    }
}

fn validate_index(index: &OciImageIndex) -> Result<(), ApplicationError> {
    if index.schema_version != 2
        || index.media_type.as_deref() != Some(OCI_IMAGE_INDEX_MEDIA_TYPE)
        || index.artifact_type.as_deref() != Some(PROVIDER_ARTIFACT_TYPE)
        || index.manifests.is_empty()
    {
        return Err(invalid_provider("provider OCI index has an invalid shape"));
    }
    Ok(())
}

fn select_platform_manifest<'a>(
    index: &'a OciImageIndex,
    platform: &str,
) -> Result<&'a oci_client::manifest::ImageIndexEntry, ApplicationError> {
    let Some((os, architecture)) = platform.split_once('/') else {
        return Err(invalid_provider("host OCI platform is invalid"));
    };
    let matches = index
        .manifests
        .iter()
        .filter(|descriptor| {
            descriptor.media_type == OCI_IMAGE_MEDIA_TYPE
                && descriptor.platform.as_ref().is_some_and(|candidate| {
                    candidate.os.to_string() == os
                        && candidate.architecture.to_string() == architecture
                        && candidate.variant.is_none()
                })
        })
        .collect::<Vec<_>>();
    let [descriptor] = matches.as_slice() else {
        return Err(ApplicationError::new(
            ApplicationErrorCode::ProviderNotFound,
            "provider release does not contain exactly one artifact for this platform",
        ));
    };
    descriptor_size(
        descriptor.size,
        MAXIMUM_MANIFEST_BYTES,
        "provider OCI manifest",
    )?;
    Ok(descriptor)
}

fn validate_manifest(
    manifest: &OciImageManifest,
) -> Result<(&OciDescriptor, &OciDescriptor), ApplicationError> {
    if manifest.schema_version != 2
        || manifest.media_type.as_deref() != Some(OCI_IMAGE_MEDIA_TYPE)
        || manifest.artifact_type.as_deref() != Some(PROVIDER_ARTIFACT_TYPE)
        || manifest.subject.is_some()
        || manifest.layers.len() != 2
    {
        return Err(invalid_provider(
            "provider platform OCI manifest has an invalid shape",
        ));
    }
    let metadata = manifest
        .layers
        .iter()
        .filter(|layer| layer.media_type == PROVIDER_METADATA_MEDIA_TYPE)
        .collect::<Vec<_>>();
    let binary = manifest
        .layers
        .iter()
        .filter(|layer| layer.media_type == PROVIDER_BINARY_MEDIA_TYPE)
        .collect::<Vec<_>>();
    let ([metadata], [binary]) = (metadata.as_slice(), binary.as_slice()) else {
        return Err(invalid_provider(
            "provider platform OCI manifest must contain one metadata and one binary layer",
        ));
    };
    descriptor_size(metadata.size, MAXIMUM_METADATA_BYTES, "provider metadata")?;
    descriptor_size(binary.size, MAXIMUM_BINARY_BYTES, "provider binary")?;
    Ok((metadata, binary))
}

fn descriptor_size(
    size: i64,
    maximum: usize,
    label: &'static str,
) -> Result<usize, ApplicationError> {
    let size = usize::try_from(size)
        .ok()
        .filter(|size| *size > 0 && *size <= maximum)
        .ok_or_else(|| invalid_provider(format!("{label} size is outside the supported limit")))?;
    Ok(size)
}

fn ensure_bounded(
    size: usize,
    maximum: usize,
    label: &'static str,
) -> Result<(), ApplicationError> {
    if size == 0 || size > maximum {
        return Err(invalid_provider(format!(
            "{label} size is outside the supported limit"
        )));
    }
    Ok(())
}

fn verify_descriptor_bytes(
    bytes: &[u8],
    digest: &str,
    size: i64,
    label: &'static str,
) -> Result<(), ApplicationError> {
    if usize::try_from(size).ok() != Some(bytes.len()) {
        return Err(invalid_provider(format!(
            "{label} size does not match its OCI descriptor"
        )));
    }
    verify_digest(bytes, digest, label)
}

fn verify_digest(bytes: &[u8], digest: &str, label: &'static str) -> Result<(), ApplicationError> {
    let expected = digest
        .strip_prefix("sha256:")
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| invalid_provider(format!("{label} uses an unsupported digest")))?;
    if hex::encode(Sha256::digest(bytes)) != expected {
        return Err(invalid_provider(format!(
            "{label} does not match its OCI digest"
        )));
    }
    Ok(())
}

pub(crate) fn valid_repository_url(value: &str) -> bool {
    parse_repository_url(value).is_ok()
}

fn parse_repository_url(value: &str) -> Result<ParsedRepository, ApplicationError> {
    let url =
        Url::parse(value).map_err(|_| invalid_provider("provider repository URL is invalid"))?;
    let protocol = match url.scheme() {
        "https" => RepositoryProtocol::Https,
        "http" => RepositoryProtocol::Http,
        _ => {
            return Err(invalid_provider(
                "provider repository URL must use HTTP or HTTPS",
            ));
        }
    };
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || value.contains([' ', '\r', '\n', '%'])
    {
        return Err(invalid_provider(
            "provider repository URL must not contain credentials, escaping, a query, or a fragment",
        ));
    }
    let host = url
        .host_str()
        .filter(|host| {
            !host.is_empty()
                && host.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'-')
                })
        })
        .ok_or_else(|| invalid_provider("provider repository URL has an invalid registry host"))?;
    if matches!(protocol, RepositoryProtocol::Http) && !is_loopback_host(host) {
        return Err(invalid_provider(
            "HTTP provider repository URLs are restricted to loopback hosts",
        ));
    }
    let repository = url
        .path()
        .strip_prefix('/')
        .filter(|path| !path.is_empty() && !path.ends_with('/'))
        .ok_or_else(|| {
            invalid_provider("provider repository URL must include a repository path")
        })?;
    if !repository.split('/').all(|component| {
        !component.is_empty()
            && component.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            })
    }) {
        return Err(invalid_provider(
            "provider repository URL has an invalid repository path",
        ));
    }
    let registry = url
        .port()
        .map_or_else(|| host.to_owned(), |port| format!("{host}:{port}"));
    let reference = format!("{registry}/{repository}");
    if value != format!("{}://{reference}", url.scheme()) {
        return Err(invalid_provider(
            "provider repository URL must use its canonical form",
        ));
    }
    Ok(ParsedRepository {
        protocol,
        reference,
    })
}

fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn available_providers(tags: &[String]) -> Vec<AvailableProviderSnapshot> {
    let mut releases = BTreeMap::<String, BTreeSet<Version>>::new();
    for tag in tags {
        if let Some((provider_id, version)) = parse_provider_release_tag(tag) {
            releases.entry(provider_id).or_default().insert(version);
        }
    }
    releases
        .into_iter()
        .map(|(provider_id, versions)| AvailableProviderSnapshot {
            provider_id,
            versions: versions
                .into_iter()
                .rev()
                .map(|version| version.to_string())
                .collect(),
        })
        .collect()
}

fn parse_provider_release_tag(tag: &str) -> Option<(String, Version)> {
    let (provider_id, version) = tag.rsplit_once("-v")?;
    let version = Version::parse(version).ok()?;
    if !valid_provider_id(provider_id) || !version.build.is_empty() {
        return None;
    }
    Some((provider_id.to_owned(), version))
}

fn provider_release_tag(provider_id: &str, version: &Version) -> Result<String, ApplicationError> {
    if !valid_provider_id(provider_id) || !version.build.is_empty() {
        return Err(invalid_provider("provider release reference is invalid"));
    }
    let tag = format!("{provider_id}-v{version}");
    if tag.len() > 128 {
        return Err(invalid_provider("provider release tag is too long"));
    }
    Ok(tag)
}

fn valid_provider_id(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
        && value
            .split(['.', '-'])
            .all(|component| !component.is_empty())
}

fn provider_source_error(error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::new(
        ApplicationErrorCode::ProviderUnavailable,
        format!("provider registry request failed: {error}"),
    )
}

fn invalid_provider(error: impl std::fmt::Display) -> ApplicationError {
    ApplicationError::new(ApplicationErrorCode::InvalidProviderData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_requires_identity_platform_and_compatible_host() {
        let metadata = ProviderMetadata {
            schema: PROVIDER_SCHEMA.to_owned(),
            id: "dev.trakkin.fixture".to_owned(),
            name: Some("Fixture".to_owned()),
            description: None,
            version: Version::new(1, 2, 3),
            platforms: vec!["linux/amd64".to_owned()],
            requires: ProviderRequirements {
                trakkin: Some(VersionReq::parse(">=0.1.0, <1.0.0").expect("requirement")),
                _additional: BTreeMap::new(),
            },
            _additional: BTreeMap::new(),
        };

        metadata
            .validate(
                "dev.trakkin.fixture",
                &Version::new(1, 2, 3),
                "linux/amd64",
                &Version::new(0, 2, 0),
            )
            .expect("metadata is compatible");
        assert!(
            metadata
                .validate(
                    "dev.trakkin.fixture",
                    &Version::new(1, 2, 3),
                    "linux/arm64",
                    &Version::new(0, 2, 0),
                )
                .is_err()
        );
        assert!(
            metadata
                .validate(
                    "dev.trakkin.fixture",
                    &Version::new(1, 2, 3),
                    "linux/amd64",
                    &Version::new(1, 0, 0),
                )
                .is_err()
        );
    }

    #[test]
    fn provider_ids_reject_empty_components() {
        assert!(valid_provider_id("dev.trakkin.fixture"));
        assert!(!valid_provider_id("dev..fixture"));
        assert!(!valid_provider_id("Dev.trakkin.fixture"));
    }

    #[test]
    fn repository_urls_include_the_registry_protocol() {
        assert!(valid_repository_url(
            "https://registry.example/trakkin/providers"
        ));
        assert!(valid_repository_url(
            "http://127.0.0.1:5000/trakkin-dev/providers"
        ));
        assert!(valid_repository_url(
            "http://localhost:5000/trakkin-dev/providers"
        ));
        assert!(!valid_repository_url(
            "http://registry.example/trakkin/providers"
        ));
        assert!(!valid_repository_url("registry.example/trakkin/providers"));
        assert!(!valid_repository_url(
            "ftp://registry.example/trakkin/providers"
        ));
        assert!(!valid_repository_url(
            "https://registry.example/trakkin/providers:latest"
        ));
    }

    #[test]
    fn release_tags_form_a_multi_provider_catalog() {
        let releases = available_providers(&[
            "dev.trakkin.fixture-v1.0.0".to_owned(),
            "dev.trakkin.calendar-v2.0.0-beta.1".to_owned(),
            "dev.trakkin.fixture-v1.2.0".to_owned(),
            "latest".to_owned(),
        ]);
        assert_eq!(
            releases,
            vec![
                AvailableProviderSnapshot {
                    provider_id: "dev.trakkin.calendar".to_owned(),
                    versions: vec!["2.0.0-beta.1".to_owned()],
                },
                AvailableProviderSnapshot {
                    provider_id: "dev.trakkin.fixture".to_owned(),
                    versions: vec!["1.2.0".to_owned(), "1.0.0".to_owned()],
                },
            ]
        );
    }
}
