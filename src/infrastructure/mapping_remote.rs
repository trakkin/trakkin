use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};

use futures_util::TryStreamExt;
use reqwest::{
    StatusCode, Url,
    header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED},
    redirect::Policy,
};
use thiserror::Error;
use tokio::net::lookup_host;
use tokio_util::io::StreamReader;

use crate::application::mappings::MappingContentReader;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteNetworkPolicy {
    PublicHttps,
    AllowPrivateHttp,
}

#[derive(Clone)]
pub(super) struct RemoteSourceFetcher {
    policy: RemoteNetworkPolicy,
}

impl RemoteSourceFetcher {
    pub fn new(policy: RemoteNetworkPolicy) -> Self {
        Self { policy }
    }

    pub fn canonical_url(&self, value: &str) -> Result<String, RemoteSourceFetchError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(RemoteSourceFetchError::InvalidUrl("remote URL is required"));
        }
        let mut url = Url::parse(value)
            .map_err(|_| RemoteSourceFetchError::InvalidUrl("remote URL is invalid"))?;
        if url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(RemoteSourceFetchError::InvalidUrl(
                "remote URL must have a host and cannot contain credentials or a fragment",
            ));
        }
        match (self.policy, url.scheme()) {
            (RemoteNetworkPolicy::PublicHttps, "https")
            | (RemoteNetworkPolicy::AllowPrivateHttp, "http" | "https") => {}
            (RemoteNetworkPolicy::PublicHttps, _) => {
                return Err(RemoteSourceFetchError::InvalidUrl(
                    "remote URL must use HTTPS",
                ));
            }
            (RemoteNetworkPolicy::AllowPrivateHttp, _) => {
                return Err(RemoteSourceFetchError::InvalidUrl(
                    "remote URL must use HTTP or HTTPS",
                ));
            }
        }
        let default_port = match url.scheme() {
            "http" => Some(80),
            "https" => Some(443),
            _ => None,
        };
        if url.port() == default_port {
            url.set_port(None)
                .map_err(|()| RemoteSourceFetchError::InvalidUrl("remote URL port is invalid"))?;
        }
        Ok(url.to_string())
    }

    pub async fn download(
        &self,
        remote_url: &str,
        http_etag: Option<&str>,
        http_last_modified: Option<&str>,
    ) -> Result<RemoteDownload, RemoteSourceFetchError> {
        let url = Url::parse(remote_url)
            .map_err(|_| RemoteSourceFetchError::InvalidUrl("stored remote URL is invalid"))?;
        let host = url.host_str().ok_or(RemoteSourceFetchError::InvalidUrl(
            "stored remote URL has no host",
        ))?;
        let port = url
            .port_or_known_default()
            .ok_or(RemoteSourceFetchError::InvalidUrl(
                "stored remote URL has no usable port",
            ))?;
        let addresses = lookup_host((host, port))
            .await
            .map_err(|error| RemoteSourceFetchError::Resolve(error.to_string()))?
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(RemoteSourceFetchError::Resolve(
                "remote host resolved to no addresses".to_owned(),
            ));
        }
        if self.policy == RemoteNetworkPolicy::PublicHttps
            && addresses
                .iter()
                .any(|address| !is_public_address(address.ip()))
        {
            return Err(RemoteSourceFetchError::AddressNotAllowed);
        }

        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .resolve_to_addrs(host, &addresses)
            .build()
            .map_err(|error| RemoteSourceFetchError::Client(error.to_string()))?;
        let mut request = client.get(url);
        if let Some(etag) = http_etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = http_last_modified {
            request = request.header(IF_MODIFIED_SINCE, last_modified);
        }
        let response = request
            .send()
            .await
            .map_err(|error| RemoteSourceFetchError::Request(error.to_string()))?;
        let validators = RemoteValidators {
            etag: response
                .headers()
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            last_modified: response
                .headers()
                .get(LAST_MODIFIED)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        };
        if response.status() == StatusCode::NOT_MODIFIED {
            return Ok(RemoteDownload::NotModified(validators));
        }
        if !response.status().is_success() {
            return Err(RemoteSourceFetchError::Status(response.status().as_u16()));
        }
        let stream = response
            .bytes_stream()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::ConnectionAborted, error));
        Ok(RemoteDownload::Modified(RemoteSourceDownload {
            reader: Box::pin(StreamReader::new(stream)),
            etag: validators.etag,
            last_modified: validators.last_modified,
        }))
    }
}

pub(super) struct RemoteValidators {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub(super) enum RemoteDownload {
    NotModified(RemoteValidators),
    Modified(RemoteSourceDownload),
}

pub(super) struct RemoteSourceDownload {
    pub reader: MappingContentReader,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Error)]
pub(super) enum RemoteSourceFetchError {
    #[error("{0}")]
    InvalidUrl(&'static str),
    #[error("remote mapping source resolved to a non-public network address")]
    AddressNotAllowed,
    #[error("remote mapping source host could not be resolved: {0}")]
    Resolve(String),
    #[error("remote mapping source client could not be created: {0}")]
    Client(String),
    #[error("remote mapping source could not be downloaded: {0}")]
    Request(String),
    #[error("remote mapping source returned HTTP {0}")]
    Status(u16),
}

impl RemoteSourceFetchError {
    pub fn is_invalid_source(&self) -> bool {
        matches!(self, Self::InvalidUrl(_) | Self::AddressNotAllowed)
    }
}

fn is_public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [first, second, ..] = address.octets();
    !(address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_broadcast()
        || address.is_documentation()
        || address.is_unspecified()
        || address.is_multicast()
        || first == 0
        || (first == 100 && (64..=127).contains(&second))
        || (first == 198 && (18..=19).contains(&second))
        || first >= 240)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    if let Some(address) = address.to_ipv4_mapped() {
        return is_public_ipv4(address);
    }
    let segments = address.segments();
    !(address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] == 0x2001 && segments[1] == 0x0db8))
}

#[cfg(test)]
mod tests {
    use super::{RemoteNetworkPolicy, RemoteSourceFetchError, RemoteSourceFetcher};

    #[test]
    fn public_remote_urls_are_canonical_https_origins_without_fragments() {
        let fetcher = RemoteSourceFetcher::new(RemoteNetworkPolicy::PublicHttps);
        assert_eq!(
            fetcher
                .canonical_url(" https://EXAMPLE.com:443/mappings.trakkin ")
                .unwrap(),
            "https://example.com/mappings.trakkin"
        );
        assert!(matches!(
            fetcher.canonical_url("http://example.com/mappings.trakkin"),
            Err(RemoteSourceFetchError::InvalidUrl(_))
        ));
        assert!(matches!(
            fetcher.canonical_url("https://example.com/mappings.trakkin#fragment"),
            Err(RemoteSourceFetchError::InvalidUrl(_))
        ));
    }

    #[tokio::test]
    async fn public_remote_fetches_reject_loopback_before_connecting() {
        let fetcher = RemoteSourceFetcher::new(RemoteNetworkPolicy::PublicHttps);
        let result = fetcher
            .download("https://127.0.0.1/mappings.trakkin", None, None)
            .await;
        assert!(matches!(
            result,
            Err(RemoteSourceFetchError::AddressNotAllowed)
        ));
    }
}
