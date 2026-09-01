use std::{
    env,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

use thiserror::Error;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use trakkin::{host::Host, http, infrastructure::connection::ConnectionMasterKey};

const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8725";
const DEFAULT_DATA_DIRECTORY: &str = ".trakkin";
const HEALTHCHECK_ARGUMENT: &str = "healthcheck";
const HEALTHCHECK_PATH: &str = "/api/v1/openapi.json";
const MASTER_KEY_FILE: &str = "master.key";

#[tokio::main]
async fn main() -> ExitCode {
    if env::args_os().nth(1).as_deref() == Some(OsStr::new(HEALTHCHECK_ARGUMENT)) {
        return match run_healthcheck() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("trakkin healthcheck failed: {error}");
                ExitCode::FAILURE
            }
        };
    }
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("trakkin failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_healthcheck() -> Result<(), io::Error> {
    let bind_address = env::var("TRAKKIN_BIND")
        .unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned())
        .parse::<SocketAddr>()
        .map_err(io::Error::other)?;
    let address = healthcheck_address(bind_address);
    let timeout = Duration::from_secs(3);
    let mut stream = TcpStream::connect_timeout(&address, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET {HEALTHCHECK_PATH} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = [0_u8; 32];
    let bytes_read = stream.read(&mut response)?;
    if healthy_http_status(&response[..bytes_read]) {
        Ok(())
    } else {
        Err(io::Error::other("HTTP endpoint did not return 200"))
    }
}

fn healthcheck_address(bind_address: SocketAddr) -> SocketAddr {
    let address = match bind_address.ip() {
        IpAddr::V4(address) if address.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(address) if address.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        address => address,
    };
    SocketAddr::new(address, bind_address.port())
}

fn healthy_http_status(response: &[u8]) -> bool {
    response.starts_with(b"HTTP/1.1 200 ") || response.starts_with(b"HTTP/1.0 200 ")
}

async fn run() -> Result<(), MainError> {
    let configuration = Configuration::load()?;
    let listener = TcpListener::bind(configuration.bind_address).await?;
    let address = listener.local_addr()?;
    let started_at = OffsetDateTime::now_utc();
    let host = match configuration.database_url.as_deref() {
        Some(database_url) => Host::connect(database_url, started_at).await?,
        None => start_embedded_host(&configuration.data_directory, started_at).await?,
    };
    let runtime = host
        .into_runtime(
            &configuration.data_directory,
            &configuration.provider_runtime_directory,
            configuration.connection_master_key,
        )
        .await?;
    let application = runtime.application();
    println!(
        "{}",
        serde_json::json!({ "status": "ready", "address": address.to_string() })
    );
    io::stdout().flush()?;
    let server = axum::serve(listener, http::router(application))
        .with_graceful_shutdown(shutdown_signal())
        .await;
    let shutdown = runtime.shutdown().await;
    server?;
    shutdown?;
    println!("trakkin stopped");
    Ok(())
}

#[cfg(feature = "embedded-database")]
async fn start_embedded_host(
    data_directory: &Path,
    started_at: OffsetDateTime,
) -> Result<Host, trakkin::host::HostError> {
    Host::start(data_directory, started_at).await
}

#[cfg(not(feature = "embedded-database"))]
async fn start_embedded_host(
    _data_directory: &Path,
    _started_at: OffsetDateTime,
) -> Result<Host, trakkin::host::HostError> {
    Err(trakkin::database::DatabaseError::EmbeddedDatabaseDisabled.into())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

struct Configuration {
    bind_address: SocketAddr,
    data_directory: PathBuf,
    database_url: Option<String>,
    provider_runtime_directory: PathBuf,
    connection_master_key: ConnectionMasterKey,
}

impl Configuration {
    fn load() -> Result<Self, ConfigurationError> {
        let bind_address = env::var("TRAKKIN_BIND")
            .unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned())
            .parse()
            .map_err(ConfigurationError::BindAddress)?;
        let data_directory = env::var_os("TRAKKIN_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
        let database_url = env::var("TRAKKIN_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let provider_runtime_directory = match env::var_os("TRAKKIN_PROVIDER_RUNTIME_DIR") {
            Some(path) => PathBuf::from(path),
            None => create_default_provider_runtime_directory()?,
        };
        let connection_master_key = env::var("TRAKKIN_MASTER_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map_or_else(
                || load_or_create_master_key(&data_directory.join(MASTER_KEY_FILE)),
                |value| decode_master_key(&value),
            )?;
        Ok(Self {
            bind_address,
            data_directory,
            database_url,
            provider_runtime_directory,
            connection_master_key,
        })
    }
}

fn create_default_provider_runtime_directory() -> Result<PathBuf, ConfigurationError> {
    for _ in 0..16 {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random)?;
        let path = env::temp_dir().join(format!("trakkin-provider-{}", hex::encode(random)));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;

            builder.mode(0o700);
        }
        match builder.create(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a private provider runtime directory",
    )
    .into())
}

fn decode_master_key(value: &str) -> Result<ConnectionMasterKey, ConfigurationError> {
    let bytes = hex::decode(value).map_err(ConfigurationError::MasterKeyEncoding)?;
    let bytes = <[u8; 32]>::try_from(bytes).map_err(|_| ConfigurationError::MasterKeyLength)?;
    Ok(ConnectionMasterKey::new(bytes))
}

fn load_or_create_master_key(path: &Path) -> Result<ConnectionMasterKey, ConfigurationError> {
    match OpenOptions::new().read(true).open(path) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            let bytes =
                <[u8; 32]>::try_from(bytes).map_err(|_| ConfigurationError::MasterKeyFileLength)?;
            Ok(ConnectionMasterKey::new(bytes))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(directory) = path.parent() {
                fs::create_dir_all(directory)?;
            }
            let mut bytes = [0_u8; 32];
            getrandom::fill(&mut bytes)?;
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(path)?;
            file.write_all(&bytes)?;
            Ok(ConnectionMasterKey::new(bytes))
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Error)]
enum MainError {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Host(#[from] trakkin::host::HostError),
    #[error("HTTP server failed: {0}")]
    Server(#[from] io::Error),
}

#[derive(Debug, Error)]
enum ConfigurationError {
    #[error("TRAKKIN_BIND is not a valid socket address: {0}")]
    BindAddress(#[source] std::net::AddrParseError),
    #[error("configuration I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("TRAKKIN_MASTER_KEY must be hexadecimal: {0}")]
    MasterKeyEncoding(#[source] hex::FromHexError),
    #[error("TRAKKIN_MASTER_KEY must decode to exactly 32 bytes")]
    MasterKeyLength,
    #[error("the persisted connection master key must contain exactly 32 bytes")]
    MasterKeyFileLength,
    #[error("connection master-key generation failed: {0}")]
    Random(#[from] getrandom::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_provider_runtime_directory_is_private_and_unique() {
        let first = create_default_provider_runtime_directory().expect("first runtime directory");
        let second = create_default_provider_runtime_directory().expect("second runtime directory");
        assert_ne!(first, second);
        assert_eq!(first.parent(), Some(env::temp_dir().as_path()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = fs::metadata(&first)
                .expect("runtime directory metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700);
        }
        fs::remove_dir(first).expect("first runtime directory is removed");
        fs::remove_dir(second).expect("second runtime directory is removed");
    }

    #[test]
    fn healthcheck_uses_loopback_for_unspecified_bind_addresses() {
        assert_eq!(
            healthcheck_address("0.0.0.0:8725".parse().expect("IPv4 bind address")),
            "127.0.0.1:8725".parse().expect("IPv4 health address")
        );
        assert_eq!(
            healthcheck_address("[::]:8725".parse().expect("IPv6 bind address")),
            "[::1]:8725".parse().expect("IPv6 health address")
        );
    }

    #[test]
    fn healthcheck_accepts_only_success_status_lines() {
        assert!(healthy_http_status(b"HTTP/1.1 200 OK\r\n"));
        assert!(healthy_http_status(b"HTTP/1.0 200 OK\r\n"));
        assert!(!healthy_http_status(
            b"HTTP/1.1 503 Service Unavailable\r\n"
        ));
    }
}
