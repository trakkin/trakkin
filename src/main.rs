use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
};

use thiserror::Error;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use trakkin::{host::Host, http, infrastructure::connection::ConnectionMasterKey};

const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8725";
const DEFAULT_DATA_DIRECTORY: &str = ".trakkin";
const MASTER_KEY_FILE: &str = "master.key";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("trakkin failed: {error}");
            ExitCode::FAILURE
        }
    }
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
    let runtime = host.into_runtime(
        configuration.fixture_adapter,
        configuration.connection_master_key,
    )?;
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
    fixture_adapter: PathBuf,
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
        let fixture_adapter = env::var_os("TRAKKIN_FIXTURE_ADAPTER")
            .map(PathBuf::from)
            .map_or_else(default_fixture_adapter, Ok)?;
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
            fixture_adapter,
            connection_master_key,
        })
    }
}

fn default_fixture_adapter() -> Result<PathBuf, ConfigurationError> {
    let executable = env::current_exe()?;
    let directory = executable
        .parent()
        .ok_or(ConfigurationError::ExecutableDirectory)?;
    Ok(directory.join(format!(
        "trakkin-fixture-adapter{}",
        env::consts::EXE_SUFFIX
    )))
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
    #[error("could not determine the fixture adapter directory")]
    ExecutableDirectory,
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
