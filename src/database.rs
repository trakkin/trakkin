use std::{path::Path, time::Duration};

#[cfg(feature = "embedded-database")]
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
};

#[cfg(feature = "embedded-database")]
use postgresql_archive::configuration::zonky;
#[cfg(feature = "embedded-database")]
use postgresql_embedded::{PostgreSQL, SettingsBuilder};
use sqlx::{PgPool, migrate::Migrator, postgres::PgPoolOptions};
use thiserror::Error;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[cfg(feature = "embedded-database")]
const DATABASE_NAME: &str = "trakkin";
#[cfg(feature = "embedded-database")]
const DATABASE_USER: &str = "trakkin";
#[cfg(feature = "embedded-database")]
const POSTGRESQL_BOOTSTRAP_USER: &str = "postgres";
#[cfg(feature = "embedded-database")]
const POSTGRESQL_ARCHIVE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/postgresql.jar"));
#[cfg(feature = "embedded-database")]
const POSTGRESQL_VERSION: &str = env!("TRAKKIN_POSTGRESQL_VERSION");

pub struct DatabaseRuntime {
    pool: PgPool,
    #[cfg(feature = "embedded-database")]
    embedded: Option<Box<PostgreSQL>>,
}

impl DatabaseRuntime {
    #[cfg(feature = "embedded-database")]
    pub async fn start(data_directory: &Path) -> Result<Self, DatabaseError> {
        let root = data_directory.join("postgres");
        fs::create_dir_all(&root)?;
        let root = root.canonicalize()?;
        let runtime_directory = root.join("runtime").join(POSTGRESQL_VERSION);
        let data_directory = root.join("data");
        let password_file = root.join("password");
        let pgpass_file = root.join(".pgpass");

        if !runtime_directory.join("bin").exists() {
            postgresql_archive::extract(
                zonky::URL,
                &POSTGRESQL_ARCHIVE.to_vec(),
                &runtime_directory,
            )
            .await?;
        }

        let password = load_or_create_password(&password_file)?;
        let mut builder = SettingsBuilder::new()
            .releases_url(zonky::URL)
            .installation_dir(runtime_directory)
            .trust_installation_dir(true)
            .data_dir(data_directory)
            .password_file(pgpass_file)
            .host("127.0.0.1")
            .port(0)
            .username(DATABASE_USER)
            .password(password)
            .temporary(false)
            .timeout(Some(Duration::from_secs(30)));

        #[cfg(unix)]
        {
            let socket_directory = root.join("socket");
            fs::create_dir_all(&socket_directory)?;
            builder = builder.socket_dir(socket_directory);
        }

        let mut postgresql = PostgreSQL::new(builder.build());
        postgresql.setup().await?;
        postgresql.start().await?;

        let pool_result = async {
            if !postgresql.database_exists(DATABASE_NAME).await? {
                postgresql.create_database(DATABASE_NAME).await?;
            }
            let bootstrap_pool = connect_as_bootstrap(&postgresql, DATABASE_NAME).await?;
            provision_roles(&bootstrap_pool).await?;
            MIGRATOR.run(&bootstrap_pool).await?;
            grant_application_access(&bootstrap_pool).await?;
            sqlx::query(
                "SELECT set_trakkin_password($1)
",
            )
            .bind(&postgresql.settings().password)
            .execute(&bootstrap_pool)
            .await?;
            bootstrap_pool.close().await;
            let pool = PgPoolOptions::new()
                .max_connections(10)
                .acquire_timeout(Duration::from_secs(5))
                .connect(&postgresql.settings().url(DATABASE_NAME))
                .await?;
            Ok::<_, DatabaseError>(pool)
        }
        .await;

        match pool_result {
            Ok(pool) => Ok(Self {
                pool,
                embedded: Some(Box::new(postgresql)),
            }),
            Err(error) => {
                let _ = postgresql.stop().await;
                Err(error)
            }
        }
    }

    #[cfg(not(feature = "embedded-database"))]
    pub async fn start(_data_directory: &Path) -> Result<Self, DatabaseError> {
        Err(DatabaseError::EmbeddedDatabaseDisabled)
    }

    pub async fn connect(database_url: &str) -> Result<Self, DatabaseError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self {
            pool,
            #[cfg(feature = "embedded-database")]
            embedded: None,
        })
    }

    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    pub async fn shutdown(self) -> Result<(), DatabaseError> {
        self.pool.close().await;
        #[cfg(feature = "embedded-database")]
        if let Some(postgresql) = self.embedded {
            postgresql.stop().await?;
        }
        Ok(())
    }
}

#[cfg(feature = "embedded-database")]
async fn provision_roles(pool: &PgPool) -> Result<(), DatabaseError> {
    sqlx::query(
        "DO $$
         BEGIN
             IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trakkin') THEN
                 CREATE ROLE trakkin LOGIN;
             END IF;
         END
         $$
",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "ALTER ROLE trakkin NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE OR REPLACE FUNCTION set_trakkin_password(new_password TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
AS $$
         BEGIN
             EXECUTE format('ALTER ROLE trakkin PASSWORD %L', new_password);
         END
         $$
",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "REVOKE ALL ON FUNCTION set_trakkin_password(TEXT) FROM PUBLIC
",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "embedded-database")]
async fn grant_application_access(pool: &PgPool) -> Result<(), DatabaseError> {
    sqlx::query(
        "GRANT CONNECT ON DATABASE trakkin TO trakkin
",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "GRANT USAGE ON SCHEMA public TO trakkin
",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO trakkin
",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trakkin
",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "embedded-database")]
async fn connect_as_bootstrap(
    postgresql: &PostgreSQL,
    database_name: &str,
) -> Result<PgPool, DatabaseError> {
    let mut settings = postgresql.settings().clone();
    settings.username = POSTGRESQL_BOOTSTRAP_USER.to_owned();
    Ok(PgPool::connect(&settings.url(database_name)).await?)
}

#[cfg(feature = "embedded-database")]
fn load_or_create_password(path: &Path) -> Result<String, DatabaseError> {
    match OpenOptions::new().read(true).open(path) {
        Ok(mut file) => {
            let mut password = String::new();
            file.read_to_string(&mut password)?;
            Ok(password)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut bytes = [0_u8; 32];
            getrandom::fill(&mut bytes)?;
            let password = hex::encode(bytes);
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(path)?;
            file.write_all(password.as_bytes())?;
            Ok(password)
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[cfg(feature = "embedded-database")]
    #[error("PostgreSQL archive error: {0}")]
    Archive(#[from] postgresql_archive::Error),
    #[cfg(feature = "embedded-database")]
    #[error("embedded PostgreSQL error: {0}")]
    Embedded(#[from] postgresql_embedded::Error),
    #[cfg(not(feature = "embedded-database"))]
    #[error("TRAKKIN_DATABASE_URL is required unless the bundled feature is enabled")]
    EmbeddedDatabaseDisabled,
    #[error("database I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("database connection error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("database credential generation failed: {0}")]
    Random(#[from] getrandom::Error),
}

#[cfg(all(test, feature = "embedded-database"))]
mod tests {
    use tempfile::tempdir;

    use super::{DATABASE_NAME, DatabaseRuntime, POSTGRESQL_BOOTSTRAP_USER};

    #[tokio::test]
    async fn fresh_external_database_url_applies_migrations() {
        let directory = tempdir().expect("temporary directory is created");
        let embedded = DatabaseRuntime::start(directory.path())
            .await
            .expect("embedded database starts");
        let mut settings = embedded
            .embedded
            .as_ref()
            .expect("start creates an embedded database")
            .settings()
            .clone();
        settings.username = POSTGRESQL_BOOTSTRAP_USER.to_owned();
        let bootstrap = sqlx::PgPool::connect(&settings.url(DATABASE_NAME))
            .await
            .expect("bootstrap database connects");
        sqlx::query("CREATE DATABASE trakkin_external")
            .execute(&bootstrap)
            .await
            .expect("fresh external database is created");
        bootstrap.close().await;

        let external = DatabaseRuntime::connect(&settings.url("trakkin_external"))
            .await
            .expect("external database connects");
        let value = sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&external.pool())
            .await
            .expect("external database executes queries");

        assert_eq!(value, 1);
        external.shutdown().await.expect("external pool shuts down");
        embedded
            .shutdown()
            .await
            .expect("embedded database shuts down");
    }
}
