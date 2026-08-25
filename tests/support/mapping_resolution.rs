use tempfile::{TempDir, tempdir};
use time::OffsetDateTime;
use trakkin::database::DatabaseRuntime;
use trakkin_mapping_contract::PortableEndpoint;
use uuid::Uuid;

pub struct MappingResolutionDatabase {
    _directory: TempDir,
    runtime: DatabaseRuntime,
}

impl MappingResolutionDatabase {
    pub async fn start() -> Self {
        let directory = tempdir().expect("temporary directory is created");
        let runtime = DatabaseRuntime::start(directory.path())
            .await
            .expect("fresh database starts");
        Self {
            _directory: directory,
            runtime,
        }
    }

    pub fn pool(&self) -> sqlx::PgPool {
        self.runtime.pool()
    }

    pub async fn shutdown(self) {
        self.runtime.shutdown().await.expect("database shuts down");
    }
}

pub fn endpoint(value: &str) -> PortableEndpoint {
    PortableEndpoint::parse(value).expect("fixture endpoint is valid")
}

pub fn fixture_time() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_893_456_245).expect("fixture time")
}

pub fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
