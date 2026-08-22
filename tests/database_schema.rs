#![cfg(feature = "embedded-database")]

use tempfile::tempdir;
use trakkin::database::DatabaseRuntime;

#[tokio::test]
async fn fresh_database_starts() {
    let directory = tempdir().expect("temporary directory is created");
    let database = DatabaseRuntime::start(directory.path())
        .await
        .expect("fresh database starts");

    database
        .shutdown()
        .await
        .expect("fresh database shuts down");
}
