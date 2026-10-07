#![cfg(all(feature = "embedded-database", unix))]

use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tempfile::tempdir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::Command,
    time::{sleep, timeout},
};
use trakkin_mappings_corpus::git::RepositoryStore;

#[path = "support/mapping_git.rs"]
#[expect(dead_code, reason = "Host smoke tests do not pause Git requests")]
mod mapping_git;
use mapping_git::GitServer;

fn host_command(directory: &std::path::Path, address: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_trakkin"));
    command
        .env("TRAKKIN_BIND", address)
        .env("TRAKKIN_DATA_DIR", directory)
        .env(
            "TRAKKIN_PROVIDER_RUNTIME_DIR",
            directory.join("provider-runtime"),
        )
        .env_remove("TRAKKIN_DATABASE_URL")
        .env_remove("TRAKKIN_MAPPINGS_METADATA")
        .env_remove("TRAKKIN_MASTER_KEY")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

#[tokio::test(flavor = "multi_thread")]
async fn external_host_starts_with_user_controlled_provider_registry_and_stops_cleanly() {
    let directory = tempdir().expect("temporary data directory is created");
    let address = available_address();
    let child = host_command(directory.path(), &address)
        .spawn()
        .expect("fresh host process starts");
    wait_until_listening(&address).await;
    assert_eq!(
        request(&address, "GET", "/api/v1/provider-sources", None).await,
        (200, json!([]))
    );
    assert_eq!(
        request(&address, "GET", "/api/v1/providers", None).await,
        (200, json!([]))
    );
    let mappings = request(&address, "GET", "/api/v1/mappings/state", None).await;
    assert_eq!(mappings.0, 200);
    assert_eq!(mappings.1["records"], 0);
    assert_eq!(mappings.1["active"], 0);
    let page = request(&address, "GET", "/api/v1/mappings", None).await;
    assert_eq!(page.0, 200);
    assert_eq!(page.1["items"], json!([]));
    assert_eq!(page.1["fingerprint"], mappings.1["fingerprint"]);
    let mutation = request(&address, "POST", "/api/v1/mappings", None).await;
    assert_eq!(mutation.0, 400);
    assert_eq!(mutation.1["message"], "mapping request body is invalid");
    let record = "com.imdb://title/tt0133093 <=> org.themoviedb://movie/603";
    let preview = request(
        &address,
        "POST",
        "/api/v1/mappings/preview",
        Some(json!({"record": record})),
    )
    .await;
    assert_eq!(preview.0, 200);
    assert_eq!(preview.1["status"], "resolved");
    let saved = request(
        &address,
        "POST",
        "/api/v1/mappings",
        Some(json!({"expectedFingerprint": mappings.1["fingerprint"], "record": record})),
    )
    .await;
    assert_eq!(saved.0, 200);
    assert_eq!(saved.1["records"], 1);
    assert_eq!(saved.1["active"], 1);
    let page = request(&address, "GET", "/api/v1/mappings", None).await;
    assert_eq!(page.0, 200);
    assert_eq!(page.1["fingerprint"], saved.1["fingerprint"]);
    assert_eq!(page.1["items"][0]["canonicalStatement"], record);
    let stale = request(
        &address,
        "POST",
        "/api/v1/mappings",
        Some(json!({"expectedFingerprint": mappings.1["fingerprint"], "record": record})),
    )
    .await;
    assert_eq!(stale.0, 409);
    assert_eq!(stale.1["code"], "mappings_changed");
    for (path, status) in [
        ("/api/v1/mappings/export", 400),
        ("/api/v1/mappings/sources/local/export", 400),
        ("/api/v1/mappings/sources/local/archive", 400),
    ] {
        assert_eq!(request(&address, "GET", path, None).await.0, status);
    }
    for (path, status) in [
        ("/api/v1/mappings/import", 400),
        ("/api/v1/mappings/import/archive", 400),
    ] {
        assert_eq!(
            request(&address, "POST", path, Some(json!({}))).await.0,
            status
        );
    }
    let repository = RepositoryStore::open(&directory.path().join("mapping-repositories"))
        .unwrap()
        .local()
        .await
        .unwrap();
    assert_eq!(
        repository.current().await.unwrap().corpus.entries().count(),
        1
    );
    let unavailable = request(&address, "POST", "/api/v1/connections", Some(json!({"providerId": "dev.trakkin.fixture", "displayName": "Unavailable fixture", "settings": [], "secrets": []}))).await;
    assert_eq!(unavailable.0, 404);
    stop_host(child).await;
    assert_eq!(
        std::fs::read(directory.path().join("master.key"))
            .expect("connection master key is persisted")
            .len(),
        32
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn user_managed_git_sources_fetch_without_a_browser_and_control_activation() {
    let directory = tempdir().unwrap();
    let remote_directory = tempdir().unwrap();
    let server = GitServer::open(remote_directory.path()).await;
    let record = "com.imdb://title/tt0133093 <=> org.themoviedb://movie/603";
    let revision = server.commit(record).await;
    let address = available_address();
    let child = host_command(directory.path(), &address).spawn().unwrap();
    wait_until_listening(&address).await;
    let initial = request(&address, "GET", "/api/v1/mappings/state", None).await;
    let sources = request(&address, "GET", "/api/v1/mappings/sources", None).await;
    assert_eq!(sources.1["sources"].as_array().unwrap().len(), 1);
    assert_eq!(sources.1["sources"][0]["id"], "local");
    assert_eq!(sources.1["fingerprint"], initial.1["fingerprint"]);
    let created = request(&address, "POST", "/api/v1/mappings/sources", Some(json!({"expectedFingerprint":initial.1["fingerprint"], "displayName":"Fixture repository", "url":server.remote, "branch":"main", "pollIntervalSeconds":60, "enabled":true}))).await;
    assert_eq!(created.0, 200);
    timeout(Duration::from_secs(10), async {
        loop {
            let statuses = request(&address, "GET", "/api/v1/mappings/sources", None).await;
            if statuses.1["sources"][1]["revision"] == revision {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let page = request(&address, "GET", "/api/v1/mappings", None).await;
    assert_eq!(page.1["items"][0]["canonicalStatement"], record);
    let statuses = request(&address, "GET", "/api/v1/mappings/sources", None).await;
    assert_eq!(statuses.0, 200);
    assert_eq!(
        statuses.1["sources"][1]["displayName"],
        "Fixture repository"
    );
    assert_eq!(statuses.1["sources"][1]["branch"], "main");
    assert_eq!(statuses.1["sources"][1]["revision"], revision);
    assert_eq!(statuses.1["sources"][1]["consecutiveFailures"], 0);
    assert!(
        statuses.1["sources"][1]["lastCheckedAt"]
            .as_str()
            .unwrap()
            .contains('T')
    );
    assert!(
        statuses.1["sources"][1]["nextCheckAt"]
            .as_str()
            .unwrap()
            .contains('T')
    );
    let source_id = statuses.1["sources"][1]["id"].as_str().unwrap();
    let source_path = format!("/api/v1/mappings/sources/{source_id}");
    let state = request(&address, "GET", "/api/v1/mappings/state", None).await;
    let disabled = request(&address, "PUT", &source_path, Some(json!({"expectedFingerprint":state.1["fingerprint"], "displayName":"Fixture repository", "url":server.remote, "branch":"main", "pollIntervalSeconds":60, "enabled":false}))).await;
    assert_eq!(disabled.0, 200);
    assert_eq!(disabled.1["records"], 0);
    let sources = request(&address, "GET", "/api/v1/mappings/sources", None).await;
    assert_eq!(sources.1["sources"][1]["cachedRecords"], 1);
    assert_eq!(sources.1["sources"][1]["revision"], revision);
    let disabled_refresh = request(
        &address,
        "POST",
        &format!("{source_path}/refresh"),
        Some(json!({"expectedFingerprint":disabled.1["fingerprint"]})),
    )
    .await;
    assert_eq!(disabled_refresh.0, 400);
    let saved = request(
        &address,
        "POST",
        "/api/v1/mappings",
        Some(json!({"expectedFingerprint":disabled.1["fingerprint"], "record":record})),
    )
    .await;
    assert_eq!(saved.0, 200);
    assert_eq!(saved.1["records"], 1);
    let repository = RepositoryStore::open(&directory.path().join("mapping-repositories"))
        .unwrap()
        .local()
        .await
        .unwrap();
    let local = repository.current().await.unwrap();
    assert_eq!(local.corpus.entries().count(), 1);
    let sources = request(&address, "GET", "/api/v1/mappings/sources", None).await;
    assert_eq!(sources.1["sources"][0]["revision"], local.commit);
    let refreshed = request(
        &address,
        "POST",
        "/api/v1/mappings/sources/local/refresh",
        Some(json!({"expectedFingerprint":saved.1["fingerprint"]})),
    )
    .await;
    assert_eq!(refreshed.0, 200);
    let removed = request(
        &address,
        "DELETE",
        &source_path,
        Some(json!({"expectedFingerprint":refreshed.1["fingerprint"]})),
    )
    .await;
    assert_eq!(removed.0, 200);
    assert_eq!(removed.1["records"], 1);
    stop_host(child).await;
}

async fn stop_host(child: tokio::process::Child) {
    let process_id = child.id().expect("running host has a process ID");
    let signal = std::process::Command::new("kill")
        .args(["-TERM", &process_id.to_string()])
        .status()
        .expect("SIGTERM command runs");
    assert!(signal.success());
    let output = timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("host stops before the deadline")
        .expect("host process can be reaped");
    assert!(output.status.success());
}

fn available_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("test port is reserved");
    listener
        .local_addr()
        .expect("test listener has an address")
        .to_string()
}

async fn wait_until_listening(address: &str) {
    timeout(Duration::from_secs(60), async {
        loop {
            if TcpStream::connect(address).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("host listens before the deadline");
}

async fn request(address: &str, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let body = body.map_or_else(String::new, |body| body.to_string());
    let mut stream = TcpStream::connect(address)
        .await
        .expect("HTTP connection opens");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("HTTP request is written");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("HTTP response is read");
    let separator = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response contains headers");
    let headers = std::str::from_utf8(&response[..separator]).expect("HTTP headers are UTF-8");
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .expect("HTTP status is valid");
    let payload = &response[separator + 4..];
    let body = if payload.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(payload).unwrap_or_else(|_| {
            Value::String(String::from_utf8(payload.to_vec()).expect("HTTP document is UTF-8"))
        })
    };
    (status, body)
}
