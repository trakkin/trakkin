#![cfg(all(feature = "embedded-database", unix))]

use std::{process::Stdio, time::Duration};

use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::Command,
    time::timeout,
};

#[tokio::test(flavor = "multi_thread")]
async fn external_host_bootstraps_provider_registry_and_stops_cleanly() {
    let directory = tempdir().expect("temporary data directory is created");
    let mut command = Command::new(env!("CARGO_BIN_EXE_trakkin"));
    command
        .env("TRAKKIN_BIND", "127.0.0.1:0")
        .env("TRAKKIN_DATA_DIR", directory.path())
        .env(
            "TRAKKIN_PROVIDER_RUNTIME_DIR",
            directory.path().join("provider-runtime"),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("fresh host process starts");
    let stdout = child.stdout.take().expect("host stdout is captured");
    let mut stdout = BufReader::new(stdout).lines();
    let readiness = timeout(Duration::from_secs(60), stdout.next_line())
        .await
        .expect("fresh host reports readiness before the deadline")
        .expect("host readiness output can be read")
        .expect("host emits a readiness line");
    let readiness: Value = serde_json::from_str(&readiness).expect("readiness is JSON");
    assert_eq!(readiness["status"], "ready");
    let address = readiness["address"]
        .as_str()
        .expect("readiness includes the bound address");

    let repositories = request(address, "GET", "/api/v1/provider-repositories", None).await;
    assert_eq!(repositories.0, 200);
    let official = repositories
        .1
        .as_array()
        .and_then(|repositories| {
            repositories
                .iter()
                .find(|repository| repository["id"] == "dev.trakkin.official")
        })
        .expect("official provider repository is bootstrapped");
    assert_eq!(official["official"], true);
    assert_eq!(official["enabled"], true);

    let installations = request(address, "GET", "/api/v1/provider-installations", None).await;
    assert_eq!(installations, (200, json!([])));

    let unavailable_connection = request(
        address,
        "POST",
        "/api/v1/connections",
        Some(json!({
            "providerId": "dev.trakkin.fixture",
            "displayName": "Unavailable fixture",
            "settings": [],
            "secrets": []
        })),
    )
    .await;
    assert_eq!(unavailable_connection.0, 404);

    let process_id = child.id().expect("running host has a process ID");
    let signal = std::process::Command::new("kill")
        .args(["-TERM", &process_id.to_string()])
        .status()
        .expect("SIGTERM command runs");
    assert!(signal.success());
    let status = timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("host stops before the deadline")
        .expect("host process can be reaped");
    assert!(status.success());
    assert_eq!(
        stdout
            .next_line()
            .await
            .expect("shutdown output can be read")
            .as_deref(),
        Some("trakkin stopped")
    );
    assert_eq!(
        std::fs::read(directory.path().join("master.key"))
            .expect("connection master key is persisted")
            .len(),
        32
    );
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
    let body = serde_json::from_slice(&response[separator + 4..]).expect("HTTP body is JSON");
    (status, body)
}
