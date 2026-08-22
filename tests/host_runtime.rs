#![cfg(all(feature = "embedded-database", unix))]

use std::{path::PathBuf, process::Stdio, time::Duration};

use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::Command,
    time::{sleep, timeout},
};

#[tokio::test(flavor = "multi_thread")]
async fn external_host_dispatches_fixture_and_stops_cleanly() {
    let directory = tempdir().expect("temporary data directory is created");
    let fixture_adapter = fixture_adapter_path();
    assert!(
        fixture_adapter.is_file(),
        "fixture adapter must be built beside the host binary"
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_trakkin"));
    command
        .env("TRAKKIN_BIND", "127.0.0.1:0")
        .env("TRAKKIN_DATA_DIR", directory.path())
        .env("TRAKKIN_FIXTURE_ADAPTER", fixture_adapter)
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

    let connection = request(
        address,
        "POST",
        "/api/v1/connections",
        Some(json!({
            "adapterKey": "fixture",
            "displayName": "Fresh fixture",
            "settings": [
                { "key": "scenario", "value": { "kind": "text", "value": "hierarchical-catalog-assets-v1" } },
                { "key": "revision", "value": { "kind": "text", "value": "v1" } },
                { "key": "fault", "value": { "kind": "text", "value": "none" } },
                { "key": "instance", "value": { "kind": "text", "value": "alpha" } }
            ],
            "secrets": [{ "key": "fixture_token", "value": bytes("fixture-secret") }]
        })),
    )
    .await;
    assert_eq!(connection.0, 201);
    let connection_id = connection.1["id"]
        .as_str()
        .expect("created connection has an ID");
    assert!(connection.1.get("secrets").is_none());

    let discovery = request(
        address,
        "POST",
        &format!("/api/v1/connections/{connection_id}/discovery-tasks"),
        None,
    )
    .await;
    assert_eq!(discovery.0, 202);
    let task_id = discovery.1["id"]
        .as_str()
        .expect("discovery task has an ID");
    let task = wait_for_task(address, connection_id, task_id).await;
    assert_eq!(task["status"], "succeeded");

    let sources = request(
        address,
        "GET",
        &format!("/api/v1/connections/{connection_id}/sources"),
        None,
    )
    .await;
    assert_eq!(sources.0, 200);
    assert!(
        sources
            .1
            .as_array()
            .is_some_and(|sources| !sources.is_empty())
    );

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

async fn wait_for_task(address: &str, connection_id: &str, task_id: &str) -> Value {
    let path = format!("/api/v1/connections/{connection_id}/tasks/{task_id}");
    for _ in 0..100 {
        let response = request(address, "GET", &path, None).await;
        assert_eq!(response.0, 200);
        if response.1["terminal"] == true {
            return response.1;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("discovery task did not become terminal");
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

fn fixture_adapter_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_trakkin"))
        .parent()
        .expect("host binary has a directory")
        .join(format!(
            "trakkin-fixture-adapter{}",
            std::env::consts::EXE_SUFFIX
        ))
}

fn bytes(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}
