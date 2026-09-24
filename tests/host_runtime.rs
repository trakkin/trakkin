#![cfg(all(feature = "embedded-database", unix))]

use std::{process::Stdio, time::Duration};

use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::Command,
    time::{sleep, timeout},
};

#[tokio::test(flavor = "multi_thread")]
async fn external_host_starts_with_user_controlled_provider_registry_and_stops_cleanly() {
    let directory = tempdir().expect("temporary data directory is created");
    let address = available_address();
    let mut command = Command::new(env!("CARGO_BIN_EXE_trakkin"));
    command
        .env("TRAKKIN_BIND", &address)
        .env("TRAKKIN_DATA_DIR", directory.path())
        .env(
            "TRAKKIN_PROVIDER_RUNTIME_DIR",
            directory.path().join("provider-runtime"),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let child = command.spawn().expect("fresh host process starts");
    wait_until_listening(&address).await;

    let sources = request(&address, "GET", "/api/v1/provider-sources", None).await;
    assert_eq!(sources, (200, json!([])));

    let providers = request(&address, "GET", "/api/v1/providers", None).await;
    assert_eq!(providers, (200, json!([])));

    let unavailable_connection = request(
        &address,
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
    let output = timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("host stops before the deadline")
        .expect("host process can be reaped");
    assert!(output.status.success());
    assert_eq!(
        std::fs::read(directory.path().join("master.key"))
            .expect("connection master key is persisted")
            .len(),
        32
    );
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
    let body = serde_json::from_slice(&response[separator + 4..]).expect("HTTP body is JSON");
    (status, body)
}
