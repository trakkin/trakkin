use std::{path::Path, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    task::JoinHandle,
    time::timeout,
};
use tonic::{Request, Response, Status, transport::Channel};
use trakkin_provider_sdk::{
    BOOTSTRAP_VERSION, LaunchRequest, LaunchToken, MAXIMUM_BOOTSTRAP_MESSAGE_BYTES,
    current_protocol_version, parse_ready_message, supported_protocol_range,
    v1::{HandshakeRequest, HandshakeResponse, ProtocolVersion, ShutdownRequest},
};

use crate::application::{
    adapter_runtime::{AdapterIdentity, AdapterRetirement},
    providers::ProviderLaunchTarget,
};

use super::AdapterProcessError;
use trakkin_provider_sdk::v1::adapter_service_client::AdapterServiceClient;

const MAXIMUM_PROVIDER_LOG_LINE_BYTES: usize = 64 * 1024;

pub(super) struct ProviderProcess {
    child: Child,
    client: AdapterServiceClient<Channel>,
    token: LaunchToken,
    identity: AdapterIdentity,
    rpc_timeout: Duration,
    shutdown_timeout: Duration,
    stderr_task: Option<JoinHandle<()>>,
}

impl ProviderProcess {
    #[tracing::instrument(
        name = "provider.launch",
        skip_all,
        fields(
            provider.id = %target.provider_id,
            provider.version = %target.version,
            process.instance.id = %process_instance_id
        )
    )]
    pub(super) async fn launch(
        target: ProviderLaunchTarget,
        process_instance_id: String,
        host_version: &str,
        ready_timeout: Duration,
        rpc_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Result<Self, AdapterProcessError> {
        if let Err(error) = validate_process_instance_id(&process_instance_id) {
            tracing::warn!(
                launch.stage = "validate",
                error.message = %error,
                "provider launch failed"
            );
            return Err(error);
        }
        let launch_token = generate_launch_token().inspect_err(|error| {
            tracing::warn!(
                launch.stage = "launch_token",
                error.message = %error,
                "provider launch failed"
            );
        })?;
        let launch = LaunchRequest {
            bootstrap_version: BOOTSTRAP_VERSION,
            process_instance_id,
            bind_address: "127.0.0.1:0".to_owned(),
            launch_token,
        };
        let mut child = match spawn_child(&target.executable, &target.working_directory) {
            Ok(child) => child,
            Err(error) => {
                tracing::warn!(
                    launch.stage = "spawn",
                    error.message = %error,
                    "provider launch failed"
                );
                return Err(error);
            }
        };
        let Some(stderr) = child.stderr.take() else {
            tracing::warn!(launch.stage = "stderr_pipe", "provider launch failed");
            return Err(AdapterProcessError::MissingChildPipe("stderr"));
        };
        let stderr_task = tokio::spawn(forward_provider_stderr(
            stderr,
            target.provider_id.clone(),
            target.version.clone(),
            launch.process_instance_id.clone(),
        ));
        let initialized = initialize_child(
            &mut child,
            &launch,
            &target,
            host_version,
            ready_timeout,
            rpc_timeout,
        )
        .await;
        let (client, token, identity) = match initialized {
            Ok(initialized) => initialized,
            Err(error) => {
                tracing::warn!(
                    launch.stage = "initialize",
                    error.message = %error,
                    "provider launch failed"
                );
                force_stop(&mut child).await;
                finish_stderr_task(stderr_task, shutdown_timeout).await;
                return Err(error);
            }
        };
        Ok(Self {
            child,
            client,
            token,
            identity,
            rpc_timeout,
            shutdown_timeout,
            stderr_task: Some(stderr_task),
        })
    }

    pub(super) fn identity(&self) -> &AdapterIdentity {
        &self.identity
    }

    pub(super) fn client(&mut self) -> &mut AdapterServiceClient<Channel> {
        &mut self.client
    }

    pub(super) fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }

    pub(super) fn request<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        self.token.apply(&mut request);
        request
    }

    pub(super) async fn force_stop(&mut self) {
        force_stop(&mut self.child).await;
        self.finish_stderr().await;
    }

    #[tracing::instrument(
        name = "provider.retire",
        skip_all,
        fields(
            provider.id = %self.identity.adapter_key,
            process.instance.id = %self.identity.process_instance_id
        )
    )]
    pub(super) async fn retire(mut self) -> Result<AdapterRetirement, AdapterProcessError> {
        let request = self.request(ShutdownRequest { grace_period: None });
        let provider_id = self.identity.adapter_key.clone();
        let process_instance_id = self.identity.process_instance_id.clone();
        let shutdown = await_rpc(
            self.rpc_timeout,
            "Shutdown",
            &provider_id,
            &process_instance_id,
            self.client.shutdown(request),
        )
        .await;
        match shutdown {
            Ok(_) => {
                let retirement = match timeout(self.shutdown_timeout, self.child.wait()).await {
                    Ok(Ok(status)) if status.success() => {
                        tracing::info!(retirement.mode = "graceful", "provider retired");
                        Ok(AdapterRetirement::Graceful)
                    }
                    Ok(Ok(status)) => {
                        tracing::warn!(
                            retirement.mode = "unexpected_exit",
                            process.exit_code = status.code(),
                            "provider retirement failed"
                        );
                        Err(AdapterProcessError::UnexpectedExit(status.code()))
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(
                            retirement.mode = "wait_failed",
                            error.message = %error,
                            "provider retirement failed"
                        );
                        Err(error.into())
                    }
                    Err(_) => {
                        self.force_stop().await;
                        tracing::warn!(
                            retirement.mode = "forced",
                            retirement.reason = "exit_timeout",
                            "provider was forcefully retired"
                        );
                        Ok(AdapterRetirement::Forced)
                    }
                };
                self.finish_stderr().await;
                retirement
            }
            Err(error) => {
                let reason = if matches!(error, AdapterProcessError::RpcTimeout(_)) {
                    "rpc_timeout"
                } else {
                    "rpc_failed"
                };
                tracing::warn!(
                    retirement.mode = "forced",
                    retirement.reason = reason,
                    "provider was forcefully retired"
                );
                self.force_stop().await;
                Ok(AdapterRetirement::Forced)
            }
        }
    }

    async fn finish_stderr(&mut self) {
        if let Some(task) = self.stderr_task.take() {
            finish_stderr_task(task, self.shutdown_timeout).await;
        }
    }
}

#[tracing::instrument(
    name = "provider.rpc",
    level = "debug",
    skip_all,
    fields(
        rpc.system = "grpc",
        rpc.service = "trakkin.adapter.v1.AdapterService",
        rpc.method = operation,
        provider.id = provider_id,
        process.instance.id = process_instance_id
    )
)]
pub(super) async fn await_rpc<T>(
    deadline: Duration,
    operation: &'static str,
    provider_id: &str,
    process_instance_id: &str,
    rpc: impl Future<Output = Result<Response<T>, Status>>,
) -> Result<T, AdapterProcessError> {
    match timeout(deadline, rpc).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => {
            tracing::warn!(
                rpc.grpc.status_code = ?status.code(),
                error.message = %status,
                "provider RPC failed"
            );
            Err(status.into())
        }
        Err(_) => {
            tracing::warn!(timeout = true, "provider RPC timed out");
            Err(AdapterProcessError::RpcTimeout(operation))
        }
    }
}

pub(super) fn validate_process_instance_id(
    process_instance_id: &str,
) -> Result<(), AdapterProcessError> {
    if process_instance_id.is_empty() || process_instance_id.len() > 256 {
        return Err(AdapterProcessError::InvalidProcessInstanceId);
    }
    Ok(())
}

async fn initialize_child(
    child: &mut Child,
    launch: &LaunchRequest,
    target: &ProviderLaunchTarget,
    host_version: &str,
    ready_timeout: Duration,
    rpc_timeout: Duration,
) -> Result<(AdapterServiceClient<Channel>, LaunchToken, AdapterIdentity), AdapterProcessError> {
    let mut stdin = child
        .stdin
        .take()
        .ok_or(AdapterProcessError::MissingChildPipe("stdin"))?;
    let mut payload = serde_json::to_vec(launch)?;
    payload.push(b'\n');
    stdin.write_all(&payload).await?;
    stdin.shutdown().await?;
    drop(stdin);

    let stdout = child
        .stdout
        .take()
        .ok_or(AdapterProcessError::MissingChildPipe("stdout"))?;
    let mut reader = BufReader::new(stdout).take(MAXIMUM_BOOTSTRAP_MESSAGE_BYTES + 1);
    let mut line = String::new();
    let bytes = timeout(ready_timeout, reader.read_line(&mut line))
        .await
        .map_err(|_| AdapterProcessError::ReadyTimeout)??;
    if bytes == 0 {
        return Err(AdapterProcessError::MissingReadyMessage);
    }
    if bytes as u64 > MAXIMUM_BOOTSTRAP_MESSAGE_BYTES || !line.ends_with('\n') {
        return Err(AdapterProcessError::ReadyMessageTooLarge);
    }
    let (_, address) = parse_ready_message(line.as_bytes(), launch)
        .map_err(|_| AdapterProcessError::InvalidReadyMessage)?;
    let endpoint = format!("http://{address}");
    let client = timeout(rpc_timeout, AdapterServiceClient::connect(endpoint))
        .await
        .map_err(|_| AdapterProcessError::RpcTimeout("connect"))??;
    let token = LaunchToken::new(&launch.launch_token)
        .map_err(|_| AdapterProcessError::InvalidLaunchToken)?;
    let mut handshake_request = Request::new(HandshakeRequest {
        supported_protocol: Some(supported_protocol_range()),
        host_version: host_version.to_owned(),
        process_instance_id: launch.process_instance_id.clone(),
    });
    token.apply(&mut handshake_request);
    let handshake = await_rpc(
        rpc_timeout,
        "Handshake",
        &target.provider_id,
        &launch.process_instance_id,
        client.clone().handshake(handshake_request),
    )
    .await?;
    let selected_protocol =
        handshake
            .selected_protocol
            .ok_or(AdapterProcessError::InvalidResponse(
                "selected protocol is missing",
            ))?;
    validate_handshake(
        &handshake,
        &selected_protocol,
        &launch.process_instance_id,
        target,
    )?;
    Ok((
        client,
        token,
        AdapterIdentity {
            adapter_key: handshake.adapter_key,
            adapter_version: handshake.adapter_version,
            process_instance_id: handshake.process_instance_id,
            protocol_major: selected_protocol.major,
            protocol_minor: selected_protocol.minor,
        },
    ))
}

fn spawn_child(executable: &Path, working_directory: &Path) -> Result<Child, AdapterProcessError> {
    Ok(Command::new(executable)
        .current_dir(working_directory)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?)
}

async fn forward_provider_stderr(
    stderr: impl AsyncRead + Unpin,
    provider_id: String,
    provider_version: String,
    process_instance_id: String,
) {
    let mut stderr = BufReader::new(stderr);
    let mut line = Vec::new();
    let mut oversized = false;
    loop {
        let available = match stderr.fill_buf().await {
            Ok(available) => available,
            Err(error) => {
                tracing::event!(
                    target: "trakkin::provider",
                    tracing::Level::WARN,
                    provider.id = %provider_id,
                    provider.version = %provider_version,
                    process.instance.id = %process_instance_id,
                    error.message = %error,
                    "could not read provider stderr"
                );
                break;
            }
        };
        if available.is_empty() {
            if oversized {
                record_oversized_provider_stderr(
                    &provider_id,
                    &provider_version,
                    &process_instance_id,
                );
            } else if !line.is_empty() {
                record_provider_stderr_line(
                    &line,
                    &provider_id,
                    &provider_version,
                    &process_instance_id,
                );
            }
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let segment_length = newline.unwrap_or(available.len());
        if !oversized {
            if line.len().saturating_add(segment_length) > MAXIMUM_PROVIDER_LOG_LINE_BYTES {
                oversized = true;
                line.clear();
            } else {
                line.extend_from_slice(&available[..segment_length]);
            }
        }
        let consumed = newline.map_or(segment_length, |position| position + 1);
        stderr.consume(consumed);
        if newline.is_some() {
            if oversized {
                record_oversized_provider_stderr(
                    &provider_id,
                    &provider_version,
                    &process_instance_id,
                );
            } else {
                record_provider_stderr_line(
                    &line,
                    &provider_id,
                    &provider_version,
                    &process_instance_id,
                );
            }
            line.clear();
            oversized = false;
        }
    }
}

fn record_provider_stderr_line(
    line: &[u8],
    provider_id: &str,
    provider_version: &str,
    process_instance_id: &str,
) {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    match std::str::from_utf8(line) {
        Ok(line) => tracing::event!(
            target: "trakkin::provider",
            tracing::Level::DEBUG,
            provider.id = provider_id,
            provider.version = provider_version,
            process.instance.id = process_instance_id,
            provider.stderr.line = line,
            "provider stderr"
        ),
        Err(error) => tracing::event!(
            target: "trakkin::provider",
            tracing::Level::WARN,
            provider.id = provider_id,
            provider.version = provider_version,
            process.instance.id = process_instance_id,
            error.message = %error,
            "discarded invalid provider stderr line"
        ),
    }
}

fn record_oversized_provider_stderr(
    provider_id: &str,
    provider_version: &str,
    process_instance_id: &str,
) {
    tracing::event!(
        target: "trakkin::provider",
        tracing::Level::WARN,
        provider.id = provider_id,
        provider.version = provider_version,
        process.instance.id = process_instance_id,
        maximum_bytes = MAXIMUM_PROVIDER_LOG_LINE_BYTES,
        "discarded oversized provider stderr line"
    );
}

async fn finish_stderr_task(mut task: JoinHandle<()>, deadline: Duration) {
    if timeout(deadline, &mut task).await.is_err() {
        task.abort();
        let _ = task.await;
    }
}

fn generate_launch_token() -> Result<String, AdapterProcessError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)?;
    Ok(hex::encode(bytes))
}

fn validate_handshake(
    handshake: &HandshakeResponse,
    selected_protocol: &ProtocolVersion,
    process_instance_id: &str,
    target: &ProviderLaunchTarget,
) -> Result<(), AdapterProcessError> {
    if handshake.adapter_key != target.provider_id
        || handshake.adapter_version != target.version
        || handshake.process_instance_id != process_instance_id
        || selected_protocol.major != current_protocol_version().major
        || selected_protocol.minor != current_protocol_version().minor
    {
        return Err(AdapterProcessError::InvalidHandshake);
    }
    Ok(())
}

async fn force_stop(child: &mut Child) {
    if child.id().is_some() {
        let _ = child.kill().await;
    }
    let _ = child.wait().await;
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[derive(Clone, Default)]
    struct CapturedOutput(Arc<Mutex<Vec<u8>>>);

    impl CapturedOutput {
        fn records(&self) -> Vec<serde_json::Value> {
            let output = self.0.lock().expect("captured output lock is available");
            String::from_utf8(output.clone())
                .expect("captured tracing output is UTF-8")
                .lines()
                .map(|line| serde_json::from_str(line).expect("captured tracing record is JSON"))
                .collect()
        }
    }

    struct CapturedWriter(CapturedOutput);

    impl Write for CapturedWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0
                .0
                .lock()
                .expect("captured output lock is available")
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for CapturedOutput {
        type Writer = CapturedWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedWriter(self.clone())
        }
    }

    #[test]
    fn handshake_identity_matches_resolved_installation() {
        let target = ProviderLaunchTarget {
            provider_id: "dev.trakkin.fixture".to_owned(),
            version: "0.0.0".to_owned(),
            artifact_id: uuid::Uuid::nil(),
            executable: PathBuf::from("bin/provider"),
            working_directory: PathBuf::from("."),
        };
        let selected_protocol = current_protocol_version();
        let mut handshake = HandshakeResponse {
            selected_protocol: Some(selected_protocol),
            adapter_key: target.provider_id.clone(),
            adapter_version: target.version.clone(),
            process_instance_id: "process-1".to_owned(),
        };

        assert!(validate_handshake(&handshake, &selected_protocol, "process-1", &target).is_ok());
        handshake.adapter_version = "invalid-version".to_owned();
        assert!(matches!(
            validate_handshake(&handshake, &selected_protocol, "process-1", &target),
            Err(AdapterProcessError::InvalidHandshake)
        ));
    }

    #[tokio::test]
    async fn stderr_forwarding_preserves_traceback_context_and_recovers() {
        let output = CapturedOutput::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(output.clone())
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let (mut writer, reader) = tokio::io::duplex(64);
        let forwarder = forward_provider_stderr(
            reader,
            "dev.trakkin.fixture".to_owned(),
            "0.0.0".to_owned(),
            "process-1".to_owned(),
        );
        let writer = async move {
            writer
                .write_all(&vec![b'x'; MAXIMUM_PROVIDER_LOG_LINE_BYTES + 1])
                .await?;
            writer.write_all(b"\n").await?;
            writer.write_all(&[0xff, b'\n']).await?;
            writer
                .write_all(b"Traceback (most recent call last):\n")
                .await?;
            writer
                .write_all(b"  File \"provider.py\", line 7, in read\n")
                .await?;
            writer
                .write_all(b"RuntimeError: source refresh failed\n")
                .await?;
            writer.shutdown().await
        };

        let (writer, ()) = timeout(Duration::from_secs(1), async {
            tokio::join!(writer, forwarder)
        })
        .await
        .expect("provider stderr writer and forwarder do not block");
        writer.expect("provider stderr remains writable after malformed lines");

        let records = output.records();
        assert!(records.iter().any(|record| {
            record["fields"]["message"] == "discarded oversized provider stderr line"
        }));
        assert!(records.iter().any(|record| {
            record["fields"]["message"] == "discarded invalid provider stderr line"
        }));
        let diagnostics = records
            .iter()
            .filter(|record| record["fields"].get("provider.stderr.line").is_some())
            .collect::<Vec<_>>();
        assert_eq!(
            diagnostics
                .iter()
                .map(|record| record["fields"]["provider.stderr.line"]
                    .as_str()
                    .expect("provider stderr line is text"))
                .collect::<Vec<_>>(),
            [
                "Traceback (most recent call last):",
                "  File \"provider.py\", line 7, in read",
                "RuntimeError: source refresh failed",
            ]
        );
        for record in diagnostics {
            assert_eq!(record["fields"]["message"], "provider stderr");
            assert_eq!(record["fields"]["provider.id"], "dev.trakkin.fixture");
            assert_eq!(record["fields"]["provider.version"], "0.0.0");
            assert_eq!(record["fields"]["process.instance.id"], "process-1");
        }
    }
}
