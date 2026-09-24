use std::{net::SocketAddr, path::Path, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    time::timeout,
};
use tonic::{Request, Response, Status, transport::Channel};
use trakkin_provider_sdk::{
    BOOTSTRAP_VERSION, LaunchRequest, LaunchToken, ReadyMessage, current_protocol_version,
    supported_protocol_range,
    v1::{HandshakeRequest, HandshakeResponse, ProtocolVersion, ShutdownRequest},
};

use crate::application::{
    adapter_runtime::{AdapterIdentity, AdapterRetirement},
    providers::ProviderLaunchTarget,
};

use super::AdapterProcessError;
use trakkin_provider_sdk::v1::adapter_service_client::AdapterServiceClient;

const MAXIMUM_READY_MESSAGE_BYTES: u64 = 16 * 1024;

pub(super) struct ProviderProcess {
    child: Child,
    client: AdapterServiceClient<Channel>,
    token: LaunchToken,
    identity: AdapterIdentity,
    rpc_timeout: Duration,
    shutdown_timeout: Duration,
}

impl ProviderProcess {
    pub(super) async fn launch(
        target: ProviderLaunchTarget,
        process_instance_id: String,
        host_version: &str,
        ready_timeout: Duration,
        rpc_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Result<Self, AdapterProcessError> {
        validate_process_instance_id(&process_instance_id)?;
        let launch = LaunchRequest {
            bootstrap_version: BOOTSTRAP_VERSION,
            process_instance_id,
            bind_address: "127.0.0.1:0".to_owned(),
            launch_token: generate_launch_token()?,
        };
        let mut child = spawn_child(&target.executable, &target.working_directory)?;
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
                force_stop(&mut child).await;
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
    }

    pub(super) async fn retire(mut self) -> Result<AdapterRetirement, AdapterProcessError> {
        let request = self.request(ShutdownRequest { grace_period: None });
        let shutdown = timeout(self.rpc_timeout, self.client.shutdown(request)).await;
        if matches!(shutdown, Ok(Ok(_))) {
            match timeout(self.shutdown_timeout, self.child.wait()).await {
                Ok(Ok(status)) if status.success() => Ok(AdapterRetirement::Graceful),
                Ok(Ok(status)) => Err(AdapterProcessError::UnexpectedExit(status.code())),
                Ok(Err(error)) => Err(error.into()),
                Err(_) => {
                    self.force_stop().await;
                    Ok(AdapterRetirement::Forced)
                }
            }
        } else {
            self.force_stop().await;
            Ok(AdapterRetirement::Forced)
        }
    }
}

pub(super) async fn await_rpc<T>(
    deadline: Duration,
    operation: &'static str,
    rpc: impl Future<Output = Result<Response<T>, Status>>,
) -> Result<T, AdapterProcessError> {
    Ok(timeout(deadline, rpc)
        .await
        .map_err(|_| AdapterProcessError::RpcTimeout(operation))??
        .into_inner())
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
    let mut reader = BufReader::new(stdout).take(MAXIMUM_READY_MESSAGE_BYTES);
    let mut line = String::new();
    let bytes = timeout(ready_timeout, reader.read_line(&mut line))
        .await
        .map_err(|_| AdapterProcessError::ReadyTimeout)??;
    if bytes == 0 {
        return Err(AdapterProcessError::MissingReadyMessage);
    }
    if !line.ends_with('\n') {
        return Err(AdapterProcessError::ReadyMessageTooLarge);
    }
    let ready = serde_json::from_str::<ReadyMessage>(&line)?;
    let address = validate_ready_message(&ready, launch)?;
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
    let handshake = timeout(rpc_timeout, client.clone().handshake(handshake_request))
        .await
        .map_err(|_| AdapterProcessError::RpcTimeout("handshake"))??
        .into_inner();
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
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?)
}

fn generate_launch_token() -> Result<String, AdapterProcessError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)?;
    Ok(hex::encode(bytes))
}

fn validate_ready_message(
    ready: &ReadyMessage,
    launch: &LaunchRequest,
) -> Result<SocketAddr, AdapterProcessError> {
    if ready.bootstrap_version != BOOTSTRAP_VERSION
        || ready.process_instance_id != launch.process_instance_id
        || ready.launch_token != launch.launch_token
    {
        return Err(AdapterProcessError::InvalidReadyMessage);
    }
    let address = ready
        .address
        .parse::<SocketAddr>()
        .map_err(|_| AdapterProcessError::InvalidReadyMessage)?;
    if !address.ip().is_loopback() {
        return Err(AdapterProcessError::InvalidReadyMessage);
    }
    Ok(address)
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
    use std::path::PathBuf;

    use super::*;

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
}
